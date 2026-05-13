% run_adaptive_convergence.m - Standalone runner for Adaptive Convergence Agent
%
% Purpose:
%   Executes intelligent adaptive mesh convergence study using the
%   AdaptiveConvergenceAgent class. This agent learns from preflight tests
%   to intelligently navigate convergence rather than using fixed grid sweeps.
%
% Location: Scripts/Modes/Convergence/ (convergence-specific components)
%
% Features:
%   - Preflight testing to gather training data
%   - Pattern recognition for convergence behavior
%   - Adaptive jump factors based on observed rates
%   - Result caching to avoid redundant runs
%   - Early stopping when criterion met
%   - Sensitivity quantification
%   - Decision trace logging
%
% Usage:
%   cd Scripts/Modes/Convergence
%   run_adaptive_convergence
%
% Outputs:
%   - Convergence trace (saved to Results/FD/Convergence/<study_id>/Data/)
%   - Selected sequence of (Nx, Ny, dt) and metrics
%   - Final recommended converged configuration
%
% Dependencies:
%   - AdaptiveConvergenceAgent.m (same directory)
%   - Scripts/Infrastructure/* (paths added below)
%
% Author: MECH0020 Framework
% Date: February 2026

clc; clear; close all;

fprintf('========================================================================\n');
fprintf('  ADAPTIVE CONVERGENCE AGENT - INTELLIGENT MESH REFINEMENT\n');
fprintf('========================================================================\n\n');

% ===== SETUP PATHS =====
% Note: This script is in Scripts/Modes/Convergence, so repo_root is 3 levels up.
script_dir = fileparts(mfilename('fullpath'));
repo_root = fullfile(script_dir, '..', '..', '..');
infra_dir = fullfile(repo_root, 'Scripts', 'Infrastructure');
if exist(infra_dir, 'dir') ~= 7
    error('AdaptiveConvergenceRunner:MissingInfrastructureDir', ...
        'Infrastructure directory not found: %s', infra_dir);
end
addpath(infra_dir);  % Bootstrap PathSetup visibility
PathSetup.attach_and_verify();

% ===== CREATE BASE PARAMETERS =====
if exist('create_default_parameters', 'file') ~= 2
    error('AdaptiveConvergenceRunner:MissingDefaultParametersFactory', ...
        ['create_default_parameters.m is required but not available on the MATLAB path. ', ...
         'Run via canonical entrypoints (PathSetup.attach_and_verify) or repair the project path.']);
end
Parameters = create_default_parameters();

% Override for convergence study (shorter time, fewer snapshots)
Parameters.Tfinal = 1.0;
Parameters.num_snapshots = 3;

fprintf('Base Parameters:\n');
fprintf('  Domain: [%.1f x %.1f]\n', Parameters.Lx, Parameters.Ly);
fprintf('  Initial: Nx=%d, Ny=%d\n', Parameters.Nx, Parameters.Ny);
fprintf('  Time: dt=%.4f, Tfinal=%.2f\n', Parameters.dt, Parameters.Tfinal);
fprintf('  IC: %s\n', Parameters.ic_type);
fprintf('  Viscosity: %.2e\n\n', Parameters.nu);

% ===== CREATE SETTINGS FOR CONVERGENCE AGENT =====
settings = struct();

% Convergence settings
settings.convergence = struct();
settings.convergence.tolerance = 5e-2;  % Target convergence tolerance
settings.convergence.save_iteration_figures = true;  % Save figures for each iteration
study_stamp = char(datetime('now', 'Format', 'yyyyMMdd_HHmmss'));
study_id = sprintf('AdaptiveConvergence_%s', study_stamp);
if exist('PathBuilder', 'class') == 8 || exist('PathBuilder', 'file') == 2
    study_paths = PathBuilder.get_run_paths('FD', 'Convergence', study_id);
    PathBuilder.ensure_directories(study_paths);
    settings.convergence.study_dir = study_paths.data;
    settings.convergence.preflight_figs_dir = fullfile(study_paths.figures_convergence, 'Preflight');
else
    error('AdaptiveConvergenceRunner:MissingPathBuilder', ...
        ['PathBuilder is required for convergence artifact paths. ', ...
         'This is a packaged internal dependency and should not be bypassed.']);
end

% Create output directories
if ~exist(settings.convergence.study_dir, 'dir')
    mkdir(settings.convergence.study_dir);
end
if ~exist(settings.convergence.preflight_figs_dir, 'dir')
    mkdir(settings.convergence.preflight_figs_dir);
end

% Figure settings
settings.figures = struct();
settings.figures.close_after_save = true;  % Free memory after saving

fprintf('Convergence Study Settings:\n');
fprintf('  Tolerance: %.2e\n', settings.convergence.tolerance);
fprintf('  Output Dir: %s\n', settings.convergence.study_dir);
fprintf('  Save Iteration Figures: %s\n\n', string(settings.convergence.save_iteration_figures));

% ===== CREATE AND INITIALIZE AGENT =====
fprintf('Initializing Adaptive Convergence Agent...\n');
agent = AdaptiveConvergenceAgent(Parameters, settings);
fprintf('Agent initialized.\n\n');

% ===== RUN PREFLIGHT TESTS =====
fprintf('========================================================================\n');
fprintf('  PHASE 1: PREFLIGHT TESTING\n');
fprintf('========================================================================\n\n');

agent.run_preflight();

fprintf('\n========================================================================\n');
fprintf('  PHASE 2: ADAPTIVE CONVERGENCE EXECUTION\n');
fprintf('========================================================================\n\n');

% ===== EXECUTE CONVERGENCE STUDY =====
[N_star, results_table, metadata] = agent.execute_convergence_study();

% ===== SAVE RESULTS =====
fprintf('\n========================================================================\n');
fprintf('  SAVING RESULTS\n');
fprintf('========================================================================\n\n');

% Save convergence trace
trace_file = fullfile(settings.convergence.study_dir, 'convergence_trace.csv');
if ~isempty(results_table)
    writetable(results_table, trace_file);
    fprintf('Convergence trace saved: %s\n', trace_file);
end

% Save metadata
meta_file = fullfile(settings.convergence.study_dir, 'convergence_metadata.mat');
safe_save_mat(meta_file, struct( ...
    'metadata', metadata, ...
    'N_star', N_star, ...
    'Parameters', Parameters, ...
    'settings', settings));
fprintf('Metadata saved: %s\n', meta_file);

% Save learning model details
learning_file = fullfile(settings.convergence.study_dir, 'learning_model.txt');
fid = fopen(learning_file, 'w');
fprintf(fid, 'ADAPTIVE CONVERGENCE AGENT - LEARNING MODEL\n');
fprintf(fid, '==========================================\n\n');
fprintf(fid, 'Convergence Rate (p): %.3f\n', metadata.learning_model.p_convergence);
fprintf(fid, 'Computational Scaling (alpha): %.3f\n', metadata.learning_model.alpha_cost);
fprintf(fid, 'Primary Quantity of Interest: %s\n', metadata.learning_model.primary_qoi);
fprintf(fid, 'Recommended Starting N: %d\n', metadata.learning_model.N_start_recommended);
fprintf(fid, 'Initial Jump Factor: %.2f\n', metadata.learning_model.initial_jump_factor);
fprintf(fid, '\n');
fprintf(fid, 'FINAL RESULTS\n');
fprintf(fid, '=============\n\n');
fprintf(fid, 'Converged Grid Resolution (N*): %d\n', N_star);
fprintf(fid, 'Target Tolerance: %.2e\n', metadata.tolerance);
fprintf(fid, 'Total Iterations: %d\n', metadata.total_iterations);
fprintf(fid, 'Total Time: %.2f seconds\n', metadata.total_time);
fprintf(fid, 'Preflight Runs: %d\n', metadata.preflight_runs);
fclose(fid);
fprintf('Learning model summary saved: %s\n', learning_file);

% ===== FINAL SUMMARY =====
fprintf('\n========================================================================\n');
fprintf('  ADAPTIVE CONVERGENCE STUDY COMPLETE\n');
fprintf('========================================================================\n\n');

fprintf('Converged Grid Resolution: N* = %d x %d\n', N_star, N_star);
fprintf('Target Tolerance: %.2e\n', metadata.tolerance);
fprintf('Total Iterations: %d\n', metadata.total_iterations);
fprintf('Total Time: %.2f seconds\n', metadata.total_time);
fprintf('Learning Model:\n');
fprintf('  Convergence Rate: p = %.2f\n', metadata.learning_model.p_convergence);
fprintf('  Cost Scaling: alpha = %.2f\n', metadata.learning_model.alpha_cost);
fprintf('  Primary QoI: %s\n', metadata.learning_model.primary_qoi);

fprintf('\nOutput Files:\n');
fprintf('  - Convergence Trace: %s\n', trace_file);
fprintf('  - Metadata: %s\n', meta_file);
fprintf('  - Learning Summary: %s\n', learning_file);
fprintf('  - Study Directory: %s\n', settings.convergence.study_dir);

fprintf('\n========================================================================\n\n');
