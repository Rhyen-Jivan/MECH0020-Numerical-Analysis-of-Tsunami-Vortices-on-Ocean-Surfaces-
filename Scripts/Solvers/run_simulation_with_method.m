function [fig_handle, analysis] = run_simulation_with_method(Parameters)
% run_simulation_with_method - Compatibility shim over ModeDispatcher.
%
% Legacy signature retained:
%   [fig_handle, analysis] = run_simulation_with_method(Parameters)
%
% Compatibility role:
%   - Preserves older script/test entrypoints that expect a direct
%     method-level launcher
%   - Delegates active FD/Spectral/FV/SWE execution to ModeDispatcher
%   - Returns [] for fig_handle to preserve the legacy signature
%
% %//NOTE
% This file remains ACTIVE_SUPPORT. New launch surfaces should use the
% UI-driven path or RunDispatcher/ModeDispatcher directly instead of adding
% more wrappers here.

    if nargin < 1 || ~isstruct(Parameters)
        error('run_simulation_with_method:InvalidInput', ...
            'Parameters must be provided as a struct.');
    end

    ensure_solver_shim_paths();

    method_normalized = normalize_method_name(Parameters);
    if method_normalized == "bathymetry"
        warning('run_simulation_with_method:BathymetryCompatibility', ...
            ['Bathymetry is compatibility-only and bypasses ModeDispatcher ', ...
             'in this shim.']);
        [fig_handle, analysis] = Variable_Bathymetry_Analysis(Parameters);
        analysis = normalize_analysis_struct(analysis, Parameters, method_normalized);
        print_metrics_summary(analysis);
        return;
    end

    run_method = resolve_dispatch_method(method_normalized);
    ic_type = resolve_ic_type(Parameters);
    run_config = Build_Run_Config(run_method, 'Evolution', ic_type);

    settings = build_compatibility_settings();
    settings.compatibility = struct();
    settings.compatibility.return_analysis = true;
    settings.compatibility.source = 'run_simulation_with_method';

    [results, ~] = ModeDispatcher(run_config, Parameters, settings);

    fig_handle = [];
    if ~isfield(results, 'analysis') || isempty(results.analysis)
        error('run_simulation_with_method:MissingAnalysis', ...
            'ModeDispatcher did not return Results.analysis for compatibility shim.');
    end

    analysis = normalize_analysis_struct(results.analysis, Parameters, method_normalized);
    print_metrics_summary(analysis);
end

function ensure_solver_shim_paths()
% ensure_solver_shim_paths - Bootstrap active paths for compatibility calls.
    if exist('PathSetup', 'class') ~= 8
        this_dir = fileparts(mfilename('fullpath'));   % .../Scripts/Solvers
        scripts_dir = fileparts(this_dir);             % .../Scripts
        addpath(genpath(scripts_dir));
        utilities_dir = fullfile(fileparts(scripts_dir), 'utilities');
        if exist(utilities_dir, 'dir') == 7
            addpath(utilities_dir);
        end
    end
    PathSetup.attach_and_verify();
end

function settings = build_compatibility_settings()
% build_compatibility_settings - Lightweight Settings payload for shim runs.
    if exist('Settings', 'file') == 2
        settings = Settings();
    else
        error('run_simulation_with_method:MissingSettings', ...
            'Settings.m is required on path for compatibility shim execution.');
    end

    % Keep compatibility runs lightweight while preserving dispatcher contract.
    settings.monitor_enabled = false;
    settings.append_to_master = false;
end

function run_method = resolve_dispatch_method(method_name)
    switch method_name
        case "finite_difference"
            run_method = 'FD';
        case "spectral"
            run_method = 'Spectral';
        case "finite_volume"
            run_method = 'FV';
        case "shallow_water"
            run_method = 'SWE';
        otherwise
            error('run_simulation_with_method:UnknownMethod', ...
                'Unknown method token: %s', char(method_name));
    end
end

function ic_type = resolve_ic_type(Parameters)
    if isfield(Parameters, 'ic_type') && ~isempty(Parameters.ic_type)
        ic_type = char(string(Parameters.ic_type));
    else
        error('run_simulation_with_method:MissingICType', ...
            'Parameters.ic_type is required for compatibility dispatch.');
    end
end

function method_name = normalize_method_name(Parameters)
    if isfield(Parameters, 'method') && ~isempty(Parameters.method)
        method_raw = lower(char(string(Parameters.method)));
    else
        error('run_simulation_with_method:MissingMethod', ...
            'Missing required Parameters.method field.');
    end

    switch method_raw
        case {'fd', 'finite_difference', 'finite difference'}
            method_name = "finite_difference";
        case {'spectral method', 'fft', 'spectral'}
            method_name = "spectral";
        case {'fv', 'finite_volume', 'finite volume'}
            method_name = "finite_volume";
        case {'swe', 'shallow_water', 'shallow water'}
            method_name = "shallow_water";
        case {'bathymetry', 'variable_bathymetry', 'variable bathymetry'}
            method_name = "bathymetry";
        otherwise
            method_name = string(method_raw);
    end
end

function analysis = normalize_analysis_struct(analysis, Parameters, method_name)
    % Ensure compatibility payloads keep the modern analysis contract.
    if ~isfield(analysis, 'method') || isempty(analysis.method)
        analysis.method = char(method_name);
    end

    % Pre-create snapshot containers so later field access is always valid.
    if ~isfield(analysis, 'omega_snaps')
        analysis.omega_snaps = [];
    end
    if ~isfield(analysis, 'psi_snaps')
        analysis.psi_snaps = [];
    end

    % Prefer solver-provided requested/actual times; otherwise recover from inputs.
    if ~isfield(analysis, 'snapshot_times_requested') || isempty(analysis.snapshot_times_requested)
        if isfield(analysis, 'snapshot_times') && ~isempty(analysis.snapshot_times)
            analysis.snapshot_times_requested = analysis.snapshot_times(:);
        elseif isfield(Parameters, 'plot_snap_times') && ~isempty(Parameters.plot_snap_times)
            analysis.snapshot_times_requested = Parameters.plot_snap_times(:);
        elseif isfield(Parameters, 'snap_times') && ~isempty(Parameters.snap_times)
            analysis.snapshot_times_requested = Parameters.snap_times(:);
        else
            analysis.snapshot_times_requested = [];
        end
    else
        analysis.snapshot_times_requested = analysis.snapshot_times_requested(:);
    end

    if ~isfield(analysis, 'snapshot_times_actual') || isempty(analysis.snapshot_times_actual)
        if isfield(analysis, 'time_vec') && ~isempty(analysis.time_vec)
            analysis.snapshot_times_actual = analysis.time_vec(:);
        elseif ~isempty(analysis.snapshot_times_requested)
            analysis.snapshot_times_actual = analysis.snapshot_times_requested;
        else
            analysis.snapshot_times_actual = [];
        end
    else
        analysis.snapshot_times_actual = analysis.snapshot_times_actual(:);
    end

    if ~isfield(analysis, 'snapshot_times') || isempty(analysis.snapshot_times)
        analysis.snapshot_times = analysis.snapshot_times_requested;
    else
        analysis.snapshot_times = analysis.snapshot_times(:);
    end

    if ~isfield(analysis, 'time_vec') || isempty(analysis.time_vec)
        analysis.time_vec = analysis.snapshot_times_actual;
    else
        analysis.time_vec = analysis.time_vec(:);
    end

    % Derive the snapshot count from whichever source is populated.
    if ~isfield(analysis, 'snapshots_stored') || isempty(analysis.snapshots_stored)
        if ~isempty(analysis.snapshot_times)
            analysis.snapshots_stored = numel(analysis.snapshot_times);
        elseif ~isempty(analysis.omega_snaps)
            analysis.snapshots_stored = size(analysis.omega_snaps, 3);
        else
            analysis.snapshots_stored = 0;
        end
    end

    % Resolve Nx, Ny from input parameters first, then infer from snapshot array shape.
    if ~isfield(analysis, 'Nx')
        if isfield(Parameters, 'Nx')
            analysis.Nx = Parameters.Nx;
        elseif ~isempty(analysis.omega_snaps)
            analysis.Nx = size(analysis.omega_snaps, 2);
        else
            analysis.Nx = 0;
        end
    end

    if ~isfield(analysis, 'Ny')
        if isfield(Parameters, 'Ny')
            analysis.Ny = Parameters.Ny;
        elseif ~isempty(analysis.omega_snaps)
            analysis.Ny = size(analysis.omega_snaps, 1);
        else
            analysis.Ny = 0;
        end
    end

    % Maintain an explicit total point count used in summaries/benchmarking.
    if ~isfield(analysis, 'grid_points') || isempty(analysis.grid_points)
        analysis.grid_points = analysis.Nx * analysis.Ny;
    end

    % Compute peak vorticity magnitude if the solver did not provide one.
    if ~isfield(analysis, 'peak_abs_omega') || isempty(analysis.peak_abs_omega)
        if ~isempty(analysis.omega_snaps)
            analysis.peak_abs_omega = max(abs(analysis.omega_snaps(:)));
        else
            analysis.peak_abs_omega = NaN;
        end
    end

    % Preserve compatibility with legacy consumers that expect peak_vorticity.
    if ~isfield(analysis, 'peak_vorticity') || isempty(analysis.peak_vorticity)
        analysis.peak_vorticity = analysis.peak_abs_omega;
    end
end

function print_metrics_summary(analysis)
    % Human-readable run summary for terminal logs and quick sanity checks.
    SafeConsoleIO.fprintf('\n');
    SafeConsoleIO.fprintf('=============== SIMULATION SUMMARY ===============\n');
    SafeConsoleIO.fprintf('Method: %s\n', char(string(analysis.method)));
    SafeConsoleIO.fprintf('Grid: %d x %d (%d points)\n', analysis.Nx, analysis.Ny, analysis.grid_points);

    if isfield(analysis, 'snapshot_times') && ~isempty(analysis.snapshot_times)
        SafeConsoleIO.fprintf('Snapshots: %d (t = %.3f to %.3f s)\n', ...
            analysis.snapshots_stored, min(analysis.snapshot_times), max(analysis.snapshot_times));
    else
        SafeConsoleIO.fprintf('Snapshots: %d\n', analysis.snapshots_stored);
    end

    if isfield(analysis, 'peak_abs_omega') && isfinite(analysis.peak_abs_omega)
        SafeConsoleIO.fprintf('Peak |omega|: %.6e\n', analysis.peak_abs_omega);
    end

    if isfield(analysis, 'kinetic_energy') && ~isempty(analysis.kinetic_energy)
        SafeConsoleIO.fprintf('Kinetic energy: %.6e -> %.6e\n', analysis.kinetic_energy(1), analysis.kinetic_energy(end));
    end

    if isfield(analysis, 'enstrophy') && ~isempty(analysis.enstrophy)
        SafeConsoleIO.fprintf('Enstrophy: %.6e -> %.6e\n', analysis.enstrophy(1), analysis.enstrophy(end));
    end

    if isfield(analysis, 'peak_speed') && isfinite(analysis.peak_speed)
        SafeConsoleIO.fprintf('Peak speed: %.6e m/s\n', analysis.peak_speed);
    end

    if isfield(analysis, 'sustainability_index') && ~isempty(analysis.sustainability_index)
        SafeConsoleIO.fprintf('Sustainability index (final): %.4f\n', analysis.sustainability_index(end));
    end

    SafeConsoleIO.fprintf('==================================================\n\n');
end
