% ========================================================================
% MetricsExtractor - Feature Extraction and Result Packing (Static Class)
% ========================================================================
% Extracts scalar metrics from solver output and packages results for tables
% Used by: Analysis.m (all run modes), convergence agents
%
% Usage: MetricsExtractor.extract_features_from_analysis(analysis)
%
% Methods:
%   extract_features_from_analysis(analysis) - Extract scalar features from solver
%   pack_result(params, run_ok, analysis, feats, ...) - Pack into table row struct
%   result_schema() - Define canonical result struct schema
%
% Dependencies: HelperUtils (safe_get, take_scalar_metric)
%
% Created: 2026-02-05
% Part of: Tsunami Vortex Analysis Framework
% ========================================================================

classdef MetricsExtractor
    methods(Static)
        function feats = extract_features_from_analysis(analysis)
            feats = struct('peak_abs_omega', NaN, 'enstrophy', NaN, ...
                'peak_u', NaN, 'peak_v', NaN, 'peak_speed', NaN, ...
                'convergence_criterion', NaN);
            if ~isstruct(analysis); return; end
            
            % Prefer pre-computed values from solver
            feats.peak_abs_omega = HelperUtils.safe_get(analysis, "peak_abs_omega", NaN);
            feats.enstrophy = HelperUtils.safe_get(analysis, "enstrophy", NaN);
            feats.peak_u = HelperUtils.safe_get(analysis, "peak_u", NaN);
            feats.peak_v = HelperUtils.safe_get(analysis, "peak_v", NaN);
            feats.peak_speed = HelperUtils.safe_get(analysis, "peak_speed", NaN);
            
            feats.peak_abs_omega = HelperUtils.take_scalar_metric(feats.peak_abs_omega);
            feats.enstrophy = HelperUtils.take_scalar_metric(feats.enstrophy);
            feats.peak_u = HelperUtils.take_scalar_metric(feats.peak_u);
            feats.peak_v = HelperUtils.take_scalar_metric(feats.peak_v);
            feats.peak_speed = HelperUtils.take_scalar_metric(feats.peak_speed);
            
            fprintf('[EXTRACT_FEATURES] Retrieved: peak_omega=%.6e, enstrophy=%.6e\n', ...
                feats.peak_abs_omega, feats.enstrophy);
            
            % Fallback: compute from snapshots if needed
            if ~isfinite(feats.peak_abs_omega) && isfield(analysis,"omega_snaps") && ~isempty(analysis.omega_snaps)
                feats.peak_abs_omega = max(abs(analysis.omega_snaps(:,:,end)), [], 'all');
            end
            if ~isfinite(feats.enstrophy) && isfield(analysis,"omega_snaps") && ~isempty(analysis.omega_snaps)
                omega_last = analysis.omega_snaps(:,:,end);
                dx = HelperUtils.safe_get(analysis, "dx", NaN);
                dy = HelperUtils.safe_get(analysis, "dy", NaN);
                if isfinite(dx) && isfinite(dy)
                    feats.enstrophy = 0.5 * sum(omega_last(:).^2) * (dx * dy);
                end
            end
            if ~isfinite(feats.peak_u) && isfield(analysis,"u_snaps") && ~isempty(analysis.u_snaps)
                feats.peak_u = max(abs(analysis.u_snaps(:,:,end)), [], 'all');
            end
            if ~isfinite(feats.peak_v) && isfield(analysis,"v_snaps") && ~isempty(analysis.v_snaps)
                feats.peak_v = max(abs(analysis.v_snaps(:,:,end)), [], 'all');
            end
            if ~isfinite(feats.peak_speed) && isfinite(feats.peak_u) && isfinite(feats.peak_v)
                feats.peak_speed = hypot(feats.peak_u, feats.peak_v);
            end
        end
        
        function out = pack_result(params, run_ok, analysis, feats, wall_time_s, cpu_time_s, mem_used_MB, mem_max_MB)
            out = MetricsExtractor.result_schema();
            out.run_ok = run_ok;
            out.Nx = params.Nx; out.Ny = params.Ny;
            out.grid_points = params.Nx * params.Ny;
            out.nu = params.nu; out.dt = params.dt; out.Tfinal = params.Tfinal;
            out.ic_type = string(params.ic_type); out.delta = params.delta;
            if params.ic_type == "stretched_gaussian"
                out.ic_coeff = mat2str(params.ic_coeff);
            else
                out.ic_coeff = "N/A";
            end
            out.wall_time_s = wall_time_s; out.cpu_time_s = cpu_time_s;
            out.mem_used_MB = mem_used_MB; out.mem_max_possible_MB = mem_max_MB;
            out.method = HelperUtils.safe_get(analysis, "method", "");
            out.poisson_matrix_nnz = HelperUtils.safe_get(analysis, "poisson_matrix_nnz", NaN);
            out.poisson_matrix_n = HelperUtils.safe_get(analysis, "poisson_matrix_n", NaN);
            if ~isfinite(out.poisson_matrix_n)
                pm_size = HelperUtils.safe_get(analysis, "poisson_matrix_size", [NaN NaN]);
                if isnumeric(pm_size) && numel(pm_size) == 2
                    out.poisson_matrix_n = pm_size(1);
                end
            end
            out.rhs_calls = HelperUtils.safe_get(analysis, "rhs_calls", NaN);
            out.poisson_solves = HelperUtils.safe_get(analysis, "poisson_solves", NaN);
            out.setup_wall_time_s = HelperUtils.safe_get(analysis, "setup_wall_time_s", NaN);
            out.solve_wall_time_s = HelperUtils.safe_get(analysis, "solve_wall_time_s", NaN);
            out.error_id = HelperUtils.safe_get(analysis, "error_id", "");
            out.error_message = HelperUtils.safe_get(analysis, "error_message", "");
            out.peak_abs_omega = feats.peak_abs_omega;
            out.enstrophy = feats.enstrophy;
            out.peak_u = feats.peak_u; out.peak_v = feats.peak_v;
            out.peak_speed = feats.peak_speed;
            out.convergence_metric = feats.convergence_criterion;
        end
        
        function out = result_schema()
            out = struct('run_ok', false, 'method', "", ...
                'Nx', NaN, 'Ny', NaN, 'grid_points', NaN, ...
                'nu', NaN, 'dt', NaN, 'Tfinal', NaN, ...
                'ic_type', "", 'delta', NaN, 'ic_coeff', "", ...
                'wall_time_s', NaN, 'cpu_time_s', NaN, ...
                'mem_used_MB', NaN, 'mem_max_possible_MB', NaN, ...
                'poisson_matrix_nnz', NaN, 'poisson_matrix_n', NaN, ...
                'rhs_calls', NaN, 'poisson_solves', NaN, ...
                'setup_wall_time_s', NaN, 'solve_wall_time_s', NaN, ...
                'error_id', "", 'error_message', "", ...
                'peak_abs_omega', NaN, 'enstrophy', NaN, ...
                'peak_u', NaN, 'peak_v', NaN, 'peak_speed', NaN, ...
                'convergence_metric', NaN);
        end
    end
end