classdef AdaptiveConvergenceAgent < handle
    % ADAPTIVECONVERGENCEAGENT Intelligent convergence study controller
    %
    % This agent learns from preflight tests to intelligently navigate
    % convergence studies. It adapts mesh refinement and timestep selection
    % based on observed parameter behavior patterns.
    %
    % Key Features:
    %   - Preflight testing to gather training data
    %   - Pattern recognition for grid refinement behavior
    %   - Adaptive jump factor computation
    %   - Physical quantity tracking (vorticity, enstrophy, velocity)
    %   - Intelligent stopping criteria
    %   - Cost-optimized convergence path selection
    %
    % Usage:
    %   agent = AdaptiveConvergenceAgent(Parameters, settings);
    %   agent.run_preflight();
    %   [N_star, T, meta] = agent.execute_convergence_study();
    %
    % Dependencies:
    %   - prepare_simulation_params (from Analysis.m)
    %   - execute_simulation (from Analysis.m)
    %   - extract_features_from_analysis (from Analysis.m)
    
    properties
        Parameters          % Base simulation parameters
        settings            % Settings structure
        preflight_data      % Preflight test results
        learning_model      % Trained pattern recognition model
        convergence_log     % Iteration log
        cache               % Result cache
        cumulative_time     % Total elapsed time
        iteration_count     % Iteration counter
    end
    
    properties (Constant)
        MIN_PREFLIGHT_N = 16        % Minimum grid for preflight
        MAX_PREFLIGHT_N = 64        % Maximum grid for preflight
        PREFLIGHT_FACTOR = 2        % Grid doubling for preflight
        DEFAULT_TOLERANCE = 5e-2    % Default convergence tolerance
        MIN_JUMP_FACTOR = 1.2       % Minimum refinement jump
        MAX_JUMP_FACTOR = 4.0       % Maximum refinement jump
    end
    
    methods
        function obj = AdaptiveConvergenceAgent(Parameters, settings)
            % Constructor
            obj.Parameters = Parameters;
            obj.settings = settings;
            obj.preflight_data = struct([]);
            obj.learning_model = struct();
            obj.convergence_log = [];
            obj.cache = struct();
            obj.cumulative_time = 0;
            obj.iteration_count = 0;
            
            fprintf('\n\n');
            fprintf('           ADAPTIVE CONVERGENCE AGENT - INTELLIGENT MESH REFINEMENT            \n');
            fprintf('\n\n');
        end
        
        function run_preflight(obj)
            % Execute preflight test to gather training data
            %
            % Performs:
            %   1. Small grid simulations (N = 16, 32, 64)
            %   2. Measures convergence metrics
            %   3. Tracks physical quantities (omega, enstrophy, velocities)
            %   4. Computes refinement rate estimates
            %   5. Identifies quantities of interest for full study
            
            fprintf('\n\n');
            fprintf('                          PREFLIGHT TEST PHASE                                  \n');
            fprintf('\n\n');
            fprintf('[PREFLIGHT] Running small-scale tests to learn parameter behavior...\n\n');
            
            % Generate preflight test grids
            N_preflight = obj.MIN_PREFLIGHT_N;
            preflight_results = [];
            
            while N_preflight <= obj.MAX_PREFLIGHT_N
                fprintf('[PREFLIGHT] Testing N = %d...\n', N_preflight);
                
                % Run simulation at this resolution
                t_start = tic;
                params = prepare_simulation_params(obj.Parameters, N_preflight, []);
                [figs, analysis, run_ok, wall_time, ~] = execute_simulation(params);
                toc(t_start); % Wall time tracked via wall_time output
                
                if ~run_ok
                    fprintf('[PREFLIGHT ERROR] Simulation failed at N=%d. Skipping.\n', N_preflight);
                    N_preflight = N_preflight * obj.PREFLIGHT_FACTOR;
                    continue;
                end
                
                % Extract physical quantities
                feats = extract_features_from_analysis(analysis);
                
                % Store preflight result
                result = struct();
                result.N = N_preflight;
                result.dt = params.dt;
                result.wall_time = wall_time;
                result.peak_omega = feats.peak_abs_omega;
                result.enstrophy = feats.enstrophy;
                result.peak_u = feats.peak_u;
                result.peak_v = feats.peak_v;
                result.peak_speed = feats.peak_speed;
                result.run_ok = run_ok;
                result.figures = figs;
                
                preflight_results = [preflight_results; result]; %#ok<AGROW>
                
                % Save preflight figures
                if obj.settings.convergence.save_iteration_figures
                    obj.save_preflight_figures(figs, N_preflight);
                end
                
                fprintf('   N=%d complete: ω_max=%.4e, Enstrophy=%.4e, Time=%.2fs\n', ...
                    N_preflight, result.peak_omega, result.enstrophy, wall_time);
                
                % Close figures to free memory
                if obj.settings.figures.close_after_save
                    close(figs);
                end
                
                N_preflight = N_preflight * obj.PREFLIGHT_FACTOR;
            end
            
            obj.preflight_data = preflight_results;
            
            % Analyze preflight results
            obj.analyze_preflight_patterns();
            
            fprintf('\n[PREFLIGHT] Complete. %d test runs executed.\n\n', length(preflight_results));
        end
        
        function analyze_preflight_patterns(obj)
            % Analyze preflight data to build learning model
            %
            % Computes:
            %   - Convergence rate estimate (p from E ~ h^p)
            %   - Computational cost scaling (t ~ N^α)
            %   - Physical quantity sensitivity to refinement
            %   - Recommended starting N for full study
            %   - Adaptive jump factor initial estimate
            
            fprintf('[LEARNING] Analyzing preflight patterns...\n');
            
            if length(obj.preflight_data) < 2
                fprintf('[LEARNING WARNING] Insufficient preflight data for pattern analysis.\n');
                obj.learning_model = obj.get_default_learning_model();
                return;
            end
            
            % Extract arrays
            N_vals = [obj.preflight_data.N]';
            omega_vals = [obj.preflight_data.peak_omega]';
            enstrophy_vals = [obj.preflight_data.enstrophy]';
            time_vals = [obj.preflight_data.wall_time]';
            
            % Compute convergence rate for vorticity
            % Assume: ||omega_N1 - omega_N2|| / ||omega_N2|| ~ (N1/N2)^(-p)
            if length(N_vals) >= 2
                % Use consecutive pairs to estimate p
                p_estimates = [];
                for i = 1:length(N_vals)-1
                    N1 = N_vals(i);
                    N2 = N_vals(i+1);
                    omega1 = omega_vals(i);
                    omega2 = omega_vals(i+1);
                    
                    if omega2 > 0 && omega1 > 0
                        % Relative difference
                        rel_diff = abs(omega2 - omega1) / omega2;
                        if rel_diff > 1e-14
                            % p = log(rel_diff) / log(N1/N2)
                            p_est = -log(rel_diff) / log(N2/N1);
                            p_estimates = [p_estimates; p_est]; %#ok<AGROW>
                        end
                    end
                end
                
                if ~isempty(p_estimates)
                    p_convergence = median(p_estimates);
                else
                    p_convergence = 2.0;  % Default 2nd order
                end
            else
                p_convergence = 2.0;
            end
            
            % Compute computational cost scaling: time ~ N^alpha
            if length(N_vals) >= 2
                % Fit log(time) = alpha * log(N) + const
                X = [ones(size(N_vals)), log(N_vals)];
                y = log(time_vals);
                beta = X \ y;
                alpha_cost = beta(2);
            else
                alpha_cost = 2.0;  % Assume O(N^2)
            end
            
            % Determine quantities of interest (highest sensitivity)
            % Track which quantities change most significantly with refinement
            omega_range = range(omega_vals) / mean(omega_vals);
            enstrophy_range = range(enstrophy_vals) / mean(enstrophy_vals);
            
            if omega_range > enstrophy_range
                primary_qoi = 'vorticity';
            else
                primary_qoi = 'enstrophy';
            end
            
            % Compute recommended starting N for full study
            % Use largest preflight N as safe lower bound
            N_start_recommended = max(N_vals);
            
            % Compute initial adaptive jump factor
            % Based on convergence rate: faster convergence  larger jumps
            if p_convergence > 2.5
                initial_jump = 2.5;  % Fast convergence, aggressive jumps
            elseif p_convergence > 2.0
                initial_jump = 2.0;  % Standard 2nd order
            else
                initial_jump = 1.5;  % Slow convergence, conservative
            end
            
            % Store in learning model
            obj.learning_model.p_convergence = p_convergence;
            obj.learning_model.alpha_cost = alpha_cost;
            obj.learning_model.primary_qoi = primary_qoi;
            obj.learning_model.N_start_recommended = N_start_recommended;
            obj.learning_model.initial_jump_factor = initial_jump;
            obj.learning_model.omega_trend = omega_vals;
            obj.learning_model.enstrophy_trend = enstrophy_vals;
            obj.learning_model.N_trend = N_vals;
            
            fprintf('   Convergence rate (p): %.2f\n', p_convergence);
            fprintf('   Computational scaling (α): %.2f (t ~ N^%.2f)\n', alpha_cost, alpha_cost);
            fprintf('   Primary quantity of interest: %s\n', primary_qoi);
            fprintf('   Recommended starting N: %d\n', N_start_recommended);
            fprintf('   Initial jump factor: %.2f\n', initial_jump);
            fprintf('[LEARNING] Pattern analysis complete.\n\n');
        end
        
        function [N_star, T, meta] = execute_convergence_study(obj)
            % Execute intelligent convergence study using learned patterns
            %
            % Strategy:
            %   1. Start at N_start (from preflight)
            %   2. Use adaptive jump factors based on observed convergence
            %   3. Track cost-benefit: don't over-refine if minimal gain
            %   4. Detect plateaus (physical limit reached)
            %   5. Binary search final bracket for exact tolerance
            %
            % Returns:
            %   N_star - Converged grid resolution
            %   T      - Results table
            %   meta   - Metadata structure
            
            fprintf('\n\n');
            fprintf('                    ADAPTIVE CONVERGENCE EXECUTION                              \n');
            fprintf('\n\n');
            
            % Get tolerance from settings or use default
            if isfield(obj.settings.convergence, 'tolerance')
                tol = obj.settings.convergence.tolerance;
            else
                tol = obj.DEFAULT_TOLERANCE;
            end
            
            fprintf('[AGENT] Target tolerance: %.2e\n', tol);
            fprintf('[AGENT] Starting adaptive refinement...\n\n');
            
            % Initialize
            N_current = obj.learning_model.N_start_recommended;
            N_max = obj.resolve_max_search_N();
            N_eval_max = max(obj.MIN_PREFLIGHT_N, floor(N_max / 2));
            N_current = min(N_current, N_eval_max);
            jump_factor = obj.learning_model.initial_jump_factor;
            bracket_low = [];
            bracket_high = [];
            
            % Phase 1: Adaptive search for convergence bracket
            fprintf('[PHASE 1] Adaptive bracket search\n');
            fprintf('\n');
            
            max_iterations = 20;
            for iter = 1:max_iterations
                % Run at current N
                [metric, ~] = obj.evaluate_at_N(N_current);
                
                if ~isfinite(metric)
                    fprintf('[AGENT ERROR] Metric invalid at N=%d. Aborting.\n', N_current);
                    N_star = NaN;
                    T = [];
                    meta = struct('status', 'failed', 'reason', 'invalid_metric');
                    return;
                end
                
                fprintf('[Iter %2d] N=%4d | Metric=%.4e | Target=%.4e | ', ...
                    iter, N_current, metric, tol);
                
                % Check convergence
                if metric <= tol
                    fprintf(' CONVERGED\n');
                    bracket_high = N_current;
                    
                    % Try to find lower bound by jumping back
                    N_test_low = max(floor(N_current / 1.5), obj.MIN_PREFLIGHT_N);
                    if isempty(bracket_low)
                        [metric_low, ~] = obj.evaluate_at_N(N_test_low);
                        if metric_low > tol
                            bracket_low = N_test_low;
                            fprintf('[BRACKET] Found: [%d, %d]\n', bracket_low, bracket_high);
                        end
                    end
                    break;
                else
                    fprintf('Not converged\n');
                    bracket_low = N_current;

                    if N_current >= N_eval_max
                        fprintf('   Reached N_max=%d (effective N=%d for Richardson pairing) without meeting tolerance.\n', ...
                            N_max, N_eval_max);
                        break;
                    end
                    
                    % Adapt jump factor based on how far from tolerance
                    ratio = metric / tol;
                    if ratio > 10
                        jump_factor = min(obj.MAX_JUMP_FACTOR, jump_factor * 1.2);
                    elseif ratio > 3
                        % jump_factor unchanged — keep steady
                    else
                        jump_factor = max(obj.MIN_JUMP_FACTOR, jump_factor * 0.8);
                    end
                    
                    fprintf('   Jump factor: %.2f\n', jump_factor);
                    
                    % Compute next N
                    N_next = ceil(N_current * jump_factor);
                    N_next = min(N_next, N_eval_max);
                    N_current = N_next;
                end
            end
            
            % Phase 2: Binary search within bracket (if found)
            if ~isempty(bracket_low) && ~isempty(bracket_high)
                fprintf('\n[PHASE 2] Binary search refinement\n');
                fprintf('\n');
                fprintf('[BRACKET] Low: N=%d, High: N=%d\n', bracket_low, bracket_high);
                
                [N_star, binary_log] = obj.binary_search_bracket(bracket_low, bracket_high, tol);
                obj.convergence_log = [obj.convergence_log; binary_log];
            elseif ~isempty(bracket_high)
                N_star = bracket_high;
                fprintf('[AGENT] Converged at N=%d (no lower bound found)\n', N_star);
            else
                N_star = NaN;
                fprintf('[AGENT ERROR] Failed to bracket convergence.\n');
            end
            
            % Build results table
            T = obj.build_results_table();
            
            % Build metadata
            meta = struct();
            meta.mode = 'adaptive_convergence';
            meta.N_star = N_star;
            meta.tolerance = tol;
            meta.preflight_runs = length(obj.preflight_data);
            meta.total_iterations = obj.iteration_count;
            meta.total_time = obj.cumulative_time;
            meta.learning_model = obj.learning_model;
            meta.convergence_log = obj.convergence_log;
            
            fprintf('\n\n');
            fprintf('CONVERGENCE STUDY COMPLETE\n');
            fprintf('\n');
            fprintf('N* = %d\n', N_star);
            fprintf('Total iterations: %d\n', meta.total_iterations);
            fprintf('Total time: %.2f s\n', meta.total_time);
            fprintf('\n\n');
        end

        function N_max = resolve_max_search_N(obj)
            N_max = 2048;
            if isfield(obj.Parameters, 'convergence') && isstruct(obj.Parameters.convergence) && ...
                    isfield(obj.Parameters.convergence, 'study') && isstruct(obj.Parameters.convergence.study) && ...
                    isfield(obj.Parameters.convergence.study, 'N_max') && ...
                    isnumeric(obj.Parameters.convergence.study.N_max) && isfinite(obj.Parameters.convergence.study.N_max)
                N_max = double(obj.Parameters.convergence.study.N_max);
            elseif isfield(obj.Parameters, 'convergence_N_max') && isnumeric(obj.Parameters.convergence_N_max) && ...
                    isfinite(obj.Parameters.convergence_N_max)
                N_max = double(obj.Parameters.convergence_N_max);
            end
            N_max = max(obj.MIN_PREFLIGHT_N, round(N_max));
        end
        
        function [metric, run_data] = evaluate_at_N(obj, N)
            % Evaluate convergence metric at resolution N
            % Uses Richardson extrapolation: compare N vs 2N
            
            % Check cache first
            cache_key = sprintf('N%d', N);
            if isfield(obj.cache, cache_key)
                cached = obj.cache.(cache_key);
                metric = cached.metric;
                run_data = cached.data;
                fprintf('  [Cache hit] ');
                return;
            end
            
            t_start = tic;
            
            % Run at N
            params_N = prepare_simulation_params(obj.Parameters, N, []);
            [figs_N, analysis_N, run_ok_N, wall_time_N, ~] = execute_simulation(params_N); %#ok<ASGLU>
            
            if ~run_ok_N
                metric = NaN;
                run_data = struct('N', N, 'run_ok', false);
                return;
            end
            
            % Run at 2N for Richardson comparison
            N2 = 2 * N;
            params_2N = prepare_simulation_params(obj.Parameters, N2, []);
            [figs_2N, analysis_2N, run_ok_2N, wall_time_2N, ~] = execute_simulation(params_2N); %#ok<ASGLU>
            
            if ~run_ok_2N
                metric = NaN;
                run_data = struct('N', N, 'run_ok', false);
                return;
            end
            
            % Compute Richardson metric
            metric = obj.compute_richardson_metric(analysis_N, analysis_2N, N, N2);
            
            t_elapsed = toc(t_start);
            obj.cumulative_time = obj.cumulative_time + t_elapsed;
            obj.iteration_count = obj.iteration_count + 1;
            
            % Extract features
            feats_N = extract_features_from_analysis(analysis_N);
            feats_2N = extract_features_from_analysis(analysis_2N);
            
            % Pack run data
            run_data = struct();
            run_data.N = N;
            run_data.N2 = N2;
            run_data.metric = metric;
            run_data.wall_time = t_elapsed;
            run_data.features_N = feats_N;
            run_data.features_2N = feats_2N;
            run_data.run_ok = true;
            run_data.figures_N = figs_N;
            run_data.figures_2N = figs_2N;
            
            % Store in cache
            obj.cache.(cache_key) = struct('metric', metric, 'data', run_data);
            
            % Save figures if enabled
            if obj.settings.convergence.save_iteration_figures
                obj.save_iteration_figures(figs_N, N, 'adaptive_search');
                obj.save_iteration_figures(figs_2N, N2, 'richardson_comparison');
            end
            
            % Close figures to free memory
            if obj.settings.figures.close_after_save
                close(figs_N);
                close(figs_2N);
            end
            
            % Log iteration
            log_entry = struct();
            log_entry.iteration = obj.iteration_count;
            log_entry.phase = 'adaptive_search';
            log_entry.N = N;
            log_entry.metric = metric;
            log_entry.wall_time = t_elapsed;
            obj.convergence_log = [obj.convergence_log; log_entry];
        end
        
        function metric = compute_richardson_metric(obj, analysis1, analysis2, N1, N2)
            % Compute Richardson extrapolation metric
            % Uses L2 relative error of primary quantity of interest
            
            % Get primary QOI from learning model
            if isfield(obj.learning_model, 'primary_qoi')
                qoi = obj.learning_model.primary_qoi;
            else
                qoi = 'vorticity';  % Default
            end
            
            % Extract quantities
            if strcmpi(qoi, 'vorticity')
                q1 = NaN;
                q2 = NaN;
                if isstruct(analysis1) && isfield(analysis1, 'peak_abs_omega')
                    q1 = analysis1.peak_abs_omega;
                end
                if isstruct(analysis2) && isfield(analysis2, 'peak_abs_omega')
                    q2 = analysis2.peak_abs_omega;
                end
            elseif strcmpi(qoi, 'enstrophy')
                q1 = NaN;
                q2 = NaN;
                if isstruct(analysis1) && isfield(analysis1, 'enstrophy')
                    q1 = analysis1.enstrophy;
                end
                if isstruct(analysis2) && isfield(analysis2, 'enstrophy')
                    q2 = analysis2.enstrophy;
                end
            else
                % Fallback: use peak vorticity
                feats1 = extract_features_from_analysis(analysis1);
                feats2 = extract_features_from_analysis(analysis2);
                q1 = feats1.peak_abs_omega;
                q2 = feats2.peak_abs_omega;
            end
            q1 = take_scalar(q1);
            q2 = take_scalar(q2);
            
            % Compute relative error
            if ~isfinite(q1) || ~isfinite(q2) || abs(q2) <= eps
                metric = NaN;
                return;
            end
            
            rel_error = abs(q2 - q1) / abs(q2);
            
            % Apply Richardson extrapolation correction if convergence rate known
            if isfield(obj.learning_model, 'p_convergence')
                p = obj.learning_model.p_convergence;
                r = N2 / N1;  % Refinement ratio
                % Richardson estimate: E_richardson = E_measured / (r^p - 1)
                metric = rel_error / (r^p - 1);
            else
                metric = rel_error;
            end
        end
        
        function [N_star, log_entries] = binary_search_bracket(obj, N_low, N_high, tol)
            % Binary search within bracket [N_low, N_high]
            
            log_entries = [];
            
            while (N_high - N_low) > 1
                N_mid = floor((N_low + N_high) / 2);
                
                [metric, run_data] = obj.evaluate_at_N(N_mid);
                
                fprintf('[Binary] N=%d | Metric=%.4e | ', N_mid, metric);
                
                if ~isfinite(metric)
                    fprintf('Invalid metric, aborting binary search.\n');
                    N_star = N_high;
                    return;
                end
                
                if metric <= tol
                    fprintf('Converged\n');
                    N_high = N_mid;
                else
                    fprintf('Not converged\n');
                    N_low = N_mid;
                end
                
                % Log entry
                log_entry = struct();
                log_entry.iteration = obj.iteration_count;
                log_entry.phase = 'binary_search';
                log_entry.N = N_mid;
                log_entry.metric = metric;
                log_entry.wall_time = run_data.wall_time;
                log_entries = [log_entries; log_entry]; %#ok<AGROW>
            end
            
            N_star = N_high;
            fprintf('[Binary] Complete: N*=%d\n', N_star);
        end
        
        function T = build_results_table(obj)
            % Build table from convergence log
            if isempty(obj.convergence_log)
                T = table();
                return;
            end
            T = struct2table(obj.convergence_log);
        end
        
        function save_preflight_figures(obj, figs, N)
            % Save preflight figures to preflight subdirectory
            if isempty(figs)
                return;
            end
            
            fig_dir = obj.settings.convergence.preflight_figs_dir;
            if ~exist(fig_dir, 'dir')
                mkdir(fig_dir);
            end
            
            for i = 1:length(figs)
                if ~isgraphics(figs(i))
                    continue;
                end
                
                fig_name = get(figs(i), 'Name');
                if isempty(fig_name)
                    fig_name = sprintf('preflight_N%d_fig%d', N, i);
                else
                    fig_name = sprintf('preflight_N%d_%s', N, strrep(fig_name, ' ', '_'));
                end
                
                png_path = fullfile(fig_dir, [fig_name '.png']);
                ResultsPlotDispatcher.save_figure_bundle(figs(i), png_path, obj.settings);
            end
        end
        
        function save_iteration_figures(obj, figs, N, phase)
            % Save iteration figures with proper naming
            if isempty(figs)
                return;
            end
            
            % Use settings to get figure directory
            iter_dir = fullfile(obj.settings.convergence.study_dir, ...
                sprintf('iteration_%03d', obj.iteration_count), 'figures', phase);
            
            if ~exist(iter_dir, 'dir')
                mkdir(iter_dir);
            end
            
            for i = 1:length(figs)
                if ~isgraphics(figs(i))
                    continue;
                end
                
                fig_name = get(figs(i), 'Name');
                if isempty(fig_name)
                    fig_name = sprintf('%s_N%d_fig%d', phase, N, i);
                else
                    fig_name = sprintf('%s_N%d_%s', phase, N, strrep(fig_name, ' ', '_'));
                end
                
                png_path = fullfile(iter_dir, [fig_name '.png']);
                ResultsPlotDispatcher.save_figure_bundle(figs(i), png_path, obj.settings);
            end
        end
    end
    
    methods (Static)
        function model = get_default_learning_model()
            % Default learning model when preflight data insufficient
            model = struct();
            model.p_convergence = 2.0;
            model.alpha_cost = 2.0;
            model.primary_qoi = 'vorticity';
            model.N_start_recommended = 32;
            model.initial_jump_factor = 2.0;
        end
    end
end

function params = prepare_simulation_params(base_params, N, ~)
% prepare_simulation_params Build a resolution-specific parameter struct.

    params = base_params;
    params.Nx = max(8, round(double(N)));
    params.Ny = max(8, round(double(N)));

    if isfield(base_params, 'Lx') && isfield(base_params, 'Ly')
        params.dx = base_params.Lx / params.Nx;
        params.dy = base_params.Ly / params.Ny;
        params.delta = min(params.dx, params.dy);
    end

    if isfield(base_params, 'Nx') && isnumeric(base_params.Nx) && base_params.Nx > 0 && ...
            isfield(base_params, 'dt') && isnumeric(base_params.dt) && isfinite(base_params.dt) && base_params.dt > 0
        refine_ratio = params.Nx / max(double(base_params.Nx), 1);
        params.dt = max(base_params.dt / max(refine_ratio, 1), 1e-6);
    end

    if isfield(base_params, 'Tfinal') && isnumeric(base_params.Tfinal) && isfinite(base_params.Tfinal)
        params.t_final = base_params.Tfinal;
    end

    n_snap = 3;
    if isfield(base_params, 'num_snapshots') && isnumeric(base_params.num_snapshots) && isfinite(base_params.num_snapshots)
        n_snap = max(2, round(double(base_params.num_snapshots)));
    end
    params.num_snapshots = n_snap;
    if isfield(params, 'Tfinal') && isnumeric(params.Tfinal) && isfinite(params.Tfinal) && params.Tfinal > 0
        params.snap_times = linspace(0, params.Tfinal, n_snap);
    end
end

function [figs, analysis, run_ok, wall_time, err_details] = execute_simulation(params)
% execute_simulation Execute one simulation for adaptive convergence agent.

    figs = [];
    analysis = struct();
    run_ok = false;
    err_details = '';
    t_start = tic;
    try
        [fig_handle, analysis] = run_simulation_with_method(params);
        wall_time = toc(t_start);
        run_ok = true;
        if ~isempty(fig_handle)
            figs = fig_handle;
        end
    catch ME
        wall_time = toc(t_start);
        err_details = sprintf('%s: %s', char(string(ME.identifier)), ME.message);
    end
end

function feats = extract_features_from_analysis(analysis)
% extract_features_from_analysis Agent-local feature extraction wrapper.

    if exist('MetricsExtractor', 'class') == 8
        feats = MetricsExtractor.extract_features_from_analysis(analysis);
    else
        feats = struct();
    end

    if ~isstruct(feats)
        feats = struct();
    end
    if ~isfield(feats, 'peak_abs_omega')
        feats.peak_abs_omega = NaN;
    end
    if ~isfield(feats, 'enstrophy')
        feats.enstrophy = NaN;
    end
    if ~isfield(feats, 'peak_u')
        feats.peak_u = NaN;
    end
    if ~isfield(feats, 'peak_v')
        feats.peak_v = NaN;
    end
    if ~isfield(feats, 'peak_speed')
        feats.peak_speed = NaN;
    end

    feats.peak_abs_omega = take_scalar(feats.peak_abs_omega);
    feats.enstrophy = take_scalar(feats.enstrophy);
    feats.peak_u = take_scalar(feats.peak_u);
    feats.peak_v = take_scalar(feats.peak_v);
    feats.peak_speed = take_scalar(feats.peak_speed);
end

function value = take_scalar(value)
    if isnumeric(value) && ~isscalar(value)
        finite_vals = value(isfinite(value));
        if isempty(finite_vals)
            value = NaN;
        else
            value = finite_vals(end);
        end
    end
    if ~isnumeric(value) || ~isfinite(value)
        value = NaN;
    end
end
