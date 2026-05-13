function [Results, paths] = mode_evolution(Run_Config, Parameters, Settings)
    % mode_evolution - METHOD-AGNOSTIC Evolution Mode
    %
    % Purpose:
    %   Orchestrates a single time evolution simulation
    %   Works with ANY numerical method (FD, Spectral, FV, SWE)
    %   Method selection handled internally via switch/case
    %
    % This is the SINGLE SOURCE OF TRUTH for Evolution mode logic
    % NO method-specific evolution files (no FD_Evolution, etc.)
    %
    % Inputs:
    %   Run_Config - .method, .mode, .ic_type, .run_id
    %   Parameters - physics + numerics
    %   Settings - IO, monitoring, logging
    %
    % Outputs:
    %   Results - simulation results and metrics
    %   paths - directory structure
    %
    % Usage:
    %   [Results, paths] = mode_evolution(Run_Config, Parameters, Settings);

    % ===== VALIDATION =====
    [ok, issues] = validate_evolution(Run_Config, Parameters);
    if ~ok
        error('Evolution mode validation failed: %s', strjoin(issues, '; '));
    end

    Parameters = sanitize_vorticity_only_legacy_fields(Parameters, 'evolution runtime', 'mode_evolution');
    Parameters = normalize_snapshot_schedule_parameters(Parameters);

    % ===== SETUP =====
    % Generate run ID if not provided
    if ~isfield(Run_Config, 'run_id') || isempty(Run_Config.run_id)
        Run_Config.run_id = RunIDGenerator.generate(Run_Config, Parameters);
    end

    % Get directory paths
    output_root = resolve_output_root(Settings);
    if use_preinitialized_artifact_root(Settings)
        paths = PathBuilder.get_existing_root_paths(output_root, Run_Config.method, Run_Config.mode);
    else
        paths = PathBuilder.get_run_paths(Run_Config.method, Run_Config.mode, Run_Config.run_id, output_root);
    end
    PathBuilder.ensure_directories(paths);
    Run_Config.storage_id = paths.storage_id;

    % Save configuration (filter out graphics objects to avoid warnings)
    Run_Config_clean = filter_graphics_objects(Run_Config);
    Parameters_clean = filter_graphics_objects(Parameters);
    Settings_clean = filter_graphics_objects(Settings);

    config_path = fullfile(paths.config, 'Config.mat');
    config_payload = struct( ...
        'Run_Config_clean', Run_Config_clean, ...
        'Parameters_clean', Parameters_clean, ...
        'Settings_clean', Settings_clean);
    safe_save_mat(config_path, config_payload);

    % ===== METHOD DISPATCH =====
    % Resolve method callbacks from the shared method contract.
    callbacks = resolve_method_callbacks(Run_Config.method);

    % ===== BUILD CONTEXT =====
    ctx = build_mode_context(Parameters, Settings);
    ctx.mode = 'evolution';

    % ===== MONITORING =====
    MonitorInterface.start(Run_Config, Settings);

    % ===== SIMULATION LOOP =====
    tic;
    run_timer = tic;
    progress_callback = resolve_progress_callback(Settings);

    % Initialize method-specific state
    cfg = prepare_cfg(Run_Config, Parameters);
    State = callbacks.init(cfg, ctx);

    % Time integration parameters
    Tfinal = Parameters.Tfinal;
    dt = Parameters.dt;
    snap_times = Parameters.snap_times;
    Nsnap = length(snap_times);

    % Storage for snapshots
    snapshot_fields = resolve_snapshot_field_names(State, cfg.Ny, cfg.Nx, Parameters);
    snapshots = allocate_snapshot_store(snapshot_fields, cfg.Ny, cfg.Nx, Nsnap, snap_times, Parameters);
    snapshots = store_snapshot_fields(snapshots, snapshot_fields, State, 1);
    snap_index = 2;
    if Nsnap >= 2
        next_snap_t = snap_times(snap_index);
    else
        next_snap_t = inf;
    end

    adaptive_step_cfg = resolve_phase1_adaptive_timestep_config(Parameters);
    phase1_mesh_sweep_context = is_phase1_local_mesh_sweep(Parameters);
    last_adaptive_timestep = empty_adaptive_timestep_metadata();
    null_field_guard = resolve_null_field_collapse_guard(Parameters, State, snapshot_fields);

    % Initial diagnostics
    Metrics = callbacks.diagnostics(State, cfg, ctx);
    if adaptive_step_cfg.enabled
        last_adaptive_timestep = compute_adaptive_timestep_components(Metrics, cfg, adaptive_step_cfg);
        initial_dt = clip_adaptive_timestep(last_adaptive_timestep, Tfinal - State.t, dt);
        Nt = estimate_adaptive_total_iterations(0, State.t, Tfinal, initial_dt, max(1, round(Tfinal / max(dt, eps))));
    else
        initial_dt = dt;
        Nt = round(Tfinal / dt);
    end

    % Progress reporting
    progress_policy = resolve_progress_policy(Settings, Nt);

    % Create progress bar if not using UI callback
    use_progress_bar = isempty(progress_callback);
    if use_progress_bar
        pb = ProgressBar(Nt, 'Prefix', sprintf('[%s Evolution]', Run_Config.method), ...
            'BarWidth', 50, 'UpdateInterval', progress_policy.min_emit_seconds);
    end

    % Time history for diagnostics
    initial_capacity = max(Nt + 1, 2);
    time_vec = zeros(1, initial_capacity);
    time_vec(1) = 0.0;
    kinetic_energy = zeros(1, initial_capacity);
    enstrophy = zeros(1, initial_capacity);
    max_vorticity = zeros(1, initial_capacity);
    mass_total = nan(1, initial_capacity);
    min_depth = nan(1, initial_capacity);
    dt_history = nan(1, max(initial_capacity - 1, 1));

    kinetic_energy(1) = Metrics.kinetic_energy;
    enstrophy(1) = Metrics.enstrophy;
    max_vorticity(1) = Metrics.max_vorticity;
    if isfield(Metrics, 'mass_total'), mass_total(1) = Metrics.mass_total; end
    if isfield(Metrics, 'min_depth'), min_depth(1) = Metrics.min_depth; end
    progress_callback = emit_progress_payload(progress_callback, Run_Config, ...
        0, Nt, State.t, Metrics, toc(run_timer), NaN, initial_dt, resolve_time_step_mode_label(adaptive_step_cfg.enabled));
    last_progress_emit_tic = tic;
    instability_meta = empty_evolution_instability_metadata();

    % Main time integration loop
    n = 0;
    if adaptive_step_cfg.enabled
        time_tolerance = max(1e-12, 10 * eps(max(1, Tfinal)));
        while State.t < Tfinal - time_tolerance
            n = n + 1;
            if n + 1 > numel(time_vec)
                growth_size = max(128, ceil(0.5 * numel(time_vec)));
                [time_vec, kinetic_energy, enstrophy, max_vorticity, mass_total, min_depth] = ...
                    grow_history_capacity(time_vec, kinetic_energy, enstrophy, max_vorticity, mass_total, min_depth, growth_size);
                dt_history = [dt_history, nan(1, growth_size)]; %#ok<AGROW>
            end

            step_meta = compute_adaptive_timestep_components(Metrics, cfg, adaptive_step_cfg);
            dt_step = clip_adaptive_timestep(step_meta, Tfinal - State.t, dt);
            step_meta = stamp_adaptive_timestep_step_usage(step_meta, adaptive_step_cfg, dt_step);
            last_adaptive_timestep = step_meta;
            cfg.dt = dt_step;
            Nt = estimate_adaptive_total_iterations(n - 1, State.t, Tfinal, dt_step, Nt);
            if use_progress_bar
                pb.total = Nt;
            end
            dt_history(n) = dt_step;

            % Advance state by one time step
            stable_state_before_step = State;
            stable_metrics_before_step = Metrics;
            stable_step_meta_before_step = last_adaptive_timestep;
            State = callbacks.step(State, cfg, ctx);

            Metrics = callbacks.diagnostics(State, cfg, ctx);
            [is_unstable, instability_meta] = detect_evolution_instability(State, Metrics, snapshot_fields, dt_step, null_field_guard);
            if is_unstable
                instability_meta.last_stable_time = stable_state_before_step.t;
                instability_meta.last_stable_step = stable_state_before_step.step;
                instability_meta.completed_stable_steps = max(0, n - 1);
                State = stable_state_before_step;
                Metrics = stable_metrics_before_step;
                last_adaptive_timestep = stable_step_meta_before_step;
                n = max(0, n - 1);
                break;
            end

            % Store diagnostics
            time_vec(n + 1) = State.t;
            kinetic_energy(n + 1) = Metrics.kinetic_energy;
            enstrophy(n + 1) = Metrics.enstrophy;
            max_vorticity(n + 1) = Metrics.max_vorticity;
            if isfield(Metrics, 'mass_total'), mass_total(n + 1) = Metrics.mass_total; end
            if isfield(Metrics, 'min_depth'), min_depth(n + 1) = Metrics.min_depth; end

            % Snapshot if needed
            while snap_index <= Nsnap && State.t >= next_snap_t - 1e-12
                snapshots = store_snapshot_fields(snapshots, snapshot_fields, State, snap_index);
                snap_index = snap_index + 1;
                if snap_index <= Nsnap
                    next_snap_t = snap_times(snap_index);
                end
            end

            % Progress reporting (cadence-controlled to keep solver loop lightweight)
            should_emit_progress = (n == 1) || (n == Nt) || ...
                (mod(n, progress_policy.iter_stride) == 0) || ...
                (toc(last_progress_emit_tic) >= progress_policy.min_emit_seconds);
            if should_emit_progress
                if use_progress_bar
                    pb.update(n, 'Message', sprintf('t=%.3f, |omega|=%.3e', State.t, Metrics.max_vorticity));
                end

                progress_callback = emit_progress_payload(progress_callback, Run_Config, ...
                    n, Nt, State.t, Metrics, toc(run_timer), NaN, dt_step, 'adaptive_stability');
                last_progress_emit_tic = tic;
            end
        end
    else
        for step_index = 1:Nt
            dt_step = cfg.dt;
            dt_history(step_index) = dt_step;

            % Advance state by one time step
            stable_state_before_step = State;
            stable_metrics_before_step = Metrics;
            State = callbacks.step(State, cfg, ctx);

            Metrics = callbacks.diagnostics(State, cfg, ctx);
            [is_unstable, instability_meta] = detect_evolution_instability(State, Metrics, snapshot_fields, dt_step, null_field_guard);
            if is_unstable
                instability_meta.last_stable_time = stable_state_before_step.t;
                instability_meta.last_stable_step = stable_state_before_step.step;
                instability_meta.completed_stable_steps = max(0, step_index - 1);
                State = stable_state_before_step;
                Metrics = stable_metrics_before_step;
                break;
            end

            % Store diagnostics
            n = step_index;
            time_vec(n + 1) = State.t;
            kinetic_energy(n + 1) = Metrics.kinetic_energy;
            enstrophy(n + 1) = Metrics.enstrophy;
            max_vorticity(n + 1) = Metrics.max_vorticity;
            if isfield(Metrics, 'mass_total'), mass_total(n + 1) = Metrics.mass_total; end
            if isfield(Metrics, 'min_depth'), min_depth(n + 1) = Metrics.min_depth; end

            % Snapshot if needed
            while snap_index <= Nsnap && State.t >= next_snap_t - 1e-12
                snapshots = store_snapshot_fields(snapshots, snapshot_fields, State, snap_index);
                snap_index = snap_index + 1;
                if snap_index <= Nsnap
                    next_snap_t = snap_times(snap_index);
                end
            end

            % Progress reporting (cadence-controlled to keep solver loop lightweight)
            should_emit_progress = (step_index == 1) || (step_index == Nt) || ...
                (mod(step_index, progress_policy.iter_stride) == 0) || ...
                (toc(last_progress_emit_tic) >= progress_policy.min_emit_seconds);
            if should_emit_progress
                if use_progress_bar
                    pb.update(step_index, 'Message', sprintf('t=%.3f, |omega|=%.3e', State.t, Metrics.max_vorticity));
                end

                progress_callback = emit_progress_payload(progress_callback, Run_Config, ...
                    step_index, Nt, State.t, Metrics, toc(run_timer), NaN, dt_step, 'fixed');
                last_progress_emit_tic = tic;
            end
        end
    end

    time_vec = time_vec(1:n + 1);
    kinetic_energy = kinetic_energy(1:n + 1);
    enstrophy = enstrophy(1:n + 1);
    max_vorticity = max_vorticity(1:n + 1);
    mass_total = mass_total(1:n + 1);
    min_depth = min_depth(1:n + 1);
    dt_history = dt_history(1:max(n, 0));
    if snap_index <= Nsnap && ~instability_meta.detected
        snapshots = backfill_remaining_snapshots(snapshots, snapshot_fields, State, snap_index, Nsnap);
    end
    snapshots = trim_snapshot_store(snapshots, snapshot_fields, max(1, snap_index - 1));

    % Finish progress bar if used
    if use_progress_bar
        if instability_meta.detected
            pb.finish('Message', sprintf('Instability detected at t=%.3f; stopped at last stable t=%.3f', ...
                instability_meta.failed_time, State.t));
        else
            pb.finish('Message', sprintf('Complete! Final t=%.3f', State.t));
        end
    end

    wall_time = toc;

    % ===== RESULTS COLLECTION =====
    Results = struct();
    Results.run_id = Run_Config.run_id;
    Results.wall_time = wall_time;
    Results.final_time = State.t;
    Results.requested_final_time = Tfinal;
    Results.total_steps = n;
    Results.method = Run_Config.method;
    Results.storage_id = paths.storage_id;
    Results.max_omega = max_vorticity(end);
    Results.final_energy = kinetic_energy(end);
    Results.final_enstrophy = enstrophy(end);
    Results.time_step_mode = resolve_time_step_mode_label(adaptive_step_cfg.enabled);
    Results.completed_full_duration = ~instability_meta.detected && State.t >= Tfinal - max(1e-12, 10 * eps(max(1, Tfinal)));
    if any(isfinite(mass_total))
        Results.final_mass = mass_total(find(isfinite(mass_total), 1, 'last'));
        Results.min_depth = min(min_depth(isfinite(min_depth)));
    end
    Results.adaptive_dt_used = adaptive_step_cfg.enabled;
    Results.instability_detected = logical(instability_meta.detected);
    Results.instability_reason = instability_meta.reason;
    Results.instability_failed_time = instability_meta.failed_time;
    Results.instability_failed_step = instability_meta.failed_step;
    Results.instability_last_stable_time = instability_meta.last_stable_time;
    Results.instability_last_stable_step = instability_meta.last_stable_step;
    Results.instability_failed_dt = instability_meta.failed_dt;
    Results.instability_failed_omega_max = instability_meta.failed_omega_max;
    Results.instability_failed_psi_max = instability_meta.failed_psi_max;

    if phase1_mesh_sweep_context
        [dt_initial_out, dt_final_out, dt_min_out, dt_max_out] = ...
            resolve_phase1_timestep_summary(adaptive_step_cfg.enabled, dt_history, dt);
        stability_meta = resolve_phase1_terminal_stability_metadata( ...
            adaptive_step_cfg.enabled, last_adaptive_timestep, Metrics, cfg, adaptive_step_cfg, dt);
        Results.dt_initial = dt_initial_out;
        Results.dt_final = dt_final_out;
        Results.dt_min = dt_min_out;
        Results.dt_max = dt_max_out;
        Results.delta_terminal = stability_meta.delta;
        Results.dt_adv_terminal = stability_meta.dt_adv;
        Results.dt_diff_terminal = stability_meta.dt_diff;
        Results.dt_final_terminal = stability_meta.dt_final;
        Results.dt_step_terminal = stability_meta.dt_step;
        Results.cfl_adv_terminal = stability_meta.cfl_adv;
        Results.cfl_diff_terminal = stability_meta.cfl_diff;
    elseif adaptive_step_cfg.enabled
        Results.dt_initial = first_finite_value(dt_history, dt);
        Results.dt_final = last_finite_value(dt_history, dt);
        Results.dt_min = min(dt_history(isfinite(dt_history)));
        Results.dt_max = max(dt_history(isfinite(dt_history)));
        Results.delta_terminal = last_adaptive_timestep.delta;
        Results.dt_adv_terminal = last_adaptive_timestep.dt_adv;
        Results.dt_diff_terminal = last_adaptive_timestep.dt_diff;
        Results.dt_final_terminal = last_adaptive_timestep.dt_final;
        Results.dt_step_terminal = last_adaptive_timestep.dt_step;
        Results.cfl_adv_terminal = last_adaptive_timestep.cfl_adv;
        Results.cfl_diff_terminal = last_adaptive_timestep.cfl_diff;
    end

    % Analysis structure (for compatibility with plotting)
    analysis = struct();
    analysis = append_snapshot_struct_to_analysis(analysis, snapshots, snapshot_fields);
    analysis.time_vec = time_vec;
    analysis.snapshot_times_requested = snapshots.times_requested(:);
    analysis.snapshot_times_actual = snapshots.times_actual(:);
    analysis.snapshot_times = snapshots.times_requested(:);
    analysis.snapshots_stored = numel(snapshots.times_requested);
    analysis.kinetic_energy = kinetic_energy;
    analysis.enstrophy = enstrophy;
    analysis.peak_vorticity = max(max_vorticity);
    analysis.method = sprintf('%s (method-agnostic)', Run_Config.method);
    analysis.storage_id = paths.storage_id;
    if any(isfinite(mass_total))
        analysis.mass_total = mass_total;
        analysis.min_depth = min_depth;
    end
    if isfield(Metrics, 'peak_speed')
        analysis.peak_speed = Metrics.peak_speed;
    end
    if isfield(Metrics, 'sustainability_index')
        analysis.sustainability_index = Metrics.sustainability_index;
    end
    analysis.completed_full_duration = logical(Results.completed_full_duration);
    analysis.requested_final_time = double(Tfinal);
    analysis.instability_detected = logical(instability_meta.detected);
    analysis.instability_reason = instability_meta.reason;
    analysis.instability_failed_time = instability_meta.failed_time;
    analysis.instability_failed_step = instability_meta.failed_step;
    analysis.instability_last_stable_time = instability_meta.last_stable_time;
    analysis.instability_last_stable_step = instability_meta.last_stable_step;
    analysis.instability_failed_dt = instability_meta.failed_dt;
    analysis.instability_failed_omega_max = instability_meta.failed_omega_max;
    analysis.instability_failed_psi_max = instability_meta.failed_psi_max;
    analysis.instability = instability_meta;

    if ~isfield(callbacks, 'finalize_analysis') || isempty(callbacks.finalize_analysis)
        error('Evolution:MissingFinalizeAnalysisCallback', ...
            'Method %s must expose callbacks.finalize_analysis for evolution results.', Run_Config.method);
    end
    analysis = callbacks.finalize_analysis(analysis, State, cfg, Parameters, ctx);
    analysis.time_step_mode = resolve_time_step_mode_label(adaptive_step_cfg.enabled);
    analysis.adaptive_dt_used = adaptive_step_cfg.enabled;
    if phase1_mesh_sweep_context
        [dt_initial_out, dt_final_out, dt_min_out, dt_max_out] = ...
            resolve_phase1_timestep_summary(adaptive_step_cfg.enabled, dt_history, dt);
        stability_meta = resolve_phase1_terminal_stability_metadata( ...
            adaptive_step_cfg.enabled, last_adaptive_timestep, Metrics, cfg, adaptive_step_cfg, dt);
        analysis.dt = dt;
        if adaptive_step_cfg.enabled
            analysis.dt_history = dt_history(:);
        end
        analysis.adaptive_timestep = struct( ...
            'enabled', logical(adaptive_step_cfg.enabled), ...
            'time_step_mode', resolve_time_step_mode_label(adaptive_step_cfg.enabled), ...
            'C_adv', adaptive_step_cfg.C_adv, ...
            'C_diff', adaptive_step_cfg.C_diff, ...
            'dt_initial', dt_initial_out, ...
            'dt_final', dt_final_out, ...
            'dt_min', dt_min_out, ...
            'dt_max', dt_max_out, ...
            'delta_terminal', stability_meta.delta, ...
            'dt_adv_terminal', stability_meta.dt_adv, ...
            'dt_diff_terminal', stability_meta.dt_diff, ...
            'dt_final_terminal', stability_meta.dt_final, ...
            'dt_step_terminal', stability_meta.dt_step, ...
            'cfl_adv_terminal', stability_meta.cfl_adv, ...
            'cfl_diff_terminal', stability_meta.cfl_diff, ...
            'steps', double(n));
    elseif adaptive_step_cfg.enabled
        analysis.dt = dt;
        analysis.dt_history = dt_history(:);
        analysis.adaptive_timestep = struct( ...
            'enabled', true, ...
            'time_step_mode', 'adaptive_stability', ...
            'C_adv', adaptive_step_cfg.C_adv, ...
            'C_diff', adaptive_step_cfg.C_diff, ...
            'dt_initial', first_finite_value(dt_history, dt), ...
            'dt_final', last_finite_value(dt_history, dt), ...
            'dt_min', min(dt_history(isfinite(dt_history))), ...
            'dt_max', max(dt_history(isfinite(dt_history))), ...
            'delta_terminal', last_adaptive_timestep.delta, ...
            'dt_adv_terminal', last_adaptive_timestep.dt_adv, ...
            'dt_diff_terminal', last_adaptive_timestep.dt_diff, ...
            'dt_final_terminal', last_adaptive_timestep.dt_final, ...
            'dt_step_terminal', last_adaptive_timestep.dt_step, ...
            'cfl_adv_terminal', last_adaptive_timestep.cfl_adv, ...
            'cfl_diff_terminal', last_adaptive_timestep.cfl_diff, ...
            'steps', double(n));
    end

    if should_return_analysis(Settings)
        Results.analysis = analysis;
    end

    % ===== SAVE OUTPUTS =====
    if Settings.save_data
        data_path = fullfile(paths.data, 'results.mat');
        State = sanitize_state_for_save(State);
        data_payload = build_results_data_payload(analysis, Results, State);
        safe_save_mat(data_path, data_payload, '-v7.3');
        Results.data_path = data_path;
    end

    defer_heavy_exports = defer_heavy_result_artifacts_requested(Settings);
    if Settings.save_figures && ~defer_heavy_exports
        fig_meta = generate_evolution_figures(analysis, Parameters, Run_Config, paths, Settings);
        Results.figure_layout_rows = fig_meta.nrows;
        Results.figure_layout_cols = fig_meta.ncols;
        Results.figure_snapshot_count = fig_meta.snapshot_count;
    elseif Settings.save_figures
        Results.figure_layout_rows = NaN;
        Results.figure_layout_cols = NaN;
        Results.figure_snapshot_count = size(analysis.omega_snaps, 3);
        Results.deferred_worker_exports = struct( ...
            'figures_deferred', true, ...
            'reason', 'host_owned_publication');
    end

    if Settings.append_to_master
        MasterRunsTable.append_run(Run_Config.run_id, Run_Config, Parameters, Results);
    end

    % ===== MONITORING COMPLETE =====
    Run_Summary = struct();
    Run_Summary.total_time = wall_time;
    Run_Summary.status = 'completed';
    MonitorInterface.stop(Run_Summary);
end

%% ===== LOCAL FUNCTIONS =====

function [ok, issues] = validate_evolution(Run_Config, Parameters)
    % Validate Evolution mode configuration
    ok = true;
    issues = {};

    % Check required fields
    if ~isfield(Run_Config, 'method')
        ok = false;
        issues{end+1} = 'Run_Config.method is required';
    end

    if ~isfield(Parameters, 'Tfinal') || Parameters.Tfinal <= 0
        ok = false;
        issues{end+1} = 'Parameters.Tfinal must be > 0';
    end

    if ~isfield(Parameters, 'dt') || Parameters.dt <= 0
        ok = false;
        issues{end+1} = 'Parameters.dt must be > 0';
    end

    if ~isfield(Parameters, 'Nx') || ~isfield(Parameters, 'Ny')
        ok = false;
        issues{end+1} = 'Parameters.Nx and Parameters.Ny are required';
    end
end

function state_out = sanitize_state_for_save(state_in)
    % Keep only serializable state fields to avoid decomposition/save warnings.
    state_out = struct();
    if ~isstruct(state_in)
        return;
    end
    keep = {'omega', 'psi', 'eta', 'h', 'hu', 'hv', 'u', 'v', 't', 'dt'};
    for i = 1:numel(keep)
        key = keep{i};
        if isfield(state_in, key)
            state_out.(key) = state_in.(key);
        end
    end
end

function data_payload = build_results_data_payload(analysis, Results, State)
    % Persist the snapshot cube only once. Saved-package readers already
    % load the top-level analysis payload when Results.analysis is absent.
    results_for_save = Results;
    if isfield(results_for_save, 'analysis')
        results_for_save = rmfield(results_for_save, 'analysis');
    end
    data_payload = struct('analysis', analysis, 'Results', results_for_save, 'State', State);
end

function callbacks = resolve_method_callbacks(method_name)
    % Resolve method callbacks from method name.
    switch lower(method_name)
        case 'fd'
            callbacks = FiniteDifferenceMethod('callbacks');
        case {'spectral', 'fft'}
            callbacks = SpectralMethod('callbacks');
        case {'fv', 'finitevolume', 'finite volume'}
            callbacks = FiniteVolumeMethod('callbacks');
        case {'swe', 'shallowwater', 'shallow water'}
            callbacks = ShallowWaterMethod('callbacks');
        otherwise
            error('Unknown method: %s. Valid: FD, Spectral, FV, SWE', method_name);
    end
end

function snapshot_fields = resolve_snapshot_field_names(State, Ny, Nx, Parameters)
    candidates = {'omega', 'psi', 'eta', 'h', 'hu', 'hv', 'u', 'v'};
    snapshot_fields = {};
    for i = 1:numel(candidates)
        key = candidates{i};
        if ~isfield(State, key) || isempty(State.(key))
            continue;
        end
        value = State.(key);
        if isnumeric(value) && isequal(size(value), [Ny, Nx])
            snapshot_fields{end + 1} = key; %#ok<AGROW>
        end
    end
    snapshot_fields = filter_snapshot_fields_for_storage(snapshot_fields, Parameters);
    if isempty(snapshot_fields)
        error('Evolution:NoSnapshotFields', ...
            'State must expose at least one Ny-by-Nx numeric snapshot field.');
    end
end

function snapshots = allocate_snapshot_store(snapshot_fields, Ny, Nx, Nsnap, snap_times, Parameters)
    snapshots = struct();
    snapshot_precision = resolve_snapshot_storage_precision(Parameters);
    for i = 1:numel(snapshot_fields)
        key = snapshot_fields{i};
        snapshots.(key) = zeros(Ny, Nx, Nsnap, snapshot_precision);
    end
    snapshots.times_requested = reshape(double(snap_times), 1, []);
    snapshots.times_actual = nan(1, Nsnap);
end

function snapshots = store_snapshot_fields(snapshots, snapshot_fields, State, snap_index)
    for i = 1:numel(snapshot_fields)
        key = snapshot_fields{i};
        if isfield(State, key)
            snapshots.(key)(:, :, snap_index) = State.(key);
        end
    end
    snapshots.times_actual(snap_index) = double(State.t);
end

function snapshots = backfill_remaining_snapshots(snapshots, snapshot_fields, State, start_index, Nsnap)
    for idx = start_index:Nsnap
        snapshots = store_snapshot_fields(snapshots, snapshot_fields, State, idx);
    end
end

function snapshots = trim_snapshot_store(snapshots, snapshot_fields, keep_count)
    keep_count = max(1, round(double(keep_count)));
    keep_count = min(keep_count, numel(snapshots.times_requested));
    snapshots.times_requested = snapshots.times_requested(1:keep_count);
    snapshots.times_actual = snapshots.times_actual(1:keep_count);
    for i = 1:numel(snapshot_fields)
        key = snapshot_fields{i};
        snapshots.(key) = snapshots.(key)(:, :, 1:keep_count);
    end
end

function meta = empty_evolution_instability_metadata()
    meta = struct( ...
        'detected', false, ...
        'reason', '', ...
        'failed_time', NaN, ...
        'failed_step', NaN, ...
        'failed_dt', NaN, ...
        'failed_omega_max', NaN, ...
        'failed_psi_max', NaN, ...
        'last_stable_time', NaN, ...
        'last_stable_step', NaN, ...
        'completed_stable_steps', 0);
end

function [is_unstable, meta] = detect_evolution_instability(State, Metrics, snapshot_fields, dt_step, null_field_guard)
    meta = empty_evolution_instability_metadata();
    meta.failed_dt = double(dt_step);
    if nargin < 5 || ~isstruct(null_field_guard)
        null_field_guard = struct('enabled', false);
    end
    if isstruct(State)
        if isfield(State, 't') && isnumeric(State.t) && isscalar(State.t)
            meta.failed_time = double(State.t);
        end
        if isfield(State, 'step') && isnumeric(State.step) && isscalar(State.step)
            meta.failed_step = double(State.step);
        end
    end

    meta.failed_omega_max = state_field_max_abs(State, 'omega');
    meta.failed_psi_max = state_field_max_abs(State, 'psi');

    state_fields = unique([snapshot_fields(:); {'omega'}; {'psi'}], 'stable');
    for i = 1:numel(state_fields)
        field_name = state_fields{i};
        if ~isstruct(State) || ~isfield(State, field_name) || ~isnumeric(State.(field_name)) || isempty(State.(field_name))
            continue;
        end
        values = double(State.(field_name));
        if any(~isfinite(values(:)))
            meta.detected = true;
            meta.reason = sprintf('non-finite values in State.%s', field_name);
            is_unstable = true;
            return;
        end
    end

    metric_fields = {'max_vorticity', 'kinetic_energy', 'enstrophy', 'peak_speed', 'max_abs_u_plus_v'};
    for i = 1:numel(metric_fields)
        field_name = metric_fields{i};
        if ~isstruct(Metrics) || ~isfield(Metrics, field_name) || ~isnumeric(Metrics.(field_name)) || isempty(Metrics.(field_name))
            continue;
        end
        values = double(Metrics.(field_name));
        if any(~isfinite(values(:)))
            meta.detected = true;
            meta.reason = sprintf('non-finite values in Metrics.%s', field_name);
            is_unstable = true;
            return;
        end
    end

    if should_trip_null_field_collapse_guard(State, null_field_guard, meta)
        meta.detected = true;
        meta.reason = sprintf(['near-null omega+psi collapse detected: ' ...
            '|omega|_max=%g, |psi|_max=%g'], meta.failed_omega_max, meta.failed_psi_max);
        is_unstable = true;
        return;
    end

    is_unstable = false;
end

function guard = resolve_null_field_collapse_guard(Parameters, State, snapshot_fields)
    guard = struct( ...
        'enabled', false, ...
        'minimum_time_s', 0.25, ...
        'omega_abs_tol', 1.0e-10, ...
        'psi_abs_tol', 1.0e-10, ...
        'omega_ratio_tol', 1.0e-6, ...
        'psi_ratio_tol', 1.0e-6, ...
        'initial_omega_max', NaN, ...
        'initial_psi_max', NaN);
    if ~(isstruct(Parameters) && isfield(Parameters, 'null_field_collapse_guard') && ...
            isstruct(Parameters.null_field_collapse_guard))
        return;
    end
    cfg = Parameters.null_field_collapse_guard;
    guard.enabled = local_pick_logical(cfg, {'enabled'}, false);
    if ~guard.enabled
        return;
    end
    guard.minimum_time_s = local_pick_numeric(cfg, {'minimum_time_s'}, guard.minimum_time_s);
    guard.omega_abs_tol = local_pick_numeric(cfg, {'omega_abs_tol'}, guard.omega_abs_tol);
    guard.psi_abs_tol = local_pick_numeric(cfg, {'psi_abs_tol'}, guard.psi_abs_tol);
    guard.omega_ratio_tol = local_pick_numeric(cfg, {'omega_ratio_tol'}, guard.omega_ratio_tol);
    guard.psi_ratio_tol = local_pick_numeric(cfg, {'psi_ratio_tol'}, guard.psi_ratio_tol);
    state_fields = unique([snapshot_fields(:); {'omega'}; {'psi'}], 'stable');
    if ~any(strcmp(state_fields, 'omega')) || ~any(strcmp(state_fields, 'psi'))
        guard.enabled = false;
        return;
    end
    guard.initial_omega_max = state_field_max_abs(State, 'omega');
    guard.initial_psi_max = state_field_max_abs(State, 'psi');
    if ~(isfinite(guard.initial_omega_max) && guard.initial_omega_max > 0 && ...
            isfinite(guard.initial_psi_max) && guard.initial_psi_max > 0)
        guard.enabled = false;
    end
end

function tf = should_trip_null_field_collapse_guard(State, guard, meta)
    tf = false;
    if ~(isstruct(guard) && isfield(guard, 'enabled') && logical(guard.enabled))
        return;
    end
    current_time = local_pick_numeric(State, {'t'}, NaN);
    if ~(isfinite(current_time) && current_time >= max(0, double(guard.minimum_time_s)))
        return;
    end

    omega_abs_ok = isfinite(meta.failed_omega_max) && meta.failed_omega_max <= double(guard.omega_abs_tol);
    psi_abs_ok = isfinite(meta.failed_psi_max) && meta.failed_psi_max <= double(guard.psi_abs_tol);
    omega_ratio_ok = isfinite(meta.failed_omega_max) && isfinite(guard.initial_omega_max) && guard.initial_omega_max > 0 && ...
        meta.failed_omega_max <= double(guard.omega_ratio_tol) * double(guard.initial_omega_max);
    psi_ratio_ok = isfinite(meta.failed_psi_max) && isfinite(guard.initial_psi_max) && guard.initial_psi_max > 0 && ...
        meta.failed_psi_max <= double(guard.psi_ratio_tol) * double(guard.initial_psi_max);

    tf = (omega_abs_ok || omega_ratio_ok) && (psi_abs_ok || psi_ratio_ok);
end

function value = state_field_max_abs(State, field_name)
    value = NaN;
    if ~(isstruct(State) && isfield(State, field_name) && isnumeric(State.(field_name)) && ~isempty(State.(field_name)))
        return;
    end
    finite_values = double(State.(field_name));
    finite_values = finite_values(isfinite(finite_values));
    if isempty(finite_values)
        return;
    end
    value = max(abs(finite_values), [], 'omitnan');
end

function analysis = append_snapshot_struct_to_analysis(analysis, snapshots, snapshot_fields)
    for i = 1:numel(snapshot_fields)
        key = snapshot_fields{i};
        analysis_key = snapshot_analysis_field_name(key);
        analysis.(analysis_key) = snapshots.(key);
    end
end

function analysis_key = snapshot_analysis_field_name(key)
    analysis_key = sprintf('%s_snaps', key);
    switch key
        case 'omega'
            analysis_key = 'omega_snaps';
        case 'psi'
            analysis_key = 'psi_snaps';
    end
end

function snapshot_fields = filter_snapshot_fields_for_storage(snapshot_fields, Parameters)
    if local_pick_logical(Parameters, {'store_velocity_snapshot_cubes'}, true)
        return;
    end
    mask = ~ismember(snapshot_fields, {'u', 'v'});
    snapshot_fields = snapshot_fields(mask);
end

function precision = resolve_snapshot_storage_precision(Parameters)
    precision = 'double';
    if nargin < 1 || ~isstruct(Parameters)
        return;
    end
    if isfield(Parameters, 'snapshot_storage_precision') && ~isempty(Parameters.snapshot_storage_precision)
        requested = lower(strtrim(char(string(Parameters.snapshot_storage_precision))));
        if any(strcmp(requested, {'single', 'double'}))
            precision = requested;
        end
    end
end

function value = local_pick_logical(s, keys, fallback)
    value = logical(fallback);
    if ~(isstruct(s) && ~isempty(keys))
        return;
    end
    for i = 1:numel(keys)
        key = keys{i};
        if isfield(s, key) && ~isempty(s.(key))
            value = logical(s.(key));
            return;
        end
    end
end

function value = local_pick_numeric(s, keys, fallback)
    value = double(fallback);
    if ~(isstruct(s) && ~isempty(keys))
        return;
    end
    for i = 1:numel(keys)
        key = keys{i};
        if isfield(s, key) && isnumeric(s.(key)) && isscalar(s.(key)) && isfinite(s.(key))
            value = double(s.(key));
            return;
        end
    end
end

function ctx = build_mode_context(~, Settings)
    % Build mode-specific context data
    ctx = struct();
    ctx.save_data = Settings.save_data;
    ctx.save_figures = Settings.save_figures;
    ctx.monitor_enabled = Settings.monitor_enabled;
end

function cfg = prepare_cfg(Run_Config, Parameters)
    % Prepare configuration struct for method entrypoints
    % This is a standardized interface between mode and method
    params_local = Parameters;
    params_local.ic_type = Run_Config.ic_type;

    cfg = MethodConfigBuilder.build( ...
        params_local, ...
        Run_Config.method, ...
        'mode_evolution.prepare_cfg');

    passthrough = {'simulation_scenario', ...
        'bathymetry_scenario', 'bathymetry_bed_slope', 'bathymetry_resolution', ...
        'bathymetry_custom_points', 'bathymetry_use_dry_mask', ...
        'bathymetry_dimension_policy', 'ic_amplitude'};
    for i = 1:numel(passthrough)
        key = passthrough{i};
        if isfield(Parameters, key)
            cfg.(key) = Parameters.(key);
        end
    end
end

function fig_meta = generate_evolution_figures(analysis, Parameters, Run_Config, paths, ~)
    % Generate evolution figures (contours, vectors, etc.)
    % Reuse existing visualization utilities

    % Create main evolution figure
    fig = figure('Position', [100, 100, 1200, 800]);
    apply_dark_theme_for_figure(fig);

    Nsnap = size(analysis.omega_snaps, 3);
    ncols = max(1, ceil(sqrt(Nsnap)));
    nrows = ceil(Nsnap / ncols);
    if isfield(Parameters, 'Lx') && isfield(Parameters, 'Ly') && isfield(Parameters, 'Nx') && isfield(Parameters, 'Ny')
        x = linspace(-Parameters.Lx / 2, Parameters.Lx / 2, Parameters.Nx);
        y = linspace(-Parameters.Ly / 2, Parameters.Ly / 2, Parameters.Ny);
    else
        x = 1:size(analysis.omega_snaps, 2);
        y = 1:size(analysis.omega_snaps, 1);
    end

    % Use snapshot_times (per-snapshot) NOT time_vec (per-step) for labels.
    % time_vec has Nt+1 entries; indexing time_vec(1:Nsnap) gives the first
    % Nsnap *time-step* times (e.g. 0,dt,2dt,...) instead of the actual
    % snapshot capture times (e.g. 0, Tfinal/8, ..., Tfinal).
    if isfield(analysis, 'snapshot_times') && numel(analysis.snapshot_times) >= Nsnap
        snap_t = analysis.snapshot_times;
    elseif isfield(Parameters, 'snap_times') && numel(Parameters.snap_times) >= Nsnap
        snap_t = Parameters.snap_times;
    else
        snap_t = linspace(0, Parameters.Tfinal, Nsnap);
    end

    for k = 1:Nsnap
        subplot(nrows, ncols, k);
        imagesc(x, y, analysis.omega_snaps(:, :, k));
        axis equal tight;
        set(gca, 'YDir', 'normal');
        colormap(turbo);
        colorbar;
        title(sprintf('t = %.3f', snap_t(k)));
    end

    sgtitle(sprintf('Evolution: %s | Method: %s', Run_Config.ic_type, Run_Config.method));

    % Save the montage as an image-only artifact; the .fig is not useful for
    % this dense tiled snapshot overview.
    fig_name = RunIDGenerator.make_figure_filename(paths.storage_id, 'evolution', '');
    fig_path = fullfile(paths.figures_evolution, fig_name);
    save_settings = Settings;
    save_settings.figure_save_fig = false;
    saved_outputs = ResultsPlotDispatcher.save_figure_bundle(fig, fig_path, save_settings);
    close(fig);

    fig_meta = struct();
    fig_meta.nrows = nrows;
    fig_meta.ncols = ncols;
    fig_meta.snapshot_count = Nsnap;
    fig_meta.path = ResultsPlotDispatcher.primary_output_path(saved_outputs, fig_path);
end

function output_root = resolve_output_root(Settings)
    output_root = 'Results';
    if isfield(Settings, 'output_root') && ~isempty(Settings.output_root)
        output_root = char(string(Settings.output_root));
    end
end

function tf = use_preinitialized_artifact_root(Settings)
    tf = false;
    if ~isstruct(Settings) || ~isfield(Settings, 'preinitialized_artifact_root')
        return;
    end
    tf = logical(Settings.preinitialized_artifact_root);
end

function progress_callback = resolve_progress_callback(Settings)
    progress_callback = resolve_runtime_progress_callback(Settings);
end

function policy = resolve_progress_policy(Settings, Nt)
    policy = struct( ...
        'min_emit_seconds', 0.2, ...
        'iter_stride', max(1, round(Nt / 200)));

    if isfield(Settings, 'resource_allocation') && isstruct(Settings.resource_allocation)
        ra = Settings.resource_allocation;
        if isfield(ra, 'progress_hz') && isnumeric(ra.progress_hz) && isfinite(ra.progress_hz) && ra.progress_hz > 0
            policy.min_emit_seconds = 1 / min(max(double(ra.progress_hz), 0.5), 30);
        end
    end
end

function apply_dark_theme_for_figure(fig_handle)
    if isempty(fig_handle) || ~isvalid(fig_handle)
        return;
    end
    try
        ResultsPlotDispatcher.apply_dark_theme(fig_handle, ResultsPlotDispatcher.default_colors());
    catch
        % Plot styling failure should not abort mode execution.
    end
end

function progress_callback = emit_progress_payload(progress_callback, Run_Config, iteration, total_iterations, sim_time, Metrics, elapsed_wall, conv_residual, dt_value, time_step_mode)
    if isempty(progress_callback)
        return;
    end

    payload = struct();
    payload.phase = 'evolution';
    payload.method = Run_Config.method;
    payload.mode = 'evolution';
    payload.iteration = iteration;
    payload.total_iterations = total_iterations;
    payload.time = sim_time;
    payload.max_vorticity = Metrics.max_vorticity;
    payload.kinetic_energy = Metrics.kinetic_energy;
    payload.enstrophy = Metrics.enstrophy;
    payload.elapsed_wall = elapsed_wall;
    payload.convergence_residual = conv_residual;
    payload.dt = dt_value;
    payload.time_step_mode = time_step_mode;
    if isfield(Run_Config, 'run_id')
        payload.run_id = Run_Config.run_id;
    end

    try
        invoke_runtime_progress_callback(progress_callback, payload);
    catch ME
        warning('mode_evolution:ProgressCallbackDisabled', ...
            'Progress callback failed and will be disabled for this run: %s', ME.message);
        % Disable noisy callback failures after first error.
        progress_callback = [];
    end
end

function tf = should_return_analysis(Settings)
    tf = false;
    if ~isfield(Settings, 'compatibility') || ~isstruct(Settings.compatibility)
        return;
    end
    if ~isfield(Settings.compatibility, 'return_analysis')
        return;
    end
    tf = logical(Settings.compatibility.return_analysis);
end

function adaptive_cfg = resolve_phase1_adaptive_timestep_config(Parameters)
    adaptive_cfg = struct('enabled', false, 'C_adv', 0.5, 'C_diff', 0.25);
    if ~isstruct(Parameters)
        return;
    end

    phase_cfg = struct();
    enable_adaptive = false;
    if isfield(Parameters, 'phase2_adaptive_timestep_enabled') && ...
            logical(Parameters.phase2_adaptive_timestep_enabled) && ...
            isfield(Parameters, 'phase2') && isstruct(Parameters.phase2)
        phase_cfg = Parameters.phase2;
        enable_adaptive = true;
    end
    if ~enable_adaptive
        return;
    end
    if ~isfield(phase_cfg, 'adaptive_timestep') || ~isstruct(phase_cfg.adaptive_timestep)
        return;
    end
    adaptive_cfg = phase_cfg.adaptive_timestep;
    adaptive_cfg.enabled = logical(pick_struct_logical(adaptive_cfg, 'enabled', true));
    adaptive_cfg.C_adv = pick_struct_numeric(adaptive_cfg, 'C_adv', 0.5);
    adaptive_cfg.C_diff = pick_struct_numeric(adaptive_cfg, 'C_diff', 0.25);
    if ~(isfinite(adaptive_cfg.C_adv) && adaptive_cfg.C_adv > 0)
        error('Evolution:InvalidAdaptiveTimeStepAdvectiveConstant', ...
            'Phase 1 adaptive timestep requires a finite positive C_adv.');
    end
    if ~(isfinite(adaptive_cfg.C_diff) && adaptive_cfg.C_diff > 0)
        error('Evolution:InvalidAdaptiveTimeStepDiffusiveConstant', ...
            'Phase 1 adaptive timestep requires a finite positive C_diff.');
    end
end

function tf = is_phase1_local_mesh_sweep(Parameters)
    tf = false;
    if ~isstruct(Parameters) || ~isfield(Parameters, 'phase1_convergence_runtime')
        return;
    end
    tf = strcmpi(char(string(Parameters.phase1_convergence_runtime)), 'local_mesh_sweep');
end

function components = compute_adaptive_timestep_components(Metrics, cfg, adaptive_cfg)
    if ~isfield(Metrics, 'max_abs_u_plus_v')
        error('Evolution:MissingAdaptiveVelocityMetric', ...
            'Adaptive timestep requires diagnostics.max_abs_u_plus_v.');
    end
    delta = resolve_adaptive_delta(cfg);
    max_abs_u_plus_v = double(Metrics.max_abs_u_plus_v);
    if ~(isfinite(max_abs_u_plus_v) && max_abs_u_plus_v > eps)
        dt_adv = inf;
    else
        dt_adv = adaptive_cfg.C_adv * delta / max_abs_u_plus_v;
    end
    if ~(isfinite(cfg.nu) && cfg.nu > 0)
        dt_diff = inf;
    else
        dt_diff = adaptive_cfg.C_diff * delta^2 / (2 * cfg.nu);
    end
    components = struct( ...
        'delta', double(delta), ...
        'dt_adv', double(dt_adv), ...
        'dt_diff', double(dt_diff), ...
        'dt_final', double(min([dt_adv, dt_diff])), ...
        'dt_step', NaN);
end

function dt_step = clip_adaptive_timestep(components, time_remaining, fallback_dt)
    dt_step = min([components.dt_final, double(time_remaining)]);
    if ~(isfinite(dt_step) && dt_step > 0)
        if isfinite(fallback_dt) && fallback_dt > 0 && isfinite(time_remaining) && time_remaining > 0
            dt_step = min(double(fallback_dt), double(time_remaining));
        else
            error('Evolution:InvalidAdaptiveTimeStep', ...
                'Adaptive timestep evaluation produced a non-positive step.');
        end
    end
end

function meta = empty_adaptive_timestep_metadata()
    meta = struct( ...
        'delta', NaN, ...
        'dt_adv', NaN, ...
        'dt_diff', NaN, ...
        'dt_final', NaN, ...
        'dt_step', NaN, ...
        'cfl_adv', NaN, ...
        'cfl_diff', NaN);
end

function delta = resolve_adaptive_delta(cfg)
    dx = pick_struct_numeric(cfg, 'dx', NaN);
    dy = pick_struct_numeric(cfg, 'dy', NaN);
    candidates = [dx, dy];
    candidates = candidates(isfinite(candidates) & candidates > 0);
    if ~isempty(candidates)
        delta = max(candidates);
        return;
    end
    if isfield(cfg, 'delta') && isfinite(cfg.delta) && cfg.delta > 0
        delta = double(cfg.delta);
        return;
    end
    error('Evolution:MissingAdaptiveMeshSpacing', ...
        'Adaptive timestep requires finite positive dx/dy or cfg.delta.');
end

function components = stamp_adaptive_timestep_step_usage(components, adaptive_cfg, dt_step)
    components.dt_step = double(dt_step);
    components.cfl_adv = resolve_terminal_adaptive_cfl(components.dt_adv, adaptive_cfg.C_adv, dt_step);
    components.cfl_diff = resolve_terminal_adaptive_cfl(components.dt_diff, adaptive_cfg.C_diff, dt_step);
end

function [dt_initial, dt_final, dt_min, dt_max] = resolve_phase1_timestep_summary(adaptive_enabled, dt_history, fallback_dt)
    if adaptive_enabled
        dt_initial = first_finite_value(dt_history, fallback_dt);
        dt_final = last_finite_value(dt_history, fallback_dt);
        dt_min = min(dt_history(isfinite(dt_history)));
        dt_max = max(dt_history(isfinite(dt_history)));
    else
        dt_initial = double(fallback_dt);
        dt_final = double(fallback_dt);
        dt_min = double(fallback_dt);
        dt_max = double(fallback_dt);
    end
end

function stability_meta = resolve_phase1_terminal_stability_metadata(adaptive_enabled, adaptive_meta, Metrics, cfg, adaptive_cfg, fixed_dt)
    if adaptive_enabled
        stability_meta = adaptive_meta;
        return;
    end
    stability_meta = compute_adaptive_timestep_components(Metrics, cfg, adaptive_cfg);
    stability_meta = stamp_adaptive_timestep_step_usage(stability_meta, adaptive_cfg, fixed_dt);
end

function cfl_value = resolve_terminal_adaptive_cfl(dt_limit, cfl_limit, dt_step)
    if ~(isfinite(dt_step) && dt_step >= 0)
        cfl_value = NaN;
        return;
    end
    if ~(isfinite(dt_limit) && dt_limit > 0)
        cfl_value = 0;
        return;
    end
    cfl_value = double(cfl_limit) * double(dt_step) / double(dt_limit);
end

function estimate = estimate_adaptive_total_iterations(completed_steps, current_time, Tfinal, current_dt, existing_total)
    remaining_time = max(0, double(Tfinal) - double(current_time));
    if ~(isfinite(current_dt) && current_dt > 0)
        estimate = max(existing_total, completed_steps + 1);
        return;
    end
    remaining_steps = ceil(remaining_time / current_dt);
    estimate = max(existing_total, completed_steps + max(remaining_steps, 1));
end

function [time_vec, kinetic_energy, enstrophy, max_vorticity, mass_total, min_depth] = ...
        grow_history_capacity(time_vec, kinetic_energy, enstrophy, max_vorticity, mass_total, min_depth, growth_size)
    time_vec = [time_vec, zeros(1, growth_size)]; %#ok<AGROW>
    kinetic_energy = [kinetic_energy, zeros(1, growth_size)]; %#ok<AGROW>
    enstrophy = [enstrophy, zeros(1, growth_size)]; %#ok<AGROW>
    max_vorticity = [max_vorticity, zeros(1, growth_size)]; %#ok<AGROW>
    mass_total = [mass_total, nan(1, growth_size)]; %#ok<AGROW>
    min_depth = [min_depth, nan(1, growth_size)]; %#ok<AGROW>
end

function value = pick_struct_numeric(source, field_name, fallback)
    value = fallback;
    if isstruct(source) && isfield(source, field_name) && isnumeric(source.(field_name)) && ...
            isscalar(source.(field_name)) && isfinite(source.(field_name))
        value = double(source.(field_name));
    end
end

function value = pick_struct_logical(source, field_name, fallback)
    value = fallback;
    if isstruct(source) && isfield(source, field_name)
        value = logical(source.(field_name));
    end
end

function value = first_finite_value(values, fallback)
    value = fallback;
    idx = find(isfinite(values), 1, 'first');
    if ~isempty(idx)
        value = values(idx);
    end
end

function value = last_finite_value(values, fallback)
    value = fallback;
    idx = find(isfinite(values), 1, 'last');
    if ~isempty(idx)
        value = values(idx);
    end
end

function label = resolve_time_step_mode_label(adaptive_enabled)
    if adaptive_enabled
        label = 'adaptive_stability';
    else
        label = 'fixed';
    end
end
