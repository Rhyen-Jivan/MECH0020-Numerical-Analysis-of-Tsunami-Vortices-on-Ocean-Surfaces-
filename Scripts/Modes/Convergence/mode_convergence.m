function [Results, paths] = mode_convergence(Run_Config, Parameters, Settings)
% mode_convergence - Method-aware staged convergence verification runtime.

    [ok, issues] = validate_convergence(Run_Config, Parameters);
    if ~ok
        error('mode_convergence:ValidationFailed', ...
            'Convergence mode validation failed: %s', strjoin(issues, '; '));
    end

    if ~isfield(Run_Config, 'study_id') || isempty(Run_Config.study_id)
        Run_Config.study_id = RunIDGenerator.generate(Run_Config, Parameters);
        Run_Config.study_id = compact_convergence_study_id(Run_Config.study_id, Run_Config.method);
    end

    output_root = resolve_output_root(Settings);
    if use_preinitialized_artifact_root(Settings)
        paths = PathBuilder.get_existing_root_paths(output_root, Run_Config.method, Run_Config.mode);
    else
        paths = PathBuilder.get_run_paths(Run_Config.method, Run_Config.mode, Run_Config.study_id, output_root);
    end
    PathBuilder.ensure_directories(paths);

    Run_Config_clean = filter_graphics_objects(Run_Config);
    Parameters_clean = filter_graphics_objects(Parameters);
    Settings_clean = filter_graphics_objects(Settings);
    safe_save_mat(fullfile(paths.config, 'Config.mat'), struct( ...
        'Run_Config_clean', Run_Config_clean, ...
        'Parameters_clean', Parameters_clean, ...
        'Settings_clean', Settings_clean));

    dispatch_info = ConvergenceAgentDispatcher.resolve_dispatch_info(Parameters);
    if strcmp(dispatch_info.strategy, 'agent_guided')
        [Results, paths] = run_agent_branch(Run_Config, Parameters, Settings, paths, dispatch_info);
        return;
    end

    spec = normalize_convergence_spec(Run_Config, Parameters);
    callbacks = resolve_method_callbacks(Run_Config.method);

    MonitorInterface.start(Run_Config, Settings);
    progress_callback = resolve_progress_callback(Settings);
    total_timer = tic;

    stage_summaries = repmat(empty_stage_summary(), 0, 1);
    run_records = repmat(empty_run_record(), 0, 1);
    stage_internals = cell(0, 1);

    for stage_idx = 1:numel(spec.stages)
        stage_spec = spec.stages(stage_idx);
        [stage_summary, stage_records, stage_internal] = execute_stage( ...
            Run_Config, Parameters, Settings, callbacks, spec, stage_spec, paths, ...
            progress_callback, dispatch_info, total_timer);
        stage_summaries(end + 1, 1) = stage_summary; %#ok<AGROW>
        if ~isempty(stage_records)
            run_records = [run_records; stage_records(:)]; %#ok<AGROW>
        end
        stage_internals{end + 1, 1} = stage_internal; %#ok<AGROW>
    end

    total_time = toc(total_timer);
    Results = assemble_results(Run_Config, Parameters, dispatch_info, spec, stage_summaries, run_records, total_time);

    if logical(Settings.save_data)
        persist_convergence_results(Results, paths);
        persist_stage_artifacts(stage_internals, paths);
    end
    if logical(Settings.save_figures)
        generate_convergence_figures(Results, stage_internals, Run_Config, paths, Settings);
    end
    if logical(Settings.save_reports)
        Results.report_paths = generate_convergence_reports(Results, stage_internals, Run_Config, paths);
    else
        Results.report_paths = struct('markdown', '', 'audit_markdown', '');
    end

    Run_Summary = struct();
    Run_Summary.total_time = total_time;
    Run_Summary.status = 'completed';
    MonitorInterface.stop(Run_Summary);
end

function [Results, paths] = run_agent_branch(Run_Config, Parameters, Settings, paths, dispatch_info)
    run_timer = tic;
    progress_callback = resolve_progress_callback(Settings);
    progress_callback = emit_convergence_progress_payload(progress_callback, Run_Config, ...
        dispatch_info, 'preflight', 0, NaN, NaN, NaN, NaN, NaN, toc(run_timer), NaN);

    [Results, dispatch_request] = ConvergenceAgentDispatcher.dispatch( ...
        Run_Config, Parameters, Settings, paths, dispatch_info);
    Results.convergence_dispatch_request = dispatch_request;
    progress_callback = emit_agent_trace_progress(progress_callback, Run_Config, dispatch_info, Results, run_timer);
    progress_callback = emit_convergence_progress_payload(progress_callback, Run_Config, ...
        dispatch_info, 'completed', NaN, NaN, double(Results.converged_N), ...
        double(Results.converged_N), NaN, NaN, toc(run_timer), NaN);

    if logical(Settings.save_data)
        safe_save_mat(fullfile(paths.data, 'convergence_results.mat'), struct('Results', Results), '-v7.3');
    end
    if logical(Settings.save_figures)
        generate_agent_compatibility_figure(Results, Run_Config, paths, Settings);
    end
    if logical(Settings.save_reports)
        Results.report_paths = generate_agent_compatibility_report(Results, Run_Config, paths);
    else
        Results.report_paths = struct('markdown', '', 'audit_markdown', '');
    end

    Run_Summary = struct();
    Run_Summary.total_time = Results.total_time;
    Run_Summary.status = 'completed';
    MonitorInterface.stop(Run_Summary);
end

function [ok, issues] = validate_convergence(Run_Config, Parameters)
    ok = true;
    issues = {};

    if ~isfield(Run_Config, 'method') || isempty(Run_Config.method)
        ok = false;
        issues{end + 1} = 'Run_Config.method is required'; %#ok<AGROW>
    end
    if ~isfield(Parameters, 'Tfinal') || ~(isfinite(double(Parameters.Tfinal)) && double(Parameters.Tfinal) > 0)
        ok = false;
        issues{end + 1} = 'Parameters.Tfinal must be finite and > 0'; %#ok<AGROW>
    end
    if ~isfield(Parameters, 'dt') || ~(isfinite(double(Parameters.dt)) && double(Parameters.dt) > 0)
        ok = false;
        issues{end + 1} = 'Parameters.dt must be finite and > 0'; %#ok<AGROW>
    end
    if ~isfield(Parameters, 'Nx') || ~isfield(Parameters, 'Ny')
        ok = false;
        issues{end + 1} = 'Parameters.Nx and Parameters.Ny are required'; %#ok<AGROW>
    end

    method_type = resolve_method_type(Run_Config.method);
    if strcmp(method_type, 'spectral')
        spectral_bc = BCDispatcher.resolve(Parameters, 'spectral', build_spectral_grid_meta(Parameters));
        if ~spectral_bc.capability.supported
            ok = false;
            issues{end + 1} = sprintf('Spectral BC unsupported for convergence: %s', spectral_bc.capability.reason); %#ok<AGROW>
        end
    end
end

function spec = normalize_convergence_spec(Run_Config, Parameters)
    method_type = resolve_method_type(Run_Config.method);
    conv_cfg = pick_struct_field(Parameters, 'convergence', struct());
    study_cfg = pick_struct_field(conv_cfg, 'study', struct());
    temporal_cfg = pick_struct_field(study_cfg, 'temporal', struct());
    spatial_cfg = pick_struct_field(study_cfg, 'spatial', struct());
    modal_cfg = pick_struct_field(study_cfg, 'modal', struct());
    reference_cfg = pick_struct_field(study_cfg, 'reference', struct());
    verdict_cfg = pick_struct_field(study_cfg, 'verdict', struct());

    tol = resolve_positive_scalar(study_cfg, {'tolerance'}, ...
        resolve_positive_scalar(Parameters, {'convergence_tol'}, 5.0e-2));
    criterion = normalize_convergence_criterion(resolve_string(study_cfg, {'criterion_type'}, ...
        resolve_string(Parameters, {'convergence_criterion_type'}, 'successive_vorticity_relative_l2')));

    case_id = resolve_string(study_cfg, {'verification_case', 'case_id'}, ...
        resolve_string(Parameters, {'convergence_verification_case', 'verification_case'}, ''));
    case_model = resolve_verification_case(method_type, case_id);

    dt_values = resolve_temporal_levels(Parameters, temporal_cfg, case_model);
    [primary_levels, primary_axis, primary_cfg] = resolve_primary_levels( ...
        Parameters, method_type, spatial_cfg, modal_cfg, case_model);

    temporal_fine_n = round(resolve_positive_scalar(temporal_cfg, {'fine_N'}, ...
        max([Parameters.Nx, Parameters.Ny, primary_levels(end)])));
    temporal_fine_n = max(8, temporal_fine_n);

    reference_preferences = struct();
    reference_preferences.requested = resolve_string(reference_cfg, {'preferred_strategy', 'strategy'}, '');
    reference_preferences.allow_self_reference = resolve_logical(reference_cfg, {'allow_self_reference'}, true);
    reference_preferences.reference_multiplier = resolve_positive_scalar(reference_cfg, {'refinement_multiplier'}, 2.0);

    verdict = struct();
    verdict.tolerance = tol;
    verdict.monotonicity_relaxation = resolve_positive_scalar(verdict_cfg, {'monotonicity_relaxation'}, 0.05);
    verdict.minimum_improvement_factor = resolve_positive_scalar(verdict_cfg, {'minimum_improvement_factor'}, 1.05);

    spec = struct();
    spec.method_type = method_type;
    spec.case_id = case_id;
    spec.case_model = case_model;
    spec.criterion_type = criterion;
    spec.tolerance = tol;
    spec.reference_preferences = reference_preferences;
    spec.verdict = verdict;
    spec.primary_axis = primary_axis;
    spec.primary_levels = primary_levels(:).';
    spec.temporal_levels = dt_values(:).';
    spec.primary_cfg = primary_cfg;
    spec.temporal_fine_n = temporal_fine_n;
    spec.primary_stage_dt = min(dt_values) / max(2.0, reference_preferences.reference_multiplier);
    spec.expected_orders = resolve_expected_orders(method_type, case_model);
    spec.metadata = struct( ...
        'spatial_or_modal_label', resolve_primary_stage_label(method_type), ...
        'reference_case_description', pick_field(case_model, 'description', ''), ...
        'verification_case', case_id);

    temporal_stage = make_stage_spec('temporal', 'dt', dt_values, tol, spec.expected_orders.temporal, ...
        reference_preferences, method_type, temporal_fine_n, primary_levels(end), case_model);
    primary_stage_name = resolve_primary_stage_label(method_type);
    primary_stage = make_stage_spec(primary_stage_name, primary_axis, primary_levels, tol, ...
        pick_field(spec.expected_orders, primary_stage_name, NaN), reference_preferences, ...
        method_type, temporal_fine_n, primary_levels(end), case_model);
    spec.stages = [temporal_stage, primary_stage];
end

function stage = make_stage_spec(stage_name, axis_name, values, tolerance, expected_order, reference_preferences, method_type, temporal_fine_n, max_primary_n, case_model)
    stage = struct();
    stage.stage_name = char(string(stage_name));
    stage.refinement_axis = char(string(axis_name));
    stage.values = double(values(:).');
    stage.tolerance = double(tolerance);
    stage.expected_order = double(expected_order);
    stage.method_type = method_type;
    stage.temporal_fine_n = temporal_fine_n;
    stage.max_primary_n = max_primary_n;
    stage.case_model = case_model;
    stage.reference_strategy = resolve_reference_strategy(stage_name, reference_preferences, case_model);
end

function strategy = resolve_reference_strategy(stage_name, reference_preferences, case_model)
    requested = lower(strtrim(char(string(pick_field(reference_preferences, 'requested', '')))));
    if ~isempty(requested)
        strategy = requested;
        return;
    end

    if ~isempty(case_model) && isfield(case_model, 'reference_mode')
        ref_mode = lower(char(string(case_model.reference_mode)));
        if strcmp(ref_mode, 'analytic')
            strategy = 'analytic_manufactured';
            return;
        end
        if strcmp(ref_mode, 'over_resolved')
            strategy = 'over_resolved_numerical';
            return;
        end
    end

    if strcmp(stage_name, 'temporal')
        strategy = 'over_resolved_numerical';
    else
        strategy = 'finest_self_reference';
        if ~isempty(case_model) && isfield(case_model, 'exact_omega')
            strategy = 'analytic_manufactured';
        end
    end
end

function [primary_levels, primary_axis, primary_cfg] = resolve_primary_levels(Parameters, method_type, spatial_cfg, modal_cfg, case_model)
    legacy_levels = resolve_legacy_n_levels(Parameters);
    primary_cfg = struct();
    switch method_type
        case {'fd', 'fv'}
            explicit_levels = resolve_numeric_vector(spatial_cfg, {'N_values', 'levels'}, []);
            if isempty(explicit_levels)
                explicit_levels = resolve_numeric_vector(Parameters, {'mesh_sizes'}, legacy_levels);
            end
            if isempty(explicit_levels) && ~isempty(case_model) && isfield(case_model, 'recommended_primary_levels')
                explicit_levels = double(case_model.recommended_primary_levels(:).');
            end
            if isempty(explicit_levels)
                explicit_levels = [32, 64, 128];
            end
            primary_levels = unique(max(8, round(explicit_levels)), 'stable');
            primary_axis = 'h';
            primary_cfg.nz_fixed = round(resolve_positive_scalar(spatial_cfg, {'Nz_fixed'}, ...
                resolve_positive_scalar(Parameters, {'Nz'}, 8)));

        case 'spectral'
            explicit_levels = resolve_numeric_vector(modal_cfg, {'N_values', 'levels'}, []);
            if isempty(explicit_levels)
                explicit_levels = resolve_numeric_vector(Parameters, {'mesh_sizes'}, legacy_levels);
            end
            if isempty(explicit_levels) && ~isempty(case_model) && isfield(case_model, 'recommended_primary_levels')
                explicit_levels = double(case_model.recommended_primary_levels(:).');
            end
            if isempty(explicit_levels)
                explicit_levels = [8, 16, 32, 64];
            end
            primary_levels = unique(max(4, round(explicit_levels)), 'stable');
            primary_axis = 'mode_count';

        otherwise
            error('mode_convergence:UnsupportedMethod', ...
                'Unsupported convergence method "%s".', method_type);
    end
end

function dt_values = resolve_temporal_levels(Parameters, temporal_cfg, case_model)
    dt_values = resolve_numeric_vector(temporal_cfg, {'dt_values', 'levels'}, []);
    if isempty(dt_values) && ~isempty(case_model) && isfield(case_model, 'recommended_dt_values')
        dt_values = double(case_model.recommended_dt_values(:).');
    end
    if isempty(dt_values)
        base_dt = double(Parameters.dt);
        dt_values = sort(unique(base_dt .* [4, 2, 1]), 'descend');
    end
    dt_values = dt_values(isfinite(dt_values) & dt_values > 0);
    dt_values = sort(unique(dt_values), 'descend');
    if numel(dt_values) < 2
        base_dt = double(Parameters.dt);
        dt_values = sort(unique([2 * base_dt, base_dt]), 'descend');
    end
end

function expected = resolve_expected_orders(method_type, case_model)
    expected = struct('temporal', NaN, 'spatial', NaN, 'modal', NaN);
    switch method_type
        case 'fd'
            expected.temporal = 4.0;
            expected.spatial = 2.0;
        case 'fv'
            expected.temporal = 3.0;
            expected.spatial = 2.0;
        case 'spectral'
            expected.temporal = 4.0;
            expected.modal = NaN;
    end
    if ~isempty(case_model) && isfield(case_model, 'expected_orders')
        fields = fieldnames(case_model.expected_orders);
        for i = 1:numel(fields)
            expected.(fields{i}) = double(case_model.expected_orders.(fields{i}));
        end
    end
end

function criterion = normalize_convergence_criterion(raw_criterion)
    token = lower(strtrim(char(string(raw_criterion))));
    token = strrep(token, '-', '_');
    token = strrep(token, ' ', '_');
    switch token
        case {'', 'l2_relative', 'l2_absolute', 'linf_relative', 'max_vorticity', ...
                'energy_dissipation', 'auto_physical', 'relative_change', ...
                'successive_relative_change', 'successive_vorticity_error', ...
                'successive_vorticity_relative_l2'}
            criterion = 'successive_vorticity_relative_l2';
        otherwise
            error('mode_convergence:UnsupportedCriterion', ...
                'Unsupported convergence criterion "%s".', char(string(raw_criterion)));
    end
end

function [stage_summary, stage_records, stage_internal] = execute_stage( ...
        Run_Config, Parameters, Settings, callbacks, spec, stage_spec, paths, ...
        progress_callback, dispatch_info, total_timer)

    n_levels = numel(stage_spec.values);
    level_results = repmat(empty_level_result(), n_levels, 1);
    progress_display = convergence_progress_display_payload(spec, stage_spec, 0, NaN, NaN);
    progress_callback = emit_convergence_progress_payload(progress_callback, Run_Config, ...
        dispatch_info, sprintf('%s_init', stage_spec.stage_name), 0, n_levels, ...
        NaN, NaN, NaN, NaN, toc(total_timer), NaN, progress_display);

    for i = 1:n_levels
        level_params = prepare_stage_level_parameters(Parameters, spec, stage_spec, stage_spec.values(i));
        [record, internal] = execute_level(Run_Config, level_params, callbacks, stage_spec);
        record.study_stage = char(string(stage_spec.stage_name));
        record.refinement_axis = char(string(stage_spec.refinement_axis));
        record.reference_strategy = 'pending';
        level_results(i).record = record;
        level_results(i).internal = internal;
        level_results(i).parameters = strip_runtime_only_fields(level_params);

        if i > 1
            prev_internal = level_results(i - 1).internal;
            record.relative_change = compute_successive_relative_change(prev_internal, internal);
            level_results(i).record.relative_change = record.relative_change;
        end

        next_hint = NaN;
        if i < n_levels
            next_hint = stage_spec.values(i + 1);
        end
        progress_display = convergence_progress_display_payload(spec, stage_spec, i, record.Nx, record.Ny);
        progress_callback = emit_convergence_progress_payload(progress_callback, Run_Config, ...
            dispatch_info, char(string(stage_spec.stage_name)), i, n_levels, ...
            record.Nx, record.Ny, record.relative_change, record.relative_change, toc(total_timer), next_hint, progress_display);

        if logical(Settings.save_data)
            safe_save_mat(fullfile(paths.data, sprintf('%s_level_%02d.mat', stage_spec.stage_name, i)), ...
                struct('record', level_results(i).record, ...
                       'internal', sanitize_internal_for_save(internal), ...
                       'Parameters', level_results(i).parameters), '-v7.3');
        end
    end

    [reference_info, reference_warning] = resolve_stage_reference( ...
        Run_Config, Parameters, callbacks, spec, stage_spec, level_results);
    [stage_records, stage_summary] = finalize_stage_records(stage_spec, spec, level_results, reference_info, reference_warning);
    stage_internal = struct();
    stage_internal.stage_spec = stage_spec;
    stage_internal.level_results = level_results;
    stage_internal.reference_info = sanitize_reference_for_save(reference_info);
    stage_internal.stage_summary = stage_summary;
end

function params = prepare_stage_level_parameters(Parameters, spec, stage_spec, stage_value)
    params = Parameters;
    params.convergence = strip_runtime_only_fields(pick_struct_field(Parameters, 'convergence', struct()));

    switch stage_spec.stage_name
        case 'temporal'
            params.dt = double(stage_value);
            params.Nx = stage_spec.temporal_fine_n;
            params.Ny = stage_spec.temporal_fine_n;
            if strcmp(spec.method_type, 'fv')
                params.Nz = max(1, round(pick_field(spec.primary_cfg, 'nz_fixed', pick_field(Parameters, 'Nz', 8))));
            end

        otherwise
            params.dt = double(spec.primary_stage_dt);
            params.Nx = round(double(stage_value));
            params.Ny = round(double(stage_value));
            if strcmp(spec.method_type, 'fv')
                params.Nz = max(1, round(pick_field(spec.primary_cfg, 'nz_fixed', pick_field(Parameters, 'Nz', 8))));
            end
    end

    params = apply_verification_case_if_present(params, spec.case_model, spec.method_type);
    params.snap_times = [0, params.Tfinal];
    params.num_snapshots = 2;

    if strcmp(spec.method_type, 'spectral')
        if isfield(params, 'kx')
            params = rmfield(params, 'kx');
        end
        if isfield(params, 'ky')
            params = rmfield(params, 'ky');
        end
    end
end

function params = apply_verification_case_if_present(params, case_model, method_type)
    if isempty(case_model)
        return;
    end

    params = apply_case_boundary_conditions(params, case_model, method_type);
    if isfield(case_model, 'initial_omega_builder')
        [X, Y] = build_initial_condition_grid(params, method_type);
        params.omega = case_model.initial_omega_builder(X, Y, params);
    end
end

function params = apply_case_boundary_conditions(params, case_model, method_type)
    if ~isfield(case_model, 'bc_case') || isempty(case_model.bc_case)
        return;
    end

    params.bc_case = case_model.bc_case;
    params.boundary_condition_case = case_model.bc_case;

    if strcmp(case_model.bc_case, 'user_defined')
        side_fields = {'bc_left', 'bc_right', 'bc_top', 'bc_bottom'};
        for i = 1:numel(side_fields)
            key = side_fields{i};
            if isfield(case_model, key)
                params.(key) = case_model.(key);
            end
        end
    end

    if strcmp(method_type, 'spectral')
        params.U_top = 0.0;
        params.U_bottom = 0.0;
        params.U_left = 0.0;
        params.U_right = 0.0;
    end
end

function [record, internal] = execute_level(Run_Config, Parameters, callbacks, stage_spec)
    record = empty_run_record();
    internal = empty_internal_record();

    cpu_start = cputime;
    wall_start = tic;
    memory_start = query_memory_mb();

    try
        cfg = MethodConfigBuilder.build(Parameters, Run_Config.method, 'mode_convergence.execute_level');
        ctx = struct('mode', 'convergence', 'study_stage', stage_spec.stage_name);
        State = callbacks.init(cfg, ctx);
        initial_state = State;
        initial_metrics = callbacks.diagnostics(State, cfg, ctx);
        initial_primary = resolve_state_primary_field(State);

        Nt = max(0, round(cfg.Tfinal / cfg.dt));
        peak_speed = resolve_metric_field(initial_metrics, 'peak_speed', NaN);
        max_cfl = estimate_cfl_from_state(peak_speed, cfg, Run_Config.method);
        nan_inf_flag = any_nonfinite_state(State);
        blow_up_flag = false;

        for step_idx = 1:Nt
            State = callbacks.step(State, cfg, ctx);
            metrics = callbacks.diagnostics(State, cfg, ctx);
            peak_speed = max(peak_speed, resolve_metric_field(metrics, 'peak_speed', NaN));
            max_cfl = max(max_cfl, estimate_cfl_from_state(resolve_metric_field(metrics, 'peak_speed', NaN), cfg, Run_Config.method));
            if any_nonfinite_state(State)
                nan_inf_flag = true;
                break;
            end
            if isfield(metrics, 'max_vorticity') && isfinite(metrics.max_vorticity)
                if metrics.max_vorticity > max(1.0e6, 1.0e4 * max(abs(initial_primary(:))))
                    blow_up_flag = true;
                    break;
                end
            end
        end

        final_metrics = callbacks.diagnostics(State, cfg, ctx);
        analysis = struct();
        analysis.snapshot_times = [0; State.t];
        analysis.time_vec = analysis.snapshot_times;
        analysis.snapshots_stored = 2;
        analysis.omega_snaps = cat(3, resolve_state_primary_field(initial_state), resolve_state_primary_field(State));
        analysis.psi_snaps = cat(3, resolve_state_auxiliary_field(initial_state, 'psi'), resolve_state_auxiliary_field(State, 'psi'));
        analysis.kinetic_energy = [resolve_metric_field(initial_metrics, 'kinetic_energy', NaN); resolve_metric_field(final_metrics, 'kinetic_energy', NaN)];
        analysis.enstrophy = [resolve_metric_field(initial_metrics, 'enstrophy', NaN); resolve_metric_field(final_metrics, 'enstrophy', NaN)];
        analysis.max_omega_history = [resolve_metric_field(initial_metrics, 'max_vorticity', NaN); resolve_metric_field(final_metrics, 'max_vorticity', NaN)];
        analysis = callbacks.finalize_analysis(analysis, State, cfg, Parameters, ctx);

        memory_end = query_memory_mb();
        internal = build_internal_record(State, cfg, analysis, initial_state, initial_metrics, final_metrics);
        record = build_run_record(Run_Config, Parameters, stage_spec, internal, ...
            cputime - cpu_start, toc(wall_start), max(memory_start, memory_end), max_cfl, ...
            nan_inf_flag, blow_up_flag);
    catch ME
        memory_end = query_memory_mb();
        record = build_failed_run_record(Run_Config, Parameters, stage_spec, ME, ...
            cputime - cpu_start, toc(wall_start), max(memory_start, memory_end));
        internal.status = 'failed';
        internal.error_message = ME.message;
        internal.error_identifier = ME.identifier;
        internal.final_field = [];
        internal.grid = struct();
    end
end

function internal = build_internal_record(State, cfg, analysis, initial_state, initial_metrics, final_metrics)
    grid = resolve_grid_from_state(State, analysis, cfg);
    final_field = resolve_state_primary_field(State);
    initial_field = resolve_state_primary_field(initial_state);
    internal = empty_internal_record();
    internal.status = 'completed';
    internal.grid = grid;
    internal.cfg = strip_runtime_only_fields(cfg);
    internal.analysis = analysis;
    internal.initial_field = initial_field;
    internal.final_field = final_field;
    internal.initial_metrics = initial_metrics;
    internal.final_metrics = final_metrics;
    internal.final_time = double(State.t);
    internal.frequency_metadata = pick_field(analysis, 'frequency_metadata', struct());
    internal.omega_hat = pick_field(State, 'omega_hat', []);
    internal.psi_hat = pick_field(State, 'psi_hat', []);
end

function record = build_run_record(Run_Config, Parameters, stage_spec, internal, cpu_time_s, wall_time_s, memory_peak_mb, max_cfl, nan_inf_flag, blow_up_flag)
    analysis = internal.analysis;
    grid = internal.grid;
    method_type = resolve_method_type(Run_Config.method);
    final_field = internal.final_field;
    initial_field = internal.initial_field;
    circulation_initial = compute_field_integral(initial_field, grid);
    circulation_final = compute_field_integral(final_field, grid);
    circulation_scale = max([ ...
        abs(circulation_initial), ...
        abs(circulation_final), ...
        compute_field_integral(abs(initial_field), grid), ...
        compute_field_integral(abs(final_field), grid), ...
        1.0e-12]);
    conservation_drift = abs(circulation_final - circulation_initial) / circulation_scale;

    record = empty_run_record();
    record.method = method_type;
    record.study_stage = char(string(stage_spec.stage_name));
    record.refinement_axis = char(string(stage_spec.refinement_axis));
    record.h = max([grid.dx, grid.dy]);
    record.dt = double(Parameters.dt);
    record.mode_count = resolve_mode_count(method_type, internal);
    record.polynomial_order = NaN;
    record.Nx = grid.Nx;
    record.Ny = grid.Ny;
    record.Nz = resolve_positive_scalar(Parameters, {'Nz'}, NaN);
    record.dof = resolve_dof(method_type, analysis, grid, record.mode_count);
    record.cells = resolve_cells(method_type, grid, record.Nz);
    record.modes = record.mode_count;
    record.reference_strategy = '';
    record.error_L1 = NaN;
    record.error_L2 = NaN;
    record.error_Linf = NaN;
    record.relative_change = NaN;
    record.observed_rate = NaN;
    record.runtime_wall_s = wall_time_s;
    record.runtime_cpu_s = cpu_time_s;
    record.memory_peak_mb = memory_peak_mb;
    record.iterations = 0;
    record.cfl = max_cfl;
    record.nan_inf_flag = logical(nan_inf_flag);
    record.stability_flags = struct( ...
        'runtime_error', false, ...
        'nan_inf', logical(nan_inf_flag), ...
        'blow_up', logical(blow_up_flag), ...
        'cfl_exceeded', logical(isfinite(max_cfl) && max_cfl > 1.0), ...
        'grid_valid', true);
    record.conservation_drift = conservation_drift;
    record.aliasing_indicator = compute_aliasing_indicator(method_type, internal);
    record.smoothness_indicator = compute_smoothness_indicator(final_field, grid);
    record.convergence_verdict = 'plateaued';
    record.stop_reason = '';
end

function record = build_failed_run_record(Run_Config, Parameters, stage_spec, ME, cpu_time_s, wall_time_s, memory_peak_mb)
    method_type = resolve_method_type(Run_Config.method);
    record = empty_run_record();
    record.method = method_type;
    record.study_stage = char(string(stage_spec.stage_name));
    record.refinement_axis = char(string(stage_spec.refinement_axis));
    record.h = NaN;
    record.dt = double(pick_field(Parameters, 'dt', NaN));
    record.mode_count = NaN;
    record.polynomial_order = NaN;
    record.Nx = double(pick_field(Parameters, 'Nx', NaN));
    record.Ny = double(pick_field(Parameters, 'Ny', NaN));
    record.Nz = double(pick_field(Parameters, 'Nz', NaN));
    record.dof = NaN;
    record.cells = NaN;
    record.modes = NaN;
    record.reference_strategy = '';
    record.error_L1 = NaN;
    record.error_L2 = NaN;
    record.error_Linf = NaN;
    record.relative_change = NaN;
    record.observed_rate = NaN;
    record.runtime_wall_s = wall_time_s;
    record.runtime_cpu_s = cpu_time_s;
    record.memory_peak_mb = memory_peak_mb;
    record.iterations = 0;
    record.cfl = NaN;
    record.nan_inf_flag = true;
    record.stability_flags = struct( ...
        'runtime_error', true, ...
        'runtime_error_id', char(string(ME.identifier)), ...
        'nan_inf', true, ...
        'blow_up', false, ...
        'cfl_exceeded', false, ...
        'grid_valid', true);
    record.conservation_drift = NaN;
    record.aliasing_indicator = NaN;
    record.smoothness_indicator = NaN;
    record.convergence_verdict = 'unstable';
    record.stop_reason = sprintf('runtime_error:%s', char(string(ME.identifier)));
end

function [reference_info, reference_warning] = resolve_stage_reference(Run_Config, Parameters, callbacks, spec, stage_spec, level_results)
    reference_warning = '';
    reference_info = struct('strategy', '', 'warning', '', 'internal', struct(), 'label', '');

    valid_indices = find(arrayfun(@(s) strcmp(s.internal.status, 'completed'), level_results));
    if isempty(valid_indices)
        reference_info.strategy = 'insufficient_reference';
        reference_info.warning = 'No successful levels were available to construct a reference.';
        reference_warning = reference_info.warning;
        return;
    end

    if strcmp(stage_spec.reference_strategy, 'analytic_manufactured') && ...
            ~isempty(spec.case_model) && isfield(spec.case_model, 'exact_omega')
        reference_info.strategy = 'analytic_manufactured';
        reference_info.label = 'analytical_manufactured_solution';
        if strcmp(stage_spec.refinement_axis, 'mode_count')
            ref_params = build_reference_parameters(Parameters, spec, stage_spec);
            ref_params = apply_verification_case_if_present(ref_params, spec.case_model, spec.method_type);
            [Xref, Yref] = build_initial_condition_grid(ref_params, spec.method_type);
            ref_grid = struct('X', Xref, 'Y', Yref, 'Nx', ref_params.Nx, 'Ny', ref_params.Ny, ...
                'dx', ref_params.Lx / ref_params.Nx, 'dy', ref_params.Ly / ref_params.Ny);
            reference_info.internal = struct( ...
                'grid', ref_grid, ...
                'final_field', spec.case_model.exact_omega(Xref, Yref, ref_params.Tfinal, ref_params), ...
                'final_time', ref_params.Tfinal);
        end
        return;
    end

    if strcmp(stage_spec.reference_strategy, 'over_resolved_numerical')
        ref_params = build_reference_parameters(Parameters, spec, stage_spec);
        ref_params = apply_verification_case_if_present(ref_params, spec.case_model, spec.method_type);
        [~, ref_internal] = execute_level(Run_Config, ref_params, callbacks, stage_spec);
        if strcmp(ref_internal.status, 'completed')
            reference_info.strategy = 'over_resolved_numerical';
            reference_info.label = 'over_resolved_numerical_reference';
            reference_info.internal = sanitize_internal_for_reference(ref_internal);
            return;
        end
        reference_warning = sprintf('Over-resolved reference failed (%s); falling back to finest self-reference.', ...
            char(string(ref_internal.error_identifier)));
    end

    finest_idx = valid_indices(end);
    reference_info.strategy = 'finest_self_reference';
    reference_info.label = 'finest_self_reference';
    reference_info.internal = sanitize_internal_for_reference(level_results(finest_idx).internal);
    if ~isempty(reference_warning)
        reference_info.warning = reference_warning;
    else
        reference_info.warning = 'Reference derived from the finest successful level; treat absolute errors with caution.';
        reference_warning = reference_info.warning;
    end
end

function ref_params = build_reference_parameters(Parameters, spec, stage_spec)
    ref_params = Parameters;
    multiplier = max(1.25, double(spec.reference_preferences.reference_multiplier));

    switch stage_spec.stage_name
        case 'temporal'
            ref_params.Nx = max(stage_spec.temporal_fine_n, round(stage_spec.max_primary_n * multiplier));
            ref_params.Ny = ref_params.Nx;
            ref_params.dt = min(stage_spec.values) / multiplier;
        otherwise
            ref_params.Nx = max(stage_spec.max_primary_n, round(stage_spec.values(end) * multiplier));
            ref_params.Ny = ref_params.Nx;
            ref_params.dt = min(spec.temporal_levels) / multiplier;
    end

    if strcmp(spec.method_type, 'fv')
        ref_params.Nz = max(1, round(pick_field(spec.primary_cfg, 'nz_fixed', pick_field(Parameters, 'Nz', 8))));
    end
    ref_params.snap_times = [0, ref_params.Tfinal];
    ref_params.num_snapshots = 2;
end

function [stage_records, stage_summary] = finalize_stage_records(stage_spec, spec, level_results, reference_info, reference_warning)
    stage_records = repmat(empty_run_record(), numel(level_results), 1);
    finite_error = nan(numel(level_results), 1);
    observed_rate = nan(numel(level_results), 1);

    for i = 1:numel(level_results)
        record = level_results(i).record;
        internal = level_results(i).internal;
        record.reference_strategy = char(string(reference_info.strategy));

        if strcmp(internal.status, 'completed')
            [errL1, errL2, errLinf] = compare_level_to_reference(level_results(i), reference_info, spec.case_model, stage_spec);
            record.error_L1 = errL1;
            record.error_L2 = errL2;
            record.error_Linf = errLinf;
            finite_error(i) = errL2;
        end

        if i > 1
            if ~(isfinite(record.relative_change) && record.relative_change > 0)
                record.relative_change = compute_successive_relative_change(level_results(i - 1).internal, internal);
            end
            observed_rate(i) = compute_observed_rate( ...
                stage_records(i - 1), record, stage_spec.refinement_axis);
            record.observed_rate = observed_rate(i);
        end
        stage_records(i) = record;
    end

    [verdict, stop_reason, monotone_ok, plateau_detected, warnings, first_converged_idx] = ...
        decide_stage_verdict(stage_records, stage_spec, spec, reference_info);
    if ~isempty(reference_warning)
        warnings{end + 1} = reference_warning; %#ok<AGROW>
    end
    for i = 1:numel(stage_records)
        [record_verdict, record_reason] = derive_record_convergence_verdict( ...
            stage_records(i), i, first_converged_idx, numel(stage_records), verdict, stop_reason, stage_spec.tolerance);
        stage_records(i).convergence_verdict = record_verdict;
        stage_records(i).stop_reason = record_reason;
    end

    stage_summary = empty_stage_summary();
    stage_summary.stage_name = stage_spec.stage_name;
    stage_summary.refinement_axis = stage_spec.refinement_axis;
    stage_summary.reference_strategy = char(string(reference_info.strategy));
    stage_summary.reference_warning = reference_warning;
    stage_summary.verdict = verdict;
    stage_summary.stop_reason = stop_reason;
    stage_summary.expected_order = stage_spec.expected_order;
    stage_summary.observed_order_last = last_finite(observed_rate);
    stage_summary.monotone_error_reduction = monotone_ok;
    stage_summary.plateau_detected = plateau_detected;
    stage_summary.final_error_L2 = last_finite(finite_error);
    stage_summary.final_relative_change = last_finite([stage_records.relative_change]);
    stage_summary.runtime_wall_s = nansum([stage_records.runtime_wall_s]);
    stage_summary.warning_messages = warnings;
    stage_summary.stable = ~strcmp(verdict, 'unstable');
end

function [errL1, errL2, errLinf] = compare_level_to_reference(level_result, reference_info, case_model, stage_spec)
    errL1 = NaN;
    errL2 = NaN;
    errLinf = NaN;
    internal = level_result.internal;
    if ~strcmp(internal.status, 'completed')
        return;
    end

    target = internal.final_field;
    if strcmp(reference_info.strategy, 'analytic_manufactured')
        if isempty(case_model) || ~isfield(case_model, 'exact_omega')
            return;
        end
        if isfield(reference_info, 'internal') && isstruct(reference_info.internal) && ...
                isfield(reference_info.internal, 'final_field') && ~isempty(reference_info.internal.final_field)
            ref_internal = reference_info.internal;
            target_on_ref = remap_field(target, internal.grid, ref_internal.grid);
            diff_field = double(target_on_ref) - double(ref_internal.final_field);
            errL1 = mean(abs(diff_field(:)));
            errL2 = sqrt(mean(diff_field(:).^2));
            errLinf = max(abs(diff_field(:)));
            return;
        else
            ref_field = case_model.exact_omega(internal.grid.X, internal.grid.Y, internal.final_time, internal.cfg);
        end
    else
        ref_internal = reference_info.internal;
        if isempty(ref_internal) || ~isfield(ref_internal, 'final_field') || isempty(ref_internal.final_field)
            return;
        end
        if isequal(size(ref_internal.final_field), size(target)) && isequal(ref_internal.grid.X, internal.grid.X)
            ref_field = ref_internal.final_field;
        else
            ref_field = remap_field(ref_internal.final_field, ref_internal.grid, internal.grid);
        end
    end

    diff_field = double(target) - double(ref_field);
    errL1 = mean(abs(diff_field(:)));
    errL2 = sqrt(mean(diff_field(:).^2));
    errLinf = max(abs(diff_field(:)));

    if strcmp(stage_spec.refinement_axis, 'mode_count')
        errL2 = max(errL2, 0);
    end
end

function mapped = remap_field(field_in, source_grid, target_grid)
    x_src = double(source_grid.X(1, :));
    y_src = double(source_grid.Y(:, 1));
    x_tgt = double(target_grid.X(1, :));
    y_tgt = double(target_grid.Y(:, 1));
    [Xq, Yq] = meshgrid(x_tgt, y_tgt);
    mapped = interp2(x_src, y_src, double(field_in), Xq, Yq, 'linear');
    if any(~isfinite(mapped(:)))
        fallback = interp2(x_src, y_src, double(field_in), Xq, Yq, 'nearest');
        mapped(~isfinite(mapped)) = fallback(~isfinite(mapped));
    end
end

function rel = compute_successive_relative_change(prev_internal, curr_internal)
    rel = NaN;
    if ~strcmp(prev_internal.status, 'completed') || ~strcmp(curr_internal.status, 'completed')
        return;
    end
    prev_on_curr = remap_field(prev_internal.final_field, prev_internal.grid, curr_internal.grid);
    delta = double(curr_internal.final_field) - double(prev_on_curr);
    rel = sqrt(mean(delta(:).^2)) / max(sqrt(mean(double(curr_internal.final_field(:)).^2)), 1.0e-12);
end

function rate = compute_observed_rate(prev_record, curr_record, axis_name)
    rate = NaN;
    e1 = prev_record.error_L2;
    e2 = curr_record.error_L2;
    if ~(isfinite(e1) && isfinite(e2) && e1 > 0 && e2 > 0)
        return;
    end

    switch char(string(axis_name))
        case {'dt', 'h'}
            a1 = prev_record.(axis_name);
            a2 = curr_record.(axis_name);
            if isfinite(a1) && isfinite(a2) && a1 > 0 && a2 > 0 && a1 ~= a2
                rate = log(e1 / e2) / log(a1 / a2);
            end
        case 'mode_count'
            a1 = prev_record.mode_count;
            a2 = curr_record.mode_count;
            if isfinite(a1) && isfinite(a2) && a1 > 0 && a2 > 0 && a1 ~= a2
                rate = log(e1 / e2) / log(a2 / a1);
            end
    end
end

function [verdict, stop_reason, monotone_ok, plateau_detected, warnings, first_converged_idx] = decide_stage_verdict(stage_records, stage_spec, spec, reference_info)
    warnings = {};
    verdict = 'plateaued';
    stop_reason = 'insufficient_improvement';
    first_converged_idx = [];
    stable_flags = arrayfun(@(r) ~logical(r.nan_inf_flag) && ~logical(pick_nested_logical(r.stability_flags, {'runtime_error'}, false)), stage_records);
    if any(~stable_flags)
        verdict = 'unstable';
        stop_reason = 'nan_inf_or_runtime_error';
        monotone_ok = false;
        plateau_detected = false;
        return;
    end

    changes = [stage_records.relative_change];
    valid_change_idx = find(isfinite(changes) & changes >= 0);
    if isempty(valid_change_idx)
        verdict = 'plateaued';
        stop_reason = 'missing_successive_vorticity_metric';
        monotone_ok = false;
        plateau_detected = false;
        return;
    end

    valid_changes = changes(valid_change_idx);
    monotone_ok = true;
    for i = 2:numel(valid_changes)
        prev = valid_changes(i - 1);
        curr = valid_changes(i);
        if curr > prev * (1 + spec.verdict.monotonicity_relaxation)
            monotone_ok = false;
            break;
        end
    end

    if ~monotone_ok
        warnings{end + 1} = sprintf([ ...
            'Successive vorticity error increased under refinement beyond the %.1f%% relaxation margin.'], ...
            100 * spec.verdict.monotonicity_relaxation); %#ok<AGROW>
    end

    first_converged_idx = find(isfinite(changes) & changes <= stage_spec.tolerance, 1, 'first');
    plateau_detected = numel(valid_changes) >= 2 && ...
        valid_changes(end) >= valid_changes(end - 1) / spec.verdict.minimum_improvement_factor;

    if ~isempty(first_converged_idx)
        verdict = 'converged';
        stop_reason = 'successive_vorticity_threshold_met';
    elseif plateau_detected
        verdict = 'plateaued';
        stop_reason = 'successive_vorticity_plateau';
    else
        verdict = 'plateaued';
        stop_reason = 'successive_vorticity_tolerance_not_met';
    end
end

function [record_verdict, record_reason] = derive_record_convergence_verdict(record, record_index, first_converged_idx, total_records, stage_verdict, stage_reason, tolerance)
    if logical(record.nan_inf_flag) || logical(pick_nested_logical(record.stability_flags, {'runtime_error'}, false))
        record_verdict = 'unstable';
        record_reason = 'nan_inf_or_runtime_error';
        return;
    end
    if record_index == 1 || ~isfinite(record.relative_change)
        record_verdict = 'baseline';
        record_reason = 'baseline_unpaired';
        return;
    end
    if isfinite(record.relative_change) && record.relative_change <= tolerance
        record_verdict = 'converged';
        record_reason = 'successive_vorticity_threshold_met';
        return;
    end
    if ~isempty(first_converged_idx) && record_index < first_converged_idx
        record_verdict = 'refining';
        record_reason = 'successive_vorticity_above_tolerance';
        return;
    end
    if record_index == total_records
        record_verdict = stage_verdict;
        record_reason = stage_reason;
        return;
    end
    record_verdict = 'refining';
    record_reason = 'successive_vorticity_above_tolerance';
end

function Results = assemble_results(Run_Config, Parameters, dispatch_info, spec, stage_summaries, run_records, total_time)
    Results = struct();
    Results.study_id = Run_Config.study_id;
    Results.method = Run_Config.method;
    Results.method_type = spec.method_type;
    Results.case_id = spec.case_id;
    Results.convergence_dispatch_strategy = dispatch_info.strategy;
    Results.convergence_objective_mode = dispatch_info.objective_mode;
    Results.total_time = total_time;
    Results.criterion_type = spec.criterion_type;
    Results.tolerance = spec.tolerance;
    Results.stage_summaries = stage_summaries;
    Results.run_records = run_records;
    Results.reference_case_description = spec.metadata.reference_case_description;
    Results.summary = struct( ...
        'overall_verdict', aggregate_stage_verdicts(stage_summaries), ...
        'primary_refinement_axis', spec.primary_axis, ...
        'stage_order', {cellstr(string({stage_summaries.stage_name}))}, ...
        'warnings', {collect_stage_warnings(stage_summaries)});

    primary_idx = find(~strcmp({stage_summaries.stage_name}, 'temporal'), 1, 'first');
    if isempty(primary_idx)
        primary_idx = numel(stage_summaries);
    end
    primary_stage_name = stage_summaries(primary_idx).stage_name;
    primary_records = run_records(strcmp({run_records.study_stage}, primary_stage_name));

    Results.refinement_axis = stage_summaries(primary_idx).refinement_axis;
    Results.convergence_order = stage_summaries(primary_idx).observed_order_last;
    Results.convergence_variable = 'relative_change';
    Results.level_labels = build_level_labels(primary_records, stage_summaries(primary_idx).refinement_axis);
    Results.Nx_values = column_or_empty(primary_records, 'Nx');
    Results.Ny_values = column_or_empty(primary_records, 'Ny');
    Results.wall_times = column_or_empty(primary_records, 'runtime_wall_s');
    Results.QoI_values = column_or_empty(primary_records, 'relative_change');
    Results.mesh_sizes = column_or_empty(primary_records, 'Nx');
    Results.h_values = column_or_empty(primary_records, 'h');
    Results.mode_count_values = column_or_empty(primary_records, 'mode_count');
    Results.verdict = Results.summary.overall_verdict;
end

function verdict = aggregate_stage_verdicts(stage_summaries)
    verdict = 'converged';
    if isempty(stage_summaries)
        verdict = 'insufficient_reference';
        return;
    end
    severity = containers.Map( ...
        {'converged', 'plateaued', 'insufficient_reference', 'nonmonotone', 'unstable'}, ...
        [1, 2, 3, 4, 5]);
    current_rank = 1;
    for i = 1:numel(stage_summaries)
        key = char(string(stage_summaries(i).verdict));
        if ~isKey(severity, key)
            continue;
        end
        if severity(key) > current_rank
            verdict = key;
            current_rank = severity(key);
        end
    end
end

function persist_convergence_results(Results, paths)
    safe_save_mat(fullfile(paths.data, 'convergence_results.mat'), struct('Results', Results), '-v7.3');
    json_text = encode_json_pretty(Results);
    fid = fopen(fullfile(paths.data, 'convergence_results.json'), 'w');
    cleaner = onCleanup(@() fclose(fid));
    fprintf(fid, '%s', json_text);
    clear cleaner
end

function persist_stage_artifacts(stage_internals, paths)
    if isempty(stage_internals)
        return;
    end
    safe_save_mat(fullfile(paths.data, 'convergence_stage_artifacts.mat'), ...
        struct('stage_internals', {stage_internals}), '-v7.3');
end

function generate_convergence_figures(Results, stage_internals, Run_Config, paths, Settings)
    for i = 1:numel(stage_internals)
        packet = stage_internals{i};
        if ~isstruct(packet) || ~isfield(packet, 'stage_summary')
            continue;
        end
        stage_summary = packet.stage_summary;
        stage_name = char(string(stage_summary.stage_name));
        records = Results.run_records(strcmp({Results.run_records.study_stage}, stage_name));
        if isempty(records)
            continue;
        end

        fig = figure('Visible', 'off', 'Position', [100, 100, 1180, 760]);
        apply_dark_theme_for_figure(fig);

        axis_values = extract_stage_axis_values(records, stage_summary.refinement_axis);
        successive_error = column_or_empty(records, 'relative_change');
        dof = column_or_empty(records, 'dof');
        wall_times = column_or_empty(records, 'runtime_wall_s');
        rates = column_or_empty(records, 'observed_rate');

        subplot(2, 2, 1);
        plot_stage_error_curve(axis_values, successive_error, stage_summary.refinement_axis);
        if isfinite(Results.tolerance) && Results.tolerance > 0
            yline(stage_summary.runtime_wall_s * 0 + Results.tolerance, '--', ...
                sprintf('tol=%.2f%%', 100 * Results.tolerance), ...
                'Color', [0.95, 0.78, 0.22], 'LineWidth', 1.1);
        end
        title(sprintf('%s: successive vorticity error vs %s', stage_name, stage_summary.refinement_axis), 'Interpreter', 'none');

        subplot(2, 2, 2);
        valid = isfinite(successive_error) & isfinite(wall_times) & successive_error > 0 & wall_times >= 0;
        if any(valid)
            loglog(successive_error(valid), wall_times(valid), 'o-', 'LineWidth', 1.8, 'MarkerSize', 7);
        end
        grid on;
        xlabel('Successive vorticity error');
        ylabel('Wall time (s)');
        title('Runtime vs successive error');

        subplot(2, 2, 3);
        valid = isfinite(successive_error) & isfinite(dof) & successive_error > 0 & dof > 0;
        if any(valid)
            loglog(dof(valid), successive_error(valid), 's-', 'LineWidth', 1.8, 'MarkerSize', 7);
        end
        grid on;
        xlabel('DOF');
        ylabel('Successive vorticity error');
        title('DOF vs successive error');

        subplot(2, 2, 4);
        idx = find(isfinite(rates));
        if ~isempty(idx)
            plot(idx, rates(idx), 'd-', 'LineWidth', 1.8, 'MarkerSize', 7);
        end
        grid on;
        xlabel('Level');
        ylabel('Observed rate');
        title(sprintf('Verdict: %s', stage_summary.verdict), 'Interpreter', 'none');

        sgtitle(sprintf('Convergence Study | %s | %s', Run_Config.method, stage_name), 'Interpreter', 'none');
        ResultsPlotDispatcher.save_figure_bundle(fig, ...
            fullfile(paths.figures_convergence, sprintf('stage_%s_summary', stage_name)), Settings);
        close(fig);

        generate_local_indicator_figure(packet, stage_name, paths, Settings);
    end

    generate_compatibility_summary_figure(Results, Run_Config, paths, Settings);
end

function generate_local_indicator_figure(packet, stage_name, paths, Settings)
    ref_info = pick_field(packet, 'reference_info', struct());
    if ~isstruct(ref_info) || ~isfield(ref_info, 'internal') || isempty(ref_info.internal) || ...
            ~isstruct(ref_info.internal) || ~isfield(ref_info.internal, 'final_field') || ...
            isempty(ref_info.internal.final_field) || ~isfield(ref_info.internal, 'grid') || ...
            isempty(ref_info.internal.grid)
        return;
    end
    levels = pick_field(packet, 'level_results', []);
    if isempty(levels)
        return;
    end
    target = levels(end).internal;
    if ~strcmp(target.status, 'completed')
        return;
    end

    if isequal(size(ref_info.internal.final_field), size(target.final_field)) && ...
            isequal(ref_info.internal.grid.X, target.grid.X)
        ref_field = ref_info.internal.final_field;
    else
        ref_field = remap_field(ref_info.internal.final_field, ref_info.internal.grid, target.grid);
    end
    local_indicator = abs(double(target.final_field) - double(ref_field));

    fig = figure('Visible', 'off', 'Position', [150, 150, 680, 560]);
    apply_dark_theme_for_figure(fig);
    imagesc(double(target.grid.X(1, :)), double(target.grid.Y(:, 1)), local_indicator);
    axis image tight;
    colorbar;
    xlabel('x');
    ylabel('y');
    title(sprintf('Local error indicator | %s', stage_name), 'Interpreter', 'none');
    ResultsPlotDispatcher.save_figure_bundle(fig, ...
        fullfile(paths.figures_convergence, sprintf('stage_%s_local_indicator', stage_name)), Settings);
    close(fig);
end

function generate_compatibility_summary_figure(Results, Run_Config, paths, Settings)
    fig = figure('Visible', 'off', 'Position', [120, 120, 1000, 420]);
    apply_dark_theme_for_figure(fig);

    stage_names = {Results.stage_summaries.stage_name};
    final_errors = [Results.stage_summaries.final_relative_change];
    runtime_stage = [Results.stage_summaries.runtime_wall_s];

    subplot(1, 2, 1);
    bar(categorical(stage_names), final_errors);
    ylabel('Final successive vorticity error');
    title(sprintf('%s convergence summary', Run_Config.method), 'Interpreter', 'none');
    grid on;

    subplot(1, 2, 2);
    bar(categorical(stage_names), runtime_stage);
    ylabel('Wall time (s)');
    title(sprintf('Overall verdict: %s', Results.summary.overall_verdict), 'Interpreter', 'none');
    grid on;

    ResultsPlotDispatcher.save_figure_bundle(fig, fullfile(paths.figures_convergence, 'convergence_plot'), Settings);
    close(fig);
end

function report_paths = generate_convergence_reports(Results, stage_internals, Run_Config, paths)
    report_paths = struct('markdown', '', 'audit_markdown', '');

    report_path = fullfile(paths.reports, 'convergence_report.md');
    fid = fopen(report_path, 'w');
    cleaner = onCleanup(@() fclose(fid));
    fprintf(fid, '# Method-Aware Convergence Report\n\n');
    fprintf(fid, '- Study ID: `%s`\n', Results.study_id);
    fprintf(fid, '- Method: `%s`\n', Run_Config.method);
    fprintf(fid, '- Overall verdict: `%s`\n', Results.summary.overall_verdict);
    fprintf(fid, '- Primary refinement axis: `%s`\n\n', Results.summary.primary_refinement_axis);

    fprintf(fid, '## Stage Verdicts\n\n');
    for i = 1:numel(Results.stage_summaries)
        s = Results.stage_summaries(i);
        fprintf(fid, '- `%s`: verdict=`%s`, axis=`%s`, reference=`%s`, final successive error=`%.6e`, observed rate=`%.4f`\n', ...
            s.stage_name, s.verdict, s.refinement_axis, s.reference_strategy, ...
            s.final_relative_change, s.observed_order_last);
        if ~isempty(s.warning_messages)
            for j = 1:numel(s.warning_messages)
                fprintf(fid, '  - warning: %s\n', s.warning_messages{j});
            end
        end
    end

    fprintf(fid, '\n## Flat Run Records\n\n');
    fprintf(fid, '| Stage | Axis | Nx | Ny | dt | h | modes | successive error | rate | wall s | verdict |\n');
    fprintf(fid, '|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|\n');
    for i = 1:numel(Results.run_records)
        r = Results.run_records(i);
        fprintf(fid, '| %s | %s | %d | %d | %.4e | %.4e | %.0f | %.4e | %.4f | %.3f | %s |\n', ...
            r.study_stage, r.refinement_axis, round(r.Nx), round(r.Ny), ...
            r.dt, r.h, r.mode_count, r.relative_change, r.observed_rate, r.runtime_wall_s, r.convergence_verdict);
    end
    clear cleaner

    audit_path = fullfile(paths.reports, 'convergence_audit_note.md');
    fid = fopen(audit_path, 'w');
    cleaner = onCleanup(@() fclose(fid));
    fprintf(fid, '# Phase-1 Audit Note\n\n');
    fprintf(fid, '## What was wrong or ambiguous\n\n');
    fprintf(fid, '- The legacy convergence runtime reduced all methods to a single refinement sweep and did not isolate temporal from spatial/modal error.\n');
    fprintf(fid, '- Finite Volume convergence was dispatcher-blocked, and spectral convergence was limited to periodic explicit-k sweeps.\n');
    fprintf(fid, '- Accuracy, stability, and computational metrics were not captured in a method-aware flat results schema.\n\n');
    fprintf(fid, '## What changed\n\n');
    fprintf(fid, '- Added staged temporal-then-primary convergence studies.\n');
    fprintf(fid, '- Enabled FV convergence dispatch and transform-family spectral convergence.\n');
    fprintf(fid, '- Added flat run records with error norms, rates, runtime, memory, CFL, conservation drift, aliasing, and verdict fields.\n');
    fprintf(fid, '- Added machine-readable `.mat` and `.json` outputs plus reproducible plots.\n\n');
    fprintf(fid, '## Assumptions\n\n');
    fprintf(fid, '- Phase 1 targets the active transform-family spectral solver only; `polynomial_order` remains reserved for future spectral-element work.\n');
    fprintf(fid, '- FV convergence refines horizontal resolution while keeping `Nz` fixed unless an explicit future ladder is supplied.\n');
    fprintf(fid, '- Manufactured exact solutions are used when available; otherwise the runtime falls back to over-resolved or finest-level self-reference with an explicit warning.\n\n');
    fprintf(fid, '## Remaining uncertainties\n\n');
    fprintf(fid, '- Bounded FD wall studies still depend on over-resolved numerical references unless the chosen wall case has a clean closed-form solution on the active grid.\n');
    fprintf(fid, '- Trusted benchmark ingestion is schema-ready but still relies on local benchmark payload wiring if external reference datasets are introduced later.\n');
    clear cleaner

    report_paths.markdown = report_path;
    report_paths.audit_markdown = audit_path;
end

function generate_agent_compatibility_figure(Results, Run_Config, paths, Settings)
    fig = figure('Visible', 'off', 'Position', [120, 120, 1000, 420]);
    apply_dark_theme_for_figure(fig);

    subplot(1, 2, 1);
    loglog(Results.h_values, abs(Results.QoI_values) + eps, 'o-', 'LineWidth', 1.8, 'MarkerSize', 7);
    grid on;
    xlabel('Grid spacing h');
    ylabel('Agent metric');
    title(sprintf('%s agent-guided trace', Run_Config.method), 'Interpreter', 'none');

    subplot(1, 2, 2);
    plot(Results.Nx_values, Results.wall_times, 's-', 'LineWidth', 1.8, 'MarkerSize', 7);
    grid on;
    xlabel('Resolution');
    ylabel('Wall time (s)');
    title('Computational cost');

    ResultsPlotDispatcher.save_figure_bundle(fig, fullfile(paths.figures_convergence, 'convergence_plot'), Settings);
    close(fig);
end

function audit_paths = generate_agent_compatibility_report(Results, Run_Config, paths)
    audit_paths = struct('markdown', '', 'audit_markdown', '');
    report_path = fullfile(paths.reports, 'convergence_report.md');
    fid = fopen(report_path, 'w');
    cleaner = onCleanup(@() fclose(fid));
    fprintf(fid, '# Agent-Guided Convergence Report\n\n');
    fprintf(fid, '- Study ID: `%s`\n', Results.study_id);
    fprintf(fid, '- Method: `%s`\n', Run_Config.method);
    fprintf(fid, '- Strategy: `agent_guided`\n');
    fprintf(fid, '- Objective mode: `%s`\n', Results.convergence_objective_mode);
    fprintf(fid, '- Converged N: `%.0f`\n', Results.converged_N);
    clear cleaner
    audit_paths.markdown = report_path;
    audit_paths.audit_markdown = report_path;
end

function callbacks = resolve_method_callbacks(method_name)
    switch lower(char(string(method_name)))
        case 'fd'
            callbacks = FiniteDifferenceMethod('callbacks');
        case {'spectral', 'fft'}
            callbacks = SpectralMethod('callbacks');
        case {'fv', 'finitevolume', 'finite volume'}
            callbacks = FiniteVolumeMethod('callbacks');
        otherwise
            error('mode_convergence:UnknownMethod', ...
                'Unknown method "%s".', char(string(method_name)));
    end
end

function method_type = resolve_method_type(method_name)
    method_token = lower(strtrim(char(string(method_name))));
    switch method_token
        case {'fd', 'finite difference', 'finite_difference', 'finitedifference'}
            method_type = 'fd';
        case {'fv', 'finite volume', 'finite_volume', 'finitevolume'}
            method_type = 'fv';
        case {'spectral', 'fft', 'pseudo_spectral'}
            method_type = 'spectral';
        otherwise
            error('mode_convergence:UnknownMethodType', ...
                'Unknown convergence method token "%s".', method_token);
    end
end

function case_model = resolve_verification_case(method_type, case_id)
    case_model = struct([]);
    token = lower(strtrim(char(string(case_id))));
    token = strrep(token, '-', '_');
    token = strrep(token, ' ', '_');
    if isempty(token)
        return;
    end

    switch token
        case {'fd_periodic_exact', 'fd_smooth_periodic'}
            if ~strcmp(method_type, 'fd')
                return;
            end
            case_model = make_exact_periodic_case(token, 'periodic', 1, 2, 2.0, ...
                'FD periodic manufactured eigenmode with exact diffusion decay.');
            case_model.expected_orders = struct('temporal', 4.0, 'spatial', 2.0);
            case_model.recommended_primary_levels = [16, 32, 64];
            case_model.recommended_dt_values = [0.02, 0.01, 0.005];

        case {'fd_wall_bounded_smooth', 'fd_wall_aware'}
            if ~strcmp(method_type, 'fd')
                return;
            end
            case_model = struct();
            case_model.id = token;
            case_model.reference_mode = 'over_resolved';
            case_model.description = 'FD bounded-wall smooth case used to expose effective-order loss relative to the periodic nominal case.';
            case_model.bc_case = 'enclosed_cavity';
            case_model.initial_omega_builder = @(X, Y, params) exp(-6 * ((X ./ max(params.Lx, eps)).^2 + (Y ./ max(params.Ly, eps)).^2));
            case_model.recommended_primary_levels = [16, 32, 64];
            case_model.recommended_dt_values = [0.02, 0.01, 0.005];
            case_model.expected_orders = struct('temporal', 4.0, 'spatial', 2.0);

        case {'fv_periodic_exact', 'fv_smooth_periodic'}
            if ~strcmp(method_type, 'fv')
                return;
            end
            case_model = make_exact_periodic_case(token, 'periodic', 1, 2, 2.0, ...
                'FV periodic manufactured eigenmode with exact diffusion decay.');
            case_model.expected_orders = struct('temporal', 3.0, 'spatial', 2.0);
            case_model.recommended_primary_levels = [12, 24, 48];
            case_model.recommended_dt_values = [0.01, 0.005, 0.0025];

        case 'spectral_periodic_modal'
            if ~strcmp(method_type, 'spectral')
                return;
            end
            case_model = struct();
            case_model.id = token;
            case_model.reference_mode = 'over_resolved';
            case_model.description = 'Spectral periodic smooth modal case using an over-resolved smooth periodic reference to verify truncation convergence.';
            case_model.bc_case = 'periodic';
            case_model.initial_omega_builder = @(X, Y, params) smooth_periodic_modal_omega(X, Y, params);
            case_model.expected_orders = struct('temporal', 4.0, 'modal', NaN);
            case_model.recommended_primary_levels = [4, 8, 16, 32];
            case_model.recommended_dt_values = [0.02, 0.01, 0.005];

        case 'spectral_periodic_exact'
            if ~strcmp(method_type, 'spectral')
                return;
            end
            case_model = make_exact_periodic_case(token, 'periodic', 2, 3, 1.0, ...
                'Spectral periodic manufactured eigenmode with exact diffusion decay.');
            case_model.expected_orders = struct('temporal', 4.0, 'modal', NaN);
            case_model.recommended_primary_levels = [8, 16, 32, 64];
            case_model.recommended_dt_values = [0.02, 0.01, 0.005];

        case 'spectral_mixed_modal'
            if ~strcmp(method_type, 'spectral')
                return;
            end
            case_model = struct();
            case_model.id = token;
            case_model.reference_mode = 'over_resolved';
            case_model.description = 'Spectral transform-family mixed periodic/Dirichlet smooth modal case using an over-resolved smooth reference.';
            case_model.bc_case = 'user_defined';
            case_model.bc_left = 'Periodic';
            case_model.bc_right = 'Periodic';
            case_model.bc_top = 'Pinned (Dirichlet)';
            case_model.bc_bottom = 'Pinned (Dirichlet)';
            case_model.initial_omega_builder = @(X, Y, params) smooth_mixed_modal_omega(X, Y, params);
            case_model.expected_orders = struct('temporal', 4.0, 'modal', NaN);
            case_model.recommended_primary_levels = [4, 8, 16, 32];
            case_model.recommended_dt_values = [0.02, 0.01, 0.005];

        case 'spectral_transform_dirichlet'
            if ~strcmp(method_type, 'spectral')
                return;
            end
            case_model = struct();
            case_model.id = token;
            case_model.reference_mode = 'analytic';
            case_model.description = 'Spectral transform-family mixed periodic/Dirichlet eigenmode with exact diffusion decay.';
            case_model.bc_case = 'user_defined';
            case_model.bc_left = 'Periodic';
            case_model.bc_right = 'Periodic';
            case_model.bc_top = 'Pinned (Dirichlet)';
            case_model.bc_bottom = 'Pinned (Dirichlet)';
            case_model.initial_omega_builder = @(X, Y, params) spectral_mixed_exact_omega(X, Y, 0.0, params, 1, 2);
            case_model.exact_omega = @(X, Y, t, cfg) spectral_mixed_exact_omega(X, Y, t, cfg, 1, 2);
            case_model.expected_orders = struct('temporal', 4.0, 'modal', NaN);
            case_model.recommended_primary_levels = [8, 16, 32, 64];
            case_model.recommended_dt_values = [0.02, 0.01, 0.005];
    end
end

function case_model = make_exact_periodic_case(token, bc_case, m, n, amplitude, description)
    case_model = struct();
    case_model.id = token;
    case_model.reference_mode = 'analytic';
    case_model.description = description;
    case_model.bc_case = bc_case;
    case_model.initial_omega_builder = @(X, Y, params) periodic_exact_omega(X, Y, 0.0, params, m, n, amplitude);
    case_model.exact_omega = @(X, Y, t, cfg) periodic_exact_omega(X, Y, t, cfg, m, n, amplitude);
end

function omega = periodic_exact_omega(X, Y, t, cfg, m, n, amplitude)
    lambda = (2 * pi * m / cfg.Lx)^2 + (2 * pi * n / cfg.Ly)^2;
    psi = amplitude .* cos(2 * pi * m * X / cfg.Lx) .* cos(2 * pi * n * Y / cfg.Ly);
    omega = lambda .* psi .* exp(-cfg.nu * lambda * t);
end

function omega = spectral_mixed_exact_omega(X, Y, t, cfg, m, n)
    lambda = (2 * pi * m / cfg.Lx)^2 + (pi * n / cfg.Ly)^2;
    psi = cos(2 * pi * m * X / cfg.Lx) .* sin(pi * n * Y / cfg.Ly);
    omega = lambda .* psi .* exp(-cfg.nu * lambda * t);
end

function omega = smooth_periodic_modal_omega(X, Y, cfg)
    x_phase = 2 * pi * X / max(cfg.Lx, eps);
    y_phase = 2 * pi * Y / max(cfg.Ly, eps);
    omega = (exp(0.45 * cos(x_phase)) - besseli(0, 0.45)) .* ...
        (0.8 * exp(0.30 * cos(y_phase)) + 0.2 * sin(y_phase));
end

function omega = smooth_mixed_modal_omega(X, Y, cfg)
    x_phase = 2 * pi * X / max(cfg.Lx, eps);
    y_phase = pi * Y / max(cfg.Ly, eps);
    envelope = sin(y_phase) .* (1 + 0.15 * cos(y_phase));
    omega = envelope .* (exp(0.35 * cos(x_phase)) - besseli(0, 0.35));
end

function [X, Y] = build_initial_condition_grid(params, method_type)
    switch method_type
        case 'fd'
            dx = params.Lx / params.Nx;
            dy = params.Ly / params.Ny;
            x = linspace(-params.Lx / 2, params.Lx / 2 - dx, params.Nx);
            y = linspace(-params.Ly / 2, params.Ly / 2 - dy, params.Ny);
        case 'fv'
            dx = params.Lx / params.Nx;
            dy = params.Ly / params.Ny;
            x = linspace(0, params.Lx - dx, params.Nx);
            y = linspace(0, params.Ly - dy, params.Ny);
        case 'spectral'
            spectral_bc = BCDispatcher.resolve(params, 'spectral', build_spectral_grid_meta(params));
            x = build_spectral_axis_nodes(params.Nx, params.Lx, spectral_bc.method.spectral.axis_x.family);
            y = build_spectral_axis_nodes(params.Ny, params.Ly, spectral_bc.method.spectral.axis_y.family);
        otherwise
            error('mode_convergence:UnsupportedGridBuilder', ...
                'Unsupported method grid builder "%s".', method_type);
    end
    [X, Y] = meshgrid(x, y);
end

function nodes = build_spectral_axis_nodes(N, L, family)
    switch char(string(family))
        case 'fft'
            nodes = linspace(0, L - (L / max(N, 1)), N);
        case 'dst'
            nodes = (1:N) * (L / (N + 1));
        case 'dct'
            nodes = ((0:(N - 1)) + 0.5) * (L / N);
        otherwise
            error('mode_convergence:UnknownSpectralAxisFamily', ...
                'Unknown spectral axis family "%s".', char(string(family)));
    end
end

function grid = resolve_grid_from_state(State, analysis, cfg)
    grid = struct();
    grid.Nx = cfg.Nx;
    grid.Ny = cfg.Ny;
    grid.dx = cfg.dx;
    grid.dy = cfg.dy;
    if isfield(State, 'setup') && isfield(State.setup, 'X') && isfield(State.setup, 'Y')
        grid.X = gather_to_cpu(State.setup.X);
        grid.Y = gather_to_cpu(State.setup.Y);
    elseif isfield(analysis, 'x') && isfield(analysis, 'y')
        [grid.X, grid.Y] = meshgrid(double(analysis.x), double(analysis.y));
    else
        x = linspace(0, cfg.Lx - cfg.dx, cfg.Nx);
        y = linspace(0, cfg.Ly - cfg.dy, cfg.Ny);
        [grid.X, grid.Y] = meshgrid(x, y);
    end
end

function field = resolve_state_primary_field(State)
    if isfield(State, 'omega') && ~isempty(State.omega)
        field = gather_to_cpu(State.omega);
        return;
    end
    error('mode_convergence:MissingPrimaryField', ...
        'State must expose omega for convergence comparison.');
end

function field = resolve_state_auxiliary_field(State, field_name)
    if isfield(State, field_name) && ~isempty(State.(field_name))
        field = gather_to_cpu(State.(field_name));
    else
        field = zeros(size(resolve_state_primary_field(State)));
    end
end

function tf = any_nonfinite_state(State)
    tf = false;
    field_names = {'omega', 'psi', 'eta', 'h', 'hu', 'hv', 'u', 'v'};
    for i = 1:numel(field_names)
        key = field_names{i};
        if isfield(State, key) && ~isempty(State.(key))
            value = gather_to_cpu(State.(key));
            if any(~isfinite(value(:)))
                tf = true;
                return;
            end
        end
    end
end

function cfl_value = estimate_cfl_from_state(peak_speed, cfg, method_name)
    if ~(isfinite(peak_speed) && peak_speed >= 0)
        cfl_value = NaN;
        return;
    end
    h_min = min([cfg.dx, cfg.dy]);
    cfl_value = peak_speed * cfg.dt / max(h_min, eps);
    if strcmpi(char(string(method_name)), 'FV') && isfield(cfg, 'Lz') && isfield(cfg, 'Nz')
        dz = cfg.Lz / max(cfg.Nz, 1);
        cfl_value = peak_speed * cfg.dt / max(min([cfg.dx, cfg.dy, dz]), eps);
    end
end

function mode_count = resolve_mode_count(method_type, internal)
    switch method_type
        case 'spectral'
            mode_count = numel(internal.final_field);
        otherwise
            mode_count = NaN;
    end
end

function dof = resolve_dof(method_type, analysis, grid, mode_count)
    switch method_type
        case 'fd'
            dof = double(grid.Nx * grid.Ny);
        case 'fv'
            dof = double(pick_field(analysis, 'grid_points', grid.Nx * grid.Ny));
        case 'spectral'
            if isfinite(mode_count)
                dof = double(mode_count);
            else
                dof = double(grid.Nx * grid.Ny);
            end
        otherwise
            dof = NaN;
    end
end

function cells = resolve_cells(method_type, grid, Nz)
    switch method_type
        case 'fv'
            if isfinite(Nz)
                cells = double(grid.Nx * grid.Ny * Nz);
            else
                cells = double(grid.Nx * grid.Ny);
            end
        otherwise
            cells = double(grid.Nx * grid.Ny);
    end
end

function integral_value = compute_field_integral(field, grid)
    if isempty(field)
        integral_value = NaN;
        return;
    end
    integral_value = sum(double(field(:))) * grid.dx * grid.dy;
end

function alias_indicator = compute_aliasing_indicator(method_type, internal)
    alias_indicator = NaN;
    if ~strcmp(method_type, 'spectral') || ~isfield(internal, 'omega_hat') || isempty(internal.omega_hat)
        return;
    end
    omega_hat = double(internal.omega_hat);
    energy = abs(omega_hat).^2;
    if ~any(energy(:) > 0)
        alias_indicator = 0.0;
        return;
    end
    ny = size(energy, 1);
    nx = size(energy, 2);
    tail_x = false(1, nx);
    tail_y = false(ny, 1);
    tail_x(max(1, floor(2 * nx / 3)):end) = true;
    tail_y(max(1, floor(2 * ny / 3)):end) = true;
    tail_mask = logical(tail_y * tail_x);
    alias_indicator = sum(energy(tail_mask), 'all') / sum(energy(:), 'all');
end

function smoothness = compute_smoothness_indicator(field, grid)
    smoothness = NaN;
    if isempty(field) || any(size(field) < 2)
        return;
    end
    [gx, gy] = gradient(double(field), grid.dx, grid.dy);
    smoothness = sqrt(mean(gx(:).^2 + gy(:).^2)) / max(sqrt(mean(double(field(:)).^2)), 1.0e-12);
end

function stage_values = extract_stage_axis_values(records, axis_name)
    switch char(string(axis_name))
        case 'dt'
            stage_values = column_or_empty(records, 'dt');
        case 'h'
            stage_values = column_or_empty(records, 'h');
        case 'mode_count'
            stage_values = column_or_empty(records, 'mode_count');
        otherwise
            stage_values = nan(numel(records), 1);
    end
end

function plot_stage_error_curve(axis_values, errors, axis_name)
    valid = isfinite(axis_values) & isfinite(errors) & errors >= 0;
    switch char(string(axis_name))
        case {'dt', 'h'}
            if any(valid)
                loglog(axis_values(valid), max(errors(valid), eps), 'o-', 'LineWidth', 1.8, 'MarkerSize', 7);
            end
            xlabel(axis_name);
        case 'mode_count'
            if any(valid)
                semilogy(axis_values(valid), max(errors(valid), eps), 'o-', 'LineWidth', 1.8, 'MarkerSize', 7);
            end
            xlabel('Modes / N');
        otherwise
            if any(valid)
                plot(axis_values(valid), errors(valid), 'o-', 'LineWidth', 1.8, 'MarkerSize', 7);
            end
            xlabel('Refinement level');
    end
    grid on;
    ylabel('Successive vorticity error');
end

function labels = build_level_labels(records, axis_name)
    labels = cell(numel(records), 1);
    for i = 1:numel(records)
        switch char(string(axis_name))
            case 'mode_count'
                labels{i} = sprintf('N%d', round(records(i).Nx));
            case 'dt'
                labels{i} = sprintf('dt=%.3e', records(i).dt);
            otherwise
                labels{i} = sprintf('N%d', round(records(i).Nx));
        end
    end
end

function values = column_or_empty(records, field_name)
    if isempty(records)
        values = zeros(0, 1);
        return;
    end
    values = nan(numel(records), 1);
    for i = 1:numel(records)
        values(i) = double(records(i).(field_name));
    end
end

function warnings = collect_stage_warnings(stage_summaries)
    warnings = {};
    for i = 1:numel(stage_summaries)
        msgs = stage_summaries(i).warning_messages;
        if isempty(msgs)
            continue;
        end
        for j = 1:numel(msgs)
            warnings{end + 1} = msgs{j}; %#ok<AGROW>
        end
    end
end

function text = encode_json_pretty(payload)
    try
        text = jsonencode(payload, 'PrettyPrint', true);
    catch
        text = jsonencode(payload);
    end
end

function cleaned = sanitize_internal_for_save(internal)
    cleaned = internal;
    if isfield(cleaned, 'analysis')
        cleaned.analysis = filter_graphics_objects(cleaned.analysis);
    end
end

function cleaned = sanitize_reference_for_save(reference_info)
    cleaned = reference_info;
    if isfield(cleaned, 'internal')
        cleaned.internal = sanitize_internal_for_save(cleaned.internal);
    end
end

function out = sanitize_internal_for_reference(internal)
    out = internal;
    if isfield(out, 'analysis')
        out.analysis = struct();
    end
end

function stripped = strip_runtime_only_fields(value)
    stripped = value;
    runtime_fields = {'ui_progress_callback', 'progress_data_queue'};
    if isstruct(stripped)
        for i = 1:numel(runtime_fields)
            key = runtime_fields{i};
            if isfield(stripped, key)
                stripped = rmfield(stripped, key);
            end
        end
    end
end

function grid_meta = build_spectral_grid_meta(Parameters)
    Nx = max(2, round(double(resolve_positive_scalar(Parameters, {'Nx'}, 16))));
    Ny = max(2, round(double(resolve_positive_scalar(Parameters, {'Ny'}, 16))));
    Lx = double(resolve_positive_scalar(Parameters, {'Lx'}, 1.0));
    Ly = double(resolve_positive_scalar(Parameters, {'Ly'}, 1.0));
    dx = Lx / Nx;
    dy = Ly / Ny;
    grid_meta = struct('X', zeros(Ny, Nx), 'Y', zeros(Ny, Nx), 'dx', dx, 'dy', dy, 'Lx', Lx, 'Ly', Ly);
end

function progress_callback = emit_agent_trace_progress(progress_callback, Run_Config, dispatch_info, Results, run_timer)
    if isempty(progress_callback)
        return;
    end
    if ~isfield(Results, 'agent_trace_table') || ~istable(Results.agent_trace_table) || isempty(Results.agent_trace_table)
        return;
    end

    trace = Results.agent_trace_table;
    n_rows = height(trace);
    for i = 1:n_rows
        phase = 'adaptive_search';
        if ismember('phase', trace.Properties.VariableNames)
            phase = char(string(trace.phase(i)));
        end
        mesh_n = NaN;
        if ismember('N', trace.Properties.VariableNames)
            mesh_n = double(trace.N(i));
        end
        metric = NaN;
        if ismember('metric', trace.Properties.VariableNames)
            metric = double(trace.metric(i));
        end
        wall_t = toc(run_timer);
        if ismember('wall_time', trace.Properties.VariableNames) && isfinite(double(trace.wall_time(i)))
            wall_t = double(trace.wall_time(i));
        end

        next_n = NaN;
        if i < n_rows && ismember('N', trace.Properties.VariableNames)
            next_n = double(trace.N(i + 1));
        end

        progress_callback = emit_convergence_progress_payload(progress_callback, Run_Config, ...
            dispatch_info, phase, i, n_rows, mesh_n, mesh_n, metric, metric, wall_t, next_n);
    end
end

function progress_callback = resolve_progress_callback(Settings)
    progress_callback = resolve_runtime_progress_callback(Settings);
end

function progress_display = convergence_progress_display_payload(spec, stage_spec, stage_iteration, mesh_nx, mesh_ny)
    progress_display = struct( ...
        'iteration', NaN, ...
        'total_iterations', NaN, ...
        'mesh_nx', NaN, ...
        'mesh_ny', NaN, ...
        'progress_pct', NaN, ...
        'stage_name', char(string(stage_spec.stage_name)));

    primary_stage_name = resolve_primary_stage_label(spec.method_type);
    primary_total = numel(spec.primary_levels);
    progress_display.total_iterations = double(primary_total);

    if strcmpi(char(string(stage_spec.stage_name)), char(string(primary_stage_name)))
        progress_display.iteration = double(stage_iteration);
        progress_display.mesh_nx = double(mesh_nx);
        progress_display.mesh_ny = double(mesh_ny);
    else
        progress_display.iteration = 0;
    end

    if isfinite(progress_display.iteration) && isfinite(progress_display.total_iterations) && progress_display.total_iterations > 0
        progress_display.progress_pct = 100 * min(max(progress_display.iteration / progress_display.total_iterations, 0), 1);
    end
end

function progress_callback = emit_convergence_progress_payload(progress_callback, Run_Config, dispatch_info, phase, iteration, total_iterations, mesh_nx, mesh_ny, qoi_value, conv_residual, elapsed_wall, recommended_next_n, progress_display)
    if isempty(progress_callback) || ~dispatch_info.progress_telemetry
        return;
    end

    if nargin < 13 || ~isstruct(progress_display)
        progress_display = struct();
    end

    payload = struct();
    payload.channel = 'convergence_progress';
    payload.phase = char(string(phase));
    payload.stage_name = char(string(pick_field(progress_display, 'stage_name', phase)));
    payload.method = Run_Config.method;
    payload.mode = 'convergence';
    payload.iteration = double(iteration);
    payload.total_iterations = double(total_iterations);
    payload.mesh_n = double(mesh_nx);
    payload.mesh_nx = double(mesh_nx);
    payload.mesh_ny = double(mesh_ny);
    payload.qoi_value = double(qoi_value);
    payload.convergence_metric = double(conv_residual);
    payload.convergence_residual = double(conv_residual);
    payload.elapsed_wall = double(elapsed_wall);
    payload.bracket_low = NaN;
    payload.bracket_high = NaN;
    payload.recommended_next_n = double(recommended_next_n);
    payload.objective_mode = dispatch_info.objective_mode;
    payload.worker_topology = summarize_worker_topology();
    payload.display_iteration = double(pick_field(progress_display, 'iteration', NaN));
    payload.display_total_iterations = double(pick_field(progress_display, 'total_iterations', NaN));
    payload.display_mesh_nx = double(pick_field(progress_display, 'mesh_nx', NaN));
    payload.display_mesh_ny = double(pick_field(progress_display, 'mesh_ny', NaN));
    payload.display_progress_pct = double(pick_field(progress_display, 'progress_pct', NaN));
    if isfield(Run_Config, 'study_id')
        payload.run_id = Run_Config.study_id;
        payload.study_id = Run_Config.study_id;
    end

    try
        invoke_runtime_progress_callback(progress_callback, payload);
    catch ME
        warning('mode_convergence:ProgressCallbackDisabled', ...
            'Progress callback failed and will be disabled for this run: %s', ME.message);
        progress_callback = [];
    end
end

function out = summarize_worker_topology()
    out = 'unknown';
    try
        pool = gcp('nocreate');
        if isempty(pool)
            out = 'serial';
        else
            out = sprintf('parallel_pool:%d', pool.NumWorkers);
        end
    catch
        out = 'serial';
    end
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

function apply_dark_theme_for_figure(fig_handle)
    if isempty(fig_handle) || ~isvalid(fig_handle)
        return;
    end
    try
        ResultsPlotDispatcher.apply_dark_theme(fig_handle, ResultsPlotDispatcher.default_colors());
    catch
    end
end

function record = empty_run_record()
    record = struct( ...
        'method', '', ...
        'study_stage', '', ...
        'refinement_axis', '', ...
        'h', NaN, ...
        'dt', NaN, ...
        'mode_count', NaN, ...
        'polynomial_order', NaN, ...
        'Nx', NaN, ...
        'Ny', NaN, ...
        'Nz', NaN, ...
        'dof', NaN, ...
        'cells', NaN, ...
        'modes', NaN, ...
        'reference_strategy', '', ...
        'error_L1', NaN, ...
        'error_L2', NaN, ...
        'error_Linf', NaN, ...
        'relative_change', NaN, ...
        'observed_rate', NaN, ...
        'runtime_wall_s', NaN, ...
        'runtime_cpu_s', NaN, ...
        'memory_peak_mb', NaN, ...
        'iterations', NaN, ...
        'cfl', NaN, ...
        'nan_inf_flag', false, ...
        'stability_flags', struct(), ...
        'conservation_drift', NaN, ...
        'aliasing_indicator', NaN, ...
        'smoothness_indicator', NaN, ...
        'convergence_verdict', '', ...
        'stop_reason', '');
end

function summary = empty_stage_summary()
    summary = struct( ...
        'stage_name', '', ...
        'refinement_axis', '', ...
        'reference_strategy', '', ...
        'reference_warning', '', ...
        'verdict', '', ...
        'stop_reason', '', ...
        'expected_order', NaN, ...
        'observed_order_last', NaN, ...
        'monotone_error_reduction', false, ...
        'plateau_detected', false, ...
        'final_error_L2', NaN, ...
        'final_relative_change', NaN, ...
        'runtime_wall_s', NaN, ...
        'warning_messages', {{}}, ...
        'stable', false);
end

function out = empty_level_result()
    out = struct('record', empty_run_record(), 'internal', empty_internal_record(), 'parameters', struct());
end

function out = empty_internal_record()
    out = struct( ...
        'status', 'pending', ...
        'error_message', '', ...
        'error_identifier', '', ...
        'grid', struct(), ...
        'cfg', struct(), ...
        'analysis', struct(), ...
        'initial_field', [], ...
        'final_field', [], ...
        'initial_metrics', struct(), ...
        'final_metrics', struct(), ...
        'final_time', NaN, ...
        'frequency_metadata', struct(), ...
        'omega_hat', [], ...
        'psi_hat', []);
end

function value = resolve_positive_scalar(source, keys, fallback)
    value = fallback;
    if ~isstruct(source)
        return;
    end
    for i = 1:numel(keys)
        key = keys{i};
        if isfield(source, key)
            candidate = double(source.(key));
            if isscalar(candidate) && isfinite(candidate) && candidate > 0
                value = candidate;
                return;
            end
        end
    end
end

function out = resolve_string(source, keys, fallback)
    out = fallback;
    if ~isstruct(source)
        return;
    end
    for i = 1:numel(keys)
        key = keys{i};
        if isfield(source, key) && ~isempty(source.(key))
            out = char(string(source.(key)));
            return;
        end
    end
end

function tf = resolve_logical(source, keys, fallback)
    tf = fallback;
    if ~isstruct(source)
        return;
    end
    for i = 1:numel(keys)
        key = keys{i};
        if isfield(source, key) && ~isempty(source.(key))
            tf = logical(source.(key));
            return;
        end
    end
end

function values = resolve_numeric_vector(source, keys, fallback)
    values = fallback;
    if ~isstruct(source)
        return;
    end
    for i = 1:numel(keys)
        key = keys{i};
        if ~isfield(source, key) || isempty(source.(key))
            continue;
        end
        candidate = double(source.(key));
        candidate = candidate(isfinite(candidate));
        if ~isempty(candidate)
            values = candidate(:).';
            return;
        end
    end
end

function mesh_sizes = resolve_legacy_n_levels(Parameters)
    n_coarse = resolve_positive_scalar(Parameters, {'convergence_N_coarse'}, 32);
    n_max = resolve_positive_scalar(Parameters, {'convergence_N_max'}, max(128, n_coarse));
    n_coarse = max(8, round(n_coarse));
    n_max = max(n_coarse, round(n_max));
    mesh_sizes = n_coarse;
    while mesh_sizes(end) < n_max
        mesh_sizes(end + 1) = min(n_max, 2 * mesh_sizes(end)); %#ok<AGROW>
    end
    mesh_sizes = unique(mesh_sizes, 'stable');
end

function s = pick_struct_field(source, field_name, fallback)
    s = fallback;
    if isstruct(source) && isfield(source, field_name) && isstruct(source.(field_name))
        s = source.(field_name);
    end
end

function out = pick_field(source, key, fallback)
    out = fallback;
    if isstruct(source) && isfield(source, key) && ~isempty(source.(key))
        out = source.(key);
    end
end

function value = resolve_metric_field(metrics, key, fallback)
    value = fallback;
    if isstruct(metrics) && isfield(metrics, key) && ~isempty(metrics.(key))
        value = double(metrics.(key));
    end
end

function out = pick_nested_logical(source, keys, fallback)
    out = fallback;
    current = source;
    for i = 1:numel(keys)
        key = keys{i};
        if ~isstruct(current) || ~isfield(current, key)
            return;
        end
        current = current.(key);
    end
    out = logical(current);
end

function last_val = last_finite(values)
    last_val = NaN;
    values = values(:);
    idx = find(isfinite(values), 1, 'last');
    if ~isempty(idx)
        last_val = values(idx);
    end
end

function field = gather_to_cpu(field)
    try
        if isa(field, 'gpuArray')
            field = gather(field);
        end
    catch
    end
end

function mem_mb = query_memory_mb()
    mem_mb = NaN;
    try
        m = memory();
        if isstruct(m) && isfield(m, 'MemUsedMATLAB')
            mem_mb = double(m.MemUsedMATLAB) / (1024 ^ 2);
        end
    catch
    end
end

function label = resolve_primary_stage_label(method_type)
    if strcmp(method_type, 'spectral')
        label = 'modal';
    else
        label = 'spatial';
    end
end

function study_id = compact_convergence_study_id(study_id, method_name)
    study_id = char(string(study_id));
    if strlength(string(study_id)) <= 64
        return;
    end
    method_tok = lower(char(string(method_name)));
    method_tok = regexprep(method_tok, '[^a-z0-9]+', '');
    stamp = char(datetime('now', 'Format', 'yyyyMMdd_HHmmss'));
    rand_tok = char(java.util.UUID.randomUUID());
    rand_tok = lower(regexprep(rand_tok, '[^a-z0-9]', ''));
    rand_tok = rand_tok(1:6);
    study_id = sprintf('cv_%s_%s_%s', method_tok, stamp, rand_tok);
end
