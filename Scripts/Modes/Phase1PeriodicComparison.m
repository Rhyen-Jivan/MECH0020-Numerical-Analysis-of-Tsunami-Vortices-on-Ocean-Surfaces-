function [Results, paths] = Phase1PeriodicComparison(Run_Config, Parameters, Settings)
% Phase1PeriodicComparison - Periodic-domain FD vs Spectral workflow.

    if nargin < 3
        error('Phase1PeriodicComparison:InvalidInputs', ...
            'Run_Config, Parameters, and Settings are required.');
    end

    mesh_workflow = is_mesh_convergence_workflow(Run_Config);
    workflow_kind = resolve_phase1_workflow_kind(Run_Config);
    phase_cfg = resolve_phase1_config(Parameters, Run_Config);
    phase_id = make_phase_id(Run_Config);
    paths = build_phase_paths(Settings, phase_id, mesh_workflow);
    ensure_phase_directories(paths);

    phase_parameters = force_phase1_parameters(Parameters, phase_cfg);
    phase_settings = normalize_phase_settings(Settings, phase_cfg);
    phase_settings = configure_phase_runtime_output_paths(phase_settings, paths);
    phase_settings = PhaseTelemetryCSVFirst.configure_phase_runtime(phase_settings, paths, phase_id, workflow_kind);
    progress_callback = resolve_progress_callback(phase_settings);
    emit_phase_runtime_log(progress_callback, sprintf('%s preflight: initializing artifacts at %s', ...
        workflow_display_name(mesh_workflow), paths.base), 'info');
    phase_timer = tic;
    telemetry_context = PhaseTelemetryCSVFirst.start_phase_session(phase_settings, paths, phase_id, workflow_kind);
    paths.raw_hwinfo_csv_path = telemetry_context.raw_csv_path;
    paths.stage_boundaries_csv_path = telemetry_context.boundary_csv_path;

    safe_save_mat(fullfile(paths.config, 'Phase1_Config.mat'), struct( ...
        'Run_Config_clean', filter_graphics_objects(Run_Config), ...
        'phase_parameters_clean', filter_graphics_objects(phase_parameters), ...
        'phase_settings_clean', filter_graphics_objects(phase_settings), ...
        'phase_cfg_clean', filter_graphics_objects(phase_cfg)));
    write_run_settings_text(paths.run_settings_path, ...
        'Run Config', Run_Config, ...
        'Phase Parameters', phase_parameters, ...
        'Phase Settings', phase_settings, ...
        'Phase Config', phase_cfg);
    emit_phase_runtime_log(progress_callback, sprintf('%s saved run settings: %s', ...
        workflow_display_name(mesh_workflow), paths.run_settings_path), 'info');

    seed_entry = ConvergedMeshRegistry.empty_entry();
    if ~mesh_workflow && logical(phase_cfg.use_converged_mesh_seed)
        seed_entry = ConvergedMeshRegistry.select_latest(phase_settings.output_root, struct( ...
            'bc_case', phase_cfg.force_bc_case, ...
            'bathymetry_scenario', phase_cfg.force_bathymetry, ...
            'ic_type', pick_text(phase_parameters, {'ic_type'}, '')));
        phase_parameters = apply_seed_to_phase_parameters(phase_parameters, seed_entry);
    end

    jobs = build_phase_queue_jobs(phase_id, phase_parameters, phase_settings, paths, Run_Config);
    initialize_phase1_queue_artifacts(jobs, phase_cfg);
    write_phase1_preflight_manifest(paths, phase_id, jobs, phase_cfg, phase_parameters, Run_Config);
    child_output_root = pick_text(paths, {'matlab_data_root', 'runs_root', 'base'}, '');
    emit_phase_runtime_log(progress_callback, sprintf('%s queue initialized: %d jobs under %s', ...
        workflow_display_name(mesh_workflow), numel(jobs), child_output_root), 'info');
    try
        queue_outputs = run_phase_queue(jobs, progress_callback, phase_id, phase_timer, phase_cfg, telemetry_context, workflow_kind);
        telemetry_context = PhaseTelemetryCSVFirst.stop_phase_session(telemetry_context);
    catch ME
        if isstruct(telemetry_context) && isfield(telemetry_context, 'active') && logical(telemetry_context.active)
            try
                telemetry_context = PhaseTelemetryCSVFirst.stop_phase_session(telemetry_context);
            catch stopME
                warning('Phase1PeriodicComparison:TelemetryShutdownFailed', ...
                    'Phase 1 telemetry shutdown failed after workflow error: %s', stopME.message);
            end
        end
        rethrow(ME);
    end

    if mesh_workflow
        [Results, paths] = finalize_mesh_convergence_workflow(phase_id, phase_cfg, queue_outputs, paths, ...
            toc(phase_timer), Run_Config, phase_parameters, phase_settings, progress_callback, seed_entry, telemetry_context);
        emit_phase_queue_payload(progress_callback, phase_id, jobs(end), 'completed', 100, toc(phase_timer), ...
            'Mesh convergence complete: FD and Spectral mesh sweeps finished.', jobs, queue_outputs, workflow_kind);
        return;
    end

    fd_run = require_output(queue_outputs, 'fd', 'evolution');
    sp_run = require_output(queue_outputs, 'spectral', 'evolution');
    fd_mesh = direct_phase1_mesh_entry_from_output('FD', fd_run, phase_cfg);
    sp_mesh = direct_phase1_mesh_entry_from_output('Spectral', sp_run, phase_cfg);
    fd_run.selected_mesh = fd_mesh;
    sp_run.selected_mesh = sp_mesh;

    fd_metrics = compute_phase1_metrics('FD', fd_run, sp_run, fd_mesh);
    sp_metrics = compute_phase1_metrics('Spectral', sp_run, fd_run, sp_mesh);
    error_vs_time = compute_phase1_error_vs_time(fd_run, sp_run);
    ic_study = build_phase1_ic_study_results(queue_outputs, phase_cfg, fd_mesh, sp_mesh);
    summary_metrics = compute_summary_metrics(fd_metrics, sp_metrics, queue_outputs, phase_cfg, error_vs_time, ic_study);

    registry_entries = [ ...
        coerce_registry_entry(seed_entry); ...
        coerce_registry_entry(fd_mesh); ...
        coerce_registry_entry(sp_mesh)];
    registry_entries = registry_entries(~cellfun(@isempty, {registry_entries.method}));
    if ~isempty(registry_entries)
        ConvergedMeshRegistry.save_registry(registry_entries, fullfile(paths.data, 'converged_mesh_registry.mat'));
    end

    Results = assemble_phase1_results(phase_id, phase_cfg, seed_entry, ...
        struct(), struct(), fd_run, sp_run, fd_metrics, sp_metrics, summary_metrics, error_vs_time, ic_study, ...
        queue_outputs, paths, toc(phase_timer), Run_Config, phase_parameters);

    Results.figure_artifacts = struct();
    Results.reference_calibration = struct();
    Results.workflow_media_artifacts = struct( ...
        'status', 'not_requested', ...
        'failure_message', '', ...
        'frame_count', 0, ...
        'case_animation_artifacts', struct([]));
    if logical(phase_cfg.save_figures)
        if phase1_defer_heavy_exports_requested(phase_settings)
            Results.figure_artifacts = struct( ...
                'deferred', true, ...
                'reason', 'host_owned_publication', ...
                'status', 'queued');
            Results.reference_calibration = struct( ...
                'deferred', true, ...
                'reason', 'host_owned_publication', ...
                'status', 'queued');
            emit_phase_runtime_log(progress_callback, ...
                'Phase 1 deferred worker-side figure generation; Results publication will autosave visuals on the host.', 'info');
        else
            [Results.figure_artifacts, Results.reference_calibration] = generate_phase1_plots(Results, paths);
            emit_artifact_struct_logs(progress_callback, 'Phase 1 figure', Results.figure_artifacts);
        end
    end
    if logical(pick_value(phase_cfg, 'create_animations', false))
        if phase1_defer_heavy_exports_requested(phase_settings)
            Results.workflow_media_artifacts = struct( ...
                'status', 'deferred', ...
                'failure_message', '', ...
                'frame_count', 0, ...
                'case_animation_artifacts', struct([]), ...
                'reason', 'host_owned_publication');
            emit_phase_runtime_log(progress_callback, ...
                'Phase 1 deferred worker-side workflow media generation; Results publication will autosave animations on the host.', 'info');
        else
            Results.workflow_media_artifacts = export_phase1_workflow_animations(Results, phase_parameters, phase_settings, paths);
            emit_artifact_struct_logs(progress_callback, 'Phase 1 media', Results.workflow_media_artifacts);
        end
    end
    Results.ic_study = attach_phase1_ic_study_artifacts(Results.ic_study, Results.figure_artifacts);
    Results.workflow_manifest = build_phase_workflow_manifest(phase_id, queue_outputs, paths, ...
        Results.children, Results.combined, Results.metrics.summary, Results.error_vs_time, ...
        Results.ic_study, Results.figure_artifacts, phase_cfg, Results.parent_parameters, Results.parent_run_config);
    Results.workflow_manifest.reference_calibration = Results.reference_calibration;
    Results.workflow_manifest.workflow_media_artifacts = Results.workflow_media_artifacts;
    phase_monitor_series = build_phase1_workflow_monitor_series(queue_outputs, phase_id, workflow_kind);
    phase_monitor_series = PhaseTelemetryCSVFirst.decorate_monitor_series(phase_monitor_series, telemetry_context);
    [Results.collector_artifacts, Results.plotting_data] = write_phase1_workflow_collector_artifacts(Results, Run_Config, paths, phase_monitor_series);
    workbook_path = pick_text(Results.collector_artifacts, {'phase_workbook_path', 'phase_workbook_root_path'}, '');
    if ~isempty(workbook_path)
        paths.run_data_workbook_path = workbook_path;
    end
    Results.paths = paths;
    Results.workflow_manifest.paths = paths;
    Results.workflow_manifest.collector_artifacts = Results.collector_artifacts;

    ResultsForSave = strip_phase1_for_persistence(Results);
    workflow_manifest = ResultsForSave.workflow_manifest;
    ResultsForSave.artifact_layout_version = char(string(paths.artifact_layout_version));
    workflow_manifest.artifact_layout_version = char(string(paths.artifact_layout_version));
    ResultsForSave.workflow_manifest = workflow_manifest;

    save(fullfile(paths.data, 'phase1_results.mat'), 'ResultsForSave', '-v7.3');
    emit_phase_runtime_log(progress_callback, sprintf('Phase 1 saved MAT results: %s', fullfile(paths.data, 'phase1_results.mat')), 'info');
    if json_saving_enabled(phase_cfg, phase_settings, phase_parameters)
        write_json(fullfile(paths.data, 'phase1_results.json'), ResultsForSave);
        emit_phase_runtime_log(progress_callback, sprintf('Phase 1 saved JSON results: %s', fullfile(paths.data, 'phase1_results.json')), 'info');
    end
    safe_save_mat(fullfile(paths.data, 'phase1_workflow_manifest.mat'), struct('workflow_manifest', workflow_manifest));
    emit_phase_runtime_log(progress_callback, sprintf('Phase 1 saved workflow manifest MAT: %s', fullfile(paths.data, 'phase1_workflow_manifest.mat')), 'info');
    if json_saving_enabled(phase_cfg, phase_settings, phase_parameters)
        write_json(fullfile(paths.data, 'phase1_workflow_manifest.json'), workflow_manifest);
        emit_phase_runtime_log(progress_callback, sprintf('Phase 1 saved workflow manifest JSON: %s', fullfile(paths.data, 'phase1_workflow_manifest.json')), 'info');
        write_phase1_artifact_manifest(paths, ResultsForSave);
        emit_phase_runtime_log(progress_callback, sprintf('Phase 1 updated artifact manifest: %s', fullfile(paths.matlab_data_root, 'artifact_manifest.json')), 'info');
    end
    emit_phase_completion_report_payload(progress_callback, ResultsForSave, paths, ...
        Run_Config, Results.parent_parameters, 'Phase 1', workflow_kind, 'phase1_workflow');
    write_phase1_report(Results, paths);
    emit_phase_runtime_log(progress_callback, sprintf('Phase 1 saved report: %s', fullfile(paths.reports, 'Phase1_Periodic_FD_vs_Spectral_Report.md')), 'info');
    append_phase1_master_rows(Results, Run_Config, phase_parameters);

    emit_phase_queue_payload(progress_callback, phase_id, jobs(end), 'completed', 100, toc(phase_timer), ...
        'Phase 1 complete: direct FD/Spectral comparison and IC-study queue finished.', ...
        jobs, queue_outputs, workflow_kind);
end

function tf = is_mesh_convergence_workflow(Run_Config)
    tf = false;
    if nargin < 1 || ~isstruct(Run_Config) || ~isfield(Run_Config, 'workflow_kind')
        return;
    end
    tf = strcmpi(char(string(Run_Config.workflow_kind)), 'mesh_convergence_study');
end

function workflow_kind = resolve_phase1_workflow_kind(Run_Config)
    if is_mesh_convergence_workflow(Run_Config)
        workflow_kind = 'mesh_convergence_study';
    else
        workflow_kind = 'phase1_periodic_comparison';
    end
end

function label = workflow_display_name(mesh_workflow)
    if nargin >= 1 && logical(mesh_workflow)
        label = 'Mesh convergence';
    else
        label = 'Phase 1';
    end
end

function label = workflow_kind_display_name(workflow_kind)
    label = workflow_display_name(strcmpi(char(string(workflow_kind)), 'mesh_convergence_study'));
end

function phase_token = workflow_phase_token(workflow_kind)
    if strcmpi(char(string(workflow_kind)), 'mesh_convergence_study')
        phase_token = 'mesh_convergence';
    else
        phase_token = 'phase1';
    end
end

function phase_cfg = resolve_phase1_config(Parameters, Run_Config)
    defaults = create_default_parameters();
    mesh_workflow = is_mesh_convergence_workflow(Run_Config);
    defaults_field = 'phase1';
    override_field = 'phase1';
    if mesh_workflow
        defaults_field = 'mesh_convergence';
        override_field = 'mesh_convergence';
    end
    if ~isfield(defaults, defaults_field) || ~isstruct(defaults.(defaults_field))
        error('Phase1PeriodicComparison:MissingDefaults', ...
            'create_default_parameters must define %s defaults.', defaults_field);
    end

    base_defaults = defaults.(defaults_field);
    phase_cfg = base_defaults;
    if isfield(Parameters, override_field) && isstruct(Parameters.(override_field))
        phase_cfg = merge_structs(phase_cfg, Parameters.(override_field));
    end
    phase1_fallback_cfg = pick_struct(defaults, {'phase1'}, struct());
    if isfield(Parameters, 'phase1') && isstruct(Parameters.phase1)
        phase1_fallback_cfg = merge_structs(phase1_fallback_cfg, Parameters.phase1);
    end
    if mesh_workflow
        phase_cfg.ic_study = struct('enabled', false, 'catalog', struct([]), 'include_arrangement_cases', false);
        fallback_numeric_fields = {'fd_grid_n', 'spectral_grid_n', 'fd_dt', 'spectral_dt', ...
            'taylor_green_fd_grid_n', 'taylor_green_spectral_grid_n', 'taylor_green_fd_dt', 'taylor_green_spectral_dt'};
        for fallback_idx = 1:numel(fallback_numeric_fields)
            field_name = fallback_numeric_fields{fallback_idx};
            if (~isfield(phase_cfg, field_name) || isempty(phase_cfg.(field_name))) && ...
                    isfield(phase1_fallback_cfg, field_name) && ~isempty(phase1_fallback_cfg.(field_name))
                phase_cfg.(field_name) = phase1_fallback_cfg.(field_name);
            end
        end
        if (~isfield(phase_cfg, 'taylor_green_honor_fixed_dt') || isempty(phase_cfg.taylor_green_honor_fixed_dt)) && ...
                isfield(phase1_fallback_cfg, 'taylor_green_honor_fixed_dt')
            phase_cfg.taylor_green_honor_fixed_dt = logical(phase1_fallback_cfg.taylor_green_honor_fixed_dt);
        end
        mesh_default_dt = max(eps, double(pick_value(phase_cfg, 'default_dt', ...
            pick_value(base_defaults, 'default_dt', pick_value(Parameters, 'dt', 0.01)))));
        phase_cfg.default_dt = mesh_default_dt;
        if ~isfield(phase_cfg, 'fd_dt') || ~(isnumeric(phase_cfg.fd_dt) && isscalar(phase_cfg.fd_dt) && isfinite(phase_cfg.fd_dt) && phase_cfg.fd_dt > 0)
            phase_cfg.fd_dt = mesh_default_dt;
        end
        if ~isfield(phase_cfg, 'spectral_dt') || ~(isnumeric(phase_cfg.spectral_dt) && isscalar(phase_cfg.spectral_dt) && isfinite(phase_cfg.spectral_dt) && phase_cfg.spectral_dt > 0)
            phase_cfg.spectral_dt = mesh_default_dt;
        end
        if ~isfield(phase_cfg, 'taylor_green_fd_dt') || ~(isnumeric(phase_cfg.taylor_green_fd_dt) && isscalar(phase_cfg.taylor_green_fd_dt) && isfinite(phase_cfg.taylor_green_fd_dt) && phase_cfg.taylor_green_fd_dt > 0)
            phase_cfg.taylor_green_fd_dt = mesh_default_dt;
        end
        if ~isfield(phase_cfg, 'taylor_green_spectral_dt') || ~(isnumeric(phase_cfg.taylor_green_spectral_dt) && isscalar(phase_cfg.taylor_green_spectral_dt) && isfinite(phase_cfg.taylor_green_spectral_dt) && phase_cfg.taylor_green_spectral_dt > 0)
            phase_cfg.taylor_green_spectral_dt = mesh_default_dt;
        end
    end

    phase_cfg.force_bc_case = char(string(pick_text(phase_cfg, {'force_bc_case'}, 'periodic')));
    phase_cfg.force_bathymetry = char(string(pick_text(phase_cfg, {'force_bathymetry'}, 'flat_2d')));
    phase_cfg.stability_scope = char(string(pick_text(phase_cfg, {'stability_scope'}, 'observed_cfl')));
    phase_cfg.use_converged_mesh_seed = logical(pick_value(phase_cfg, 'use_converged_mesh_seed', false));
    phase_cfg.allow_unconverged_mesh_fallback = logical(pick_value(phase_cfg, 'allow_unconverged_mesh_fallback', true));
    phase_cfg.save_figures = logical(pick_value(phase_cfg, 'save_figures', true));
    phase_cfg.save_level_visuals = logical(pick_value(phase_cfg, 'save_level_visuals', true));
    phase_cfg.save_json = logical(pick_value(phase_cfg, 'save_json', pick_value(Parameters, 'save_json', true)));
    phase_cfg.save_reports = logical(pick_value(phase_cfg, 'save_reports', pick_value(Parameters, 'save_reports', true)));
    phase_cfg.taylor_green_fd_dt = max(eps, double(pick_value(phase_cfg, 'taylor_green_fd_dt', ...
        pick_value(base_defaults, 'taylor_green_fd_dt', pick_value(phase_cfg, 'fd_dt', pick_value(Parameters, 'dt', 0.01))))));
    phase_cfg.taylor_green_spectral_dt = max(eps, double(pick_value(phase_cfg, 'taylor_green_spectral_dt', ...
        pick_value(base_defaults, 'taylor_green_spectral_dt', pick_value(phase_cfg, 'spectral_dt', pick_value(Parameters, 'dt', 0.01))))));
    phase_cfg.taylor_green_fd_grid_n = max(8, round(double(pick_value(phase_cfg, 'taylor_green_fd_grid_n', ...
        pick_value(base_defaults, 'taylor_green_fd_grid_n', pick_value(phase_cfg, 'fd_grid_n', pick_value(Parameters, 'Nx', 128)))))));
    phase_cfg.taylor_green_spectral_grid_n = max(8, round(double(pick_value(phase_cfg, 'taylor_green_spectral_grid_n', ...
        pick_value(base_defaults, 'taylor_green_spectral_grid_n', pick_value(phase_cfg, 'spectral_grid_n', pick_value(Parameters, 'Nx', 128)))))));
    phase_cfg.taylor_green_honor_fixed_dt = logical(pick_value(phase_cfg, 'taylor_green_honor_fixed_dt', ...
        pick_value(base_defaults, 'taylor_green_honor_fixed_dt', true)));
    phase_cfg.ic_study = normalize_phase1_ic_study_config( ...
        pick_value(phase_cfg, 'ic_study', struct()), pick_value(defaults.phase1, 'ic_study', struct()));
    phase_cfg.ic_type = pick_text(phase_cfg, {'ic_type'}, pick_text(Parameters, {'ic_type'}, 'stretched_gaussian'));
    baseline_meta = phase1_baseline_case_metadata(pick_text(phase_cfg.ic_study, {'baseline_ic_type'}, ...
        pick_text(Parameters, {'ic_type'}, 'stretched_gaussian')));
    phase_cfg.ic_study.baseline_ic_type = baseline_meta.ic_type;
    phase_cfg.ic_study.baseline_label = baseline_meta.label;
    if isempty(pick_text(phase_cfg.ic_study, {'selected_case_id'}, '')) || ...
            any(strcmpi(pick_text(phase_cfg.ic_study, {'selected_case_id'}, ''), {'baseline_stretched_single', 'baseline_elliptic_single'}))
        phase_cfg.ic_study.selected_case_id = baseline_meta.case_id;
    end
    if ~isfield(phase_cfg, 'ic_snapshot') || ~isstruct(phase_cfg.ic_snapshot)
        phase_cfg.ic_snapshot = struct();
    end
    phase_cfg.mesh_level_count = resolve_phase1_mesh_level_count(phase_cfg, ...
        pick_value(base_defaults, 'mesh_level_count', numel(pick_value(base_defaults, 'convergence_mesh_levels_fd', [32, 155, 277, 400, 523, 645, 768]))));
    phase_cfg.mesh_ladder_mode = normalize_phase1_mesh_ladder_mode( ...
        pick_text(phase_cfg, {'mesh_ladder_mode'}, pick_text(base_defaults, {'mesh_ladder_mode'}, 'bounded')));
    phase_cfg.mesh_powers_of_two_max_n = max(8, round(double(pick_value(phase_cfg, 'mesh_powers_of_two_max_n', ...
        pick_value(base_defaults, 'mesh_powers_of_two_max_n', 1024)))));
    phase_cfg.convergence_tolerance = max(eps, double(pick_value(phase_cfg, 'convergence_tolerance', ...
        pick_value(base_defaults, 'convergence_tolerance', 1.0))));
    phase_cfg.convergence_mesh_levels_fd = resolve_phase1_mesh_levels(phase_cfg, ...
        'convergence_mesh_levels_fd', [32, 155, 277, 400, 523, 645, 768], false);
    phase_cfg.convergence_mesh_levels_spectral = resolve_phase1_mesh_levels(phase_cfg, ...
        'convergence_mesh_levels_spectral', [32, 155, 277, 400, 523, 645, 768], true);
    phase_cfg.mesh_selection_policy = normalize_phase1_mesh_selection_policy( ...
        pick_text(phase_cfg, {'mesh_selection_policy'}, 'first_converged_or_finest_mesh'));
    phase_cfg.adaptive_timestep = normalize_phase1_adaptive_timestep_config( ...
        pick_value(phase_cfg, 'adaptive_timestep', struct()), ...
        pick_value(base_defaults, 'adaptive_timestep', struct('enabled', false, 'C_adv', 0.5, 'C_diff', 0.25)));
    phase_cfg.adaptive_timestep.enabled = false;
    if ~strcmpi(phase_cfg.mesh_selection_policy, 'first_converged_or_finest_mesh')
        error('Phase1PeriodicComparison:UnsupportedMeshSelectionPolicy', ...
            'Unsupported Phase 1 mesh selection policy "%s".', phase_cfg.mesh_selection_policy);
    end
end

function mode = normalize_phase1_mesh_ladder_mode(mode_value)
    mode = lower(strtrim(char(string(mode_value))));
    switch mode
        case {'bounded', 'bounds'}
            mode = 'bounded';
        case {'powers_of_2', 'powers_of_two', 'powers of 2', 'pow2'}
            mode = 'powers_of_2';
        otherwise
            error('Phase1PeriodicComparison:UnsupportedMeshLadderMode', ...
                'Unsupported Phase 1 mesh ladder mode "%s".', char(string(mode_value)));
    end
end

function policy = normalize_phase1_mesh_selection_policy(policy_value)
    policy = char(string(policy_value));
    switch lower(strtrim(policy))
        case {'first_converged_or_finest_mesh', 'first_converged_or_best_error', 'first_converged_or_max'}
            policy = 'first_converged_or_finest_mesh';
        otherwise
            error('Phase1PeriodicComparison:UnsupportedMeshSelectionPolicy', ...
                'Unsupported Phase 1 mesh selection policy "%s".', char(string(policy_value)));
    end
end

function adaptive_cfg = normalize_phase1_adaptive_timestep_config(adaptive_cfg, default_cfg)
    if nargin < 1 || ~isstruct(adaptive_cfg)
        adaptive_cfg = struct();
    end
    if nargin < 2 || ~isstruct(default_cfg)
        default_cfg = struct('enabled', false, 'C_adv', 0.5, 'C_diff', 0.25);
    end
    adaptive_cfg = merge_structs(default_cfg, adaptive_cfg);
    adaptive_cfg.enabled = false;
    adaptive_cfg.C_adv = double(pick_value(adaptive_cfg, 'C_adv', 0.5));
    adaptive_cfg.C_diff = double(pick_value(adaptive_cfg, 'C_diff', 0.25));
    if ~(isfinite(adaptive_cfg.C_adv) && adaptive_cfg.C_adv > 0)
        error('Phase1PeriodicComparison:InvalidAdaptiveCAdv', ...
            'Phase 1 adaptive timestep requires a finite positive C_adv.');
    end
    if ~(isfinite(adaptive_cfg.C_diff) && adaptive_cfg.C_diff > 0)
        error('Phase1PeriodicComparison:InvalidAdaptiveCDiff', ...
            'Phase 1 adaptive timestep requires a finite positive C_diff.');
    end
end

function phase_id = make_phase_id(Run_Config)
    if isfield(Run_Config, 'phase_id') && ~isempty(Run_Config.phase_id)
        phase_id = char(string(Run_Config.phase_id));
        return;
    end

    ic = 'ic';
    if isfield(Run_Config, 'ic_type') && ~isempty(Run_Config.ic_type)
        ic = regexprep(lower(char(string(Run_Config.ic_type))), '[^a-z0-9]+', '_');
    end
    if is_mesh_convergence_workflow(Run_Config)
        phase_id = sprintf('mesh_convergence_%s_%s', ic, char(datetime('now', 'Format', 'yyyyMMdd_HHmmss')));
    else
        phase_id = sprintf('phase1_%s_%s', ic, char(datetime('now', 'Format', 'yyyyMMdd_HHmmss')));
    end
end

function ic_cfg = normalize_phase1_ic_study_config(ic_cfg, default_cfg)
    if nargin < 1 || ~isstruct(ic_cfg)
        ic_cfg = struct();
    end
    if nargin < 2 || ~isstruct(default_cfg)
        default_cfg = struct();
    end
    ic_cfg = merge_structs(default_cfg, ic_cfg);
    ic_cfg.enabled = logical(pick_value(ic_cfg, 'enabled', true));
    ic_cfg.include_arrangement_cases = logical(pick_value(ic_cfg, 'include_arrangement_cases', ...
        pick_value(default_cfg, 'include_arrangement_cases', false)));
    if ~isfield(ic_cfg, 'groups') || ~isstruct(ic_cfg.groups) || isempty(ic_cfg.groups)
        ic_cfg.groups = pick_value(default_cfg, 'groups', struct([]));
    end
    if ~isfield(ic_cfg, 'catalog') || ~isstruct(ic_cfg.catalog) || isempty(ic_cfg.catalog)
        ic_cfg.catalog = pick_value(default_cfg, 'catalog', struct([]));
    end
    if ~isfield(ic_cfg, 'baseline_label') || isempty(ic_cfg.baseline_label)
        ic_cfg.baseline_label = pick_text(default_cfg, {'baseline_label'}, 'Elliptic');
    end
    if ~isfield(ic_cfg, 'baseline_ic_type') || isempty(ic_cfg.baseline_ic_type)
        ic_cfg.baseline_ic_type = pick_text(default_cfg, {'baseline_ic_type'}, 'elliptical_vortex');
    end

    group_labels = {};
    if isstruct(ic_cfg.groups) && ~isempty(ic_cfg.groups)
        group_labels = {ic_cfg.groups.label};
    end
    selected_group = pick_text(ic_cfg, {'selected_group'}, '');
    if isempty(selected_group) || (~isempty(group_labels) && ~any(strcmpi(selected_group, group_labels)))
        if ~isempty(group_labels)
            selected_group = char(string(group_labels{1}));
        else
            selected_group = pick_text(default_cfg, {'selected_group'}, 'Elliptical Vortex');
        end
    end
    ic_cfg.selected_group = selected_group;

    selected_preset = pick_text(ic_cfg, {'selected_preset'}, '');
    preset_options = phase1_ic_study_presets_for_group(ic_cfg, selected_group);
    if isempty(selected_preset) || (~isempty(preset_options) && ~any(strcmpi(selected_preset, preset_options)))
        if ~isempty(preset_options)
            selected_preset = char(string(preset_options{1}));
        else
            selected_preset = 'Taylor-Green';
        end
    end
    ic_cfg.selected_preset = selected_preset;
    ic_cfg.chart_case_labels = [{char(string(ic_cfg.baseline_label))}, {ic_cfg.catalog.label}];
end

function presets = phase1_ic_study_presets_for_group(ic_cfg, group_label)
    presets = {};
    groups = pick_value(ic_cfg, 'groups', struct([]));
    if ~isstruct(groups)
        return;
    end
    idx = find(strcmpi({groups.label}, char(string(group_label))), 1, 'first');
    if isempty(idx)
        return;
    end
    presets = cellstr(string(groups(idx).presets));
end

function catalog = resolve_phase1_ic_study_catalog(phase_cfg)
    catalog = struct([]);
    ic_cfg = pick_value(phase_cfg, 'ic_study', struct());
    if ~logical(pick_value(ic_cfg, 'enabled', false))
        return;
    end
    if isfield(ic_cfg, 'catalog') && isstruct(ic_cfg.catalog)
        catalog = ic_cfg.catalog;
    end
    if isempty(catalog)
        return;
    end
    active_case_ids = {};
    if isfield(ic_cfg, 'active_case_ids') && ~isempty(ic_cfg.active_case_ids)
        active_case_ids = cellstr(string(ic_cfg.active_case_ids));
        active_case_ids = active_case_ids(~cellfun(@isempty, active_case_ids));
    end
    if ~isempty(active_case_ids)
        keep_mask = false(1, numel(catalog));
        for i = 1:numel(catalog)
            keep_mask(i) = any(strcmpi(pick_text(catalog(i), {'case_id'}, ''), active_case_ids));
        end
        catalog = catalog(keep_mask);
        return;
    end
    if ~logical(pick_value(ic_cfg, 'include_arrangement_cases', false))
        keep_mask = false(1, numel(catalog));
        for i = 1:numel(catalog)
            keep_mask(i) = strcmpi(pick_text(catalog(i), {'case_id'}, ''), 'taylor_green');
        end
        catalog = catalog(keep_mask);
    end
end

function paths = build_phase_paths(Settings, phase_id, mesh_workflow)
    output_root = 'Results';
    if isfield(Settings, 'output_root') && ~isempty(Settings.output_root)
        output_root = Settings.output_root;
    end
    phase_token = 'Phase1';
    if nargin >= 3 && logical(mesh_workflow)
        phase_token = 'MeshConvergence';
    end
    paths = PathBuilder.get_phase_paths(phase_token, phase_id, output_root);
    paths.phase_id = phase_id;
end

function ensure_phase_directories(paths)
    PathBuilder.ensure_directories(paths);
end

function params = force_phase1_parameters(Parameters, phase_cfg)
    params = Parameters;
    params.bc_case = phase_cfg.force_bc_case;
    params.boundary_condition_case = phase_cfg.force_bc_case;
    params.bc_top = 'Periodic';
    params.bc_bottom = 'Periodic';
    params.bc_left = 'Periodic';
    params.bc_right = 'Periodic';
    params.bc_top_math = 'periodic';
    params.bc_bottom_math = 'periodic';
    params.bc_left_math = 'periodic';
    params.bc_right_math = 'periodic';
    params.bc_top_physical = 'periodic';
    params.bc_bottom_physical = 'periodic';
    params.bc_left_physical = 'periodic';
    params.bc_right_physical = 'periodic';
    params.U_top = 0.0;
    params.U_bottom = 0.0;
    params.U_left = 0.0;
    params.U_right = 0.0;
    params.bathymetry_scenario = phase_cfg.force_bathymetry;
    params.bathymetry_dimension_policy = 'by_method';
    params.phase1 = phase_cfg;
    if ~isfield(params, 'resource_strategy') || isempty(params.resource_strategy) || ...
            strcmpi(char(string(params.resource_strategy)), 'mode_adaptive')
        params.resource_strategy = pick_text(phase_cfg, {'resource_strategy'}, 'throughput_first');
    end
    params = apply_phase1_ic_snapshot(params, phase_cfg);
    params.convergence_tol = phase_cfg.convergence_tolerance;
    params.create_animations = false;
    if ~isfield(params, 'convergence') || ~isstruct(params.convergence)
        params.convergence = struct();
    end
    if ~isfield(params.convergence, 'study') || ~isstruct(params.convergence.study)
        params.convergence.study = struct();
    end
    params.convergence.study.tolerance = phase_cfg.convergence_tolerance;
    if isfield(params, 'convergence') && isstruct(params.convergence) && ...
            isfield(params.convergence, 'study') && isstruct(params.convergence.study)
        params.convergence.study.reference.preferred_strategy = 'over_resolved_numerical';
    end
end

function params = apply_phase1_ic_snapshot(params, phase_cfg)
    if nargin < 2 || ~isstruct(phase_cfg) || ~isfield(phase_cfg, 'ic_snapshot') || ...
            ~isstruct(phase_cfg.ic_snapshot) || isempty(fieldnames(phase_cfg.ic_snapshot))
        return;
    end

    snapshot = phase_cfg.ic_snapshot;
    if isfield(snapshot, 'ic_type') && ~isempty(snapshot.ic_type)
        params.ic_type = char(string(snapshot.ic_type));
    end
    if isfield(snapshot, 'ic_scenario') && ~isempty(snapshot.ic_scenario)
        params.ic_scenario = char(string(snapshot.ic_scenario));
    end

    arrangement = '';
    if isfield(snapshot, 'ic_arrangement') && ~isempty(snapshot.ic_arrangement)
        arrangement = char(string(snapshot.ic_arrangement));
    elseif isfield(snapshot, 'ic_pattern') && ~isempty(snapshot.ic_pattern)
        arrangement = char(string(snapshot.ic_pattern));
    end
    if ~isempty(arrangement)
        params.ic_arrangement = arrangement;
        params.ic_pattern = arrangement;
    end

    if isfield(snapshot, 'ic_coeff') && isnumeric(snapshot.ic_coeff)
        params.ic_coeff = double(snapshot.ic_coeff(:).');
    end
    if isfield(snapshot, 'ic_dynamic_values') && isstruct(snapshot.ic_dynamic_values)
        params.ic_dynamic_values = snapshot.ic_dynamic_values;
    end
    if isfield(snapshot, 'ic_multi_vortex_rows')
        params.ic_multi_vortex_rows = snapshot.ic_multi_vortex_rows;
    end

    scalar_fields = {'ic_center_x', 'ic_center_y', 'ic_scale', 'ic_amplitude', ...
        'ic_coeff1', 'ic_coeff2', 'ic_coeff3', 'ic_coeff4', 'nu'};
    for i = 1:numel(scalar_fields)
        key = scalar_fields{i};
        if isfield(snapshot, key) && isnumeric(snapshot.(key)) && isscalar(snapshot.(key)) && ...
                isfinite(snapshot.(key))
            params.(key) = double(snapshot.(key));
        end
    end
    if isfield(snapshot, 'ic_count') && isnumeric(snapshot.ic_count) && isscalar(snapshot.ic_count) && ...
            isfinite(snapshot.ic_count)
        params.ic_count = max(1, round(double(snapshot.ic_count)));
    end
end

function params = apply_seed_to_phase_parameters(params, seed_entry)
    if ~isstruct(seed_entry) || ~isfield(seed_entry, 'method') || isempty(seed_entry.method)
        return;
    end
    params = ConvergedMeshRegistry.apply_to_parameters(params, seed_entry);
    if isfield(seed_entry, 'Nx') && isfinite(seed_entry.Nx)
        n_seed = max(8, round(seed_entry.Nx));
        params.convergence_N_max = n_seed;
        params.convergence_N_coarse = max(8, round(n_seed / 2));
        if isfield(params, 'convergence') && isstruct(params.convergence) && ...
                isfield(params.convergence, 'study') && isstruct(params.convergence.study)
            params.convergence.study.N_max = params.convergence_N_max;
            params.convergence.study.N_coarse = params.convergence_N_coarse;
            params.convergence.study.temporal.fine_N = params.convergence_N_max;
        end
    end
end

function settings = normalize_phase_settings(SettingsInput, phase_cfg)
    settings = Settings();
    if nargin >= 1 && isstruct(SettingsInput)
        settings = merge_structs(settings, SettingsInput);
    end
    settings.save_data = true;
    settings.save_reports = logical(pick_value(phase_cfg, 'save_reports', true));
    settings.save_json = logical(pick_value(phase_cfg, 'save_json', pick_value(settings, 'save_json', true)));
    settings.save_figures = logical(phase_cfg.save_figures);
    settings.append_to_master = false;
    settings.animation_enabled = false;
    if isfield(settings, 'media') && isstruct(settings.media)
        settings.media.enabled = false;
    end
    if ~isfield(settings, 'compatibility') || ~isstruct(settings.compatibility)
        settings.compatibility = struct();
    end
    settings.compatibility.return_analysis = true;
    if ~isfield(settings, 'output_root') || isempty(settings.output_root)
        settings.output_root = 'Results';
    end
    desired_strategy = pick_text(phase_cfg, {'resource_strategy'}, 'throughput_first');
    if ~isfield(settings, 'resource_allocation') || ~isstruct(settings.resource_allocation)
        settings.resource_allocation = struct();
    end
    if ~isfield(settings.resource_allocation, 'resource_strategy') || ...
            isempty(settings.resource_allocation.resource_strategy) || ...
            strcmpi(char(string(settings.resource_allocation.resource_strategy)), 'mode_adaptive')
        settings.resource_allocation.resource_strategy = desired_strategy;
    end
end

function settings = configure_phase_runtime_output_paths(settings, paths)
    if nargin < 1 || ~isstruct(settings)
        settings = struct();
    end
    if nargin < 2 || ~isstruct(paths)
        return;
    end

    if ~isfield(settings, 'sustainability') || ~isstruct(settings.sustainability)
        settings.sustainability = struct();
    end
    if ~isfield(settings.sustainability, 'collector_runtime') || ...
            ~isstruct(settings.sustainability.collector_runtime)
        settings.sustainability.collector_runtime = struct();
    end

    settings.sustainability.collector_runtime.session_output_dir = pick_text(paths, {'metrics_root'}, '');
    settings.sustainability.collector_runtime.hwinfo_csv_target_dir = pick_text(paths, {'metrics_root'}, '');
    settings.sustainability.collector_runtime.hwinfo_csv_target_path = pick_text(paths, {'raw_hwinfo_csv_path'}, '');
end

function initialize_phase1_queue_artifacts(jobs, phase_cfg)
    if nargin < 1 || ~isstruct(jobs) || isempty(jobs)
        return;
    end
    for i = 1:numel(jobs)
        if ~isfield(jobs(i), 'output_root') || isempty(jobs(i).output_root)
            continue;
        end
        paths = build_phase1_job_paths(jobs(i).output_root, pick_text(jobs(i), {'method'}, 'FD'), pick_text(jobs(i), {'stage'}, ''));
        ensure_phase_directories(paths);
        PathBuilder.ensure_run_settings_placeholder(paths.run_settings_path, pick_text(jobs(i), {'job_key'}, sprintf('job_%02d', i)));
        levels = double(reshape(pick_value(jobs(i).parameters, 'mesh_sizes', []), 1, []));
        if isempty(levels)
            levels = resolve_phase1_expected_levels(jobs(i), phase_cfg);
        end
        for li = 1:numel(levels)
            level_dir = phase1_mesh_level_dir_name(li, levels(li));
            target = fullfile(paths.levels_root, level_dir);
            level_paths = PathBuilder.get_existing_root_paths(target, pick_text(jobs(i), {'method'}, 'FD'), 'Evolution');
            PathBuilder.ensure_directories(level_paths);
            PathBuilder.ensure_run_settings_placeholder(level_paths.run_settings_path, sprintf('%s_%s', ...
                pick_text(jobs(i), {'job_key'}, sprintf('job_%02d', i)), level_dir));
        end
    end
end

function levels = resolve_phase1_expected_levels(job, phase_cfg)
    levels = [];
    if ~isstruct(job)
        return;
    end
    if strcmpi(pick_text(job, {'stage'}, ''), 'convergence')
        levels = double(reshape(pick_value(job.parameters, 'mesh_sizes', []), 1, []));
        if isempty(levels)
            levels = double(reshape(resolve_phase1_mesh_levels_from_job(job, phase_cfg), 1, []));
        end
    end
end

function levels = resolve_phase1_mesh_levels_from_job(job, phase_cfg)
    method_key = normalize_method_key(pick_text(job, {'method'}, 'FD'));
    if strcmpi(method_key, 'spectral')
        levels = pick_value(phase_cfg, 'convergence_mesh_levels_spectral', []);
    else
        levels = pick_value(phase_cfg, 'convergence_mesh_levels_fd', []);
    end
end

function level_dir = phase1_mesh_level_dir_name(level_index, mesh_n)
    if nargin < 2 || ~isfinite(double(mesh_n))
        level_dir = sprintf('L%02d', round(double(level_index)));
    else
        level_dir = sprintf('L%02d_%03d', round(double(level_index)), round(double(mesh_n)));
    end
end

function progress_callback = resolve_progress_callback(Settings)
    progress_callback = resolve_runtime_progress_callback(Settings);
end

function jobs = build_phase_queue_jobs(phase_id, params, settings, paths, Run_Config)
    fd_mesh = [];
    sp_mesh = [];
    mesh_workflow = is_mesh_convergence_workflow(Run_Config);
    phase_cfg = pick_value(params, 'phase1', struct());
    study_catalog = resolve_phase1_ic_study_catalog(phase_cfg);

    if mesh_workflow
        jobs = repmat(empty_job(), 1, 2);
        jobs(1) = build_convergence_job('FD', 1, phase_id, params, settings, paths, fd_mesh);
        jobs(2) = build_convergence_job('Spectral', 2, phase_id, params, settings, paths, sp_mesh);
        return;
    end

    jobs = repmat(empty_job(), 1, 2 + 2 * numel(study_catalog));
    jobs(1) = build_main_comparison_job('FD', 1, phase_id, params, settings, paths);
    jobs(2) = build_main_comparison_job('Spectral', 2, phase_id, params, settings, paths);
    queue_index = 3;
    for case_idx = 1:numel(study_catalog)
        jobs(queue_index) = build_ic_study_job('FD', queue_index, phase_id, params, settings, paths, ...
            study_catalog(case_idx), fd_mesh);
        jobs(queue_index + 1) = build_ic_study_job('Spectral', queue_index + 1, phase_id, params, settings, paths, ...
            study_catalog(case_idx), sp_mesh);
        queue_index = queue_index + 2;
    end
end

function job = build_main_comparison_job(method_name, queue_index, phase_id, params, settings, paths)
    p = apply_phase1_direct_method_settings(params, method_name);
    p = apply_phase1_taylor_green_timestep_cap(p, 'phase1_periodic_comparison');
    p.method = method_to_parameter_token(method_name);
    p.analysis_method = method_name;
    p.mode = 'Evolution';
    p.run_mode_internal = 'Evolution';
    p.time_integrator = resolve_phase_method_integrator(method_name);
    plot_snapshot_count = resolve_phase1_plot_snapshot_count(p);
    animation_frame_count = resolve_phase1_animation_frame_count(p, settings);
    p.create_animations = false;
    p.num_plot_snapshots = plot_snapshot_count;
    p.animation_num_frames = animation_frame_count;
    p.num_animation_frames = animation_frame_count;
    p.num_snapshots = max(plot_snapshot_count, animation_frame_count);
    p = apply_phase1_snapshot_memory_policy(p);
    p = normalize_snapshot_schedule_parameters(p);

    rc = Build_Run_Config(method_name, 'Evolution', pick_text(p, {'ic_type'}, pick_text(params, {'ic_type'}, '')));
    rc.run_id = make_phase_child_identifier(phase_id, method_name, 'periodic');
    rc.phase_id = phase_id;
    rc.phase_stage = 'periodic_comparison';
    rc.ic_type = pick_text(p, {'ic_type'}, pick_text(params, {'ic_type'}, ''));
    baseline_meta = phase1_baseline_case_metadata(pick_text(p, {'ic_type'}, pick_text(params, {'ic_type'}, 'stretched_gaussian')));
    rc.phase1_publication_case_id = baseline_meta.case_id;
    rc.phase1_publication_case_label = baseline_meta.label;

    job_key = sprintf('%s_periodic', normalize_method_key(method_name));
    job = make_job(sprintf('%s direct periodic evolution', method_name), method_name, 'evolution', job_key, ...
        queue_index, rc, p, settings, paths);
end

function meta = phase1_baseline_case_metadata(ic_type)
    token = lower(strtrim(char(string(ic_type))));
    switch token
        case {'elliptical_vortex', 'elliptic_vortex', 'elliptic'}
            meta = struct('ic_type', 'elliptical_vortex', 'case_id', 'baseline_elliptic_single', 'label', 'Elliptic');
        case {'taylor_green', 'taylor-green', 'taylorgreen'}
            meta = struct('ic_type', 'taylor_green', 'case_id', 'taylor_green', 'label', 'Taylor-Green');
        otherwise
            meta = struct('ic_type', 'stretched_gaussian', 'case_id', 'baseline_stretched_single', 'label', 'Stretched Gaussian');
    end
end

function params = apply_phase1_direct_method_settings(params, method_name)
    phase_cfg = pick_value(params, 'phase1', struct());
    method_key = normalize_method_key(method_name);
    is_taylor_green = strcmpi(pick_text(params, {'ic_type'}, ''), 'taylor_green');
    if strcmpi(method_key, 'spectral')
        grid_n = max(8, round(pick_numeric(phase_cfg, {'spectral_grid_n'}, pick_numeric(params, {'Nx'}, 128))));
        dt_value = pick_numeric(phase_cfg, {'spectral_dt'}, pick_numeric(params, {'dt'}, 0.01));
        if is_taylor_green
            grid_n = max(8, round(pick_numeric(phase_cfg, {'taylor_green_spectral_grid_n'}, grid_n)));
            dt_value = pick_numeric(phase_cfg, {'taylor_green_spectral_dt'}, dt_value);
        end
    else
        grid_n = max(8, round(pick_numeric(phase_cfg, {'fd_grid_n'}, pick_numeric(params, {'Nx'}, 128))));
        dt_value = pick_numeric(phase_cfg, {'fd_dt'}, pick_numeric(params, {'dt'}, 0.01));
        if is_taylor_green
            grid_n = max(8, round(pick_numeric(phase_cfg, {'taylor_green_fd_grid_n'}, grid_n)));
            dt_value = pick_numeric(phase_cfg, {'taylor_green_fd_dt'}, dt_value);
        end
    end
    params.Nx = grid_n;
    params.Ny = grid_n;
    params.dt = dt_value;
    params.taylor_green_honor_fixed_dt = is_taylor_green && ...
        logical(pick_value(phase_cfg, 'taylor_green_honor_fixed_dt', true));
end

function params = apply_phase1_taylor_green_timestep_cap(params, context_label)
    phase_cfg = pick_value(params, 'phase1', struct());
    adaptive_cfg = pick_value(phase_cfg, 'adaptive_timestep', struct('enabled', false, 'C_adv', 0.5, 'C_diff', 0.25));
    [params, meta] = apply_taylor_green_timestep_cap(params, adaptive_cfg, context_label);
    if meta.applied
        if ~isfield(params, 'phase1') || ~isstruct(params.phase1)
            params.phase1 = struct();
        end
        params.phase1.adaptive_timestep = adaptive_cfg;
        params.phase1.adaptive_timestep.enabled = false;
        params.phase1_adaptive_timestep_enabled = false;
        params.phase1_taylor_green_timestep_meta = meta;
    end
end

function params = apply_phase1_snapshot_memory_policy(params)
    phase_cfg = pick_value(params, 'phase1', struct());
    params.snapshot_storage_precision = pick_text(phase_cfg, {'snapshot_storage_precision'}, ...
        pick_text(params, {'snapshot_storage_precision'}, 'double'));
    params.store_velocity_snapshot_cubes = logical(pick_value(phase_cfg, ...
        'store_velocity_snapshot_cubes', pick_value(params, 'store_velocity_snapshot_cubes', true)));
    params.store_native_velocity_snapshots = logical(pick_value(phase_cfg, ...
        'store_native_velocity_snapshots', pick_value(params, 'store_native_velocity_snapshots', true)));
end

function mesh = direct_phase1_mesh_entry(method_name, phase_cfg)
    method_key = normalize_method_key(method_name);
    mesh = ConvergedMeshRegistry.empty_entry();
    mesh.method = method_key;
    ic_snapshot = pick_struct(phase_cfg, {'ic_snapshot'}, struct());
    is_taylor_green = strcmpi(pick_text(ic_snapshot, {'ic_type'}, pick_text(phase_cfg, {'ic_type'}, '')), 'taylor_green');
    if strcmpi(method_key, 'spectral')
        mesh.Nx = max(8, round(pick_numeric(phase_cfg, {'spectral_grid_n'}, NaN)));
        if is_taylor_green
            mesh.Nx = max(8, round(pick_numeric(phase_cfg, {'taylor_green_spectral_grid_n'}, mesh.Nx)));
        end
        mesh.Ny = mesh.Nx;
        mesh.dt = pick_numeric(phase_cfg, {'spectral_dt'}, NaN);
        if is_taylor_green
            mesh.dt = pick_numeric(phase_cfg, {'taylor_green_spectral_dt'}, mesh.dt);
        end
    else
        mesh.Nx = max(8, round(pick_numeric(phase_cfg, {'fd_grid_n'}, NaN)));
        if is_taylor_green
            mesh.Nx = max(8, round(pick_numeric(phase_cfg, {'taylor_green_fd_grid_n'}, mesh.Nx)));
        end
        mesh.Ny = mesh.Nx;
        mesh.dt = pick_numeric(phase_cfg, {'fd_dt'}, NaN);
        if is_taylor_green
            mesh.dt = pick_numeric(phase_cfg, {'taylor_green_fd_dt'}, mesh.dt);
        end
    end
    mesh.verdict = 'direct_phase_config';
    mesh.status = 'direct_phase_config';
    mesh.selection_reason = 'direct_phase_config';
    mesh.tolerance = NaN;
    mesh.fallback_used = false;
    mesh.continued_after_unconverged_mesh = false;
    mesh.selected_level = NaN;
    mesh.selected_mesh_index = NaN;
end

function mesh = direct_phase1_mesh_entry_from_output(method_name, method_output, phase_cfg)
    if nargin < 3 || ~isstruct(phase_cfg)
        phase_cfg = struct();
    end
    mesh = direct_phase1_mesh_entry(method_name, phase_cfg);
    if nargin < 2 || ~isstruct(method_output)
        return;
    end

    params = pick_struct(method_output, {'parameters'}, struct());
    run_cfg = pick_struct(method_output, {'run_config'}, struct());
    if isempty(fieldnames(params)) && isempty(fieldnames(run_cfg))
        return;
    end

    mesh.Nx = max(8, round(pick_numeric(params, {'Nx'}, mesh.Nx)));
    mesh.Ny = max(8, round(pick_numeric(params, {'Ny'}, mesh.Nx)));
    mesh.dt = pick_numeric(params, {'dt'}, mesh.dt);
    mesh.status = 'direct_phase_output';
    mesh.verdict = 'direct_phase_output';
    mesh.selection_reason = 'direct_phase_output';

    ic_type = pick_text(params, {'ic_type'}, pick_text(run_cfg, {'ic_type'}, pick_text(phase_cfg, {'ic_type'}, '')));
    if strcmpi(ic_type, 'taylor_green')
        mesh.continued_after_unconverged_mesh = false;
    end
end

function job = build_convergence_job(method_name, queue_index, phase_id, params, settings, paths, mesh_override)
    p = params;
    if nargin >= 8 && ~isempty(mesh_override)
        p = ConvergedMeshRegistry.apply_to_parameters(p, mesh_override);
    end
    p = apply_phase1_direct_method_settings(p, method_name);
    p.method = method_to_parameter_token(method_name);
    p.analysis_method = method_name;
    p.mode = 'Evolution';
    p.run_mode_internal = 'Evolution';
    p.phase1_convergence_runtime = 'local_mesh_sweep';
    p.time_integrator = resolve_phase_method_integrator(method_name);
    p = apply_phase1_convergence_ladder(p, method_name);

    rc = Build_Run_Config(method_name, 'Evolution', pick_text(params, {'ic_type'}, ''));
    rc.study_id = make_phase_child_identifier(phase_id, method_name, 'convergence');
    rc.phase_id = phase_id;
    rc.phase_stage = 'mesh_sweep';

    job_key = sprintf('%s_convergence', normalize_method_key(method_name));
    job = make_job(sprintf('%s convergence', method_name), method_name, 'convergence', job_key, ...
        queue_index, rc, p, settings, paths);
end

function job = build_ic_study_job(method_name, queue_index, phase_id, params, settings, paths, case_cfg, mesh_override)
    p = params;
    p = apply_phase1_ic_study_case(p, case_cfg);
    p = apply_phase1_direct_method_settings(p, method_name);
    if nargin >= 9 && ~isempty(mesh_override)
        p = ConvergedMeshRegistry.apply_to_parameters(p, mesh_override);
    end
    p = apply_phase1_taylor_green_timestep_cap(p, 'phase1_ic_study');
    p.method = method_to_parameter_token(method_name);
    p.analysis_method = method_name;
    p.mode = 'Evolution';
    p.run_mode_internal = 'Evolution';
    p.time_integrator = resolve_phase_method_integrator(method_name);
    plot_snapshot_count = resolve_phase1_plot_snapshot_count(params);
    animation_frame_count = resolve_phase1_animation_frame_count(params, settings);
    p.create_animations = false;
    p.num_plot_snapshots = plot_snapshot_count;
    p.animation_num_frames = animation_frame_count;
    p.num_animation_frames = animation_frame_count;
    p.num_snapshots = max(plot_snapshot_count, animation_frame_count);
    p = apply_phase1_snapshot_memory_policy(p);
    p = normalize_snapshot_schedule_parameters(p);
    p.phase1_ic_study_case_id = char(string(case_cfg.case_id));
    p.phase1_ic_study_case_label = char(string(case_cfg.label));
    p.phase1_ic_study_group_label = char(string(case_cfg.group_label));

    rc = Build_Run_Config(method_name, 'Evolution', pick_text(p, {'ic_type'}, pick_text(params, {'ic_type'}, '')));
    rc.run_id = make_phase_child_identifier(phase_id, method_name, sprintf('ic_%s', char(string(case_cfg.case_id))));
    rc.phase_id = phase_id;
    rc.phase_stage = 'ic_study';
    rc.phase1_ic_study_case_id = char(string(case_cfg.case_id));
    rc.phase1_ic_study_case_label = char(string(case_cfg.label));
    rc.ic_type = pick_text(p, {'ic_type'}, pick_text(params, {'ic_type'}, ''));
    rc.phase1_publication_case_id = char(string(case_cfg.case_id));
    rc.phase1_publication_case_label = phase1_case_display_label(case_cfg.case_id, case_cfg.label);

    job_key = sprintf('%s_ic_%s', normalize_method_key(method_name), char(string(case_cfg.case_id)));
    job = make_job(sprintf('%s | %s', method_name, char(string(case_cfg.label))), ...
        method_name, 'ic_study', job_key, queue_index, rc, p, settings, paths);
end

function params = apply_phase1_ic_study_case(params, case_cfg)
    if ~isstruct(case_cfg)
        return;
    end
    reset_defaults = struct( ...
        'ic_coeff', [], ...
        'ic_coeff1', 0.0, ...
        'ic_coeff2', 0.0, ...
        'ic_coeff3', 0.0, ...
        'ic_coeff4', 0.0, ...
        'ic_center_x', 0.0, ...
        'ic_center_y', 0.0, ...
        'ic_scale', 1.0, ...
        'ic_amplitude', 1.0, ...
        'ic_count', 1, ...
        'ic_pattern', 'single', ...
        'ic_arrangement', 'single', ...
        'ic_dynamic_values', struct(), ...
        'ic_multi_vortex_rows', struct([]), ...
        'ic_multi_vortex_experimental', false);
    reset_fields = fieldnames(reset_defaults);
    for i = 1:numel(reset_fields)
        params.(reset_fields{i}) = reset_defaults.(reset_fields{i});
    end

    overrides = pick_struct(case_cfg, {'runtime_overrides'}, struct());
    text_fields = {'ic_type', 'ic_scenario', 'ic_pattern', 'ic_arrangement'};
    for i = 1:numel(text_fields)
        key = text_fields{i};
        if isfield(overrides, key) && ~isempty(overrides.(key))
            params.(key) = char(string(overrides.(key)));
        end
    end

    numeric_fields = {'ic_center_x', 'ic_center_y', 'ic_scale', 'ic_amplitude', ...
        'ic_coeff1', 'ic_coeff2', 'ic_coeff3', 'ic_coeff4', 'nu'};
    for i = 1:numel(numeric_fields)
        key = numeric_fields{i};
        if isfield(overrides, key) && isnumeric(overrides.(key)) && isscalar(overrides.(key)) && ...
                isfinite(overrides.(key))
            params.(key) = double(overrides.(key));
        end
    end

    if isfield(overrides, 'ic_coeff') && isnumeric(overrides.ic_coeff)
        params.ic_coeff = double(overrides.ic_coeff(:).');
    end
    if isfield(overrides, 'ic_count') && isnumeric(overrides.ic_count) && isscalar(overrides.ic_count) && ...
            isfinite(overrides.ic_count)
        params.ic_count = max(1, round(double(overrides.ic_count)));
    end
    if isfield(overrides, 'ic_dynamic_values') && isstruct(overrides.ic_dynamic_values)
        params.ic_dynamic_values = overrides.ic_dynamic_values;
    end
    if isfield(overrides, 'ic_multi_vortex_rows')
        params.ic_multi_vortex_rows = overrides.ic_multi_vortex_rows;
    end
    if isfield(overrides, 'ic_multi_vortex_experimental')
        params.ic_multi_vortex_experimental = logical(overrides.ic_multi_vortex_experimental);
    else
        params.ic_multi_vortex_experimental = false;
    end
end

function count = resolve_phase1_plot_snapshot_count(params)
    count = max(1, round(double(pick_value(pick_value(params, 'phase1', struct()), ...
        'num_plot_snapshots', pick_value(params, 'num_plot_snapshots', 9)))));
end

function count = resolve_phase1_animation_frame_count(params, settings)
    count = NaN;
    phase_cfg = pick_value(params, 'phase1', struct());
    candidate_fields = {'animation_num_frames', 'num_animation_frames', 'animation_frame_count'};
    for i = 1:numel(candidate_fields)
        if isfield(phase_cfg, candidate_fields{i}) && isnumeric(phase_cfg.(candidate_fields{i})) && ...
                isscalar(phase_cfg.(candidate_fields{i})) && isfinite(phase_cfg.(candidate_fields{i}))
            count = double(phase_cfg.(candidate_fields{i}));
            break;
        end
    end
    if ~isfinite(count)
        for i = 1:numel(candidate_fields)
            if isfield(params, candidate_fields{i}) && isnumeric(params.(candidate_fields{i})) && ...
                    isscalar(params.(candidate_fields{i})) && isfinite(params.(candidate_fields{i}))
                count = double(params.(candidate_fields{i}));
                break;
            end
        end
    end
    if ~isfinite(count) && nargin >= 2 && isstruct(settings)
        for i = 1:numel(candidate_fields)
            if isfield(settings, candidate_fields{i}) && isnumeric(settings.(candidate_fields{i})) && ...
                    isscalar(settings.(candidate_fields{i})) && isfinite(settings.(candidate_fields{i}))
                count = double(settings.(candidate_fields{i}));
                break;
            end
        end
        if ~isfinite(count) && isfield(settings, 'media') && isstruct(settings.media) && ...
                isfield(settings.media, 'frame_count') && isnumeric(settings.media.frame_count) && ...
                isscalar(settings.media.frame_count) && isfinite(settings.media.frame_count)
            count = double(settings.media.frame_count);
        end
    end
    if ~isfinite(count)
        count = resolve_phase1_plot_snapshot_count(params);
    end
    count = max(2, round(count));
end

function outputs = run_phase_queue(jobs, progress_callback, phase_id, phase_timer, phase_cfg, telemetry_context, workflow_kind)
    outputs = repmat(empty_output(), 1, numel(jobs));
    workflow_label = workflow_display_name(strcmpi(char(string(workflow_kind)), 'mesh_convergence_study'));
    for i = 1:numel(jobs)
        emit_phase_queue_payload(progress_callback, phase_id, jobs(i), 'queued', 0, toc(phase_timer), ...
            sprintf('Queued %s child job %d/%d: %s', workflow_label, i, numel(jobs), jobs(i).label), ...
            jobs, outputs, workflow_kind);
    end

    for i = 1:numel(jobs)
        running_pct = 100 * ((i - 1) / max(numel(jobs), 1));
        emit_phase_queue_payload(progress_callback, phase_id, jobs(i), 'running', running_pct, toc(phase_timer), ...
            sprintf('Starting %s child job %d/%d: %s', workflow_label, i, numel(jobs), jobs(i).label), ...
            jobs, outputs, workflow_kind);
        append_phase1_job_boundary(telemetry_context, 'start', jobs(i), toc(phase_timer));
        try
            execution_mode = 'dispatcher_queue';
            if strcmp(jobs(i).stage, 'convergence')
                [result_payload, path_payload] = run_local_phase1_mesh_sweep( ...
                    jobs(i), phase_cfg, progress_callback, phase_timer, telemetry_context);
                execution_mode = 'phase1_local_mesh_sweep';
            else
                [result_payload, path_payload] = run_dispatched_job(jobs(i).run_config, jobs(i).parameters, jobs(i).settings);
            end
            outputs(i) = make_output(jobs(i), result_payload, path_payload, execution_mode);
            outputs(i) = promote_output_quick_access(outputs(i), jobs(i).output_root);
            if strcmp(jobs(i).stage, 'convergence')
                mesh = select_mesh_from_convergence(result_payload, path_payload, phase_cfg);
                outputs(i).selected_mesh = mesh;
                if i < numel(jobs)
                    outputs = inject_selected_mesh_into_queue(outputs, jobs, mesh, jobs(i).method_key);
                    jobs = propagate_selected_mesh(jobs, mesh, jobs(i).method_key);
                end
            end
            completed_pct = 100 * (i / max(numel(jobs), 1));
            append_phase1_job_boundary(telemetry_context, 'end', jobs(i), toc(phase_timer));
            emit_phase_queue_payload(progress_callback, phase_id, jobs(i), 'completed', completed_pct, toc(phase_timer), ...
                build_child_completion_message(jobs(i), outputs(i), i, numel(jobs), workflow_label), ...
                jobs, outputs, workflow_kind);
        catch ME
            outputs(i) = make_failed_output(jobs(i), ME);
            append_phase1_job_boundary(telemetry_context, 'end', jobs(i), toc(phase_timer));
            emit_phase_queue_payload(progress_callback, phase_id, jobs(i), 'failed', NaN, toc(phase_timer), ...
                sprintf('%s child job failed: [%s] %s', workflow_label, ME.identifier, ME.message), ...
                jobs, outputs, workflow_kind);
            rethrow(ME);
        end
    end
end

function [result_payload, path_payload] = run_dispatched_job(run_config, parameters, settings)
    child_run_config = run_config;
    if isfield(child_run_config, 'workflow_kind')
        child_run_config = rmfield(child_run_config, 'workflow_kind');
    end
    [result_payload, path_payload] = run_phase1_child_dispatch(child_run_config, parameters, settings);
end

function append_phase1_job_boundary(telemetry_context, boundary_event, job, elapsed_wall)
    if nargin < 4
        elapsed_wall = NaN;
    end
    if ~(isstruct(telemetry_context) && isfield(telemetry_context, 'enabled') && logical(telemetry_context.enabled))
        return;
    end

    stage_id = sprintf('%s_%s', char(string(job.method_key)), char(string(job.stage)));
    stage_label = sprintf('%s %s', phase1_method_display_label(job.method_key), ...
        phase1_stage_label(char(string(job.stage))));
    substage_id = '';
    substage_label = '';
    substage_type = '';
    if strcmp(job.stage, 'ic_study')
        substage_id = pick_text(job.run_config, {'phase1_ic_study_case_id'}, '');
        substage_label = pick_text(job.run_config, {'phase1_ic_study_case_label'}, job.label);
        substage_type = 'ic_case';
    end

    PhaseTelemetryCSVFirst.append_boundary(telemetry_context, boundary_event, struct( ...
        'session_time_s', double(elapsed_wall), ...
        'stage_id', stage_id, ...
        'stage_label', stage_label, ...
        'stage_type', char(string(job.stage)), ...
        'substage_id', substage_id, ...
        'substage_label', substage_label, ...
        'substage_type', substage_type, ...
        'stage_method', phase1_method_display_label(job.method_key), ...
        'scenario_id', '', ...
        'mesh_level', NaN, ...
        'mesh_nx', pick_numeric(job.parameters, {'Nx'}, NaN), ...
        'mesh_ny', pick_numeric(job.parameters, {'Ny'}, NaN), ...
        'child_run_index', double(job.queue_index)));
end

function append_phase1_mesh_level_boundary(telemetry_context, boundary_event, job, level_index, mesh_n, elapsed_wall)
    if nargin < 6
        elapsed_wall = NaN;
    end
    if ~(isstruct(telemetry_context) && isfield(telemetry_context, 'enabled') && logical(telemetry_context.enabled))
        return;
    end
    level_label = sprintf('L%02d', round(double(level_index)));
    PhaseTelemetryCSVFirst.append_boundary(telemetry_context, boundary_event, struct( ...
        'session_time_s', double(elapsed_wall), ...
        'stage_id', sprintf('%s_convergence', char(string(job.method_key))), ...
        'stage_label', sprintf('%s Convergence', phase1_method_display_label(job.method_key)), ...
        'stage_type', 'convergence', ...
        'substage_id', level_label, ...
        'substage_label', level_label, ...
        'substage_type', 'mesh_level', ...
        'stage_method', phase1_method_display_label(job.method_key), ...
        'scenario_id', '', ...
        'mesh_level', double(level_index), ...
        'mesh_nx', double(mesh_n), ...
        'mesh_ny', double(mesh_n), ...
        'child_run_index', double(job.queue_index)));
end

function label = phase1_stage_label(stage_token)
    switch lower(strtrim(char(string(stage_token))))
        case 'convergence'
            label = 'Convergence';
        case 'ic_study'
            label = 'IC Study';
        otherwise
            label = char(string(stage_token));
    end
end

function [results, paths] = run_local_phase1_mesh_sweep(job, phase_cfg, progress_callback, phase_timer, telemetry_context)
    levels = double(reshape(pick_value(job.parameters, 'mesh_sizes', []), 1, []));
    if isempty(levels)
        error('Phase1PeriodicComparison:MissingMeshSweepLevels', ...
            'Phase 1 local mesh sweep requires a resolved mesh_sizes ladder from the Phase 1 UI/config path.');
    end

    paths = build_phase1_job_paths(job.output_root, job.method, job.stage);
    ensure_phase_directories(paths);
    safe_save_mat(fullfile(paths.config, 'Config.mat'), struct( ...
        'Run_Config_clean', filter_graphics_objects(job.run_config), ...
        'Parameters_clean', filter_graphics_objects(job.parameters), ...
        'Settings_clean', filter_graphics_objects(job.settings), ...
        'phase_cfg_clean', filter_graphics_objects(phase_cfg)));
    write_run_settings_text(paths.run_settings_path, ...
        'Run Config', job.run_config, ...
        'Parameters', job.parameters, ...
        'Settings', job.settings, ...
        'Phase Config', phase_cfg);

    n_levels = numel(levels);
    run_records = repmat(empty_phase1_mesh_record(), n_levels, 1);
    level_outputs = repmat(struct( ...
        'results', struct(), ...
        'paths', struct(), ...
        'analysis', struct(), ...
        'record', empty_phase1_mesh_record()), n_levels, 1);
    level_timer = tic;

    SafeConsoleIO.fprintf('Phase 1 mesh sweep | %s | mode=%s | ladder=%s\n', ...
        phase1_method_display_label(job.method_key), ...
        phase1_mesh_ladder_mode_text(pick_text(phase_cfg, {'mesh_ladder_mode'}, 'bounded')), ...
        phase1_mesh_ladder_text(levels));

    for i = 1:n_levels
        mesh_n = round(double(levels(i)));
        level_params = job.parameters;
        level_params = apply_phase1_direct_method_settings(level_params, job.method);
        level_params.Nx = mesh_n;
        level_params.Ny = mesh_n;
        level_params = apply_phase1_taylor_green_timestep_cap(level_params, 'phase1_mesh_convergence');
        level_params.mode = 'Evolution';
        level_params.run_mode_internal = 'Evolution';
        level_params.create_animations = false;
        if ~isfield(level_params, 'phase1') || ~isstruct(level_params.phase1)
            level_params.phase1 = struct();
        end
        if ~isfield(level_params.phase1, 'adaptive_timestep') || ~isstruct(level_params.phase1.adaptive_timestep)
            level_params.phase1.adaptive_timestep = struct();
        end
        level_params.phase1.adaptive_timestep.enabled = false;

        level_run_config = build_phase1_mesh_sweep_run_config(job.run_config, job.method, i);
        level_progress_callback = build_phase1_mesh_sweep_level_progress_callback( ...
            progress_callback, job, i, n_levels, mesh_n, phase_timer, levels);
        level_settings = build_phase1_mesh_sweep_settings(job.settings, paths, i, mesh_n, level_progress_callback);
        emit_phase1_mesh_sweep_level_start_progress(progress_callback, job, i, n_levels, mesh_n, ...
            toc(phase_timer), levels);
        append_phase1_mesh_level_boundary(telemetry_context, 'start', job, i, mesh_n, toc(phase_timer));

        mesh_level_summary_path = fullfile(paths.data, sprintf('mesh_level_%02d.mat', i));
        level_results = struct();
        level_paths = struct();
        level_analysis = struct();
        save_analysis = struct();
        try
            [level_results, level_paths] = run_phase1_child_dispatch(level_run_config, level_params, level_settings);
            level_analysis = require_analysis(level_results, sprintf('%s Phase 1 mesh sweep', job.method));
            record = build_phase1_mesh_sweep_record(job.method, level_params, level_results, level_analysis, i);
            record.xi_tol = pick_numeric(phase_cfg, {'convergence_tolerance'}, NaN);
            previous_stable_index = find(arrayfun(@(candidate) logical(candidate.level_stable), run_records(1:i-1)), 1, 'last');
            if record.level_stable && ~isempty(previous_stable_index)
                comparison = compute_phase1_successive_relative_change(level_outputs(previous_stable_index).analysis, level_analysis);
                record.comparison_source_index = double(previous_stable_index);
                record.omega_diff_l2 = comparison.omega_diff_l2;
                record.omega_curr_l2 = comparison.omega_curr_l2;
                record.error_L2 = comparison.omega_diff_l2;
                record.xi_fraction = comparison.xi_fraction;
                record.relative_change_fraction = comparison.xi_fraction;
                record.xi = comparison.xi_percent;
                record.relative_change = comparison.xi_percent;
                record.peak_vorticity_prev = comparison.peak_vorticity_prev;
                record.peak_vorticity_curr = comparison.peak_vorticity_curr;
                record.max_vorticity_rel_error_fraction = comparison.max_vorticity_rel_error_fraction;
                record.max_vorticity_rel_error_pct = comparison.max_vorticity_rel_error_pct;
                record.observed_rate = compute_phase1_mesh_observed_rate(level_outputs(previous_stable_index).record, record);
            elseif record.level_stable
                record.comparison_source_index = NaN;
            end
            if ~record.level_stable
                SafeConsoleIO.fprintf('Phase 1 mesh sweep | %s | %s destabilized: %s\n', ...
                    phase1_method_display_label(job.method_key), record.mesh_level_label, record.failure_reason);
            end
            save_analysis = level_analysis;
        catch ME
            record = build_phase1_failed_mesh_sweep_record(job.method, level_params, i, ME);
            record.xi_tol = pick_numeric(phase_cfg, {'convergence_tolerance'}, NaN);
            level_results = struct('error_identifier', ME.identifier, 'error_message', ME.message);
            level_paths = struct();
            save_analysis = struct('error_identifier', ME.identifier, 'error_message', ME.message);
            SafeConsoleIO.fprintf('Phase 1 mesh sweep | %s | %s failed: %s\n', ...
                phase1_method_display_label(job.method_key), record.mesh_level_label, ME.message);
        end
        record.mesh_level_summary_path = mesh_level_summary_path;

        run_records(i) = record;
        level_outputs(i).results = level_results;
        level_outputs(i).paths = level_paths;
        level_outputs(i).analysis = level_analysis;
        level_outputs(i).record = record;

        log_phase1_mesh_sweep_record(record);

        safe_save_mat(mesh_level_summary_path, struct( ...
            'record', filter_graphics_objects(record), ...
            'results_summary', filter_graphics_objects(strip_phase_view_summary_for_persistence(struct( ...
                'results', level_results, ...
                'analysis', save_analysis))), ...
            'analysis', filter_graphics_objects(save_analysis), ...
            'run_config', filter_graphics_objects(level_run_config), ...
            'parameters', filter_graphics_objects(level_params), ...
            'paths', filter_graphics_objects(level_paths)), '-v7.3');

        append_phase1_mesh_level_boundary(telemetry_context, 'end', job, i, mesh_n, toc(phase_timer));
        emit_phase1_mesh_sweep_progress(progress_callback, job, i, n_levels, mesh_n, ...
            record, toc(phase_timer), levels);
    end

    [run_records, stage_summary, summary_payload] = finalize_phase1_mesh_sweep_records(run_records, levels, phase_cfg);
    mesh_level_animation = build_phase1_mesh_level_animation_payload(job, level_outputs, levels);
    results = build_phase1_mesh_sweep_results(job, phase_cfg, paths, run_records, stage_summary, summary_payload, ...
        mesh_level_animation, toc(level_timer));
    safe_save_mat(fullfile(paths.data, 'convergence_results.mat'), struct('Results', filter_graphics_objects(results)), '-v7.3');
    if json_saving_enabled(phase_cfg, job.parameters)
        write_json(fullfile(paths.data, 'convergence_results.json'), filter_graphics_objects(results));
    end
    write_phase1_mesh_sweep_report(results, paths);
end

function paths = build_phase1_job_paths(base_root, method_name, stage_name)
    if nargin < 2 || isempty(method_name)
        method_name = 'FD';
    end
    if nargin < 3 || isempty(stage_name)
        stage_name = 'ic_study';
    end
    mode_name = 'Evolution';
    if strcmpi(char(string(stage_name)), 'convergence')
        mode_name = 'Convergence';
    end
    paths = PathBuilder.get_existing_root_paths(base_root, method_name, mode_name);
    paths.scratch_root = '';
    if strcmpi(mode_name, 'Convergence')
        paths.levels_root = fullfile(paths.data, 'Levels');
    else
        paths.levels_root = '';
    end
end

function run_config = build_phase1_mesh_sweep_run_config(job_run_config, method_name, level_index)
    ic_type = pick_text(job_run_config, {'ic_type'}, '');
    run_config = Build_Run_Config(method_name, 'Evolution', ic_type);
    parent_id = pick_text(job_run_config, {'study_id', 'run_id'}, sprintf('p1%s', compact_phase_stage_token(method_name, 'convergence')));
    run_config.run_id = sprintf('%sl%02d', parent_id, round(double(level_index)));
    run_config.phase_id = pick_text(job_run_config, {'phase_id'}, '');
    run_config.phase_label = 'Phase 1';
    run_config.phase_stage = 'mesh_sweep_level';
end

function level_settings = build_phase1_mesh_sweep_settings(job_settings, paths, level_index, mesh_n, level_progress_callback)
    level_settings = job_settings;
    if nargin < 5
        level_progress_callback = [];
    end
    if nargin < 4 || ~isfinite(double(mesh_n))
        mesh_n = NaN;
    end
    level_dir = phase1_mesh_level_dir_name(level_index, mesh_n);
    level_settings.output_root = fullfile(paths.levels_root, level_dir);
    level_settings.preinitialized_artifact_root = true;
    level_settings.save_data = false;
    level_settings.save_reports = false;
    level_settings.save_figures = false;
    level_settings.append_to_master = false;
    level_settings.animation_enabled = false;
    if isfield(level_settings, 'media') && isstruct(level_settings.media)
        level_settings.media.enabled = false;
    end
    if isfield(level_settings, 'ui_progress_callback')
        level_settings = rmfield(level_settings, 'ui_progress_callback');
    end
    if isfield(level_settings, 'progress_data_queue')
        level_settings = rmfield(level_settings, 'progress_data_queue');
    end
    if ~isfield(level_settings, 'compatibility') || ~isstruct(level_settings.compatibility)
        level_settings.compatibility = struct();
    end
    level_settings.compatibility.return_analysis = true;
    if isa(level_progress_callback, 'function_handle')
        level_settings.ui_progress_callback = level_progress_callback;
    end
end

function progress_callback = build_phase1_mesh_sweep_level_progress_callback(outer_progress_callback, job, level_index, total_levels, mesh_n, phase_timer, levels)
    if nargin < 7
        levels = [];
    end
    if isempty(outer_progress_callback)
        progress_callback = [];
        return;
    end
    progress_callback = @(payload) relay_phase1_mesh_sweep_level_progress( ...
        outer_progress_callback, payload, job, level_index, total_levels, mesh_n, phase_timer, levels);
end

function relay_phase1_mesh_sweep_level_progress(outer_progress_callback, payload, job, level_index, total_levels, mesh_n, phase_timer, levels)
    if isempty(outer_progress_callback) || ~isstruct(payload)
        return;
    end

    channel = lower(strtrim(pick_text(payload, {'channel'}, 'solver')));
    if any(strcmp(channel, {'report', 'metrics', 'log'}))
        return;
    end
    if ~isempty(channel) && ~any(strcmp(channel, {'solver', 'convergence_progress'}))
        return;
    end

    inner_iteration = pick_numeric(payload, {'iteration', 'step'}, NaN);
    inner_total_iterations = pick_numeric(payload, {'total_iterations', 'total'}, NaN);
    inner_progress_pct = pick_numeric(payload, {'progress_pct'}, NaN);
    if ~isfinite(inner_progress_pct) && isfinite(inner_iteration) && isfinite(inner_total_iterations) && inner_total_iterations > 0
        inner_progress_pct = 100 * min(max(inner_iteration / inner_total_iterations, 0), 1);
    end
    if ~isfinite(inner_progress_pct)
        inner_progress_pct = 0;
    end

    if ~(isfinite(inner_total_iterations) && inner_total_iterations > 0)
        inner_total_iterations = max(round(double(pick_numeric(job.parameters, {'num_steps', 'Nt'}, 1))), 1);
    end
    if ~(isfinite(inner_iteration) && inner_iteration >= 0)
        inner_iteration = round(double(inner_total_iterations * inner_progress_pct / 100));
    end

    inner_total_iterations = max(round(double(inner_total_iterations)), 1);
    inner_iteration = min(max(round(double(inner_iteration)), 0), inner_total_iterations);
    normalized_progress_pct = 100 * ((double(level_index) - 1) + inner_progress_pct / 100) / max(double(total_levels), 1);
    global_iteration = max(0, (double(level_index) - 1) * inner_total_iterations + inner_iteration);
    global_total_iterations = max(1, double(total_levels) * inner_total_iterations);

    translated_payload = struct();
    translated_payload.channel = 'convergence_progress';
    translated_payload.phase = 'mesh_sweep_level_runtime';
    translated_payload.stage_name = 'mesh_sweep';
    translated_payload.method = job.method;
    translated_payload.mode = 'convergence';
    translated_payload.iteration = double(global_iteration);
    translated_payload.total_iterations = double(global_total_iterations);
    translated_payload.mesh_n = double(mesh_n);
    translated_payload.mesh_nx = double(mesh_n);
    translated_payload.mesh_ny = double(mesh_n);
    translated_payload.time = pick_numeric(payload, {'time', 't'}, NaN);
    translated_payload.max_vorticity = pick_numeric(payload, {'max_vorticity', 'max_omega'}, NaN);
    translated_payload.kinetic_energy = pick_numeric(payload, {'kinetic_energy', 'energy_proxy'}, NaN);
    translated_payload.enstrophy = pick_numeric(payload, {'enstrophy', 'enstrophy_proxy'}, NaN);
    translated_payload.elapsed_wall = phase1_mesh_sweep_elapsed_seconds(phase_timer, payload);
    translated_payload.convergence_metric = NaN;
    translated_payload.convergence_residual = NaN;
    translated_payload.convergence_metric_l2 = NaN;
    translated_payload.convergence_residual_l2 = NaN;
    translated_payload.convergence_metric_peak = NaN;
    translated_payload.convergence_residual_peak = NaN;
    translated_payload.xi_l2_pct = NaN;
    translated_payload.xi_peak_pct = NaN;
    translated_payload.qoi_value = NaN;
    translated_payload.dt_used = pick_numeric(payload, {'dt_used', 'dt_step_used', 'dt_step', 'dt'}, NaN);
    translated_payload.time_step_mode = pick_text(payload, {'time_step_mode'}, 'fixed');
    translated_payload.recommended_next_n = NaN;
    if ~isempty(levels) && numel(levels) >= level_index + 1
        translated_payload.recommended_next_n = double(levels(level_index + 1));
    end
    translated_payload.display_iteration = double(level_index);
    translated_payload.display_total_iterations = double(total_levels);
    translated_payload.display_mesh_nx = double(mesh_n);
    translated_payload.display_mesh_ny = double(mesh_n);
    translated_payload.display_progress_pct = double(normalized_progress_pct);
    translated_payload.progress_pct = double(normalized_progress_pct);
    translated_payload.run_id = pick_text(job.run_config, {'study_id', 'run_id'}, '');
    translated_payload.study_id = translated_payload.run_id;

    try
        invoke_runtime_progress_callback(outer_progress_callback, translated_payload);
    catch ME
        warning('Phase1PeriodicComparison:MeshSweepLevelProgressCallbackDisabled', ...
            'Phase 1 mesh sweep level progress callback failed and will be ignored: %s', ME.message);
    end
end

function emit_phase1_mesh_sweep_level_start_progress(progress_callback, job, iteration, total_iterations, mesh_n, elapsed_wall, levels)
    if isempty(progress_callback)
        return;
    end

    completed_iterations = max(double(iteration) - 1, 0);
    payload = struct();
    payload.channel = 'convergence_progress';
    payload.phase = 'mesh_sweep_level_start';
    payload.stage_name = 'mesh_sweep';
    payload.method = job.method;
    payload.mode = 'convergence';
    payload.iteration = completed_iterations;
    payload.total_iterations = double(total_iterations);
    payload.active_level = double(iteration);
    payload.active_level_label = sprintf('L%02d', round(double(iteration)));
    payload.mesh_n = double(mesh_n);
    payload.mesh_nx = double(mesh_n);
    payload.mesh_ny = double(mesh_n);
    payload.qoi_value = NaN;
    payload.convergence_metric = NaN;
    payload.convergence_residual = NaN;
    payload.convergence_metric_l2 = NaN;
    payload.convergence_residual_l2 = NaN;
    payload.convergence_metric_peak = NaN;
    payload.convergence_residual_peak = NaN;
    payload.xi_l2_pct = NaN;
    payload.xi_peak_pct = NaN;
    payload.elapsed_wall = double(elapsed_wall);
    payload.dt_used = pick_numeric(job.parameters, {'dt'}, NaN);
    payload.dt_cfl = NaN;
    payload.dt_adv = NaN;
    payload.dt_diff = NaN;
    payload.cfl_adv = NaN;
    payload.cfl_diff = NaN;
    payload.cfl_observed = NaN;
    payload.cfl_terminal = NaN;
    payload.time_step_mode = 'fixed';
    payload.recommended_next_n = NaN;
    if nargin >= 7 && numel(levels) >= iteration + 1
        payload.recommended_next_n = double(levels(iteration + 1));
    end
    payload.display_iteration = completed_iterations;
    payload.display_total_iterations = double(total_iterations);
    payload.display_mesh_nx = double(mesh_n);
    payload.display_mesh_ny = double(mesh_n);
    payload.display_progress_pct = 100 * (completed_iterations / max(double(total_iterations), 1));
    payload.progress_pct = payload.display_progress_pct;
    payload.run_id = pick_text(job.run_config, {'study_id', 'run_id'}, '');
    payload.study_id = payload.run_id;
    try
        invoke_runtime_progress_callback(progress_callback, payload);
    catch ME
        warning('Phase1PeriodicComparison:MeshSweepLevelStartProgressCallbackDisabled', ...
            'Phase 1 mesh sweep level-start progress callback failed and will be ignored: %s', ME.message);
    end
end

function elapsed_wall = phase1_mesh_sweep_elapsed_seconds(phase_timer, payload)
    elapsed_wall = pick_numeric(payload, {'elapsed_wall', 'wall_time', 'elapsed_time', 'elapsed_seconds', 'wall_time_s'}, NaN);
    if isfinite(elapsed_wall)
        return;
    end
    if ~isempty(phase_timer)
        try
            elapsed_wall = toc(phase_timer);
            return;
        catch
        end
    end
    elapsed_wall = NaN;
end

function record = build_phase1_mesh_sweep_record(method_name, parameters, results, analysis, level_index)
    record = empty_phase1_mesh_record();
    record.method = normalize_method_key(method_name);
    record.method_label = phase1_method_display_label(record.method);
    record.study_stage = 'mesh_sweep';
    record.refinement_axis = 'h';
    record.mesh_level_index = double(level_index);
    record.mesh_level_label = sprintf('L%02d', round(double(level_index)));
    record.dt = pick_numeric(parameters, {'dt'}, NaN);
    record.Nx = pick_numeric(parameters, {'Nx'}, NaN);
    record.Ny = pick_numeric(parameters, {'Ny'}, NaN);
    record.Nz = pick_numeric(parameters, {'Nz'}, NaN);
    [~, ~, dx, dy] = grid_spacing_from_analysis(analysis, size(extract_omega_field(analysis, 'final')));
    record.h = max([dx, dy]);
    if strcmp(record.method, 'spectral')
        record.mode_count = double(numel(extract_omega_field(analysis, 'final')));
    else
        record.mode_count = NaN;
    end
    record.dof = double(record.Nx * record.Ny);
    record.cells = double(record.Nx * record.Ny);
    record.modes = record.mode_count;
    record.runtime_wall_s = pick_numeric(results, {'wall_time', 'total_time'}, NaN);
    record.final_time = pick_numeric(results, {'final_time'}, pick_numeric(parameters, {'Tfinal'}, NaN));
    record.data_path = pick_text(results, {'data_path'}, '');
    record.iterations = max(1, round(pick_numeric(results, {'total_steps'}, pick_numeric(parameters, {'Tfinal'}, 0) / max(pick_numeric(parameters, {'dt'}, eps), eps))));
    record.cfl_observed = observed_cfl(analysis, parameters);
    record.time_step_mode = pick_text(analysis, {'time_step_mode'}, 'fixed');
    record.adaptive_dt_used = logical(pick_value(analysis, 'adaptive_dt_used', false));
    adaptive_meta = pick_struct(analysis, {'adaptive_timestep'}, struct());
    record.dt_initial = pick_numeric(adaptive_meta, {'dt_initial'}, record.dt);
    record.delta = pick_numeric(adaptive_meta, {'delta_terminal'}, record.h);
    record.dt_adv = pick_numeric(adaptive_meta, {'dt_adv_terminal'}, NaN);
    record.dt_diff = pick_numeric(adaptive_meta, {'dt_diff_terminal'}, NaN);
    record.dt_final = pick_numeric(adaptive_meta, {'dt_final_terminal'}, ...
        pick_numeric(adaptive_meta, {'dt_final'}, record.dt));
    record.dt_used = pick_numeric(adaptive_meta, {'dt_step_terminal'}, ...
        pick_numeric(adaptive_meta, {'dt_step'}, record.dt_final));
    record.dt_min = pick_numeric(adaptive_meta, {'dt_min'}, record.dt);
    record.dt_max = pick_numeric(adaptive_meta, {'dt_max'}, record.dt);
    record.cfl_adv = pick_numeric(adaptive_meta, {'cfl_adv_terminal'}, NaN);
    record.cfl_diff = pick_numeric(adaptive_meta, {'cfl_diff_terminal'}, NaN);
    record.cfl = pick_numeric(record, {'cfl_adv'}, record.cfl_observed);
    omega_initial = extract_omega_field(analysis, 'initial');
    omega_final = extract_omega_field(analysis, 'final');
    record.nan_inf_flag = any(~isfinite(omega_final(:)));
    record.stability_flags = struct( ...
        'runtime_error', false, ...
        'nan_inf', logical(record.nan_inf_flag), ...
        'blow_up', false, ...
        'cfl_exceeded', logical(isfinite(record.cfl) && record.cfl > 1.0), ...
        'grid_valid', true);
    record.conservation_drift = conservation_drift(analysis, omega_initial, omega_final);
    record.circulation_drift = record.conservation_drift;
    record.kinetic_energy_drift = history_drift(analysis, 'kinetic_energy');
    record.enstrophy_drift = history_drift(analysis, 'enstrophy');
    record.initial_energy = first_finite_local(pick_value(analysis, 'kinetic_energy', []));
    record.final_energy = pick_numeric(results, {'final_energy'}, ...
        last_finite_local(pick_value(analysis, 'kinetic_energy', [])));
    record.initial_enstrophy = first_finite_local(pick_value(analysis, 'enstrophy', []));
    record.final_enstrophy = pick_numeric(results, {'final_enstrophy'}, ...
        last_finite_local(pick_value(analysis, 'enstrophy', [])));
    record.initial_circulation = first_finite_local(pick_value(analysis, 'circulation', []));
    record.final_circulation = last_finite_local(pick_value(analysis, 'circulation', []));
    record.aliasing_indicator = NaN;
    record.smoothness_indicator = NaN;
    record.reference_strategy = 'successive_level_remap';
    record.error_L1 = NaN;
    record.error_L2 = NaN;
    record.error_Linf = NaN;
    record.omega_diff_l2 = NaN;
    record.omega_curr_l2 = NaN;
    record.xi_fraction = NaN;
    record.relative_change_fraction = NaN;
    record.peak_vorticity_prev = NaN;
    record.peak_vorticity_curr = NaN;
    record.max_vorticity_rel_error_fraction = NaN;
    record.max_vorticity_rel_error_pct = NaN;
    record.xi = NaN;
    record.xi_tol = NaN;
    [record.nan_inf_flag, nonfinite_detail] = phase1_analysis_has_nonfinite_output(analysis);
    record.stability_flags.nan_inf = logical(record.nan_inf_flag);
    record.level_stable = ~record.nan_inf_flag;
    record.eligible_for_selection = record.level_stable;
    if record.level_stable
        record.level_status = 'stable';
        record.failure_reason = '';
    else
        record.level_status = 'unstable';
        record.failure_reason = char(string(nonfinite_detail));
    end
    record.convergence_verdict = 'baseline';
    record.stop_reason = 'baseline_unpaired';
end

function record = build_phase1_failed_mesh_sweep_record(method_name, parameters, level_index, ME)
    record = empty_phase1_mesh_record();
    record.method = normalize_method_key(method_name);
    record.method_label = phase1_method_display_label(record.method);
    record.study_stage = 'mesh_sweep';
    record.refinement_axis = 'h';
    record.mesh_level_index = double(level_index);
    record.mesh_level_label = sprintf('L%02d', round(double(level_index)));
    record.dt = pick_numeric(parameters, {'dt'}, NaN);
    record.Nx = pick_numeric(parameters, {'Nx'}, NaN);
    record.Ny = pick_numeric(parameters, {'Ny'}, NaN);
    record.Nz = pick_numeric(parameters, {'Nz'}, NaN);
    record.dof = double(record.Nx * record.Ny);
    record.cells = double(record.Nx * record.Ny);
    record.iterations = 0;
    record.level_status = 'failed';
    record.failure_identifier = char(string(ME.identifier));
    record.failure_reason = char(string(ME.message));
    record.stability_flags = struct( ...
        'runtime_error', true, ...
        'nan_inf', false, ...
        'blow_up', false, ...
        'cfl_exceeded', false, ...
        'grid_valid', isfinite(record.Nx) && isfinite(record.Ny));
    record.level_stable = false;
    record.eligible_for_selection = false;
    record.nan_inf_flag = false;
    record.convergence_verdict = 'unstable';
    record.stop_reason = 'runtime_error';
end

function [has_nonfinite, detail] = phase1_analysis_has_nonfinite_output(analysis)
    has_nonfinite = false;
    detail = '';
    if ~isstruct(analysis)
        has_nonfinite = true;
        detail = 'missing analysis payload';
        return;
    end
    if isfield(analysis, 'instability_detected') && logical(analysis.instability_detected)
        has_nonfinite = true;
        detail = pick_text(analysis, {'instability_reason'}, 'runtime instability detected');
        return;
    end
    if isfield(analysis, 'instability') && isstruct(analysis.instability) && ...
            logical(pick_value(analysis.instability, 'detected', false))
        has_nonfinite = true;
        detail = pick_text(analysis.instability, {'reason'}, 'runtime instability detected');
        return;
    end
    fields_to_check = {'omega_snaps', 'omega', 'psi', 'u_snaps', 'v_snaps', ...
        'kinetic_energy', 'enstrophy', 'circulation', 'time_vec', ...
        'snapshot_times', 'snapshot_times_requested'};
    for i = 1:numel(fields_to_check)
        field_name = fields_to_check{i};
        if ~isfield(analysis, field_name) || ~isnumeric(analysis.(field_name)) || isempty(analysis.(field_name))
            continue;
        end
        values = double(analysis.(field_name));
        if any(~isfinite(values(:)))
            has_nonfinite = true;
            detail = sprintf('non-finite values in analysis.%s', field_name);
            return;
        end
    end
end

function comparison = compute_phase1_successive_relative_change(prev_analysis, curr_analysis)
    comparison = struct( ...
        'omega_diff_l2', NaN, ...
        'omega_curr_l2', NaN, ...
        'xi_fraction', NaN, ...
        'xi_percent', NaN, ...
        'peak_vorticity_prev', NaN, ...
        'peak_vorticity_curr', NaN, ...
        'max_vorticity_rel_error_fraction', NaN, ...
        'max_vorticity_rel_error_pct', NaN);
    if ~isstruct(prev_analysis) || ~isstruct(curr_analysis)
        return;
    end
    prev_final = extract_omega_field(prev_analysis, 'final');
    curr_final = extract_omega_field(curr_analysis, 'final');
    [X_prev, Y_prev] = analysis_grid(prev_analysis, size(prev_final));
    [X_curr, Y_curr] = analysis_grid(curr_analysis, size(curr_final));
    prev_on_curr = interp2(X_prev, Y_prev, double(prev_final), X_curr, Y_curr, 'linear', NaN);
    if any(~isfinite(prev_on_curr(:)))
        prev_on_curr = interp2(X_prev, Y_prev, double(prev_final), X_curr, Y_curr, 'nearest', 0);
    end
    delta = double(curr_final) - double(prev_on_curr);
    omega_diff_l2 = norm(delta(:), 2);
    omega_curr_l2 = norm(double(curr_final(:)), 2);
    xi_fraction = omega_diff_l2 / max(omega_curr_l2, 1.0e-12);
    peak_prev = max(abs(prev_on_curr(:)));
    peak_curr = max(abs(curr_final(:)));
    peak_rel_fraction = abs(peak_curr - peak_prev) / max(peak_curr, eps);
    comparison = struct( ...
        'omega_diff_l2', double(omega_diff_l2), ...
        'omega_curr_l2', double(omega_curr_l2), ...
        'xi_fraction', double(xi_fraction), ...
        'xi_percent', double(100 * xi_fraction), ...
        'peak_vorticity_prev', double(peak_prev), ...
        'peak_vorticity_curr', double(peak_curr), ...
        'max_vorticity_rel_error_fraction', double(peak_rel_fraction), ...
        'max_vorticity_rel_error_pct', double(100 * peak_rel_fraction));
end

function observed_rate = compute_phase1_mesh_observed_rate(prev_record, curr_record)
    observed_rate = NaN;
    prev_err = pick_numeric(prev_record, {'xi', 'relative_change'}, NaN);
    curr_err = pick_numeric(curr_record, {'xi', 'relative_change'}, NaN);
    prev_h = pick_numeric(prev_record, {'h'}, NaN);
    curr_h = pick_numeric(curr_record, {'h'}, NaN);
    if ~(isfinite(prev_err) && isfinite(curr_err) && prev_err > 0 && curr_err > 0)
        return;
    end
    if ~(isfinite(prev_h) && isfinite(curr_h) && prev_h > 0 && curr_h > 0 && prev_h ~= curr_h)
        return;
    end
    observed_rate = log(prev_err / curr_err) / log(prev_h / curr_h);
end

function emit_phase1_mesh_sweep_progress(progress_callback, job, iteration, total_iterations, mesh_n, record, elapsed_wall, levels)
    if isempty(progress_callback)
        return;
    end
    conv_residual = pick_numeric(record, {'xi', 'relative_change'}, NaN);
    peak_residual = pick_numeric(record, {'max_vorticity_rel_error_pct'}, NaN);
    payload = struct();
    payload.channel = 'convergence_progress';
    payload.phase = 'mesh_sweep';
    payload.stage_name = 'mesh_sweep';
    payload.method = job.method;
    payload.mode = 'convergence';
    payload.iteration = double(iteration);
    payload.total_iterations = double(total_iterations);
    payload.mesh_n = double(mesh_n);
    payload.mesh_nx = double(mesh_n);
    payload.mesh_ny = double(mesh_n);
    payload.qoi_value = double(conv_residual);
    payload.convergence_metric = double(conv_residual);
    payload.convergence_residual = double(conv_residual);
    payload.convergence_metric_l2 = double(conv_residual);
    payload.convergence_residual_l2 = double(conv_residual);
    payload.convergence_metric_peak = double(peak_residual);
    payload.convergence_residual_peak = double(peak_residual);
    payload.xi_l2_pct = double(conv_residual);
    payload.xi_peak_pct = double(peak_residual);
    payload.elapsed_wall = double(elapsed_wall);
    payload.delta = pick_numeric(record, {'delta', 'h'}, NaN);
    payload.cfl_adv = pick_numeric(record, {'cfl_adv'}, NaN);
    payload.cfl_diff = pick_numeric(record, {'cfl_diff'}, NaN);
    payload.dt_used = pick_numeric(record, {'dt_used', 'dt_final', 'dt'}, NaN);
    payload.dt_cfl = pick_numeric(record, {'dt_final', 'dt'}, NaN);
    payload.dt_adv = pick_numeric(record, {'dt_adv'}, NaN);
    payload.dt_diff = pick_numeric(record, {'dt_diff'}, NaN);
    payload.cfl_observed = pick_numeric(record, {'cfl_observed'}, NaN);
    payload.cfl_terminal = pick_numeric(record, {'cfl', 'cfl_observed'}, NaN);
    payload.time_step_mode = pick_text(record, {'time_step_mode'}, 'fixed');
    payload.recommended_next_n = NaN;
    if nargin >= 8 && numel(levels) >= iteration + 1
        payload.recommended_next_n = double(levels(iteration + 1));
    end
    payload.display_iteration = double(iteration);
    payload.display_total_iterations = double(total_iterations);
    payload.display_mesh_nx = double(mesh_n);
    payload.display_mesh_ny = double(mesh_n);
    payload.display_progress_pct = 100 * min(max(iteration / max(total_iterations, 1), 0), 1);
    payload.run_id = pick_text(job.run_config, {'study_id', 'run_id'}, '');
    payload.study_id = payload.run_id;
    try
        invoke_runtime_progress_callback(progress_callback, payload);
    catch ME
        warning('Phase1PeriodicComparison:MeshSweepProgressCallbackDisabled', ...
            'Phase 1 mesh sweep progress callback failed and will be ignored: %s', ME.message);
    end
end

function [run_records, stage_summary, summary_payload] = finalize_phase1_mesh_sweep_records(run_records, levels, phase_cfg)
    tolerance = pick_numeric(phase_cfg, {'convergence_tolerance'}, NaN);
    finite_l2 = reshape([run_records.xi], 1, []);
    finite_peak = reshape([run_records.max_vorticity_rel_error_pct], 1, []);
    stable_flags = reshape(arrayfun(@(record) logical(pick_value(record, 'level_stable', false)), run_records), 1, []);
    unstable_indices = find(~stable_flags);
    unstable_levels_present = ~isempty(unstable_indices);
    comparable_flags = stable_flags & isfinite(finite_l2) & isfinite(finite_peak);
    comparable_indices = find(comparable_flags);

    if isempty(comparable_indices)
        if any(stable_flags)
            overall_verdict = 'plateaued';
            stop_reason = 'insufficient_stable_comparisons';
            selected_index = find(stable_flags, 1, 'last');
            selection_reason = 'finest_stable_fallback';
        else
            overall_verdict = 'unstable';
            stop_reason = 'no_usable_stable_mesh_path';
            selected_index = numel(run_records);
            selection_reason = 'unstable_last_level';
        end
    else
        joint_tolerance_flags = comparable_flags & (finite_l2 < tolerance) & (finite_peak < tolerance);
        first_converged_idx = find(joint_tolerance_flags, 1, 'first');
        if ~isempty(first_converged_idx)
            overall_verdict = 'converged';
            stop_reason = 'joint_vorticity_threshold_met';
            selected_index = first_converged_idx;
            selection_reason = 'first_converged';
        else
            overall_verdict = 'plateaued';
            stop_reason = 'joint_vorticity_tolerance_not_met';
            selected_index = comparable_indices(end);
            selection_reason = 'finest_stable_fallback';
        end
    end

    for i = 1:numel(run_records)
        run_records(i).xi_l2_tol_met = isfinite(run_records(i).xi) && isfinite(tolerance) && ...
            run_records(i).xi < tolerance;
        run_records(i).xi_peak_tol_met = isfinite(run_records(i).max_vorticity_rel_error_pct) && ...
            isfinite(tolerance) && run_records(i).max_vorticity_rel_error_pct < tolerance;
        run_records(i).joint_tolerance_met = run_records(i).xi_l2_tol_met && run_records(i).xi_peak_tol_met;
        run_records(i).selected_level = (i == selected_index);
        run_records(i).fallback_selected = (i == selected_index) && ~strcmp(selection_reason, 'first_converged');
        run_records(i).eligible_for_selection = stable_flags(i) && isfinite(run_records(i).Nx) && isfinite(run_records(i).Ny);
        if ~stable_flags(i)
            run_records(i).convergence_verdict = 'unstable';
            if logical(pick_value(run_records(i).stability_flags, 'runtime_error', false))
                run_records(i).stop_reason = 'runtime_error';
            else
                run_records(i).stop_reason = 'nonfinite_output';
            end
            continue;
        end
        if isnan(run_records(i).comparison_source_index) || ~isfinite(run_records(i).xi)
            run_records(i).convergence_verdict = 'baseline';
            run_records(i).stop_reason = 'baseline_unpaired';
            continue;
        end
        if strcmp(selection_reason, 'first_converged') && run_records(i).joint_tolerance_met
            run_records(i).convergence_verdict = 'converged';
            run_records(i).stop_reason = 'joint_vorticity_threshold_met';
        elseif i == selected_index
            run_records(i).convergence_verdict = overall_verdict;
            run_records(i).stop_reason = stop_reason;
        else
            run_records(i).convergence_verdict = 'refining';
            run_records(i).stop_reason = 'joint_vorticity_above_tolerance';
        end
    end
    run_records(selected_index).selection_reason = selection_reason;

    stage_summary = empty_phase1_stage_summary();
    stage_summary.stage_name = 'mesh_sweep';
    stage_summary.refinement_axis = 'h';
    stage_summary.reference_strategy = 'successive_level_remap';
    stage_summary.reference_warning = '';
    stage_summary.verdict = overall_verdict;
    stage_summary.stop_reason = stop_reason;
    stage_summary.expected_order = NaN;
    stage_summary.observed_order_last = last_finite_local([run_records(stable_flags).observed_rate]);
    stage_summary.monotone_error_reduction = local_phase1_monotone_ok(finite_l2(comparable_flags));
    stage_summary.plateau_detected = local_phase1_plateau_detected(finite_l2(comparable_flags));
    stage_summary.final_error_L2 = NaN;
    stage_summary.final_relative_change = last_finite_local(finite_l2(comparable_flags));
    stage_summary.final_xi = last_finite_local(finite_l2(comparable_flags));
    stage_summary.final_peak_error = last_finite_local(finite_peak(comparable_flags));
    stage_summary.runtime_wall_s = nansum([run_records.runtime_wall_s]);
    stage_summary.warning_messages = {};
    stage_summary.stable = any(stable_flags);
    stage_summary.unstable_levels_present = unstable_levels_present;
    stage_summary.unstable_level_indices = double(unstable_indices(:)).';
    stage_summary.unstable_level_labels = {run_records(unstable_indices).mesh_level_label};

    summary_payload = struct( ...
        'overall_verdict', overall_verdict, ...
        'primary_refinement_axis', 'h', ...
        'stop_reason', stop_reason, ...
        'selected_mesh_index', double(selected_index), ...
        'selected_mesh_level', double(levels(selected_index)), ...
        'mesh_ladder_mode', char(string(pick_text(phase_cfg, {'mesh_ladder_mode'}, 'bounded'))), ...
        'mesh_ladder', double(levels(:)).', ...
        'selection_reason', selection_reason, ...
        'level_count', double(numel(levels)), ...
        'xi_values', finite_l2, ...
        'xi_peak_values', finite_peak, ...
        'xi_tol', double(tolerance), ...
        'convergence_achieved', strcmpi(selection_reason, 'first_converged'), ...
        'fallback_used', ~strcmpi(selection_reason, 'first_converged'), ...
        'unstable_levels_present', unstable_levels_present, ...
        'unstable_level_indices', double(unstable_indices(:)).', ...
        'unstable_level_labels', {run_records(unstable_indices).mesh_level_label});
end

function results = build_phase1_mesh_sweep_results(job, phase_cfg, paths, run_records, stage_summary, summary_payload, mesh_level_animation, wall_time)
    level_labels = arrayfun(@(idx) sprintf('L%d', idx), 1:numel(run_records), 'UniformOutput', false);
    summary_payload_scalar = summary_payload;
    if isstruct(summary_payload_scalar)
        if isempty(summary_payload_scalar)
            summary_payload_scalar = struct();
        elseif ~isscalar(summary_payload_scalar)
            summary_payload_scalar = summary_payload_scalar(1);
        end
    end
    results = struct();
    results.study_id = pick_text(job.run_config, {'study_id', 'run_id'}, '');
    results.method = char(string(job.method));
    results.method_type = normalize_method_key(job.method);
    results.phase_local_mesh_sweep = true;
    results.summary = summary_payload_scalar;
    results.stage_summaries = stage_summary;
    results.run_records = run_records;
    results.tolerance = pick_numeric(phase_cfg, {'convergence_tolerance'}, NaN);
    results.xi_tol = pick_numeric(phase_cfg, {'convergence_tolerance'}, NaN);
    results.refinement_axis = 'h';
    results.convergence_order = pick_numeric(stage_summary, {'observed_order_last'}, NaN);
    results.convergence_variable = 'xi';
    results.level_labels = level_labels;
    results.Nx_values = [run_records.Nx];
    results.Ny_values = [run_records.Ny];
    results.wall_times = [run_records.runtime_wall_s];
    results.QoI_values = [run_records.xi];
    results.xi_values = [run_records.xi];
    results.xi_peak_values = [run_records.max_vorticity_rel_error_pct];
    results.xi_fraction_values = [run_records.xi_fraction];
    results.relative_change_values = [run_records.relative_change];
    results.relative_change_fraction_values = [run_records.relative_change_fraction];
    results.omega_diff_l2_values = [run_records.omega_diff_l2];
    results.omega_curr_l2_values = [run_records.omega_curr_l2];
    results.mesh_sizes = [run_records.Nx];
    mesh_ladder_value = [run_records.Nx];
    if isstruct(summary_payload_scalar) && isfield(summary_payload_scalar, 'mesh_ladder')
        candidate_mesh_ladder = summary_payload_scalar.mesh_ladder;
        keep_candidate = true;
        try
            keep_candidate = numel(candidate_mesh_ladder) > 0;
        catch
            keep_candidate = true;
        end
        if keep_candidate
            mesh_ladder_value = candidate_mesh_ladder;
        end
    end
    results.mesh_ladder = double(reshape(mesh_ladder_value, 1, []));
    results.mesh_ladder_mode = char(string(pick_text(summary_payload_scalar, {'mesh_ladder_mode'}, pick_text(phase_cfg, {'mesh_ladder_mode'}, 'bounded'))));
    results.h_values = [run_records.h];
    results.delta_values = [run_records.delta];
    results.dt_adv_values = [run_records.dt_adv];
    results.dt_diff_values = [run_records.dt_diff];
    results.dt_final_values = [run_records.dt_final];
    results.dt_used_values = [run_records.dt_used];
    results.cfl_adv_values = [run_records.cfl_adv];
    results.cfl_diff_values = [run_records.cfl_diff];
    results.peak_vorticity_prev_values = [run_records.peak_vorticity_prev];
    results.peak_vorticity_curr_values = [run_records.peak_vorticity_curr];
    results.max_vorticity_rel_error_fraction_values = [run_records.max_vorticity_rel_error_fraction];
    results.max_vorticity_rel_error_pct_values = [run_records.max_vorticity_rel_error_pct];
    results.mesh_level_summary_paths = {run_records.mesh_level_summary_path};
    results.selected_level_flags = [run_records.selected_level];
    results.fallback_selected_flags = [run_records.fallback_selected];
    results.joint_tolerance_flags = [run_records.joint_tolerance_met];
    results.mode_count_values = [run_records.mode_count];
    results.verdict = char(string(pick_text(summary_payload_scalar, {'overall_verdict'}, '')));
    results.adaptive_timestep = pick_struct(phase_cfg, {'adaptive_timestep'}, struct());
    results.adaptive_timestep.enabled = false;
    results.adaptive_timestep.time_step_mode = 'fixed';
    results.mesh_level_animation = mesh_level_animation;
    results.total_time = double(wall_time);
    results.wall_time = double(wall_time);
    results.data_path = fullfile(paths.data, 'convergence_results.mat');
end

function write_phase1_artifact_manifest(paths, results_for_save)
    matlab_data_root = pick_text(paths, {'matlab_data_root', 'data'}, '');
    if isempty(matlab_data_root)
        return;
    end
    manifest_path = fullfile(matlab_data_root, 'artifact_manifest.json');
    payload = struct( ...
        'artifact_layout_version', char(string(pick_text(paths, {'artifact_layout_version'}, 'compact_v3'))), ...
        'phase_root', char(string(pick_text(paths, {'base'}, ''))), ...
        'run_settings_path', char(string(pick_text(paths, {'run_settings_path'}, ''))), ...
        'matlab_data_root', char(string(matlab_data_root)), ...
        'metrics_root', char(string(pick_text(paths, {'metrics_root', 'reports'}, ''))), ...
        'visuals_root', char(string(pick_text(paths, {'visuals_root', 'figures_root'}, ''))), ...
        'workflow_kind', 'phase1_periodic_comparison', ...
        'phase_id', char(string(pick_text(results_for_save, {'phase_id'}, ''))), ...
        'result_layout_kind', 'phase1_workflow', ...
        'publication_status', 'pending_ui_publication');
    write_json(manifest_path, payload);
end

function write_phase1_preflight_manifest(paths, phase_id, jobs, phase_cfg, phase_parameters, Run_Config)
    if nargin < 3 || isempty(jobs)
        return;
    end
    queued_outputs = repmat(empty_output(), 1, numel(jobs));
    queue_snapshot = build_queue_status_snapshot(jobs, queued_outputs, 0, 'queued');
    payload = struct( ...
        'artifact_layout_version', char(string(pick_text(paths, {'artifact_layout_version'}, 'compact_v3'))), ...
        'workflow_kind', 'phase1_periodic_comparison', ...
        'result_layout_kind', 'phase1_workflow', ...
        'phase_id', char(string(phase_id)), ...
        'phase_root', char(string(pick_text(paths, {'base'}, ''))), ...
        'queue', queue_snapshot, ...
        'phase_cfg', filter_graphics_objects(phase_cfg), ...
        'parent_parameters', filter_graphics_objects(phase_parameters), ...
        'parent_run_config', filter_graphics_objects(Run_Config), ...
        'status', 'preflight_complete');
    safe_save_mat(fullfile(paths.data, 'phase1_workflow_manifest.mat'), struct('workflow_manifest', payload));
    if json_saving_enabled(phase_cfg, phase_parameters)
        write_json(fullfile(paths.data, 'phase1_workflow_manifest.json'), payload);
        write_phase1_artifact_manifest(paths, struct('phase_id', phase_id));
    end
end

function emit_phase_runtime_log(progress_callback, message, log_type)
    if nargin < 3 || isempty(log_type)
        log_type = 'info';
    end
    if isempty(progress_callback) || ~isa(progress_callback, 'function_handle')
        return;
    end
    payload = struct( ...
        'channel', 'log', ...
        'log_message', char(string(message)), ...
        'log_type', char(string(log_type)));
    try
        invoke_runtime_progress_callback(progress_callback, payload);
    catch
    end
end

function emit_phase_completion_report_payload(progress_callback, results_for_save, paths, run_config, parameters, phase_label, workflow_kind, result_layout_kind)
    if isempty(progress_callback) || ~isa(progress_callback, 'function_handle') || ~isstruct(results_for_save)
        return;
    end
    if nargin < 4 || ~isstruct(run_config)
        run_config = struct();
    end
    if nargin < 5 || ~isstruct(parameters)
        parameters = struct();
    end
    if nargin < 6 || strlength(string(phase_label)) == 0
        phase_label = 'Workflow';
    end
    if nargin < 7 || strlength(string(workflow_kind)) == 0
        workflow_kind = pick_text(results_for_save, {'workflow_kind'}, '');
    end
    if nargin < 8 || strlength(string(result_layout_kind)) == 0
        result_layout_kind = pick_text(results_for_save, {'result_layout_kind'}, '');
    end

    published_run_config = filter_graphics_objects(run_config);
    published_run_config.workflow_kind = char(string(workflow_kind));
    published_run_config.result_layout_kind = char(string(result_layout_kind));
    published_run_config.phase_label = char(string(phase_label));
    published_run_config.launch_origin = pick_text(published_run_config, {'launch_origin'}, 'phase_button');

    phase_id = pick_text(results_for_save, {'phase_id', 'run_id'}, pick_text(published_run_config, {'phase_id', 'run_id'}, ''));
    if ~isempty(phase_id)
        published_run_config.phase_id = phase_id;
    end

    if exist('emit_completion_report_payload', 'file') == 2
        emit_completion_report_payload(progress_callback, results_for_save, paths, published_run_config, parameters, struct( ...
            'phase_label', phase_label, ...
            'workflow_kind', workflow_kind, ...
            'result_layout_kind', result_layout_kind, ...
            'result_publication_mode', 'manual', ...
            'completion_results_already_persisted', true));
        return;
    end

    summary = struct( ...
        'mode', pick_text(published_run_config, {'mode'}, 'Evolution'), ...
        'run_config', published_run_config, ...
        'parameters', filter_graphics_objects(parameters), ...
        'results', filter_graphics_objects(results_for_save), ...
        'paths', filter_graphics_objects(paths), ...
        'wall_time', double(pick_numeric(results_for_save, {'wall_time'}, NaN)), ...
        'completion_results_already_persisted', true, ...
        'workflow_kind', char(string(workflow_kind)), ...
        'result_layout_kind', char(string(result_layout_kind)), ...
        'phase_label', char(string(phase_label)), ...
        'solver_complete', true, ...
        'minimal_results_persisted', true, ...
        'results_published', false, ...
        'exports_complete', false);

    payload = struct( ...
        'channel', 'report', ...
        'phase', 'completion', ...
        'progress_pct', 100, ...
        'summary', summary, ...
        'results', summary.results, ...
        'paths', summary.paths, ...
        'run_config', published_run_config, ...
        'parameters', summary.parameters, ...
        'result_publication_mode', 'manual', ...
        'completion_results_already_persisted', true, ...
        'workflow_kind', char(string(workflow_kind)), ...
        'result_layout_kind', char(string(result_layout_kind)), ...
        'phase_label', char(string(phase_label)), ...
        'phase_id', phase_id, ...
        'run_id', pick_text(results_for_save, {'run_id'}, phase_id), ...
        'wall_time', summary.wall_time, ...
        'solver_complete', true, ...
        'minimal_results_persisted', true, ...
        'results_published', false, ...
        'exports_complete', false);
    try
        invoke_runtime_progress_callback(progress_callback, payload);
    catch
    end
end

function tf = phase1_defer_heavy_exports_requested(settings)
    tf = false;
    if nargin < 1 || ~isstruct(settings)
        return;
    end
    if exist('defer_heavy_result_artifacts_requested', 'file') ~= 2
        return;
    end
    tf = logical(defer_heavy_result_artifacts_requested(settings));
end

function emit_artifact_struct_logs(progress_callback, label_prefix, payload)
    if isempty(progress_callback) || ~isa(progress_callback, 'function_handle')
        return;
    end
    if isstruct(payload)
        if numel(payload) > 1
            for i = 1:numel(payload)
                emit_artifact_struct_logs(progress_callback, sprintf('%s %d', char(string(label_prefix)), i), payload(i));
            end
            return;
        end
        fields = fieldnames(payload);
        for i = 1:numel(fields)
            field_name = fields{i};
            next_label = sprintf('%s %s', char(string(label_prefix)), field_name);
            emit_artifact_struct_logs(progress_callback, next_label, payload.(field_name));
        end
        return;
    end
    if iscell(payload)
        for i = 1:numel(payload)
            emit_artifact_struct_logs(progress_callback, sprintf('%s %d', char(string(label_prefix)), i), payload{i});
        end
        return;
    end
    if ~(ischar(payload) || isstring(payload))
        return;
    end
    path_text = char(string(payload));
    if isempty(path_text)
        return;
    end
    if exist(path_text, 'file') ~= 2 && exist(path_text, 'dir') ~= 7
        return;
    end
    emit_phase_runtime_log(progress_callback, sprintf('Saved %s: %s', char(string(label_prefix)), path_text), 'info');
end

function payload = build_phase1_mesh_level_animation_payload(job, level_outputs, levels)
    payload = struct();
    if nargin < 2 || ~isstruct(level_outputs) || isempty(level_outputs)
        return;
    end
    ref_analysis = level_outputs(end).analysis;
    if ~isstruct(ref_analysis) || isempty(fieldnames(ref_analysis))
        return;
    end
    ref_initial = extract_omega_field(ref_analysis, 'initial');
    [X_ref, Y_ref] = analysis_grid(ref_analysis, size(ref_initial));
    x_ref = X_ref(1, :);
    y_ref = Y_ref(:, 1);
    n_levels = numel(level_outputs);
    omega_cube = zeros(size(ref_initial, 1), size(ref_initial, 2), n_levels);
    mesh_sizes = NaN(1, n_levels);
    mesh_labels = cell(1, n_levels);
    for i = 1:n_levels
        if ~isstruct(level_outputs(i).analysis) || isempty(fieldnames(level_outputs(i).analysis))
            payload = struct();
            return;
        end
        omega_initial = extract_omega_field(level_outputs(i).analysis, 'initial');
        [X_src, Y_src] = analysis_grid(level_outputs(i).analysis, size(omega_initial));
        omega_cube(:, :, i) = interp2(X_src, Y_src, double(omega_initial), X_ref, Y_ref, 'linear', 0);
        mesh_n = pick_numeric(level_outputs(i).record, {'Nx', 'Ny'}, NaN);
        if ~isfinite(mesh_n) && nargin >= 3 && numel(levels) >= i
            mesh_n = double(levels(i));
        end
        mesh_sizes(i) = mesh_n;
        mesh_labels{i} = sprintf('Mesh %dx%d', round(mesh_n), round(mesh_n));
    end
    payload = struct( ...
        'omega_initial_cube', omega_cube, ...
        'x', x_ref, ...
        'y', y_ref, ...
        'mesh_levels', double(levels(:)).', ...
        'mesh_sizes', double(mesh_sizes(:)).', ...
        'mesh_labels', {mesh_labels}, ...
        'method', normalize_method_key(job.method), ...
        'stage_label', sprintf('%s convergence mesh levels', char(string(job.method))), ...
        'run_id', pick_text(job.run_config, {'study_id', 'run_id'}, ''), ...
        'phase_id', pick_text(job.run_config, {'phase_id'}, ''));
end

function write_phase1_mesh_sweep_report(results, paths)
    report_path = fullfile(paths.reports, 'convergence_report.md');
    ensure_parent_directory(report_path);
    fid = fopen(report_path, 'w');
    if fid < 0
        error('Phase1PeriodicComparison:MeshSweepReportWriteFailed', ...
            'Could not write Phase 1 mesh sweep report: %s', report_path);
    end
    cleaner = onCleanup(@() fclose(fid));
    fprintf(fid, '# Phase 1 Mesh Sweep Report\n\n');
    fprintf(fid, '- Method: `%s`\n', char(string(pick_text(results, {'method'}, ''))));
    fprintf(fid, '- Overall verdict: `%s`\n', char(string(pick_text(results.summary, {'overall_verdict'}, ''))));
    fprintf(fid, '- Selected mesh index: `%d`\n', round(double(pick_numeric(results.summary, {'selected_mesh_index'}, NaN))));
    fprintf(fid, '- Selected mesh level: `%d`\n', round(double(pick_numeric(results.summary, {'selected_mesh_level'}, NaN))));
    fprintf(fid, '- Selection reason: `%s`\n', char(string(pick_text(results.summary, {'selection_reason'}, ''))));
    fprintf(fid, '- Mesh ladder mode: `%s`\n', phase1_mesh_ladder_mode_text(pick_text(results, {'mesh_ladder_mode'}, 'bounded')));
    fprintf(fid, '- Mesh ladder: `%s`\n', phase1_mesh_ladder_text(pick_value(results, 'mesh_ladder', [])));
    fprintf(fid, '- xi tolerance: `%s`\n', phase1_percent_text(pick_numeric(results, {'xi_tol', 'tolerance'}, NaN)));
    fprintf(fid, '- Adaptive timestep: `%s`\n', logical_text_local(logical(pick_value(results.adaptive_timestep, 'enabled', false))));
    fprintf(fid, '- C_adv: `%.6g`\n', double(pick_numeric(results.adaptive_timestep, {'C_adv'}, NaN)));
    fprintf(fid, '- C_diff: `%.6g`\n', double(pick_numeric(results.adaptive_timestep, {'C_diff'}, NaN)));
    fprintf(fid, '- Convergence achieved: `%s`\n', logical_text_local(logical(pick_value(results.summary, 'convergence_achieved', false))));
    fprintf(fid, '- Fallback used: `%s`\n\n', logical_text_local(logical(pick_value(results.summary, 'fallback_used', false))));
    fprintf(fid, '| Level | Method | Nx | Ny | delta | dt_adv | dt_diff | dt_final | xi_L2 %% | xi_peak %% | selected | selection reason | wall s |\n');
    fprintf(fid, '| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | ---: |\n');
    for i = 1:numel(results.run_records)
        record = results.run_records(i);
        fprintf(fid, '| %s | %s | %d | %d | %s | %s | %s | %s | %s | %s | %s | %s | %.3f |\n', ...
            pick_text(record, {'mesh_level_label'}, sprintf('L%02d', i)), ...
            pick_text(record, {'method_label'}, upper(char(string(pick_text(record, {'method'}, ''))))), ...
            round(double(record.Nx)), round(double(record.Ny)), ...
            phase1_numeric_text(pick_numeric(record, {'delta', 'h'}, NaN)), ...
            phase1_numeric_text(pick_numeric(record, {'dt_adv'}, NaN)), ...
            phase1_numeric_text(pick_numeric(record, {'dt_diff'}, NaN)), ...
            phase1_numeric_text(pick_numeric(record, {'dt_final', 'dt'}, NaN)), ...
            phase1_percent_text(pick_numeric(record, {'xi', 'relative_change'}, NaN)), ...
            phase1_percent_text(pick_numeric(record, {'max_vorticity_rel_error_pct'}, NaN)), ...
            logical_text_local(logical(pick_value(record, 'selected_level', false))), ...
            pick_text(record, {'selection_reason', 'convergence_verdict'}, '--'), ...
            double(record.runtime_wall_s));
    end
    clear cleaner
end

function record = empty_phase1_mesh_record()
    record = struct( ...
        'method', '', ...
        'method_label', '', ...
        'study_stage', '', ...
        'refinement_axis', '', ...
        'mesh_level_index', NaN, ...
        'mesh_level_label', '', ...
        'h', NaN, ...
        'delta', NaN, ...
        'dt', NaN, ...
        'dt_initial', NaN, ...
        'dt_adv', NaN, ...
        'dt_diff', NaN, ...
        'dt_final', NaN, ...
        'dt_used', NaN, ...
        'dt_min', NaN, ...
        'dt_max', NaN, ...
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
        'xi_fraction', NaN, ...
        'relative_change_fraction', NaN, ...
        'omega_diff_l2', NaN, ...
        'omega_curr_l2', NaN, ...
        'peak_vorticity_prev', NaN, ...
        'peak_vorticity_curr', NaN, ...
        'max_vorticity_rel_error_fraction', NaN, ...
        'max_vorticity_rel_error_pct', NaN, ...
        'xi_l2_tol_met', false, ...
        'xi_peak_tol_met', false, ...
        'joint_tolerance_met', false, ...
        'observed_rate', NaN, ...
        'runtime_wall_s', NaN, ...
        'runtime_cpu_s', NaN, ...
        'memory_peak_mb', NaN, ...
        'iterations', NaN, ...
        'final_time', NaN, ...
        'cfl', NaN, ...
        'cfl_adv', NaN, ...
        'cfl_diff', NaN, ...
        'cfl_observed', NaN, ...
        'data_path', '', ...
        'mesh_level_summary_path', '', ...
        'time_step_mode', 'fixed', ...
        'adaptive_dt_used', false, ...
        'nan_inf_flag', false, ...
        'stability_flags', struct(), ...
        'level_status', 'pending', ...
        'failure_reason', '', ...
        'failure_identifier', '', ...
        'level_stable', false, ...
        'eligible_for_selection', false, ...
        'comparison_source_index', NaN, ...
        'conservation_drift', NaN, ...
        'kinetic_energy_drift', NaN, ...
        'enstrophy_drift', NaN, ...
        'circulation_drift', NaN, ...
        'initial_energy', NaN, ...
        'final_energy', NaN, ...
        'initial_enstrophy', NaN, ...
        'final_enstrophy', NaN, ...
        'initial_circulation', NaN, ...
        'final_circulation', NaN, ...
        'aliasing_indicator', NaN, ...
        'smoothness_indicator', NaN, ...
        'xi', NaN, ...
        'xi_tol', NaN, ...
        'selected_level', false, ...
        'fallback_selected', false, ...
        'convergence_verdict', '', ...
        'stop_reason', '', ...
        'selection_reason', '');
end

function summary = empty_phase1_stage_summary()
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
        'final_xi', NaN, ...
        'final_peak_error', NaN, ...
        'runtime_wall_s', NaN, ...
        'warning_messages', {{}}, ...
        'stable', false, ...
        'unstable_levels_present', false, ...
        'unstable_level_indices', [], ...
        'unstable_level_labels', {{}});
end

function output = promote_phase1_mesh_level_summaries(output, phase_paths)
    if ~isstruct(output) || ~isfield(output, 'results') || ~isstruct(output.results)
        return;
    end
    run_records = pick_value(output.results, 'run_records', struct([]));
    if ~isstruct(run_records) || isempty(run_records)
        return;
    end

    method_name = pick_text(output, {'method_key', 'method'}, pick_text(output.results, {'method', 'method_type'}, 'method'));
    method_dir = resolve_mesh_convergence_method_summary_dir(method_name, phase_paths);
    if isempty(strtrim(method_dir))
        return;
    end
    if exist(method_dir, 'dir') ~= 7
        mkdir(method_dir);
    end

    promoted_paths = cell(1, numel(run_records));
    for i = 1:numel(run_records)
        source_path = resolve_phase1_mesh_level_summary_path(run_records(i), output, i);
        if isempty(source_path) || exist(source_path, 'file') ~= 2
            continue;
        end
        target_path = fullfile(method_dir, sprintf('mesh_level_%02d.mat', i));
        if ~strcmpi(char(string(source_path)), char(string(target_path)))
            copyfile(source_path, target_path, 'f');
        end
        run_records(i).mesh_level_summary_path = target_path;
        promoted_paths{i} = target_path;
    end

    output.results.run_records = run_records;
    output.results.mesh_level_summary_paths = promoted_paths;
end

function method_dir = resolve_mesh_convergence_method_summary_dir(method_name, phase_paths)
    method_label = phase1_publication_method_label(method_name);
    method_dir = fullfile(pick_text(phase_paths, {'data'}, ''), 'MeshLevelSummaries', method_label);
end

function output = sanitize_mesh_convergence_child_output(output, phase_paths, phase_cfg)
    if nargin < 3 || ~isstruct(phase_cfg)
        phase_cfg = struct();
    end
    if ~isstruct(output) || ~isfield(output, 'results') || ~isstruct(output.results)
        return;
    end
    output = promote_phase1_mesh_level_summaries(output, phase_paths);

    method_dir = resolve_mesh_convergence_method_summary_dir( ...
        pick_text(output, {'method_key', 'method'}, pick_text(output.results, {'method', 'method_type'}, 'method')), ...
        phase_paths);
    if isempty(strtrim(method_dir))
        return;
    end
    if exist(method_dir, 'dir') ~= 7
        mkdir(method_dir);
    end

    published_results_path = fullfile(method_dir, 'convergence_results.mat');
    results_clean = filter_graphics_objects(output.results);
    safe_save_mat(published_results_path, struct('results', results_clean), '-v7.3');
    if json_saving_enabled(phase_cfg, output.results)
        write_json(fullfile(method_dir, 'convergence_results.json'), results_clean);
    end

    output.results.data_path = published_results_path;
    if ~isfield(output, 'paths') || ~isstruct(output.paths)
        output.paths = struct();
    end
    output.paths.base = method_dir;
    output.paths.data = method_dir;
    output.paths.method_root = method_dir;
    output.paths.mesh_level_summary_root = method_dir;
end

function queue_outputs = replace_mesh_convergence_queue_output(queue_outputs, replacement_output)
    if ~isstruct(queue_outputs) || isempty(queue_outputs) || ~isstruct(replacement_output)
        return;
    end
    replace_index = NaN;
    if isfield(replacement_output, 'queue_index')
        replace_index = double(replacement_output.queue_index);
    end
    for i = 1:numel(queue_outputs)
        if isfinite(replace_index) && isfield(queue_outputs(i), 'queue_index') && ...
                double(queue_outputs(i).queue_index) == replace_index
            queue_outputs(i) = replacement_output;
            return;
        end
        if strcmpi(pick_text(queue_outputs(i), {'method_key', 'method'}, ''), ...
                pick_text(replacement_output, {'method_key', 'method'}, '')) && ...
                strcmpi(pick_text(queue_outputs(i), {'stage'}, ''), pick_text(replacement_output, {'stage'}, ''))
            queue_outputs(i) = replacement_output;
            return;
        end
    end
end

function cleanup_roots = collect_mesh_convergence_cleanup_roots(queue_outputs, phase_paths)
    cleanup_roots = strings(1, 0);
    phase_root = char(string(pick_text(phase_paths, {'base'}, '')));
    for i = 1:numel(queue_outputs)
        candidate = char(string(pick_text(pick_struct(queue_outputs(i), {'paths'}, struct()), {'base'}, '')));
        if strlength(string(strtrim(candidate))) == 0
            continue;
        end
        if startsWith(string(candidate), string(phase_root), 'IgnoreCase', true)
            cleanup_roots(end + 1) = string(candidate); %#ok<AGROW>
        end
    end
    cleanup_roots = unique(cleanup_roots, 'stable');
end

function cleanup_mesh_convergence_child_roots(cleanup_roots, phase_paths, progress_callback)
    if isempty(cleanup_roots)
        return;
    end
    phase_root = char(string(pick_text(phase_paths, {'base'}, '')));
    for i = 1:numel(cleanup_roots)
        root_path = char(string(cleanup_roots(i)));
        if isempty(root_path) || exist(root_path, 'dir') ~= 7
            continue;
        end
        [parent_dir, leaf_name] = fileparts(root_path);
        if ~strcmpi(parent_dir, phase_root)
            continue;
        end
        if ~startsWith(leaf_name, '01_') && ~startsWith(leaf_name, '02_')
            continue;
        end
        try
            rmdir(root_path, 's');
            emit_phase_runtime_log(progress_callback, sprintf('Removed transient mesh child folder: %s', root_path), 'info');
        catch ME
            emit_phase_runtime_log(progress_callback, sprintf('Could not remove transient mesh child folder %s: %s', root_path, ME.message), 'warning');
        end
    end
end

function source_path = resolve_phase1_mesh_level_summary_path(record, output, level_index)
    source_path = pick_text(record, {'mesh_level_summary_path'}, '');
    if ~isempty(source_path) && exist(source_path, 'file') == 2
        return;
    end

    output_data_root = pick_text(output.paths, {'data'}, '');
    if ~isempty(output_data_root)
        candidate = fullfile(output_data_root, sprintf('mesh_level_%02d.mat', level_index));
        if exist(candidate, 'file') == 2
            source_path = candidate;
            return;
        end
    end

    data_path = pick_text(record, {'data_path'}, '');
    if ~isempty(data_path)
        candidate = fullfile(fileparts(data_path), sprintf('mesh_level_%02d.mat', level_index));
        if exist(candidate, 'file') == 2
            source_path = candidate;
            return;
        end
    end

    source_path = '';
end

function tf = local_phase1_monotone_ok(relative_changes)
    finite_changes = relative_changes(isfinite(relative_changes) & relative_changes >= 0);
    if numel(finite_changes) < 2
        tf = true;
        return;
    end
    tf = true;
    for i = 2:numel(finite_changes)
        if finite_changes(i) > finite_changes(i - 1) * 1.05
            tf = false;
            return;
        end
    end
end

function tf = local_phase1_plateau_detected(relative_changes)
    finite_changes = relative_changes(isfinite(relative_changes) & relative_changes >= 0);
    tf = numel(finite_changes) >= 2 && finite_changes(end) >= finite_changes(end - 1) / 1.05;
end

function value = last_finite_local(values)
    value = NaN;
    values = reshape(double(values), 1, []);
    idx = find(isfinite(values), 1, 'last');
    if ~isempty(idx)
        value = values(idx);
    end
end

function value = first_finite_local(values)
    value = NaN;
    values = reshape(double(values), 1, []);
    idx = find(isfinite(values), 1, 'first');
    if ~isempty(idx)
        value = values(idx);
    end
end

function jobs = propagate_selected_mesh(jobs, mesh, method_key)
    for i = 1:numel(jobs)
        if strcmp(jobs(i).method_key, method_key) && any(strcmp(jobs(i).stage, {'evolution', 'ic_study'}))
            jobs(i).selected_mesh = mesh;
            jobs(i).parameters = ConvergedMeshRegistry.apply_to_parameters(jobs(i).parameters, mesh);
            jobs(i).settings.selected_mesh = mesh;
        end
    end
end

function outputs = inject_selected_mesh_into_queue(outputs, jobs, mesh, method_key)
    for i = 1:numel(outputs)
        if strcmp(jobs(i).method_key, method_key) && strcmp(jobs(i).stage, 'convergence')
            outputs(i).selected_mesh = mesh;
        end
    end
end

function emit_phase_queue_payload(progress_callback, phase_id, job, status, progress_pct, elapsed_wall, terminal_message, jobs, outputs, workflow_kind)
    if isempty(progress_callback)
        return;
    end
    if nargin < 10 || strlength(string(workflow_kind)) == 0
        workflow_kind = 'phase1_periodic_comparison';
    end

    queue_total = numel(jobs);
    queue_status = build_queue_status_snapshot(jobs, outputs, job.queue_index, status);
    current_output = empty_output();
    if numel(outputs) >= job.queue_index && isstruct(outputs(job.queue_index))
        current_output = outputs(job.queue_index);
    end
    child_run_id = pick_text(current_output.run_config, {'run_id', 'study_id'}, ...
        pick_text(job.run_config, {'run_id', 'study_id'}, ''));
    child_artifact_root = pick_text(current_output.paths, {'base'}, char(string(job.output_root)));
    child_figures_root = pick_text(current_output.paths, {'figures_root', 'figures_evolution'}, '');
    child_reports_root = pick_text(current_output.paths, {'reports'}, '');
    selected_mesh = struct();
    if isstruct(current_output) && isfield(current_output, 'selected_mesh') && isstruct(current_output.selected_mesh)
        selected_mesh = current_output.selected_mesh;
    elseif isstruct(job.selected_mesh) && ~isempty(job.selected_mesh)
        selected_mesh = job.selected_mesh;
    end
    [child_mesh_nx, child_mesh_ny] = phase1_job_display_mesh(job);
    if isstruct(selected_mesh) && ~isempty(selected_mesh)
        child_mesh_nx = pick_numeric(selected_mesh, {'Nx'}, child_mesh_nx);
        child_mesh_ny = pick_numeric(selected_mesh, {'Ny'}, child_mesh_ny);
    end

    workflow_label = workflow_kind_display_name(workflow_kind);
    workflow_phase = workflow_phase_token(workflow_kind);
    payload = struct();
    payload.channel = 'workflow';
    payload.phase = workflow_phase;
    payload.phase_id = phase_id;
    payload.run_id = phase_id;
    payload.workflow_kind = char(string(workflow_kind));
    payload.stage_name = job.job_key;
    payload.stage_index = double(job.queue_index);
    payload.stage_total = double(queue_total);
    payload.queue_index = double(job.queue_index);
    payload.queue_total = double(queue_total);
    payload.job_key = job.job_key;
    payload.job_label = job.label;
    payload.method = job.method;
    payload.mode = job.stage;
    payload.status = char(string(status));
    payload.artifact_root = char(string(job.output_root));
    payload.child_run_id = child_run_id;
    payload.child_artifact_root = child_artifact_root;
    payload.child_figures_root = child_figures_root;
    payload.child_reports_root = child_reports_root;
    payload.mesh_nx = double(child_mesh_nx);
    payload.mesh_ny = double(child_mesh_ny);
    payload.test_case_setup = build_phase1_test_case_setup(job.parameters);
    payload.scenario_label = job.label;
    payload.workflow_overall_progress_pct = double(progress_pct);
    payload.progress_pct = double(progress_pct);
    payload.elapsed_wall = double(elapsed_wall);
    payload.status_text = sprintf('%s [%d/%d] %s (%s)', ...
        workflow_label, round(double(job.queue_index)), round(double(queue_total)), job.label, char(string(status)));
    payload.event_key = sprintf('%s_%02d_%s_%s', phase_id, round(double(job.queue_index)), job.job_key, lower(char(string(status))));
    payload.queue_status = queue_status;
    if nargin >= 7
        payload.terminal_message = char(string(terminal_message));
    else
        payload.terminal_message = '';
    end

    try
        invoke_runtime_progress_callback(progress_callback, payload);
    catch ME
        warning('Phase1PeriodicComparison:ProgressCallbackDisabled', ...
            'Phase workflow progress callback failed and will be ignored: %s', ME.message);
    end
end

function queue_status = build_queue_status_snapshot(jobs, outputs, active_index, active_status)
    queue_status = repmat(struct( ...
        'queue_index', NaN, ...
        'job_key', '', ...
        'job_label', '', ...
        'method', '', ...
        'mode', '', ...
        'mesh_nx', NaN, ...
        'mesh_ny', NaN, ...
        'test_case_setup', '', ...
        'scenario_label', '', ...
        'status', 'queued', ...
        'run_id', '', ...
        'artifact_root', '', ...
        'figures_root', '', ...
        'reports_root', ''), 1, numel(jobs));

    for i = 1:numel(jobs)
        queue_status(i).queue_index = jobs(i).queue_index;
        queue_status(i).job_key = jobs(i).job_key;
        queue_status(i).job_label = jobs(i).label;
        queue_status(i).method = jobs(i).method;
        queue_status(i).mode = jobs(i).stage;
        queue_status(i).artifact_root = jobs(i).output_root;
        [queue_status(i).mesh_nx, queue_status(i).mesh_ny] = phase1_job_display_mesh(jobs(i));
        queue_status(i).test_case_setup = build_phase1_test_case_setup(jobs(i).parameters);
        queue_status(i).scenario_label = jobs(i).label;
        if nargin >= 2 && numel(outputs) >= i && isstruct(outputs(i)) && ...
                isfield(outputs(i), 'status') && ~isempty(outputs(i).status)
            queue_status(i).status = outputs(i).status;
        elseif i < active_index
            queue_status(i).status = 'completed';
        elseif i == active_index
            queue_status(i).status = active_status;
        else
            queue_status(i).status = 'queued';
        end

        if nargin >= 2 && numel(outputs) >= i && isstruct(outputs(i))
            queue_status(i).run_id = pick_text(outputs(i).run_config, {'run_id', 'study_id'}, '');
            if isfield(outputs(i), 'paths') && isstruct(outputs(i).paths) && isfield(outputs(i).paths, 'base')
                queue_status(i).artifact_root = char(string(outputs(i).paths.base));
            end
            if isfield(outputs(i), 'selected_mesh') && isstruct(outputs(i).selected_mesh) && ~isempty(outputs(i).selected_mesh)
                queue_status(i).mesh_nx = pick_numeric(outputs(i).selected_mesh, {'Nx'}, queue_status(i).mesh_nx);
                queue_status(i).mesh_ny = pick_numeric(outputs(i).selected_mesh, {'Ny'}, queue_status(i).mesh_ny);
            elseif isstruct(jobs(i).selected_mesh) && ~isempty(jobs(i).selected_mesh)
                queue_status(i).mesh_nx = pick_numeric(jobs(i).selected_mesh, {'Nx'}, queue_status(i).mesh_nx);
                queue_status(i).mesh_ny = pick_numeric(jobs(i).selected_mesh, {'Ny'}, queue_status(i).mesh_ny);
            end
            queue_status(i).figures_root = pick_text(outputs(i).paths, {'figures_root', 'figures_evolution'}, '');
            queue_status(i).reports_root = pick_text(outputs(i).paths, {'reports'}, '');
        elseif isstruct(jobs(i).selected_mesh) && ~isempty(jobs(i).selected_mesh)
            queue_status(i).mesh_nx = pick_numeric(jobs(i).selected_mesh, {'Nx'}, queue_status(i).mesh_nx);
            queue_status(i).mesh_ny = pick_numeric(jobs(i).selected_mesh, {'Ny'}, queue_status(i).mesh_ny);
        end
    end
end

function [mesh_nx, mesh_ny] = phase1_job_display_mesh(job)
    mesh_nx = pick_numeric(job.parameters, {'Nx'}, NaN);
    mesh_ny = pick_numeric(job.parameters, {'Ny'}, NaN);
    if isfield(job, 'stage') && strcmp(job.stage, 'convergence')
        levels = double(reshape(pick_value(job.parameters, 'mesh_sizes', []), 1, []));
        levels = levels(isfinite(levels));
        if ~isempty(levels)
            first_mesh = round(double(levels(1)));
            mesh_nx = first_mesh;
            mesh_ny = first_mesh;
        end
    end
end

function message = build_child_completion_message(job, output, queue_index, queue_total, workflow_label)
    if nargin < 5 || strlength(string(workflow_label)) == 0
        workflow_label = 'Phase 1';
    end
    message = sprintf('Completed %s child job %d/%d: %s', workflow_label, queue_index, queue_total, job.label);
    if strcmp(job.stage, 'convergence') && isstruct(output) && isfield(output, 'selected_mesh') && ...
            isstruct(output.selected_mesh) && ~isempty(output.selected_mesh)
        mesh_label = mesh_label_from_entry(output.selected_mesh);
        selection_reason = pick_text(output.selected_mesh, {'selection_reason'}, '');
        final_change = pick_numeric(output.selected_mesh, {'final_relative_change'}, NaN);
        peak_change = pick_numeric(output.selected_mesh, {'final_peak_error', 'xi_peak'}, NaN);
        tolerance = pick_numeric(output.selected_mesh, {'tolerance'}, NaN);
        tolerance_met = logical(pick_value(output.selected_mesh, 'tolerance_met', false));
        l2_tolerance_met = logical(pick_value(output.selected_mesh, 'xi_l2_tol_met', false));
        peak_tolerance_met = logical(pick_value(output.selected_mesh, 'xi_peak_tol_met', false));
        fallback_continued = logical(pick_value(output.selected_mesh, 'continued_after_unconverged_mesh', false));
        convergence_status = pick_text(output.selected_mesh, {'convergence_status', 'verdict'}, '');
        reason_text = '';
        if strlength(string(strtrim(selection_reason))) > 0
            reason_text = sprintf(' | selection=%s', selection_reason);
        end
        if strlength(string(strtrim(convergence_status))) > 0
            reason_text = sprintf('%s | verdict=%s', reason_text, convergence_status);
        end
        metric_text = '';
        if isfinite(final_change) && isfinite(peak_change) && isfinite(tolerance)
            if tolerance_met
                metric_text = sprintf(' | xi_L2=%.2f%% | xi_peak=%.2f%% | tol=%.2f%% | both_pass=[%s,%s]', ...
                    final_change, peak_change, tolerance, logical_text_local(l2_tolerance_met), logical_text_local(peak_tolerance_met));
            elseif fallback_continued
                metric_text = sprintf(' | xi_L2=%.2f%% | xi_peak=%.2f%% | tol=%.2f%% | both_pass=[%s,%s] | continuing with finest fallback mesh', ...
                    final_change, peak_change, tolerance, logical_text_local(l2_tolerance_met), logical_text_local(peak_tolerance_met));
            else
                metric_text = sprintf(' | xi_L2=%.2f%% | xi_peak=%.2f%% | tol=%.2f%% | both_pass=[%s,%s]', ...
                    final_change, peak_change, tolerance, logical_text_local(l2_tolerance_met), logical_text_local(peak_tolerance_met));
            end
        elseif isfinite(final_change) || isfinite(peak_change)
            metric_text = sprintf(' | xi_L2=%s | xi_peak=%s', ...
                phase1_percent_text(final_change), phase1_percent_text(peak_change));
        end
        message = sprintf('%s | selected mesh=%s%s%s', message, mesh_label, reason_text, metric_text);
    end
end

function text = build_phase1_test_case_setup(parameters)
    bc_case = pick_text(parameters, {'bc_case', 'boundary_condition_case'}, 'periodic');
    bathymetry_id = pick_text(parameters, {'bathymetry_scenario'}, 'flat_2d');
    text = sprintf('bc_case=%s | bathymetry=%s', char(string(bc_case)), char(string(bathymetry_id)));
    case_label = pick_text(parameters, {'phase1_ic_study_case_label'}, '');
    if ~isempty(case_label)
        text = sprintf('%s | ic_study=%s', text, char(string(case_label)));
    end
end

function output = require_output(outputs, method_key, stage_name)
    idx = find(strcmp({outputs.method_key}, method_key) & strcmp({outputs.stage}, stage_name), 1, 'first');
    if isempty(idx)
        error('Phase1PeriodicComparison:MissingOutput', ...
            'Missing %s output for %s.', stage_name, method_key);
    end
    output = outputs(idx);
end

function mesh = select_mesh_from_convergence(results, convergence_paths, phase_cfg)
    if nargin < 3 || ~isstruct(phase_cfg)
        phase_cfg = struct();
    end
    mesh = ConvergedMeshRegistry.from_results(results, ...
        fullfile(convergence_paths.data, 'convergence_results.mat'), ...
        fullfile(convergence_paths.config, 'Config.mat'), struct('datenum', now));
    record = select_phase1_mesh_record(results);
    if ~isempty(record)
        mesh.Nx = pick_numeric(record, {'Nx'}, mesh.Nx);
        mesh.Ny = pick_numeric(record, {'Ny'}, mesh.Ny);
        mesh.Nz = pick_numeric(record, {'Nz'}, mesh.Nz);
        mesh.dt = pick_numeric(record, {'dt'}, mesh.dt);
        mesh.h = pick_numeric(record, {'h'}, mesh.h);
        mesh.mode_count = pick_numeric(record, {'mode_count'}, mesh.mode_count);
        mesh.dof = pick_numeric(record, {'dof'}, mesh.dof);
        mesh.refinement_axis = pick_text(record, {'refinement_axis'}, mesh.refinement_axis);
        mesh.verdict = pick_text(record, {'convergence_verdict'}, mesh.verdict);
        mesh.is_converged = strcmpi(mesh.verdict, 'converged');
        mesh.selection_reason = pick_text(record, {'selection_reason'}, '');
        mesh.final_relative_change = pick_numeric(record, {'xi', 'relative_change'}, NaN);
        mesh.final_peak_error = pick_numeric(record, {'max_vorticity_rel_error_pct'}, NaN);
        mesh.selection_metric = pick_numeric(record, {'xi', 'relative_change'}, NaN);
        mesh.xi = pick_numeric(record, {'xi', 'relative_change'}, NaN);
        mesh.xi_peak = pick_numeric(record, {'max_vorticity_rel_error_pct'}, NaN);
        mesh.xi_tol = pick_numeric(record, {'xi_tol'}, NaN);
        mesh.xi_l2_tol_met = logical(pick_value(record, 'xi_l2_tol_met', false));
        mesh.xi_peak_tol_met = logical(pick_value(record, 'xi_peak_tol_met', false));
        mesh.joint_tolerance_met = logical(pick_value(record, 'joint_tolerance_met', false));
    end
    mesh.convergence_status = pick_text(results, {'verdict'}, pick_text(pick_struct(results, {'summary'}, struct()), {'overall_verdict'}, mesh.verdict));
    mesh.tolerance = pick_numeric(results, {'xi_tol', 'tolerance'}, pick_numeric(phase_cfg, {'convergence_tolerance'}, NaN));
    mesh.tolerance_met = strcmpi(mesh.verdict, 'converged') || logical(pick_value(mesh, 'joint_tolerance_met', false));
    if ~mesh.tolerance_met && isfinite(mesh.final_relative_change) && isfinite(mesh.final_peak_error) && isfinite(mesh.tolerance)
        mesh.xi_l2_tol_met = mesh.final_relative_change < mesh.tolerance;
        mesh.xi_peak_tol_met = mesh.final_peak_error < mesh.tolerance;
        mesh.joint_tolerance_met = mesh.xi_l2_tol_met && mesh.xi_peak_tol_met;
        mesh.tolerance_met = mesh.joint_tolerance_met;
    end
    mesh.fallback_policy = pick_text(phase_cfg, {'mesh_selection_policy'}, 'first_converged_or_finest_mesh');
    mesh.fallback_used = any(strcmpi(mesh.selection_reason, {'finest_mesh_fallback', 'finest_stable_fallback'}));
    mesh.continued_after_unconverged_mesh = mesh.fallback_used && ...
        logical(pick_value(phase_cfg, 'allow_unconverged_mesh_fallback', true)) && ...
        ~strcmpi(mesh.convergence_status, 'unstable');
    if ~isfinite(mesh.Nx) || ~isfinite(mesh.Ny)
        error('Phase1PeriodicComparison:InvalidConvergedMesh', ...
            'Convergence output did not contain finite Nx/Ny mesh metadata.');
    end
    if mesh.fallback_used && ~mesh.continued_after_unconverged_mesh
        error('Phase1PeriodicComparison:UnconvergedMeshFallbackBlocked', ...
            'Phase 1 convergence fallback is blocked because the study ended with verdict "%s".', ...
            char(string(mesh.convergence_status)));
    end
end

function params = apply_phase1_convergence_ladder(params, method_name)
    phase_cfg = pick_struct(params, {'phase1'}, struct());
    if strcmpi(char(string(method_name)), 'spectral')
        levels = resolve_phase1_mesh_levels(phase_cfg, ...
            'convergence_mesh_levels_spectral', [32, 155, 277, 400, 523, 645, 768], true);
    else
        levels = resolve_phase1_mesh_levels(phase_cfg, ...
            'convergence_mesh_levels_fd', [32, 155, 277, 400, 523, 645, 768], false);
    end

    params.mesh_sizes = levels;
    params.convergence_N_coarse = double(levels(1));
    params.convergence_N_max = double(levels(end));

    if ~isfield(params, 'convergence') || ~isstruct(params.convergence)
        params.convergence = struct();
    end
    if ~isfield(params.convergence, 'study') || ~isstruct(params.convergence.study)
        params.convergence.study = struct();
    end
    if ~isfield(params.convergence.study, 'spatial') || ~isstruct(params.convergence.study.spatial)
        params.convergence.study.spatial = struct();
    end
    if ~isfield(params.convergence.study, 'temporal') || ~isstruct(params.convergence.study.temporal)
        params.convergence.study.temporal = struct();
    end

    params.convergence.study.N_coarse = params.convergence_N_coarse;
    params.convergence.study.N_max = params.convergence_N_max;
    params.convergence.study.spatial.N_values = levels;
    params.convergence.study.temporal.fine_N = params.convergence_N_max;
end

function levels = resolve_phase1_mesh_levels(cfg_struct, field_name, default_levels, ~)
    phase_cfg = cfg_struct;
    if ~isstruct(phase_cfg)
        phase_cfg = struct();
    end
    phase_cfg.mesh_ladder_mode = normalize_phase1_mesh_ladder_mode( ...
        pick_text(phase_cfg, {'mesh_ladder_mode'}, 'bounded'));
    phase_cfg.mesh_powers_of_two_max_n = max(8, round(double(pick_value(phase_cfg, 'mesh_powers_of_two_max_n', 1024))));
    if ~isfield(phase_cfg, 'mesh_level_count') || ~isnumeric(phase_cfg.mesh_level_count) || ...
            ~isscalar(phase_cfg.mesh_level_count) || ~isfinite(phase_cfg.mesh_level_count)
        phase_cfg.mesh_level_count = numel(default_levels);
    end

    expected_levels = double(reshape(Phase1MeshLadder(phase_cfg), 1, []));
    levels = expected_levels;
    if isstruct(cfg_struct) && isfield(cfg_struct, field_name) && isnumeric(cfg_struct.(field_name))
        explicit_levels = double(cfg_struct.(field_name));
        explicit_levels = reshape(round(explicit_levels), 1, []);
        explicit_levels = explicit_levels(isfinite(explicit_levels) & explicit_levels > 0);
        explicit_levels = unique(explicit_levels, 'stable');
        if ~isempty(explicit_levels) && ~isequal(explicit_levels, expected_levels)
            SafeConsoleIO.fprintf('Phase 1 mesh ladder normalization | %s | mode=%s | configured=%s | resolved=%s\n', ...
                field_name, phase1_mesh_ladder_mode_text(phase_cfg.mesh_ladder_mode), ...
                phase1_mesh_ladder_text(explicit_levels), phase1_mesh_ladder_text(expected_levels));
        end
    end

    if isempty(levels)
        error('Phase1PeriodicComparison:InvalidMeshLevelCount', ...
            'Phase 1 %s did not resolve to any valid mesh levels.', field_name);
    end
    if any(diff(levels) <= 0)
        error('Phase1PeriodicComparison:NonMonotoneMeshLevels', ...
            'Phase 1 %s must be strictly increasing.', field_name);
    end
    if strcmpi(phase_cfg.mesh_ladder_mode, 'powers_of_2') && max(levels) > phase_cfg.mesh_powers_of_two_max_n
        error('Phase1PeriodicComparison:MeshLevelsTooLarge', ...
            'Phase 1 %s must not exceed %d in powers-of-two mode.', field_name, phase_cfg.mesh_powers_of_two_max_n);
    end
end

function mesh_level_count = resolve_phase1_mesh_level_count(cfg_struct, default_count)
    mesh_level_count = max(2, round(double(default_count)));
    if isstruct(cfg_struct) && isfield(cfg_struct, 'mesh_level_count') && isnumeric(cfg_struct.mesh_level_count) ...
            && isscalar(cfg_struct.mesh_level_count) && isfinite(cfg_struct.mesh_level_count)
        mesh_level_count = max(2, round(double(cfg_struct.mesh_level_count)));
    end
end

function [start_n, final_n] = resolve_phase1_mesh_bounds(cfg_struct, default_levels)
    sanitized_defaults = reshape(round(double(default_levels)), 1, []);
    sanitized_defaults = sanitized_defaults(isfinite(sanitized_defaults) & sanitized_defaults > 0);
    if isempty(sanitized_defaults)
        sanitized_defaults = [32, 155, 277, 400, 523, 645, 768];
    end

    start_n = sanitized_defaults(1);
    final_n = sanitized_defaults(end);
    if ~isstruct(cfg_struct)
        return;
    end

    use_start_equal = true;
    if isfield(cfg_struct, 'mesh_start_equal_xy')
        use_start_equal = logical(cfg_struct.mesh_start_equal_xy);
    end
    use_final_equal = true;
    if isfield(cfg_struct, 'mesh_final_equal_xy')
        use_final_equal = logical(cfg_struct.mesh_final_equal_xy);
    end

    if use_start_equal
        if isfield(cfg_struct, 'mesh_start_n') && isnumeric(cfg_struct.mesh_start_n) && isscalar(cfg_struct.mesh_start_n) ...
                && isfinite(cfg_struct.mesh_start_n)
            start_n = round(double(cfg_struct.mesh_start_n));
        end
    else
        start_candidates = [];
        if isfield(cfg_struct, 'mesh_start_nx') && isnumeric(cfg_struct.mesh_start_nx) && isscalar(cfg_struct.mesh_start_nx) ...
                && isfinite(cfg_struct.mesh_start_nx)
            start_candidates(end + 1) = double(cfg_struct.mesh_start_nx); %#ok<AGROW>
        end
        if isfield(cfg_struct, 'mesh_start_ny') && isnumeric(cfg_struct.mesh_start_ny) && isscalar(cfg_struct.mesh_start_ny) ...
                && isfinite(cfg_struct.mesh_start_ny)
            start_candidates(end + 1) = double(cfg_struct.mesh_start_ny); %#ok<AGROW>
        end
        if ~isempty(start_candidates)
            start_n = round(max(start_candidates));
        end
    end

    if use_final_equal
        if isfield(cfg_struct, 'mesh_final_n') && isnumeric(cfg_struct.mesh_final_n) && isscalar(cfg_struct.mesh_final_n) ...
                && isfinite(cfg_struct.mesh_final_n)
            final_n = round(double(cfg_struct.mesh_final_n));
        end
    else
        final_candidates = [];
        if isfield(cfg_struct, 'mesh_final_nx') && isnumeric(cfg_struct.mesh_final_nx) && isscalar(cfg_struct.mesh_final_nx) ...
                && isfinite(cfg_struct.mesh_final_nx)
            final_candidates(end + 1) = double(cfg_struct.mesh_final_nx); %#ok<AGROW>
        end
        if isfield(cfg_struct, 'mesh_final_ny') && isnumeric(cfg_struct.mesh_final_ny) && isscalar(cfg_struct.mesh_final_ny) ...
                && isfinite(cfg_struct.mesh_final_ny)
            final_candidates(end + 1) = double(cfg_struct.mesh_final_ny); %#ok<AGROW>
        end
        if ~isempty(final_candidates)
            final_n = round(max(final_candidates));
        end
    end

    start_n = max(8, round(start_n));
    final_n = max(start_n, round(final_n));
end

function levels = build_phase1_mesh_ladder(start_n, final_n, count)
    start_n = max(8, round(double(start_n)));
    final_n = max(start_n, round(double(final_n)));
    count = max(2, round(double(count)));

    if (final_n - start_n + 1) < count
        error('Phase1PeriodicComparison:MeshRangeTooNarrow', ...
            'Phase 1 mesh range [%d, %d] cannot supply %d unique mesh levels.', ...
            start_n, final_n, count);
    end

    levels = round(linspace(start_n, final_n, count));
    levels(1) = start_n;
    levels(end) = final_n;

    for i = 2:count
        min_allowed = levels(i - 1) + 1;
        max_allowed = final_n - (count - i);
        levels(i) = min(max(levels(i), min_allowed), max_allowed);
    end

    for i = count-1:-1:1
        min_allowed = start_n + (i - 1);
        max_allowed = levels(i + 1) - 1;
        levels(i) = max(min(levels(i), max_allowed), min_allowed);
    end

    levels(1) = start_n;
    levels(end) = final_n;
end

function entry = coerce_registry_entry(raw_entry)
    entry = ConvergedMeshRegistry.empty_entry();
    if ~isstruct(raw_entry) || isempty(fieldnames(raw_entry))
        return;
    end
    valid_fields = intersect(fieldnames(entry), fieldnames(raw_entry), 'stable');
    for i = 1:numel(valid_fields)
        entry.(valid_fields{i}) = raw_entry.(valid_fields{i});
    end
end

function record = select_phase1_mesh_record(results)
    record = Phase1SelectMeshRecord(results);
end

function output = promote_phase1_selected_mesh_output(convergence_output, selected_mesh)
% Promote the selected convergence level into the baseline evolution slot.
%//NOTE Phase 1 no longer reruns the baseline elliptic case after mesh
% convergence; the first jointly accepted level becomes the canonical
% baseline evolution artifact for downstream RMSE, plots, and UI export.

    record = select_phase1_mesh_record(pick_struct(convergence_output, {'results'}, struct()));
    if isempty(record)
        error('Phase1PeriodicComparison:MissingSelectedMeshRecord', ...
            'Could not resolve the selected Phase 1 convergence record for %s.', ...
            char(string(pick_text(convergence_output, {'method'}, 'method'))));
    end

    summary_path = pick_text(record, {'mesh_level_summary_path'}, '');
    if isempty(summary_path) || exist(summary_path, 'file') ~= 2
        error('Phase1PeriodicComparison:MissingSelectedMeshSummary', ...
            'Selected Phase 1 mesh record does not expose a saved mesh-level summary MAT file.');
    end

    summary_payload = load(summary_path);
    level_paths = pick_struct(summary_payload, {'paths'}, struct());
    level_run_config = pick_struct(summary_payload, {'run_config'}, struct());
    level_parameters = pick_struct(summary_payload, {'parameters'}, struct());
    level_results = load_phase1_selected_mesh_results(record, summary_payload);

    output = empty_output();
    output.label = sprintf('%s selected baseline', char(string(convergence_output.method)));
    output.method = convergence_output.method;
    output.method_key = convergence_output.method_key;
    output.stage = 'evolution';
    output.job_key = sprintf('%s_baseline_from_convergence', normalize_method_key(convergence_output.method));
    output.queue_index = double(convergence_output.queue_index);
    output.run_config = level_run_config;
    output.parameters = level_parameters;
    output.settings = pick_struct(convergence_output, {'settings'}, struct());
    output.resource_allocation = pick_struct(convergence_output, {'resource_allocation'}, struct());
    output.results = level_results;
    output.paths = level_paths;
    output.wall_time = pick_numeric(level_results, {'wall_time', 'total_time'}, ...
        pick_numeric(record, {'runtime_wall_s'}, convergence_output.wall_time));
    output.execution_mode = 'phase1_selected_mesh_promotion';
    output.status = 'completed';
    output.selected_mesh = selected_mesh;
    output = promote_output_quick_access(output, pick_text(level_paths, {'base'}, ''));
end

function level_results = load_phase1_selected_mesh_results(record, summary_payload)
    level_results = struct();
    data_path = pick_text(record, {'data_path'}, '');
    if ~isempty(data_path) && exist(data_path, 'file') == 2
        data_payload = load(data_path);
        if isfield(data_payload, 'Results') && isstruct(data_payload.Results)
            level_results = data_payload.Results;
        end
        if (~isfield(level_results, 'analysis') || ~isstruct(level_results.analysis)) && ...
                isfield(data_payload, 'analysis') && isstruct(data_payload.analysis)
            level_results.analysis = data_payload.analysis;
        end
    end

    if isempty(fieldnames(level_results))
        results_summary = pick_struct(summary_payload, {'results_summary'}, struct());
        if isfield(results_summary, 'results') && isstruct(results_summary.results)
            level_results = results_summary.results;
        end
        if isfield(summary_payload, 'analysis') && isstruct(summary_payload.analysis)
            level_results.analysis = summary_payload.analysis;
        end
    end

    if ~isempty(data_path) && ~isfield(level_results, 'data_path')
        level_results.data_path = data_path;
    end
end

function job = make_job(label, method, stage, job_key, queue_index, run_config, parameters, settings, phase_paths)
    job = empty_job();
    job.label = label;
    job.method = method;
    job.method_key = normalize_method_key(method);
    job.stage = stage;
    job.job_key = job_key;
    job.queue_index = queue_index;
    job.output_root = build_phase1_child_output_root(phase_paths, queue_index, method, stage, job_key, run_config);
    job.run_config = run_config;
    job.parameters = parameters;
    job.settings = settings;
    job.settings.output_root = job.output_root;
    job.settings.preinitialized_artifact_root = true;
    job.selected_mesh = struct([]);
end

function output_root = build_phase1_child_output_root(phase_paths, queue_index, method_name, stage, job_key, run_config)
    phase_token = pick_text(phase_paths, {'phase'}, '');
    if strcmpi(phase_token, 'Phase1')
        child_root = pick_text(phase_paths, {'matlab_data_root', 'runs_root', 'base'}, '');
        child_token = phase1_publication_child_dir_name(method_name, stage, job_key, run_config);
        output_root = fullfile(child_root, child_token);
        return;
    end
    queue_token = compact_phase_job_dir_name(queue_index, method_name, stage);
    output_root = fullfile(pick_text(phase_paths, {'runs_root'}, pick_text(phase_paths, {'matlab_data_root'}, '')), queue_token);
end

function dir_name = phase1_publication_child_dir_name(method_name, stage, job_key, run_config)
    if nargin < 4 || ~isstruct(run_config)
        run_config = struct();
    end
    method_token = phase1_publication_method_label(method_name);
    case_id = pick_text(run_config, {'phase1_publication_case_id', 'phase1_ic_study_case_id'}, '');
    switch lower(strtrim(char(string(stage))))
        case 'convergence'
            dir_name = sprintf('%s_Convergence', method_token);
        otherwise
            if isempty(case_id)
                if contains(lower(char(string(job_key))), 'taylor_green')
                    case_id = 'taylor_green';
                else
                    baseline_meta = phase1_baseline_case_metadata(pick_text(run_config, {'ic_type'}, 'stretched_gaussian'));
                    case_id = baseline_meta.case_id;
                end
            end
            dir_name = sprintf('%s_%s', method_token, phase1_case_folder_name(case_id));
    end
end

function label = phase1_publication_method_label(method_name)
    if strcmpi(normalize_method_key(method_name), 'spectral')
        label = 'SM';
    else
        label = 'FD';
    end
end

function output_dir = phase1_workflow_visual_child_root(paths, method_name, case_id, fallback_token)
    visuals_root = pick_text(paths, {'visuals_root', 'figures_root', 'base'}, pwd);
    method_dir = phase1_publication_method_label(method_name);
    case_dir = phase1_case_folder_name(case_id);
    if isempty(strtrim(case_dir))
        case_dir = phase1_case_folder_name(fallback_token);
    end
    output_dir = fullfile(char(string(visuals_root)), char(string(method_dir)), char(string(case_dir)));
end

function label = phase1_case_display_label(case_id, fallback_label)
    case_id = lower(strtrim(char(string(case_id))));
    if nargin < 2
        fallback_label = '';
    end
    switch case_id
        case {'baseline_stretched_single', 'stretched', 'stretched_gaussian'}
            label = 'Stretched Gaussian';
        case {'', 'baseline_elliptic_single', 'elliptic', 'elliptical_vortex', 'elliptic_vortex'}
            label = 'Elliptic';
        case {'taylor_green', 'taylorgreen'}
            label = 'Taylor-Green';
        otherwise
            label = char(string(fallback_label));
            if isempty(strtrim(label))
                label = strrep(phase1_case_folder_name(case_id), '_', ' ');
            end
    end
end

function folder_name = phase1_case_folder_name(case_id)
    case_id = lower(strtrim(char(string(case_id))));
    switch case_id
        case {'baseline_stretched_single', 'stretched', 'stretched_gaussian'}
            folder_name = 'Stretched_Gaussian';
        case {'', 'baseline_elliptic_single', 'elliptic', 'elliptical_vortex', 'elliptic_vortex'}
            folder_name = 'Elliptic';
        case {'taylor_green', 'taylorgreen'}
            folder_name = 'Taylor_Green';
        otherwise
            token = regexprep(case_id, '[^a-zA-Z0-9]+', '_');
            token = regexprep(token, '_+', '_');
            token = regexprep(token, '^_+|_+$', '');
            if isempty(token)
                folder_name = 'Case';
                return;
            end
            parts = split(string(lower(token)), '_');
            for i = 1:numel(parts)
                if strlength(parts(i)) == 0
                    continue;
                end
                part = char(parts(i));
                part(1) = upper(part(1));
                parts(i) = string(part);
            end
            folder_name = char(strjoin(parts, "_"));
    end
end

function storage_id = make_phase_storage_id(phase_id)
    raw = lower(regexprep(char(string(phase_id)), '[^a-z0-9]+', '_'));
    stamp = compact_phase_stamp_token(raw);
    label = regexprep(raw, '_?\d{8}_\d{6}$', '');
    label = regexprep(label, '^phase\d*_?', '');
    ic_token = compact_phase_label_token(label);
    storage_id = sprintf('p1_%s_%s', stamp, ic_token);
end

function child_id = make_phase_child_identifier(~, method_name, stage)
    % The enclosing phase storage root and queue folder are already unique,
    % so keeping child ids compact prevents long worker save paths on
    % Windows.
    child_id = sprintf('p1%s', compact_phase_stage_token(method_name, stage));
end

function dir_name = compact_phase_job_dir_name(queue_index, method_name, stage)
    dir_name = sprintf('%02d_%s', round(double(queue_index)), compact_phase_stage_token(method_name, stage));
end

function token = compact_phase_internal_token(path_text, fallback)
    path_text = char(string(path_text));
    if nargin < 2 || strlength(string(fallback)) == 0
        fallback = 'p1job';
    end

    [~, leaf] = fileparts(path_text);
    leaf = lower(regexprep(char(string(leaf)), '[^a-z0-9]+', ''));
    if isempty(leaf)
        leaf = char(string(fallback));
    end
    leaf = leaf(1:min(numel(leaf), 8));

    normalized = lower(regexprep(path_text, '[^a-z0-9]+', '_'));
    stamp = compact_phase_stamp_token(normalized);
    stamp = stamp(1:min(numel(stamp), 6));
    token = sprintf('%s_%s', leaf, stamp);
end

function label = mesh_label_from_entry(entry)
    label = '--';
    if ~isstruct(entry) || isempty(entry)
        return;
    end
    nx = pick_numeric(entry, {'Nx'}, NaN);
    ny = pick_numeric(entry, {'Ny'}, NaN);
    if isfinite(nx) && isfinite(ny)
        label = sprintf('%dx%d', round(nx), round(ny));
    end
end

function stamp = compact_phase_stamp_token(raw_phase_id)
    raw_phase_id = char(string(raw_phase_id));
    stamp_match = regexp(raw_phase_id, '\d{8}_\d{6}$', 'match', 'once');
    if ~isempty(stamp_match)
        stamp = [stamp_match(3:8), stamp_match(10:15)];
        return;
    end

    hash_value = 0;
    for i = 1:numel(raw_phase_id)
        hash_value = mod(hash_value * 131 + double(raw_phase_id(i)), 1e9);
    end
    stamp = sprintf('%09.0f', hash_value);
end

function token = compact_phase_label_token(label_raw)
    label_raw = char(string(label_raw));
    parts = regexp(label_raw, '_+', 'split');
    parts = parts(~cellfun(@isempty, parts));
    if isempty(parts)
        token = 'wf';
        return;
    end

    if numel(parts) == 1
        token = parts{1}(1:min(4, strlength(string(parts{1}))));
        token = char(string(token));
        return;
    end

    initials = cellfun(@(p) p(1), parts(1:min(4, numel(parts))));
    token = char(initials);
end

function token = compact_phase_stage_token(method_name, stage)
    method_name = lower(char(string(method_name)));
    stage = lower(char(string(stage)));

    switch method_name
        case {'fd', 'finite difference', 'finite_difference'}
            method_token = 'fd';
        case {'spectral', 'fft', 'pseudo_spectral'}
            method_token = 'sp';
        case {'fv', 'finite volume', 'finite_volume'}
            method_token = 'fv';
        otherwise
            method_token = method_name(1:min(2, numel(method_name)));
    end

    switch stage
        case {'convergence', 'conv'}
            stage_token = 'c';
        case {'evolution', 'evo'}
            stage_token = 'e';
        otherwise
            stage_token = stage(1:min(1, numel(stage)));
    end

    token = sprintf('%s%s', method_token, stage_token);
end

function job = empty_job()
    job = struct( ...
        'label', '', ...
        'method', '', ...
        'method_key', '', ...
        'stage', '', ...
        'job_key', '', ...
        'queue_index', NaN, ...
        'output_root', '', ...
        'run_config', struct(), ...
        'parameters', struct(), ...
        'settings', struct(), ...
        'selected_mesh', struct([]));
end

function output = make_output(job, results, paths, execution_mode)
    output = empty_output();
    output.label = job.label;
    output.method = job.method;
    output.method_key = job.method_key;
    output.stage = job.stage;
    output.job_key = job.job_key;
    output.queue_index = job.queue_index;
    output.run_config = job.run_config;
    output.parameters = job.parameters;
    output.settings = filter_graphics_objects(job.settings);
    output.resource_allocation = pick_struct(job.settings, {'resource_allocation'}, struct());
    output.results = results;
    output.paths = paths;
    output.wall_time = pick_numeric(results, {'wall_time', 'total_time'}, NaN);
    output.execution_mode = execution_mode;
    output.status = 'completed';
    output.selected_mesh = job.selected_mesh;
end

function output = make_failed_output(job, ME)
    output = empty_output();
    output.label = job.label;
    output.method = job.method;
    output.method_key = job.method_key;
    output.stage = job.stage;
    output.job_key = job.job_key;
    output.queue_index = job.queue_index;
    output.run_config = job.run_config;
    output.parameters = job.parameters;
    output.settings = filter_graphics_objects(job.settings);
    output.resource_allocation = pick_struct(job.settings, {'resource_allocation'}, struct());
    output.results = struct('error_identifier', ME.identifier, 'error_message', ME.message);
    output.paths = struct('base', job.output_root);
    output.wall_time = NaN;
    output.execution_mode = 'dispatcher_queue';
    output.status = 'failed';
    output.selected_mesh = job.selected_mesh;
end

function output = empty_output()
    output = struct( ...
        'label', '', ...
        'method', '', ...
        'method_key', '', ...
        'stage', '', ...
        'job_key', '', ...
        'queue_index', NaN, ...
        'run_config', struct(), ...
        'parameters', struct(), ...
        'settings', struct(), ...
        'resource_allocation', struct(), ...
        'results', struct(), ...
        'paths', struct(), ...
        'quick_access', struct('root', '', 'data', '', 'figures', '', 'reports', '', 'sustainability', ''), ...
        'wall_time', NaN, ...
        'execution_mode', '', ...
        'status', '', ...
        'selected_mesh', struct([]));
end

function output = promote_output_quick_access(output, quick_root)
    if ~isstruct(output) || ~isfield(output, 'paths') || ~isstruct(output.paths)
        return;
    end
    quick_access = promote_phase_artifacts(output.paths, quick_root);
    output.quick_access = quick_access;
    if isstruct(quick_access)
        output.paths.quick_access_root = pick_text(quick_access, {'root'}, '');
        output.paths.quick_access_data = pick_text(quick_access, {'data'}, '');
        output.paths.quick_access_figures = pick_text(quick_access, {'figures'}, '');
        output.paths.quick_access_reports = pick_text(quick_access, {'reports'}, '');
        output.paths.quick_access_sustainability = pick_text(quick_access, {'sustainability'}, '');
    end
end

function metrics = compute_phase1_metrics(method_name, method_output, peer_output, mesh, convergence_output)
    if ~phase1_results_have_analysis(pick_struct(method_output, {'results'}, struct())) || ...
            ~phase1_results_have_analysis(pick_struct(peer_output, {'results'}, struct()))
        metrics = empty_phase1_metrics_struct(method_name, method_output, peer_output, mesh, convergence_output);
        return;
    end
    analysis = require_analysis(method_output.results, method_name);
    omega_initial = extract_omega_field(analysis, 'initial');
    omega_final = extract_omega_field(analysis, 'final');
    mismatch = compute_phase1_cross_method_mismatch_metrics(method_output, peer_output);
    if nargin < 4 || ~isstruct(mesh)
        mesh = ConvergedMeshRegistry.empty_entry();
    end
    if nargin < 5 || ~isstruct(convergence_output)
        convergence_output = struct();
    end
    has_convergence_output = isfield(convergence_output, 'results') && ...
        isstruct(convergence_output.results) && ~isempty(fieldnames(convergence_output.results));
    if has_convergence_output
        conv = summarize_convergence(convergence_output.results);
        mesh_source_path = fullfile(pick_text(convergence_output.paths, {'data'}, ''), 'convergence_results.mat');
    else
        conv = summarize_direct_mesh(mesh);
        mesh_source_path = pick_text(method_output.paths, {'base'}, '');
    end

    metrics = struct();
    metrics.method = char(string(method_name));
    metrics.phase_id = method_output.run_config.phase_id;
    metrics.comparison_target = char(string(peer_output.method));
    metrics.Nx = pick_numeric(method_output.parameters, {'Nx'}, NaN);
    metrics.Ny = pick_numeric(method_output.parameters, {'Ny'}, NaN);
    metrics.dt = pick_numeric(method_output.parameters, {'dt'}, NaN);
    metrics.dof = metrics.Nx * metrics.Ny;
    metrics.cross_method_mismatch_l2 = mismatch.cross_method_mismatch_l2;
    metrics.cross_method_mismatch_linf = mismatch.cross_method_mismatch_linf;
    metrics.relative_vorticity_error_L2 = metrics.cross_method_mismatch_l2;
    metrics.relative_vorticity_error_Linf = metrics.cross_method_mismatch_linf;
    metrics.cross_method_streamfunction_relative_l2_mismatch = mismatch.cross_method_streamfunction_relative_l2_mismatch;
    metrics.cross_method_speed_relative_l2_mismatch = mismatch.cross_method_speed_relative_l2_mismatch;
    metrics.cross_method_velocity_vector_relative_l2_mismatch = mismatch.cross_method_velocity_vector_relative_l2_mismatch;
    metrics.cross_method_streamline_direction_relative_l2_mismatch = mismatch.cross_method_streamline_direction_relative_l2_mismatch;

    metrics.observed_spatial_rate = conv.primary_observed_rate;
    metrics.observed_temporal_rate = conv.temporal_observed_rate;
    metrics.mesh_convergence_verdict = pick_text(mesh, {'verdict'}, conv.primary_verdict);
    metrics.mesh_refinement_axis = conv.primary_refinement_axis;
    metrics.mesh_source_path = mesh_source_path;
    metrics.mesh_selection_reason = pick_text(mesh, {'selection_reason'}, '');
    metrics.mesh_final_successive_vorticity_error = pick_numeric(mesh, {'final_relative_change'}, conv.primary_final_relative_change);
    metrics.mesh_final_peak_vorticity_error = pick_numeric(mesh, {'final_peak_error', 'xi_peak'}, NaN);
    metrics.mesh_tolerance = pick_numeric(mesh, {'tolerance'}, conv.tolerance);
    metrics.mesh_convergence_status = pick_text(mesh, {'convergence_status', 'status'}, conv.overall_verdict);
    metrics.mesh_tolerance_met = logical(pick_value(mesh, 'tolerance_met', strcmpi(metrics.mesh_convergence_verdict, 'converged')));
    metrics.mesh_l2_tolerance_met = logical(pick_value(mesh, 'xi_l2_tol_met', false));
    metrics.mesh_peak_tolerance_met = logical(pick_value(mesh, 'xi_peak_tol_met', false));
    metrics.mesh_fallback_used = logical(pick_value(mesh, 'fallback_used', false));
    metrics.continued_after_unconverged_mesh = logical(pick_value(mesh, 'continued_after_unconverged_mesh', false));

    vortex_initial = vortex_diagnostics(omega_initial, analysis);
    vortex_final = vortex_diagnostics(omega_final, analysis);
    metrics.peak_vorticity_ratio = vortex_final.peak_abs_omega / max(vortex_initial.peak_abs_omega, eps);
    metrics.centroid_drift = hypot(vortex_final.centroid_x - vortex_initial.centroid_x, ...
        vortex_final.centroid_y - vortex_initial.centroid_y);
    metrics.core_radius_initial = vortex_initial.core_radius;
    metrics.core_radius_final = vortex_final.core_radius;
    metrics.core_anisotropy_initial = vortex_initial.core_anisotropy;
    metrics.core_anisotropy_final = vortex_final.core_anisotropy;
    metrics.peak_vorticity_ratio_error = abs(metrics.peak_vorticity_ratio - 1);
    metrics.core_anisotropy_error = abs(metrics.core_anisotropy_final - metrics.core_anisotropy_initial);
    metrics.vortex_core_detected = vortex_final.core_detected;

    metrics.circulation_drift = conservation_drift(analysis, omega_initial, omega_final);
    metrics.kinetic_energy_drift = history_drift(analysis, 'kinetic_energy');
    metrics.enstrophy_drift = history_drift(analysis, 'enstrophy');
    metrics.initial_energy = first_finite_local(pick_value(analysis, 'kinetic_energy', []));
    metrics.final_energy = pick_numeric(method_output.results, {'final_energy'}, ...
        last_finite_local(pick_value(analysis, 'kinetic_energy', [])));
    metrics.initial_enstrophy = first_finite_local(pick_value(analysis, 'enstrophy', []));
    metrics.final_enstrophy = pick_numeric(method_output.results, {'final_enstrophy'}, ...
        last_finite_local(pick_value(analysis, 'enstrophy', [])));
    metrics.initial_circulation = first_finite_local(pick_value(analysis, 'circulation', []));
    metrics.final_circulation = last_finite_local(pick_value(analysis, 'circulation', []));
    metrics.observed_cfl = observed_cfl(analysis, method_output.parameters);
    metrics.cfl_adv = pick_numeric(method_output.results, {'cfl_adv_terminal'}, NaN);
    metrics.cfl_diff = pick_numeric(method_output.results, {'cfl_diff_terminal'}, NaN);
    metrics.cfl_terminal = pick_numeric(metrics, {'cfl_adv'}, metrics.observed_cfl);
    metrics.runtime_wall_s = pick_numeric(method_output.results, {'wall_time'}, method_output.wall_time);
    metrics.total_steps = pick_numeric(method_output.results, {'total_steps'}, NaN);
    metrics.time_per_step_s = metrics.runtime_wall_s / max(metrics.total_steps, 1);
    metrics.cost_accuracy = metrics.runtime_wall_s * metrics.cross_method_mismatch_l2;
    metrics.nan_inf_flag = logical(mismatch.nan_inf_flag);
    metrics.method_grid = sprintf('%dx%d', metrics.Nx, metrics.Ny);
    metrics.mesh_verdict = pick_text(mesh, {'verdict'}, metrics.mesh_convergence_verdict);
end

function mismatch = compute_phase1_cross_method_mismatch_metrics(method_output, peer_output)
    if ~phase1_results_have_analysis(pick_struct(method_output, {'results'}, struct())) || ...
            ~phase1_results_have_analysis(pick_struct(peer_output, {'results'}, struct()))
        mismatch = struct( ...
            'cross_method_mismatch_l2', NaN, ...
            'cross_method_mismatch_linf', NaN, ...
            'cross_method_mse', NaN, ...
            'cross_method_rmse', NaN, ...
            'cross_method_streamfunction_relative_l2_mismatch', NaN, ...
            'cross_method_speed_relative_l2_mismatch', NaN, ...
            'cross_method_velocity_vector_relative_l2_mismatch', NaN, ...
            'cross_method_streamline_direction_relative_l2_mismatch', NaN, ...
            'nan_inf_flag', true);
        return;
    end
    analysis = require_analysis(method_output.results, method_output.method);
    peer_analysis = require_analysis(peer_output.results, sprintf('%s comparison peer', peer_output.method));
    omega_final = extract_omega_field(analysis, 'final');
    method_state = resolve_phase1_comparison_state(analysis, omega_final, NaN);
    peer_state = resolve_phase1_comparison_state(peer_analysis, extract_omega_field(peer_analysis, 'final'), NaN);
    peer_state_on_method = remap_phase1_comparison_state(peer_state, peer_analysis, analysis, size(omega_final));
    snapshot_mismatch = compute_phase1_snapshot_mismatch(method_state, peer_state_on_method);
    diff_field = method_state.omega - peer_state_on_method.omega;
    mismatch = struct();
    mismatch.cross_method_mismatch_l2 = snapshot_mismatch.vorticity_relative_l2;
    mismatch.cross_method_mismatch_linf = field_relative_linf(diff_field, peer_state_on_method.omega);
    mismatch.cross_method_mse = mean(diff_field(:).^2, 'omitnan');
    mismatch.cross_method_rmse = sqrt(mismatch.cross_method_mse);
    mismatch.cross_method_streamfunction_relative_l2_mismatch = snapshot_mismatch.streamfunction_relative_l2;
    mismatch.cross_method_speed_relative_l2_mismatch = snapshot_mismatch.speed_relative_l2;
    mismatch.cross_method_velocity_vector_relative_l2_mismatch = snapshot_mismatch.velocity_vector_relative_l2;
    mismatch.cross_method_streamline_direction_relative_l2_mismatch = snapshot_mismatch.streamline_direction_relative_l2;
    mismatch.nan_inf_flag = snapshot_mismatch.nan_inf_flag;
end

function conv = summarize_direct_mesh(mesh)
    verdict = pick_text(mesh, {'verdict', 'status'}, 'direct_phase_config');
    conv = struct( ...
        'overall_verdict', verdict, ...
        'primary_refinement_axis', 'direct_config', ...
        'primary_observed_rate', NaN, ...
        'temporal_observed_rate', NaN, ...
        'primary_verdict', verdict, ...
        'primary_final_relative_change', NaN, ...
        'temporal_verdict', verdict, ...
        'temporal_final_relative_change', NaN, ...
        'tolerance', pick_numeric(mesh, {'tolerance'}, NaN));
end

function tf = phase1_results_have_analysis(results)
    tf = isstruct(results) && isfield(results, 'analysis') && isstruct(results.analysis) && ...
        (isfield(results.analysis, 'omega_snaps') || isfield(results.analysis, 'omega'));
end

function metrics = empty_phase1_metrics_struct(method_name, method_output, peer_output, mesh, convergence_output)
    if nargin < 4 || ~isstruct(mesh)
        mesh = ConvergedMeshRegistry.empty_entry();
    end
    if nargin < 5 || ~isstruct(convergence_output)
        convergence_output = struct();
    end
    if isfield(convergence_output, 'results') && isstruct(convergence_output.results) && ~isempty(fieldnames(convergence_output.results))
        conv = summarize_convergence(convergence_output.results);
        mesh_source_path = fullfile(pick_text(convergence_output.paths, {'data'}, ''), 'convergence_results.mat');
    else
        conv = summarize_direct_mesh(mesh);
        mesh_source_path = pick_text(method_output.paths, {'base'}, '');
    end
    metrics = struct();
    metrics.method = char(string(method_name));
    metrics.phase_id = pick_text(method_output.run_config, {'phase_id'}, '');
    metrics.comparison_target = char(string(pick_text(peer_output, {'method'}, '')));
    metrics.Nx = pick_numeric(method_output.parameters, {'Nx'}, NaN);
    metrics.Ny = pick_numeric(method_output.parameters, {'Ny'}, NaN);
    metrics.dt = pick_numeric(method_output.parameters, {'dt'}, NaN);
    metrics.dof = metrics.Nx * metrics.Ny;
    metrics.cross_method_mismatch_l2 = NaN;
    metrics.cross_method_mismatch_linf = NaN;
    metrics.relative_vorticity_error_L2 = NaN;
    metrics.relative_vorticity_error_Linf = NaN;
    metrics.cross_method_streamfunction_relative_l2_mismatch = NaN;
    metrics.cross_method_speed_relative_l2_mismatch = NaN;
    metrics.cross_method_velocity_vector_relative_l2_mismatch = NaN;
    metrics.cross_method_streamline_direction_relative_l2_mismatch = NaN;
    metrics.observed_spatial_rate = conv.primary_observed_rate;
    metrics.observed_temporal_rate = conv.temporal_observed_rate;
    metrics.mesh_convergence_verdict = pick_text(mesh, {'verdict'}, conv.primary_verdict);
    metrics.mesh_refinement_axis = conv.primary_refinement_axis;
    metrics.mesh_source_path = mesh_source_path;
    metrics.mesh_selection_reason = pick_text(mesh, {'selection_reason'}, '');
    metrics.mesh_final_successive_vorticity_error = pick_numeric(mesh, {'final_relative_change'}, conv.primary_final_relative_change);
    metrics.mesh_final_peak_vorticity_error = pick_numeric(mesh, {'final_peak_error', 'xi_peak'}, NaN);
    metrics.mesh_tolerance = pick_numeric(mesh, {'tolerance'}, conv.tolerance);
    metrics.mesh_convergence_status = pick_text(mesh, {'convergence_status', 'status'}, conv.overall_verdict);
    metrics.mesh_tolerance_met = logical(pick_value(mesh, 'tolerance_met', false));
    metrics.mesh_l2_tolerance_met = logical(pick_value(mesh, 'xi_l2_tol_met', false));
    metrics.mesh_peak_tolerance_met = logical(pick_value(mesh, 'xi_peak_tol_met', false));
    metrics.mesh_fallback_used = logical(pick_value(mesh, 'fallback_used', false));
    metrics.continued_after_unconverged_mesh = logical(pick_value(mesh, 'continued_after_unconverged_mesh', false));
    metrics.peak_vorticity_ratio = NaN;
    metrics.centroid_drift = NaN;
    metrics.core_radius_initial = NaN;
    metrics.core_radius_final = NaN;
    metrics.core_anisotropy_initial = NaN;
    metrics.core_anisotropy_final = NaN;
    metrics.peak_vorticity_ratio_error = NaN;
    metrics.core_anisotropy_error = NaN;
    metrics.vortex_core_detected = false;
    metrics.circulation_drift = NaN;
    metrics.kinetic_energy_drift = NaN;
    metrics.enstrophy_drift = NaN;
    metrics.initial_energy = NaN;
    metrics.final_energy = NaN;
    metrics.initial_enstrophy = NaN;
    metrics.final_enstrophy = NaN;
    metrics.initial_circulation = NaN;
    metrics.final_circulation = NaN;
    metrics.runtime_cpu_s = NaN;
    metrics.runtime_wall_s = pick_numeric(method_output.results, {'wall_time'}, method_output.wall_time);
    metrics.total_steps = pick_numeric(method_output.results, {'total_steps'}, NaN);
    metrics.time_per_step_s = NaN;
    metrics.cost_accuracy = NaN;
    metrics.nan_inf_flag = true;
    metrics.method_grid = sprintf('%dx%d', round(metrics.Nx), round(metrics.Ny));
    metrics.mesh_verdict = pick_text(mesh, {'verdict'}, metrics.mesh_convergence_verdict);
    metrics.cfl_adv = NaN;
    metrics.cfl_diff = NaN;
    metrics.observed_cfl = NaN;
    metrics.cfl_terminal = NaN;
end

function summary = compute_summary_metrics(fd_metrics, sp_metrics, queue_outputs, phase_cfg, error_vs_time, ic_study)
    summary = struct();
    summary.phase_id = fd_metrics.phase_id;
    summary.fd_vs_spectral_mismatch_l2 = fd_metrics.cross_method_mismatch_l2;
    summary.spectral_vs_fd_mismatch_l2 = sp_metrics.cross_method_mismatch_l2;
    summary.mean_cross_method_mismatch_l2 = mean([fd_metrics.cross_method_mismatch_l2, sp_metrics.cross_method_mismatch_l2], 'omitnan');
    summary.fd_vs_spectral_mismatch_linf = fd_metrics.cross_method_mismatch_linf;
    summary.spectral_vs_fd_mismatch_linf = sp_metrics.cross_method_mismatch_linf;
    summary.mean_cross_method_mismatch_linf = mean([fd_metrics.cross_method_mismatch_linf, sp_metrics.cross_method_mismatch_linf], 'omitnan');
    summary.fd_vs_spectral_streamfunction_mismatch_l2 = pick_numeric(fd_metrics, {'cross_method_streamfunction_relative_l2_mismatch'}, NaN);
    summary.spectral_vs_fd_streamfunction_mismatch_l2 = pick_numeric(sp_metrics, {'cross_method_streamfunction_relative_l2_mismatch'}, NaN);
    summary.mean_cross_method_streamfunction_mismatch_l2 = mean([ ...
        summary.fd_vs_spectral_streamfunction_mismatch_l2, ...
        summary.spectral_vs_fd_streamfunction_mismatch_l2], 'omitnan');
    summary.fd_vs_spectral_speed_mismatch_l2 = pick_numeric(fd_metrics, {'cross_method_speed_relative_l2_mismatch'}, NaN);
    summary.spectral_vs_fd_speed_mismatch_l2 = pick_numeric(sp_metrics, {'cross_method_speed_relative_l2_mismatch'}, NaN);
    summary.mean_cross_method_speed_mismatch_l2 = mean([ ...
        summary.fd_vs_spectral_speed_mismatch_l2, ...
        summary.spectral_vs_fd_speed_mismatch_l2], 'omitnan');
    summary.fd_vs_spectral_velocity_vector_mismatch_l2 = pick_numeric(fd_metrics, {'cross_method_velocity_vector_relative_l2_mismatch'}, NaN);
    summary.spectral_vs_fd_velocity_vector_mismatch_l2 = pick_numeric(sp_metrics, {'cross_method_velocity_vector_relative_l2_mismatch'}, NaN);
    summary.mean_cross_method_velocity_vector_mismatch_l2 = mean([ ...
        summary.fd_vs_spectral_velocity_vector_mismatch_l2, ...
        summary.spectral_vs_fd_velocity_vector_mismatch_l2], 'omitnan');
    summary.fd_vs_spectral_streamline_direction_mismatch_l2 = pick_numeric(fd_metrics, {'cross_method_streamline_direction_relative_l2_mismatch'}, NaN);
    summary.spectral_vs_fd_streamline_direction_mismatch_l2 = pick_numeric(sp_metrics, {'cross_method_streamline_direction_relative_l2_mismatch'}, NaN);
    summary.mean_cross_method_streamline_direction_mismatch_l2 = mean([ ...
        summary.fd_vs_spectral_streamline_direction_mismatch_l2, ...
        summary.spectral_vs_fd_streamline_direction_mismatch_l2], 'omitnan');
    summary.fd_runtime_wall_s = fd_metrics.runtime_wall_s;
    summary.spectral_runtime_wall_s = sp_metrics.runtime_wall_s;
    summary.runtime_ratio_fd_over_spectral = fd_metrics.runtime_wall_s / max(sp_metrics.runtime_wall_s, eps);
    summary.fd_convergence_verdict = fd_metrics.mesh_convergence_verdict;
    summary.spectral_convergence_verdict = sp_metrics.mesh_convergence_verdict;
    summary.fd_mesh_fallback_used = logical(fd_metrics.mesh_fallback_used);
    summary.spectral_mesh_fallback_used = logical(sp_metrics.mesh_fallback_used);
    summary.fd_continued_after_unconverged_mesh = logical(fd_metrics.continued_after_unconverged_mesh);
    summary.spectral_continued_after_unconverged_mesh = logical(sp_metrics.continued_after_unconverged_mesh);
    summary.continued_with_unconverged_mesh = summary.fd_continued_after_unconverged_mesh || ...
        summary.spectral_continued_after_unconverged_mesh;
    summary.stability_scope = phase_cfg.stability_scope;
    summary.execution_modes = {queue_outputs.execution_mode};
    if nargin >= 5 && isstruct(error_vs_time) && ~isempty(fieldnames(error_vs_time))
        summary.phase1_mse_mean = pick_numeric(error_vs_time, {'mse_mean'}, NaN);
        summary.phase1_mse_peak = pick_numeric(error_vs_time, {'mse_peak'}, NaN);
        summary.phase1_rmse_mean = pick_numeric(error_vs_time, {'rmse_mean'}, NaN);
        summary.phase1_rmse_peak = pick_numeric(error_vs_time, {'rmse_peak'}, NaN);
        summary.phase1_vorticity_l2_error_mean = pick_numeric(error_vs_time, {'vorticity_vector_relative_l2_mismatch_mean'}, NaN);
        summary.phase1_vorticity_l2_error_peak = pick_numeric(error_vs_time, {'vorticity_vector_relative_l2_mismatch_peak'}, NaN);
        summary.phase1_streamfunction_l2_error_mean = pick_numeric(error_vs_time, {'streamfunction_relative_l2_mismatch_mean'}, NaN);
        summary.phase1_streamfunction_l2_error_peak = pick_numeric(error_vs_time, {'streamfunction_relative_l2_mismatch_peak'}, NaN);
        summary.phase1_speed_l2_error_mean = pick_numeric(error_vs_time, {'speed_relative_l2_mismatch_mean'}, NaN);
        summary.phase1_speed_l2_error_peak = pick_numeric(error_vs_time, {'speed_relative_l2_mismatch_peak'}, NaN);
        summary.phase1_velocity_vector_l2_error_mean = pick_numeric(error_vs_time, {'velocity_vector_relative_l2_mismatch_mean'}, NaN);
        summary.phase1_velocity_vector_l2_error_peak = pick_numeric(error_vs_time, {'velocity_vector_relative_l2_mismatch_peak'}, NaN);
        summary.phase1_streamline_direction_l2_error_mean = pick_numeric(error_vs_time, {'streamline_direction_relative_l2_mismatch_mean'}, NaN);
        summary.phase1_streamline_direction_l2_error_peak = pick_numeric(error_vs_time, {'streamline_direction_relative_l2_mismatch_peak'}, NaN);
    end
    if nargin >= 6 && isstruct(ic_study) && isfield(ic_study, 'cases')
        summary.ic_study_enabled = logical(pick_value(ic_study, 'enabled', false));
        summary.ic_study_case_count = numel(ic_study.cases);
        if ~isempty(ic_study.cases)
            summary.ic_study_case_labels = {ic_study.cases.label};
        else
            summary.ic_study_case_labels = {};
        end
    end
end

function ic_study = build_phase1_ic_study_results(queue_outputs, phase_cfg, fd_mesh, sp_mesh)
    ic_cfg = pick_value(phase_cfg, 'ic_study', struct());
    catalog = resolve_phase1_ic_study_catalog(phase_cfg);
    baseline_meta = phase1_baseline_case_metadata(pick_text(ic_cfg, {'baseline_ic_type'}, 'stretched_gaussian'));
    executed_case_ids = {};
    if isstruct(catalog) && ~isempty(catalog) && isfield(catalog, 'case_id')
        executed_case_ids = cellstr(string({catalog.case_id}));
    end
    ic_study = struct( ...
        'enabled', logical(pick_value(ic_cfg, 'enabled', false)), ...
        'include_arrangement_cases', logical(pick_value(ic_cfg, 'include_arrangement_cases', false)), ...
        'baseline_case_id', baseline_meta.case_id, ...
        'baseline_ic_type', baseline_meta.ic_type, ...
        'baseline_label', pick_text(ic_cfg, {'baseline_label'}, 'Stretched Gaussian'), ...
        'case_catalog', catalog, ...
        'cases', struct([]), ...
        'artifacts', struct(), ...
        'fd_selected_mesh', fd_mesh, ...
        'spectral_selected_mesh', sp_mesh, ...
        'executed_case_ids', {executed_case_ids});
    if ~ic_study.enabled || isempty(catalog)
        return;
    end

    cases = repmat(struct( ...
        'case_id', '', ...
        'label', '', ...
        'group_label', '', ...
        'display_label', '', ...
        'fd', struct(), ...
        'spectral', struct()), 1, numel(catalog));
    for i = 1:numel(catalog)
        fd_output = require_ic_study_output(queue_outputs, 'fd', catalog(i).case_id);
        spectral_output = require_ic_study_output(queue_outputs, 'spectral', catalog(i).case_id);
        cases(i) = build_phase1_ic_study_case_result(catalog(i), fd_output, spectral_output, fd_mesh, sp_mesh);
    end
    ic_study.cases = cases;
end

function output = require_ic_study_output(outputs, method_key, case_id)
    idx = [];
    for i = 1:numel(outputs)
        if ~strcmp(outputs(i).method_key, method_key) || ~strcmp(outputs(i).stage, 'ic_study')
            continue;
        end
        output_case_id = pick_text(outputs(i).run_config, {'phase1_ic_study_case_id'}, ...
            pick_text(outputs(i).parameters, {'phase1_ic_study_case_id'}, ''));
        if strcmpi(output_case_id, char(string(case_id)))
            idx = i;
            break;
        end
    end
    if isempty(idx)
        error('Phase1PeriodicComparison:MissingICStudyOutput', ...
            'Missing Phase 1 IC-study output for %s / %s.', method_key, char(string(case_id)));
    end
    output = outputs(idx);
end

function case_result = build_phase1_ic_study_case_result(case_cfg, fd_output, spectral_output, fd_mesh, sp_mesh)
    case_result = struct( ...
        'case_id', char(string(case_cfg.case_id)), ...
        'label', char(string(case_cfg.label)), ...
        'group_label', char(string(case_cfg.group_label)), ...
        'display_label', phase1_case_display_label(case_cfg.case_id, case_cfg.label), ...
        'fd', build_phase1_ic_study_method_result(fd_output, spectral_output, fd_mesh, case_cfg), ...
        'spectral', build_phase1_ic_study_method_result(spectral_output, fd_output, sp_mesh, case_cfg));
end

function method_result = build_phase1_ic_study_method_result(output, peer_output, selected_mesh, case_cfg)
    runtime_wall_s = pick_numeric(output.results, {'wall_time', 'total_time'}, output.wall_time);
    analysis = require_analysis(output.results, sprintf('%s IC study analysis', char(string(output.method))));
    omega_initial = extract_omega_field(analysis, 'initial');
    omega_final = extract_omega_field(analysis, 'final');
    vortex_initial = vortex_diagnostics(omega_initial, analysis);
    vortex_final = vortex_diagnostics(omega_final, analysis);
    mismatch_metrics = compute_phase1_cross_method_mismatch_metrics(output, peer_output);
    method_metrics = struct( ...
        'runtime_wall_s', runtime_wall_s, ...
        'total_steps', pick_numeric(output.results, {'total_steps'}, NaN), ...
        'selected_mesh_label', mesh_label_from_entry(selected_mesh), ...
        'peak_vorticity_ratio', vortex_final.peak_abs_omega / max(vortex_initial.peak_abs_omega, eps), ...
        'centroid_drift', hypot(vortex_final.centroid_x - vortex_initial.centroid_x, ...
            vortex_final.centroid_y - vortex_initial.centroid_y), ...
        'cross_method_mismatch_l2', mismatch_metrics.cross_method_mismatch_l2, ...
        'cross_method_mismatch_linf', mismatch_metrics.cross_method_mismatch_linf, ...
        'relative_vorticity_error_L2', mismatch_metrics.cross_method_mismatch_l2, ...
        'relative_vorticity_error_Linf', mismatch_metrics.cross_method_mismatch_linf, ...
        'cross_method_streamfunction_relative_l2_mismatch', mismatch_metrics.cross_method_streamfunction_relative_l2_mismatch, ...
        'cross_method_speed_relative_l2_mismatch', mismatch_metrics.cross_method_speed_relative_l2_mismatch, ...
        'cross_method_velocity_vector_relative_l2_mismatch', mismatch_metrics.cross_method_velocity_vector_relative_l2_mismatch, ...
        'cross_method_streamline_direction_relative_l2_mismatch', mismatch_metrics.cross_method_streamline_direction_relative_l2_mismatch, ...
        'cross_method_mse', mismatch_metrics.cross_method_mse, ...
        'cross_method_rmse', mismatch_metrics.cross_method_rmse, ...
        'nan_inf_flag', logical(mismatch_metrics.nan_inf_flag), ...
        'kinetic_energy_drift', history_drift(analysis, 'kinetic_energy'), ...
        'enstrophy_drift', history_drift(analysis, 'enstrophy'), ...
        'circulation_drift', conservation_drift(analysis, omega_initial, omega_final), ...
        'final_energy', pick_numeric(output.results, {'final_energy'}, NaN), ...
        'final_enstrophy', pick_numeric(output.results, {'final_enstrophy'}, NaN), ...
        'final_circulation', last_finite_local(pick_value(analysis, 'circulation', [])));
    pseudo_metrics = struct( ...
        'runtime_wall_s', runtime_wall_s, ...
        'peak_vorticity_ratio', method_metrics.peak_vorticity_ratio, ...
        'total_steps', method_metrics.total_steps);
    view_summary = build_phase_child_view_summary(output, pseudo_metrics);
    view_summary.metadata.ic_study_case_id = char(string(case_cfg.case_id));
    view_summary.metadata.ic_study_case_label = char(string(case_cfg.label));
    view_summary.metadata.ic_study_group_label = char(string(case_cfg.group_label));
    view_summary.metadata.selected_mesh_label = mesh_label_from_entry(selected_mesh);
    view_summary.metadata.workflow_stage = 'ic_study';
    method_result = struct( ...
        'method', output.method, ...
        'method_key', output.method_key, ...
        'case_id', char(string(case_cfg.case_id)), ...
        'label', char(string(case_cfg.label)), ...
        'group_label', char(string(case_cfg.group_label)), ...
        'selected_mesh', selected_mesh, ...
        'runtime_wall_s', runtime_wall_s, ...
        'run_id', pick_text(output.run_config, {'run_id', 'study_id'}, ''), ...
        'artifact_root', pick_text(output.paths, {'base'}, ''), ...
        'stage_id', phase1_ic_study_stage_id(output.method_key, case_cfg.case_id), ...
        'visual_prefix', phase1_ic_study_visual_prefix(output.method, case_cfg.case_id), ...
        'display_label', phase1_case_display_label(case_cfg.case_id, case_cfg.label), ...
        'metrics', method_metrics, ...
        'output', strip_heavy_outputs(output), ...
        'view_summary', view_summary);
end

function stage_id = phase1_ic_study_stage_id(method_key, case_id)
    stage_id = sprintf('%s_ic_%s', normalize_method_key(method_key), char(string(case_id)));
end

function prefix = phase1_ic_study_visual_prefix(method_name, case_id)
    method_prefix = phase1_publication_method_label(method_name);
    prefix = sprintf('%s_%s', method_prefix, phase1_case_prefix_token(case_id));
end

function token = phase1_case_prefix_token(case_id)
    folder_name = phase1_case_folder_name(case_id);
    token = strrep(folder_name, '_', '');
end

function conv = summarize_convergence(results)
    conv = struct('overall_verdict', '', 'primary_refinement_axis', '', ...
        'primary_observed_rate', NaN, 'temporal_observed_rate', NaN, ...
        'primary_verdict', '', 'primary_final_relative_change', NaN, ...
        'temporal_verdict', '', 'temporal_final_relative_change', NaN, ...
        'tolerance', pick_numeric(results, {'tolerance'}, NaN));
    if isfield(results, 'summary') && isstruct(results.summary)
        conv.overall_verdict = pick_text(results.summary, {'overall_verdict'}, '');
        conv.primary_refinement_axis = pick_text(results.summary, {'primary_refinement_axis'}, '');
    end
    if isfield(results, 'stage_summaries') && ~isempty(results.stage_summaries)
        for i = 1:numel(results.stage_summaries)
            stage = results.stage_summaries(i);
            rate = pick_numeric(stage, {'observed_order_last'}, NaN);
            if strcmpi(pick_text(stage, {'stage_name'}, ''), 'temporal')
                conv.temporal_observed_rate = rate;
                conv.temporal_verdict = pick_text(stage, {'verdict'}, conv.temporal_verdict);
                conv.temporal_final_relative_change = pick_numeric(stage, {'final_relative_change'}, conv.temporal_final_relative_change);
            else
                conv.primary_observed_rate = rate;
                conv.primary_verdict = pick_text(stage, {'verdict'}, conv.primary_verdict);
                conv.primary_final_relative_change = pick_numeric(stage, {'final_relative_change'}, conv.primary_final_relative_change);
            end
        end
    end
    if strlength(string(strtrim(conv.primary_verdict))) == 0
        conv.primary_verdict = conv.overall_verdict;
    end
end

function analysis = require_analysis(results, label)
    if ~isfield(results, 'analysis') || ~isstruct(results.analysis)
        error('Phase1PeriodicComparison:MissingAnalysis', ...
            '%s run did not return Results.analysis.', char(string(label)));
    end
    analysis = results.analysis;
end

function omega = extract_omega_field(analysis, which_field)
    if isfield(analysis, 'omega_snaps') && ~isempty(analysis.omega_snaps)
        if strcmp(which_field, 'initial')
            omega = double(analysis.omega_snaps(:, :, 1));
        else
            omega = double(analysis.omega_snaps(:, :, end));
        end
    elseif isfield(analysis, 'omega') && ~isempty(analysis.omega)
        omega = double(analysis.omega);
    else
        error('Phase1PeriodicComparison:MissingVorticityField', ...
            'Analysis is missing omega snapshots.');
    end
end

function cube = extract_omega_snapshot_cube(analysis)
    if isfield(analysis, 'omega_snaps') && ~isempty(analysis.omega_snaps)
        cube = double(analysis.omega_snaps);
        if ndims(cube) == 2
            cube = reshape(cube, size(cube, 1), size(cube, 2), 1);
        end
        return;
    end
    if isfield(analysis, 'omega') && ~isempty(analysis.omega)
        omega = double(analysis.omega);
        cube = reshape(omega, size(omega, 1), size(omega, 2), 1);
        return;
    end
    error('Phase1PeriodicComparison:MissingVorticitySnapshots', ...
        'Analysis is missing vorticity snapshots for RMSE comparison.');
end

function error_vs_time = compute_phase1_error_vs_time(fd_output, spectral_output)
    fd_analysis = require_analysis(fd_output.results, 'FD error analysis');
    spectral_analysis = require_analysis(spectral_output.results, 'Spectral error analysis');
    fd_cube = extract_omega_snapshot_cube(fd_analysis);
    spectral_cube = extract_omega_snapshot_cube(spectral_analysis);
    fd_times = resolve_snapshot_time_vector(fd_analysis, size(fd_cube, 3));
    spectral_times = resolve_snapshot_time_vector(spectral_analysis, size(spectral_cube, 3));
    fd_u_cube = extract_optional_snapshot_cube(fd_analysis, 'u');
    fd_v_cube = extract_optional_snapshot_cube(fd_analysis, 'v');
    spectral_u_cube = extract_optional_snapshot_cube(spectral_analysis, 'u');
    spectral_v_cube = extract_optional_snapshot_cube(spectral_analysis, 'v');

    overlap_start = max(fd_times(1), spectral_times(1));
    overlap_end = min(fd_times(end), spectral_times(end));
    if ~(isfinite(overlap_start) && isfinite(overlap_end) && overlap_end >= overlap_start)
        error('Phase1PeriodicComparison:NoErrorTimeOverlap', ...
            'FD and Spectral runs do not share an overlapping snapshot time range.');
    end
    common_times = unique([ ...
        fd_times(fd_times >= overlap_start & fd_times <= overlap_end); ...
        spectral_times(spectral_times >= overlap_start & spectral_times <= overlap_end)]);
    common_times = common_times(isfinite(common_times));
    if isempty(common_times)
        common_times = linspace(overlap_start, overlap_end, max([numel(fd_times), numel(spectral_times), 2])).';
    elseif numel(common_times) == 1 && overlap_end > overlap_start
        common_times = linspace(overlap_start, overlap_end, max([numel(fd_times), numel(spectral_times), 2])).';
    else
        common_times = common_times(:);
    end

    fd_abs_rmse = nan(numel(common_times), 1);
    fd_rel_rmse = nan(numel(common_times), 1);
    spectral_abs_rmse = nan(numel(common_times), 1);
    spectral_rel_rmse = nan(numel(common_times), 1);
    fd_rel_l2 = nan(numel(common_times), 1);
    spectral_rel_l2 = nan(numel(common_times), 1);
    fd_rel_linf = nan(numel(common_times), 1);
    spectral_rel_linf = nan(numel(common_times), 1);
    mse_series = nan(numel(common_times), 1);
    rmse_series = nan(numel(common_times), 1);
    vorticity_l2_series = nan(numel(common_times), 1);
    streamfunction_l2_series = nan(numel(common_times), 1);
    speed_l2_series = nan(numel(common_times), 1);
    velocity_vector_l2_series = nan(numel(common_times), 1);
    streamline_direction_l2_series = nan(numel(common_times), 1);
    peak_vorticity_series = nan(numel(common_times), 1);
    circulation_series = nan(numel(common_times), 1);
    kinetic_energy_series = nan(numel(common_times), 1);
    enstrophy_series = nan(numel(common_times), 1);
    for i = 1:numel(common_times)
        t_query = common_times(i);
        fd_slice = double(interpolate_snapshot_cube_in_time(fd_cube, fd_times, t_query));
        spectral_slice = double(interpolate_snapshot_cube_in_time(spectral_cube, spectral_times, t_query));
        fd_state = resolve_phase1_comparison_state(fd_analysis, fd_slice, t_query);
        spectral_state = resolve_phase1_comparison_state(spectral_analysis, spectral_slice, t_query);
        spectral_on_fd = interpolate_field_to_analysis(spectral_slice, spectral_analysis, fd_analysis, size(fd_slice));
        fd_on_spectral = interpolate_field_to_analysis(fd_slice, fd_analysis, spectral_analysis, size(spectral_slice));
        spectral_on_fd_state = remap_phase1_comparison_state(spectral_state, spectral_analysis, fd_analysis, size(fd_slice));
        fd_on_spectral_state = remap_phase1_comparison_state(fd_state, fd_analysis, spectral_analysis, size(spectral_slice));
        fd_snapshot_mismatch = compute_phase1_snapshot_mismatch(fd_state, spectral_on_fd_state);
        spectral_snapshot_mismatch = compute_phase1_snapshot_mismatch(spectral_state, fd_on_spectral_state);

        fd_diff = fd_slice - spectral_on_fd;
        spectral_diff = spectral_slice - fd_on_spectral;

        fd_abs_rmse(i) = field_abs_rmse(fd_diff);
        spectral_abs_rmse(i) = field_abs_rmse(spectral_diff);
        fd_rel_rmse(i) = safe_ratio(fd_abs_rmse(i), field_rms(spectral_on_fd));
        spectral_rel_rmse(i) = safe_ratio(spectral_abs_rmse(i), field_rms(fd_on_spectral));
        fd_rel_l2(i) = field_relative_l2(fd_diff, spectral_on_fd);
        spectral_rel_l2(i) = field_relative_l2(spectral_diff, fd_on_spectral);
        fd_rel_linf(i) = field_relative_linf(fd_diff, spectral_on_fd);
        spectral_rel_linf(i) = field_relative_linf(spectral_diff, fd_on_spectral);

        mse_series(i) = mean([fd_abs_rmse(i).^2, spectral_abs_rmse(i).^2], 'omitnan');
        rmse_series(i) = sqrt(mse_series(i));
        vorticity_l2_series(i) = mean([fd_snapshot_mismatch.vorticity_relative_l2, spectral_snapshot_mismatch.vorticity_relative_l2], 'omitnan');
        streamfunction_l2_series(i) = mean([fd_snapshot_mismatch.streamfunction_relative_l2, spectral_snapshot_mismatch.streamfunction_relative_l2], 'omitnan');
        speed_l2_series(i) = mean([fd_snapshot_mismatch.speed_relative_l2, spectral_snapshot_mismatch.speed_relative_l2], 'omitnan');
        velocity_vector_l2_series(i) = mean([fd_snapshot_mismatch.velocity_vector_relative_l2, spectral_snapshot_mismatch.velocity_vector_relative_l2], 'omitnan');
        streamline_direction_l2_series(i) = mean([fd_snapshot_mismatch.streamline_direction_relative_l2, spectral_snapshot_mismatch.streamline_direction_relative_l2], 'omitnan');
        peak_vorticity_series(i) = symmetric_relative_difference( ...
            snapshot_peak_vorticity(fd_slice), snapshot_peak_vorticity(spectral_slice));
        circulation_series(i) = symmetric_relative_difference( ...
            snapshot_circulation(fd_slice, fd_analysis), ...
            snapshot_circulation(spectral_slice, spectral_analysis));
        kinetic_energy_series(i) = symmetric_relative_difference( ...
            snapshot_kinetic_energy(fd_analysis, fd_u_cube, fd_v_cube, t_query, fd_slice), ...
            snapshot_kinetic_energy(spectral_analysis, spectral_u_cube, spectral_v_cube, t_query, spectral_slice));
        enstrophy_series(i) = symmetric_relative_difference( ...
            snapshot_enstrophy(fd_slice, fd_analysis), ...
            snapshot_enstrophy(spectral_slice, spectral_analysis));
    end

    abs_rmse = mean([fd_abs_rmse, spectral_abs_rmse], 2, 'omitnan');
    rel_rmse = mean([fd_rel_rmse, spectral_rel_rmse], 2, 'omitnan');
    rel_l2 = mean([fd_rel_l2, spectral_rel_l2], 2, 'omitnan');
    rel_linf = mean([fd_rel_linf, spectral_rel_linf], 2, 'omitnan');

    error_vs_time = struct();
    error_vs_time.time_s = common_times(:).';
    error_vs_time.fd_absolute_rmse = fd_abs_rmse(:).';
    error_vs_time.fd_relative_rmse = fd_rel_rmse(:).';
    error_vs_time.spectral_absolute_rmse = spectral_abs_rmse(:).';
    error_vs_time.spectral_relative_rmse = spectral_rel_rmse(:).';
    error_vs_time.fd_relative_l2_mismatch = fd_rel_l2(:).';
    error_vs_time.spectral_relative_l2_mismatch = spectral_rel_l2(:).';
    error_vs_time.fd_relative_linf_mismatch = fd_rel_linf(:).';
    error_vs_time.spectral_relative_linf_mismatch = spectral_rel_linf(:).';
    error_vs_time.absolute_rmse = abs_rmse(:).';
    error_vs_time.relative_rmse = rel_rmse(:).';
    error_vs_time.relative_l2_mismatch = rel_l2(:).';
    error_vs_time.relative_linf_mismatch = rel_linf(:).';
    error_vs_time.mse = mse_series(:).';
    error_vs_time.rmse = rmse_series(:).';
    error_vs_time.vorticity_vector_relative_l2_mismatch = vorticity_l2_series(:).';
    error_vs_time.streamfunction_relative_l2_mismatch = streamfunction_l2_series(:).';
    error_vs_time.speed_relative_l2_mismatch = speed_l2_series(:).';
    error_vs_time.velocity_vector_relative_l2_mismatch = velocity_vector_l2_series(:).';
    error_vs_time.streamline_direction_relative_l2_mismatch = streamline_direction_l2_series(:).';
    error_vs_time.peak_vorticity_relative_error = peak_vorticity_series(:).';
    error_vs_time.circulation_relative_error = circulation_series(:).';
    error_vs_time.kinetic_energy_relative_error = kinetic_energy_series(:).';
    error_vs_time.enstrophy_relative_error = enstrophy_series(:).';
    error_vs_time.mse_mean = finite_series_mean(mse_series);
    error_vs_time.mse_peak = finite_series_peak(mse_series);
    error_vs_time.rmse_mean = finite_series_mean(rmse_series);
    error_vs_time.rmse_peak = finite_series_peak(rmse_series);
    error_vs_time.vorticity_vector_relative_l2_mismatch_mean = finite_series_mean(vorticity_l2_series);
    error_vs_time.vorticity_vector_relative_l2_mismatch_peak = finite_series_peak(vorticity_l2_series);
    error_vs_time.streamfunction_relative_l2_mismatch_mean = finite_series_mean(streamfunction_l2_series);
    error_vs_time.streamfunction_relative_l2_mismatch_peak = finite_series_peak(streamfunction_l2_series);
    error_vs_time.speed_relative_l2_mismatch_mean = finite_series_mean(speed_l2_series);
    error_vs_time.speed_relative_l2_mismatch_peak = finite_series_peak(speed_l2_series);
    error_vs_time.velocity_vector_relative_l2_mismatch_mean = finite_series_mean(velocity_vector_l2_series);
    error_vs_time.velocity_vector_relative_l2_mismatch_peak = finite_series_peak(velocity_vector_l2_series);
    error_vs_time.streamline_direction_relative_l2_mismatch_mean = finite_series_mean(streamline_direction_l2_series);
    error_vs_time.streamline_direction_relative_l2_mismatch_peak = finite_series_peak(streamline_direction_l2_series);
    error_vs_time.peak_vorticity_relative_error_mean = finite_series_mean(peak_vorticity_series);
    error_vs_time.peak_vorticity_relative_error_peak = finite_series_peak(peak_vorticity_series);
    error_vs_time.circulation_relative_error_mean = finite_series_mean(circulation_series);
    error_vs_time.circulation_relative_error_peak = finite_series_peak(circulation_series);
    error_vs_time.kinetic_energy_relative_error_mean = finite_series_mean(kinetic_energy_series);
    error_vs_time.kinetic_energy_relative_error_peak = finite_series_peak(kinetic_energy_series);
    error_vs_time.enstrophy_relative_error_mean = finite_series_mean(enstrophy_series);
    error_vs_time.enstrophy_relative_error_peak = finite_series_peak(enstrophy_series);
    error_vs_time.abs_rmse_mean = finite_series_mean(abs_rmse);
    error_vs_time.abs_rmse_peak = finite_series_peak(abs_rmse);
    error_vs_time.rel_rmse_mean = finite_series_mean(rel_rmse);
    error_vs_time.rel_rmse_peak = finite_series_peak(rel_rmse);
    error_vs_time.rel_l2_mean = finite_series_mean(rel_l2);
    error_vs_time.rel_l2_peak = finite_series_peak(rel_l2);
    error_vs_time.rel_linf_mean = finite_series_mean(rel_linf);
    error_vs_time.rel_linf_peak = finite_series_peak(rel_linf);
    error_vs_time.primary_metric = 'vorticity_vector_relative_l2_mismatch';
    error_vs_time.metric_basis = 'cross_method_snapshot_alignment';
    error_vs_time.comparison_grid_method = 'directional_method_grid';
    error_vs_time.comparison_grid_label = 'Directional native-grid remap (FD <-> SM)';
    error_vs_time.fd_mesh_label = sprintf('%dx%d', size(fd_cube, 2), size(fd_cube, 1));
    error_vs_time.spectral_mesh_label = sprintf('%dx%d', size(spectral_cube, 2), size(spectral_cube, 1));
    error_vs_time.fd_reference_label = 'Spectral remapped onto FD grid';
    error_vs_time.spectral_reference_label = 'FD remapped onto Spectral grid';
    error_vs_time.spatial_interpolation = 'linear_then_nearest_zero';
    error_vs_time.temporal_interpolation = 'piecewise_linear';
    error_vs_time.overlap_time_window_s = [overlap_start, overlap_end];
    error_vs_time.fd_snapshot_count = size(fd_cube, 3);
    error_vs_time.spectral_snapshot_count = size(spectral_cube, 3);
    error_vs_time.compared_methods = {'FD', 'Spectral'};
    error_vs_time.metric_labels = struct( ...
        'mse', 'MSE', ...
        'rmse', 'RMSE', ...
        'vorticity_vector_relative_l2_mismatch', 'Vorticity vector relative L2 mismatch', ...
        'streamfunction_relative_l2_mismatch', 'Streamfunction relative L2 mismatch', ...
        'speed_relative_l2_mismatch', 'Speed relative L2 mismatch', ...
        'velocity_vector_relative_l2_mismatch', 'Velocity vector relative L2 mismatch', ...
        'streamline_direction_relative_l2_mismatch', 'Streamline direction relative L2 mismatch', ...
        'peak_vorticity_relative_error', 'Peak vorticity relative error', ...
        'circulation_relative_error', 'Circulation relative error', ...
        'kinetic_energy_relative_error', 'Kinetic energy relative error', ...
        'enstrophy_relative_error', 'Enstrophy relative error');
end

function rmse_vs_time = compute_phase1_rmse_vs_time(fd_output, spectral_output)
    rmse_vs_time = compute_phase1_error_vs_time(fd_output, spectral_output);
end

function cube = extract_optional_snapshot_cube(analysis, field_name)
    cube = [];
    if ~isstruct(analysis) || nargin < 2
        return;
    end
    field_name = char(string(field_name));
    snap_name = sprintf('%s_snaps', field_name);
    if isfield(analysis, snap_name) && ~isempty(analysis.(snap_name))
        cube = double(analysis.(snap_name));
    elseif isfield(analysis, field_name) && ~isempty(analysis.(field_name))
        raw_value = double(analysis.(field_name));
        if ndims(raw_value) >= 3
            cube = raw_value;
        elseif ndims(raw_value) == 2
            cube = reshape(raw_value, size(raw_value, 1), size(raw_value, 2), 1);
        end
    end
end

function value = snapshot_peak_vorticity(slice)
    if isempty(slice)
        value = NaN;
        return;
    end
    value = max(abs(double(slice(:))), [], 'omitnan');
end

function value = snapshot_circulation(slice, analysis)
    value = field_integral(slice, analysis);
end

function value = snapshot_enstrophy(slice, analysis)
    area = analysis_cell_area(analysis, size(slice));
    if ~isfinite(area)
        value = NaN;
        return;
    end
    vec = double(slice(:));
    vec = vec(isfinite(vec));
    if isempty(vec)
        value = NaN;
        return;
    end
    value = 0.5 * area * sum(vec .^ 2, 'omitnan');
end

function value = snapshot_kinetic_energy(analysis, u_cube, v_cube, t_query, omega_slice)
    value = interpolate_history_value(analysis, 'kinetic_energy', t_query);
    if isfinite(value)
        return;
    end
    if isempty(u_cube) || isempty(v_cube)
        value = NaN;
        return;
    end
    u_slice = interpolate_snapshot_cube_in_time(u_cube, resolve_snapshot_time_vector(analysis, size(u_cube, 3)), t_query);
    v_slice = interpolate_snapshot_cube_in_time(v_cube, resolve_snapshot_time_vector(analysis, size(v_cube, 3)), t_query);
    area = analysis_cell_area(analysis, size(omega_slice));
    if ~isfinite(area)
        value = NaN;
        return;
    end
    speed_sq = double(u_slice).^2 + double(v_slice).^2;
    speed_sq = speed_sq(isfinite(speed_sq));
    if isempty(speed_sq)
        value = NaN;
        return;
    end
    value = 0.5 * area * sum(speed_sq, 'omitnan');
end

function value = interpolate_history_value(analysis, field_name, t_query)
    value = NaN;
    if ~isstruct(analysis) || ~isfield(analysis, field_name) || ~isnumeric(analysis.(field_name))
        return;
    end
    series = double(analysis.(field_name)(:));
    if isempty(series)
        return;
    end
    if ~isfield(analysis, 'time_vec') || ~isnumeric(analysis.time_vec)
        if numel(series) == 1
            value = series(1);
        end
        return;
    end
    time_vec = double(analysis.time_vec(:));
    n = min(numel(series), numel(time_vec));
    if n < 1
        return;
    end
    series = series(1:n);
    time_vec = time_vec(1:n);
    valid = isfinite(time_vec) & isfinite(series);
    if nnz(valid) < 1
        return;
    end
    time_vec = time_vec(valid);
    series = series(valid);
    if numel(series) == 1
        value = series(1);
        return;
    end
    value = interp1(time_vec, series, t_query, 'linear', 'extrap');
end

function area = analysis_cell_area(analysis, field_size)
    area = NaN;
    if nargin < 2 || numel(field_size) < 2
        return;
    end
    [~, ~, dx, dy] = grid_spacing_from_analysis(analysis, field_size);
    if isfinite(dx) && isfinite(dy)
        area = abs(dx * dy);
    end
end

function value = field_integral(field, analysis)
    area = analysis_cell_area(analysis, size(field));
    if ~isfinite(area)
        value = NaN;
        return;
    end
    vec = double(field(:));
    vec = vec(isfinite(vec));
    if isempty(vec)
        value = NaN;
        return;
    end
    value = area * sum(vec, 'omitnan');
end

function value = symmetric_relative_difference(a, b)
    value = NaN;
    if ~(isfinite(a) && isfinite(b))
        return;
    end
    scale = max([abs(a), abs(b), 1.0e-12]);
    value = abs(a - b) / scale;
end

function times = resolve_snapshot_time_vector(analysis, n_frames)
    times = [];
    if isfield(analysis, 'snapshot_times_requested') && ~isempty(analysis.snapshot_times_requested) && numel(analysis.snapshot_times_requested) == n_frames
        times = double(analysis.snapshot_times_requested(:));
    elseif isfield(analysis, 'snapshot_times') && ~isempty(analysis.snapshot_times) && numel(analysis.snapshot_times) == n_frames
        times = double(analysis.snapshot_times(:));
    elseif isfield(analysis, 'time_vec') && ~isempty(analysis.time_vec) && numel(analysis.time_vec) == n_frames
        times = double(analysis.time_vec(:));
    end
    if isempty(times)
        tfinal = pick_numeric(analysis, {'Tfinal'}, max(n_frames - 1, 1));
        times = linspace(0, tfinal, n_frames).';
    end
    times = double(times(:));
    if numel(times) > 1 && any(diff(times) < 0)
        times = linspace(times(1), times(end), n_frames).';
    end
end

function slice = interpolate_snapshot_cube_in_time(cube, times, t_query)
    cube = double(cube);
    if size(cube, 3) <= 1 || numel(times) <= 1
        slice = cube(:, :, 1);
        return;
    end
    times = double(times(:));
    if t_query <= times(1)
        slice = cube(:, :, 1);
        return;
    end
    if t_query >= times(end)
        slice = cube(:, :, end);
        return;
    end
    lower_idx = find(times <= t_query, 1, 'last');
    upper_idx = find(times >= t_query, 1, 'first');
    if isempty(lower_idx), lower_idx = 1; end
    if isempty(upper_idx), upper_idx = size(cube, 3); end
    if lower_idx == upper_idx || abs(times(upper_idx) - times(lower_idx)) <= eps(max(abs(times([lower_idx, upper_idx]))))
        slice = cube(:, :, lower_idx);
        return;
    end
    alpha = (t_query - times(lower_idx)) / (times(upper_idx) - times(lower_idx));
    slice = (1 - alpha) * cube(:, :, lower_idx) + alpha * cube(:, :, upper_idx);
end

function omega_peer = interpolate_peer_to_method(peer_analysis, method_analysis)
    omega_peer_final = extract_omega_field(peer_analysis, 'final');
    method_final = extract_omega_field(method_analysis, 'final');
    omega_peer = interpolate_field_to_analysis(omega_peer_final, peer_analysis, method_analysis, size(method_final));
end

function remapped_field = interpolate_field_to_analysis(source_field, source_analysis, target_analysis, target_size)
    [Xr, Yr] = analysis_grid(source_analysis, size(source_field));
    [Xm, Ym] = analysis_grid(target_analysis, target_size);
    remapped_field = interp2(Xr, Yr, double(source_field), Xm, Ym, 'linear', NaN);
    if any(~isfinite(remapped_field(:)))
        remapped_field = interp2(Xr, Yr, double(source_field), Xm, Ym, 'nearest', 0);
    end
end

function state = resolve_phase1_comparison_state(analysis, omega_slice, t_query)
    omega_slice = double(omega_slice);
    [X, Y] = analysis_grid(analysis, size(omega_slice));
    x_vec = X(1, :);
    y_vec = Y(:, 1);
    psi_slice = extract_optional_snapshot_slice(analysis, 'psi', t_query, size(omega_slice));
    u_slice = extract_optional_snapshot_slice(analysis, 'u', t_query, size(omega_slice));
    v_slice = extract_optional_snapshot_slice(analysis, 'v', t_query, size(omega_slice));
    [psi_reconstructed, u_reconstructed, v_reconstructed] = velocity_from_omega_slice_local(omega_slice, x_vec, y_vec);
    if isempty(psi_slice)
        psi_slice = psi_reconstructed;
    end
    if isempty(u_slice) || isempty(v_slice)
        u_slice = u_reconstructed;
        v_slice = v_reconstructed;
    end
    psi_slice = center_streamfunction_field(psi_slice);
    psi_slice(~isfinite(psi_slice)) = 0;
    u_slice(~isfinite(u_slice)) = 0;
    v_slice(~isfinite(v_slice)) = 0;
    state = struct( ...
        'omega', omega_slice, ...
        'psi', psi_slice, ...
        'u', double(u_slice), ...
        'v', double(v_slice), ...
        'speed', hypot(double(u_slice), double(v_slice)));
end

function slice = extract_optional_snapshot_slice(analysis, field_name, t_query, target_size)
    slice = [];
    cube = extract_optional_snapshot_cube(analysis, field_name);
    if isempty(cube)
        return;
    end
    if nargin >= 4 && ~isempty(target_size)
        if size(cube, 1) ~= target_size(1) || size(cube, 2) ~= target_size(2)
            return;
        end
    end
    if nargin >= 3 && isfinite(double(t_query))
        times = resolve_snapshot_time_vector(analysis, size(cube, 3));
        slice = interpolate_snapshot_cube_in_time(cube, times, t_query);
    else
        slice = cube(:, :, end);
    end
end

function remapped_state = remap_phase1_comparison_state(source_state, source_analysis, target_analysis, target_size)
    remapped_state = struct();
    remapped_state.omega = interpolate_field_to_analysis(source_state.omega, source_analysis, target_analysis, target_size);
    remapped_state.psi = center_streamfunction_field(interpolate_field_to_analysis(source_state.psi, source_analysis, target_analysis, target_size));
    remapped_state.u = interpolate_field_to_analysis(source_state.u, source_analysis, target_analysis, target_size);
    remapped_state.v = interpolate_field_to_analysis(source_state.v, source_analysis, target_analysis, target_size);
    remapped_state.u(~isfinite(remapped_state.u)) = 0;
    remapped_state.v(~isfinite(remapped_state.v)) = 0;
    remapped_state.speed = hypot(remapped_state.u, remapped_state.v);
end

function mismatch = compute_phase1_snapshot_mismatch(method_state, peer_state)
    diff_omega = method_state.omega - peer_state.omega;
    diff_psi = center_streamfunction_field(method_state.psi) - center_streamfunction_field(peer_state.psi);
    diff_speed = method_state.speed - peer_state.speed;
    diff_u = method_state.u - peer_state.u;
    diff_v = method_state.v - peer_state.v;
    mismatch = struct();
    mismatch.vorticity_relative_l2 = field_relative_l2(diff_omega, peer_state.omega);
    mismatch.streamfunction_relative_l2 = field_relative_l2(diff_psi, center_streamfunction_field(peer_state.psi));
    mismatch.speed_relative_l2 = field_relative_l2(diff_speed, peer_state.speed);
    mismatch.velocity_vector_relative_l2 = vector_field_relative_l2(diff_u, diff_v, peer_state.u, peer_state.v);
    mismatch.streamline_direction_relative_l2 = streamline_direction_relative_l2(method_state.u, method_state.v, peer_state.u, peer_state.v);
    mismatch.nan_inf_flag = any(~isfinite(diff_omega(:))) || any(~isfinite(diff_psi(:))) || ...
        any(~isfinite(diff_u(:))) || any(~isfinite(diff_v(:)));
end

function value = vector_field_relative_l2(diff_u, diff_v, ref_u, ref_v)
    value = NaN;
    diff_u = double(diff_u(:));
    diff_v = double(diff_v(:));
    ref_u = double(ref_u(:));
    ref_v = double(ref_v(:));
    valid = isfinite(diff_u) & isfinite(diff_v) & isfinite(ref_u) & isfinite(ref_v);
    if ~any(valid)
        return;
    end
    diff_norm = sqrt(sum(diff_u(valid) .^ 2 + diff_v(valid) .^ 2, 'omitnan'));
    ref_norm = sqrt(sum(ref_u(valid) .^ 2 + ref_v(valid) .^ 2, 'omitnan'));
    value = safe_ratio(diff_norm, ref_norm);
end

function value = streamline_direction_relative_l2(u_method, v_method, u_peer, v_peer)
    [dir_method_u, dir_method_v, valid_method] = normalize_velocity_direction(u_method, v_method);
    [dir_peer_u, dir_peer_v, valid_peer] = normalize_velocity_direction(u_peer, v_peer);
    valid = valid_method & valid_peer;
    if ~any(valid(:))
        value = NaN;
        return;
    end
    diff_u = dir_method_u(valid) - dir_peer_u(valid);
    diff_v = dir_method_v(valid) - dir_peer_v(valid);
    ref_u = dir_peer_u(valid);
    ref_v = dir_peer_v(valid);
    value = vector_field_relative_l2(diff_u, diff_v, ref_u, ref_v);
end

function [dir_u, dir_v, valid] = normalize_velocity_direction(u_slice, v_slice)
    u_slice = double(u_slice);
    v_slice = double(v_slice);
    speed = hypot(u_slice, v_slice);
    speed_peak = max(speed(:), [], 'omitnan');
    if ~isfinite(speed_peak)
        speed_peak = 1.0e-12;
    end
    threshold = max(speed_peak, 1.0e-12) * 1.0e-6;
    valid = isfinite(speed) & speed > threshold;
    dir_u = zeros(size(u_slice));
    dir_v = zeros(size(v_slice));
    dir_u(valid) = u_slice(valid) ./ speed(valid);
    dir_v(valid) = v_slice(valid) ./ speed(valid);
end

function centered = center_streamfunction_field(field)
    centered = double(field);
    finite_values = centered(isfinite(centered));
    if isempty(finite_values)
        centered(:) = 0;
        return;
    end
    centered = centered - mean(finite_values, 'omitnan');
end

function [psi, u, v] = velocity_from_omega_slice_local(omega_slice, x_vec, y_vec)
    omega_slice = double(omega_slice);
    omega_slice(~isfinite(omega_slice)) = 0;
    ny = size(omega_slice, 1);
    nx = size(omega_slice, 2);
    dx = max(mean(diff(double(x_vec))), eps);
    dy = max(mean(diff(double(y_vec))), eps);
    omega_zero_mean = omega_slice - mean(omega_slice(:), 'omitnan');
    omega_hat = fft2(omega_zero_mean);
    kx = (2 * pi / (nx * dx)) * [0:floor(nx / 2), -floor((nx - 1) / 2):-1];
    ky = (2 * pi / (ny * dy)) * [0:floor(ny / 2), -floor((ny - 1) / 2):-1];
    [KX, KY] = meshgrid(kx, ky);
    k2 = KX .^ 2 + KY .^ 2;
    psi_hat = zeros(size(omega_hat));
    active_modes = k2 > 0;
    psi_hat(active_modes) = -omega_hat(active_modes) ./ k2(active_modes);
    psi = real(ifft2(psi_hat));
    u = -real(ifft2(1i * KY .* psi_hat));
    v = real(ifft2(1i * KX .* psi_hat));
end

function value = field_abs_rmse(diff_field)
    diff_vec = double(diff_field(:));
    diff_vec = diff_vec(isfinite(diff_vec));
    if isempty(diff_vec)
        value = NaN;
        return;
    end
    value = sqrt(mean(diff_vec .^ 2, 'omitnan'));
end

function value = field_rms(field)
    field_vec = double(field(:));
    field_vec = field_vec(isfinite(field_vec));
    if isempty(field_vec)
        value = NaN;
        return;
    end
    value = sqrt(mean(field_vec .^ 2, 'omitnan'));
end

function value = field_relative_l2(diff_field, reference_field)
    diff_vec = double(diff_field(:));
    ref_vec = double(reference_field(:));
    valid = isfinite(diff_vec) & isfinite(ref_vec);
    if ~any(valid)
        value = NaN;
        return;
    end
    value = safe_ratio(norm(diff_vec(valid)), norm(ref_vec(valid)));
end

function value = field_relative_linf(diff_field, reference_field)
    diff_vec = double(diff_field(:));
    ref_vec = double(reference_field(:));
    valid = isfinite(diff_vec) & isfinite(ref_vec);
    if ~any(valid)
        value = NaN;
        return;
    end
    value = safe_ratio(max(abs(diff_vec(valid))), max(abs(ref_vec(valid))));
end

function value = safe_ratio(numerator, denominator)
    value = NaN;
    if ~(isfinite(numerator) && isfinite(denominator))
        return;
    end
    value = numerator / max(denominator, 1.0e-12);
end

function value = finite_series_mean(values)
    finite_values = double(values(isfinite(values)));
    if isempty(finite_values)
        value = NaN;
        return;
    end
    value = mean(finite_values, 'omitnan');
end

function value = finite_series_peak(values)
    finite_values = double(values(isfinite(values)));
    if isempty(finite_values)
        value = NaN;
        return;
    end
    value = max(finite_values);
end

function [X, Y] = analysis_grid(analysis, field_size)
    ny = field_size(1);
    nx = field_size(2);
    if isfield(analysis, 'x') && numel(analysis.x) == nx
        x = double(analysis.x(:)).';
    else
        Lx = pick_numeric(analysis, {'Lx'}, nx);
        x = linspace(-Lx / 2, Lx / 2, nx);
    end
    if isfield(analysis, 'y') && numel(analysis.y) == ny
        y = double(analysis.y(:));
    else
        Ly = pick_numeric(analysis, {'Ly'}, ny);
        y = linspace(-Ly / 2, Ly / 2, ny).';
    end
    [X, Y] = meshgrid(x, y);
end

function d = vortex_diagnostics(omega, analysis)
    [X, Y] = analysis_grid(analysis, size(omega));
    weight = abs(double(omega));
    total = sum(weight(:));
    d = struct('peak_abs_omega', max(weight(:)), 'centroid_x', NaN, ...
        'centroid_y', NaN, 'core_radius', NaN, 'core_anisotropy', NaN, ...
        'core_detected', false);
    if ~(isfinite(total) && total > eps)
        return;
    end
    d.centroid_x = sum(X(:) .* weight(:)) / total;
    d.centroid_y = sum(Y(:) .* weight(:)) / total;
    sx2 = sum(((X(:) - d.centroid_x) .^ 2) .* weight(:)) / total;
    sy2 = sum(((Y(:) - d.centroid_y) .^ 2) .* weight(:)) / total;
    sx = sqrt(max(sx2, 0));
    sy = sqrt(max(sy2, 0));
    d.core_radius = sqrt(max(sx * sy, 0));
    d.core_anisotropy = sx / max(sy, eps);
    d.core_detected = isfinite(d.core_radius) && d.peak_abs_omega > 0;
end

function drift = conservation_drift(analysis, omega_initial, omega_final)
    if isfield(analysis, 'circulation') && ~isempty(analysis.circulation)
        drift = history_drift(analysis, 'circulation');
        return;
    end
    [~, ~, dx, dy] = grid_spacing_from_analysis(analysis, size(omega_initial));
    initial = sum(omega_initial(:)) * dx * dy;
    final = sum(omega_final(:)) * dx * dy;
    drift = abs(final - initial) / max(abs(initial), eps);
end

function drift = history_drift(analysis, field_name)
    drift = NaN;
    if ~isfield(analysis, field_name) || isempty(analysis.(field_name))
        return;
    end
    hist = double(analysis.(field_name)(:));
    hist = hist(isfinite(hist));
    if ~isempty(hist)
        drift = abs(hist(end) - hist(1)) / max(abs(hist(1)), eps);
    end
end

function cfl = observed_cfl(analysis, parameters)
    [~, ~, dx, dy] = grid_spacing_from_analysis(analysis, ...
        [pick_numeric(parameters, {'Ny'}, 1), pick_numeric(parameters, {'Nx'}, 1)]);
    dt = pick_numeric(parameters, {'dt'}, pick_numeric(analysis, {'dt'}, NaN));
    speed = NaN;
    if isfield(analysis, 'peak_speed_history') && ~isempty(analysis.peak_speed_history)
        speed = max(double(analysis.peak_speed_history(:)), [], 'omitnan');
    elseif isfield(analysis, 'peak_speed')
        speed = double(analysis.peak_speed);
    end
    cfl = speed * dt / max(min(dx, dy), eps);
end

function [x, y, dx, dy] = grid_spacing_from_analysis(analysis, field_size)
    [X, Y] = analysis_grid(analysis, field_size);
    x = X(1, :);
    y = Y(:, 1);
    dx = pick_numeric(analysis, {'dx'}, 1);
    dy = pick_numeric(analysis, {'dy'}, 1);
    if numel(x) > 1
        dx = mean(abs(diff(x)));
    end
    if numel(y) > 1
        dy = mean(abs(diff(y)));
    end
end

function [Results, paths] = finalize_mesh_convergence_workflow(phase_id, phase_cfg, queue_outputs, paths, ...
        wall_time, parent_run_config, phase_parameters, phase_settings, progress_callback, seed_entry, telemetry_context)
    fd_conv = require_output(queue_outputs, 'fd', 'convergence');
    sp_conv = require_output(queue_outputs, 'spectral', 'convergence');
    fd_mesh = select_mesh_from_convergence(fd_conv.results, fd_conv.paths, phase_cfg);
    sp_mesh = select_mesh_from_convergence(sp_conv.results, sp_conv.paths, phase_cfg);
    fd_selected_output = promote_phase1_selected_mesh_output(fd_conv, fd_mesh);
    sp_selected_output = promote_phase1_selected_mesh_output(sp_conv, sp_mesh);
    fd_selected_metrics = compute_phase1_metrics('FD', fd_selected_output, sp_selected_output, fd_mesh, fd_conv);
    sp_selected_metrics = compute_phase1_metrics('Spectral', sp_selected_output, fd_selected_output, sp_mesh, sp_conv);
    selected_error_vs_time = compute_phase1_error_vs_time(fd_selected_output, sp_selected_output);
    summary_metrics = compute_summary_metrics( ...
        fd_selected_metrics, sp_selected_metrics, queue_outputs, phase_cfg, selected_error_vs_time, ...
        struct('enabled', false, 'cases', struct([])));
    summary_metrics.fd_convergence_verdict = pick_text(fd_mesh, {'verdict'}, summary_metrics.fd_convergence_verdict);
    summary_metrics.spectral_convergence_verdict = pick_text(sp_mesh, {'verdict'}, summary_metrics.spectral_convergence_verdict);
    summary_metrics.fd_selected_mesh = mesh_label_from_entry(fd_mesh);
    summary_metrics.spectral_selected_mesh = mesh_label_from_entry(sp_mesh);
    cleanup_roots = collect_mesh_convergence_cleanup_roots(queue_outputs, paths);
    fd_conv = sanitize_mesh_convergence_child_output(fd_conv, paths, phase_cfg);
    sp_conv = sanitize_mesh_convergence_child_output(sp_conv, paths, phase_cfg);
    queue_outputs = replace_mesh_convergence_queue_output(queue_outputs, fd_conv);
    queue_outputs = replace_mesh_convergence_queue_output(queue_outputs, sp_conv);

    Results = struct();
    Results.run_id = phase_id;
    Results.phase_id = phase_id;
    Results.workflow_kind = 'mesh_convergence_study';
    Results.result_layout_kind = 'mesh_convergence_workflow';
    Results.phase_label = 'Mesh Convergence';
    Results.phase_name = 'Mesh convergence workflow';
    Results.phase_config = phase_cfg;
    Results.seed_entry = seed_entry;
    Results.parent_run_config = filter_graphics_objects(parent_run_config);
    Results.parent_parameters = summarize_phase_parameters(phase_parameters);
    Results.children = struct( ...
        'fd', struct( ...
            'method', 'FD', ...
            'method_key', 'fd', ...
            'selected_mesh', fd_mesh, ...
            'convergence_output', strip_heavy_outputs(fd_conv), ...
            'view_summary', build_phase_child_view_summary(fd_selected_output, fd_selected_metrics), ...
            'metrics', fd_selected_metrics), ...
        'spectral', struct( ...
            'method', 'Spectral', ...
            'method_key', 'spectral', ...
            'selected_mesh', sp_mesh, ...
            'convergence_output', strip_heavy_outputs(sp_conv), ...
            'view_summary', build_phase_child_view_summary(sp_selected_output, sp_selected_metrics), ...
            'metrics', sp_selected_metrics));
    Results.combined = struct( ...
        'summary_metrics', summary_metrics, ...
        'fd_metrics', fd_selected_metrics, ...
        'spectral_metrics', sp_selected_metrics, ...
        'error_vs_time', selected_error_vs_time, ...
        'rmse_vs_time', selected_error_vs_time, ...
        'fd_view_summary', build_phase_child_view_summary(fd_selected_output, fd_selected_metrics), ...
        'spectral_view_summary', build_phase_child_view_summary(sp_selected_output, sp_selected_metrics), ...
        'fd_convergence', strip_heavy_outputs(fd_conv), ...
        'spectral_convergence', strip_heavy_outputs(sp_conv), ...
        'paths', paths);
    Results.metrics = struct('FD', fd_selected_metrics, 'Spectral', sp_selected_metrics, 'summary', summary_metrics);
    Results.workflow_queue = build_queue_status_snapshot(queue_outputs_to_jobs(queue_outputs), queue_outputs, numel(queue_outputs), 'completed');
    Results.paths = paths;
    Results.error_vs_time = selected_error_vs_time;
    Results.rmse_vs_time = selected_error_vs_time;
    Results.ic_study = struct('enabled', false, 'cases', struct([]));
    Results.figure_artifacts = struct();
    if logical(pick_value(phase_cfg, 'save_figures', true))
        if phase1_defer_heavy_exports_requested(phase_settings)
            Results.figure_artifacts = struct( ...
                'deferred', true, ...
                'reason', 'host_owned_publication', ...
                'status', 'queued');
            emit_phase_runtime_log(progress_callback, ...
                'Mesh convergence deferred worker-side figure generation; Results publication will autosave visuals on the host.', 'info');
        else
            Results.figure_artifacts = generate_mesh_convergence_plots(Results, paths);
            emit_artifact_struct_logs(progress_callback, 'Mesh convergence figure', Results.figure_artifacts);
        end
    end
    Results.workflow_manifest = build_mesh_convergence_manifest(phase_id, queue_outputs, paths, Results, phase_cfg);
    Results.wall_time = double(wall_time);
    Results.created_at = char(datetime('now', 'Format', 'yyyy-MM-dd HH:mm:ss'));
    phase_monitor_series = build_phase1_workflow_monitor_series(queue_outputs, phase_id, Results.workflow_kind);
    phase_monitor_series = PhaseTelemetryCSVFirst.decorate_monitor_series(phase_monitor_series, telemetry_context);
    [Results.collector_artifacts, Results.plotting_data] = write_mesh_convergence_workflow_collector_artifacts( ...
        Results, parent_run_config, paths, phase_monitor_series);
    workbook_path = pick_text(Results.collector_artifacts, {'phase_workbook_path', 'phase_workbook_root_path'}, '');
    if ~isempty(workbook_path)
        paths.run_data_workbook_path = workbook_path;
    end
    Results.paths = paths;
    Results.workflow_manifest.collector_artifacts = Results.collector_artifacts;
    Results.workflow_manifest.plotting_data_csv_path = pick_text(Results.collector_artifacts, {'mesh_plotting_data_csv_path'}, '');

    ResultsForSave = filter_graphics_objects(Results);
    ResultsForSave.artifact_layout_version = char(string(paths.artifact_layout_version));
    save(fullfile(paths.data, 'mesh_convergence_results.mat'), 'ResultsForSave', '-v7.3');
    emit_phase_runtime_log(progress_callback, sprintf('Mesh convergence saved MAT results: %s', fullfile(paths.data, 'mesh_convergence_results.mat')), 'info');
    if json_saving_enabled(phase_cfg, phase_settings, phase_parameters)
        write_json(fullfile(paths.data, 'mesh_convergence_results.json'), ResultsForSave);
        emit_phase_runtime_log(progress_callback, sprintf('Mesh convergence saved JSON results: %s', fullfile(paths.data, 'mesh_convergence_results.json')), 'info');
    end
    safe_save_mat(fullfile(paths.data, 'mesh_convergence_workflow_manifest.mat'), struct('workflow_manifest', Results.workflow_manifest));
    if json_saving_enabled(phase_cfg, phase_settings, phase_parameters)
        write_json(fullfile(paths.data, 'mesh_convergence_workflow_manifest.json'), Results.workflow_manifest);
        write_phase1_artifact_manifest(paths, ResultsForSave);
    end
    emit_phase_completion_report_payload(progress_callback, ResultsForSave, paths, ...
        parent_run_config, Results.parent_parameters, 'Mesh Convergence', ...
        Results.workflow_kind, 'mesh_convergence_workflow');
    cleanup_mesh_convergence_child_roots(cleanup_roots, paths, progress_callback);
end

function Results = assemble_phase1_results(phase_id, phase_cfg, seed_entry, ...
        fd_conv, sp_conv, fd_run, sp_run, fd_metrics, sp_metrics, summary_metrics, error_vs_time, ic_study, ...
        queue_outputs, paths, wall_time, parent_run_config, phase_parameters)
    children = struct( ...
        'fd', build_phase_child_result(fd_conv, fd_run, fd_metrics, fd_run.selected_mesh), ...
        'spectral', build_phase_child_result(sp_conv, sp_run, sp_metrics, sp_run.selected_mesh));
    combined = build_phase_combined_payload(fd_run, sp_run, fd_conv, sp_conv, ...
        fd_metrics, sp_metrics, summary_metrics, error_vs_time, paths);

    Results = struct();
    Results.run_id = phase_id;
    Results.phase_id = phase_id;
    Results.workflow_kind = 'phase1_periodic_comparison';
    Results.result_layout_kind = 'phase1_workflow';
    Results.phase_name = 'Phase 1 periodic FD vs Spectral verification';
    Results.phase_config = phase_cfg;
    Results.seed_entry = seed_entry;
    Results.parent_run_config = filter_graphics_objects(parent_run_config);
    Results.parent_parameters = summarize_phase_parameters(phase_parameters);
    Results.convergence_outputs = struct([]);
    Results.evolution_outputs = strip_heavy_outputs([fd_run, sp_run]);
    Results.workflow_queue = build_queue_status_snapshot(queue_outputs_to_jobs(queue_outputs), queue_outputs, numel(queue_outputs), 'completed');
    Results.children = children;
    Results.combined = combined;
    Results.error_vs_time = error_vs_time;
    Results.rmse_vs_time = error_vs_time;
    Results.ic_study = ic_study;
    Results.metrics = struct('FD', fd_metrics, 'Spectral', sp_metrics, 'summary', summary_metrics);
    Results.paths = paths;
    Results.workflow_manifest = build_phase_workflow_manifest(phase_id, queue_outputs, paths, ...
        children, combined, summary_metrics, error_vs_time, ic_study, struct(), ...
        phase_cfg, Results.parent_parameters, Results.parent_run_config);
    Results.wall_time = double(wall_time);
    Results.created_at = char(datetime('now', 'Format', 'yyyy-MM-dd HH:mm:ss'));
end

function payload = build_phase_combined_payload(fd_run, sp_run, fd_conv, sp_conv, fd_metrics, sp_metrics, summary_metrics, error_vs_time, paths)
    payload = struct();
    payload.summary_metrics = summary_metrics;
    payload.fd_metrics = fd_metrics;
    payload.spectral_metrics = sp_metrics;
    payload.error_vs_time = error_vs_time;
    payload.rmse_vs_time = error_vs_time;
    payload.fd_convergence = struct();
    payload.spectral_convergence = struct();
    payload.fd_view_summary = build_phase_child_view_summary(fd_run, fd_metrics);
    payload.spectral_view_summary = build_phase_child_view_summary(sp_run, sp_metrics);
    payload.paths = paths;
end

function child = build_phase_child_result(convergence_output, evolution_output, metrics, mesh)
    child = struct();
    child.method = evolution_output.method;
    child.method_key = evolution_output.method_key;
    child.queue_index = evolution_output.queue_index;
    child.job_key = evolution_output.job_key;
    child.selected_mesh = mesh;
    child.metrics = metrics;
    child.convergence_output = strip_heavy_outputs(convergence_output);
    child.convergence_media = struct();
    if isfield(convergence_output, 'results') && isstruct(convergence_output.results) && ...
            isfield(convergence_output.results, 'mesh_level_animation') && ...
            isstruct(convergence_output.results.mesh_level_animation)
        child.convergence_media = convergence_output.results.mesh_level_animation;
    end
    child.evolution_output = strip_heavy_outputs(evolution_output);
    child.view_summary = build_phase_child_view_summary(evolution_output, metrics);
end

function summary = build_phase_child_view_summary(evolution_output, metrics)
    meta = build_phase_child_metadata(evolution_output, metrics);
    summary = struct( ...
        'results', evolution_output.results, ...
        'parameters', evolution_output.parameters, ...
        'run_config', evolution_output.run_config, ...
        'analysis', require_analysis(evolution_output.results, evolution_output.method), ...
        'paths', evolution_output.paths, ...
        'metadata', meta, ...
        'wall_time', evolution_output.wall_time, ...
        'workflow_child', true);
end

function meta = build_phase_child_metadata(evolution_output, metrics)
    meta = struct();
    meta.method = evolution_output.method;
    meta.mode = 'Evolution';
    meta.ic_type = pick_text(evolution_output.run_config, {'ic_type'}, '');
    meta.bc_case = pick_text(evolution_output.parameters, {'boundary_condition_case', 'bc_case'}, 'periodic');
    meta.run_id = pick_text(evolution_output.run_config, {'run_id', 'study_id'}, '');
    meta.timestamp = char(datetime('now', 'Format', 'yyyy-MM-dd HH:mm:ss'));
    meta.wall_time = metrics.runtime_wall_s;
    meta.max_omega = metrics.peak_vorticity_ratio;
    meta.final_energy = NaN;
    meta.final_enstrophy = NaN;
    meta.total_steps = metrics.total_steps;
    meta.num_plot_snapshots = pick_numeric(evolution_output.parameters, {'num_plot_snapshots'}, NaN);
    meta.animation_num_frames = pick_numeric(evolution_output.parameters, {'animation_num_frames', 'num_animation_frames'}, NaN);
    meta.num_snapshots = pick_numeric(evolution_output.parameters, {'num_snapshots'}, NaN);
    if isfield(evolution_output.results, 'final_energy')
        meta.final_energy = evolution_output.results.final_energy;
    end
    if isfield(evolution_output.results, 'final_enstrophy')
        meta.final_enstrophy = evolution_output.results.final_enstrophy;
    end
end

function jobs = queue_outputs_to_jobs(outputs)
    jobs = repmat(empty_job(), 1, numel(outputs));
    for i = 1:numel(outputs)
        jobs(i).label = outputs(i).label;
        jobs(i).method = outputs(i).method;
        jobs(i).method_key = outputs(i).method_key;
        jobs(i).stage = outputs(i).stage;
        jobs(i).job_key = outputs(i).job_key;
        jobs(i).queue_index = outputs(i).queue_index;
        if isfield(outputs(i), 'paths') && isstruct(outputs(i).paths) && isfield(outputs(i).paths, 'base')
            jobs(i).output_root = outputs(i).paths.base;
        end
    end
end

function manifest = build_phase_workflow_manifest(phase_id, queue_outputs, paths, children, combined, summary_metrics, error_vs_time, ic_study, figure_artifacts, phase_cfg, parent_parameters, parent_run_config)
    manifest = struct();
    manifest.phase_id = phase_id;
    manifest.workflow_kind = 'phase1_periodic_comparison';
    manifest.phase_root = paths.base;
    manifest.paths = paths;
    manifest.phase_config = filter_graphics_objects(phase_cfg);
    manifest.parent_parameters = filter_graphics_objects(parent_parameters);
    manifest.parent_run_config = filter_graphics_objects(parent_run_config);
    manifest.children = struct( ...
        'fd', strip_phase_child_for_persistence(children.fd), ...
        'spectral', strip_phase_child_for_persistence(children.spectral));
    manifest.combined = struct( ...
        'summary_metrics', summary_metrics, ...
        'error_vs_time', error_vs_time, ...
        'rmse_vs_time', error_vs_time, ...
        'fd_view_summary', strip_phase_view_summary_for_persistence(combined.fd_view_summary), ...
        'spectral_view_summary', strip_phase_view_summary_for_persistence(combined.spectral_view_summary), ...
        'paths', combined.paths);
    manifest.ic_study = strip_phase1_ic_study_for_persistence(ic_study);
    manifest.figure_artifacts = filter_graphics_objects(figure_artifacts);
    manifest.metrics_summary = summary_metrics;
    manifest.error_vs_time = error_vs_time;
    manifest.rmse_vs_time = error_vs_time;
    manifest.queue = repmat(struct( ...
        'queue_index', NaN, ...
        'job_key', '', ...
        'job_label', '', ...
        'method', '', ...
        'mode', '', ...
        'status', '', ...
        'run_id', '', ...
        'artifact_root', '', ...
        'data_path', ''), 1, numel(queue_outputs));

    for i = 1:numel(queue_outputs)
        manifest.queue(i).queue_index = queue_outputs(i).queue_index;
        manifest.queue(i).job_key = queue_outputs(i).job_key;
        manifest.queue(i).job_label = queue_outputs(i).label;
        manifest.queue(i).method = queue_outputs(i).method;
        manifest.queue(i).mode = queue_outputs(i).stage;
        manifest.queue(i).status = queue_outputs(i).status;
        manifest.queue(i).run_id = pick_text(queue_outputs(i).run_config, {'run_id', 'study_id'}, '');
        manifest.queue(i).artifact_root = pick_text(queue_outputs(i).paths, {'base'}, '');
        manifest.queue(i).data_path = pick_text(queue_outputs(i).results, {'data_path'}, '');
    end
end

function manifest = build_mesh_convergence_manifest(phase_id, queue_outputs, paths, Results, phase_cfg)
    manifest = struct();
    manifest.phase_id = phase_id;
    manifest.workflow_kind = 'mesh_convergence_study';
    manifest.result_layout_kind = 'mesh_convergence_workflow';
    manifest.phase_root = paths.base;
    manifest.paths = paths;
    manifest.phase_config = filter_graphics_objects(phase_cfg);
    manifest.children = filter_graphics_objects(Results.children);
    manifest.combined = filter_graphics_objects(Results.combined);
    manifest.figure_artifacts = filter_graphics_objects(Results.figure_artifacts);
    manifest.error_vs_time = filter_graphics_objects(pick_struct(Results, {'error_vs_time', 'rmse_vs_time'}, struct()));
    manifest.rmse_vs_time = manifest.error_vs_time;
    manifest.metrics_summary = filter_graphics_objects(Results.metrics.summary);
    manifest.queue = repmat(struct( ...
        'queue_index', NaN, ...
        'job_key', '', ...
        'job_label', '', ...
        'method', '', ...
        'mode', '', ...
        'status', '', ...
        'run_id', '', ...
        'artifact_root', '', ...
        'data_path', ''), 1, numel(queue_outputs));
    for i = 1:numel(queue_outputs)
        manifest.queue(i).queue_index = queue_outputs(i).queue_index;
        manifest.queue(i).job_key = queue_outputs(i).job_key;
        manifest.queue(i).job_label = queue_outputs(i).label;
        manifest.queue(i).method = queue_outputs(i).method;
        manifest.queue(i).mode = queue_outputs(i).stage;
        manifest.queue(i).status = queue_outputs(i).status;
        manifest.queue(i).run_id = pick_text(queue_outputs(i).run_config, {'run_id', 'study_id'}, '');
        manifest.queue(i).artifact_root = pick_text(queue_outputs(i).paths, {'base'}, '');
        manifest.queue(i).data_path = pick_text(queue_outputs(i).results, {'data_path'}, '');
    end
end

function [artifact_summary, plotting_data] = write_phase1_workflow_collector_artifacts(Results, Run_Config, paths, phase_monitor_series)
    if nargin < 4 || ~isstruct(phase_monitor_series)
        phase_monitor_series = struct();
    end
    plotting_data = struct([]);
    summary_context = struct( ...
        'run_id', pick_text(Results, {'run_id'}, ''), ...
        'phase_id', pick_text(Results, {'phase_id'}, ''), ...
        'workflow_kind', pick_text(Results, {'workflow_kind'}, ''), ...
        'run_config', filter_graphics_objects(Run_Config), ...
        'monitor_series', filter_graphics_objects(phase_monitor_series), ...
        'results', strip_phase1_for_persistence(Results), ...
        'paths', paths);
    artifact_summary = ExternalCollectorDispatcher.write_run_artifacts(summary_context);
    try
        stage_summary = table();
        stage_summary_path = pick_text(artifact_summary, {'stage_summary_csv_path'}, '');
        if ~isempty(stage_summary_path) && exist(stage_summary_path, 'file') == 2
            stage_summary = readtable(stage_summary_path);
        end
        plotting_table = ExternalCollectorDispatcher.export_phase1_plotting_data_table(summary_context, stage_summary);
        if istable(plotting_table) && ~isempty(plotting_table)
            plotting_data = table2struct(plotting_table);
        end
    catch ME
        warning('Phase1PeriodicComparison:PlottingDataBuildFailed', ...
            'Could not build Phase 1 plotting-data payload: %s', ME.message);
        plotting_data = struct([]);
    end
end

function [artifact_summary, plotting_data] = write_mesh_convergence_workflow_collector_artifacts(Results, Run_Config, paths, phase_monitor_series)
    if nargin < 4 || ~isstruct(phase_monitor_series)
        phase_monitor_series = struct();
    end
    plotting_data = struct([]);
    artifact_summary = struct();
    summary_context = struct( ...
        'run_id', pick_text(Results, {'run_id'}, ''), ...
        'phase_id', pick_text(Results, {'phase_id'}, ''), ...
        'workflow_kind', pick_text(Results, {'workflow_kind'}, ''), ...
        'run_config', filter_graphics_objects(Run_Config), ...
        'monitor_series', filter_graphics_objects(phase_monitor_series), ...
        'results', strip_phase1_for_persistence(Results), ...
        'paths', paths);
    artifact_summary = ExternalCollectorDispatcher.write_run_artifacts(summary_context);
    artifact_summary.mesh_plotting_data_csv_path = '';
    try
        stage_summary = table();
        stage_summary_path = pick_text(artifact_summary, {'stage_summary_csv_path'}, '');
        if ~isempty(stage_summary_path) && exist(stage_summary_path, 'file') == 2
            stage_summary = readtable(stage_summary_path);
        end
        plotting_table = ExternalCollectorDispatcher.export_mesh_convergence_plotting_data_table(summary_context, stage_summary);
        if istable(plotting_table) && ~isempty(plotting_table)
            artifact_summary.mesh_plotting_data_csv_path = fullfile(paths.data, 'mesh_convergence_plotting_data.csv');
            writetable(plotting_table, artifact_summary.mesh_plotting_data_csv_path);
            plotting_data = table2struct(plotting_table);
        end
    catch ME
        warning('Phase1PeriodicComparison:MeshPlottingDataBuildFailed', ...
            'Could not build mesh-convergence plotting-data payload: %s', ME.message);
        artifact_summary.mesh_plotting_data_csv_path = '';
        plotting_data = struct([]);
    end
end

function [result_payload, path_payload] = run_phase1_child_dispatch(run_config, parameters, settings)
    child_live_monitor = phase_child_telemetry_requested(settings);
    collectors_enabled = phase_child_external_collectors_enabled(settings);
    dispatch_settings = settings;
    dispatch_settings.force_synchronous_execution = true;
    dispatch_settings.suppress_standard_completion_payload = true;
    dispatch_settings = disable_phase_child_external_collectors(dispatch_settings);
    if isfield(dispatch_settings, 'progress_data_queue')
        dispatch_settings = rmfield(dispatch_settings, 'progress_data_queue');
    end
    if child_live_monitor
        dispatch_settings.ui_progress_callback = resolve_runtime_progress_callback(settings);
    end
    [result_payload, path_payload] = RunDispatcher(run_config, parameters, dispatch_settings);

    if collectors_enabled
        result_payload = attach_phase_child_collector_probe(result_payload, settings);
    end
end

function tf = phase_child_telemetry_requested(settings)
    tf = false;
    if ~isstruct(settings)
        return;
    end
    if ~isempty(resolve_runtime_progress_callback(settings))
        tf = true;
        return;
    end
    if isfield(settings, 'monitor_enabled') && logical(settings.monitor_enabled)
        tf = true;
    end
end

function tf = phase_child_external_collectors_enabled(settings)
    tf = false;
    if ~isstruct(settings)
        return;
    end
    sustainability_cfg = pick_struct(settings, {'sustainability'}, struct());
    external_cfg = pick_struct(sustainability_cfg, {'external_collectors'}, struct());
    collector_fields = {'hwinfo', 'icue'};
    for i = 1:numel(collector_fields)
        if isfield(external_cfg, collector_fields{i}) && logical(external_cfg.(collector_fields{i}))
            tf = true;
            return;
        end
    end
end

function settings = disable_phase_child_external_collectors(settings)
    if ~isstruct(settings)
        settings = struct();
        return;
    end
    if ~isfield(settings, 'sustainability') || ~isstruct(settings.sustainability)
        settings.sustainability = struct();
    end
    if ~isfield(settings.sustainability, 'external_collectors') || ...
            ~isstruct(settings.sustainability.external_collectors)
        settings.sustainability.external_collectors = struct();
    end
    settings.sustainability.external_collectors.hwinfo = false;
    settings.sustainability.external_collectors.icue = false;
end

function result_payload = attach_phase_child_collector_probe(result_payload, settings)
    if ~isstruct(result_payload)
        result_payload = struct();
    end
    sample = build_phase_child_collector_probe_sample(settings);
    if ~(isstruct(sample) && ~isempty(fieldnames(sample)))
        return;
    end
    result_payload.collector_last_sample = sample;
    if ~isfield(result_payload, 'collector_session') || ...
            ~(isstruct(result_payload.collector_session) && ~isempty(fieldnames(result_payload.collector_session)))
        result_payload.collector_session = sample;
    end
end

function sample = build_phase_child_collector_probe_sample(settings)
    sample = struct( ...
        'timestamp_utc', char(datetime('now', 'TimeZone', 'UTC', ...
            'Format', 'yyyy-MM-dd''T''HH:mm:ss''Z''')), ...
        'metrics', struct(), ...
        'collector_series', struct('hwinfo', struct(), 'icue', struct()), ...
        'collector_status', struct('hwinfo', 'disabled', 'icue', 'disabled'), ...
        'coverage_domains', struct('hwinfo', {{}}, 'icue', {{}}), ...
        'preferred_source', struct(), ...
        'raw_log_paths', struct('hwinfo', '', 'icue', ''), ...
        'overlay_metrics', {{'cpu_proxy', 'gpu_series', 'memory_series', 'system_power_w', 'cpu_temp_c'}}, ...
        'collector_metric_catalog', struct([]), ...
        'hwinfo_transport', 'none', ...
        'hwinfo_status_reason', '', ...
        'collector_probe_details', struct('hwinfo', struct(), 'icue', struct()));

    if ~isstruct(settings)
        return;
    end

    probe_settings = settings;
    if ~isfield(probe_settings, 'sustainability') || ~isstruct(probe_settings.sustainability)
        probe_settings.sustainability = struct();
    end
    if ~isfield(probe_settings.sustainability, 'collector_runtime') || ...
            ~isstruct(probe_settings.sustainability.collector_runtime)
        probe_settings.sustainability.collector_runtime = struct();
    end
    probe_settings.sustainability.collector_runtime.hwinfo_transport_mode = 'csv';

    try
        probe = ExternalCollectorDispatcher.probe_collectors(probe_settings);
    catch ME
        warning('Phase1PeriodicComparison:ChildCollectorProbeFailed', ...
            'Could not capture child collector CSV probe after direct workflow dispatch: %s', ME.message);
        return;
    end
    if ~(isstruct(probe) && isfield(probe, 'sources') && isstruct(probe.sources))
        return;
    end

    source_names = {'hwinfo', 'icue'};
    for i = 1:numel(source_names)
        source = source_names{i};
        snapshot = pick_struct(probe.sources, {source}, struct());
        sample.collector_status.(source) = pick_text(snapshot, {'status'}, 'disabled');
        if isfield(snapshot, 'probe_details') && isstruct(snapshot.probe_details)
            sample.collector_probe_details.(source) = snapshot.probe_details;
        end
        if strcmpi(source, 'hwinfo')
            sample.hwinfo_transport = pick_text(snapshot, {'transport'}, 'none');
            sample.hwinfo_status_reason = pick_text(snapshot, {'status_reason', 'message'}, '');
            csv_path = pick_text(snapshot, {'csv_path'}, '');
            if isempty(csv_path)
                csv_path = pick_text(snapshot, {'csv_target_path'}, '');
            end
            sample.raw_log_paths.hwinfo = csv_path;
        else
            csv_path = pick_text(snapshot, {'csv_path'}, '');
            sample.raw_log_paths.icue = csv_path;
        end
    end
end

function monitor_series = build_phase1_workflow_monitor_series(queue_outputs, phase_id, workflow_kind)
    if nargin < 3 || strlength(string(workflow_kind)) == 0
        workflow_kind = 'phase1_periodic_comparison';
    end
    segments = phase1_workflow_monitor_segments(queue_outputs);
    monitor_series = concatenate_workflow_monitor_segments(segments, workflow_kind, phase_id);
    if ~isempty(fieldnames(monitor_series))
        segment_manifest = strip_workflow_monitor_segments_for_manifest(segments);
        for i = 1:numel(segment_manifest)
            segment_manifest(i).phase_id = phase_id;
            segment_manifest(i).workflow_kind = workflow_kind;
        end
        monitor_series.workflow_segment_manifest = segment_manifest;
    end
end

function segments = phase1_workflow_monitor_segments(queue_outputs)
    segments = repmat(empty_workflow_monitor_segment(), 1, 0);
    child_index = 0;
    for i = 1:numel(queue_outputs)
        output = queue_outputs(i);
        method_label = phase1_method_display_label(output.method_key);
        switch lower(char(string(output.stage)))
            case 'convergence'
                run_records = pick_value(pick_struct(output.results, {'run_records'}, struct([])), 'run_records', struct([]));
                if ~isstruct(run_records) || isempty(run_records)
                    run_records = pick_value(output.results, 'run_records', struct([]));
                end
                if ~isstruct(run_records)
                    continue;
                end
                for ri = 1:numel(run_records)
                    child_index = child_index + 1;
                    record = run_records(ri);
                    summary_path = pick_text(record, {'mesh_level_summary_path'}, '');
                    if isempty(summary_path) || exist(summary_path, 'file') ~= 2
                        continue;
                    end
                    summary_data = load(summary_path);
                    segment = empty_workflow_monitor_segment();
                    segment.result_struct = struct();
                    if isfield(summary_data, 'results_summary') && isstruct(summary_data.results_summary) && ...
                            isfield(summary_data.results_summary, 'results') && isstruct(summary_data.results_summary.results)
                        segment.result_struct = summary_data.results_summary.results;
                    end
                    if isempty(fieldnames(segment.result_struct))
                        segment.result_struct = pick_struct(summary_data, {'results'}, struct());
                    end
                    segment.wall_time_s = pick_numeric(record, {'runtime_wall_s'}, NaN);
                    segment.stage_id = sprintf('%s_convergence', output.method_key);
                    segment.stage_label = sprintf('%s Convergence', method_label);
                    segment.stage_type = 'convergence';
                    segment.stage_method = method_label;
                    segment.substage_id = pick_text(record, {'mesh_level_label'}, sprintf('L%02d', ri));
                    segment.substage_label = segment.substage_id;
                    segment.substage_type = 'mesh_level';
                    segment.scenario_id = '';
                    segment.mesh_level = pick_numeric(record, {'mesh_level_index'}, ri);
                    segment.mesh_index = double(ri);
                    segment.child_run_index = double(child_index);
                    segment.mesh_nx = pick_numeric(record, {'Nx'}, NaN);
                    segment.mesh_ny = pick_numeric(record, {'Ny'}, NaN);
                    segment.raw_hwinfo_csv_path = workflow_segment_hwinfo_csv(segment.result_struct);
                    segments(end + 1) = segment; %#ok<AGROW>
                end
            case 'evolution'
                child_index = child_index + 1;
                segment = empty_workflow_monitor_segment();
                phase1_cfg = pick_struct(output.parameters, {'phase1'}, struct());
                ic_cfg = pick_struct(phase1_cfg, {'ic_study'}, struct());
                segment.result_struct = output.results;
                segment.wall_time_s = pick_numeric(output.results, {'wall_time', 'total_time'}, output.wall_time);
                segment.stage_id = sprintf('%s_evolution', output.method_key);
                segment.stage_label = sprintf('%s Evolution', method_label);
                segment.stage_type = 'evolution';
                segment.stage_method = method_label;
                baseline_meta = phase1_baseline_case_metadata(pick_text(output.run_config, {'ic_type'}, pick_text(output.parameters, {'ic_type'}, 'stretched_gaussian')));
                segment.substage_id = baseline_meta.case_id;
                segment.substage_label = pick_text(ic_cfg, {'baseline_label'}, baseline_meta.label);
                segment.substage_type = 'ic_case';
                segment.scenario_id = '';
                segment.mesh_level = NaN;
                segment.mesh_index = NaN;
                segment.child_run_index = double(child_index);
                segment.mesh_nx = pick_numeric(output.parameters, {'Nx'}, NaN);
                segment.mesh_ny = pick_numeric(output.parameters, {'Ny'}, NaN);
                segment.raw_hwinfo_csv_path = workflow_segment_hwinfo_csv(segment.result_struct);
                segments(end + 1) = segment; %#ok<AGROW>
            case 'ic_study'
                child_index = child_index + 1;
                case_id = pick_text(output.run_config, {'phase1_ic_study_case_id'}, ...
                    pick_text(output.parameters, {'phase1_ic_study_case_id'}, sprintf('case_%02d', child_index)));
                case_label = pick_text(output.run_config, {'phase1_ic_study_case_label'}, ...
                    pick_text(output.parameters, {'phase1_ic_study_case_label'}, case_id));
                segment = empty_workflow_monitor_segment();
                segment.result_struct = output.results;
                segment.wall_time_s = pick_numeric(output.results, {'wall_time', 'total_time'}, output.wall_time);
                segment.stage_id = sprintf('%s_ic_study', output.method_key);
                segment.stage_label = sprintf('%s IC Study', method_label);
                segment.stage_type = 'ic_study';
                segment.stage_method = method_label;
                segment.substage_id = case_id;
                segment.substage_label = case_label;
                segment.substage_type = 'ic_case';
                segment.scenario_id = '';
                segment.mesh_level = NaN;
                segment.mesh_index = NaN;
                segment.child_run_index = double(child_index);
                segment.mesh_nx = pick_numeric(output.parameters, {'Nx'}, NaN);
                segment.mesh_ny = pick_numeric(output.parameters, {'Ny'}, NaN);
                segment.raw_hwinfo_csv_path = workflow_segment_hwinfo_csv(segment.result_struct);
                segments(end + 1) = segment; %#ok<AGROW>
        end
    end
end

function segment = empty_workflow_monitor_segment()
    segment = struct( ...
        'result_struct', struct(), ...
        'wall_time_s', NaN, ...
        'stage_id', '', ...
        'stage_label', '', ...
        'stage_type', '', ...
        'stage_method', '', ...
        'phase_id', '', ...
        'workflow_kind', '', ...
        'substage_id', '', ...
        'substage_label', '', ...
        'substage_type', '', ...
        'scenario_id', '', ...
        'mesh_level', NaN, ...
        'mesh_index', NaN, ...
        'child_run_index', NaN, ...
        'mesh_nx', NaN, ...
        'mesh_ny', NaN, ...
        'raw_hwinfo_csv_path', '');
end

function monitor_series = concatenate_workflow_monitor_segments(segments, workflow_kind, phase_id)
    monitor_series = struct();
    if nargin < 1 || ~isstruct(segments) || isempty(segments)
        return;
    end

    offset_t = 0;
    gap_t = 1.0e-6;
    for i = 1:numel(segments)
        segment_series = workflow_monitor_series_from_segment(segments(i), workflow_kind, phase_id);
        if isempty(fieldnames(segment_series)) || ~isfield(segment_series, 't') || isempty(segment_series.t)
            continue;
        end
        local_t = reshape(double(segment_series.t), 1, []);
        local_t = local_t - local_t(1);
        if i > 1
            local_t = local_t + offset_t + gap_t;
        end
        segment_series.t = local_t;
        segment_series.elapsed_wall_time = local_t;
        monitor_series = append_workflow_monitor_series(monitor_series, segment_series);
        offset_t = local_t(end);
    end

    if isempty(fieldnames(monitor_series))
        return;
    end
    monitor_series.workflow_kind = workflow_kind;
    monitor_series.workflow_phase_id = phase_id;
    monitor_series = ExternalCollectorDispatcher.normalize_collector_payload(monitor_series);
end

function segment_series = workflow_monitor_series_from_segment(segment, workflow_kind, phase_id)
    segment_series = struct();
    result_struct = pick_struct(segment, {'result_struct'}, struct());
    if isempty(fieldnames(result_struct))
        return;
    end

    sample = choose_segment_collector_sample(result_struct);
    csv_path = '';
    if isstruct(sample) && isfield(sample, 'raw_log_paths') && isstruct(sample.raw_log_paths)
        csv_path = pick_text(sample.raw_log_paths, {'hwinfo'}, '');
    end
    if ~isempty(csv_path) && exist(csv_path, 'file') == 2
        segment_series = workflow_monitor_series_from_hwinfo_csv(csv_path, result_struct, sample);
    elseif ~isempty(fieldnames(sample))
        segment_series = workflow_monitor_series_from_sample(sample, pick_numeric(segment, {'wall_time_s'}, NaN));
    end
    if isempty(fieldnames(segment_series)) || ~isfield(segment_series, 't') || isempty(segment_series.t)
        return;
    end

    n = numel(segment_series.t);
    segment_series.workflow_kind = workflow_kind;
    segment_series.workflow_phase_id = phase_id;
    segment_series.workflow_kind_series = repmat(string(workflow_kind), 1, n);
    segment_series.workflow_phase_id_series = repmat(string(phase_id), 1, n);
    segment_series.workflow_stage_id_series = repmat(string(pick_text(segment, {'stage_id'}, 'stage')), 1, n);
    segment_series.workflow_stage_label_series = repmat(string(pick_text(segment, {'stage_label'}, 'Stage')), 1, n);
    segment_series.workflow_stage_type_series = repmat(string(pick_text(segment, {'stage_type'}, 'stage')), 1, n);
    segment_series.workflow_method_series = repmat(string(pick_text(segment, {'stage_method'}, '')), 1, n);
    segment_series.workflow_substage_id_series = repmat(string(pick_text(segment, {'substage_id'}, '')), 1, n);
    segment_series.workflow_substage_label_series = repmat(string(pick_text(segment, {'substage_label'}, '')), 1, n);
    segment_series.workflow_substage_type_series = repmat(string(pick_text(segment, {'substage_type'}, '')), 1, n);
    segment_series.workflow_scenario_id_series = repmat(string(pick_text(segment, {'scenario_id'}, '')), 1, n);
    segment_series.workflow_stage_wall_time_series = repmat(double(pick_numeric(segment, {'wall_time_s'}, NaN)), 1, n);
    segment_series.workflow_mesh_level_series = repmat(double(pick_numeric(segment, {'mesh_level'}, NaN)), 1, n);
    segment_series.workflow_mesh_index_series = repmat(double(pick_numeric(segment, {'mesh_index'}, NaN)), 1, n);
    segment_series.workflow_child_run_index_series = repmat(double(pick_numeric(segment, {'child_run_index'}, NaN)), 1, n);
    segment_series.workflow_child_mesh_nx_series = repmat(double(pick_numeric(segment, {'mesh_nx'}, NaN)), 1, n);
    segment_series.workflow_child_mesh_ny_series = repmat(double(pick_numeric(segment, {'mesh_ny'}, NaN)), 1, n);
end

function sample = choose_segment_collector_sample(result_struct)
    sample = struct();
    if isfield(result_struct, 'collector_session') && isstruct(result_struct.collector_session)
        sample = ExternalCollectorDispatcher.normalize_collector_payload(result_struct.collector_session);
    end
    if isfield(result_struct, 'collector_last_sample') && isstruct(result_struct.collector_last_sample)
        latest_sample = ExternalCollectorDispatcher.normalize_collector_payload(result_struct.collector_last_sample);
        if isempty(fieldnames(sample))
            sample = latest_sample;
        else
            sample = merge_structs(sample, latest_sample);
        end
    end
end

function segment_series = workflow_monitor_series_from_hwinfo_csv(csv_path, result_struct, sample)
    segment_series = struct();
    try
        data_table = readtable(csv_path);
    catch
        return;
    end
    if isempty(data_table) || ~ismember('session_time_s', data_table.Properties.VariableNames)
        return;
    end

    metric_keys = {'cpu_proxy', 'gpu_series', 'memory_series', 'cpu_temp_c', 'system_power_w', ...
        'cpu_voltage_v', 'gpu_voltage_v', 'memory_voltage_v', 'cpu_power_w_hwinfo', ...
        'gpu_power_w_hwinfo', 'memory_power_w_or_proxy', 'environmental_energy_wh_cum', ...
        'environmental_co2_g_cum', 'fan_rpm', 'pump_rpm', 'coolant_temp_c', 'device_battery_level'};
    segment_series = local_empty_monitor_series();
    segment_series.collector_status.hwinfo = table_last_text_local(data_table, 'hwinfo_status', 'shared_memory_connected');
    segment_series.hwinfo_transport = table_last_text_local(data_table, 'hwinfo_transport', 'shared_memory');
    segment_series.raw_log_paths.hwinfo = csv_path;
    segment_series.t = reshape(double(data_table.session_time_s), 1, []);
    segment_series.elapsed_wall_time = segment_series.t;
    if ismember('timestamp_utc', data_table.Properties.VariableNames)
        segment_series.wall_clock_time = reshape(local_utc_series_to_posix(data_table.timestamp_utc), 1, []);
    end
    segment_series.collector_series.hwinfo = struct();
    for i = 1:numel(metric_keys)
        key = metric_keys{i};
        if ~ismember(key, data_table.Properties.VariableNames)
            continue;
        end
        values = reshape(double(data_table.(key)), 1, []);
        segment_series.collector_series.hwinfo.(key) = values;
        segment_series.(key) = values;
    end
    if isfield(result_struct, 'collector_metric_catalog') && ~isempty(result_struct.collector_metric_catalog)
        segment_series.collector_metric_catalog = result_struct.collector_metric_catalog;
    elseif isstruct(sample) && isfield(sample, 'collector_metric_catalog') && ~isempty(sample.collector_metric_catalog)
        segment_series.collector_metric_catalog = sample.collector_metric_catalog;
    else
        segment_series.collector_metric_catalog = struct([]);
    end
end

function segment_series = workflow_monitor_series_from_sample(sample, wall_time_s)
    segment_series = ExternalCollectorDispatcher.normalize_collector_payload(sample);
    if isempty(fieldnames(segment_series))
        return;
    end
    if ~(isfinite(wall_time_s) && wall_time_s > 0)
        wall_time_s = 1.0;
    end
    metric_keys = {'cpu_proxy', 'gpu_series', 'memory_series', 'cpu_temp_c', 'power_w', ...
        'cpu_voltage_v', 'gpu_voltage_v', 'memory_voltage_v', 'cpu_power_w_hwinfo', ...
        'gpu_power_w_hwinfo', 'memory_power_w_or_proxy', 'system_power_w', ...
        'environmental_energy_wh_cum', 'environmental_co2_g_cum', 'fan_rpm', ...
        'pump_rpm', 'coolant_temp_c', 'device_battery_level'};
    segment_series.t = [0, wall_time_s];
    segment_series.elapsed_wall_time = segment_series.t;
    if ~isfield(segment_series, 'collector_series') || ~isstruct(segment_series.collector_series)
        segment_series.collector_series = local_empty_monitor_series().collector_series;
    end
    for i = 1:numel(metric_keys)
        key = metric_keys{i};
        value = NaN;
        if isfield(segment_series, key) && isnumeric(segment_series.(key)) && ~isempty(segment_series.(key))
            raw = reshape(double(segment_series.(key)), 1, []);
            value = raw(end);
        elseif isfield(sample, 'metrics') && isstruct(sample.metrics) && isfield(sample.metrics, key) && ...
                isnumeric(sample.metrics.(key)) && isscalar(sample.metrics.(key)) && isfinite(sample.metrics.(key))
            value = double(sample.metrics.(key));
        end
        if isfinite(value)
            segment_series.(key) = [value, value];
            if ~isfield(segment_series.collector_series, 'hwinfo') || ~isstruct(segment_series.collector_series.hwinfo)
                segment_series.collector_series.hwinfo = struct();
            end
            if ~isfield(segment_series.collector_series.hwinfo, key) || isempty(segment_series.collector_series.hwinfo.(key))
                segment_series.collector_series.hwinfo.(key) = [value, value];
            end
        end
    end
    if ~isfield(segment_series, 'collector_metric_catalog') || isempty(segment_series.collector_metric_catalog)
        if isstruct(sample) && isfield(sample, 'collector_metric_catalog') && ~isempty(sample.collector_metric_catalog)
            segment_series.collector_metric_catalog = sample.collector_metric_catalog;
        else
            segment_series.collector_metric_catalog = struct([]);
        end
    end
end

function combined = append_workflow_monitor_series(combined, segment)
    if isempty(fieldnames(combined))
        combined = segment;
        return;
    end

    numeric_fields = {'t', 'elapsed_wall_time', 'wall_clock_time', 'iters', 'iter_rate', 'iter_completion_pct', ...
        'cpu_proxy', 'gpu_series', 'memory_series', 'cpu_temp_c', 'power_w', 'system_power_w', ...
        'cpu_voltage_v', 'gpu_voltage_v', 'memory_voltage_v', 'cpu_power_w_hwinfo', ...
        'gpu_power_w_hwinfo', 'memory_power_w_or_proxy', 'environmental_energy_wh_cum', ...
        'environmental_co2_g_cum', 'fan_rpm', 'pump_rpm', 'coolant_temp_c', 'device_battery_level', ...
        'workflow_stage_wall_time_series', 'workflow_mesh_level_series', 'workflow_mesh_index_series', 'workflow_child_run_index_series', ...
        'workflow_child_mesh_nx_series', 'workflow_child_mesh_ny_series'};
    text_fields = {'workflow_kind_series', 'workflow_phase_id_series', 'workflow_stage_id_series', ...
        'workflow_stage_label_series', 'workflow_stage_type_series', 'workflow_method_series', ...
        'workflow_substage_id_series', 'workflow_substage_label_series', 'workflow_substage_type_series', ...
        'workflow_scenario_id_series'};

    for i = 1:numel(numeric_fields)
        key = numeric_fields{i};
        combined.(key) = concat_numeric_field_local(combined, segment, key);
    end
    for i = 1:numel(text_fields)
        key = text_fields{i};
        combined.(key) = concat_text_field_local(combined, segment, key);
    end

    if ~isfield(combined, 'collector_series') || ~isstruct(combined.collector_series)
        combined.collector_series = struct();
    end
    if isfield(segment, 'collector_series') && isstruct(segment.collector_series)
        source_names = union(fieldnames(combined.collector_series), fieldnames(segment.collector_series));
        for si = 1:numel(source_names)
            source = source_names{si};
            if ~isfield(combined.collector_series, source) || ~isstruct(combined.collector_series.(source))
                combined.collector_series.(source) = struct();
            end
            source_fields = union(fieldnames(combined.collector_series.(source)), ...
                fieldnames(pick_struct(segment.collector_series, {source}, struct())));
            for fi = 1:numel(source_fields)
                field_name = source_fields{fi};
                combined.collector_series.(source).(field_name) = concat_numeric_struct_field_local( ...
                    combined.collector_series.(source), pick_struct(segment.collector_series, {source}, struct()), field_name);
            end
        end
    end

    combined = merge_structs(combined, rmfield_if_present(segment, [numeric_fields, text_fields, {'collector_series'}]));
end

function manifest = strip_workflow_monitor_segments_for_manifest(segments)
    manifest = repmat(struct( ...
        'phase_id', '', ...
        'workflow_kind', '', ...
        'stage_id', '', ...
        'stage_label', '', ...
        'stage_type', '', ...
        'stage_method', '', ...
        'substage_id', '', ...
        'substage_label', '', ...
        'substage_type', '', ...
        'scenario_id', '', ...
        'mesh_level', NaN, ...
        'mesh_index', NaN, ...
        'child_run_index', NaN, ...
        'mesh_nx', NaN, ...
        'mesh_ny', NaN, ...
        'wall_time_s', NaN, ...
        'raw_hwinfo_csv_path', ''), 1, 0);
    if ~isstruct(segments) || isempty(segments)
        return;
    end
    for i = 1:numel(segments)
        manifest(end + 1) = struct( ... %#ok<AGROW>
            'phase_id', '', ...
            'workflow_kind', '', ...
            'stage_id', pick_text(segments(i), {'stage_id'}, ''), ...
            'stage_label', pick_text(segments(i), {'stage_label'}, ''), ...
            'stage_type', pick_text(segments(i), {'stage_type'}, ''), ...
            'stage_method', pick_text(segments(i), {'stage_method'}, ''), ...
            'substage_id', pick_text(segments(i), {'substage_id'}, ''), ...
            'substage_label', pick_text(segments(i), {'substage_label'}, ''), ...
            'substage_type', pick_text(segments(i), {'substage_type'}, ''), ...
            'scenario_id', pick_text(segments(i), {'scenario_id'}, ''), ...
            'mesh_level', pick_numeric(segments(i), {'mesh_level'}, NaN), ...
            'mesh_index', pick_numeric(segments(i), {'mesh_index'}, NaN), ...
            'child_run_index', pick_numeric(segments(i), {'child_run_index'}, NaN), ...
            'mesh_nx', pick_numeric(segments(i), {'mesh_nx'}, NaN), ...
            'mesh_ny', pick_numeric(segments(i), {'mesh_ny'}, NaN), ...
            'wall_time_s', pick_numeric(segments(i), {'wall_time_s'}, NaN), ...
            'raw_hwinfo_csv_path', pick_text(segments(i), {'raw_hwinfo_csv_path'}, ''));
    end
end

function csv_path = workflow_segment_hwinfo_csv(result_struct)
    csv_path = '';
    sample = choose_segment_collector_sample(result_struct);
    if isstruct(sample) && isfield(sample, 'raw_log_paths') && isstruct(sample.raw_log_paths)
        csv_path = pick_text(sample.raw_log_paths, {'hwinfo'}, '');
    end
end

function value = concat_numeric_field_local(base_struct, overlay_struct, field_name)
    value = zeros(1, 0);
    if isfield(base_struct, field_name) && isnumeric(base_struct.(field_name))
        value = reshape(double(base_struct.(field_name)), 1, []);
    end
    if isfield(overlay_struct, field_name) && isnumeric(overlay_struct.(field_name))
        value = [value, reshape(double(overlay_struct.(field_name)), 1, [])]; %#ok<AGROW>
    end
end

function value = concat_numeric_struct_field_local(base_struct, overlay_struct, field_name)
    value = zeros(1, 0);
    if isfield(base_struct, field_name) && isnumeric(base_struct.(field_name))
        value = reshape(double(base_struct.(field_name)), 1, []);
    end
    if isfield(overlay_struct, field_name) && isnumeric(overlay_struct.(field_name))
        value = [value, reshape(double(overlay_struct.(field_name)), 1, [])]; %#ok<AGROW>
    end
end

function value = concat_text_field_local(base_struct, overlay_struct, field_name)
    value = strings(1, 0);
    if isfield(base_struct, field_name) && ~isempty(base_struct.(field_name))
        value = reshape(string(base_struct.(field_name)), 1, []);
    end
    if isfield(overlay_struct, field_name) && ~isempty(overlay_struct.(field_name))
        value = [value, reshape(string(overlay_struct.(field_name)), 1, [])]; %#ok<AGROW>
    end
end

function posix_values = local_utc_series_to_posix(values_in)
    posix_values = nan(numel(values_in), 1);
    try
        dt = datetime(string(values_in), 'TimeZone', 'UTC', 'InputFormat', 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z''');
    catch
        try
            dt = datetime(string(values_in), 'TimeZone', 'UTC');
        catch
            return;
        end
    end
    posix_values = reshape(posixtime(dt), [], 1);
end

function text = table_last_text_local(tbl, column_name, fallback)
    text = fallback;
    if isempty(tbl) || ~ismember(column_name, tbl.Properties.VariableNames)
        return;
    end
    values = string(tbl.(column_name));
    values = values(strlength(strtrim(values)) > 0);
    if ~isempty(values)
        text = char(values(end));
    end
end

function out = rmfield_if_present(in, field_names)
    out = in;
    if ~isstruct(out)
        return;
    end
    present = intersect(fieldnames(out), field_names, 'stable');
    if ~isempty(present)
        out = rmfield(out, present);
    end
end

function sample = local_empty_monitor_series()
    sample = struct( ...
        'timestamp_utc', '', ...
        'metrics', struct(), ...
        'collector_series', struct('hwinfo', struct(), 'icue', struct()), ...
        'collector_status', struct('hwinfo', 'disabled', 'icue', 'disabled'), ...
        'coverage_domains', struct('hwinfo', {{}}, 'icue', {{}}), ...
        'preferred_source', struct(), ...
        'raw_log_paths', struct('hwinfo', '', 'icue', ''), ...
        'overlay_metrics', {{'cpu_proxy', 'gpu_series', 'memory_series', 'system_power_w', 'cpu_temp_c'}}, ...
        'collector_metric_catalog', struct([]), ...
        'hwinfo_transport', 'none', ...
        'hwinfo_status_reason', '', ...
        'collector_probe_details', struct('hwinfo', struct(), 'icue', struct()));
end

function stripped = strip_heavy_outputs(outputs)
    stripped = outputs;
    for i = 1:numel(stripped)
        if isfield(stripped(i), 'results') && isstruct(stripped(i).results) && ...
                isfield(stripped(i).results, 'analysis')
            stripped(i).results.analysis = summarize_analysis(stripped(i).results.analysis);
        end
    end
end

function summary = summarize_analysis(analysis)
    summary = struct();
    keep = {'Nx', 'Ny', 'dx', 'dy', 'dt', 'Tfinal', 'grid_points', 'kinetic_energy', ...
        'enstrophy', 'circulation', 'peak_omega_history', 'max_omega_history', ...
        'peak_speed_history', 'time_vec', 'snapshot_times'};
    for i = 1:numel(keep)
        if isfield(analysis, keep{i})
            summary.(keep{i}) = analysis.(keep{i});
        end
    end
end

function params_summary = summarize_phase_parameters(parameters)
    params_summary = struct();
    keep = {'ic_type', 'bc_case', 'boundary_condition_case', 'bathymetry_scenario', 'Nx', 'Ny', ...
        'Lx', 'Ly', 'dt', 'Tfinal', 'nu', 'num_snapshots', 'num_plot_snapshots', ...
        'animation_num_frames', 'num_animation_frames'};
    for i = 1:numel(keep)
        if isfield(parameters, keep{i})
            params_summary.(keep{i}) = parameters.(keep{i});
        end
    end
end

function persisted = strip_phase1_for_persistence(Results)
    persisted = Results;
    if isfield(persisted, 'children') && isstruct(persisted.children)
        if isfield(persisted.children, 'fd')
            persisted.children.fd = strip_phase_child_for_persistence(persisted.children.fd);
        end
        if isfield(persisted.children, 'spectral')
            persisted.children.spectral = strip_phase_child_for_persistence(persisted.children.spectral);
        end
    end
    if isfield(persisted, 'combined') && isstruct(persisted.combined)
        if isfield(persisted.combined, 'fd_view_summary')
            persisted.combined.fd_view_summary = strip_phase_view_summary_for_persistence(persisted.combined.fd_view_summary);
        end
        if isfield(persisted.combined, 'spectral_view_summary')
            persisted.combined.spectral_view_summary = strip_phase_view_summary_for_persistence(persisted.combined.spectral_view_summary);
        end
    end
    if isfield(persisted, 'ic_study')
        persisted.ic_study = strip_phase1_ic_study_for_persistence(persisted.ic_study);
    end
end

function child = strip_phase_child_for_persistence(child)
    if isfield(child, 'view_summary')
        child.view_summary = strip_phase_view_summary_for_persistence(child.view_summary);
    end
end

function summary = strip_phase_view_summary_for_persistence(summary)
    if ~isstruct(summary)
        return;
    end
    if isfield(summary, 'analysis')
        summary = rmfield(summary, 'analysis');
    end
    if isfield(summary, 'results') && isstruct(summary.results) && isfield(summary.results, 'analysis')
        summary.results.analysis = summarize_analysis(summary.results.analysis);
    end
end

function ic_study = attach_phase1_ic_study_artifacts(ic_study, figure_artifacts)
    if ~isstruct(ic_study)
        ic_study = struct();
        return;
    end
    artifacts = pick_struct(figure_artifacts, {'comparisons'}, struct());
    if ~isfield(ic_study, 'artifacts') || ~isstruct(ic_study.artifacts)
        ic_study.artifacts = struct();
    end
    if isfield(artifacts, 'runtime_per_ic_case')
        ic_study.artifacts.runtime_per_ic_case = artifacts.runtime_per_ic_case;
    end
    if isfield(artifacts, 'runtime_vs_resolution')
        ic_study.artifacts.runtime_vs_resolution = artifacts.runtime_vs_resolution;
    end
end

function artifact_summary = export_phase1_workflow_animations(Results, phase_parameters, phase_settings, paths)
    artifact_summary = struct( ...
        'status', 'not_requested', ...
        'failure_message', '', ...
        'frame_count', 0, ...
        'case_animation_artifacts', struct([]));
    if ~isstruct(Results) || ~logical(pick_value(pick_value(Results, 'phase_config', struct()), 'create_animations', false))
        return;
    end
    panes = pick_value(Results.phase_config, 'workflow_animation_panes', {'evolution', 'streamfunction', 'speed', 'vector', 'contour'});
    panes = phase1_normalize_animation_panes(panes);
    if isempty(panes)
        panes = {'evolution', 'streamfunction', 'speed', 'vector', 'contour'};
    end
    views = collect_phase1_animation_views(Results);
    artifacts = repmat(struct( ...
        'view_id', '', ...
        'case_id', '', ...
        'case_label', '', ...
        'method', '', ...
        'status', 'not_requested', ...
        'failure_message', '', ...
        'pane_mp4s', struct(), ...
        'pane_gifs', struct()), 1, numel(views));
    any_created = false;
    failures = {};
    for i = 1:numel(views)
        artifacts(i).view_id = views(i).view_id;
        artifacts(i).case_id = views(i).case_id;
        artifacts(i).case_label = views(i).case_label;
        artifacts(i).method = views(i).method;
        try
            view_summary = views(i).view_summary;
            analysis = pick_struct(view_summary, {'analysis'}, struct());
            params = pick_struct(view_summary, {'parameters'}, phase_parameters);
            run_cfg = pick_struct(view_summary, {'run_config'}, struct());
            child_paths = pick_struct(view_summary, {'paths'}, struct());
            if isempty(fieldnames(analysis)) || ~isfield(analysis, 'omega_snaps') || size(analysis.omega_snaps, 3) < 2
                artifacts(i).status = 'skipped_no_snapshots';
                continue;
            end
            if isempty(fieldnames(child_paths))
                child_paths = paths;
            end
            child_paths.disable_combined_animation_dir = true;
            child_paths.media_flatten_pane_dirs = true;
            child_visual_root = phase1_workflow_visual_child_root(paths, views(i).method, views(i).case_id, views(i).view_id);
            child_paths.visuals_root = child_visual_root;
            child_paths.figures_root = child_visual_root;
            child_paths.figures = child_visual_root;
            child_paths.figures_evolution_root = child_visual_root;
            child_paths.figures_evolution = child_visual_root;
            child_paths.figures_evolution_evolution = child_visual_root;
            child_paths.figures_evolution_streamfunction = child_visual_root;
            child_paths.figures_evolution_velocity = child_visual_root;
            child_paths.figures_evolution_vector = child_visual_root;
            child_paths.figures_evolution_contour = child_visual_root;
            child_paths.media_animation_panes = child_visual_root;
            child_paths.pane_media_stem_map = phase1_animation_stem_map(views(i).view_id);
            params.create_animations = true;
            params.animation_format = 'gif';
            params.animation_export_format = 'gif';
            params.animation_export_formats = {'gif'};
            params.animation_export_resolution_px = [1600, 1200];
            params.animation_export_dpi = 200;
            params.animation_export_width_in = 8.0;
            params.animation_export_height_in = 6.0;
            params.contour_levels = pick_numeric(pick_value(Results, 'phase_config', struct()), {'contour_levels'}, 36);
            params.animation_num_frames = pick_numeric(params, {'animation_num_frames', 'num_animation_frames'}, ...
                resolve_phase1_animation_frame_count(phase_parameters, phase_settings));
            params.num_animation_frames = params.animation_num_frames;
            local_settings = phase_settings;
            local_settings.animation_enabled = true;
            local_settings.create_animations = true;
            if ~isfield(local_settings, 'media') || ~isstruct(local_settings.media)
                local_settings.media = struct();
            end
            local_settings.media.enabled = true;
            local_settings.media.format = 'gif';
            local_settings.media.formats = {'gif'};
            local_settings.media.export_combined_mp4 = false;
            local_settings.media.export_combined_gif = false;
            local_settings.media.export_pane_mp4s = false;
            local_settings.media.export_pane_gifs = true;
            local_settings.media.export_panes = true;
            local_settings.media.pane_tokens = panes;
            local_settings.media.contour_levels = params.contour_levels;
            local_settings.animation_export_resolution_px = [1600, 1200];
            local_settings.animation_export_dpi = 200;
            local_settings.animation_export_width_in = 8.0;
            local_settings.animation_export_height_in = 6.0;
            local_settings.animation_format = 'gif';
            local_settings.animation_export_format = 'gif';
            local_settings.animation_export_formats = {'gif'};
            local_settings.media.resolution_px = [1600, 1200];
            local_settings.media.dpi = 200;
            local_settings.media.width_in = 8.0;
            local_settings.media.height_in = 6.0;
            exports = ResultsAnimationExporter.export_from_analysis(analysis, params, run_cfg, child_paths, local_settings);
            artifacts(i).pane_mp4s = pick_struct(exports, {'pane_mp4s'}, struct());
            artifacts(i).pane_gifs = pick_struct(exports, {'pane_gifs'}, struct());
            if ~isempty(fieldnames(artifacts(i).pane_mp4s)) || ~isempty(fieldnames(artifacts(i).pane_gifs))
                artifacts(i).status = 'created';
                any_created = true;
            else
                artifacts(i).status = 'skipped_no_outputs';
            end
        catch ME
            artifacts(i).status = 'failed';
            artifacts(i).failure_message = ME.message;
            failures{end + 1} = sprintf('%s: %s', artifacts(i).view_id, ME.message); %#ok<AGROW>
        end
    end
    artifact_summary.case_animation_artifacts = artifacts;
    artifact_summary.frame_count = resolve_phase1_animation_frame_count(phase_parameters, phase_settings);
    if any_created && isempty(failures)
        artifact_summary.status = 'created';
    elseif any_created
        artifact_summary.status = 'created_with_warnings';
        artifact_summary.failure_message = strjoin(failures, ' | ');
    elseif ~isempty(failures)
        artifact_summary.status = 'failed';
        artifact_summary.failure_message = strjoin(failures, ' | ');
    elseif isempty(views)
        artifact_summary.status = 'skipped';
        artifact_summary.failure_message = 'No Phase 1 child views were available for animation export.';
    else
        artifact_summary.status = 'skipped';
        artifact_summary.failure_message = 'No Phase 1 animation outputs were created.';
    end
end

function views = collect_phase1_animation_views(Results)
    views = repmat(struct( ...
        'view_id', '', ...
        'case_id', '', ...
        'case_label', '', ...
        'method', '', ...
        'view_summary', struct()), 1, 0);
    if ~isstruct(Results)
        return;
    end
    ic_study = pick_struct(Results, {'ic_study'}, struct());
    baseline_case_id = pick_text(ic_study, {'baseline_case_id'}, 'baseline');
    baseline_label = pick_text(ic_study, {'baseline_label'}, 'Baseline');
    children = pick_struct(Results, {'children'}, struct());
    if isfield(children, 'fd')
        views = append_phase1_animation_view(views, 'baseline_fd', baseline_case_id, baseline_label, 'FD', ...
            pick_struct(children.fd, {'view_summary'}, struct()));
    end
    if isfield(children, 'spectral')
        views = append_phase1_animation_view(views, 'baseline_spectral', baseline_case_id, baseline_label, 'Spectral', ...
            pick_struct(children.spectral, {'view_summary'}, struct()));
    end
    if isfield(ic_study, 'cases') && isstruct(ic_study.cases)
        for i = 1:numel(ic_study.cases)
            case_id = pick_text(ic_study.cases(i), {'case_id'}, sprintf('case_%02d', i));
            case_label = pick_text(ic_study.cases(i), {'display_label', 'label'}, case_id);
            if isfield(ic_study.cases(i), 'fd')
                views = append_phase1_animation_view(views, sprintf('%s_fd', case_id), case_id, case_label, 'FD', ...
                    pick_struct(ic_study.cases(i).fd, {'view_summary'}, struct()));
            end
            if isfield(ic_study.cases(i), 'spectral')
                views = append_phase1_animation_view(views, sprintf('%s_spectral', case_id), case_id, case_label, 'Spectral', ...
                    pick_struct(ic_study.cases(i).spectral, {'view_summary'}, struct()));
            end
        end
    end
end

function views = append_phase1_animation_view(views, view_id, case_id, case_label, method_label, view_summary)
    if ~isstruct(view_summary) || isempty(fieldnames(view_summary))
        return;
    end
    views(end + 1) = struct( ... %#ok<AGROW>
        'view_id', char(string(view_id)), ...
        'case_id', char(string(case_id)), ...
        'case_label', char(string(case_label)), ...
        'method', char(string(method_label)), ...
        'view_summary', view_summary);
end

function panes = phase1_normalize_animation_panes(raw_panes)
    if ischar(raw_panes) || isstring(raw_panes)
        raw_panes = cellstr(string(raw_panes));
    end
    panes = {};
    for i = 1:numel(raw_panes)
        token = lower(strtrim(char(string(raw_panes{i}))));
        switch token
            case {'vorticity', 'omega'}
                token = 'evolution';
            case {'velocity', 'velocity_magnitude'}
                token = 'speed';
            case 'streamline'
                token = 'streamlines';
        end
        if any(strcmp(token, {'evolution', 'streamfunction', 'speed', 'vector', 'contour', 'streamlines'})) && ~any(strcmp(token, panes))
            panes{end + 1} = token; %#ok<AGROW>
        end
    end
end

function stem_map = phase1_animation_stem_map(view_id)
    token = compact_phase_label_token(view_id);
    stem_map = struct( ...
        'evolution', sprintf('%s_vorticity_evolution', token), ...
        'streamfunction', sprintf('%s_streamfunction_evolution', token), ...
        'speed', sprintf('%s_velocity_evolution', token), ...
        'vector', sprintf('%s_vector_evolution', token), ...
        'contour', sprintf('%s_contour_evolution', token), ...
        'streamlines', sprintf('%s_streamlines_evolution', token));
end

function ic_study = strip_phase1_ic_study_for_persistence(ic_study)
    if ~isstruct(ic_study)
        return;
    end
    if ~isfield(ic_study, 'cases') || ~isstruct(ic_study.cases)
        return;
    end
    for i = 1:numel(ic_study.cases)
        if isfield(ic_study.cases(i), 'fd')
            ic_study.cases(i).fd = strip_phase1_ic_study_method_for_persistence(ic_study.cases(i).fd);
        end
        if isfield(ic_study.cases(i), 'spectral')
            ic_study.cases(i).spectral = strip_phase1_ic_study_method_for_persistence(ic_study.cases(i).spectral);
        end
    end
end

function method_result = strip_phase1_ic_study_method_for_persistence(method_result)
    if ~isstruct(method_result)
        return;
    end
    if isfield(method_result, 'view_summary')
        method_result.view_summary = strip_phase_view_summary_for_persistence(method_result.view_summary);
    end
end

function [artifacts, reference_calibration] = generate_phase1_plots(Results, paths)
    artifacts = struct('comparisons', struct(), 'diagnostics', struct());
    reference_calibration = struct();
    m = Results.metrics;
    labels = categorical({'FD', 'Spectral'});
    labels = reordercats(labels, {'FD', 'Spectral'});
    light_colors = ResultsPlotDispatcher.default_light_colors();
    comparisons_dir = pick_text(paths, {'figures_comparisons', 'figures'}, pick_text(paths, {'figures'}, ''));
    diagnostics_dir = comparisons_dir;
    delete_phase_figure_if_present(comparisons_dir, 'phase1_runtime_vs_mismatch');

    fig = build_phase1_cross_method_field_comparison_figure(Results, light_colors);
    artifacts.comparisons.cross_method_mismatch = save_phase_figure(fig, comparisons_dir, 'phase1_cross_method_mismatch');

    fig = figure('Visible', 'off', 'Color', 'w');
    bar(labels, [m.FD.kinetic_energy_drift, m.FD.enstrophy_drift, m.FD.circulation_drift; ...
        m.Spectral.kinetic_energy_drift, m.Spectral.enstrophy_drift, m.Spectral.circulation_drift]);
    xlabel('Method');
    ylabel('Relative drift');
    legend({'Kinetic energy', 'Enstrophy', 'Circulation'}, 'Location', 'best');
    title('Phase 1 Conservation Drift');
    artifacts.comparisons.conservation_drift = save_phase_figure(fig, diagnostics_dir, 'phase1_conservation_drift');
    artifacts.diagnostics.conservation_drift = artifacts.comparisons.conservation_drift;

    fig = figure('Visible', 'off', 'Color', 'w');
    bar(labels, [m.FD.peak_vorticity_ratio, m.FD.centroid_drift, m.FD.core_anisotropy_final; ...
        m.Spectral.peak_vorticity_ratio, m.Spectral.centroid_drift, m.Spectral.core_anisotropy_final]);
    xlabel('Method');
    ylabel('Metric value');
    legend({'Peak ratio', 'Centroid drift', 'Core anisotropy'}, 'Location', 'best');
    title('Phase 1 vortex preservation');
    artifacts.comparisons.vortex_preservation = save_phase_figure(fig, diagnostics_dir, 'phase1_vortex_preservation');
    artifacts.diagnostics.vortex_preservation = artifacts.comparisons.vortex_preservation;

    [case_labels, case_runtime_matrix] = build_phase1_ic_runtime_chart_data(Results);
    if ~isempty(case_labels) && ~isempty(case_runtime_matrix)
        fig = figure('Visible', 'off', 'Color', 'w');
        bars = bar(case_runtime_matrix, 'grouped');
        ax = gca;
        ax.XTick = 1:numel(case_labels);
        ax.XTickLabel = case_labels;
        ax.XTickLabelRotation = 20;
        xlabel('Initial Condition Case');
        ylabel('Computational time (s)');
        if numel(bars) >= 2
            legend(ax, bars(1:2), {'FD', 'SM'}, 'Location', 'northwest');
        end
        title('Method Comparison: Time per Initial Condition');
        grid(ax, 'on');
        for series_idx = 1:size(case_runtime_matrix, 2)
            x_positions = (1:size(case_runtime_matrix, 1)) + ((series_idx - 1.5) * 0.28);
            for row_idx = 1:size(case_runtime_matrix, 1)
                value = case_runtime_matrix(row_idx, series_idx);
                if isfinite(value)
                    text(x_positions(row_idx), value, sprintf(' %.2g', value), ...
                        'Rotation', 90, 'VerticalAlignment', 'bottom', 'FontSize', 8);
                end
            end
        end
        artifacts.comparisons.runtime_per_ic_case = save_phase_figure(fig, comparisons_dir, 'phase1_runtime_per_ic_case');
    end

    [resolution_labels, resolution_runtime_matrix] = build_phase1_resolution_runtime_chart_data(Results);
    if ~isempty(resolution_labels) && ~isempty(resolution_runtime_matrix)
        fig = figure('Visible', 'off', 'Color', 'w');
        bars = bar(resolution_runtime_matrix, 'grouped');
        ax = gca;
        ax.XTick = 1:numel(resolution_labels);
        ax.XTickLabel = resolution_labels;
        ylabel('Computational time (s)');
        xlabel('Resolution $\kappa_N=N_x\times N_y=N^2$', 'Interpreter', 'latex');
        if numel(bars) >= 2
            legend(ax, bars(1:2), {'FD', 'SM'}, 'Location', 'northwest');
        end
        title('Computational Time vs Resolution');
        grid(ax, 'on');
        for series_idx = 1:size(resolution_runtime_matrix, 2)
            x_positions = (1:size(resolution_runtime_matrix, 1)) + ((series_idx - 1.5) * 0.28);
            for row_idx = 1:size(resolution_runtime_matrix, 1)
                value = resolution_runtime_matrix(row_idx, series_idx);
                if isfinite(value)
                    text(x_positions(row_idx), value, sprintf(' %.2g', value), ...
                        'Rotation', 90, 'VerticalAlignment', 'bottom', 'FontSize', 8);
                end
            end
        end
        artifacts.comparisons.runtime_vs_resolution = save_phase_figure(fig, comparisons_dir, 'phase1_runtime_vs_resolution');
    end

    if isfield(Results, 'error_vs_time') && isstruct(Results.error_vs_time) && ...
            isfield(Results.error_vs_time, 'time_s') && ~isempty(Results.error_vs_time.time_s)
        fig = build_phase1_error_vs_time_figure(Results.error_vs_time, light_colors);
        artifacts.comparisons.error_vs_time = save_phase_figure(fig, comparisons_dir, 'phase1_error_vs_time');
    end

    reference_fig = build_phase1_reference_evolution_grid_figure(Results);
    if ~isempty(reference_fig) && isgraphics(reference_fig)
        artifacts.comparisons.reference_evolution_grid = save_phase_figure(reference_fig, comparisons_dir, ...
            'phase1_reference_evolution_grid');
    end

    [reference_calibration, reference_artifacts] = export_phase1_reference_calibration_artifacts(Results, comparisons_dir);
    if ~isempty(fieldnames(reference_artifacts))
        artifact_fields = fieldnames(reference_artifacts);
        for idx = 1:numel(artifact_fields)
            artifacts.comparisons.(artifact_fields{idx}) = reference_artifacts.(artifact_fields{idx});
        end
    end

    fig = build_phase1_overview_triptych(Results, light_colors);
    artifacts.comparisons.overview_triptych = save_phase_figure(fig, comparisons_dir, 'phase1_overview_triptych');
end

function artifacts = generate_mesh_convergence_plots(Results, paths)
    artifacts = struct('comparisons', struct(), 'diagnostics', struct(), ...
        'levels', struct('fd', struct([]), 'spectral', struct([])));
    light_colors = ResultsPlotDispatcher.default_light_colors();
    comparisons_dir = pick_text(paths, {'figures_comparisons', 'figures'}, pick_text(paths, {'figures'}, ''));
    phase_cfg = pick_struct(Results, {'phase_config'}, struct());
    save_level_visuals = logical(pick_value(phase_cfg, 'save_level_visuals', true));
    mesh_save_settings = struct( ...
        'figure_save_png', false, ...
        'figure_save_pdf', false, ...
        'figure_save_fig', true, ...
        'figure_dpi', 200);
    fig = build_phase1_convergence_comparison_figure(Results, light_colors);
    artifacts.comparisons.convergence_comparison = save_phase_figure(fig, comparisons_dir, 'mesh_convergence_comparison', mesh_save_settings);
    fig = build_mesh_convergence_runtime_vs_resolution_figure(Results, light_colors);
    artifacts.comparisons.runtime_vs_resolution = save_phase_figure(fig, comparisons_dir, 'mesh_convergence_runtime_vs_resolution', mesh_save_settings);
    fig = build_phase1_adaptive_timestep_convergence_figure(Results, light_colors);
    artifacts.comparisons.adaptive_timestep_convergence = save_phase_figure(fig, comparisons_dir, 'mesh_convergence_adaptive_timestep', mesh_save_settings);
    if isfield(Results, 'error_vs_time') && isstruct(Results.error_vs_time) && ...
            isfield(Results.error_vs_time, 'time_s') && ~isempty(Results.error_vs_time.time_s)
        fig = build_phase1_error_vs_time_figure(Results.error_vs_time, light_colors);
        artifacts.comparisons.selected_mesh_error_vs_time = save_phase_figure(fig, comparisons_dir, ...
            'mesh_convergence_selected_mesh_error_vs_time', mesh_save_settings);
    end
    fig = build_mesh_convergence_cross_method_field_figure(Results, light_colors);
    artifacts.comparisons.cross_method_fields = save_phase_figure(fig, comparisons_dir, ...
        'mesh_convergence_cross_method_fields', mesh_save_settings);
    fig = build_mesh_convergence_overview_triptych(Results, light_colors);
    artifacts.comparisons.overview_triptych = save_phase_figure(fig, comparisons_dir, 'mesh_convergence_overview_triptych', mesh_save_settings);
    if save_level_visuals
        artifacts.levels.fd = export_mesh_convergence_level_vorticity_figures( ...
            pick_struct(Results.children, {'fd'}, struct()), paths, 'FD', mesh_save_settings);
        artifacts.levels.spectral = export_mesh_convergence_level_vorticity_figures( ...
            pick_struct(Results.children, {'spectral'}, struct()), paths, 'Spectral', mesh_save_settings);
    end
end

function artifacts = export_mesh_convergence_level_vorticity_figures(child_struct, paths, method_label, mesh_save_settings)
    artifacts = struct([]);
    if nargin < 4 || ~isstruct(mesh_save_settings)
        mesh_save_settings = struct('figure_save_png', false, 'figure_save_pdf', false, 'figure_save_fig', true, 'figure_dpi', 200);
    end
    convergence_output = pick_struct(child_struct, {'convergence_output'}, struct());
    results = pick_struct(convergence_output, {'results'}, struct());
    run_records = pick_value(results, 'run_records', struct([]));
    if ~isstruct(run_records) || isempty(run_records)
        return;
    end
    artifacts = repmat(struct( ...
        'mesh_level_label', '', ...
        'mesh_n', NaN, ...
        'summary_path', '', ...
        'vorticity_3x3', struct()), 1, numel(run_records));
    for i = 1:numel(run_records)
        summary_path = pick_text(run_records(i), {'mesh_level_summary_path'}, '');
        mesh_n = pick_numeric(run_records(i), {'Nx', 'Ny'}, NaN);
        level_label = pick_text(run_records(i), {'mesh_level_label'}, phase1_mesh_level_dir_name(i, mesh_n));
        artifacts(i).mesh_level_label = level_label;
        artifacts(i).mesh_n = mesh_n;
        artifacts(i).summary_path = summary_path;
        if isempty(summary_path) || exist(summary_path, 'file') ~= 2
            continue;
        end
        try
            loaded = load(summary_path, 'analysis');
        catch
            continue;
        end
        analysis = pick_struct(loaded, {'analysis'}, struct());
        if ~isstruct(analysis) || isempty(fieldnames(analysis))
            continue;
        end
        level_dir = fullfile(pick_text(paths, {'visuals_root', 'figures_root'}, ''), method_label);
        figure_stem = sprintf('%s_Vorticity_3x3', phase1_mesh_level_dir_name(i, mesh_n));
        fig = build_mesh_level_vorticity_gallery_figure(analysis, method_label, level_label, mesh_n);
        artifacts(i).vorticity_3x3 = save_phase_figure(fig, level_dir, figure_stem, mesh_save_settings);
    end
end

function fig = build_mesh_level_vorticity_gallery_figure(analysis, method_label, level_label, mesh_n)
    omega_cube = extract_omega_snapshot_cube(analysis);
    snapshot_count = size(omega_cube, 3);
    selected_indices = select_mesh_vorticity_gallery_indices(snapshot_count);
    if isempty(selected_indices)
        selected_indices = 1:min(snapshot_count, 1);
    end
    snapshot_times = resolve_snapshot_time_vector(analysis, snapshot_count);
    [X, Y] = analysis_grid(analysis, size(omega_cube(:, :, 1)));
    x = X(1, :);
    y = Y(:, 1);
    finite_vals = omega_cube(isfinite(omega_cube));
    cmin = -1;
    cmax = 1;
    if ~isempty(finite_vals)
        cmin = min(finite_vals);
        cmax = max(finite_vals);
        if ~(isfinite(cmin) && isfinite(cmax) && cmax > cmin)
            cmin = -1;
            cmax = 1;
        end
    end

    mesh_n_display = round(double(mesh_n));
    if ~isfinite(mesh_n_display)
        mesh_n_display = size(omega_cube, 2);
    end
    fig = figure('Visible', 'off', 'Color', 'w', 'Units', 'pixels', 'Position', [120 120 1180 920]);
    layout = tiledlayout(fig, 3, 3, 'TileSpacing', 'compact', 'Padding', 'compact');
    title(layout, sprintf('%s | %s | Mesh %dx%d | Vorticity 3x3', method_label, level_label, mesh_n_display, mesh_n_display));
    for k = 1:9
        ax = nexttile(layout, k);
        if k <= numel(selected_indices)
            frame_index = selected_indices(k);
            imagesc(ax, x, y, double(omega_cube(:, :, frame_index)));
            set(ax, 'YDir', 'normal');
            axis(ax, 'equal');
            axis(ax, 'tight');
            colormap(ax, turbo);
            clim(ax, [cmin cmax]);
            title(ax, sprintf('t = %.3g s', snapshot_times(frame_index)));
            xlabel(ax, 'x');
            ylabel(ax, 'y');
            grid(ax, 'on');
            box(ax, 'on');
        else
            axis(ax, 'off');
        end
    end
end

function selected_indices = select_mesh_vorticity_gallery_indices(snapshot_count)
    if nargin < 1 || ~isfinite(double(snapshot_count)) || snapshot_count <= 0
        selected_indices = zeros(1, 0);
        return;
    end
    snapshot_count = round(double(snapshot_count));
    target_count = min(snapshot_count, 9);
    selected_indices = unique(round(linspace(1, snapshot_count, target_count)), 'stable');
    selected_indices = selected_indices(selected_indices >= 1 & selected_indices <= snapshot_count);
    if isempty(selected_indices)
        selected_indices = 1;
    end
end

function fig = build_phase1_cross_method_field_comparison_figure(Results, colors)
    if nargin < 2 || ~isstruct(colors)
        colors = ResultsPlotDispatcher.default_light_colors();
    end
    fig = figure('Visible', 'off', 'Color', 'w', 'Units', 'pixels', 'Position', [120 120 1480 820]);
    layout = tiledlayout(fig, 2, 3, 'TileSpacing', 'compact', 'Padding', 'compact');
    title(layout, 'Phase 1 Cross-Method Field Mismatch');
    metrics = pick_struct(Results, {'metrics'}, struct());
    metric_specs = { ...
        {'cross_method_mismatch_l2', 'Vorticity L2 mismatch', 'Relative error'}, ...
        {'cross_method_streamfunction_relative_l2_mismatch', 'Streamfunction L2 mismatch', 'Relative error'}, ...
        {'cross_method_speed_relative_l2_mismatch', 'Speed L2 mismatch', 'Relative error'}, ...
        {'cross_method_velocity_vector_relative_l2_mismatch', 'Velocity-vector L2 mismatch', 'Relative error'}, ...
        {'cross_method_streamline_direction_relative_l2_mismatch', 'Streamline-direction L2 mismatch', 'Relative error'}};
    for i = 1:numel(metric_specs)
        ax = nexttile(layout, i);
        field_name = metric_specs{i}{1};
        values = [ ...
            pick_numeric(pick_struct(metrics, {'FD'}, struct()), {field_name}, NaN), ...
            pick_numeric(pick_struct(metrics, {'Spectral'}, struct()), {field_name}, NaN)];
        plot_cross_method_field_summary_axis(ax, values, metric_specs{i}{2}, metric_specs{i}{3}, colors);
    end
    ax_summary = nexttile(layout, 6);
    axis(ax_summary, 'off');
    summary = pick_struct(metrics, {'summary'}, struct());
    summary_lines = { ...
        sprintf('Mean vorticity L2: %.3e', pick_numeric(summary, {'mean_cross_method_mismatch_l2'}, NaN)); ...
        sprintf('Mean streamfunction L2: %.3e', pick_numeric(summary, {'mean_cross_method_streamfunction_mismatch_l2'}, NaN)); ...
        sprintf('Mean speed L2: %.3e', pick_numeric(summary, {'mean_cross_method_speed_mismatch_l2'}, NaN)); ...
        sprintf('Mean velocity-vector L2: %.3e', pick_numeric(summary, {'mean_cross_method_velocity_vector_mismatch_l2'}, NaN)); ...
        sprintf('Mean streamline-direction L2: %.3e', pick_numeric(summary, {'mean_cross_method_streamline_direction_mismatch_l2'}, NaN))};
    text(ax_summary, 0.02, 0.98, summary_lines, 'Units', 'normalized', ...
        'HorizontalAlignment', 'left', 'VerticalAlignment', 'top', 'FontSize', 11, 'Interpreter', 'none');
end

function fig = build_mesh_convergence_cross_method_field_figure(Results, colors)
    if nargin < 2 || ~isstruct(colors)
        colors = ResultsPlotDispatcher.default_light_colors();
    end
    fig = figure('Visible', 'off', 'Color', 'w', 'Units', 'pixels', 'Position', [120 120 860 420]);
    ax = axes(fig); %#ok<LAXES>
    plot_mesh_cross_method_summary_axis(ax, Results, colors);
end

function plot_mesh_cross_method_summary_axis(ax, Results, colors)
    summary = pick_struct(pick_struct(Results, {'metrics'}, struct()), {'summary'}, struct());
    values = [ ...
        pick_numeric(summary, {'mean_cross_method_speed_mismatch_l2'}, NaN), ...
        pick_numeric(summary, {'mean_cross_method_mismatch_l2'}, NaN), ...
        pick_numeric(summary, {'mean_cross_method_streamfunction_mismatch_l2'}, NaN)];
    labels = categorical({'Speed', 'Vorticity', 'Streamfunction'});
    labels = reordercats(labels, {'Speed', 'Vorticity', 'Streamfunction'});
    bars = bar(ax, labels, values, 'FaceColor', 'flat');
    if ~isempty(bars)
        bars.CData = [0.10 0.45 0.88; 0.88 0.30 0.10; 0.10 0.58 0.34];
        bars.EdgeColor = [0.15 0.15 0.15];
    end
    for i = 1:numel(values)
        if isfinite(values(i))
            text(ax, i, values(i), sprintf('%.3g', values(i)), ...
                'HorizontalAlignment', 'center', 'VerticalAlignment', 'bottom', ...
                'FontSize', 9, 'Interpreter', 'none');
        end
    end
    ylabel(ax, {'Relative $L_2$ mismatch', ...
        '$\epsilon_{L_2}=\|q_{\mathrm{FD}}-q_{\mathrm{SM}}\|_2/\|q_{\mathrm{SM}}\|_2$'}, ...
        'Interpreter', 'latex');
    title(ax, 'Selected-mesh cross-method field mismatch');
    grid(ax, 'on');
    box(ax, 'on');
end

function plot_cross_method_field_summary_axis(ax, values, title_text, ylabel_text, colors)
    labels = categorical({'FD', 'Spectral'});
    labels = reordercats(labels, {'FD', 'Spectral'});
    bars = bar(ax, labels, values(:), 'FaceColor', 'flat');
    if numel(values) >= 1
        bars.CData(1, :) = colors.primary;
    end
    if numel(values) >= 2
        bars.CData(2, :) = colors.tertiary;
    end
    ylabel(ax, ylabel_text);
    title(ax, title_text);
    grid(ax, 'on');
end

function fig = build_phase1_reference_evolution_grid_figure(Results)
    fig = [];
    ref_cfg = resolve_phase1_reference_evolution_config(Results);
    asset_path = pick_text(ref_cfg, {'asset_path'}, '');
    if ~phase1_reference_workflow_publication_enabled(ref_cfg) || isempty(asset_path) || exist(asset_path, 'file') ~= 2
        return;
    end
    try
        fig = ReferenceEvolutionCalibration.build_reference_grid_figure(ref_cfg, ...
            sprintf('Phase 1 Reference Evolution Grid | %s', ...
            pick_text(ref_cfg, {'asset_name'}, 'Reference GIF')));
    catch
        if ~isempty(fig) && isgraphics(fig)
            close(fig);
        end
        fig = [];
    end
end

function [reference_calibration, artifact_struct] = export_phase1_reference_calibration_artifacts(Results, comparisons_dir)
    reference_calibration = struct();
    artifact_struct = struct();
    ref_cfg = resolve_phase1_reference_evolution_config(Results);
    asset_path = pick_text(ref_cfg, {'asset_path'}, '');
    if ~phase1_reference_workflow_publication_enabled(ref_cfg) || isempty(asset_path) || exist(asset_path, 'file') ~= 2
        return;
    end

    method_specs = { ...
        {'fd', 'FD', 'fd'}, ...
        {'spectral', 'Spectral', 'spectral'}};
    for i = 1:numel(method_specs)
        method_key = method_specs{i}{1};
        method_label = method_specs{i}{2};
        method_token = method_specs{i}{3};
        [entry, sim_gallery, metrics, reference] = build_phase1_reference_calibration_entry(Results, ref_cfg, method_key, method_label);
        if isempty(fieldnames(entry))
            continue;
        end

        sim_stem = sprintf('phase1_reference_simulation_grid_%s', method_token);
        cmp_stem = sprintf('phase1_reference_vs_simulation_%s', method_token);
        sim_fig = ReferenceEvolutionCalibration.build_simulation_grid_figure(sim_gallery, ...
            sprintf('%s | %s', method_label, sim_gallery.label_text));
        cmp_fig = ReferenceEvolutionCalibration.build_reference_vs_simulation_figure(reference, sim_gallery, metrics, ...
            sprintf('%s | Reference vs Simulation', method_label));
        if ~isempty(sim_fig) && isgraphics(sim_fig)
            entry.artifacts.simulation_grid = save_phase_figure(sim_fig, comparisons_dir, sim_stem);
            artifact_struct.(sprintf('%s_reference_simulation_grid', method_token)) = entry.artifacts.simulation_grid;
        end
        if ~isempty(cmp_fig) && isgraphics(cmp_fig)
            entry.artifacts.reference_vs_simulation = save_phase_figure(cmp_fig, comparisons_dir, cmp_stem);
            artifact_struct.(sprintf('%s_reference_vs_simulation', method_token)) = entry.artifacts.reference_vs_simulation;
        end
        reference_calibration.(method_key) = entry;
    end
end

function [entry, sim_gallery, metrics, reference] = build_phase1_reference_calibration_entry(Results, ref_cfg, method_key, method_label)
    entry = struct();
    sim_gallery = struct();
    metrics = struct();
    reference = struct();

    analysis = phase1_reference_calibration_analysis(Results, method_key);
    if ~isstruct(analysis) || isempty(fieldnames(analysis))
        return;
    end

    reference = ReferenceEvolutionCalibration.load_reference(ref_cfg);
    if isempty(reference.selected_indices)
        return;
    end

    sim_gallery = ReferenceEvolutionCalibration.build_simulation_gallery(analysis, ref_cfg, ...
        sprintf('%s GIF-Matched Evolution Grid', method_label), method_label);
    if isempty(sim_gallery.selected_indices)
        return;
    end

    preset_id = resolve_phase1_reference_calibration_preset_id(Results, method_key);
    grid_n = size(sim_gallery.omega_selected, 2);
    metrics = ReferenceEvolutionCalibration.compute_metrics(reference, sim_gallery, struct( ...
        'preset_id', preset_id, ...
        'method_label', method_label, ...
        'grid_n', grid_n));
    entry = struct( ...
        'enabled', true, ...
        'preset_id', preset_id, ...
        'method_label', method_label, ...
        'reference_asset_path', pick_text(reference, {'asset_path'}, ''), ...
        'reference_asset_name', pick_text(reference, {'asset_name'}, ''), ...
        'grid_n', grid_n, ...
        'frame_count', numel(sim_gallery.selected_indices), ...
        'reference_selected_indices', reference.selected_indices, ...
        'simulation_selected_indices', sim_gallery.selected_indices, ...
        'snapshot_times_s', sim_gallery.snapshot_times, ...
        'plot_box', reference.plot_box, ...
        'frame_metrics', metrics.frame_metrics, ...
        'summary', metrics.summary, ...
        'artifacts', struct());
end

function analysis = phase1_reference_calibration_analysis(Results, method_key)
    analysis = struct();
    child = pick_struct(pick_struct(Results, {'children'}, struct()), {method_key}, struct());
    analysis = pick_struct(pick_struct(pick_struct(child, {'view_summary'}, struct()), {'results'}, struct()), {'analysis'}, struct());
    if ~phase1_reference_analysis_has_snapshot_cube(analysis)
        analysis = pick_struct(pick_struct(pick_struct(child, {'evolution_output'}, struct()), {'results'}, struct()), {'analysis'}, struct());
    end
    if ~phase1_reference_analysis_has_snapshot_cube(analysis)
        combined_key = sprintf('%s_view_summary', method_key);
        analysis = pick_struct(pick_struct(pick_struct(pick_struct(Results, {'combined'}, struct()), {combined_key}, struct()), {'results'}, struct()), {'analysis'}, struct());
    end
    if ~phase1_reference_analysis_has_snapshot_cube(analysis)
        analysis = load_phase1_reference_analysis_from_path( ...
            pick_text(pick_struct(pick_struct(child, {'view_summary'}, struct()), {'results'}, struct()), {'data_path'}, ''));
    end
    if ~phase1_reference_analysis_has_snapshot_cube(analysis)
        analysis = load_phase1_reference_analysis_from_path( ...
            pick_text(pick_struct(pick_struct(child, {'evolution_output'}, struct()), {'results'}, struct()), {'data_path'}, ''));
    end
    if ~phase1_reference_analysis_has_snapshot_cube(analysis)
        analysis = struct();
    end
end

function preset_id = resolve_phase1_reference_calibration_preset_id(Results, method_key)
    preset_id = 'reference_spiral_stretched_gaussian';
    analysis = phase1_reference_calibration_analysis(Results, method_key);
    ic_type = pick_text(analysis, {'ic_type'}, '');
    if isempty(ic_type)
        child = pick_struct(pick_struct(Results, {'children'}, struct()), {method_key}, struct());
        ic_type = pick_text(pick_struct(pick_struct(child, {'view_summary'}, struct()), {'run_config'}, struct()), {'ic_type'}, '');
    end
    if strcmpi(ic_type, 'elliptical_vortex')
        preset_id = 'reference_spiral_elliptical_vortex';
    end
end

function ref_cfg = resolve_phase1_reference_evolution_config(Results)
    ref_cfg = pick_struct(pick_struct(pick_struct(Results, {'phase_config'}, struct()), {'ic_study'}, struct()), ...
        {'reference_evolution'}, struct());
    if isempty(fieldnames(ref_cfg))
        parent_phase1 = pick_struct(pick_struct(Results, {'parent_parameters'}, struct()), {'phase1'}, struct());
        parent_ic_study = pick_struct(parent_phase1, {'ic_study'}, struct());
        ref_cfg = pick_struct(parent_ic_study, {'reference_evolution'}, struct());
    end
end

function tf = phase1_reference_workflow_publication_enabled(ref_cfg)
    tf = logical(pick_value(ref_cfg, 'enabled', false)) && ...
        logical(pick_value(ref_cfg, 'publish_in_phase_workflow', false));
end

function analysis = load_phase1_reference_analysis_from_path(data_path)
    analysis = struct();
    if isempty(data_path) || exist(data_path, 'file') ~= 2
        return;
    end
    try
        loaded = load(data_path, 'analysis', 'ResultsForSave');
    catch
        return;
    end
    analysis = pick_struct(loaded, {'analysis'}, struct());
    if ~isempty(fieldnames(analysis))
        return;
    end
    results_struct = pick_struct(loaded, {'ResultsForSave'}, struct());
    analysis = pick_struct(results_struct, {'analysis'}, struct());
end

function tf = phase1_reference_analysis_has_snapshot_cube(analysis)
    tf = isstruct(analysis) && isfield(analysis, 'omega_snaps') && ~isempty(analysis.omega_snaps);
end

function fig = build_phase1_error_vs_time_figure(error_payload, colors)
    if nargin < 2 || ~isstruct(colors)
        colors = ResultsPlotDispatcher.default_light_colors();
    end
    fig = figure('Visible', 'off', 'Color', 'w', 'Units', 'pixels', 'Position', [120 120 1380 430]);
    layout = tiledlayout(fig, 1, 3, 'TileSpacing', 'compact', 'Padding', 'compact');
    title(layout, 'Selected-Mesh Cross-Method Error Over Time');
    time_s = pick_value(error_payload, 'time_s', []);
    plot_error_metric_axis(nexttile(layout, 1), time_s, ...
        {pick_value(error_payload, 'fd_relative_l2_mismatch', nan(size(time_s))), ...
         pick_value(error_payload, 'fd_relative_rmse', nan(size(time_s)))}, ...
        {'Relative L2', 'Relative RMSE'}, [colors.primary; colors.secondary], ...
        'FD-grid directional errors', 'Relative error');
    plot_error_metric_axis(nexttile(layout, 2), time_s, ...
        {pick_value(error_payload, 'spectral_relative_l2_mismatch', nan(size(time_s))), ...
         pick_value(error_payload, 'spectral_relative_rmse', nan(size(time_s)))}, ...
        {'Relative L2', 'Relative RMSE'}, [colors.tertiary; colors.secondary], ...
        'SM-grid directional errors', 'Relative error');
    plot_error_metric_axis(nexttile(layout, 3), time_s, ...
        {pick_value(error_payload, 'rmse', nan(size(time_s)))}, ...
        {'RMSE'}, [0.35 0.20 0.70], 'Vorticity RMSE between FD and SM', ...
        {'$\mathrm{RMSE}$', '$\mathrm{RMSE}=\sqrt{\frac{1}{M}\sum_i(\omega_i^{\mathrm{FD}}-\omega_i^{\mathrm{SM}})^2}$'});
end

function plot_error_metric_axis(ax, time_s, series_list, labels, color_rows, title_text, y_label, normalize_series)
    if nargin < 8
        normalize_series = false;
    end
    cla(ax);
    hold(ax, 'on');
    if isempty(time_s)
        text(ax, 0.5, 0.5, 'No error data available.', 'HorizontalAlignment', 'center', 'VerticalAlignment', 'middle');
        axis(ax, 'off');
        return;
    end
    if isvector(color_rows) && numel(color_rows) == 3
        color_rows = reshape(color_rows, 1, 3);
    end
    for i = 1:numel(series_list)
        color_idx = min(i, size(color_rows, 1));
        series_values = series_list{i};
        if normalize_series
            finite_values = abs(double(series_values(isfinite(series_values))));
            if ~isempty(finite_values)
                scale_value = max(finite_values, [], 'omitnan');
                if isfinite(scale_value) && scale_value > 0
                    series_values = series_values ./ scale_value;
                end
            end
        end
        plot(ax, time_s, series_values, 'LineWidth', 1.6, ...
            'Color', color_rows(color_idx, :), 'DisplayName', labels{i});
    end
    hold(ax, 'off');
    title(ax, title_text);
    xlabel(ax, 'Evolution snapshot time (s)');
    if iscell(y_label) || (ischar(y_label) && contains(y_label, '$')) || (isstring(y_label) && contains(y_label, "$"))
        ylabel(ax, y_label, 'Interpreter', 'latex');
    else
        ylabel(ax, y_label);
    end
    grid(ax, 'on');
    box(ax, 'on');
    legend(ax, 'Location', 'best');
end

function saved = save_phase_figure(fig, output_dir, stem, varargin)
    if exist(output_dir, 'dir') ~= 7
        mkdir(output_dir);
    end
    save_settings = struct( ...
        'figure_save_png', true, ...
        'figure_save_pdf', false, ...
        'figure_save_fig', true, ...
        'figure_dpi', 200);
    if nargin >= 4 && isstruct(varargin{1}) && ~isempty(fieldnames(varargin{1}))
        save_settings = merge_structs(save_settings, varargin{1});
    end
    outputs = ResultsPlotDispatcher.save_figure_bundle(fig, fullfile(output_dir, stem), save_settings);
    saved = struct( ...
        'fig', pick_text(outputs, {'fig_path'}, ''), ...
        'png', pick_text(outputs, {'png_path'}, ''), ...
        'pdf', pick_text(outputs, {'pdf_path'}, ''));
    close(fig);
end

function fig = build_mesh_convergence_runtime_vs_resolution_figure(Results, colors)
    if nargin < 2 || ~isstruct(colors)
        colors = ResultsPlotDispatcher.default_light_colors();
    end
    fig = figure('Visible', 'off', 'Color', 'w', 'Units', 'pixels', 'Position', [120 120 860 420]);
    ax = axes(fig); %#ok<LAXES>
    plot_mesh_runtime_vs_resolution_axes(ax, Results, colors, true);
end

function fig = build_mesh_convergence_overview_triptych(Results, colors)
    if nargin < 2 || ~isstruct(colors)
        colors = ResultsPlotDispatcher.default_light_colors();
    end
    fig = figure('Visible', 'off', 'Color', 'w', 'Units', 'pixels', 'Position', [120 120 1440 320]);
    layout = tiledlayout(fig, 1, 3, 'TileSpacing', 'compact', 'Padding', 'compact');
    title(layout, 'Mesh Convergence Overview');

    ax = nexttile(layout, 1);
    plot_phase1_convergence_axes(ax, Results, colors, false);

    ax = nexttile(layout, 2);
    plot_mesh_runtime_vs_resolution_axes(ax, Results, colors, false, true);

    ax = nexttile(layout, 3);
    plot_mesh_cfl_dt_adv_axes(ax, Results, colors);
end

function plot_mesh_runtime_vs_resolution_axes(ax, Results, colors, use_detailed_title, use_log_y)
    if nargin < 4
        use_detailed_title = true;
    end
    if nargin < 5
        use_log_y = false;
    end
    [resolution_labels, resolution_runtime_matrix] = build_phase1_resolution_runtime_chart_data(Results);
    if isempty(resolution_labels) || isempty(resolution_runtime_matrix)
        text(ax, 0.5, 0.5, 'No runtime-vs-resolution data available.', ...
            'HorizontalAlignment', 'center', 'VerticalAlignment', 'middle');
        axis(ax, 'off');
        return;
    end

    bars = bar(ax, resolution_runtime_matrix, 'grouped');
    if numel(bars) >= 2
        bars(1).FaceColor = colors.primary;
        bars(1).DisplayName = 'FD';
        bars(2).FaceColor = colors.tertiary;
        bars(2).DisplayName = 'SM';
    end
    ax.XTick = 1:numel(resolution_labels);
    ax.XTickLabel = resolution_labels;
    ylabel(ax, 'Computational time (s)');
    xlabel(ax, 'Resolution $\kappa_N=N_x\times N_y=N^2$', 'Interpreter', 'latex');
    if use_log_y
        set(ax, 'YScale', 'log');
    end
    if use_detailed_title
        title(ax, 'Computational Time vs Resolution');
    else
        title(ax, 'Runtime vs Resolution');
    end
    grid(ax, 'on');
    box(ax, 'on');
    legend(ax, 'Location', 'northwest');
end

function plot_mesh_cfl_dt_adv_axes(ax, Results, colors) %#ok<INUSD>
    cla(ax);
    [fd_mesh, fd_dt, fd_cfl] = extract_phase1_cfl_dt_adv_series(pick_struct(Results.children, {'fd'}, struct()));
    [sm_mesh, sm_dt, sm_cfl] = extract_phase1_cfl_dt_adv_series(pick_struct(Results.children, {'spectral'}, struct()));
    yyaxis(ax, 'left');
    semilogy(ax, fd_mesh, fd_cfl, 'o-', 'LineWidth', 1.4, 'Color', [0.00 0.28 0.72], ...
        'MarkerFaceColor', [0.00 0.28 0.72], 'DisplayName', 'FD $C_{\mathrm{adv}}$');
    hold(ax, 'on');
    semilogy(ax, sm_mesh, sm_cfl, 's-', 'LineWidth', 1.4, 'Color', [0.10 0.58 0.22], ...
        'MarkerFaceColor', [0.10 0.58 0.22], 'DisplayName', 'SM $C_{\mathrm{adv}}$');
    ylabel(ax, '$C_{\mathrm{adv}}$', 'Interpreter', 'latex');
    yyaxis(ax, 'right');
    semilogy(ax, fd_mesh, fd_dt, 'o--', 'LineWidth', 1.4, 'Color', [0.90 0.35 0.00], ...
        'DisplayName', 'FD $\Delta t_{\mathrm{adv}}$');
    semilogy(ax, sm_mesh, sm_dt, 's--', 'LineWidth', 1.4, 'Color', [0.68 0.10 0.55], ...
        'DisplayName', 'SM $\Delta t_{\mathrm{adv}}$');
    ylabel(ax, '$\Delta t_{\mathrm{adv}}$ (s)', 'Interpreter', 'latex');
    offset_right_axis_label(ax);
    hold(ax, 'off');
    xlabel(ax, 'Resolution $\kappa_N=N_x=N_y=N$', 'Interpreter', 'latex');
    title(ax, 'Advective CFL and timestep vs resolution');
    grid(ax, 'on');
    box(ax, 'on');
    legend(ax, 'Location', 'best', 'Interpreter', 'latex');
end

function plot_mesh_selected_timestep_summary(ax, Results, colors)
    selected_matrix = nan(2, 3);
    method_labels = {'FD', 'SM'};
    child_specs = {'fd', 'spectral'};
    for i = 1:numel(child_specs)
        child_struct = pick_struct(Results.children, {child_specs{i}}, struct());
        [mesh_labels, timestep_matrix, selected_index] = extract_phase1_convergence_timestep_series(child_struct); %#ok<ASGLU>
        if isempty(timestep_matrix)
            continue;
        end
        if ~(isfinite(selected_index) && selected_index >= 1 && selected_index <= size(timestep_matrix, 1))
            selected_index = 1;
        end
        selected_matrix(i, :) = timestep_matrix(selected_index, :);
    end

    if ~any(isfinite(selected_matrix), 'all')
        text(ax, 0.5, 0.5, 'No selected-mesh timestep data available.', ...
            'HorizontalAlignment', 'center', 'VerticalAlignment', 'middle');
        axis(ax, 'off');
        return;
    end

    bars = bar(ax, selected_matrix, 'grouped');
    if numel(bars) >= 3
        bars(1).FaceColor = colors.primary;
        bars(1).DisplayName = 'dt_advection';
        bars(2).FaceColor = colors.secondary;
        bars(2).DisplayName = 'dt_diffusion';
        bars(3).FaceColor = colors.tertiary;
        bars(3).DisplayName = 'dt_CFL';
    end
    ax.XTick = 1:numel(method_labels);
    ax.XTickLabel = method_labels;
    xlabel(ax, 'Method');
    ylabel(ax, 'Selected timestep value (s)');
    title(ax, 'Selected Mesh Timestep Summary');
    grid(ax, 'on');
    box(ax, 'on');
    legend(ax, 'Location', 'best');
end

function [labels, runtime_matrix] = build_phase1_ic_runtime_chart_data(Results)
    labels = {};
    runtime_matrix = [];
    if ~isstruct(Results) || ~isfield(Results, 'metrics')
        return;
    end
    ic_study = pick_struct(Results, {'ic_study'}, struct());
    baseline_case_id = pick_text(ic_study, {'baseline_case_id'}, 'baseline_elliptic_single');
    baseline_label = phase1_case_display_label(baseline_case_id, pick_text(ic_study, {'baseline_label'}, 'Elliptic'));
    labels = {baseline_label};
    runtime_matrix = [ ...
        pick_numeric(Results.metrics.FD, {'runtime_wall_s'}, NaN), ...
        pick_numeric(Results.metrics.Spectral, {'runtime_wall_s'}, NaN)];
    if isfield(ic_study, 'cases') && isstruct(ic_study.cases)
        for i = 1:numel(ic_study.cases)
            labels{end + 1} = phase1_case_display_label( ...
                pick_text(ic_study.cases(i), {'case_id'}, sprintf('case_%02d', i)), ...
                pick_text(ic_study.cases(i), {'display_label', 'label'}, sprintf('Case %d', i))); %#ok<AGROW>
            runtime_matrix(end + 1, :) = [ ... %#ok<AGROW>
                pick_numeric(ic_study.cases(i).fd, {'runtime_wall_s'}, NaN), ...
                pick_numeric(ic_study.cases(i).spectral, {'runtime_wall_s'}, NaN)];
        end
    end
end

function [labels, runtime_matrix] = build_phase1_resolution_runtime_chart_data(Results)
    labels = {};
    runtime_matrix = [];
    if ~isstruct(Results) || ~isfield(Results, 'children')
        return;
    end
    [fd_mesh, fd_runtime] = extract_phase1_convergence_runtime_series(pick_struct(Results.children, {'fd'}, struct()));
    [sp_mesh, sp_runtime] = extract_phase1_convergence_runtime_series(pick_struct(Results.children, {'spectral'}, struct()));
    common_mesh = unique([fd_mesh(:); sp_mesh(:)]).';
    common_mesh = common_mesh(isfinite(common_mesh));
    if isempty(common_mesh)
        return;
    end
    labels = arrayfun(@(n) sprintf('%d', round(n)), common_mesh, 'UniformOutput', false);
    runtime_matrix = nan(numel(common_mesh), 2);
    for i = 1:numel(common_mesh)
        fd_idx = find(fd_mesh == common_mesh(i), 1, 'first');
        sp_idx = find(sp_mesh == common_mesh(i), 1, 'first');
        if ~isempty(fd_idx)
            runtime_matrix(i, 1) = fd_runtime(fd_idx);
        end
        if ~isempty(sp_idx)
            runtime_matrix(i, 2) = sp_runtime(sp_idx);
        end
    end
end

function [mesh_values, runtime_values, xi_values] = extract_phase1_convergence_runtime_series(child_struct)
    mesh_values = [];
    runtime_values = [];
    xi_values = [];
    output = pick_struct(child_struct, {'convergence_output'}, struct());
    results = pick_struct(output, {'results'}, struct());
    records = pick_value(results, 'run_records', struct([]));
    if ~isstruct(records) || isempty(records)
        return;
    end
    if isfield(records, 'study_stage')
        primary_idx = ~strcmp({records.study_stage}, 'temporal');
        if any(primary_idx)
            records = records(primary_idx);
        end
    end
    mesh_values = nan(1, numel(records));
    runtime_values = nan(1, numel(records));
    xi_values = nan(1, numel(records));
    for i = 1:numel(records)
        mesh_values(i) = pick_numeric(records(i), {'Nx', 'Ny'}, NaN);
        runtime_values(i) = pick_numeric(records(i), {'runtime_wall_s', 'wall_time', 'total_time'}, NaN);
        xi_values(i) = pick_numeric(records(i), {'xi', 'relative_change'}, NaN);
    end
end

function fig = build_phase1_adaptive_timestep_convergence_figure(Results, colors)
    if nargin < 2 || ~isstruct(colors)
        colors = ResultsPlotDispatcher.default_light_colors();
    end
    fig = figure('Visible', 'off', 'Color', 'w', 'Units', 'pixels', 'Position', [120 120 1040 460]);
    ax = axes(fig); %#ok<LAXES>
    plot_phase1_adaptive_timestep_comparison_axis(ax, Results, colors);
end

function plot_phase1_adaptive_timestep_comparison_axis(ax, Results, colors)
    [fd_labels, fd_matrix] = extract_phase1_convergence_timestep_series(pick_struct(Results.children, {'fd'}, struct()));
    [sm_labels, sm_matrix] = extract_phase1_convergence_timestep_series(pick_struct(Results.children, {'spectral'}, struct()));
    labels = fd_labels;
    if isempty(labels)
        labels = sm_labels;
    end
    if isempty(labels)
        text(ax, 0.5, 0.5, 'No advective timestep data available.', ...
            'HorizontalAlignment', 'center', 'VerticalAlignment', 'middle');
        axis(ax, 'off');
        return;
    end
    n = numel(labels);
    data = nan(n, 2);
    if ~isempty(fd_matrix)
        data(1:min(n, size(fd_matrix, 1)), 1) = fd_matrix(1:min(n, size(fd_matrix, 1)), 1);
    end
    if ~isempty(sm_matrix)
        data(1:min(n, size(sm_matrix, 1)), 2) = sm_matrix(1:min(n, size(sm_matrix, 1)), 1);
    end
    bars = bar(ax, data, 'grouped');
    if numel(bars) >= 2
        bars(1).FaceColor = [0.08 0.36 0.78];
        bars(1).DisplayName = 'FD';
        bars(2).FaceColor = [0.88 0.32 0.10];
        bars(2).DisplayName = 'SM';
    end
    ax.XTick = 1:numel(labels);
    ax.XTickLabel = labels;
    ax.XTickLabelRotation = 25;
    xlabel(ax, 'Resolution $\kappa_N=N_x=N_y=N$', 'Interpreter', 'latex');
    ylabel(ax, '$\Delta t_{\mathrm{adv}}$ recommendation (s)', 'Interpreter', 'latex');
    title(ax, 'Convergence of the advective timestep recommendation');
    grid(ax, 'on');
    box(ax, 'on');
    legend(ax, 'Location', 'northeast');
end

function plot_phase1_adaptive_timestep_tile(ax, child_struct, method_label, colors)
    [mesh_labels, timestep_matrix] = extract_phase1_convergence_timestep_series(child_struct);
    if isempty(timestep_matrix)
        text(ax, 0.5, 0.5, 'No stability timestep data available.', ...
            'HorizontalAlignment', 'center', 'VerticalAlignment', 'middle');
        axis(ax, 'off');
        return;
    end

    bars = bar(ax, timestep_matrix, 'grouped');
    if numel(bars) >= 3
        bars(1).FaceColor = colors.primary;
        bars(1).DisplayName = 'dt_advection';
        bars(2).FaceColor = colors.secondary;
        bars(2).DisplayName = 'dt_diffusion';
        bars(3).FaceColor = colors.tertiary;
        bars(3).DisplayName = 'dt_CFL';
    end
    ax.XTick = 1:numel(mesh_labels);
    ax.XTickLabel = mesh_labels;
    ax.XTickLabelRotation = 20;
    xlabel(ax, 'Mesh level / N');
    ylabel(ax, 'Timestep value (s)');
    title(ax, method_label);
    grid(ax, 'on');
    box(ax, 'on');
    legend(ax, 'Location', 'best');
end

function [mesh_labels, timestep_matrix, selected_index] = extract_phase1_convergence_timestep_series(child_struct)
    mesh_labels = {};
    timestep_matrix = [];
    selected_index = NaN;
    output = pick_struct(child_struct, {'convergence_output'}, struct());
    results = pick_struct(output, {'results'}, struct());
    records = pick_value(results, 'run_records', struct([]));
    if ~isstruct(records) || isempty(records)
        return;
    end
    if isfield(records, 'study_stage')
        primary_idx = ~strcmp({records.study_stage}, 'temporal');
        if any(primary_idx)
            records = records(primary_idx);
        end
    end
    mesh_labels = cell(1, numel(records));
    timestep_matrix = nan(numel(records), 3);
    for i = 1:numel(records)
        mesh_n = pick_numeric(records(i), {'Nx', 'Ny'}, NaN);
        if isfinite(mesh_n)
            mesh_labels{i} = sprintf('%s / %d', ...
                pick_text(records(i), {'mesh_level_label'}, sprintf('L%02d', i)), round(mesh_n));
        else
            mesh_labels{i} = pick_text(records(i), {'mesh_level_label'}, sprintf('L%02d', i));
        end
        timestep_matrix(i, :) = [ ...
            pick_numeric(records(i), {'dt_adv'}, NaN), ...
            pick_numeric(records(i), {'dt_diff'}, NaN), ...
            pick_numeric(records(i), {'dt_final', 'dt'}, NaN)];
        if logical(pick_value(records(i), 'selected_level', false))
            selected_index = i;
        end
    end
end

function [mesh_values, dt_adv, cfl_adv] = extract_phase1_cfl_dt_adv_series(child_struct)
    mesh_values = [];
    dt_adv = [];
    cfl_adv = [];
    output = pick_struct(child_struct, {'convergence_output'}, struct());
    results = pick_struct(output, {'results'}, struct());
    records = pick_value(results, 'run_records', struct([]));
    if ~isstruct(records) || isempty(records)
        return;
    end
    if isfield(records, 'study_stage')
        primary_idx = ~strcmp({records.study_stage}, 'temporal');
        if any(primary_idx)
            records = records(primary_idx);
        end
    end
    mesh_values = nan(1, numel(records));
    dt_adv = nan(1, numel(records));
    cfl_adv = nan(1, numel(records));
    for i = 1:numel(records)
        mesh_values(i) = pick_numeric(records(i), {'Nx', 'Ny'}, NaN);
        dt_adv(i) = pick_numeric(records(i), {'dt_adv'}, NaN);
        cfl_adv(i) = pick_numeric(records(i), {'cfl_adv'}, NaN);
    end
end

function delete_phase_figure_if_present(output_dir, stem)
    if exist(output_dir, 'dir') ~= 7
        return;
    end
    fig_path = fullfile(output_dir, [stem '.fig']);
    png_path = fullfile(output_dir, [stem '.png']);
    if exist(fig_path, 'file') == 2
        delete(fig_path);
    end
    if exist(png_path, 'file') == 2
        delete(png_path);
    end
end

function log_phase1_mesh_sweep_record(record)
    SafeConsoleIO.fprintf(['Phase 1 mesh sweep | %s | %s | N=%s | delta=%s | CFLa=%s | CFLd=%s | ', ...
        'dt_used=%s | dt_cfl=%s | dt_adv=%s | dt_diff=%s | xi_L2=%s | xi_peak=%s\n'], ...
        pick_text(record, {'method_label'}, upper(char(string(pick_text(record, {'method'}, ''))))), ...
        pick_text(record, {'mesh_level_label'}, '--'), ...
        phase1_mesh_size_text(record), ...
        phase1_numeric_text(pick_numeric(record, {'delta', 'h'}, NaN)), ...
        phase1_numeric_text(pick_numeric(record, {'cfl_adv'}, NaN)), ...
        phase1_numeric_text(pick_numeric(record, {'cfl_diff'}, NaN)), ...
        phase1_numeric_text(pick_numeric(record, {'dt_used', 'dt_final', 'dt'}, NaN)), ...
        phase1_numeric_text(pick_numeric(record, {'dt_final', 'dt'}, NaN)), ...
        phase1_numeric_text(pick_numeric(record, {'dt_adv'}, NaN)), ...
        phase1_numeric_text(pick_numeric(record, {'dt_diff'}, NaN)), ...
        phase1_percent_text(pick_numeric(record, {'xi', 'relative_change'}, NaN)), ...
        phase1_percent_text(pick_numeric(record, {'max_vorticity_rel_error_pct'}, NaN)));
end

function txt = phase1_mesh_size_text(record)
    nx = pick_numeric(record, {'Nx'}, NaN);
    ny = pick_numeric(record, {'Ny'}, NaN);
    if isfinite(nx) && isfinite(ny)
        txt = sprintf('%dx%d', round(nx), round(ny));
    else
        txt = '--';
    end
end

function txt = phase1_numeric_text(value)
    if ~isfinite(double(value))
        txt = '--';
        return;
    end
    txt = sprintf('%.6e', double(value));
end

function txt = phase1_percent_text(value)
    if ~isfinite(double(value))
        txt = '--';
        return;
    end
    txt = sprintf('%.3f%%', double(value));
end

function txt = phase1_mesh_ladder_text(levels)
    levels = double(reshape(levels, 1, []));
    levels = levels(isfinite(levels));
    if isempty(levels)
        txt = '--';
        return;
    end
    txt = strjoin(arrayfun(@(v) sprintf('%d', round(v)), levels, 'UniformOutput', false), ', ');
end

function txt = phase1_mesh_ladder_mode_text(mode_value)
    mode_value = normalize_phase1_mesh_ladder_mode(mode_value);
    switch mode_value
        case 'bounded'
            txt = 'bounds';
        case 'powers_of_2'
            txt = 'powers_of_2';
        otherwise
            txt = char(string(mode_value));
    end
end

function label = phase1_method_display_label(method_key)
    method_key = normalize_method_key(method_key);
    switch method_key
        case 'fd'
            label = 'FD';
        case 'spectral'
            label = 'SM';
        otherwise
            label = upper(char(string(method_key)));
    end
end

function fig = build_phase1_overview_triptych(Results, colors)
    if nargin < 2 || ~isstruct(colors)
        colors = ResultsPlotDispatcher.default_light_colors();
    end
    fig = figure('Visible', 'off', 'Color', 'w', 'Units', 'pixels', 'Position', [120 120 1440 320]);
    layout = tiledlayout(fig, 1, 3, 'TileSpacing', 'compact', 'Padding', 'compact');
    title(layout, 'Phase 1 Overview');

    ax = nexttile(layout, 1);
    plot_phase1_convergence_axes(ax, Results, colors, false);

    ax = nexttile(layout, 2);
    plot_phase1_conservation_triptych(ax, Results, colors);

    ax = nexttile(layout, 3);
    plot_phase1_rmse_triptych(ax, Results, colors);
end

function plot_phase1_conservation_triptych(ax, Results, colors)
    [category_labels, error_matrix] = build_phase1_conservation_chart_data(Results);
    if isempty(error_matrix)
        text(ax, 0.5, 0.5, 'No conservation metrics available.', ...
            'HorizontalAlignment', 'center', 'VerticalAlignment', 'middle');
        axis(ax, 'off');
        return;
    end
    bars = bar(ax, error_matrix, 'grouped');
    if numel(bars) >= 2
        bars(1).FaceColor = colors.primary;
        bars(1).DisplayName = 'FD';
        bars(2).FaceColor = colors.tertiary;
        bars(2).DisplayName = 'SM';
    end
    ax.XTick = 1:numel(category_labels);
    ax.XTickLabel = category_labels;
    ax.XTickLabelRotation = 18;
    xlabel(ax, 'Metric');
    ylabel(ax, 'Relative drift');
    title(ax, 'Conservation Drift');
    grid(ax, 'on');
    box(ax, 'on');
    legend(ax, 'Location', 'best');
end

function plot_phase1_rmse_triptych(ax, Results, colors)
    rmse_payload = pick_struct(Results, {'error_vs_time', 'rmse_vs_time'}, struct());
    time_s = pick_value(rmse_payload, 'time_s', []);
    if isempty(time_s)
        text(ax, 0.5, 0.5, 'No error data available.', ...
            'HorizontalAlignment', 'center', 'VerticalAlignment', 'middle');
        axis(ax, 'off');
        return;
    end

    hold(ax, 'on');
    h_rmse = plot(ax, time_s, pick_value(rmse_payload, 'rmse', pick_value(rmse_payload, 'relative_rmse', nan(size(time_s)))), '-', ...
        'LineWidth', 1.7, 'Color', colors.primary, 'DisplayName', 'RMSE');
    h_vorticity = plot(ax, time_s, pick_value(rmse_payload, 'vorticity_vector_relative_l2_mismatch', pick_value(rmse_payload, 'relative_l2_mismatch', nan(size(time_s)))), '--', ...
        'LineWidth', 1.5, 'Color', colors.secondary, 'DisplayName', 'Vorticity L2');
    h_velocity = plot(ax, time_s, pick_value(rmse_payload, 'velocity_vector_relative_l2_mismatch', nan(size(time_s))), '-.', ...
        'LineWidth', 1.7, 'Color', max(colors.tertiary - 0.12, 0), 'DisplayName', 'Velocity vectors');
    hold(ax, 'off');
    xlabel(ax, 'Evolution time (s)');
    ylabel(ax, 'Error');
    title(ax, 'Error vs Time');
    grid(ax, 'on');
    box(ax, 'on');
    legend(ax, [h_rmse, h_vorticity, h_velocity], ...
        {'RMSE', 'Vorticity L2', 'Velocity vectors'}, 'Location', 'best');
end

function [category_labels, error_matrix] = build_phase1_vortex_preservation_error_chart_data(Results)
    category_labels = {'Peak Ratio Error', 'Centroid Drift', 'Core Anisotropy Error'};
    error_matrix = [];
    metrics = pick_struct(Results, {'metrics'}, struct());
    fd_metrics = pick_struct(metrics, {'FD'}, struct());
    sp_metrics = pick_struct(metrics, {'Spectral'}, struct());
    if isempty(fieldnames(fd_metrics)) || isempty(fieldnames(sp_metrics))
        return;
    end
    fd_values = [ ...
        pick_numeric(fd_metrics, {'peak_vorticity_ratio_error'}, abs(pick_numeric(fd_metrics, {'peak_vorticity_ratio'}, NaN) - 1)), ...
        pick_numeric(fd_metrics, {'centroid_drift'}, NaN), ...
        pick_numeric(fd_metrics, {'core_anisotropy_error'}, ...
            abs(pick_numeric(fd_metrics, {'core_anisotropy_final'}, NaN) - ...
            pick_numeric(fd_metrics, {'core_anisotropy_initial'}, NaN)))];
    sp_values = [ ...
        pick_numeric(sp_metrics, {'peak_vorticity_ratio_error'}, abs(pick_numeric(sp_metrics, {'peak_vorticity_ratio'}, NaN) - 1)), ...
        pick_numeric(sp_metrics, {'centroid_drift'}, NaN), ...
        pick_numeric(sp_metrics, {'core_anisotropy_error'}, ...
            abs(pick_numeric(sp_metrics, {'core_anisotropy_final'}, NaN) - ...
            pick_numeric(sp_metrics, {'core_anisotropy_initial'}, NaN)))];
    error_matrix = [fd_values(:), sp_values(:)];
end

function [category_labels, error_matrix] = build_phase1_conservation_chart_data(Results)
    category_labels = {'Kinetic Energy', 'Enstrophy', 'Circulation'};
    error_matrix = [];
    metrics = pick_struct(Results, {'metrics'}, struct());
    fd_metrics = pick_struct(metrics, {'FD'}, struct());
    sp_metrics = pick_struct(metrics, {'Spectral'}, struct());
    if isempty(fieldnames(fd_metrics)) || isempty(fieldnames(sp_metrics))
        return;
    end
    fd_values = [ ...
        pick_numeric(fd_metrics, {'kinetic_energy_drift'}, NaN), ...
        pick_numeric(fd_metrics, {'enstrophy_drift'}, NaN), ...
        pick_numeric(fd_metrics, {'circulation_drift'}, NaN)];
    sp_values = [ ...
        pick_numeric(sp_metrics, {'kinetic_energy_drift'}, NaN), ...
        pick_numeric(sp_metrics, {'enstrophy_drift'}, NaN), ...
        pick_numeric(sp_metrics, {'circulation_drift'}, NaN)];
    error_matrix = [fd_values(:), sp_values(:)];
end

function fig = build_phase1_convergence_comparison_figure(Results, colors)
    if nargin < 2 || ~isstruct(colors)
        colors = ResultsPlotDispatcher.default_light_colors();
    end
    fig = figure('Visible', 'off', 'Color', 'w', 'Units', 'pixels', 'Position', [120 120 880 520]);
    ax = axes(fig); %#ok<LAXES>
    plot_phase1_convergence_axes(ax, Results, colors, true);
end

function plot_phase1_convergence_axes(ax, Results, colors, use_detailed_title)
    if nargin < 4
        use_detailed_title = false;
    end
    sm_abs_color = [0.68, 0.10, 0.55];
    if isstruct(colors) && isfield(colors, 'quaternary') && ...
            isnumeric(colors.quaternary) && numel(colors.quaternary) == 3
        sm_abs_color = double(reshape(colors.quaternary, 1, 3));
    elseif isstruct(colors) && isfield(colors, 'tertiary') && ...
            isnumeric(colors.tertiary) && numel(colors.tertiary) == 3
        sm_abs_color = max(0, double(reshape(colors.tertiary, 1, 3)) - 0.12);
    end
    cla(ax);
    hold(ax, 'on');

    plot_phase1_method_convergence_series(ax, pick_struct(Results.children, {'fd'}, struct()), ...
        'FD', [0.00 0.28 0.72], [0.90 0.35 0.00]);
    plot_phase1_method_convergence_series(ax, pick_struct(Results.children, {'spectral'}, struct()), ...
        'SM', [0.10 0.58 0.22], sm_abs_color);

    tolerance = resolve_phase1_convergence_tolerance(Results);
    if isfinite(tolerance) && tolerance > 0
        yline(ax, tolerance, '--', 'Tolerance (%)', ...
            'Color', [0.30 0.30 0.30], 'LineWidth', 1.2, ...
            'LabelHorizontalAlignment', 'left', 'HandleVisibility', 'off');
    end

    hold(ax, 'off');
    grid(ax, 'on');
    box(ax, 'on');
    xlabel(ax, 'Resolution $\kappa_N=N_x=N_y=N$', 'Interpreter', 'latex');
    ylabel(ax, {'Convergence error (\%)', '$\xi_{L_2}$ and $\xi_{|\omega|_{\max}}$'}, 'Interpreter', 'latex');
    if use_detailed_title
        title(ax, 'Mesh convergence of vorticity error metrics');
    else
        title(ax, 'Mesh Convergence');
    end
    apply_phase1_convergence_axis_limits(ax);
    if ~isempty(ax.Children)
        legend(ax, 'Location', 'best');
    end
end

function plot_phase1_method_convergence_series(ax, child_struct, method_label, color_l2, color_peak)
    [mesh_values, xi_l2, xi_peak, tolerance] = extract_phase1_convergence_metric_series(child_struct);
    if isempty(mesh_values)
        return;
    end

    plot(ax, mesh_values, xi_l2, 'o-', ...
        'Color', color_l2, 'MarkerFaceColor', color_l2, ...
        'LineWidth', 1.4, 'DisplayName', sprintf('%s L2', method_label));
    plot(ax, mesh_values, xi_peak, 's-', ...
        'Color', color_peak, 'MarkerFaceColor', color_peak, ...
        'LineWidth', 1.4, 'DisplayName', sprintf('%s peak', method_label));

    if isfinite(tolerance)
        joint_idx = find(xi_l2 <= tolerance, 1, 'first');
    else
        joint_idx = [];
    end
    if ~isempty(joint_idx)
        xline(ax, mesh_values(joint_idx), '--', sprintf('%s L2 converged', method_label), ...
            'Color', color_l2, 'LineWidth', 1.4, 'HandleVisibility', 'off');
    end
end

function apply_phase1_convergence_axis_limits(ax)
    if nargin < 1 || ~isgraphics(ax, 'axes')
        return;
    end
    y_values = [];
    for i = 1:numel(ax.Children)
        child = ax.Children(i);
        if isprop(child, 'YData')
            child_y = double(child.YData);
            y_values = [y_values, child_y(:).']; %#ok<AGROW>
        elseif isprop(child, 'Value')
            y_values = [y_values, double(child.Value)]; %#ok<AGROW>
        end
    end
    y_values = y_values(isfinite(y_values));
    if isempty(y_values)
        return;
    end
    y_min = min(y_values);
    y_max = max(y_values);
    if ~isfinite(y_min) || ~isfinite(y_max)
        return;
    end
    if y_max <= y_min
        pad = max(1.0e-3, abs(y_max) * 0.05 + 1.0e-3);
        lower = y_min - pad;
        if all(y_values >= 0)
            lower = min(lower, -0.05 * max(abs(y_max), 1.0));
        end
        ylim(ax, [lower, y_max + pad]);
        return;
    end
    pad = 0.08 * (y_max - y_min);
    lower = y_min - pad;
    if all(y_values >= 0)
        lower = min(lower, -0.05 * max(abs(y_max), 1.0));
    end
    ylim(ax, [lower, y_max + pad]);
end

function [mesh_values, xi_l2, xi_peak, tolerance] = extract_phase1_convergence_metric_series(child_struct)
    mesh_values = [];
    xi_l2 = [];
    xi_peak = [];
    tolerance = NaN;
    output = pick_struct(child_struct, {'convergence_output'}, struct());
    results = pick_struct(output, {'results'}, struct());
    records = pick_value(results, 'run_records', struct([]));
    if ~isstruct(records) || isempty(records)
        return;
    end
    if isfield(records, 'study_stage')
        primary_idx = ~strcmp({records.study_stage}, 'temporal');
        if any(primary_idx)
            records = records(primary_idx);
        end
    end
    mesh_values = nan(1, numel(records));
    xi_l2 = nan(1, numel(records));
    xi_peak = nan(1, numel(records));
    for i = 1:numel(records)
        mesh_values(i) = pick_numeric(records(i), {'Nx', 'Ny'}, NaN);
        xi_l2(i) = pick_numeric(records(i), {'xi', 'relative_change'}, NaN);
        xi_peak(i) = pick_numeric(records(i), {'max_vorticity_rel_error_pct'}, NaN);
    end
    tolerance = pick_numeric(results, {'xi_tol'}, NaN);
    if ~isfinite(tolerance)
        tolerance = pick_numeric(pick_struct(child_struct, {'metrics'}, struct()), {'mesh_tolerance'}, NaN);
    end
end

function tolerance = resolve_phase1_convergence_tolerance(Results)
    tolerance = NaN;
    fd_metrics = pick_struct(pick_struct(Results, {'metrics'}, struct()), {'FD'}, struct());
    sp_metrics = pick_struct(pick_struct(Results, {'metrics'}, struct()), {'Spectral'}, struct());
    tolerance = pick_numeric(fd_metrics, {'mesh_tolerance'}, NaN);
    if ~isfinite(tolerance)
        tolerance = pick_numeric(sp_metrics, {'mesh_tolerance'}, NaN);
    end
end

function color_out = lighten_color(color_in, blend_factor)
    if nargin < 2 || ~isfinite(blend_factor)
        blend_factor = 0.2;
    end
    blend_factor = min(max(double(blend_factor), 0), 1);
    color_out = color_in + (1 - color_in) * blend_factor;
end

function values = sanitize_log_plot_values(values)
    if isempty(values)
        return;
    end
    finite_idx = isfinite(values);
    values(~finite_idx) = NaN;
    positive_idx = finite_idx & values > 0;
    zero_idx = finite_idx & values <= 0;
    values(positive_idx) = max(values(positive_idx), eps);
    values(zero_idx) = eps;
end

function write_phase1_report(Results, paths)
    report_path = fullfile(paths.reports, 'Phase1_Periodic_FD_vs_Spectral_Report.md');
    ensure_parent_directory(report_path);
    fid = fopen(report_path, 'w');
    if fid < 0
        error('Phase1PeriodicComparison:ReportWriteFailed', 'Could not write report: %s', report_path);
    end
    cleaner = onCleanup(@() fclose(fid));
    m = Results.metrics;
    fprintf(fid, '# Phase 1 Periodic FD vs Spectral Verification\n\n');
    fprintf(fid, '- Phase ID: `%s`\n', Results.phase_id);
    fprintf(fid, '- Forced BC: `%s`\n', Results.phase_config.force_bc_case);
    fprintf(fid, '- Forced bathymetry: `%s`\n', Results.phase_config.force_bathymetry);
    fprintf(fid, '- Continue on unconverged mesh fallback: `%s`\n\n', ...
        logical_text_local(pick_value(Results.phase_config, 'allow_unconverged_mesh_fallback', true)));
    if logical(pick_value(m.summary, 'continued_with_unconverged_mesh', false))
        fprintf(fid, '> Phase 1 continued after at least one convergence study missed the tolerance. ');
        fprintf(fid, 'The workflow used the finest-mesh fallback instead of locking the run.\n\n');
    end
    fd_unstable = pick_value(pick_struct(pick_struct(Results, {'children'}, struct()), {'fd'}, struct()), ...
        'convergence_output', struct());
    fd_unstable_labels = pick_value(pick_struct(pick_struct(fd_unstable, {'results'}, struct()), {'summary'}, struct()), ...
        'unstable_level_labels', {});
    sp_unstable = pick_value(pick_struct(pick_struct(Results, {'children'}, struct()), {'spectral'}, struct()), ...
        'convergence_output', struct());
    sp_unstable_labels = pick_value(pick_struct(pick_struct(sp_unstable, {'results'}, struct()), {'summary'}, struct()), ...
        'unstable_level_labels', {});
    if ~isempty(fd_unstable_labels) || ~isempty(sp_unstable_labels)
        fprintf(fid, '## Destabilized Mesh Levels\n\n');
        fprintf(fid, '- FD unstable levels: `%s`\n', strjoin(cellstr(string(fd_unstable_labels)), ', '));
        fprintf(fid, '- Spectral unstable levels: `%s`\n\n', strjoin(cellstr(string(sp_unstable_labels)), ', '));
    end
    fprintf(fid, '| Method | mismatch L2 | mismatch Linf | spatial/modal rate | temporal rate | convergence verdict | selected mesh | selection reason | fallback continued | xi_L2 %% | xi_peak %% | tolerance %% | runtime (s) |\n');
    fprintf(fid, '| --- | ---: | ---: | ---: | ---: | --- | --- | --- | --- | ---: | ---: | ---: | ---: |\n');
    fprintf(fid, '| FD | %.6e | %.6e | %.3f | %.3f | %s | %s | %s | %s | %.6f | %.6f | %.6f | %.3f |\n', ...
        m.FD.cross_method_mismatch_l2, m.FD.cross_method_mismatch_linf, ...
        m.FD.observed_spatial_rate, m.FD.observed_temporal_rate, ...
        m.FD.mesh_convergence_verdict, mesh_label_from_entry(Results.children.fd.selected_mesh), ...
        pick_text(Results.children.fd.selected_mesh, {'selection_reason'}, ''), ...
        logical_text_local(m.FD.continued_after_unconverged_mesh), ...
        m.FD.mesh_final_successive_vorticity_error, m.FD.mesh_final_peak_vorticity_error, m.FD.mesh_tolerance, m.FD.runtime_wall_s);
    fprintf(fid, '| Spectral | %.6e | %.6e | %.3f | %.3f | %s | %s | %s | %s | %.6f | %.6f | %.6f | %.3f |\n', ...
        m.Spectral.cross_method_mismatch_l2, m.Spectral.cross_method_mismatch_linf, ...
        m.Spectral.observed_spatial_rate, m.Spectral.observed_temporal_rate, ...
        m.Spectral.mesh_convergence_verdict, mesh_label_from_entry(Results.children.spectral.selected_mesh), ...
        pick_text(Results.children.spectral.selected_mesh, {'selection_reason'}, ''), ...
        logical_text_local(m.Spectral.continued_after_unconverged_mesh), ...
        m.Spectral.mesh_final_successive_vorticity_error, m.Spectral.mesh_final_peak_vorticity_error, m.Spectral.mesh_tolerance, m.Spectral.runtime_wall_s);
    if isfield(Results, 'error_vs_time') && isstruct(Results.error_vs_time) && ~isempty(fieldnames(Results.error_vs_time))
        fprintf(fid, '\n## Error vs Time for Different Metrics\n\n');
        fprintf(fid, '- Metric basis: `%s`\n', pick_text(Results.error_vs_time, {'metric_basis'}, '--'));
        fprintf(fid, '- Comparison grid: `%s`\n', pick_text(Results.error_vs_time, {'comparison_grid_label'}, '--'));
        fprintf(fid, '- Spatial interpolation: `%s`\n', pick_text(Results.error_vs_time, {'spatial_interpolation'}, '--'));
        fprintf(fid, '- Temporal interpolation: `%s`\n', pick_text(Results.error_vs_time, {'temporal_interpolation'}, '--'));
        fprintf(fid, '- Overlap window: `[%.6g, %.6g]` s\n', Results.error_vs_time.overlap_time_window_s(1), Results.error_vs_time.overlap_time_window_s(2));
        fprintf(fid, '- Mean MSE: `%.6e`\n', pick_numeric(Results.error_vs_time, {'mse_mean'}, NaN));
        fprintf(fid, '- Peak MSE: `%.6e`\n', pick_numeric(Results.error_vs_time, {'mse_peak'}, NaN));
        fprintf(fid, '- Mean RMSE: `%.6e`\n', pick_numeric(Results.error_vs_time, {'rmse_mean'}, NaN));
        fprintf(fid, '- Peak RMSE: `%.6e`\n', pick_numeric(Results.error_vs_time, {'rmse_peak'}, NaN));
        fprintf(fid, '- Mean vorticity L2 mismatch: `%.6e`\n', pick_numeric(Results.error_vs_time, {'vorticity_vector_relative_l2_mismatch_mean'}, NaN));
        fprintf(fid, '- Peak vorticity L2 mismatch: `%.6e`\n', pick_numeric(Results.error_vs_time, {'vorticity_vector_relative_l2_mismatch_peak'}, NaN));
        fprintf(fid, '- Mean streamfunction L2 mismatch: `%.6e`\n', pick_numeric(Results.error_vs_time, {'streamfunction_relative_l2_mismatch_mean'}, NaN));
        fprintf(fid, '- Peak streamfunction L2 mismatch: `%.6e`\n', pick_numeric(Results.error_vs_time, {'streamfunction_relative_l2_mismatch_peak'}, NaN));
        fprintf(fid, '- Mean speed L2 mismatch: `%.6e`\n', pick_numeric(Results.error_vs_time, {'speed_relative_l2_mismatch_mean'}, NaN));
        fprintf(fid, '- Peak speed L2 mismatch: `%.6e`\n', pick_numeric(Results.error_vs_time, {'speed_relative_l2_mismatch_peak'}, NaN));
        fprintf(fid, '- Mean velocity-vector L2 mismatch: `%.6e`\n', pick_numeric(Results.error_vs_time, {'velocity_vector_relative_l2_mismatch_mean'}, NaN));
        fprintf(fid, '- Peak velocity-vector L2 mismatch: `%.6e`\n', pick_numeric(Results.error_vs_time, {'velocity_vector_relative_l2_mismatch_peak'}, NaN));
        fprintf(fid, '- Mean streamline-direction L2 mismatch: `%.6e`\n', pick_numeric(Results.error_vs_time, {'streamline_direction_relative_l2_mismatch_mean'}, NaN));
        fprintf(fid, '- Peak streamline-direction L2 mismatch: `%.6e`\n', pick_numeric(Results.error_vs_time, {'streamline_direction_relative_l2_mismatch_peak'}, NaN));
        fprintf(fid, '- Mean circulation relative error: `%.6e`\n', pick_numeric(Results.error_vs_time, {'circulation_relative_error_mean'}, NaN));
        fprintf(fid, '- Peak circulation relative error: `%.6e`\n', pick_numeric(Results.error_vs_time, {'circulation_relative_error_peak'}, NaN));
        fprintf(fid, '- Figure: `%s`\n', fullfile(paths.figures, 'phase1_error_vs_time.png'));
        fprintf(fid, '- Overview triptych: `%s`\n', pick_text(pick_struct(pick_struct(pick_struct(Results, {'figure_artifacts'}, struct()), ...
            {'comparisons'}, struct()), {'overview_triptych'}, struct()), {'png'}, '--'));
    end
    ref_fig = pick_text(pick_struct(pick_struct(pick_struct(Results, {'figure_artifacts'}, struct()), ...
        {'comparisons'}, struct()), {'reference_evolution_grid'}, struct()), {'png', 'fig'}, '');
    if ~isempty(ref_fig)
        fprintf(fid, '\n## Reference Evolution Grid\n\n');
        fprintf(fid, '- Comparison asset: `%s`\n', ref_fig);
        fprintf(fid, '- Simulation animation frame count remains independent of the 9-frame reference GIF.\n');
    end
    if isfield(Results, 'reference_calibration') && isstruct(Results.reference_calibration) && ...
            ~isempty(fieldnames(Results.reference_calibration))
        fprintf(fid, '\n## GIF Reference Calibration\n\n');
        method_fields = fieldnames(Results.reference_calibration);
        for i = 1:numel(method_fields)
            entry = Results.reference_calibration.(method_fields{i});
            if ~isstruct(entry) || isempty(fieldnames(entry))
                continue;
            end
            summary = pick_struct(entry, {'summary'}, struct());
            fprintf(fid, '### %s\n\n', pick_text(entry, {'method_label'}, upper(method_fields{i})));
            fprintf(fid, '- Preset ID: `%s`\n', pick_text(entry, {'preset_id'}, '--'));
            fprintf(fid, '- Reference asset: `%s`\n', pick_text(entry, {'reference_asset_path'}, '--'));
            fprintf(fid, '- Calibration grid: `%dx%d`\n', round(pick_numeric(entry, {'grid_n'}, NaN)), round(pick_numeric(entry, {'grid_n'}, NaN)));
            fprintf(fid, '- Mean grayscale RMSE: `%.6e`\n', pick_numeric(summary, {'mean_grayscale_rmse'}, NaN));
            fprintf(fid, '- Mean contour overlap loss: `%.6e`\n', pick_numeric(summary, {'mean_contour_overlap_loss'}, NaN));
            fprintf(fid, '- Mean principal-axis angle error (rad): `%.6e`\n', pick_numeric(summary, {'mean_core_principal_axis_angle_error_rad'}, NaN));
            fprintf(fid, '- Mean spiral-arm angle error (rad): `%.6e`\n', pick_numeric(summary, {'mean_spiral_arm_angle_error_rad'}, NaN));
            fprintf(fid, '- Mean composite GIF-match score: `%.6e`\n', pick_numeric(summary, {'mean_composite_gif_match_score'}, NaN));
            fprintf(fid, '- Simulation grid: `%s`\n', pick_text(pick_struct(pick_struct(entry, {'artifacts'}, struct()), {'simulation_grid'}, struct()), {'png', 'fig'}, '--'));
            fprintf(fid, '- Reference-vs-simulation grid: `%s`\n\n', ...
                pick_text(pick_struct(pick_struct(entry, {'artifacts'}, struct()), {'reference_vs_simulation'}, struct()), {'png', 'fig'}, '--'));
        end
    end
    if isfield(Results, 'ic_study') && isstruct(Results.ic_study) && isfield(Results.ic_study, 'cases') && ...
            ~isempty(Results.ic_study.cases)
        fprintf(fid, '\n## Post-Convergence IC Study\n\n');
        fprintf(fid, '- Baseline case: `%s`\n', pick_text(Results.ic_study, {'baseline_label'}, 'Stretched Gaussian'));
        fprintf(fid, '- Extra case count: `%d`\n', numel(Results.ic_study.cases));
        fprintf(fid, '- Runtime-per-IC chart: `%s`\n', pick_text(pick_struct(pick_struct(pick_struct(Results, {'figure_artifacts'}, struct()), ...
            {'comparisons'}, struct()), {'runtime_per_ic_case'}, struct()), {'png'}, '--'));
        fprintf(fid, '- Runtime-vs-resolution chart: `%s`\n', pick_text(pick_struct(pick_struct(pick_struct(Results, {'figure_artifacts'}, struct()), ...
            {'comparisons'}, struct()), {'runtime_vs_resolution'}, struct()), {'png'}, '--'));
        fprintf(fid, '\n| Case | FD mesh | FD runtime (s) | Spectral mesh | Spectral runtime (s) |\n');
        fprintf(fid, '| --- | --- | ---: | --- | ---: |\n');
        for i = 1:numel(Results.ic_study.cases)
            case_entry = Results.ic_study.cases(i);
            fprintf(fid, '| %s | %s | %.3f | %s | %.3f |\n', ...
                pick_text(case_entry, {'label'}, sprintf('Case %d', i)), ...
                mesh_label_from_entry(pick_struct(case_entry, {'fd', 'selected_mesh'}, struct())), ...
                pick_numeric(pick_struct(case_entry, {'fd'}, struct()), {'runtime_wall_s'}, NaN), ...
                mesh_label_from_entry(pick_struct(case_entry, {'spectral', 'selected_mesh'}, struct())), ...
                pick_numeric(pick_struct(case_entry, {'spectral'}, struct()), {'runtime_wall_s'}, NaN));
        end
    end
    clear cleaner
end

function txt = logical_text_local(value)
    if logical(value)
        txt = 'yes';
    else
        txt = 'no';
    end
end

function append_phase1_master_rows(Results, base_run_config, base_parameters)
    phase_id = Results.phase_id;
    convergence = Results.convergence_outputs;
    for i = 1:numel(convergence)
        rc = phase_row_config(base_run_config, convergence(i).method, phase_id);
        rc.run_id = sprintf('%s_%s_convergence_row', phase_id, convergence(i).method_key);
        res = struct('row_type', 'phase1_convergence', ...
            'wall_time', pick_numeric(convergence(i).results, {'total_time', 'wall_time'}, NaN), ...
            'phase_metrics', convergence_row_metrics(phase_id, convergence(i)));
        MasterRunsTable.append_run(rc.run_id, rc, base_parameters, res);
    end
    MasterRunsTable.append_run(sprintf('%s_fd_method_row', phase_id), ...
        phase_row_config(base_run_config, 'FD', phase_id), base_parameters, ...
        phase_method_result('phase1_method_run', Results.metrics.FD));
    MasterRunsTable.append_run(sprintf('%s_spectral_method_row', phase_id), ...
        phase_row_config(base_run_config, 'Spectral', phase_id), base_parameters, ...
        phase_method_result('phase1_method_run', Results.metrics.Spectral));
    summary_result = struct('row_type', 'phase1_summary', 'wall_time', NaN, ...
        'phase_metrics', Results.metrics.summary);
    MasterRunsTable.append_run(sprintf('%s_summary_row', phase_id), ...
        phase_row_config(base_run_config, 'Phase1', phase_id), base_parameters, summary_result);
end

function rc = phase_row_config(base_run_config, method, phase_id)
    rc = base_run_config;
    rc.method = method;
    rc.mode = 'PhaseComparison';
    rc.phase_id = phase_id;
end

function res = phase_method_result(row_type, metrics)
    res = struct();
    res.row_type = row_type;
    res.wall_time = metrics.runtime_wall_s;
    res.total_steps = metrics.total_steps;
    res.max_omega = metrics.peak_vorticity_ratio;
    res.phase_metrics = metrics;
end

function metrics = convergence_row_metrics(phase_id, output)
    conv = summarize_convergence(output.results);
    metrics = struct();
    metrics.phase_id = phase_id;
    metrics.method = output.method;
    metrics.mesh_convergence_verdict = conv.overall_verdict;
    metrics.observed_spatial_rate = conv.primary_observed_rate;
    metrics.observed_temporal_rate = conv.temporal_observed_rate;
    metrics.mesh_source_path = fullfile(output.paths.data, 'convergence_results.mat');
end

function tf = json_saving_enabled(varargin)
    tf = true;
    for i = 1:nargin
        source = varargin{i};
        if ~isstruct(source)
            continue;
        end
        if isfield(source, 'save_json') && ~isempty(source.save_json)
            tf = logical(source.save_json);
            return;
        end
        if isfield(source, 'phase_config') && isstruct(source.phase_config) && ...
                isfield(source.phase_config, 'save_json') && ~isempty(source.phase_config.save_json)
            tf = logical(source.phase_config.save_json);
            return;
        end
        if isfield(source, 'mesh_convergence') && isstruct(source.mesh_convergence) && ...
                isfield(source.mesh_convergence, 'save_json') && ~isempty(source.mesh_convergence.save_json)
            tf = logical(source.mesh_convergence.save_json);
            return;
        end
        if isfield(source, 'phase1') && isstruct(source.phase1) && ...
                isfield(source.phase1, 'save_json') && ~isempty(source.phase1.save_json)
            tf = logical(source.phase1.save_json);
            return;
        end
    end
end

function write_json(path, payload)
    ensure_parent_directory(path);
    fid = fopen(path, 'w');
    if fid < 0
        error('Phase1PeriodicComparison:JsonWriteFailed', 'Could not write JSON file: %s', path);
    end
    cleaner = onCleanup(@() fclose(fid));
    fprintf(fid, '%s', jsonencode(payload));
    clear cleaner
end

function ensure_parent_directory(path)
    parent_dir = fileparts(char(string(path)));
    if isempty(parent_dir)
        return;
    end
    if exist(parent_dir, 'dir') ~= 7
        mkdir(parent_dir);
    end
end

function token = method_to_parameter_token(method)
    switch normalize_method_key(method)
        case 'fd'
            token = 'finite_difference';
        case 'spectral'
            token = 'spectral';
        otherwise
            token = lower(char(string(method)));
    end
end

function integrator = resolve_phase_method_integrator(method)
    switch normalize_method_key(method)
        case {'fd', 'spectral'}
            integrator = 'RK4';
        otherwise
            integrator = 'RK4';
    end
end

function token = method_job_key(method)
    switch normalize_method_key(method)
        case 'fd'
            token = 'fd';
        case 'spectral'
            token = 'sp';
        otherwise
            token = regexprep(lower(char(string(method))), '[^a-z0-9]+', '');
            if isempty(token)
                token = 'm';
            end
    end
end

function key = normalize_method_key(method)
    token = lower(strtrim(char(string(method))));
    token = regexprep(token, '[\s_-]+', '_');
    switch token
        case {'fd', 'finite_difference'}
            key = 'fd';
        case {'spectral', 'fft', 'pseudo_spectral'}
            key = 'spectral';
        otherwise
            key = token;
    end
end

function val = pick_value(s, field, default)
    if isstruct(s) && isfield(s, field) && ~isempty(s.(field))
        val = s.(field);
    else
        val = default;
    end
end

function out = pick_struct(s, fields, default)
    out = default;
    if ~isstruct(s)
        return;
    end
    for i = 1:numel(fields)
        if isfield(s, fields{i}) && isstruct(s.(fields{i}))
            out = s.(fields{i});
            return;
        end
    end
end

function txt = pick_text(s, fields, default)
    txt = default;
    if ~isstruct(s)
        return;
    end
    for i = 1:numel(fields)
        if isfield(s, fields{i}) && ~isempty(s.(fields{i}))
            txt = char(string(s.(fields{i})));
            return;
        end
    end
end

function val = pick_numeric(s, fields, default)
    val = default;
    if ~isstruct(s)
        return;
    end
    for i = 1:numel(fields)
        if isfield(s, fields{i}) && ~isempty(s.(fields{i})) && isnumeric(s.(fields{i}))
            candidate = double(s.(fields{i}));
            if isscalar(candidate)
                val = candidate;
                return;
            end
        end
    end
end

function offset_right_axis_label(ax)
    try
        if numel(ax.YAxis) < 2
            return;
        end
        ax.YAxis(2).Label.Units = 'normalized';
        ax.YAxis(2).Label.Position = [1.14, 0.5, 0];
        ax.YAxis(2).Label.HorizontalAlignment = 'center';
        ax.YAxis(2).Label.VerticalAlignment = 'middle';
    catch
        % Cosmetic spacing only; never block workflow export on label layout.
    end
end
