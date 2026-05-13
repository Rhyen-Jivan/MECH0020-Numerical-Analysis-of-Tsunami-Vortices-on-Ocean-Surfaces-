function [Results, paths] = Phase2BoundaryConditionStudy(Run_Config, Parameters, Settings)
% Phase2BoundaryConditionStudy - Boundary-condition workflow with FD-only runs.
    if nargin < 3
        error('Phase2BoundaryConditionStudy:InvalidInputs', ...
            'Run_Config, Parameters, and Settings are required.');
    end
    phase_cfg = resolve_phase2_config(Parameters);
    phase_id = make_phase_id(Run_Config);
    paths = build_phase_paths(Settings, phase_id);
    ensure_phase_directories(paths);

    workflow_kind = 'phase2_boundary_condition_study';
    phase_parameters = force_phase2_parameters(Parameters, phase_cfg);
    phase_settings = normalize_phase_settings(Settings, phase_cfg);
    phase_settings = configure_phase2_runtime_output_paths(phase_settings, paths);
    phase_settings = PhaseTelemetryCSVFirst.configure_phase_runtime(phase_settings, paths, phase_id, workflow_kind);
    progress_callback = resolve_progress_callback(phase_settings);
    emit_phase2_runtime_log(progress_callback, sprintf('Phase 2 preflight: initializing artifacts at %s', paths.base), 'info');
    phase_timer = tic;
    telemetry_context = PhaseTelemetryCSVFirst.start_phase_session(phase_settings, paths, phase_id, workflow_kind);
    paths.raw_hwinfo_csv_path = telemetry_context.raw_csv_path;
    paths.stage_boundaries_csv_path = telemetry_context.boundary_csv_path;

    safe_save_mat(fullfile(paths.config, 'Phase2_Config.mat'), struct( ...
        'Run_Config_clean', filter_graphics_objects(Run_Config), ...
        'phase_parameters_clean', filter_graphics_objects(phase_parameters), ...
        'phase_settings_clean', filter_graphics_objects(phase_settings), ...
        'phase_cfg_clean', filter_graphics_objects(phase_cfg)));
    write_run_settings_text(paths.run_settings_path, ...
        'Run Config', Run_Config, ...
        'Phase Parameters', phase_parameters, ...
        'Phase Settings', phase_settings, ...
        'Phase Config', phase_cfg);
    emit_phase2_runtime_log(progress_callback, sprintf('Phase 2 saved run settings: %s', paths.run_settings_path), 'info');

    jobs = build_phase_queue_jobs(phase_id, phase_parameters, phase_settings, paths, phase_cfg);
    initialize_phase2_queue_artifacts(jobs);
    child_output_root = pick_text(paths, {'matlab_data_root', 'runs_root', 'base'}, '');
    emit_phase2_runtime_log(progress_callback, sprintf('Phase 2 queue initialized: %d jobs under %s', numel(jobs), child_output_root), 'info');
    try
        queue_outputs = run_phase_queue(jobs, progress_callback, phase_id, phase_timer, telemetry_context);
        telemetry_context = PhaseTelemetryCSVFirst.stop_phase_session(telemetry_context);
    catch ME
        if isstruct(telemetry_context) && isfield(telemetry_context, 'active') && logical(telemetry_context.active)
            try
                telemetry_context = PhaseTelemetryCSVFirst.stop_phase_session(telemetry_context);
            catch stopME
                warning('Phase2BoundaryConditionStudy:TelemetryShutdownFailed', ...
                    'Phase 2 telemetry shutdown failed after workflow error: %s', stopME.message);
            end
        end
        rethrow(ME);
    end
    scenarios = assemble_scenarios(queue_outputs, phase_cfg);

    Results = struct();
    Results.run_id = phase_id;
    Results.phase_id = phase_id;
    Results.workflow_kind = 'phase2_boundary_condition_study';
    Results.result_layout_kind = 'phase2_workflow';
    Results.phase_name = 'Phase 2 boundary-condition study';
    Results.phase_config = phase_cfg;
    Results.parent_run_config = filter_graphics_objects(Run_Config);
    Results.parent_parameters = summarize_phase_parameters(phase_parameters);
    Results.scenarios = scenarios;
    Results.workflow_queue = build_queue_status_snapshot(queue_outputs_to_jobs(queue_outputs), queue_outputs, numel(queue_outputs), 'completed');
    Results.paths = paths;
    Results.wall_time = toc(phase_timer);
    Results.workflow_manifest = build_phase_workflow_manifest(phase_id, queue_outputs, phase_cfg, paths, Results.parent_parameters, Results.parent_run_config, scenarios);
    if phase2_defer_heavy_exports_requested(phase_settings)
        Results.workflow_media_artifacts = struct( ...
            'scenario_triptych_gif_path', '', ...
            'frame_count', 0, ...
            'status', 'deferred', ...
            'failure_message', '', ...
            'reason', 'host_owned_publication');
        emit_phase2_runtime_log(progress_callback, ...
            'Phase 2 deferred worker-side combined media generation; Results publication will autosave visuals on the host.', 'info');
    else
        Results.workflow_media_artifacts = export_phase2_workflow_animations(Results, phase_parameters, phase_settings, paths);
        emit_phase2_artifact_logs(progress_callback, 'Phase 2 media', Results.workflow_media_artifacts);
    end
    phase_monitor_series = build_phase2_workflow_monitor_series(queue_outputs, phase_id);
    phase_monitor_series = PhaseTelemetryCSVFirst.decorate_monitor_series(phase_monitor_series, telemetry_context);
    Results.collector_artifacts = write_phase2_workflow_collector_artifacts(Results, Run_Config, paths, phase_monitor_series);
    workbook_path = pick_text(Results.collector_artifacts, {'phase_workbook_path', 'phase_workbook_root_path'}, '');
    if ~isempty(workbook_path)
        paths.run_data_workbook_path = workbook_path;
        append_phase2_workbook_job_sheets(workbook_path, Results);
    end
    triptych_gif_path = pick_text(Results.workflow_media_artifacts, {'scenario_triptych_gif_path'}, '');
    if ~isempty(triptych_gif_path)
        paths.phase2_combined_gif_path = triptych_gif_path;
    end
    Results.paths = paths;
    Results.workflow_manifest.paths = paths;
    Results.workflow_manifest.collector_artifacts = Results.collector_artifacts;
    Results.workflow_manifest.workflow_media_artifacts = Results.workflow_media_artifacts;

    ResultsForSave = strip_phase2_for_persistence(Results);
    ResultsForSave.artifact_layout_version = char(string(paths.artifact_layout_version));
    ResultsForSave.workflow_manifest.artifact_layout_version = char(string(paths.artifact_layout_version));
    save(fullfile(paths.data, 'phase2_results.mat'), 'ResultsForSave', '-v7.3');
    emit_phase2_runtime_log(progress_callback, sprintf('Phase 2 saved MAT results: %s', fullfile(paths.data, 'phase2_results.mat')), 'info');
    if json_saving_enabled(phase_cfg, phase_settings, phase_parameters)
        write_json(fullfile(paths.data, 'phase2_results.json'), ResultsForSave);
        emit_phase2_runtime_log(progress_callback, sprintf('Phase 2 saved JSON results: %s', fullfile(paths.data, 'phase2_results.json')), 'info');
    end
    safe_save_mat(fullfile(paths.data, 'phase2_workflow_manifest.mat'), struct('workflow_manifest', ResultsForSave.workflow_manifest));
    emit_phase2_runtime_log(progress_callback, sprintf('Phase 2 saved workflow manifest MAT: %s', fullfile(paths.data, 'phase2_workflow_manifest.mat')), 'info');
    if json_saving_enabled(phase_cfg, phase_settings, phase_parameters)
        write_json(fullfile(paths.data, 'phase2_workflow_manifest.json'), ResultsForSave.workflow_manifest);
        emit_phase2_runtime_log(progress_callback, sprintf('Phase 2 saved workflow manifest JSON: %s', fullfile(paths.data, 'phase2_workflow_manifest.json')), 'info');
    end
    emit_phase2_completion_report_payload(progress_callback, ResultsForSave, paths, ...
        Run_Config, Results.parent_parameters, 'Phase 2', ...
        Results.workflow_kind, Results.result_layout_kind);
    write_phase2_report(ResultsForSave, paths);
    emit_phase2_runtime_log(progress_callback, sprintf('Phase 2 saved report: %s', fullfile(paths.reports, 'Phase2_Boundary_Case_Study_Report.md')), 'info');

    emit_phase_queue_payload(progress_callback, phase_id, jobs(end), 'completed', 100, toc(phase_timer), ...
        'Phase 2 complete: enclosed cavity, channel flow, and enclosed shear FD scenarios finished.', jobs, queue_outputs);
end

function phase_cfg = resolve_phase2_config(Parameters)
    defaults = create_default_parameters();
    if ~isfield(defaults, 'phase2') || ~isstruct(defaults.phase2)
        error('Phase2BoundaryConditionStudy:MissingDefaults', ...
            'create_default_parameters must define phase2 defaults.');
    end
    phase_cfg = defaults.phase2;
    explicit_phase2_cfg = struct();
    if isfield(Parameters, 'phase2') && isstruct(Parameters.phase2)
        explicit_phase2_cfg = Parameters.phase2;
        phase_cfg = merge_structs(phase_cfg, Parameters.phase2);
    end
    phase_cfg = sync_phase2_timestep_fields(phase_cfg, explicit_phase2_cfg);
    if ~isfield(phase_cfg, 'scenarios') || ~isstruct(phase_cfg.scenarios) || isempty(phase_cfg.scenarios)
        error('Phase2BoundaryConditionStudy:MissingScenarioDefaults', ...
            'Phase 2 defaults must define three editable scenarios.');
    end
    if numel(phase_cfg.scenarios) ~= 3
        error('Phase2BoundaryConditionStudy:ScenarioCount', ...
            'Phase 2 must define exactly three scenarios.');
    end
    for i = 1:numel(phase_cfg.scenarios)
        phase_cfg.scenarios(i) = normalize_phase2_scenario(phase_cfg.scenarios(i), i);
    end
    phase_cfg.ic_cases = normalize_phase2_ic_cases(pick_value(phase_cfg, 'ic_cases', struct([])), defaults, phase_cfg);
    phase_cfg.taylor_green_fd_grid_n = max(8, round(double(pick_value(phase_cfg, 'taylor_green_fd_grid_n', ...
        pick_value(defaults.phase2, 'taylor_green_fd_grid_n', pick_value(phase_cfg, 'fd_grid_n', 128))))));
    phase_cfg.no_initial_condition_tfinal = double(pick_value(phase_cfg, 'no_initial_condition_tfinal', 60.0));
    phase_cfg.boundary_visual_crop_cells = max(0, round(double(pick_value(phase_cfg, 'boundary_visual_crop_cells', 1))));
    phase_cfg.contour_levels = max(8, round(double(pick_value(phase_cfg, 'contour_levels', 36))));
    if ~isfield(phase_cfg, 'workflow_animation_panes') || isempty(phase_cfg.workflow_animation_panes)
        phase_cfg.workflow_animation_panes = {'evolution', 'streamfunction', 'speed', 'vector', 'contour'};
    end
    phase_cfg.workflow_scenarios = expand_phase2_workflow_scenarios(phase_cfg);
    phase_cfg.fd_first_only = true;
    phase_cfg.save_figures = logical(pick_value(phase_cfg, 'save_figures', true));
    phase_cfg.create_animations = logical(pick_value(phase_cfg, 'create_animations', false));
    phase_cfg.num_plot_snapshots = max(1, round(double(pick_value(phase_cfg, 'num_plot_snapshots', 9))));
    if ~isfield(phase_cfg, 'methods') || isempty(phase_cfg.methods)
        phase_cfg.methods = {'FD'};
    end
end

function phase_cfg = sync_phase2_timestep_fields(phase_cfg, explicit_phase2_cfg)
    if nargin < 1 || ~isstruct(phase_cfg)
        phase_cfg = struct();
    end
    if nargin < 2 || ~isstruct(explicit_phase2_cfg)
        explicit_phase2_cfg = struct();
    end

    phase_cfg.fd_dt = double(pick_value(phase_cfg, 'fd_dt', 0.01));
    phase_cfg.taylor_green_honor_fixed_dt = logical(pick_value(phase_cfg, 'taylor_green_honor_fixed_dt', true));

    has_explicit_fd_dt = isfield(explicit_phase2_cfg, 'fd_dt') && ...
        isnumeric(explicit_phase2_cfg.fd_dt) && isscalar(explicit_phase2_cfg.fd_dt) && isfinite(explicit_phase2_cfg.fd_dt);
    has_explicit_tg_dt = isfield(explicit_phase2_cfg, 'taylor_green_fd_dt') && ...
        isnumeric(explicit_phase2_cfg.taylor_green_fd_dt) && isscalar(explicit_phase2_cfg.taylor_green_fd_dt) && isfinite(explicit_phase2_cfg.taylor_green_fd_dt);

    if has_explicit_fd_dt && ~has_explicit_tg_dt
        phase_cfg.taylor_green_fd_dt = phase_cfg.fd_dt;
    else
        tg_dt = pick_value(phase_cfg, 'taylor_green_fd_dt', NaN);
        if ~(isnumeric(tg_dt) && isscalar(tg_dt) && isfinite(tg_dt) && tg_dt > 0)
            phase_cfg.taylor_green_fd_dt = phase_cfg.fd_dt;
        else
            phase_cfg.taylor_green_fd_dt = double(tg_dt);
        end
    end
end

function ic_cases = normalize_phase2_ic_cases(raw_cases, defaults, phase_cfg)
    if nargin < 2 || ~isstruct(defaults)
        defaults = create_default_parameters();
    end
    if nargin < 3 || ~isstruct(phase_cfg)
        phase_cfg = struct();
    end
    if isempty(raw_cases) || ~isstruct(raw_cases)
        raw_cases = [ ...
            struct('id', 'elliptical_vortex', 'label', 'Calibrated Elliptic Gaussian', 'ic_type', 'elliptical_vortex', 'tfinal_s', NaN), ...
            struct('id', 'taylor_green', 'label', 'Taylor-Green', 'ic_type', 'taylor_green', 'tfinal_s', NaN)];
    end
    ic_cases = repmat(struct('id', '', 'label', '', 'ic_type', '', 'tfinal_s', NaN, 'runtime_overrides', struct()), 1, numel(raw_cases));
    reference_presets = pick_value(defaults, 'reference_calibration_presets', struct());
    for i = 1:numel(raw_cases)
        ic_type = lower(strtrim(char(string(pick_text(raw_cases(i), {'ic_type', 'id'}, 'elliptical_vortex')))));
        switch ic_type
            case {'elliptic_vortex', 'elliptic', 'elliptical'}
                ic_type = 'elliptical_vortex';
            case {'none', 'zero', 'zero_initial_condition'}
                ic_type = 'no_initial_condition';
        end
        case_id = char(string(pick_text(raw_cases(i), {'id'}, ic_type)));
        label = pick_text(raw_cases(i), {'label'}, humanize_phase2_ic_case(ic_type));
        runtime_overrides = struct();
        if strcmpi(ic_type, 'elliptical_vortex') && isstruct(reference_presets) && ...
                isfield(reference_presets, 'elliptical_vortex') && isstruct(reference_presets.elliptical_vortex)
            runtime_overrides = pick_value(reference_presets.elliptical_vortex, 'runtime_overrides', struct());
        elseif strcmpi(ic_type, 'taylor_green')
            runtime_overrides = pick_struct(phase_cfg, {'taylor_green_runtime_overrides'}, struct());
            if isempty(fieldnames(runtime_overrides)) && isfield(defaults, 'phase2') && isstruct(defaults.phase2)
                runtime_overrides = pick_struct(defaults.phase2, {'taylor_green_runtime_overrides'}, struct());
            end
            if isempty(fieldnames(runtime_overrides))
                runtime_overrides = phase2_taylor_green_runtime_from_phase1(defaults);
            end
        elseif strcmpi(ic_type, 'no_initial_condition')
            runtime_overrides = struct( ...
                'ic_scenario', 'No Initial Condition', ...
                'ic_coeff', [], ...
                'ic_dynamic_values', struct(), ...
                'ic_scale', 0.0, ...
                'ic_amplitude', 0.0, ...
                'ic_count', 0, ...
                'ic_pattern', 'none', ...
                'ic_arrangement', 'none', ...
                'ic_multi_vortex_experimental', false, ...
                'ic_multi_vortex_rows', struct([]));
        end
        ic_cases(i) = struct( ...
            'id', regexprep(lower(case_id), '[^a-z0-9]+', '_'), ...
            'label', char(string(label)), ...
            'ic_type', ic_type, ...
            'tfinal_s', pick_numeric(raw_cases(i), {'tfinal_s', 'Tfinal'}, NaN), ...
            'runtime_overrides', runtime_overrides);
    end
end

function label = humanize_phase2_ic_case(ic_type)
    switch lower(char(string(ic_type)))
        case 'elliptical_vortex'
            label = 'Calibrated Elliptic Gaussian';
        case 'no_initial_condition'
            label = 'No Initial Condition';
        case 'taylor_green'
            label = 'Taylor-Green';
        otherwise
            label = strrep(char(string(ic_type)), '_', ' ');
    end
end

function runtime = phase2_taylor_green_runtime_from_phase1(defaults)
runtime = struct();
if ~isstruct(defaults) || ~isfield(defaults, 'phase1') || ~isstruct(defaults.phase1)
    return;
end
phase1 = defaults.phase1;
if ~isfield(phase1, 'ic_study') || ~isstruct(phase1.ic_study) || ...
        ~isfield(phase1.ic_study, 'catalog') || ~isstruct(phase1.ic_study.catalog)
    return;
end
catalog = phase1.ic_study.catalog;
for i = 1:numel(catalog)
    if strcmpi(pick_text(catalog(i), {'case_id'}, ''), 'taylor_green')
        runtime = pick_struct(catalog(i), {'runtime_overrides'}, struct());
        return;
    end
end
end

function scenarios = expand_phase2_workflow_scenarios(phase_cfg)
    base_scenarios = phase_cfg.scenarios;
    ic_cases = phase_cfg.ic_cases;
    scenarios = repmat(empty_phase2_workflow_scenario(), 1, 0);
    for ic_idx = 1:numel(ic_cases)
        for bc_idx = 1:numel(base_scenarios)
            base = base_scenarios(bc_idx);
            scenario = empty_phase2_workflow_scenario();
            scenario.id = sprintf('%s__%s', ic_cases(ic_idx).id, base.id);
            scenario.label = sprintf('%s | %s', ic_cases(ic_idx).label, base.label);
            scenario.top_speed_mps = base.top_speed_mps;
            scenario.bottom_speed_mps = base.bottom_speed_mps;
            scenario.left_speed_mps = base.left_speed_mps;
            scenario.right_speed_mps = base.right_speed_mps;
            scenario.base_scenario_id = base.id;
            scenario.base_scenario_label = base.label;
            scenario.ic_case_id = ic_cases(ic_idx).id;
            scenario.ic_case_label = ic_cases(ic_idx).label;
            scenario.ic_type = ic_cases(ic_idx).ic_type;
            scenario.ic_runtime_overrides = ic_cases(ic_idx).runtime_overrides;
            scenario.ic_tfinal_s = ic_cases(ic_idx).tfinal_s;
            scenarios(end + 1) = scenario; %#ok<AGROW>
        end
    end
end

function scenario = empty_phase2_workflow_scenario()
    scenario = struct( ...
        'id', '', ...
        'label', '', ...
        'top_speed_mps', 0.0, ...
        'bottom_speed_mps', 0.0, ...
        'left_speed_mps', 0.0, ...
        'right_speed_mps', 0.0, ...
        'base_scenario_id', '', ...
        'base_scenario_label', '', ...
        'ic_case_id', '', ...
        'ic_case_label', '', ...
        'ic_type', '', ...
        'ic_runtime_overrides', struct(), ...
        'ic_tfinal_s', NaN);
end

function scenarios = phase2_workflow_scenarios(phase_cfg)
    if isfield(phase_cfg, 'workflow_scenarios') && isstruct(phase_cfg.workflow_scenarios) && ~isempty(phase_cfg.workflow_scenarios)
        scenarios = phase_cfg.workflow_scenarios;
    else
        scenarios = expand_phase2_workflow_scenarios(phase_cfg);
    end
end

function scenario = normalize_phase2_scenario(scenario, index)
    scenario = filter_graphics_objects(scenario);
    scenario.id = char(string(pick_text(scenario, {'id'}, sprintf('scenario_%d', index))));
    scenario.id = BCDispatcher.extract_bc_case(struct('bc_case', scenario.id));
    scenario.label = char(string(pick_text(scenario, {'label'}, humanize_phase2_scenario(scenario.id))));
    scenario.top_speed_mps = double(pick_value(scenario, 'top_speed_mps', 0.0));
    scenario.bottom_speed_mps = double(pick_value(scenario, 'bottom_speed_mps', 0.0));
    scenario.left_speed_mps = double(pick_value(scenario, 'left_speed_mps', 0.0));
    scenario.right_speed_mps = double(pick_value(scenario, 'right_speed_mps', 0.0));
end

function label = humanize_phase2_scenario(case_id)
    switch lower(char(string(case_id)))
        case 'lid_driven_cavity'
            label = 'Enclosed Driven Cavity';
        case 'driven_channel_flow'
            label = 'Driven Channel Flow';
        case 'enclosed_shear_layer'
            label = 'Enclosed Shear Layer';
        otherwise
            label = strrep(char(string(case_id)), '_', ' ');
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
    phase_id = sprintf('phase2_%s_%s', ic, char(datetime('now', 'Format', 'yyyyMMdd_HHmmss')));
end

function paths = build_phase_paths(Settings, phase_id)
    output_root = 'Results';
    if isfield(Settings, 'output_root') && ~isempty(Settings.output_root)
        output_root = Settings.output_root;
    end
    paths = PathBuilder.get_phase_paths('Phase2', phase_id, output_root);
    paths.phase_id = phase_id;
end

function ensure_phase_directories(paths)
    candidate_fields = { ...
        'base', ...
        'matlab_data_root', ...
        'metrics_root', ...
        'visuals_root', ...
        'config', ...
        'runs_root'};
    mkdir_targets = {};
    for i = 1:numel(candidate_fields)
        if isfield(paths, candidate_fields{i}) && ~isempty(paths.(candidate_fields{i}))
            mkdir_targets{end + 1} = char(string(paths.(candidate_fields{i}))); %#ok<AGROW>
        end
    end
    mkdir_targets = unique(mkdir_targets);
    for i = 1:numel(mkdir_targets)
        if exist(mkdir_targets{i}, 'dir') ~= 7
            mkdir(mkdir_targets{i});
        end
    end
end

function params = force_phase2_parameters(Parameters, phase_cfg)
    params = Parameters;
    params.phase2 = phase_cfg;
    params.create_animations = false;
    params.bathymetry_dimension_policy = 'by_method';
    if ~isfield(params, 'resource_strategy') || isempty(params.resource_strategy) || ...
            strcmpi(char(string(params.resource_strategy)), 'mode_adaptive')
        params.resource_strategy = pick_text(phase_cfg, {'resource_strategy'}, 'throughput_first');
    end
end

function settings = normalize_phase_settings(SettingsInput, phase_cfg)
    settings = Settings();
    if nargin >= 1 && isstruct(SettingsInput)
        settings = merge_structs(settings, SettingsInput);
    end
    settings.save_data = true;
    settings.save_reports = logical(pick_value(phase_cfg, 'save_reports', true));
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

function settings = configure_phase2_runtime_output_paths(settings, paths)
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

function initialize_phase2_queue_artifacts(jobs)
    if nargin < 1 || ~isstruct(jobs) || isempty(jobs)
        return;
    end
    for i = 1:numel(jobs)
        base_root = pick_text(jobs(i), {'output_root'}, '');
        if strlength(string(base_root)) == 0
            continue;
        end
        paths = PathBuilder.get_existing_root_paths(base_root, pick_text(jobs(i), {'method'}, 'FD'), 'Evolution');
        PathBuilder.ensure_directories(paths);
        PathBuilder.ensure_run_settings_placeholder(paths.run_settings_path, pick_text(jobs(i), {'job_key'}, sprintf('job_%02d', i)));
    end
end

function emit_phase2_runtime_log(progress_callback, message, log_type)
    if nargin < 3 || isempty(log_type)
        log_type = 'info';
    end
    if isempty(progress_callback) || ~isa(progress_callback, 'function_handle')
        return;
    end
    payload = struct('channel', 'log', 'log_message', char(string(message)), 'log_type', char(string(log_type)));
    try
        invoke_runtime_progress_callback(progress_callback, payload);
    catch
    end
end

function emit_phase2_completion_report_payload(progress_callback, results_for_save, paths, run_config, parameters, phase_label, workflow_kind, result_layout_kind)
    if isempty(progress_callback) || ~isa(progress_callback, 'function_handle') || ...
            ~isstruct(results_for_save) || exist('emit_completion_report_payload', 'file') ~= 2
        return;
    end
    if nargin < 4 || ~isstruct(run_config)
        run_config = struct();
    end
    if nargin < 5 || ~isstruct(parameters)
        parameters = struct();
    end
    if nargin < 6 || strlength(string(phase_label)) == 0
        phase_label = 'Phase 2';
    end
    if nargin < 7 || strlength(string(workflow_kind)) == 0
        workflow_kind = pick_text(results_for_save, {'workflow_kind'}, 'phase2_boundary_condition_study');
    end
    if nargin < 8 || strlength(string(result_layout_kind)) == 0
        result_layout_kind = pick_text(results_for_save, {'result_layout_kind'}, 'phase2_workflow');
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
    emit_completion_report_payload(progress_callback, results_for_save, paths, published_run_config, parameters, struct( ...
        'phase_label', phase_label, ...
        'workflow_kind', workflow_kind, ...
        'result_layout_kind', result_layout_kind, ...
        'result_publication_mode', 'manual', ...
        'completion_results_already_persisted', true));
end

function tf = phase2_defer_heavy_exports_requested(settings)
    tf = false;
    if nargin < 1 || ~isstruct(settings)
        return;
    end
    if exist('defer_heavy_result_artifacts_requested', 'file') ~= 2
        return;
    end
    tf = logical(defer_heavy_result_artifacts_requested(settings));
end

function emit_phase2_artifact_logs(progress_callback, label_prefix, payload)
    if isempty(progress_callback) || ~isa(progress_callback, 'function_handle')
        return;
    end
    if isstruct(payload)
        fields = fieldnames(payload);
        for i = 1:numel(fields)
            emit_phase2_artifact_logs(progress_callback, sprintf('%s %s', char(string(label_prefix)), fields{i}), payload.(fields{i}));
        end
        return;
    end
    if iscell(payload)
        for i = 1:numel(payload)
            emit_phase2_artifact_logs(progress_callback, sprintf('%s %d', char(string(label_prefix)), i), payload{i});
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
    emit_phase2_runtime_log(progress_callback, sprintf('Saved %s: %s', char(string(label_prefix)), path_text), 'info');
end

function progress_callback = resolve_progress_callback(Settings)
    progress_callback = resolve_runtime_progress_callback(Settings);
end

function jobs = build_phase_queue_jobs(phase_id, params, settings, paths, phase_cfg)
    methods = resolve_phase2_methods(phase_cfg);
    scenarios = phase2_workflow_scenarios(phase_cfg);
    jobs = repmat(empty_job(), 1, numel(scenarios) * numel(methods));
    cursor = 1;
    for i = 1:numel(scenarios)
        for j = 1:numel(methods)
            jobs(cursor) = build_scenario_job(methods{j}, cursor, phase_id, params, settings, paths, scenarios(i));
            cursor = cursor + 1;
        end
    end
end

function job = build_scenario_job(method_name, queue_index, phase_id, params, settings, paths, scenario)
    p = apply_phase2_scenario_to_parameters(params, scenario);
    p.method = method_to_parameter_token(method_name);
    p.analysis_method = method_name;
    p.mode = 'Evolution';
    p.time_integrator = resolve_phase_method_integrator(method_name);
    plot_snapshot_count = resolve_phase2_plot_snapshot_count(params);
    animation_frame_count = resolve_phase2_animation_frame_count(params, settings);
    p.create_animations = false;
    p.num_plot_snapshots = plot_snapshot_count;
    p.animation_num_frames = animation_frame_count;
    p.num_animation_frames = animation_frame_count;
    p.num_snapshots = max(plot_snapshot_count, animation_frame_count);
    p = normalize_snapshot_schedule_parameters(p);

    rc = Build_Run_Config(method_name, 'Evolution', pick_text(p, {'ic_type'}, pick_text(params, {'ic_type'}, '')));
    rc.run_id = make_phase_child_identifier(phase_id, queue_index, scenario.id, method_name);
    rc.phase_id = phase_id;
    rc.phase_label = 'Phase 2';
    rc.phase_scenario_id = scenario.id;
    rc.phase_scenario_label = scenario.label;
    rc.phase2_base_scenario_id = pick_text(scenario, {'base_scenario_id'}, scenario.id);
    rc.phase2_ic_case_id = pick_text(scenario, {'ic_case_id'}, pick_text(p, {'ic_type'}, ''));
    rc.ic_type = pick_text(p, {'ic_type'}, pick_text(params, {'ic_type'}, ''));

    job_key = sprintf('%s_%s', normalize_method_key(method_name), scenario.id);
    job = make_job(sprintf('%s | %s', scenario.label, method_name), method_name, 'evolution', ...
        job_key, queue_index, rc, p, settings, paths, scenario);
end

function p = apply_phase2_scenario_to_parameters(params, scenario)
    p = params;
    phase_cfg = pick_value(params, 'phase2', struct());
    p = apply_phase2_ic_case_to_parameters(p, scenario, phase_cfg);
    bc_case = pick_text(scenario, {'base_scenario_id'}, pick_text(scenario, {'id'}, 'lid_driven_cavity'));
    p.bc_case = bc_case;
    p.boundary_condition_case = bc_case;
    p.phase2_active_scenario = scenario.id;
    p.phase2_active_scenario_label = scenario.label;
    p.phase2_base_scenario = bc_case;
    p.phase2_base_scenario_label = pick_text(scenario, {'base_scenario_label'}, scenario.label);
    p.phase2_ic_case_id = pick_text(scenario, {'ic_case_id'}, pick_text(p, {'ic_type'}, ''));
    p.phase2_ic_case_label = pick_text(scenario, {'ic_case_label'}, pick_text(p, {'ic_type'}, ''));
    p.allow_preset_speed_overrides = true;
    p.U_top = scenario.top_speed_mps;
    p.U_bottom = scenario.bottom_speed_mps;
    p.U_left = scenario.left_speed_mps;
    p.U_right = scenario.right_speed_mps;
    p.Nx = max(8, round(pick_numeric(phase_cfg, {'fd_grid_n'}, pick_numeric(params, {'Nx'}, 128))));
    if strcmpi(pick_text(p, {'ic_type'}, ''), 'taylor_green')
        p.Nx = max(8, round(pick_numeric(phase_cfg, {'taylor_green_fd_grid_n'}, p.Nx)));
    end
    p.Ny = p.Nx;
    p.dt = max(eps, resolve_phase2_dt_for_ic(phase_cfg, p, params));
    p.plot_trim_layers = max(0, round(pick_numeric(phase_cfg, {'boundary_visual_crop_cells'}, 1)));
    p.plot_limit_mode = 'trimmed_interior_extrema';
    p.contour_levels = max(8, round(pick_numeric(phase_cfg, {'contour_levels'}, 36)));
    p.taylor_green_honor_fixed_dt = strcmpi(pick_text(p, {'ic_type'}, ''), 'taylor_green') && ...
        logical(pick_value(phase_cfg, 'taylor_green_honor_fixed_dt', false));
    p = sync_phase2_boundary_metadata(p);
    p = apply_phase2_fixed_step_stability_contract(p, phase_cfg);
end

function dt_value = resolve_phase2_dt_for_ic(phase_cfg, scenario_params, fallback_params)
    dt_value = pick_numeric(phase_cfg, {'fd_dt'}, pick_numeric(fallback_params, {'dt'}, 0.01));
    if strcmpi(pick_text(scenario_params, {'ic_type'}, ''), 'taylor_green')
        dt_value = pick_numeric(phase_cfg, {'taylor_green_fd_dt'}, dt_value);
    end
    dt_value = double(dt_value);
end

function p = sync_phase2_boundary_metadata(p)
    try
        bc = BCDispatcher.resolve(p, 'fd', struct());
        common = bc.common;
        p.bc_top = bc.bc_top;
        p.bc_bottom = bc.bc_bottom;
        p.bc_left = bc.bc_left;
        p.bc_right = bc.bc_right;
        p.bc_top_math = common.sides.top.math_type;
        p.bc_bottom_math = common.sides.bottom.math_type;
        p.bc_left_math = common.sides.left.math_type;
        p.bc_right_math = common.sides.right.math_type;
        p.bc_top_physical = common.sides.top.physical_type;
        p.bc_bottom_physical = common.sides.bottom.physical_type;
        p.bc_left_physical = common.sides.left.physical_type;
        p.bc_right_physical = common.sides.right.physical_type;
    catch
        % Keep the scenario metadata already stored on the parameter payload
        % when boundary preview resolution is unavailable.
    end
end

function p = apply_phase2_fixed_step_stability_contract(p, phase_cfg)
    adaptive_cfg = normalize_phase2_adaptive_timestep_config( ...
        pick_value(phase_cfg, 'adaptive_timestep', struct('enabled', false, 'C_adv', 0.5, 'C_diff', 0.25)));
    if ~isfield(p, 'phase2') || ~isstruct(p.phase2)
        p.phase2 = struct();
    end
    p.phase2.adaptive_timestep = adaptive_cfg;
    p.phase2.adaptive_timestep.enabled = false;
    p.phase2_adaptive_timestep_enabled = false;

    nu_floor = pick_numeric(phase_cfg, {'boundary_nu_floor'}, 1.0e-2);
    if isfinite(nu_floor) && nu_floor >= 0
        p.nu = max(double(p.nu), double(nu_floor));
    end

    dt_max = pick_numeric(phase_cfg, {'boundary_dt_max'}, inf);
    if isfinite(dt_max) && dt_max > 0
        p.dt = min(double(p.dt), double(dt_max));
    end

    p.phase2_boundary_nu_floor = nu_floor;
    p.phase2_boundary_dt_max = dt_max;

    [p, meta] = apply_taylor_green_timestep_cap(p, adaptive_cfg, 'phase2_boundary_condition_study');
    if meta.applied
        p.phase2_taylor_green_timestep_meta = meta;
    end
end

function adaptive_cfg = normalize_phase2_adaptive_timestep_config(adaptive_cfg)
    if nargin < 1 || ~isstruct(adaptive_cfg)
        adaptive_cfg = struct();
    end
    adaptive_cfg.enabled = logical(pick_value(adaptive_cfg, 'enabled', true));
    adaptive_cfg.C_adv = double(pick_value(adaptive_cfg, 'C_adv', 0.5));
    adaptive_cfg.C_diff = double(pick_value(adaptive_cfg, 'C_diff', 0.25));
    if ~(isfinite(adaptive_cfg.C_adv) && adaptive_cfg.C_adv > 0)
        adaptive_cfg.C_adv = 0.5;
    end
    if ~(isfinite(adaptive_cfg.C_diff) && adaptive_cfg.C_diff > 0)
        adaptive_cfg.C_diff = 0.25;
    end
end

function p = apply_phase2_ic_case_to_parameters(p, scenario, phase_cfg)
    ic_type = pick_text(scenario, {'ic_type'}, pick_text(p, {'ic_type'}, 'elliptical_vortex'));
    p.ic_type = char(string(ic_type));
    overrides = pick_struct(scenario, {'ic_runtime_overrides'}, struct());
    if isstruct(overrides) && ~isempty(fieldnames(overrides))
        override_fields = fieldnames(overrides);
        for i = 1:numel(override_fields)
            p.(override_fields{i}) = overrides.(override_fields{i});
        end
    end
    if strcmpi(ic_type, 'no_initial_condition')
        p.ic_coeff = [];
        p.ic_dynamic_values = struct();
        p.ic_scale = 0.0;
        p.ic_amplitude = 0.0;
        p.ic_count = 0;
        p.ic_pattern = 'none';
        p.ic_arrangement = 'none';
        p.ic_multi_vortex_experimental = false;
        p.ic_multi_vortex_rows = struct([]);
        t_no_ic = pick_numeric(scenario, {'ic_tfinal_s'}, NaN);
        if ~isfinite(t_no_ic)
            t_no_ic = pick_numeric(phase_cfg, {'no_initial_condition_tfinal'}, 60.0);
        end
        p.Tfinal = t_no_ic;
        p.t_final = t_no_ic;
    end
end

function count = resolve_phase2_plot_snapshot_count(params)
    count = max(1, round(double(pick_value(pick_value(params, 'phase2', struct()), ...
        'num_plot_snapshots', pick_value(params, 'num_plot_snapshots', 9)))));
end

function count = resolve_phase2_animation_frame_count(params, settings)
    count = NaN;
    phase_cfg = pick_value(params, 'phase2', struct());
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
        count = resolve_phase2_plot_snapshot_count(params);
    end
    count = max(2, round(count));
end

function outputs = run_phase_queue(jobs, progress_callback, phase_id, phase_timer, telemetry_context)
    outputs = repmat(empty_output(), 1, numel(jobs));
    for i = 1:numel(jobs)
        emit_phase_queue_payload(progress_callback, phase_id, jobs(i), 'queued', 0, toc(phase_timer), ...
            sprintf('Queued Phase 2 scenario job %d/%d: %s | %s', i, numel(jobs), jobs(i).scenario.label, jobs(i).method), jobs, outputs);
    end
    for i = 1:numel(jobs)
        running_pct = 100 * ((i - 1) / max(numel(jobs), 1));
        emit_phase_queue_payload(progress_callback, phase_id, jobs(i), 'running', running_pct, toc(phase_timer), ...
            sprintf('Starting Phase 2 scenario job %d/%d: %s | %s', i, numel(jobs), jobs(i).scenario.label, jobs(i).method), jobs, outputs);
        append_phase2_job_boundary(telemetry_context, 'start', jobs(i), toc(phase_timer));
        try
            [result_payload, path_payload] = run_dispatched_job(jobs(i).run_config, jobs(i).parameters, jobs(i).settings);
            outputs(i) = make_output(jobs(i), result_payload, path_payload, 'dispatcher_queue');
            outputs(i) = promote_output_quick_access(outputs(i), jobs(i).output_root);
            completed_pct = 100 * (i / max(numel(jobs), 1));
            append_phase2_job_boundary(telemetry_context, 'end', jobs(i), toc(phase_timer));
            emit_phase_queue_payload(progress_callback, phase_id, jobs(i), 'completed', completed_pct, toc(phase_timer), ...
                build_child_completion_message(jobs(i), outputs(i), i, numel(jobs)), jobs, outputs);
        catch ME
            outputs(i) = make_failed_output(jobs(i), ME);
            append_phase2_job_boundary(telemetry_context, 'end', jobs(i), toc(phase_timer));
            emit_phase_queue_payload(progress_callback, phase_id, jobs(i), 'failed', NaN, toc(phase_timer), ...
                sprintf('Phase 2 scenario failed: [%s] %s', ME.identifier, ME.message), jobs, outputs);
            rethrow(ME);
        end
    end
end

function [result_payload, path_payload] = run_dispatched_job(run_config, parameters, settings)
    child_run_config = run_config;
    if isfield(child_run_config, 'workflow_kind')
        child_run_config = rmfield(child_run_config, 'workflow_kind');
    end
    child_live_monitor = phase2_child_telemetry_requested(settings);
    collectors_enabled = phase2_child_external_collectors_enabled(settings);
    dispatch_settings = settings;
    dispatch_settings.force_synchronous_execution = true;
    dispatch_settings.suppress_standard_completion_payload = true;
    dispatch_settings = disable_phase2_child_external_collectors(dispatch_settings);
    if isfield(dispatch_settings, 'progress_data_queue')
        dispatch_settings = rmfield(dispatch_settings, 'progress_data_queue');
    end
    if child_live_monitor
        dispatch_settings.ui_progress_callback = resolve_runtime_progress_callback(settings);
    end
    [result_payload, path_payload] = RunDispatcher(child_run_config, parameters, dispatch_settings);
    if collectors_enabled
        result_payload = attach_phase2_child_collector_probe(result_payload, settings);
    end
end

function append_phase2_job_boundary(telemetry_context, boundary_event, job, elapsed_wall)
    if nargin < 4
        elapsed_wall = NaN;
    end
    if ~(isstruct(telemetry_context) && isfield(telemetry_context, 'enabled') && logical(telemetry_context.enabled))
        return;
    end
    PhaseTelemetryCSVFirst.append_boundary(telemetry_context, boundary_event, struct( ...
        'session_time_s', double(elapsed_wall), ...
        'stage_id', sprintf('scenario_%s', char(string(job.scenario.id))), ...
        'stage_label', char(string(job.scenario.label)), ...
        'stage_type', 'scenario', ...
        'substage_id', lower(char(string(job.method))), ...
        'substage_label', char(string(job.method)), ...
        'substage_type', 'method_run', ...
        'stage_method', char(string(job.method)), ...
        'scenario_id', char(string(job.scenario.id)), ...
        'base_scenario_id', pick_text(job.scenario, {'base_scenario_id'}, job.scenario.id), ...
        'ic_case_id', pick_text(job.scenario, {'ic_case_id'}, ''), ...
        'mesh_level', NaN, ...
        'mesh_nx', pick_numeric(job.parameters, {'Nx'}, NaN), ...
        'mesh_ny', pick_numeric(job.parameters, {'Ny'}, NaN), ...
        'child_run_index', double(job.queue_index)));
end

function emit_phase_queue_payload(progress_callback, phase_id, job, status, progress_pct, elapsed_wall, terminal_message, jobs, outputs)
    if isempty(progress_callback)
        return;
    end
    queue_total = numel(jobs);
    queue_status = build_queue_status_snapshot(jobs, outputs, job.queue_index, status);
    current_output = empty_output();
    if numel(outputs) >= job.queue_index && isstruct(outputs(job.queue_index))
        current_output = outputs(job.queue_index);
    end
    child_run_id = pick_text(current_output.run_config, {'run_id', 'study_id'}, pick_text(job.run_config, {'run_id', 'study_id'}, ''));
    child_artifact_root = pick_text(current_output.paths, {'base'}, char(string(job.output_root)));
    child_figures_root = pick_text(current_output.paths, {'figures_root', 'figures_evolution'}, '');
    child_reports_root = pick_text(current_output.paths, {'reports'}, '');

    payload = struct();
    payload.channel = 'workflow';
    payload.phase = 'phase2';
    payload.phase_id = phase_id;
    payload.run_id = phase_id;
    payload.workflow_kind = 'phase2_boundary_condition_study';
    payload.stage_name = job.job_key;
    payload.stage_index = double(job.queue_index);
    payload.stage_total = double(queue_total);
    payload.queue_index = double(job.queue_index);
    payload.queue_total = double(queue_total);
    payload.job_key = job.job_key;
    payload.job_label = job.scenario.label;
    payload.method = job.method;
    payload.mode = job.stage;
    payload.status = char(string(status));
    payload.artifact_root = char(string(job.output_root));
    payload.child_run_id = child_run_id;
    payload.child_artifact_root = child_artifact_root;
    payload.child_figures_root = child_figures_root;
    payload.child_reports_root = child_reports_root;
    payload.mesh_nx = double(pick_numeric(job.parameters, {'Nx'}, NaN));
    payload.mesh_ny = double(pick_numeric(job.parameters, {'Ny'}, NaN));
    payload.test_case_setup = build_phase2_test_case_setup(job.scenario);
    payload.scenario_label = job.scenario.label;
    payload.workflow_overall_progress_pct = double(progress_pct);
    payload.progress_pct = double(progress_pct);
    payload.elapsed_wall = double(elapsed_wall);
    payload.status_text = sprintf('Phase 2 [%d/%d] %s (%s)', round(double(job.queue_index)), round(double(queue_total)), job.scenario.label, char(string(status)));
    payload.event_key = sprintf('%s_%02d_%s_%s', phase_id, round(double(job.queue_index)), job.job_key, lower(char(string(status))));
    payload.queue_status = queue_status;
    payload.terminal_message = char(string(terminal_message));
    try
        invoke_runtime_progress_callback(progress_callback, payload);
    catch ME
        warning('Phase2BoundaryConditionStudy:ProgressCallbackDisabled', ...
            'Phase workflow progress callback failed and will be ignored: %s', ME.message);
    end
end

function queue_status = build_queue_status_snapshot(jobs, outputs, active_index, active_status)
    queue_status = repmat(struct('queue_index', NaN, 'job_key', '', 'job_label', '', 'method', '', ...
        'mode', '', 'mesh_nx', NaN, 'mesh_ny', NaN, 'test_case_setup', '', 'scenario_label', '', 'status', 'queued', 'run_id', '', ...
        'artifact_root', '', 'figures_root', '', 'reports_root', ''), 1, numel(jobs));
    for i = 1:numel(jobs)
        queue_status(i).queue_index = jobs(i).queue_index;
        queue_status(i).job_key = jobs(i).job_key;
        queue_status(i).job_label = jobs(i).scenario.label;
        queue_status(i).method = jobs(i).method;
        queue_status(i).mode = jobs(i).stage;
        queue_status(i).artifact_root = jobs(i).output_root;
        queue_status(i).mesh_nx = pick_numeric(jobs(i).parameters, {'Nx'}, NaN);
        queue_status(i).mesh_ny = pick_numeric(jobs(i).parameters, {'Ny'}, NaN);
        queue_status(i).test_case_setup = build_phase2_test_case_setup(jobs(i).scenario);
        queue_status(i).scenario_label = jobs(i).scenario.label;
        if nargin >= 2 && numel(outputs) >= i && isstruct(outputs(i)) && isfield(outputs(i), 'status') && ~isempty(outputs(i).status)
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
            queue_status(i).artifact_root = pick_text(outputs(i).paths, {'base'}, queue_status(i).artifact_root);
            queue_status(i).figures_root = pick_text(outputs(i).paths, {'figures_root', 'figures_evolution'}, '');
            queue_status(i).reports_root = pick_text(outputs(i).paths, {'reports'}, '');
        end
    end
end

function message = build_child_completion_message(job, output, queue_index, queue_total)
    analysis = require_analysis(output.results, job.label);
    nx = pick_numeric(analysis, {'Nx'}, pick_numeric(job.parameters, {'Nx'}, NaN));
    ny = pick_numeric(analysis, {'Ny'}, pick_numeric(job.parameters, {'Ny'}, NaN));
    message = sprintf('Completed Phase 2 scenario job %d/%d: %s | %s | mesh=%dx%d', ...
        queue_index, queue_total, job.scenario.label, job.method, round(nx), round(ny));
end

function text = build_phase2_test_case_setup(scenario)
    scenario_id = pick_text(scenario, {'base_scenario_id', 'id'}, '');
    ic_case = pick_text(scenario, {'ic_case_id', 'ic_type'}, '');
    parts = {sprintf('bc_case=%s', char(string(scenario_id)))};
    if ~isempty(ic_case)
        parts{end + 1} = sprintf('ic=%s', char(string(ic_case))); %#ok<AGROW>
    end
    text = strjoin(parts, ' | ');
end

function scenarios = assemble_scenarios(queue_outputs, phase_cfg)
    workflow_scenarios = phase2_workflow_scenarios(phase_cfg);
    scenarios = repmat(struct( ...
        'scenario_id', '', ...
        'scenario_label', '', ...
        'base_scenario_id', '', ...
        'base_scenario_label', '', ...
        'ic_case_id', '', ...
        'ic_case_label', '', ...
        'ic_type', '', ...
        'result_layout_kind', 'phase2_scenario', ...
        'fd', struct(), ...
        'spectral', struct(), ...
        'combined', struct(), ...
        'summary', struct()), 1, numel(workflow_scenarios));

    for i = 1:numel(workflow_scenarios)
        scenario = workflow_scenarios(i);
        fd_output = resolve_phase2_output(queue_outputs, scenario.id, 'fd');
        spectral_output = try_resolve_phase2_output(queue_outputs, scenario.id, 'spectral');
        has_spectral = isstruct(spectral_output) && isfield(spectral_output, 'results') && ...
            isstruct(spectral_output.results) && ~isempty(fieldnames(spectral_output.results));
        fd_summary = build_phase_child_view_summary(fd_output);
        spectral_reason = 'Phase 2 is configured for FD-only scenario runs.';
        spectral_payload = struct( ...
            'supported', false, ...
            'method', 'Spectral', ...
            'reason', spectral_reason, ...
            'paths', struct(), ...
            'view_summary', struct(), ...
            'metadata', struct(), ...
            'output', struct());
        if has_spectral
            spectral_summary = build_phase_child_view_summary(spectral_output);
            spectral_reason = '';
            spectral_payload = struct( ...
                'supported', true, ...
                'method', 'Spectral', ...
                'reason', '', ...
                'paths', spectral_output.paths, ...
                'view_summary', spectral_summary, ...
                'metadata', build_phase_child_metadata(spectral_output), ...
                'output', strip_heavy_outputs(spectral_output));
            combined = build_phase2_combined_payload(fd_output, spectral_output, phase_cfg);
        else
            combined = build_phase2_fd_only_combined_payload(fd_output, phase_cfg, spectral_reason);
        end
        summary = struct( ...
            'scenario_id', scenario.id, ...
            'scenario_label', scenario.label, ...
            'base_scenario_id', pick_text(scenario, {'base_scenario_id'}, scenario.id), ...
            'base_scenario_label', pick_text(scenario, {'base_scenario_label'}, scenario.label), ...
            'ic_case_id', pick_text(scenario, {'ic_case_id'}, ''), ...
            'ic_case_label', pick_text(scenario, {'ic_case_label'}, ''), ...
            'ic_type', pick_text(scenario, {'ic_type'}, ''), ...
            'fd_supported', true, ...
            'spectral_supported', logical(has_spectral), ...
            'spectral_reason', spectral_reason, ...
            'artifact_root', pick_text(fd_output.paths, {'base'}, ''), ...
            'figures_root', pick_text(fd_output.paths, {'figures_root', 'figures_evolution'}, ''), ...
            'reports_root', pick_text(fd_output.paths, {'reports'}, ''), ...
            'runtime_wall_s', pick_numeric(fd_output.results, {'wall_time', 'total_time'}, fd_output.wall_time), ...
            'spectral_runtime_wall_s', pick_numeric(pick_struct(spectral_output, {'results'}, struct()), {'wall_time', 'total_time'}, NaN), ...
            'mesh_nx', pick_numeric(fd_output.parameters, {'Nx'}, NaN), ...
            'mesh_ny', pick_numeric(fd_output.parameters, {'Ny'}, NaN), ...
            'dt', pick_numeric(fd_output.parameters, {'dt'}, NaN), ...
            'Tfinal', pick_numeric(fd_output.parameters, {'Tfinal', 't_final'}, NaN), ...
            'spectral_mesh_nx', pick_numeric(pick_struct(spectral_output, {'parameters'}, struct()), {'Nx'}, NaN), ...
            'spectral_mesh_ny', pick_numeric(pick_struct(spectral_output, {'parameters'}, struct()), {'Ny'}, NaN), ...
            'spectral_dt', pick_numeric(pick_struct(spectral_output, {'parameters'}, struct()), {'dt'}, NaN), ...
            'spectral_Tfinal', pick_numeric(pick_struct(spectral_output, {'parameters'}, struct()), {'Tfinal', 't_final'}, NaN));

        scenarios(i).scenario_id = scenario.id;
        scenarios(i).scenario_label = scenario.label;
        scenarios(i).base_scenario_id = pick_text(scenario, {'base_scenario_id'}, scenario.id);
        scenarios(i).base_scenario_label = pick_text(scenario, {'base_scenario_label'}, scenario.label);
        scenarios(i).ic_case_id = pick_text(scenario, {'ic_case_id'}, '');
        scenarios(i).ic_case_label = pick_text(scenario, {'ic_case_label'}, '');
        scenarios(i).ic_type = pick_text(scenario, {'ic_type'}, '');
        scenarios(i).fd = struct( ...
            'supported', true, ...
            'output', strip_heavy_outputs(fd_output), ...
            'view_summary', fd_summary);
        scenarios(i).spectral = spectral_payload;
        scenarios(i).combined = combined;
        scenarios(i).summary = summary;
    end
end

function summary = build_phase_child_view_summary(evolution_output)
    meta = build_phase_child_metadata(evolution_output);
    summary = struct( ...
        'results', evolution_output.results, ...
        'parameters', evolution_output.parameters, ...
        'run_config', evolution_output.run_config, ...
        'analysis', require_analysis(evolution_output.results, evolution_output.label), ...
        'paths', evolution_output.paths, ...
        'metadata', meta, ...
        'wall_time', evolution_output.wall_time, ...
        'workflow_child', true);
end

function meta = build_phase_child_metadata(evolution_output)
    meta = struct();
    meta.method = evolution_output.method;
    meta.mode = 'Evolution';
    meta.ic_type = pick_text(evolution_output.run_config, {'ic_type'}, '');
    meta.bc_case = pick_text(evolution_output.parameters, {'boundary_condition_case', 'bc_case'}, '');
    meta.run_id = pick_text(evolution_output.run_config, {'run_id', 'study_id'}, '');
    meta.timestamp = char(datetime('now', 'Format', 'yyyy-MM-dd HH:mm:ss'));
    meta.wall_time = pick_numeric(evolution_output.results, {'wall_time', 'total_time'}, evolution_output.wall_time);
    meta.max_omega = pick_numeric(evolution_output.results, {'max_omega'}, NaN);
    meta.total_steps = pick_numeric(evolution_output.results, {'total_steps'}, NaN);
    meta.scenario_id = pick_text(evolution_output.scenario, {'id'}, '');
    meta.scenario_label = pick_text(evolution_output.scenario, {'label'}, '');
    meta.base_scenario_id = pick_text(evolution_output.scenario, {'base_scenario_id'}, meta.scenario_id);
    meta.base_scenario_label = pick_text(evolution_output.scenario, {'base_scenario_label'}, meta.scenario_label);
    meta.ic_case_id = pick_text(evolution_output.scenario, {'ic_case_id'}, '');
    meta.ic_case_label = pick_text(evolution_output.scenario, {'ic_case_label'}, '');
    meta.num_plot_snapshots = pick_numeric(evolution_output.parameters, {'num_plot_snapshots'}, NaN);
    meta.animation_num_frames = pick_numeric(evolution_output.parameters, {'animation_num_frames', 'num_animation_frames'}, NaN);
    meta.num_snapshots = pick_numeric(evolution_output.parameters, {'num_snapshots'}, NaN);
end

function combined = build_phase2_combined_payload(fd_output, spectral_output, phase_cfg)
    fd_analysis = require_analysis(fd_output.results, fd_output.label);
    sp_analysis = require_analysis(spectral_output.results, spectral_output.label);
    elapsed_time_fd = extract_elapsed_time_series(fd_analysis);
    elapsed_time_sp = extract_elapsed_time_series(sp_analysis);
    kinetic_energy_fd = extract_series(fd_analysis, {'kinetic_energy'}, []);
    kinetic_energy_sp = extract_series(sp_analysis, {'kinetic_energy'}, []);
    enstrophy_fd = extract_series(fd_analysis, {'enstrophy'}, []);
    enstrophy_sp = extract_series(sp_analysis, {'enstrophy'}, []);
    circulation_fd = extract_series(fd_analysis, {'circulation'}, []);
    circulation_sp = extract_series(sp_analysis, {'circulation'}, []);

    trace_key = struct( ...
        'fd_color_family', 'blue', ...
        'spectral_color_family', 'orange', ...
        'legend_policy', 'external_only', ...
        'scenario_label', fd_output.scenario.label, ...
        'scenario_id', fd_output.scenario.id, ...
        'fd_trace_label', sprintf('FD | %s', fd_output.scenario.label), ...
        'spectral_trace_label', sprintf('Spectral | %s', fd_output.scenario.label));

    overlay = struct( ...
        'time_axis_mode', 'elapsed_from_zero', ...
        'legend_policy', 'external_only', ...
        'trace_key', trace_key, ...
        'fd', struct( ...
            'elapsed_wall_time', elapsed_time_fd, ...
            'kinetic_energy', kinetic_energy_fd, ...
            'enstrophy', enstrophy_fd, ...
            'circulation', circulation_fd), ...
        'spectral', struct( ...
            'elapsed_wall_time', elapsed_time_sp, ...
            'kinetic_energy', kinetic_energy_sp, ...
            'enstrophy', enstrophy_sp, ...
            'circulation', circulation_sp, ...
            'supported', true, ...
            'reason', ''));

    combined = struct();
    combined.supported = true;
    combined.phase_policy = struct( ...
        'fd_first_only', logical(pick_value(phase_cfg, 'fd_first_only', false)), ...
        'spectral_supported', true);
    combined.trace_key = trace_key;
    combined.overlay = overlay;
    combined.summary_metrics = struct( ...
        'scenario_id', fd_output.scenario.id, ...
        'scenario_label', fd_output.scenario.label, ...
        'fd_runtime_wall_s', pick_numeric(fd_output.results, {'wall_time', 'total_time'}, fd_output.wall_time), ...
        'spectral_runtime_wall_s', pick_numeric(spectral_output.results, {'wall_time', 'total_time'}, spectral_output.wall_time), ...
        'fd_mesh', sprintf('%sx%s', ...
            num2str(round(pick_numeric(fd_output.parameters, {'Nx'}, NaN))), ...
            num2str(round(pick_numeric(fd_output.parameters, {'Ny'}, NaN)))), ...
        'spectral_mesh', sprintf('%sx%s', ...
            num2str(round(pick_numeric(spectral_output.parameters, {'Nx'}, NaN))), ...
            num2str(round(pick_numeric(spectral_output.parameters, {'Ny'}, NaN)))), ...
        'spectral_supported', true, ...
        'spectral_reason', '');
    combined.fd_view_summary = build_phase_child_view_summary(fd_output);
    combined.spectral_view_summary = build_phase_child_view_summary(spectral_output);
    combined.collector_overlay = struct( ...
        'shared_per_case', true, ...
        'fd_supported', true, ...
        'spectral_supported', true, ...
        'time_axis_mode', 'elapsed_from_zero');
    combined.paths = struct( ...
        'fd_artifact_root', pick_text(fd_output.paths, {'base'}, ''), ...
        'fd_figures_root', pick_text(fd_output.paths, {'figures_root', 'figures_evolution'}, ''), ...
        'fd_reports_root', pick_text(fd_output.paths, {'reports'}, ''), ...
        'spectral_artifact_root', pick_text(spectral_output.paths, {'base'}, ''), ...
        'spectral_figures_root', pick_text(spectral_output.paths, {'figures_root', 'figures_evolution'}, ''), ...
        'spectral_reports_root', pick_text(spectral_output.paths, {'reports'}, ''));
end

function combined = build_phase2_fd_only_combined_payload(fd_output, phase_cfg, spectral_reason)
    fd_analysis = require_analysis(fd_output.results, fd_output.label);
    elapsed_time_fd = extract_elapsed_time_series(fd_analysis);
    kinetic_energy_fd = extract_series(fd_analysis, {'kinetic_energy'}, []);
    enstrophy_fd = extract_series(fd_analysis, {'enstrophy'}, []);
    circulation_fd = extract_series(fd_analysis, {'circulation'}, []);

    trace_key = struct( ...
        'fd_color_family', 'blue', ...
        'spectral_color_family', 'orange', ...
        'legend_policy', 'external_only', ...
        'scenario_label', fd_output.scenario.label, ...
        'scenario_id', fd_output.scenario.id, ...
        'fd_trace_label', sprintf('FD | %s', fd_output.scenario.label), ...
        'spectral_trace_label', sprintf('Spectral | %s', fd_output.scenario.label));

    combined = struct();
    combined.supported = false;
    combined.phase_policy = struct( ...
        'fd_first_only', logical(pick_value(phase_cfg, 'fd_first_only', true)), ...
        'spectral_supported', false);
    combined.trace_key = trace_key;
    combined.overlay = struct( ...
        'time_axis_mode', 'elapsed_from_zero', ...
        'legend_policy', 'external_only', ...
        'trace_key', trace_key, ...
        'fd', struct( ...
            'elapsed_wall_time', elapsed_time_fd, ...
            'kinetic_energy', kinetic_energy_fd, ...
            'enstrophy', enstrophy_fd, ...
            'circulation', circulation_fd), ...
        'spectral', struct( ...
            'elapsed_wall_time', zeros(1, 0), ...
            'kinetic_energy', zeros(1, 0), ...
            'enstrophy', zeros(1, 0), ...
            'circulation', zeros(1, 0), ...
            'supported', false, ...
            'reason', spectral_reason));
    combined.summary_metrics = struct( ...
        'scenario_id', fd_output.scenario.id, ...
        'scenario_label', fd_output.scenario.label, ...
        'fd_runtime_wall_s', pick_numeric(fd_output.results, {'wall_time', 'total_time'}, fd_output.wall_time), ...
        'spectral_runtime_wall_s', NaN, ...
        'fd_mesh', sprintf('%sx%s', ...
            num2str(round(pick_numeric(fd_output.parameters, {'Nx'}, NaN))), ...
            num2str(round(pick_numeric(fd_output.parameters, {'Ny'}, NaN)))), ...
        'spectral_mesh', '--', ...
        'spectral_supported', false, ...
        'spectral_reason', spectral_reason);
    combined.fd_view_summary = build_phase_child_view_summary(fd_output);
    combined.spectral_view_summary = struct();
    combined.collector_overlay = struct( ...
        'shared_per_case', true, ...
        'fd_supported', true, ...
        'spectral_supported', false, ...
        'time_axis_mode', 'elapsed_from_zero');
    combined.paths = struct( ...
        'fd_artifact_root', pick_text(fd_output.paths, {'base'}, ''), ...
        'fd_figures_root', pick_text(fd_output.paths, {'figures_root', 'figures_evolution'}, ''), ...
        'fd_reports_root', pick_text(fd_output.paths, {'reports'}, ''), ...
        'spectral_artifact_root', '', ...
        'spectral_figures_root', '', ...
        'spectral_reports_root', '');
end

function elapsed = extract_elapsed_time_series(analysis)
    elapsed = extract_series(analysis, {'elapsed_wall_time', 'wall_time_history', 'time_vec'}, []);
    elapsed = reshape(double(elapsed), 1, []);
    if isempty(elapsed)
        elapsed = zeros(1, 0);
        return;
    end
    elapsed = elapsed - elapsed(1);
end

function series = extract_series(s, fields, default)
    series = default;
    if ~isstruct(s)
        return;
    end
    for i = 1:numel(fields)
        if isfield(s, fields{i}) && ~isempty(s.(fields{i}))
            series = s.(fields{i});
            return;
        end
    end
end

function analysis = require_analysis(results, label)
    if isstruct(results) && isfield(results, 'analysis') && isstruct(results.analysis) && ...
            ~isempty(fieldnames(results.analysis))
        analysis = results.analysis;
        return;
    end
    data_path = pick_text(results, {'data_path'}, '');
    if ~isempty(data_path) && exist(data_path, 'file') == 2
        loaded = load(data_path, 'analysis');
        if isfield(loaded, 'analysis') && isstruct(loaded.analysis)
            analysis = loaded.analysis;
            return;
        end
    end
    error('Phase2BoundaryConditionStudy:MissingAnalysis', ...
        'Could not resolve analysis payload for %s.', char(string(label)));
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
        'peak_speed_history', 'time_vec', 'snapshot_times', 'elapsed_wall_time', ...
        'wall_model', 'lifting_model', 'boundary_profile'};
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
        'animation_num_frames', 'num_animation_frames', 'phase2_active_scenario'};
    for i = 1:numel(keep)
        if isfield(parameters, keep{i})
            params_summary.(keep{i}) = parameters.(keep{i});
        end
    end
end

function manifest = build_phase_workflow_manifest(phase_id, queue_outputs, phase_cfg, paths, parent_parameters, parent_run_config, scenarios)
    manifest = struct();
    manifest.phase_id = phase_id;
    manifest.workflow_kind = 'phase2_boundary_condition_study';
    manifest.phase_root = paths.base;
    manifest.paths = paths;
    manifest.phase_config = filter_graphics_objects(phase_cfg);
    manifest.parent_parameters = filter_graphics_objects(parent_parameters);
    manifest.parent_run_config = filter_graphics_objects(parent_run_config);
    manifest.scenarios = strip_phase2_scenarios_for_persistence(scenarios);
    manifest.queue = repmat(struct( ...
        'queue_index', NaN, ...
        'job_key', '', ...
        'job_label', '', ...
        'scenario_id', '', ...
        'base_scenario_id', '', ...
        'ic_case_id', '', ...
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
        manifest.queue(i).scenario_id = pick_text(queue_outputs(i).scenario, {'id'}, '');
        manifest.queue(i).base_scenario_id = pick_text(queue_outputs(i).scenario, {'base_scenario_id'}, ...
            manifest.queue(i).scenario_id);
        manifest.queue(i).ic_case_id = pick_text(queue_outputs(i).scenario, {'ic_case_id'}, '');
        manifest.queue(i).method = queue_outputs(i).method;
        manifest.queue(i).mode = queue_outputs(i).stage;
        manifest.queue(i).status = queue_outputs(i).status;
        manifest.queue(i).run_id = pick_text(queue_outputs(i).run_config, {'run_id', 'study_id'}, '');
        manifest.queue(i).artifact_root = pick_text(queue_outputs(i).paths, {'base'}, '');
        manifest.queue(i).data_path = pick_text(queue_outputs(i).results, {'data_path'}, '');
    end
end

function methods = resolve_phase2_methods(phase_cfg)
    methods = pick_value(phase_cfg, 'methods', {'FD'});
    if isstring(methods)
        methods = cellstr(methods(:).');
    end
    if ischar(methods)
        methods = {methods};
    end
    if ~iscell(methods) || isempty(methods)
        methods = {'FD'};
    end

    normalized = cell(1, 0);
    for i = 1:numel(methods)
        key = normalize_method_key(methods{i});
        switch key
            case 'fd'
                normalized{end + 1} = 'FD'; %#ok<AGROW>
            otherwise
                error('Phase2BoundaryConditionStudy:UnsupportedMethod', ...
                    'Phase 2 active runtime supports FD only. Unsupported token "%s".', char(string(methods{i})));
        end
    end
    methods = normalized;
    if numel(methods) ~= 1 || ~strcmpi(methods{1}, 'FD')
        error('Phase2BoundaryConditionStudy:FDOnlyRuntime', ...
            'Phase 2 active runtime must resolve to exactly one FD method.');
    end
end

function output = resolve_phase2_output(queue_outputs, scenario_id, method_key)
    idx = find(arrayfun(@(entry) strcmpi(char(string(entry.scenario.id)), scenario_id) && ...
        strcmpi(char(string(entry.method_key)), method_key), queue_outputs), 1, 'first');
    if isempty(idx)
        error('Phase2BoundaryConditionStudy:MissingScenarioMethodOutput', ...
            'Missing Phase 2 output for scenario "%s" and method "%s".', ...
            char(string(scenario_id)), char(string(method_key)));
    end
    output = queue_outputs(idx);
end

function output = try_resolve_phase2_output(queue_outputs, scenario_id, method_key)
    output = empty_output();
    idx = find(arrayfun(@(entry) strcmpi(char(string(entry.scenario.id)), scenario_id) && ...
        strcmpi(char(string(entry.method_key)), method_key), queue_outputs), 1, 'first');
    if ~isempty(idx)
        output = queue_outputs(idx);
    end
end

function persisted = strip_phase2_for_persistence(results_in)
    persisted = results_in;
    persisted.scenarios = strip_phase2_scenarios_for_persistence(persisted.scenarios);
    if isfield(persisted, 'workflow_manifest') && isstruct(persisted.workflow_manifest)
        persisted.workflow_manifest.scenarios = strip_phase2_scenarios_for_persistence( ...
            persisted.workflow_manifest.scenarios);
    end
end

function scenarios = strip_phase2_scenarios_for_persistence(scenarios)
    if ~isstruct(scenarios) || isempty(scenarios)
        return;
    end
    for i = 1:numel(scenarios)
        if isfield(scenarios(i), 'fd') && isstruct(scenarios(i).fd) && isfield(scenarios(i).fd, 'view_summary')
            scenarios(i).fd.view_summary = strip_phase_view_summary_for_persistence(scenarios(i).fd.view_summary);
        end
        if isfield(scenarios(i), 'spectral') && isstruct(scenarios(i).spectral) && isfield(scenarios(i).spectral, 'view_summary')
            scenarios(i).spectral.view_summary = strip_phase_view_summary_for_persistence(scenarios(i).spectral.view_summary);
        end
        if isfield(scenarios(i), 'combined') && isstruct(scenarios(i).combined)
            if isfield(scenarios(i).combined, 'fd_view_summary')
                scenarios(i).combined.fd_view_summary = strip_phase_view_summary_for_persistence(scenarios(i).combined.fd_view_summary);
            end
            if isfield(scenarios(i).combined, 'spectral_view_summary')
                scenarios(i).combined.spectral_view_summary = strip_phase_view_summary_for_persistence(scenarios(i).combined.spectral_view_summary);
            end
        end
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

function artifact_summary = write_phase2_workflow_collector_artifacts(Results, Run_Config, paths, phase_monitor_series)
    if nargin < 4 || ~isstruct(phase_monitor_series)
        phase_monitor_series = struct();
    end
    summary_context = struct( ...
        'run_id', pick_text(Results, {'run_id'}, ''), ...
        'phase_id', pick_text(Results, {'phase_id'}, ''), ...
        'workflow_kind', pick_text(Results, {'workflow_kind'}, ''), ...
        'run_config', filter_graphics_objects(Run_Config), ...
        'monitor_series', filter_graphics_objects(phase_monitor_series), ...
        'results', strip_phase2_for_persistence(Results), ...
        'paths', paths);
    artifact_summary = ExternalCollectorDispatcher.write_run_artifacts(summary_context);
end

function append_phase2_workbook_job_sheets(workbook_path, Results)
    if isempty(workbook_path) || exist(workbook_path, 'file') ~= 2 || ~isstruct(Results)
        return;
    end
    try
        job_table = build_phase2_job_metrics_table(Results);
        if isempty(job_table)
            writecell({'No Phase 2 job metric rows available.'}, workbook_path, 'Sheet', 'phase2_job_metrics');
        else
            writetable(job_table, workbook_path, 'Sheet', 'phase2_job_metrics');
        end
        provenance_table = build_phase2_plotting_provenance_table(Results);
        if isempty(provenance_table)
            writecell({'No Phase 2 plotting provenance rows available.'}, workbook_path, 'Sheet', 'phase2_plotting_provenance');
        else
            writetable(provenance_table, workbook_path, 'Sheet', 'phase2_plotting_provenance');
        end
    catch ME
        warning('Phase2BoundaryConditionStudy:WorkbookAppendFailed', ...
            'Phase 2 workbook enrichment failed: %s', ME.message);
    end
end

function table_out = build_phase2_job_metrics_table(Results)
    table_out = table();
    if ~isstruct(Results) || ~isfield(Results, 'scenarios') || ~isstruct(Results.scenarios)
        return;
    end
    rows = repmat(phase2_empty_job_metric_row(), 1, 0);
    job_index = 0;
    for i = 1:numel(Results.scenarios)
        scen = Results.scenarios(i);
        fd_view = pick_struct(pick_struct(scen, {'fd'}, struct()), {'view_summary'}, struct());
        fd_row = phase2_build_job_metric_row(scen, fd_view, 'FD');
        if phase2_job_metric_row_has_payload(fd_row)
            job_index = job_index + 1;
            fd_row.job_index = double(job_index);
            rows(end + 1) = fd_row; %#ok<AGROW>
        end

        spectral_view = pick_struct(pick_struct(scen, {'spectral'}, struct()), {'view_summary'}, struct());
        spectral_row = phase2_build_job_metric_row(scen, spectral_view, 'SM');
        spectral_supported = logical(pick_value(pick_struct(scen, {'summary'}, struct()), 'spectral_supported', false));
        if spectral_supported && phase2_job_metric_row_has_payload(spectral_row)
            job_index = job_index + 1;
            spectral_row.job_index = double(job_index);
            rows(end + 1) = spectral_row; %#ok<AGROW>
        end
    end
    if isempty(rows)
        return;
    end
    table_out = struct2table(rows);
end

function row = phase2_empty_job_metric_row()
    row = struct( ...
        'job_index', NaN, ...
        'scenario_id', '', ...
        'scenario_label', '', ...
        'base_scenario_id', '', ...
        'base_scenario_label', '', ...
        'ic_case_id', '', ...
        'ic_case_label', '', ...
        'ic_type', '', ...
        'method', 'FD', ...
        'grid_nx', NaN, ...
        'grid_ny', NaN, ...
        'dt', NaN, ...
        'Tfinal', NaN, ...
        'runtime_wall_s', NaN, ...
        'kinetic_energy_final', NaN, ...
        'enstrophy_final', NaN, ...
        'peak_vorticity_final', NaN, ...
        'circulation_final', NaN, ...
        'centroid_drift', NaN, ...
        'core_anisotropy_final', NaN, ...
        'cfl_advective_peak', NaN, ...
        'cfl_diffusive_peak', NaN, ...
        'artifact_root', '', ...
        'data_path', '', ...
        'visuals_root', '');
end

function row = phase2_build_job_metric_row(scen, view_summary, method_label)
    row = phase2_empty_job_metric_row();
    method_key = normalize_method_key(method_label);
    summary_payload = pick_struct(scen, {'summary'}, struct());
    params = pick_struct(view_summary, {'parameters'}, struct());
    analysis = pick_struct(view_summary, {'analysis'}, struct());
    results_payload = pick_struct(view_summary, {'results'}, struct());
    paths_payload = pick_struct(view_summary, {'paths'}, struct());

    row.scenario_id = pick_text(scen, {'scenario_id'}, '');
    row.scenario_label = pick_text(scen, {'scenario_label'}, '');
    row.base_scenario_id = pick_text(scen, {'base_scenario_id'}, '');
    row.base_scenario_label = pick_text(scen, {'base_scenario_label'}, '');
    row.ic_case_id = pick_text(scen, {'ic_case_id'}, '');
    row.ic_case_label = pick_text(scen, {'ic_case_label'}, '');
    row.ic_type = pick_text(scen, {'ic_type'}, pick_text(params, {'ic_type'}, ''));
    row.method = upper(char(string(method_label)));

    switch method_key
        case 'spectral'
            row.grid_nx = pick_numeric(params, {'Nx'}, pick_numeric(summary_payload, {'spectral_mesh_nx'}, pick_numeric(analysis, {'Nx'}, NaN)));
            row.grid_ny = pick_numeric(params, {'Ny'}, pick_numeric(summary_payload, {'spectral_mesh_ny'}, pick_numeric(analysis, {'Ny'}, NaN)));
            row.dt = pick_numeric(params, {'dt'}, pick_numeric(summary_payload, {'spectral_dt'}, pick_numeric(analysis, {'dt'}, NaN)));
            row.Tfinal = pick_numeric(params, {'Tfinal', 't_final'}, pick_numeric(summary_payload, {'spectral_Tfinal'}, pick_numeric(analysis, {'Tfinal'}, NaN)));
            row.runtime_wall_s = pick_numeric(summary_payload, {'spectral_runtime_wall_s'}, pick_numeric(results_payload, {'wall_time', 'total_time'}, NaN));
        otherwise
            row.grid_nx = pick_numeric(params, {'Nx'}, pick_numeric(analysis, {'Nx'}, NaN));
            row.grid_ny = pick_numeric(params, {'Ny'}, pick_numeric(analysis, {'Ny'}, NaN));
            row.dt = pick_numeric(params, {'dt'}, pick_numeric(analysis, {'dt'}, NaN));
            row.Tfinal = pick_numeric(params, {'Tfinal', 't_final'}, pick_numeric(analysis, {'Tfinal'}, NaN));
            row.runtime_wall_s = pick_numeric(summary_payload, {'runtime_wall_s'}, pick_numeric(results_payload, {'wall_time', 'total_time'}, NaN));
    end

    row.kinetic_energy_final = last_finite_phase2(extract_phase2_metric_series(analysis, results_payload, {'kinetic_energy', 'final_kinetic_energy', 'final_energy'}));
    row.enstrophy_final = last_finite_phase2(extract_phase2_metric_series(analysis, results_payload, {'enstrophy', 'final_enstrophy'}));
    row.peak_vorticity_final = last_finite_phase2(extract_phase2_metric_series(analysis, results_payload, {'peak_omega_history', 'max_omega_history', 'peak_vorticity', 'max_omega'}));
    row.circulation_final = last_finite_phase2(extract_phase2_metric_series(analysis, results_payload, {'circulation', 'final_circulation'}));
    row.centroid_drift = phase2_centroid_drift(analysis);
    row.core_anisotropy_final = last_finite_phase2(extract_phase2_metric_series(analysis, results_payload, {'core_anisotropy', 'anisotropy_history'}));
    row.cfl_advective_peak = finite_peak_phase2(extract_phase2_metric_series(analysis, results_payload, {'cfl_adv', 'CFL_adv', 'cfl_advective', 'cfl_adv_history'}));
    row.cfl_diffusive_peak = finite_peak_phase2(extract_phase2_metric_series(analysis, results_payload, {'cfl_diff', 'CFL_diff', 'cfl_diffusive', 'cfl_diff_history'}));
    row.artifact_root = pick_text(paths_payload, {'base'}, pick_text(summary_payload, {'artifact_root'}, ''));
    row.data_path = pick_text(results_payload, {'data_path'}, '');
    row.visuals_root = pick_text(paths_payload, {'figures_root', 'figures_evolution', 'visuals_root'}, pick_text(summary_payload, {'figures_root'}, ''));
end

function tf = phase2_job_metric_row_has_payload(row)
    tf = false;
    if ~isstruct(row)
        return;
    end
    tf = strlength(string(row.scenario_id)) > 0 && ...
        (isfinite(row.runtime_wall_s) || isfinite(row.grid_nx) || isfinite(row.grid_ny) || ...
        isfinite(row.kinetic_energy_final) || strlength(string(row.artifact_root)) > 0 || ...
        strlength(string(row.visuals_root)) > 0);
end

function table_out = build_phase2_plotting_provenance_table(Results)
    table_out = table();
    if ~isstruct(Results) || ~isfield(Results, 'scenarios') || ~isstruct(Results.scenarios)
        return;
    end
    rows = repmat(struct('scenario_id', '', 'plot_family', '', 'artifact_path', '', ...
        'source_sheet', 'phase2_job_metrics', 'source_columns', ''), 1, 0);
    for i = 1:numel(Results.scenarios)
        scen = Results.scenarios(i);
        fd_view = pick_struct(pick_struct(scen, {'fd'}, struct()), {'view_summary'}, struct());
        paths_payload = pick_struct(fd_view, {'paths'}, struct());
        scenario_id = pick_text(scen, {'scenario_id'}, '');
        visuals_root = pick_text(paths_payload, {'figures_root', 'figures_evolution', 'visuals_root'}, '');
        plot_families = {'vorticity', 'streamfunction', 'velocity', 'contour', 'streamlines'};
        for pi = 1:numel(plot_families)
            rows(end + 1) = struct( ... %#ok<AGROW>
                'scenario_id', scenario_id, ...
                'plot_family', plot_families{pi}, ...
                'artifact_path', visuals_root, ...
                'source_sheet', 'phase2_job_metrics', ...
                'source_columns', 'scenario_id,ic_case_id,base_scenario_id,Tfinal,runtime_wall_s,kinetic_energy_final,enstrophy_final,peak_vorticity_final,circulation_final,cfl_advective_peak,cfl_diffusive_peak');
        end
    end
    if ~isempty(rows)
        table_out = struct2table(rows);
    end
end

function values = extract_phase2_metric_series(analysis, results_payload, fields)
    values = [];
    for i = 1:numel(fields)
        field_name = fields{i};
        if isstruct(analysis) && isfield(analysis, field_name) && isnumeric(analysis.(field_name)) && ~isempty(analysis.(field_name))
            values = double(analysis.(field_name)(:));
            return;
        end
        if isstruct(results_payload) && isfield(results_payload, field_name) && isnumeric(results_payload.(field_name)) && ~isempty(results_payload.(field_name))
            values = double(results_payload.(field_name)(:));
            return;
        end
    end
end

function value = last_finite_phase2(values)
    value = NaN;
    if isempty(values) || ~isnumeric(values)
        return;
    end
    values = double(values(:));
    values = values(isfinite(values));
    if ~isempty(values)
        value = values(end);
    end
end

function value = finite_peak_phase2(values)
    value = NaN;
    if isempty(values) || ~isnumeric(values)
        return;
    end
    values = abs(double(values(:)));
    values = values(isfinite(values));
    if ~isempty(values)
        value = max(values);
    end
end

function drift = phase2_centroid_drift(analysis)
    drift = NaN;
    if ~isstruct(analysis)
        return;
    end
    x_series = extract_phase2_metric_series(analysis, struct(), {'centroid_x', 'vortex_centroid_x'});
    y_series = extract_phase2_metric_series(analysis, struct(), {'centroid_y', 'vortex_centroid_y'});
    if numel(x_series) >= 2 && numel(y_series) >= 2
        n = min(numel(x_series), numel(y_series));
        x_series = double(x_series(1:n));
        y_series = double(y_series(1:n));
        drift = hypot(x_series(end) - x_series(1), y_series(end) - y_series(1));
    end
end

function tf = phase2_child_telemetry_requested(settings)
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

function tf = phase2_child_external_collectors_enabled(settings)
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

function settings = disable_phase2_child_external_collectors(settings)
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

function result_payload = attach_phase2_child_collector_probe(result_payload, settings)
    if ~isstruct(result_payload)
        result_payload = struct();
    end
    sample = build_phase2_child_collector_probe_sample(settings);
    if ~(isstruct(sample) && ~isempty(fieldnames(sample)))
        return;
    end
    result_payload.collector_last_sample = sample;
    if ~isfield(result_payload, 'collector_session') || ...
            ~(isstruct(result_payload.collector_session) && ~isempty(fieldnames(result_payload.collector_session)))
        result_payload.collector_session = sample;
    end
end

function sample = build_phase2_child_collector_probe_sample(settings)
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
        warning('Phase2BoundaryConditionStudy:ChildCollectorProbeFailed', ...
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

function monitor_series = build_phase2_workflow_monitor_series(queue_outputs, phase_id)
    workflow_kind = 'phase2_boundary_condition_study';
    monitor_series = struct();
    segment_manifest = repmat(struct( ...
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
        'wall_time_s', NaN, ...
        'raw_hwinfo_csv_path', ''), 1, 0);
    offset_t = 0;
    gap_t = 1.0e-6;
    for i = 1:numel(queue_outputs)
        output = queue_outputs(i);
        scenario_label = output.scenario.label;
        scenario_id = output.scenario.id;
        stage_method = char(string(output.method));
        segment = choose_phase2_segment_series(output.results, output.wall_time);
        if isempty(fieldnames(segment)) || ~isfield(segment, 't') || isempty(segment.t)
            continue;
        end
        local_t = reshape(double(segment.t), 1, []);
        local_t = local_t - local_t(1);
        if i > 1
            local_t = local_t + offset_t + gap_t;
        end
        n = numel(local_t);
        segment.t = local_t;
        segment.elapsed_wall_time = local_t;
        segment.workflow_kind = workflow_kind;
        segment.workflow_phase_id = phase_id;
        segment.workflow_kind_series = repmat(string(workflow_kind), 1, n);
        segment.workflow_phase_id_series = repmat(string(phase_id), 1, n);
        segment.workflow_stage_id_series = repmat(string(sprintf('scenario_%s', scenario_id)), 1, n);
        segment.workflow_stage_label_series = repmat(string(scenario_label), 1, n);
        segment.workflow_stage_type_series = repmat("scenario", 1, n);
        segment.workflow_method_series = repmat(string(stage_method), 1, n);
        segment.workflow_substage_id_series = repmat(string(scenario_id), 1, n);
        segment.workflow_substage_label_series = repmat(string(sprintf('%s | %s', scenario_label, stage_method)), 1, n);
        segment.workflow_substage_type_series = repmat("scenario", 1, n);
        segment.workflow_scenario_id_series = repmat(string(scenario_id), 1, n);
        segment.workflow_stage_wall_time_series = repmat(double(output.wall_time), 1, n);
        segment.workflow_mesh_level_series = repmat(NaN, 1, n);
        segment.workflow_mesh_index_series = repmat(NaN, 1, n);
        segment.workflow_child_run_index_series = repmat(double(i), 1, n);
        segment.workflow_child_mesh_nx_series = repmat(double(pick_numeric(output.parameters, {'Nx'}, NaN)), 1, n);
        segment.workflow_child_mesh_ny_series = repmat(double(pick_numeric(output.parameters, {'Ny'}, NaN)), 1, n);
        monitor_series = append_phase2_monitor_series(monitor_series, segment);
        segment_manifest(end + 1) = struct( ... %#ok<AGROW>
            'stage_id', sprintf('scenario_%s', scenario_id), ...
            'stage_label', scenario_label, ...
            'stage_type', 'scenario', ...
            'stage_method', stage_method, ...
            'phase_id', phase_id, ...
            'workflow_kind', workflow_kind, ...
            'substage_id', scenario_id, ...
            'substage_label', sprintf('%s | %s', scenario_label, stage_method), ...
            'substage_type', 'scenario', ...
            'scenario_id', scenario_id, ...
            'mesh_level', NaN, ...
            'mesh_index', NaN, ...
            'child_run_index', double(i), ...
            'mesh_nx', double(pick_numeric(output.parameters, {'Nx'}, NaN)), ...
            'mesh_ny', double(pick_numeric(output.parameters, {'Ny'}, NaN)), ...
            'wall_time_s', double(output.wall_time), ...
            'raw_hwinfo_csv_path', phase2_segment_hwinfo_csv(output.results));
        offset_t = local_t(end);
    end
    if isempty(fieldnames(monitor_series))
        return;
    end
    monitor_series.workflow_kind = workflow_kind;
    monitor_series.workflow_phase_id = phase_id;
    monitor_series.workflow_segment_manifest = segment_manifest;
    monitor_series = ExternalCollectorDispatcher.normalize_collector_payload(monitor_series);
end

function segment = choose_phase2_segment_series(result_struct, wall_time_s)
    segment = struct();
    if ~isstruct(result_struct)
        return;
    end
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
    csv_path = '';
    if isstruct(sample) && isfield(sample, 'raw_log_paths') && isstruct(sample.raw_log_paths)
        csv_path = pick_text(sample.raw_log_paths, {'hwinfo'}, '');
    end
    if ~isempty(csv_path) && exist(csv_path, 'file') == 2
        segment = phase2_segment_series_from_csv(csv_path, result_struct, sample);
    elseif ~isempty(fieldnames(sample))
        segment = phase2_segment_series_from_sample(sample, wall_time_s);
    end
end

function segment = phase2_segment_series_from_csv(csv_path, result_struct, sample)
    segment = struct();
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
    segment = phase2_empty_monitor_series();
    segment.collector_status.hwinfo = phase2_table_last_text(data_table, 'hwinfo_status', 'shared_memory_connected');
    segment.hwinfo_transport = phase2_table_last_text(data_table, 'hwinfo_transport', 'shared_memory');
    segment.raw_log_paths.hwinfo = csv_path;
    segment.t = reshape(double(data_table.session_time_s), 1, []);
    segment.elapsed_wall_time = segment.t;
    if ismember('timestamp_utc', data_table.Properties.VariableNames)
        segment.wall_clock_time = reshape(phase2_utc_series_to_posix(data_table.timestamp_utc), 1, []);
    end
    segment.collector_series.hwinfo = struct();
    for i = 1:numel(metric_keys)
        key = metric_keys{i};
        if ~ismember(key, data_table.Properties.VariableNames)
            continue;
        end
        values = reshape(double(data_table.(key)), 1, []);
        segment.collector_series.hwinfo.(key) = values;
        segment.(key) = values;
    end
    if isfield(result_struct, 'collector_metric_catalog') && ~isempty(result_struct.collector_metric_catalog)
        segment.collector_metric_catalog = result_struct.collector_metric_catalog;
    elseif isstruct(sample) && isfield(sample, 'collector_metric_catalog') && ~isempty(sample.collector_metric_catalog)
        segment.collector_metric_catalog = sample.collector_metric_catalog;
    else
        segment.collector_metric_catalog = struct([]);
    end
end

function segment = phase2_segment_series_from_sample(sample, wall_time_s)
    segment = ExternalCollectorDispatcher.normalize_collector_payload(sample);
    if isempty(fieldnames(segment))
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
    segment.t = [0, wall_time_s];
    segment.elapsed_wall_time = segment.t;
    if ~isfield(segment, 'collector_series') || ~isstruct(segment.collector_series)
        segment.collector_series = phase2_empty_monitor_series().collector_series;
    end
    for i = 1:numel(metric_keys)
        key = metric_keys{i};
        value = NaN;
        if isfield(segment, key) && isnumeric(segment.(key)) && ~isempty(segment.(key))
            raw = reshape(double(segment.(key)), 1, []);
            value = raw(end);
        elseif isfield(sample, 'metrics') && isstruct(sample.metrics) && isfield(sample.metrics, key) && ...
                isnumeric(sample.metrics.(key)) && isscalar(sample.metrics.(key)) && isfinite(sample.metrics.(key))
            value = double(sample.metrics.(key));
        end
        if isfinite(value)
            segment.(key) = [value, value];
            if ~isfield(segment.collector_series, 'hwinfo') || ~isstruct(segment.collector_series.hwinfo)
                segment.collector_series.hwinfo = struct();
            end
            if ~isfield(segment.collector_series.hwinfo, key) || isempty(segment.collector_series.hwinfo.(key))
                segment.collector_series.hwinfo.(key) = [value, value];
            end
        end
    end
    if ~isfield(segment, 'collector_metric_catalog') || isempty(segment.collector_metric_catalog)
        if isstruct(sample) && isfield(sample, 'collector_metric_catalog') && ~isempty(sample.collector_metric_catalog)
            segment.collector_metric_catalog = sample.collector_metric_catalog;
        else
            segment.collector_metric_catalog = struct([]);
        end
    end
end

function combined = append_phase2_monitor_series(combined, segment)
    if isempty(fieldnames(combined))
        combined = segment;
        return;
    end
    numeric_fields = {'t', 'elapsed_wall_time', 'wall_clock_time', 'cpu_proxy', 'gpu_series', ...
        'memory_series', 'cpu_temp_c', 'power_w', 'system_power_w', 'cpu_voltage_v', ...
        'gpu_voltage_v', 'memory_voltage_v', 'cpu_power_w_hwinfo', 'gpu_power_w_hwinfo', ...
        'memory_power_w_or_proxy', 'environmental_energy_wh_cum', 'environmental_co2_g_cum', ...
        'fan_rpm', 'pump_rpm', 'coolant_temp_c', 'device_battery_level', ...
        'workflow_stage_wall_time_series', 'workflow_mesh_level_series', 'workflow_mesh_index_series', 'workflow_child_run_index_series', ...
        'workflow_child_mesh_nx_series', 'workflow_child_mesh_ny_series'};
    text_fields = {'workflow_kind_series', 'workflow_phase_id_series', 'workflow_stage_id_series', ...
        'workflow_stage_label_series', 'workflow_stage_type_series', 'workflow_method_series', ...
        'workflow_substage_id_series', 'workflow_substage_label_series', 'workflow_substage_type_series', ...
        'workflow_scenario_id_series'};
    for i = 1:numel(numeric_fields)
        combined.(numeric_fields{i}) = phase2_concat_numeric_field(combined, segment, numeric_fields{i});
    end
    for i = 1:numel(text_fields)
        combined.(text_fields{i}) = phase2_concat_text_field(combined, segment, text_fields{i});
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
                combined.collector_series.(source).(field_name) = phase2_concat_numeric_struct_field( ...
                    combined.collector_series.(source), pick_struct(segment.collector_series, {source}, struct()), field_name);
            end
        end
    end
    combined = merge_structs(combined, phase2_rmfield_if_present(segment, [numeric_fields, text_fields, {'collector_series'}]));
end

function csv_path = phase2_segment_hwinfo_csv(result_struct)
    csv_path = '';
    if ~isstruct(result_struct)
        return;
    end
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
    if isstruct(sample) && isfield(sample, 'raw_log_paths') && isstruct(sample.raw_log_paths)
        csv_path = pick_text(sample.raw_log_paths, {'hwinfo'}, '');
    end
end

function value = phase2_concat_numeric_field(base_struct, overlay_struct, field_name)
    value = zeros(1, 0);
    if isfield(base_struct, field_name) && isnumeric(base_struct.(field_name))
        value = reshape(double(base_struct.(field_name)), 1, []);
    end
    if isfield(overlay_struct, field_name) && isnumeric(overlay_struct.(field_name))
        value = [value, reshape(double(overlay_struct.(field_name)), 1, [])]; %#ok<AGROW>
    end
end

function value = phase2_concat_numeric_struct_field(base_struct, overlay_struct, field_name)
    value = zeros(1, 0);
    if isfield(base_struct, field_name) && isnumeric(base_struct.(field_name))
        value = reshape(double(base_struct.(field_name)), 1, []);
    end
    if isfield(overlay_struct, field_name) && isnumeric(overlay_struct.(field_name))
        value = [value, reshape(double(overlay_struct.(field_name)), 1, [])]; %#ok<AGROW>
    end
end

function value = phase2_concat_text_field(base_struct, overlay_struct, field_name)
    value = strings(1, 0);
    if isfield(base_struct, field_name) && ~isempty(base_struct.(field_name))
        value = reshape(string(base_struct.(field_name)), 1, []);
    end
    if isfield(overlay_struct, field_name) && ~isempty(overlay_struct.(field_name))
        value = [value, reshape(string(overlay_struct.(field_name)), 1, [])]; %#ok<AGROW>
    end
end

function posix_values = phase2_utc_series_to_posix(values_in)
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

function text = phase2_table_last_text(tbl, column_name, fallback)
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

function out = phase2_rmfield_if_present(in, field_names)
    out = in;
    if ~isstruct(out)
        return;
    end
    present = intersect(fieldnames(out), field_names, 'stable');
    if ~isempty(present)
        out = rmfield(out, present);
    end
end

function sample = phase2_empty_monitor_series()
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

function artifact_summary = export_phase2_combined_triptych_gif(Results, phase_parameters, phase_settings, paths)
    artifact_summary = struct( ...
        'scenario_triptych_gif_path', '', ...
        'frame_count', 0, ...
        'status', 'not_requested', ...
        'failure_message', '');
    if ~logical(pick_value(Results.phase_config, 'create_animations', false))
        return;
    end

    scenario_count = numel(Results.scenarios);
    if scenario_count ~= 3
        artifact_summary.status = 'skipped';
        artifact_summary.failure_message = sprintf('Expected 3 Phase 2 scenarios, found %d.', scenario_count);
        return;
    end

    try
        media = resolve_phase2_triptych_media(phase_parameters, phase_settings);

        plot_payloads = repmat(struct( ...
            'label', '', ...
            'x', [], ...
            'y', [], ...
            'omega', [], ...
            'display_times', []), 1, scenario_count);
        cmax = 0;
        for i = 1:scenario_count
            fd_view = pick_struct(Results.scenarios(i).fd, {'view_summary'}, struct());
            analysis = pick_struct(fd_view, {'analysis'}, struct());
            params = pick_struct(fd_view, {'parameters'}, struct());
            if ~isstruct(analysis) || ~isfield(analysis, 'omega_snaps') || size(analysis.omega_snaps, 3) < 2
                error('Phase2BoundaryConditionStudy:MissingAnimationSnapshots', ...
                    'Scenario "%s" is missing snapshot data for the combined Phase 2 GIF.', ...
                    char(string(Results.scenarios(i).scenario_label)));
            end
            [x_vec, y_vec] = phase2_resolve_plot_axes(analysis, params, size(analysis.omega_snaps));
            plot_times = phase2_resolve_plot_times(analysis, params, size(analysis.omega_snaps, 3));
            [omega_cube, display_times] = phase2_resample_snapshot_cube( ...
                double(analysis.omega_snaps), plot_times, media.frame_count);
            local_cmax = max(abs(omega_cube(:)), [], 'omitnan');
            if isfinite(local_cmax)
                cmax = max(cmax, local_cmax);
            end
            plot_payloads(i).label = char(string(Results.scenarios(i).scenario_label));
            plot_payloads(i).x = reshape(double(x_vec), 1, []);
            plot_payloads(i).y = reshape(double(y_vec), 1, []);
            plot_payloads(i).omega = omega_cube;
            plot_payloads(i).display_times = reshape(double(display_times), 1, []);
        end
        if ~(isfinite(cmax) && cmax > 0)
            cmax = 1;
        end

        output_dir = fullfile(char(string(paths.visuals_root)), 'Combined');
        if exist(output_dir, 'dir') ~= 7
            mkdir(output_dir);
        end
        gif_path = fullfile(output_dir, sprintf('%s__scenario_evolution_3x1.gif', char(string(Results.phase_id))));
        fig = figure('Visible', 'off', 'HandleVisibility', 'off', 'Color', [1 1 1], ...
            'MenuBar', 'none', 'ToolBar', 'none', 'Units', 'inches', ...
            'Position', [0.5 0.5 12.0 4.2], 'PaperPositionMode', 'auto');
        cleanup_fig = onCleanup(@() phase2_safe_close_figure(fig)); %#ok<NASGU>
        tl = tiledlayout(fig, 1, scenario_count, 'Padding', 'compact', 'TileSpacing', 'compact');
        axes_list = gobjects(1, scenario_count);
        for i = 1:scenario_count
            axes_list(i) = nexttile(tl, i);
        end

        frame_rgb_sequence = cell(1, media.frame_count);
        for frame_index = 1:media.frame_count
            for scenario_index = 1:scenario_count
                ax = axes_list(scenario_index);
                cla(ax);
                payload = plot_payloads(scenario_index);
                omega_slice = phase2_prepare_plot_matrix(payload.omega(:, :, frame_index), payload.x, payload.y);
                imagesc(ax, payload.x, payload.y, omega_slice);
                axis(ax, 'xy');
                axis(ax, 'tight');
                pbaspect(ax, [1 1 1]);
                colormap(ax, turbo(256));
                caxis(ax, [-cmax, cmax]);
                xlabel(ax, 'x');
                ylabel(ax, 'y');
                title(ax, sprintf('t = %.3g s', phase2_time_value(payload.display_times, frame_index)), ...
                    'Interpreter', 'none');
                grid(ax, 'off');
                box(ax, 'on');
            end
            title(tl, 'Phase 2 Scenario Evolution (3x1)', 'Interpreter', 'none');
            frame_rgb_sequence{frame_index} = phase2_capture_triptych_frame(fig, output_dir, frame_index, media);
        end
        gif_status = ResultsAnimationExporter.write_frame_sequence_gif(gif_path, frame_rgb_sequence, media);
        artifact_summary.scenario_triptych_gif_path = pick_text(gif_status, {'validated_output_path', 'output_path'}, gif_path);
        artifact_summary.frame_count = media.frame_count;
        artifact_summary.status = pick_text(gif_status, {'status'}, 'created');
        artifact_summary.gif_status = gif_status;
        if ~strcmpi(artifact_summary.status, 'saved')
            artifact_summary.failure_message = pick_text(gif_status, {'failure_message'}, '');
        else
            artifact_summary.status = 'created';
        end
    catch ME
        artifact_summary.status = 'failed';
        artifact_summary.failure_message = ME.message;
    end
end

function artifact_summary = export_phase2_workflow_animations(Results, phase_parameters, phase_settings, paths)
    artifact_summary = struct( ...
        'scenario_triptych_gif_path', '', ...
        'frame_count', 0, ...
        'status', 'not_requested', ...
        'failure_message', '', ...
        'scenario_animation_artifacts', struct([]));
    if ~logical(pick_value(Results.phase_config, 'create_animations', false))
        return;
    end
    panes = pick_value(Results.phase_config, 'workflow_animation_panes', {'evolution', 'streamfunction', 'speed', 'vector', 'contour'});
    panes = phase2_normalize_animation_panes(panes);
    if isempty(panes)
        panes = {'evolution', 'streamfunction', 'speed', 'vector', 'contour'};
    end
    artifacts = repmat(struct( ...
        'scenario_id', '', ...
        'status', 'not_requested', ...
        'failure_message', '', ...
        'pane_mp4s', struct(), ...
        'pane_gifs', struct()), 1, numel(Results.scenarios));
    any_created = false;
    failures = {};
    for i = 1:numel(Results.scenarios)
        artifacts(i).scenario_id = pick_text(Results.scenarios(i), {'scenario_id'}, sprintf('scenario_%02d', i));
        try
            fd_view = pick_struct(Results.scenarios(i).fd, {'view_summary'}, struct());
            analysis = pick_struct(fd_view, {'analysis'}, struct());
            params = pick_struct(fd_view, {'parameters'}, phase_parameters);
            run_cfg = pick_struct(fd_view, {'run_config'}, struct());
            child_paths = pick_struct(fd_view, {'paths'}, struct());
            if isempty(fieldnames(analysis)) || ~isfield(analysis, 'omega_snaps') || size(analysis.omega_snaps, 3) < 2
                artifacts(i).status = 'skipped_no_snapshots';
                continue;
            end
            if isempty(fieldnames(child_paths))
                child_paths = paths;
            end
            child_paths.disable_combined_animation_dir = true;
            child_paths.media_flatten_pane_dirs = true;
            child_visual_root = phase2_workflow_visual_child_root(paths, artifacts(i).scenario_id, 'FD');
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
            child_paths.pane_media_stem_map = phase2_animation_stem_map(artifacts(i).scenario_id);
            params.create_animations = true;
            params.animation_format = 'gif';
            params.animation_export_format = 'gif';
            params.animation_export_formats = {'gif'};
            params.contour_levels = pick_numeric(Results.phase_config, {'contour_levels'}, 36);
            params.plot_trim_layers = pick_numeric(Results.phase_config, {'boundary_visual_crop_cells'}, 1);
            params.animation_num_frames = pick_numeric(params, {'animation_num_frames', 'num_animation_frames'}, ...
                resolve_phase2_animation_frame_count(phase_parameters, phase_settings));
            params.num_animation_frames = params.animation_num_frames;
            params.animation_gif_min_frames = params.animation_num_frames;
            params.animation_export_resolution_px = [1600, 1200];
            params.animation_export_dpi = 200;
            params.animation_export_width_in = 8.0;
            params.animation_export_height_in = 6.0;
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
            local_settings.media.contour_levels = pick_numeric(Results.phase_config, {'contour_levels'}, 36);
            local_settings.media.frame_count = params.animation_num_frames;
            local_settings.media.gif_min_frame_count = params.animation_num_frames;
            local_settings.media.resolution_px = [1600, 1200];
            local_settings.media.dpi = 200;
            local_settings.media.width_in = 8.0;
            local_settings.media.height_in = 6.0;
            local_settings.animation_format = 'gif';
            local_settings.animation_export_format = 'gif';
            local_settings.animation_export_formats = {'gif'};
            local_settings.animation_frame_count = params.animation_num_frames;
            local_settings.animation_num_frames = params.animation_num_frames;
            local_settings.animation_gif_min_frames = params.animation_num_frames;
            local_settings.animation_export_resolution_px = [1600, 1200];
            local_settings.animation_export_dpi = 200;
            local_settings.animation_export_width_in = 8.0;
            local_settings.animation_export_height_in = 6.0;
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
            failures{end + 1} = sprintf('%s: %s', artifacts(i).scenario_id, ME.message); %#ok<AGROW>
        end
    end
    artifact_summary.scenario_animation_artifacts = artifacts;
    artifact_summary.frame_count = resolve_phase2_animation_frame_count(phase_parameters, phase_settings);
    if any_created && isempty(failures)
        artifact_summary.status = 'created';
    elseif any_created
        artifact_summary.status = 'created_with_warnings';
        artifact_summary.failure_message = strjoin(failures, ' | ');
    elseif ~isempty(failures)
        artifact_summary.status = 'failed';
        artifact_summary.failure_message = strjoin(failures, ' | ');
    else
        artifact_summary.status = 'skipped';
        artifact_summary.failure_message = 'No scenario animation outputs were created.';
    end
end

function panes = phase2_normalize_animation_panes(raw_panes)
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

function stem_map = phase2_animation_stem_map(scenario_id)
    token = compact_phase_label_token(scenario_id);
    stem_map = struct( ...
        'evolution', sprintf('%s_vorticity_evolution', token), ...
        'streamfunction', sprintf('%s_streamfunction_evolution', token), ...
        'speed', sprintf('%s_velocity_evolution', token), ...
        'vector', sprintf('%s_vector_evolution', token), ...
        'contour', sprintf('%s_contour_evolution', token), ...
        'streamlines', sprintf('%s_streamlines_evolution', token));
end

function output_dir = phase2_workflow_visual_child_root(paths, scenario_id, method_name)
    visuals_root = pick_text(paths, {'visuals_root', 'figures_root', 'base'}, pwd);
    method_token = normalize_method_key(method_name);
    if strcmpi(method_token, 'spectral')
        method_dir = 'SM';
    else
        method_dir = 'FD';
    end
    output_dir = fullfile(char(string(visuals_root)), method_dir, phase2_visual_folder_name(scenario_id));
end

function folder_name = phase2_visual_folder_name(token)
    token = char(string(token));
    token = regexprep(token, '[^a-zA-Z0-9]+', '_');
    token = regexprep(token, '_+', '_');
    token = regexprep(token, '^_+|_+$', '');
    if isempty(token)
        folder_name = 'Scenario';
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

function z_plot = phase2_prepare_plot_matrix(z_slice, x_vec, y_vec)
    z_plot = double(z_slice);
    if size(z_plot, 1) == numel(y_vec) && size(z_plot, 2) == numel(x_vec)
        return;
    end
    if size(z_plot, 1) == numel(x_vec) && size(z_plot, 2) == numel(y_vec)
        z_plot = z_plot.';
    end
end

function value = phase2_time_value(display_times, index)
    value = NaN;
    if index >= 1 && index <= numel(display_times)
        value = double(display_times(index));
    end
end

function media = resolve_phase2_triptych_media(params, settings)
    frame_count = max(2, round(double(resolve_phase2_animation_frame_count(params, settings))));
    fps = pick_numeric(params, {'animation_fps'}, NaN);
    if ~isfinite(fps) && isstruct(settings) && isfield(settings, 'animation_fps')
        fps = double(settings.animation_fps);
    end
    if ~isfinite(fps) && isstruct(settings) && isfield(settings, 'media') && isstruct(settings.media) && ...
            isfield(settings.media, 'fps')
        fps = double(settings.media.fps);
    end
    if ~(isfinite(fps) && fps > 0)
        fps = 6;
    end
    media = struct('frame_count', frame_count, 'fps', fps, 'dpi', 144);
end

function [x_vec, y_vec] = phase2_resolve_plot_axes(analysis, params, cube_size)
    x_vec = phase2_axis_vector(pick_value(analysis, 'x', []), pick_numeric(params, {'Lx', 'Lx_m'}, 1), cube_size(2));
    y_vec = phase2_axis_vector(pick_value(analysis, 'y', []), pick_numeric(params, {'Ly', 'Ly_m'}, 1), cube_size(1));
end

function axis_vec = phase2_axis_vector(candidate, domain_length, n_points)
    axis_vec = reshape(double(candidate), 1, []);
    if numel(axis_vec) == n_points
        return;
    end
    if ~(isfinite(domain_length) && domain_length > 0)
        domain_length = 1;
    end
    axis_vec = linspace(-domain_length / 2, domain_length / 2, n_points);
end

function plot_times = phase2_resolve_plot_times(analysis, params, frame_count)
    plot_times = reshape(double(pick_value(analysis, 'snapshot_times_requested', [])), 1, []);
    if isempty(plot_times)
        plot_times = reshape(double(pick_value(analysis, 'snapshot_times', [])), 1, []);
    end
    if isempty(plot_times)
        plot_times = reshape(double(pick_value(analysis, 'time_vec', [])), 1, []);
    end
    if numel(plot_times) ~= frame_count
        tfinal = pick_numeric(analysis, {'Tfinal'}, NaN);
        if ~isfinite(tfinal)
            tfinal = pick_numeric(params, {'Tfinal', 't_final'}, 1);
        end
        plot_times = linspace(0, tfinal, frame_count);
    end
end

function [cube_out, display_times] = phase2_resample_snapshot_cube(cube_in, plot_times, target_count)
    source_count = size(cube_in, 3);
    target_count = max(2, round(double(target_count)));
    if numel(plot_times) ~= source_count
        plot_times = linspace(0, 1, source_count);
    end
    display_times = linspace(plot_times(1), plot_times(end), target_count);
    source_indices = interp1(plot_times, 1:source_count, display_times, 'nearest', 'extrap');
    source_indices = max(1, min(source_count, round(source_indices)));
    cube_out = cube_in(:, :, source_indices);
end

function rgb_frame = phase2_capture_triptych_frame(fig, output_dir, frame_index, media)
    frame_path = fullfile(output_dir, sprintf('__phase2_triptych_frame_%03d.png', frame_index));
    cleanup_frame = onCleanup(@() phase2_delete_if_exists(frame_path)); %#ok<NASGU>
    exportgraphics(fig, frame_path, 'Resolution', media.dpi);
    rgb_frame = imread(frame_path);
end

function phase2_safe_close_figure(fig)
    try
        if ~isempty(fig) && isgraphics(fig)
            close(fig);
        end
    catch
    end
end

function phase2_delete_if_exists(path_text)
    if exist(path_text, 'file') == 2
        delete(path_text);
    end
end

function write_phase2_report(Results, paths)
    report_path = fullfile(paths.reports, 'Phase2_Boundary_Case_Study_Report.md');
    ensure_parent_directory(report_path);
    fid = fopen(report_path, 'w');
    if fid < 0
        error('Phase2BoundaryConditionStudy:ReportWriteFailed', 'Could not write report: %s', report_path);
    end
    cleaner = onCleanup(@() fclose(fid));
    fprintf(fid, '# Phase 2 Boundary Condition Study\n\n');
    fprintf(fid, '- Phase ID: `%s`\n', Results.phase_id);
    fprintf(fid, '- Workflow: `%s`\n', Results.workflow_kind);
    fprintf(fid, '- Method policy: FD-only active runtime\n\n');
    fprintf(fid, '| Scenario | FD runtime (s) | FD mesh |\n');
    fprintf(fid, '| --- | ---: | --- |\n');
    for i = 1:numel(Results.scenarios)
        scen = Results.scenarios(i);
        fprintf(fid, '| %s | %.3f | %s |\n', ...
            scen.scenario_label, ...
            double(pick_value(scen.summary, 'runtime_wall_s', NaN)), ...
            sprintf('%dx%d', round(double(pick_value(scen.summary, 'mesh_nx', NaN))), ...
                round(double(pick_value(scen.summary, 'mesh_ny', NaN)))));
    end
    clear cleaner
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

function key = normalize_method_key(method)
    token = lower(strtrim(char(string(method))));
    token = regexprep(token, '[\s_-]+', '_');
    switch token
        case {'fd', 'finite_difference'}
            key = 'fd';
        case {'spectral', 'fft', 'pseudo_spectral', 'sm'}
            key = 'spectral';
        otherwise
            key = token;
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

function job = make_job(label, method, stage, job_key, queue_index, run_config, parameters, settings, phase_paths, scenario)
    job = empty_job();
    job.label = label;
    job.method = method;
    job.method_key = normalize_method_key(method);
    job.stage = stage;
    job.job_key = job_key;
    job.queue_index = queue_index;
    child_root = pick_text(phase_paths, {'matlab_data_root', 'runs_root', 'base'}, '');
    job.output_root = fullfile(child_root, compact_phase_job_dir_name(queue_index, scenario.id, method));
    job.run_config = run_config;
    job.parameters = parameters;
    job.settings = settings;
    job.settings.output_root = job.output_root;
    job.settings.preinitialized_artifact_root = true;
    job.scenario = scenario;
end

function storage_id = make_phase_storage_id(phase_id)
    raw = lower(regexprep(char(string(phase_id)), '[^a-z0-9]+', '_'));
    stamp = compact_phase_stamp_token(raw);
    label = regexprep(raw, '_?\d{8}_\d{6}$', '');
    label = regexprep(label, '^phase\d*_?', '');
    ic_token = compact_phase_label_token(label);
    storage_id = sprintf('p2_%s_%s', stamp, ic_token);
end

function child_id = make_phase_child_identifier(phase_id, queue_index, scenario_id, method_name)
    %#ok<INUSD>
    method_tag = normalize_method_key(method_name);
    scenario_tag = compact_phase_label_token(scenario_id);
    scenario_tag = scenario_tag(1:min(3, numel(scenario_tag)));
    child_id = sprintf('p2%02d%s%s', round(double(queue_index)), method_tag, scenario_tag);
end

function dir_name = compact_phase_job_dir_name(queue_index, scenario_id, method_name)
    dir_name = sprintf('%02d_%s_%s', round(double(queue_index)), ...
        compact_phase_label_token(method_name), compact_phase_label_token(scenario_id));
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
    label_raw = regexprep(lower(char(string(label_raw))), '[^a-z0-9]+', '_');
    parts = regexp(label_raw, '_+', 'split');
    parts = parts(~cellfun(@isempty, parts));
    if isempty(parts)
        token = 'wf';
        return;
    end
    if numel(parts) == 1
        token = parts{1}(1:min(8, numel(parts{1})));
        return;
    end
    initials = '';
    for i = 1:min(4, numel(parts))
        initials(end + 1) = parts{i}(1); %#ok<AGROW>
    end
    token = initials;
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
        'scenario', struct());
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
    output.scenario = job.scenario;
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
    output.scenario = job.scenario;
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
        'scenario', struct());
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

function jobs = queue_outputs_to_jobs(outputs)
    jobs = repmat(empty_job(), 1, numel(outputs));
    for i = 1:numel(outputs)
        jobs(i).label = outputs(i).label;
        jobs(i).method = outputs(i).method;
        jobs(i).method_key = outputs(i).method_key;
        jobs(i).stage = outputs(i).stage;
        jobs(i).job_key = outputs(i).job_key;
        jobs(i).queue_index = outputs(i).queue_index;
        jobs(i).scenario = outputs(i).scenario;
        if isfield(outputs(i), 'paths') && isstruct(outputs(i).paths) && isfield(outputs(i).paths, 'base')
            jobs(i).output_root = outputs(i).paths.base;
        end
    end
end

function write_json(path, payload)
    ensure_parent_directory(path);
    fid = fopen(path, 'w');
    if fid < 0
        error('Phase2BoundaryConditionStudy:JsonWriteFailed', 'Could not write JSON file: %s', path);
    end
    cleaner = onCleanup(@() fclose(fid));
    fprintf(fid, '%s', jsonencode(payload));
    clear cleaner
end

function tf = json_saving_enabled(varargin)
    tf = false;
    for i = 1:nargin
        source = varargin{i};
        if ~isstruct(source)
            continue;
        end
        if isfield(source, 'save_json') && ~isempty(source.save_json)
            tf = logical(source.save_json);
        end
    end
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

function val = pick_value(s, field, default)
    if isstruct(s) && isfield(s, field) && ~isempty(s.(field))
        val = s.(field);
    else
        val = default;
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

function out = pick_struct(s, fields, default)
    out = default;
    if ~isstruct(s)
        return;
    end
    for i = 1:numel(fields)
        if isfield(s, fields{i}) && isstruct(s.(fields{i})) && ~isempty(fieldnames(s.(fields{i})))
            out = s.(fields{i});
            return;
        end
    end
end
