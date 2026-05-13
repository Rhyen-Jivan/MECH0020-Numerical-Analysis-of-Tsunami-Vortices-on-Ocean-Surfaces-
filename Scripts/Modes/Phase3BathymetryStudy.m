function [Results, paths] = Phase3BathymetryStudy(Run_Config, Parameters, Settings)
% Phase3BathymetryStudy - Bathymetry workflow with FD + Spectral runs.
    if nargin < 3
        error('Phase3BathymetryStudy:InvalidInputs', ...
            'Run_Config, Parameters, and Settings are required.');
    end

    phase_cfg = resolve_phase3_config(Parameters);
    phase_id = make_phase3_id(Run_Config);
    paths = build_phase3_paths(Settings, phase_id);
    ensure_phase3_directories(paths);

    phase_parameters = force_phase3_parameters(Parameters, phase_cfg);
    phase_settings = normalize_phase3_settings(Settings, phase_cfg);
    phase_settings = configure_phase3_runtime_output_paths(phase_settings, paths);
    progress_callback = resolve_phase3_progress_callback(phase_settings);
    emit_phase3_runtime_log(progress_callback, sprintf('Phase 3 preflight: initializing artifacts at %s', paths.base), 'info');
    phase_timer = tic;

    safe_save_mat(fullfile(paths.config, 'Phase3_Config.mat'), struct( ...
        'Run_Config_clean', filter_graphics_objects(Run_Config), ...
        'phase_parameters_clean', filter_graphics_objects(phase_parameters), ...
        'phase_settings_clean', filter_graphics_objects(phase_settings), ...
        'phase_cfg_clean', filter_graphics_objects(phase_cfg)));
    write_run_settings_text(paths.run_settings_path, ...
        'Run Config', Run_Config, ...
        'Phase Parameters', phase_parameters, ...
        'Phase Settings', phase_settings, ...
        'Phase Config', phase_cfg);
    emit_phase3_runtime_log(progress_callback, sprintf('Phase 3 saved run settings: %s', paths.run_settings_path), 'info');

    jobs = build_phase3_queue_jobs(phase_id, phase_parameters, phase_settings, paths, phase_cfg);
    initialize_phase3_queue_artifacts(jobs);
    emit_phase3_runtime_log(progress_callback, sprintf('Phase 3 queue initialized: %d jobs under %s', numel(jobs), paths.runs_root), 'info');
    queue_outputs = run_phase3_queue(jobs, progress_callback, phase_id, phase_timer);
    scenarios = assemble_phase3_scenarios(queue_outputs, phase_cfg);

    Results = struct();
    Results.run_id = phase_id;
    Results.phase_id = phase_id;
    Results.workflow_kind = 'phase3_bathymetry_study';
    Results.result_layout_kind = 'phase3_workflow';
    Results.phase_name = 'Phase 3 bathymetry study';
    Results.phase_config = phase_cfg;
    Results.parent_run_config = filter_graphics_objects(Run_Config);
    Results.parent_parameters = summarize_phase3_parameters(phase_parameters);
    Results.scenarios = scenarios;
    Results.workflow_queue = build_phase3_queue_status_snapshot(queue_outputs_to_phase3_jobs(queue_outputs), queue_outputs, numel(queue_outputs), 'completed');
    Results.paths = paths;
    Results.wall_time = toc(phase_timer);
    Results.workflow_manifest = build_phase3_workflow_manifest( ...
        phase_id, queue_outputs, phase_cfg, paths, Results.parent_parameters, Results.parent_run_config, scenarios);

    ResultsForSave = strip_phase3_for_persistence(Results);
    ResultsForSave.artifact_layout_version = char(string(paths.artifact_layout_version));
    ResultsForSave.workflow_manifest.artifact_layout_version = char(string(paths.artifact_layout_version));
    save(fullfile(paths.data, 'phase3_results.mat'), 'ResultsForSave', '-v7.3');
    emit_phase3_runtime_log(progress_callback, sprintf('Phase 3 saved MAT results: %s', fullfile(paths.data, 'phase3_results.mat')), 'info');
    write_phase3_json(fullfile(paths.data, 'phase3_results.json'), ResultsForSave);
    emit_phase3_runtime_log(progress_callback, sprintf('Phase 3 saved JSON results: %s', fullfile(paths.data, 'phase3_results.json')), 'info');
    safe_save_mat(fullfile(paths.data, 'phase3_workflow_manifest.mat'), struct('workflow_manifest', ResultsForSave.workflow_manifest));
    emit_phase3_runtime_log(progress_callback, sprintf('Phase 3 saved workflow manifest MAT: %s', fullfile(paths.data, 'phase3_workflow_manifest.mat')), 'info');
    write_phase3_json(fullfile(paths.data, 'phase3_workflow_manifest.json'), ResultsForSave.workflow_manifest);
    emit_phase3_runtime_log(progress_callback, sprintf('Phase 3 saved workflow manifest JSON: %s', fullfile(paths.data, 'phase3_workflow_manifest.json')), 'info');
    emit_phase3_completion_report_payload(progress_callback, ResultsForSave, paths, ...
        Run_Config, Results.parent_parameters, 'Phase 3', ...
        Results.workflow_kind, Results.result_layout_kind);
    write_phase3_report(ResultsForSave, paths);
    emit_phase3_runtime_log(progress_callback, sprintf('Phase 3 saved report: %s', fullfile(paths.reports, 'Phase3_Bathymetry_Study_Report.md')), 'info');

    emit_phase3_queue_payload(progress_callback, phase_id, jobs(end), 'completed', 100, toc(phase_timer), ...
        'Phase 3 complete: simple default, composite tsunami run-up, and Tohoku bathymetry scenarios finished.', jobs, queue_outputs);
end

function phase_cfg = resolve_phase3_config(Parameters)
    defaults = create_default_parameters();
    if ~isfield(defaults, 'phase3') || ~isstruct(defaults.phase3)
        error('Phase3BathymetryStudy:MissingDefaults', ...
            'create_default_parameters must define phase3 defaults.');
    end
    phase_cfg = defaults.phase3;
    if isfield(Parameters, 'phase3') && isstruct(Parameters.phase3)
        phase_cfg = merge_structs(phase_cfg, Parameters.phase3);
    end
    if ~isfield(phase_cfg, 'scenarios') || ~isstruct(phase_cfg.scenarios) || isempty(phase_cfg.scenarios)
        error('Phase3BathymetryStudy:MissingScenarioDefaults', ...
            'Phase 3 defaults must define three editable scenarios.');
    end
    if numel(phase_cfg.scenarios) ~= 3
        error('Phase3BathymetryStudy:ScenarioCount', ...
            'Phase 3 must define exactly three scenarios.');
    end
    phase_cfg.domain_equal_mesh = logical(pick_phase3_value(phase_cfg, 'domain_equal_mesh', false));
    phase_cfg.domain_nx = max(8, round(double(pick_phase3_value(phase_cfg, 'domain_nx', 128))));
    phase_cfg.domain_ny = max(8, round(double(pick_phase3_value(phase_cfg, 'domain_ny', phase_cfg.domain_nx))));
    if phase_cfg.domain_equal_mesh
        phase_cfg.domain_ny = phase_cfg.domain_nx;
    end
    phase_cfg.domain_lx = max(1.0e-6, double(pick_phase3_value(phase_cfg, 'domain_lx', 10.0)));
    phase_cfg.domain_ly = max(1.0e-6, double(pick_phase3_value(phase_cfg, 'domain_ly', 10.0)));
    phase_cfg.save_figures = logical(pick_phase3_value(phase_cfg, 'save_figures', true));
    phase_cfg.create_animations = logical(pick_phase3_value(phase_cfg, 'create_animations', false));
    phase_cfg.num_plot_snapshots = max(1, round(double(pick_phase3_value(phase_cfg, 'num_plot_snapshots', 9))));
    if ~isfield(phase_cfg, 'methods') || isempty(phase_cfg.methods)
        phase_cfg.methods = {'FD', 'Spectral'};
    end
    for i = 1:numel(phase_cfg.scenarios)
        phase_cfg.scenarios(i) = normalize_phase3_scenario(phase_cfg.scenarios(i), i);
    end
end

function scenario = normalize_phase3_scenario(scenario, index)
    scenario = filter_graphics_objects(scenario);
    scenario.id = char(string(pick_phase3_text(scenario, {'id'}, sprintf('phase3_scenario_%d', index))));
    scenario.label = char(string(pick_phase3_text(scenario, {'label', 'bathymetry_label'}, sprintf('Scenario %d', index))));
    scenario.bc_id = BCDispatcher.extract_bc_case(struct('bc_case', pick_phase3_text(scenario, {'bc_id'}, 'enclosed_shear_layer')));
    scenario.bc_label = char(string(pick_phase3_text(scenario, {'bc_label'}, scenario.bc_id)));
    scenario.bathymetry_id = normalize_bathymetry_scenario_token(pick_phase3_text(scenario, {'bathymetry_id'}, 'flat_2d'));
    scenario.bathymetry_label = char(string(pick_phase3_text(scenario, {'bathymetry_label'}, scenario.bathymetry_id)));
    scenario.top_speed_mps = double(pick_phase3_value(scenario, 'top_speed_mps', 0.0));
    scenario.bottom_speed_mps = double(pick_phase3_value(scenario, 'bottom_speed_mps', 0.0));
    scenario.left_speed_mps = double(pick_phase3_value(scenario, 'left_speed_mps', 0.0));
    scenario.right_speed_mps = double(pick_phase3_value(scenario, 'right_speed_mps', 0.0));
    if ~isfield(scenario, 'bathymetry_state') || ~isstruct(scenario.bathymetry_state)
        scenario.bathymetry_state = struct();
    end
end

function phase_id = make_phase3_id(Run_Config)
    if isfield(Run_Config, 'phase_id') && ~isempty(Run_Config.phase_id)
        phase_id = char(string(Run_Config.phase_id));
        return;
    end
    ic = 'ic';
    if isfield(Run_Config, 'ic_type') && ~isempty(Run_Config.ic_type)
        ic = regexprep(lower(char(string(Run_Config.ic_type))), '[^a-z0-9]+', '_');
    end
    phase_id = sprintf('phase3_%s_%s', ic, char(datetime('now', 'Format', 'yyyyMMdd_HHmmss')));
end

function paths = build_phase3_paths(Settings, phase_id)
    output_root = 'Results';
    if isfield(Settings, 'output_root') && ~isempty(Settings.output_root)
        output_root = Settings.output_root;
    end
    paths = PathBuilder.get_phase_paths('Phase3', phase_id, output_root);
    paths.phase_id = phase_id;
end

function ensure_phase3_directories(paths)
    targets = { ...
        paths.base, ...
        paths.matlab_data_root, ...
        paths.metrics_root, ...
        paths.visuals_root, ...
        paths.config, ...
        paths.runs_root};
    for i = 1:numel(targets)
        target = char(string(targets{i}));
        if ~isempty(target) && exist(target, 'dir') ~= 7
            mkdir(target);
        end
    end
end

function params = force_phase3_parameters(Parameters, phase_cfg)
    params = Parameters;
    params.phase3 = phase_cfg;
    params.create_animations = false;
    params.bathymetry_dimension_policy = 'by_method';
    if ~isfield(params, 'resource_strategy') || isempty(params.resource_strategy) || ...
            strcmpi(char(string(params.resource_strategy)), 'mode_adaptive')
        params.resource_strategy = pick_phase3_text(phase_cfg, {'resource_strategy'}, 'throughput_first');
    end
end

function settings = normalize_phase3_settings(SettingsInput, phase_cfg)
    settings = Settings();
    if nargin >= 1 && isstruct(SettingsInput)
        settings = merge_structs(settings, SettingsInput);
    end
    settings.save_data = true;
    settings.save_reports = true;
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
    desired_strategy = pick_phase3_text(phase_cfg, {'resource_strategy'}, 'throughput_first');
    if ~isfield(settings, 'resource_allocation') || ~isstruct(settings.resource_allocation)
        settings.resource_allocation = struct();
    end
    if ~isfield(settings.resource_allocation, 'resource_strategy') || ...
            isempty(settings.resource_allocation.resource_strategy) || ...
            strcmpi(char(string(settings.resource_allocation.resource_strategy)), 'mode_adaptive')
        settings.resource_allocation.resource_strategy = desired_strategy;
    end
end

function settings = configure_phase3_runtime_output_paths(settings, paths)
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
    settings.sustainability.collector_runtime.session_output_dir = pick_phase3_text(paths, {'metrics_root'}, '');
    settings.sustainability.collector_runtime.hwinfo_csv_target_dir = pick_phase3_text(paths, {'metrics_root'}, '');
    settings.sustainability.collector_runtime.hwinfo_csv_target_path = pick_phase3_text(paths, {'raw_hwinfo_csv_path'}, '');
end

function initialize_phase3_queue_artifacts(jobs)
    if nargin < 1 || ~isstruct(jobs) || isempty(jobs)
        return;
    end
    for i = 1:numel(jobs)
        base_root = pick_phase3_text(jobs(i), {'output_root'}, '');
        if strlength(string(base_root)) == 0
            continue;
        end
        paths = PathBuilder.get_existing_root_paths(base_root, pick_phase3_text(jobs(i), {'method'}, 'FD'), 'Evolution');
        PathBuilder.ensure_directories(paths);
        PathBuilder.ensure_run_settings_placeholder(paths.run_settings_path, pick_phase3_text(jobs(i), {'job_key'}, sprintf('job_%02d', i)));
    end
end

function emit_phase3_runtime_log(progress_callback, message, log_type)
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

function emit_phase3_completion_report_payload(progress_callback, results_for_save, paths, run_config, parameters, phase_label, workflow_kind, result_layout_kind)
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
        phase_label = 'Phase 3';
    end
    if nargin < 7 || strlength(string(workflow_kind)) == 0
        workflow_kind = pick_phase3_text(results_for_save, {'workflow_kind'}, 'phase3_bathymetry_study');
    end
    if nargin < 8 || strlength(string(result_layout_kind)) == 0
        result_layout_kind = pick_phase3_text(results_for_save, {'result_layout_kind'}, 'phase3_workflow');
    end
    published_run_config = filter_graphics_objects(run_config);
    published_run_config.workflow_kind = char(string(workflow_kind));
    published_run_config.result_layout_kind = char(string(result_layout_kind));
    published_run_config.phase_label = char(string(phase_label));
    published_run_config.launch_origin = pick_phase3_text(published_run_config, {'launch_origin'}, 'phase_button');
    phase_id = pick_phase3_text(results_for_save, {'phase_id', 'run_id'}, pick_phase3_text(published_run_config, {'phase_id', 'run_id'}, ''));
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

function progress_callback = resolve_phase3_progress_callback(Settings)
    progress_callback = resolve_runtime_progress_callback(Settings);
end

function jobs = build_phase3_queue_jobs(phase_id, params, settings, paths, phase_cfg)
    methods = resolve_phase3_methods(phase_cfg);
    jobs = repmat(empty_phase3_job(), 1, numel(phase_cfg.scenarios) * numel(methods));
    cursor = 1;
    for i = 1:numel(phase_cfg.scenarios)
        for j = 1:numel(methods)
            jobs(cursor) = build_phase3_scenario_job(methods{j}, cursor, phase_id, params, settings, paths, phase_cfg, phase_cfg.scenarios(i));
            cursor = cursor + 1;
        end
    end
end

function job = build_phase3_scenario_job(method_name, queue_index, phase_id, params, settings, paths, phase_cfg, scenario)
    p = apply_phase3_scenario_to_parameters(params, phase_cfg, scenario);
    p.method = phase3_method_to_parameter_token(method_name);
    p.analysis_method = method_name;
    p.mode = 'Evolution';
    p.time_integrator = resolve_phase3_method_integrator(method_name);
    plot_snapshot_count = resolve_phase3_plot_snapshot_count(params);
    animation_frame_count = resolve_phase3_animation_frame_count(params, settings);
    p.create_animations = false;
    p.num_plot_snapshots = plot_snapshot_count;
    p.animation_num_frames = animation_frame_count;
    p.num_animation_frames = animation_frame_count;
    p.num_snapshots = max(plot_snapshot_count, animation_frame_count);
    p = normalize_snapshot_schedule_parameters(p);

    rc = Build_Run_Config(method_name, 'Evolution', pick_phase3_text(params, {'ic_type'}, ''));
    rc.run_id = make_phase3_child_identifier(phase_id, queue_index, scenario.id, method_name);
    rc.phase_id = phase_id;
    rc.phase_label = 'Phase 3';
    rc.phase_scenario_id = scenario.id;
    rc.phase_scenario_label = scenario.label;

    job_key = sprintf('%s_%s', normalize_phase3_method_key(method_name), scenario.id);
    job = make_phase3_job(sprintf('%s | %s', scenario.label, method_name), method_name, 'evolution', ...
        job_key, queue_index, rc, p, settings, paths, scenario);
end

function p = apply_phase3_scenario_to_parameters(params, phase_cfg, scenario)
    p = params;
    p.Nx = phase_cfg.domain_nx;
    p.Ny = phase_cfg.domain_ny;
    p.Lx = phase_cfg.domain_lx;
    p.Ly = phase_cfg.domain_ly;
    p.bc_case = scenario.bc_id;
    p.boundary_condition_case = scenario.bc_id;
    p.phase3_active_scenario = scenario.id;
    p.phase3_active_scenario_label = scenario.label;
    p.phase3_active_bc_id = scenario.bc_id;
    p.phase3_active_bathymetry_id = scenario.bathymetry_id;
    p.allow_preset_speed_overrides = true;
    p.U_top = scenario.top_speed_mps;
    p.U_bottom = scenario.bottom_speed_mps;
    p.U_left = scenario.left_speed_mps;
    p.U_right = scenario.right_speed_mps;
    p.bathymetry_scenario = scenario.bathymetry_id;
    if ~isfield(p, 'bathymetry_dynamic_params') || ~isstruct(p.bathymetry_dynamic_params)
        p.bathymetry_dynamic_params = struct();
    end
    p.bathymetry_dynamic_params.(scenario.bathymetry_id) = scenario.bathymetry_state;
    bathy_keys = fieldnames(scenario.bathymetry_state);
    for i = 1:numel(bathy_keys)
        p.(bathy_keys{i}) = scenario.bathymetry_state.(bathy_keys{i});
    end
    if isfield(scenario.bathymetry_state, 'bed_slope')
        p.bathymetry_bed_slope = double(scenario.bathymetry_state.bed_slope);
    end
    if isfield(scenario.bathymetry_state, 'depth_offset')
        p.bathymetry_depth_offset = double(scenario.bathymetry_state.depth_offset);
    end
    if isfield(scenario.bathymetry_state, 'relief_amplitude')
        p.bathymetry_relief_amplitude = double(scenario.bathymetry_state.relief_amplitude);
    end
    if isfield(scenario.bathymetry_state, 'interpolation_resolution')
        p.bathymetry_resolution = round(double(scenario.bathymetry_state.interpolation_resolution));
    end
end

function count = resolve_phase3_plot_snapshot_count(params)
    count = max(1, round(double(pick_phase3_value(pick_phase3_value(params, 'phase3', struct()), ...
        'num_plot_snapshots', pick_phase3_value(params, 'num_plot_snapshots', 9)))));
end

function count = resolve_phase3_animation_frame_count(params, settings)
    count = NaN;
    phase_cfg = pick_phase3_value(params, 'phase3', struct());
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
        count = resolve_phase3_plot_snapshot_count(params);
    end
    count = max(2, round(count));
end

function outputs = run_phase3_queue(jobs, progress_callback, phase_id, phase_timer)
    outputs = repmat(empty_phase3_output(), 1, numel(jobs));
    for i = 1:numel(jobs)
        emit_phase3_queue_payload(progress_callback, phase_id, jobs(i), 'queued', 0, toc(phase_timer), ...
            sprintf('Queued Phase 3 scenario job %d/%d: %s | %s', i, numel(jobs), jobs(i).scenario.label, jobs(i).method), jobs, outputs);
    end
    for i = 1:numel(jobs)
        running_pct = 100 * ((i - 1) / max(numel(jobs), 1));
        emit_phase3_queue_payload(progress_callback, phase_id, jobs(i), 'running', running_pct, toc(phase_timer), ...
            sprintf('Starting Phase 3 scenario job %d/%d: %s | %s', i, numel(jobs), jobs(i).scenario.label, jobs(i).method), jobs, outputs);
        try
            [result_payload, path_payload] = run_phase3_dispatched_job(jobs(i).run_config, jobs(i).parameters, jobs(i).settings);
            outputs(i) = make_phase3_output(jobs(i), result_payload, path_payload, 'dispatcher_queue');
            completed_pct = 100 * (i / max(numel(jobs), 1));
            emit_phase3_queue_payload(progress_callback, phase_id, jobs(i), 'completed', completed_pct, toc(phase_timer), ...
                build_phase3_child_completion_message(jobs(i), outputs(i), i, numel(jobs)), jobs, outputs);
        catch ME
            outputs(i) = make_phase3_failed_output(jobs(i), ME);
            emit_phase3_queue_payload(progress_callback, phase_id, jobs(i), 'failed', NaN, toc(phase_timer), ...
                sprintf('Phase 3 scenario failed: [%s] %s', ME.identifier, ME.message), jobs, outputs);
            rethrow(ME);
        end
    end
end

function [result_payload, path_payload] = run_phase3_dispatched_job(run_config, parameters, settings)
    child_run_config = run_config;
    if isfield(child_run_config, 'workflow_kind')
        child_run_config = rmfield(child_run_config, 'workflow_kind');
    end
    [result_payload, path_payload] = RunDispatcher(child_run_config, parameters, settings);
end

function emit_phase3_queue_payload(progress_callback, phase_id, job, status, progress_pct, elapsed_wall, terminal_message, jobs, outputs)
    if isempty(progress_callback)
        return;
    end
    queue_total = numel(jobs);
    queue_status = build_phase3_queue_status_snapshot(jobs, outputs, job.queue_index, status);
    current_output = empty_phase3_output();
    if numel(outputs) >= job.queue_index && isstruct(outputs(job.queue_index))
        current_output = outputs(job.queue_index);
    end
    child_run_id = pick_phase3_text(current_output.run_config, {'run_id', 'study_id'}, pick_phase3_text(job.run_config, {'run_id', 'study_id'}, ''));
    child_artifact_root = pick_phase3_text(current_output.paths, {'base'}, char(string(job.output_root)));
    child_figures_root = pick_phase3_text(current_output.paths, {'figures_root', 'figures_evolution'}, '');
    child_reports_root = pick_phase3_text(current_output.paths, {'reports'}, '');

    payload = struct();
    payload.channel = 'workflow';
    payload.phase = 'phase3';
    payload.phase_id = phase_id;
    payload.run_id = phase_id;
    payload.workflow_kind = 'phase3_bathymetry_study';
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
    payload.mesh_nx = double(pick_phase3_numeric(job.parameters, {'Nx'}, NaN));
    payload.mesh_ny = double(pick_phase3_numeric(job.parameters, {'Ny'}, NaN));
    payload.test_case_setup = build_phase3_test_case_setup(job.scenario);
    payload.scenario_label = job.scenario.label;
    payload.workflow_overall_progress_pct = double(progress_pct);
    payload.progress_pct = double(progress_pct);
    payload.elapsed_wall = double(elapsed_wall);
    payload.status_text = sprintf('Phase 3 [%d/%d] %s (%s)', round(double(job.queue_index)), round(double(queue_total)), job.scenario.label, char(string(status)));
    payload.event_key = sprintf('%s_%02d_%s_%s', phase_id, round(double(job.queue_index)), job.job_key, lower(char(string(status))));
    payload.queue_status = queue_status;
    payload.terminal_message = char(string(terminal_message));
    payload.bc_id = job.scenario.bc_id;
    payload.bathymetry_id = job.scenario.bathymetry_id;
    try
        invoke_runtime_progress_callback(progress_callback, payload);
    catch ME
        warning('Phase3BathymetryStudy:ProgressCallbackDisabled', ...
            'Phase workflow progress callback failed and will be ignored: %s', ME.message);
    end
end

function queue_status = build_phase3_queue_status_snapshot(jobs, outputs, active_index, active_status)
    queue_status = repmat(struct('queue_index', NaN, 'job_key', '', 'job_label', '', 'method', '', ...
        'mode', '', 'mesh_nx', NaN, 'mesh_ny', NaN, 'test_case_setup', '', 'scenario_label', '', 'status', 'queued', 'run_id', '', ...
        'artifact_root', '', 'figures_root', '', 'reports_root', '', 'bc_id', '', 'bathymetry_id', ''), 1, numel(jobs));
    for i = 1:numel(jobs)
        queue_status(i).queue_index = jobs(i).queue_index;
        queue_status(i).job_key = jobs(i).job_key;
        queue_status(i).job_label = jobs(i).scenario.label;
        queue_status(i).method = jobs(i).method;
        queue_status(i).mode = jobs(i).stage;
        queue_status(i).artifact_root = jobs(i).output_root;
        queue_status(i).mesh_nx = pick_phase3_numeric(jobs(i).parameters, {'Nx'}, NaN);
        queue_status(i).mesh_ny = pick_phase3_numeric(jobs(i).parameters, {'Ny'}, NaN);
        queue_status(i).test_case_setup = build_phase3_test_case_setup(jobs(i).scenario);
        queue_status(i).scenario_label = jobs(i).scenario.label;
        queue_status(i).bc_id = pick_phase3_text(jobs(i).scenario, {'bc_id'}, '');
        queue_status(i).bathymetry_id = pick_phase3_text(jobs(i).scenario, {'bathymetry_id'}, '');
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
            queue_status(i).run_id = pick_phase3_text(outputs(i).run_config, {'run_id', 'study_id'}, '');
            queue_status(i).artifact_root = pick_phase3_text(outputs(i).paths, {'base'}, queue_status(i).artifact_root);
            queue_status(i).figures_root = pick_phase3_text(outputs(i).paths, {'figures_root', 'figures_evolution'}, '');
            queue_status(i).reports_root = pick_phase3_text(outputs(i).paths, {'reports'}, '');
        end
    end
end

function message = build_phase3_child_completion_message(job, output, queue_index, queue_total)
    analysis = require_phase3_analysis(output.results, job.label);
    nx = pick_phase3_numeric(analysis, {'Nx'}, pick_phase3_numeric(job.parameters, {'Nx'}, NaN));
    ny = pick_phase3_numeric(analysis, {'Ny'}, pick_phase3_numeric(job.parameters, {'Ny'}, NaN));
    message = sprintf('Completed Phase 3 scenario job %d/%d: %s | %s | bathymetry=%s | mesh=%dx%d', ...
        queue_index, queue_total, job.scenario.label, job.method, job.scenario.bathymetry_label, round(nx), round(ny));
end

function text = build_phase3_test_case_setup(scenario)
    bc_id = pick_phase3_text(scenario, {'bc_id'}, '');
    bathymetry_id = pick_phase3_text(scenario, {'bathymetry_id'}, '');
    text = sprintf('bc_case=%s | bathymetry=%s', char(string(bc_id)), char(string(bathymetry_id)));
end

function scenarios = assemble_phase3_scenarios(queue_outputs, phase_cfg)
    scenarios = repmat(struct( ...
        'scenario_id', '', ...
        'scenario_label', '', ...
        'bc_id', '', ...
        'bc_label', '', ...
        'bathymetry_id', '', ...
        'bathymetry_label', '', ...
        'result_layout_kind', 'phase3_scenario', ...
        'fd', struct(), ...
        'spectral', struct(), ...
        'combined', struct(), ...
        'summary', struct()), 1, numel(phase_cfg.scenarios));

    for i = 1:numel(phase_cfg.scenarios)
        scenario = phase_cfg.scenarios(i);
        fd_output = resolve_phase3_output(queue_outputs, scenario.id, 'fd');
        spectral_output = resolve_phase3_output(queue_outputs, scenario.id, 'spectral');
        fd_summary = build_phase3_child_view_summary(fd_output);
        spectral_summary = build_phase3_child_view_summary(spectral_output);
        combined = build_phase3_combined_payload(fd_output, spectral_output, phase_cfg);
        summary = struct( ...
            'scenario_id', scenario.id, ...
            'scenario_label', scenario.label, ...
            'bc_id', scenario.bc_id, ...
            'bc_label', scenario.bc_label, ...
            'bathymetry_id', scenario.bathymetry_id, ...
            'bathymetry_label', scenario.bathymetry_label, ...
            'artifact_root', pick_phase3_text(fd_output.paths, {'base'}, ''), ...
            'figures_root', pick_phase3_text(fd_output.paths, {'figures_root', 'figures_evolution'}, ''), ...
            'reports_root', pick_phase3_text(fd_output.paths, {'reports'}, ''), ...
            'runtime_wall_s', pick_phase3_numeric(fd_output.results, {'wall_time', 'total_time'}, fd_output.wall_time), ...
            'spectral_runtime_wall_s', pick_phase3_numeric(spectral_output.results, {'wall_time', 'total_time'}, spectral_output.wall_time), ...
            'mesh_nx', pick_phase3_numeric(fd_output.parameters, {'Nx'}, NaN), ...
            'mesh_ny', pick_phase3_numeric(fd_output.parameters, {'Ny'}, NaN), ...
            'spectral_mesh_nx', pick_phase3_numeric(spectral_output.parameters, {'Nx'}, NaN), ...
            'spectral_mesh_ny', pick_phase3_numeric(spectral_output.parameters, {'Ny'}, NaN));

        scenarios(i).scenario_id = scenario.id;
        scenarios(i).scenario_label = scenario.label;
        scenarios(i).bc_id = scenario.bc_id;
        scenarios(i).bc_label = scenario.bc_label;
        scenarios(i).bathymetry_id = scenario.bathymetry_id;
        scenarios(i).bathymetry_label = scenario.bathymetry_label;
        scenarios(i).fd = struct('supported', true, 'output', strip_phase3_heavy_outputs(fd_output), 'view_summary', fd_summary);
        scenarios(i).spectral = struct('supported', true, 'output', strip_phase3_heavy_outputs(spectral_output), 'view_summary', spectral_summary);
        scenarios(i).combined = combined;
        scenarios(i).summary = summary;
    end
end

function summary = build_phase3_child_view_summary(evolution_output)
    meta = build_phase3_child_metadata(evolution_output);
    summary = struct( ...
        'results', evolution_output.results, ...
        'parameters', evolution_output.parameters, ...
        'run_config', evolution_output.run_config, ...
        'analysis', require_phase3_analysis(evolution_output.results, evolution_output.label), ...
        'paths', evolution_output.paths, ...
        'metadata', meta, ...
        'wall_time', evolution_output.wall_time, ...
        'workflow_child', true);
end

function meta = build_phase3_child_metadata(evolution_output)
    meta = struct();
    meta.method = evolution_output.method;
    meta.mode = 'Evolution';
    meta.ic_type = pick_phase3_text(evolution_output.run_config, {'ic_type'}, '');
    meta.bc_case = pick_phase3_text(evolution_output.parameters, {'boundary_condition_case', 'bc_case'}, '');
    meta.bathymetry_scenario = pick_phase3_text(evolution_output.parameters, {'bathymetry_scenario'}, '');
    meta.run_id = pick_phase3_text(evolution_output.run_config, {'run_id', 'study_id'}, '');
    meta.timestamp = char(datetime('now', 'Format', 'yyyy-MM-dd HH:mm:ss'));
    meta.wall_time = pick_phase3_numeric(evolution_output.results, {'wall_time', 'total_time'}, evolution_output.wall_time);
    meta.max_omega = pick_phase3_numeric(evolution_output.results, {'max_omega'}, NaN);
    meta.total_steps = pick_phase3_numeric(evolution_output.results, {'total_steps'}, NaN);
    meta.scenario_id = pick_phase3_text(evolution_output.scenario, {'id'}, '');
    meta.scenario_label = pick_phase3_text(evolution_output.scenario, {'label'}, '');
    meta.num_plot_snapshots = pick_phase3_numeric(evolution_output.parameters, {'num_plot_snapshots'}, NaN);
    meta.animation_num_frames = pick_phase3_numeric(evolution_output.parameters, {'animation_num_frames', 'num_animation_frames'}, NaN);
    meta.num_snapshots = pick_phase3_numeric(evolution_output.parameters, {'num_snapshots'}, NaN);
end

function combined = build_phase3_combined_payload(fd_output, spectral_output, phase_cfg)
    fd_analysis = require_phase3_analysis(fd_output.results, fd_output.label);
    sp_analysis = require_phase3_analysis(spectral_output.results, spectral_output.label);
    elapsed_time_fd = extract_phase3_elapsed_time_series(fd_analysis);
    elapsed_time_sp = extract_phase3_elapsed_time_series(sp_analysis);
    kinetic_energy_fd = extract_phase3_series(fd_analysis, {'kinetic_energy'}, []);
    kinetic_energy_sp = extract_phase3_series(sp_analysis, {'kinetic_energy'}, []);
    enstrophy_fd = extract_phase3_series(fd_analysis, {'enstrophy'}, []);
    enstrophy_sp = extract_phase3_series(sp_analysis, {'enstrophy'}, []);
    circulation_fd = extract_phase3_series(fd_analysis, {'circulation'}, []);
    circulation_sp = extract_phase3_series(sp_analysis, {'circulation'}, []);

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
        'fd', struct('elapsed_wall_time', elapsed_time_fd, 'kinetic_energy', kinetic_energy_fd, 'enstrophy', enstrophy_fd, 'circulation', circulation_fd), ...
        'spectral', struct('elapsed_wall_time', elapsed_time_sp, 'kinetic_energy', kinetic_energy_sp, 'enstrophy', enstrophy_sp, 'circulation', circulation_sp, 'supported', true, 'reason', ''));

    combined = struct();
    combined.phase_policy = struct('spectral_supported', true, 'workflow_kind', 'phase3_bathymetry_study');
    combined.trace_key = trace_key;
    combined.overlay = overlay;
    combined.summary_metrics = struct( ...
        'scenario_id', fd_output.scenario.id, ...
        'scenario_label', fd_output.scenario.label, ...
        'bc_label', fd_output.scenario.bc_label, ...
        'bathymetry_label', fd_output.scenario.bathymetry_label, ...
        'fd_runtime_wall_s', pick_phase3_numeric(fd_output.results, {'wall_time', 'total_time'}, fd_output.wall_time), ...
        'spectral_runtime_wall_s', pick_phase3_numeric(spectral_output.results, {'wall_time', 'total_time'}, spectral_output.wall_time), ...
        'fd_mesh', sprintf('%sx%s', num2str(round(pick_phase3_numeric(fd_output.parameters, {'Nx'}, NaN))), num2str(round(pick_phase3_numeric(fd_output.parameters, {'Ny'}, NaN)))), ...
        'spectral_mesh', sprintf('%sx%s', num2str(round(pick_phase3_numeric(spectral_output.parameters, {'Nx'}, NaN))), num2str(round(pick_phase3_numeric(spectral_output.parameters, {'Ny'}, NaN)))), ...
        'spectral_supported', true, ...
        'spectral_reason', '');
    combined.fd_view_summary = build_phase3_child_view_summary(fd_output);
    combined.spectral_view_summary = build_phase3_child_view_summary(spectral_output);
    combined.collector_overlay = struct('shared_per_case', true, 'fd_supported', true, 'spectral_supported', true, 'time_axis_mode', 'elapsed_from_zero');
    combined.paths = struct( ...
        'fd_artifact_root', pick_phase3_text(fd_output.paths, {'base'}, ''), ...
        'fd_figures_root', pick_phase3_text(fd_output.paths, {'figures_root', 'figures_evolution'}, ''), ...
        'fd_reports_root', pick_phase3_text(fd_output.paths, {'reports'}, ''), ...
        'spectral_artifact_root', pick_phase3_text(spectral_output.paths, {'base'}, ''), ...
        'spectral_figures_root', pick_phase3_text(spectral_output.paths, {'figures_root', 'figures_evolution'}, ''), ...
        'spectral_reports_root', pick_phase3_text(spectral_output.paths, {'reports'}, ''));
    combined.phase_cfg = struct('save_figures', logical(pick_phase3_value(phase_cfg, 'save_figures', true)));
end

function elapsed = extract_phase3_elapsed_time_series(analysis)
    elapsed = extract_phase3_series(analysis, {'elapsed_wall_time', 'wall_time_history', 'time_vec'}, []);
    elapsed = reshape(double(elapsed), 1, []);
    if isempty(elapsed)
        elapsed = zeros(1, 0);
        return;
    end
    elapsed = elapsed - elapsed(1);
end

function series = extract_phase3_series(s, fields, default)
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

function analysis = require_phase3_analysis(results, label)
    if isstruct(results) && isfield(results, 'analysis') && isstruct(results.analysis) && ~isempty(fieldnames(results.analysis))
        analysis = results.analysis;
        return;
    end
    data_path = pick_phase3_text(results, {'data_path'}, '');
    if ~isempty(data_path) && exist(data_path, 'file') == 2
        loaded = load(data_path, 'analysis');
        if isfield(loaded, 'analysis') && isstruct(loaded.analysis)
            analysis = loaded.analysis;
            return;
        end
    end
    error('Phase3BathymetryStudy:MissingAnalysis', 'Could not resolve analysis payload for %s.', char(string(label)));
end

function stripped = strip_phase3_heavy_outputs(outputs)
    stripped = outputs;
    for i = 1:numel(stripped)
        if isfield(stripped(i), 'results') && isstruct(stripped(i).results) && isfield(stripped(i).results, 'analysis')
            stripped(i).results.analysis = summarize_phase3_analysis(stripped(i).results.analysis);
        end
    end
end

function summary = summarize_phase3_analysis(analysis)
    summary = struct();
    keep = {'Nx', 'Ny', 'dx', 'dy', 'dt', 'Tfinal', 'grid_points', 'kinetic_energy', ...
        'enstrophy', 'circulation', 'peak_omega_history', 'max_omega_history', ...
        'peak_speed_history', 'time_vec', 'snapshot_times', 'elapsed_wall_time', ...
        'wall_model', 'lifting_model', 'boundary_profile', 'bathymetry_model', 'bathymetry_scenario'};
    for i = 1:numel(keep)
        if isfield(analysis, keep{i})
            summary.(keep{i}) = analysis.(keep{i});
        end
    end
end

function params_summary = summarize_phase3_parameters(parameters)
    params_summary = struct();
    keep = {'ic_type', 'bc_case', 'boundary_condition_case', 'bathymetry_scenario', 'Nx', 'Ny', ...
        'Lx', 'Ly', 'dt', 'Tfinal', 'nu', 'num_snapshots', 'num_plot_snapshots', ...
        'animation_num_frames', 'num_animation_frames', ...
        'phase3_active_scenario', 'phase3_active_scenario_label', 'phase3_active_bc_id', ...
        'phase3_active_bathymetry_id'};
    for i = 1:numel(keep)
        if isfield(parameters, keep{i})
            params_summary.(keep{i}) = parameters.(keep{i});
        end
    end
end

function manifest = build_phase3_workflow_manifest(phase_id, queue_outputs, phase_cfg, paths, parent_parameters, parent_run_config, scenarios)
    manifest = struct();
    manifest.phase_id = phase_id;
    manifest.workflow_kind = 'phase3_bathymetry_study';
    manifest.phase_root = paths.base;
    manifest.paths = paths;
    manifest.phase_config = filter_graphics_objects(phase_cfg);
    manifest.parent_parameters = filter_graphics_objects(parent_parameters);
    manifest.parent_run_config = filter_graphics_objects(parent_run_config);
    manifest.scenarios = strip_phase3_scenarios_for_persistence(scenarios);
    manifest.queue = repmat(struct( ...
        'queue_index', NaN, ...
        'job_key', '', ...
        'job_label', '', ...
        'scenario_id', '', ...
        'scenario_label', '', ...
        'bc_id', '', ...
        'bathymetry_id', '', ...
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
        manifest.queue(i).scenario_id = pick_phase3_text(queue_outputs(i).scenario, {'id'}, '');
        manifest.queue(i).scenario_label = pick_phase3_text(queue_outputs(i).scenario, {'label'}, '');
        manifest.queue(i).bc_id = pick_phase3_text(queue_outputs(i).scenario, {'bc_id'}, '');
        manifest.queue(i).bathymetry_id = pick_phase3_text(queue_outputs(i).scenario, {'bathymetry_id'}, '');
        manifest.queue(i).method = queue_outputs(i).method;
        manifest.queue(i).mode = queue_outputs(i).stage;
        manifest.queue(i).status = queue_outputs(i).status;
        manifest.queue(i).run_id = pick_phase3_text(queue_outputs(i).run_config, {'run_id', 'study_id'}, '');
        manifest.queue(i).artifact_root = pick_phase3_text(queue_outputs(i).paths, {'base'}, '');
        manifest.queue(i).data_path = pick_phase3_text(queue_outputs(i).results, {'data_path'}, '');
    end
end

function methods = resolve_phase3_methods(phase_cfg)
    methods = pick_phase3_value(phase_cfg, 'methods', {'FD', 'Spectral'});
    if isstring(methods)
        methods = cellstr(methods(:).');
    end
    if ischar(methods)
        methods = {methods};
    end
    if ~iscell(methods) || isempty(methods)
        methods = {'FD', 'Spectral'};
    end

    normalized = cell(1, 0);
    for i = 1:numel(methods)
        key = normalize_phase3_method_key(methods{i});
        switch key
            case 'fd'
                normalized{end + 1} = 'FD'; %#ok<AGROW>
            case 'spectral'
                normalized{end + 1} = 'Spectral'; %#ok<AGROW>
            otherwise
                error('Phase3BathymetryStudy:UnsupportedMethod', ...
                    'Phase 3 does not support method token "%s".', char(string(methods{i})));
        end
    end
    methods = normalized;
end

function output = resolve_phase3_output(queue_outputs, scenario_id, method_key)
    idx = find(arrayfun(@(entry) strcmpi(char(string(entry.scenario.id)), scenario_id) && ...
        strcmpi(char(string(entry.method_key)), method_key), queue_outputs), 1, 'first');
    if isempty(idx)
        error('Phase3BathymetryStudy:MissingScenarioMethodOutput', ...
            'Missing Phase 3 output for scenario "%s" and method "%s".', ...
            char(string(scenario_id)), char(string(method_key)));
    end
    output = queue_outputs(idx);
end

function persisted = strip_phase3_for_persistence(results_in)
    persisted = results_in;
    persisted.scenarios = strip_phase3_scenarios_for_persistence(persisted.scenarios);
    if isfield(persisted, 'workflow_manifest') && isstruct(persisted.workflow_manifest)
        persisted.workflow_manifest.scenarios = strip_phase3_scenarios_for_persistence( ...
            persisted.workflow_manifest.scenarios);
    end
end

function scenarios = strip_phase3_scenarios_for_persistence(scenarios)
    if ~isstruct(scenarios) || isempty(scenarios)
        return;
    end
    for i = 1:numel(scenarios)
        if isfield(scenarios(i), 'fd') && isstruct(scenarios(i).fd) && isfield(scenarios(i).fd, 'view_summary')
            scenarios(i).fd.view_summary = strip_phase3_view_summary_for_persistence(scenarios(i).fd.view_summary);
        end
        if isfield(scenarios(i), 'spectral') && isstruct(scenarios(i).spectral) && isfield(scenarios(i).spectral, 'view_summary')
            scenarios(i).spectral.view_summary = strip_phase3_view_summary_for_persistence(scenarios(i).spectral.view_summary);
        end
        if isfield(scenarios(i), 'combined') && isstruct(scenarios(i).combined)
            if isfield(scenarios(i).combined, 'fd_view_summary')
                scenarios(i).combined.fd_view_summary = strip_phase3_view_summary_for_persistence(scenarios(i).combined.fd_view_summary);
            end
            if isfield(scenarios(i).combined, 'spectral_view_summary')
                scenarios(i).combined.spectral_view_summary = strip_phase3_view_summary_for_persistence(scenarios(i).combined.spectral_view_summary);
            end
        end
    end
end

function summary = strip_phase3_view_summary_for_persistence(summary)
    if ~isstruct(summary)
        return;
    end
    if isfield(summary, 'analysis')
        summary = rmfield(summary, 'analysis');
    end
    if isfield(summary, 'results') && isstruct(summary.results) && isfield(summary.results, 'analysis')
        summary.results.analysis = summarize_phase3_analysis(summary.results.analysis);
    end
end

function write_phase3_report(Results, paths)
    report_path = fullfile(paths.reports, 'Phase3_Bathymetry_Study_Report.md');
    fid = fopen(report_path, 'w');
    if fid < 0
        error('Phase3BathymetryStudy:ReportWriteFailed', 'Could not write report: %s', report_path);
    end
    cleaner = onCleanup(@() fclose(fid));
    fprintf(fid, '# Phase 3 Bathymetry Study\n\n');
    fprintf(fid, '- Phase ID: `%s`\n', Results.phase_id);
    fprintf(fid, '- Workflow: `%s`\n', Results.workflow_kind);
    fprintf(fid, '- Spectral policy: active lifted walls + immersed-mask 2D bathymetry\n\n');
    fprintf(fid, '| Scenario | BC | Bathymetry | FD runtime (s) | FD mesh | Spectral runtime (s) | Spectral mesh |\n');
    fprintf(fid, '| --- | --- | --- | ---: | --- | ---: | --- |\n');
    for i = 1:numel(Results.scenarios)
        scen = Results.scenarios(i);
        fprintf(fid, '| %s | %s | %s | %.3f | %s | %.3f | %s |\n', ...
            scen.scenario_label, ...
            pick_phase3_text(scen.summary, {'bc_label', 'bc_id'}, '--'), ...
            pick_phase3_text(scen.summary, {'bathymetry_label', 'bathymetry_id'}, '--'), ...
            double(pick_phase3_value(scen.summary, 'runtime_wall_s', NaN)), ...
            sprintf('%dx%d', round(double(pick_phase3_value(scen.summary, 'mesh_nx', NaN))), ...
                round(double(pick_phase3_value(scen.summary, 'mesh_ny', NaN)))), ...
            double(pick_phase3_value(scen.summary, 'spectral_runtime_wall_s', NaN)), ...
            sprintf('%dx%d', round(double(pick_phase3_value(scen.summary, 'spectral_mesh_nx', NaN))), ...
                round(double(pick_phase3_value(scen.summary, 'spectral_mesh_ny', NaN)))));
    end
    clear cleaner
end

function token = phase3_method_to_parameter_token(method)
    switch normalize_phase3_method_key(method)
        case 'fd'
            token = 'finite_difference';
        case 'spectral'
            token = 'spectral';
        otherwise
            token = lower(char(string(method)));
    end
end

function key = normalize_phase3_method_key(method)
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

function integrator = resolve_phase3_method_integrator(method)
    switch normalize_phase3_method_key(method)
        case {'fd', 'spectral'}
            integrator = 'RK4';
        otherwise
            integrator = 'RK4';
    end
end

function job = make_phase3_job(label, method, stage, job_key, queue_index, run_config, parameters, settings, phase_paths, scenario)
    job = empty_phase3_job();
    job.label = label;
    job.method = method;
    job.method_key = normalize_phase3_method_key(method);
    job.stage = stage;
    job.job_key = job_key;
    job.queue_index = queue_index;
    job.output_root = fullfile(phase_paths.runs_root, compact_phase3_job_dir_name(queue_index, scenario.id, method));
    job.run_config = run_config;
    job.parameters = parameters;
    job.settings = settings;
    job.settings.output_root = job.output_root;
    job.settings.preinitialized_artifact_root = true;
    job.scenario = scenario;
end

function storage_id = make_phase3_storage_id(phase_id)
    raw = lower(regexprep(char(string(phase_id)), '[^a-z0-9]+', '_'));
    stamp = compact_phase3_stamp_token(raw);
    label = regexprep(raw, '_?\d{8}_\d{6}$', '');
    label = regexprep(label, '^phase\d*_?', '');
    ic_token = compact_phase3_label_token(label);
    storage_id = sprintf('p3_%s_%s', stamp, ic_token);
end

function child_id = make_phase3_child_identifier(phase_id, queue_index, scenario_id, method_name)
    raw = lower(regexprep(char(string(phase_id)), '[^a-z0-9]+', '_'));
    stamp = compact_phase3_stamp_token(raw);
    child_id = sprintf('p3%s%02d%s%s', stamp, round(double(queue_index)), ...
        compact_phase3_label_token(method_name), compact_phase3_label_token(scenario_id));
end

function dir_name = compact_phase3_job_dir_name(queue_index, scenario_id, method_name)
    dir_name = sprintf('%02d_%s_%s', round(double(queue_index)), ...
        compact_phase3_label_token(method_name), compact_phase3_label_token(scenario_id));
end

function stamp = compact_phase3_stamp_token(raw_phase_id)
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

function token = compact_phase3_label_token(label_raw)
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

function job = empty_phase3_job()
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

function output = make_phase3_output(job, results, paths, execution_mode)
    output = empty_phase3_output();
    output.label = job.label;
    output.method = job.method;
    output.method_key = job.method_key;
    output.stage = job.stage;
    output.job_key = job.job_key;
    output.queue_index = job.queue_index;
    output.run_config = job.run_config;
    output.parameters = job.parameters;
    output.results = results;
    output.paths = paths;
    output.wall_time = pick_phase3_numeric(results, {'wall_time', 'total_time'}, NaN);
    output.execution_mode = execution_mode;
    output.status = 'completed';
    output.scenario = job.scenario;
end

function output = make_phase3_failed_output(job, ME)
    output = empty_phase3_output();
    output.label = job.label;
    output.method = job.method;
    output.method_key = job.method_key;
    output.stage = job.stage;
    output.job_key = job.job_key;
    output.queue_index = job.queue_index;
    output.run_config = job.run_config;
    output.parameters = job.parameters;
    output.results = struct('error_identifier', ME.identifier, 'error_message', ME.message);
    output.paths = struct('base', job.output_root);
    output.wall_time = NaN;
    output.execution_mode = 'dispatcher_queue';
    output.status = 'failed';
    output.scenario = job.scenario;
end

function output = empty_phase3_output()
    output = struct( ...
        'label', '', ...
        'method', '', ...
        'method_key', '', ...
        'stage', '', ...
        'job_key', '', ...
        'queue_index', NaN, ...
        'run_config', struct(), ...
        'parameters', struct(), ...
        'results', struct(), ...
        'paths', struct(), ...
        'wall_time', NaN, ...
        'execution_mode', '', ...
        'status', '', ...
        'scenario', struct());
end

function jobs = queue_outputs_to_phase3_jobs(outputs)
    jobs = repmat(empty_phase3_job(), 1, numel(outputs));
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

function write_phase3_json(path, payload)
    fid = fopen(path, 'w');
    if fid < 0
        error('Phase3BathymetryStudy:JsonWriteFailed', 'Could not write JSON file: %s', path);
    end
    cleaner = onCleanup(@() fclose(fid));
    fprintf(fid, '%s', jsonencode(payload));
    clear cleaner
end

function val = pick_phase3_value(s, field, default)
    if isstruct(s) && isfield(s, field) && ~isempty(s.(field))
        val = s.(field);
    else
        val = default;
    end
end

function txt = pick_phase3_text(s, fields, default)
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

function val = pick_phase3_numeric(s, fields, default)
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

function merged = merge_structs(base, override)
    merged = base;
    keys = fieldnames(override);
    for i = 1:numel(keys)
        base_value = [];
        if isfield(merged, keys{i})
            base_value = merged.(keys{i});
        end
        override_value = override.(keys{i});
        if isfield(merged, keys{i}) && isstruct(base_value) && isscalar(base_value) && ...
                isstruct(override_value) && isscalar(override_value)
            merged.(keys{i}) = merge_structs(merged.(keys{i}), override.(keys{i}));
        else
            merged.(keys{i}) = override_value;
        end
    end
end
