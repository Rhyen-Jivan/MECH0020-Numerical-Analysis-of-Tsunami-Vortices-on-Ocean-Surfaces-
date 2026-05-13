function payload = emit_completion_report_payload(progress_callback, results_for_save, paths, run_config, parameters, options)
%emit_completion_report_payload Emit a publish-safe completion payload.
%
% This helper is used by worker-owned runtimes to tell the host that the
% minimal saved package exists and the Results tab can be populated before
% richer exports finish.

    payload = struct();
    if nargin < 1 || isempty(progress_callback) || ~isa(progress_callback, 'function_handle')
        return;
    end
    if nargin < 2 || ~isstruct(results_for_save)
        return;
    end
    if nargin < 3 || ~isstruct(paths)
        paths = struct();
    end
    if nargin < 4 || ~isstruct(run_config)
        run_config = struct();
    end
    if nargin < 5 || ~isstruct(parameters)
        parameters = struct();
    end
    if nargin < 6 || ~isstruct(options)
        options = struct();
    end

    phase_label = local_pick_text(options, {'phase_label'}, '');
    workflow_kind = local_pick_text(options, {'workflow_kind'}, ...
        local_pick_text(results_for_save, {'workflow_kind'}, ''));
    result_layout_kind = local_pick_text(options, {'result_layout_kind'}, ...
        local_pick_text(results_for_save, {'result_layout_kind'}, ''));
    publication_mode = local_pick_text(options, {'result_publication_mode'}, 'manual');
    completion_results_already_persisted = local_pick_logical(options, {'completion_results_already_persisted'}, false);

    published_run_config = filter_graphics_objects(run_config);
    if strlength(string(strtrim(workflow_kind))) > 0
        published_run_config.workflow_kind = char(string(workflow_kind));
    end
    if strlength(string(strtrim(result_layout_kind))) > 0
        published_run_config.result_layout_kind = char(string(result_layout_kind));
    end
    if strlength(string(strtrim(phase_label))) > 0
        published_run_config.phase_label = char(string(phase_label));
    end
    published_run_config.launch_origin = local_pick_text(published_run_config, {'launch_origin'}, 'launch_button');

    phase_id = local_pick_text(results_for_save, {'phase_id', 'run_id'}, ...
        local_pick_text(published_run_config, {'phase_id', 'run_id'}, ''));
    if strlength(string(strtrim(phase_id))) > 0
        published_run_config.phase_id = phase_id;
    end

    summary = struct( ...
        'mode', local_pick_text(published_run_config, {'mode'}, local_pick_text(results_for_save, {'mode'}, 'Evolution')), ...
        'run_config', published_run_config, ...
        'parameters', filter_graphics_objects(parameters), ...
        'results', filter_graphics_objects(results_for_save), ...
        'paths', filter_graphics_objects(paths), ...
        'wall_time', double(local_pick_numeric(results_for_save, {'wall_time'}, NaN)), ...
        'completion_results_already_persisted', logical(completion_results_already_persisted), ...
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
        'result_publication_mode', char(string(publication_mode)), ...
        'completion_results_already_persisted', logical(completion_results_already_persisted), ...
        'workflow_kind', char(string(workflow_kind)), ...
        'result_layout_kind', char(string(result_layout_kind)), ...
        'phase_label', char(string(phase_label)), ...
        'phase_id', phase_id, ...
        'run_id', local_pick_text(results_for_save, {'run_id'}, phase_id), ...
        'wall_time', summary.wall_time, ...
        'solver_complete', true, ...
        'minimal_results_persisted', true, ...
        'results_published', false, ...
        'exports_complete', false);
    try
        invoke_runtime_progress_callback(progress_callback, payload);
    catch
        payload = struct();
    end
end

function value = local_pick_text(s, keys, fallback)
    value = char(string(fallback));
    if ~(isstruct(s) && ~isempty(keys))
        return;
    end
    for i = 1:numel(keys)
        key = keys{i};
        if isfield(s, key) && ~isempty(s.(key))
            value = char(string(s.(key)));
            return;
        end
    end
end

function value = local_pick_numeric(s, keys, fallback)
    value = fallback;
    if ~(isstruct(s) && ~isempty(keys))
        return;
    end
    for i = 1:numel(keys)
        key = keys{i};
        if isfield(s, key) && isnumeric(s.(key)) && isscalar(s.(key))
            value = double(s.(key));
            return;
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
        if isfield(s, key)
            value = logical(s.(key));
            return;
        end
    end
end
