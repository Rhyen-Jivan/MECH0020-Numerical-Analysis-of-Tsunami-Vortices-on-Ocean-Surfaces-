classdef PhaseTelemetryCSVFirst
    % PhaseTelemetryCSVFirst - Phase-scoped HWiNFO Pro CSV-first orchestration.
    %
    % Runtime role:
    %   - Reserve phase-root telemetry artifacts before the queue starts
    %   - Launch one HWiNFO Pro CSV session for the whole phase workflow
    %   - Record explicit stage/substage boundary timestamps during runtime
    %   - Decorate phase monitor payloads so workbook/export code can use the
    %     phase-root raw CSV instead of live shared-memory samples

    methods (Static)
        function settings = configure_phase_runtime(settings, paths, phase_id, workflow_kind)
            if nargin < 1 || ~isstruct(settings)
                settings = struct();
            end
            if ~isfield(settings, 'sustainability') || ~isstruct(settings.sustainability)
                settings.sustainability = struct();
            end
            if ~isfield(settings.sustainability, 'collector_runtime') || ...
                    ~isstruct(settings.sustainability.collector_runtime)
                settings.sustainability.collector_runtime = struct();
            end

            runtime = settings.sustainability.collector_runtime;
            runtime.hwinfo_control_mode = PhaseTelemetryCSVFirst.pick_text(runtime, ...
                {'hwinfo_control_mode'}, 'pro_cli_csv');
            runtime.hwinfo_pro_cli_enabled = PhaseTelemetryCSVFirst.pick_logical(runtime, ...
                {'hwinfo_pro_cli_enabled'}, true);
            runtime.hwinfo_pro_log_write_direct = PhaseTelemetryCSVFirst.pick_logical(runtime, ...
                {'hwinfo_pro_log_write_direct'}, true);
            runtime.hwinfo_pro_poll_rate_ms = PhaseTelemetryCSVFirst.resolve_poll_rate_ms(settings, runtime);
            runtime.hwinfo_pro_launch_timeout_s = PhaseTelemetryCSVFirst.pick_numeric(runtime, ...
                {'hwinfo_pro_launch_timeout_s'}, 20);
            runtime.hwinfo_pro_csv_timeout_s = PhaseTelemetryCSVFirst.pick_numeric(runtime, ...
                {'hwinfo_pro_csv_timeout_s'}, 45);
            runtime.hwinfo_pro_force_stop_fallback = PhaseTelemetryCSVFirst.pick_logical(runtime, ...
                {'hwinfo_pro_force_stop_fallback'}, true);
            runtime.hwinfo_pro_csv_path = char(string(paths.raw_hwinfo_csv_path));
            runtime.hwinfo_csv_target_dir = char(string(paths.metrics_root));
            runtime.hwinfo_csv_target_path = char(string(paths.raw_hwinfo_csv_path));
            runtime.hwinfo_transport_mode = 'csv';
            runtime.collector_launch_root_dir = char(string(paths.metrics_root));
            runtime.collector_launch_csv_file = 'HWiNFO_Telemetry.csv';
            runtime.hwinfo_phase_id = char(string(phase_id));
            runtime.hwinfo_workflow_kind = char(string(workflow_kind));
            settings.sustainability.collector_runtime = runtime;
        end

        function tf = phase_csv_mode_enabled(settings, workflow_kind)
            tf = false;
            if nargin < 1 || ~isstruct(settings)
                return;
            end
            if nargin >= 2 && ~PhaseTelemetryCSVFirst.is_phase_workflow(workflow_kind)
                return;
            end
            external_cfg = PhaseTelemetryCSVFirst.pick_struct( ...
                PhaseTelemetryCSVFirst.pick_struct(settings, {'sustainability'}, struct()), ...
                {'external_collectors'}, struct());
            if ~PhaseTelemetryCSVFirst.pick_logical(external_cfg, {'hwinfo'}, false)
                return;
            end
            runtime = PhaseTelemetryCSVFirst.pick_struct( ...
                PhaseTelemetryCSVFirst.pick_struct(settings, {'sustainability'}, struct()), ...
                {'collector_runtime'}, struct());
            mode_value = PhaseTelemetryCSVFirst.pick_text(runtime, {'hwinfo_control_mode'}, 'pro_cli_csv');
            tf = strcmpi(mode_value, 'pro_cli_csv') && ...
                PhaseTelemetryCSVFirst.pick_logical(runtime, {'hwinfo_pro_cli_enabled'}, true);
        end

        function context = start_phase_session(settings, paths, phase_id, workflow_kind)
            context = PhaseTelemetryCSVFirst.empty_context(paths, phase_id, workflow_kind);
            context.host_timezone = char(string(datetime('now', 'TimeZone', 'local').TimeZone));
            context.save_json = PhaseTelemetryCSVFirst.pick_logical(settings, {'save_json'}, false);
            if ~context.save_json
                context.session_json_path = fullfile(tempdir, sprintf('%s_hwinfo_pro_session.json', ...
                    PhaseTelemetryCSVFirst.sanitize_token(phase_id)));
            end

            runtime = PhaseTelemetryCSVFirst.pick_struct( ...
                PhaseTelemetryCSVFirst.pick_struct(settings, {'sustainability'}, struct()), ...
                {'collector_runtime'}, struct());
            context.control_mode = PhaseTelemetryCSVFirst.pick_text(runtime, {'hwinfo_control_mode'}, 'pro_cli_csv');
            context.telemetry_requested = PhaseTelemetryCSVFirst.phase_csv_mode_enabled(settings, workflow_kind);
            context.enabled = context.telemetry_requested;
            context.poll_rate_ms = PhaseTelemetryCSVFirst.resolve_poll_rate_ms(settings, runtime);
            context.paths = paths;
            context.runtime = runtime;
            context.executable_path = PhaseTelemetryCSVFirst.resolve_hwinfo_path(settings);
            context.telemetry_disable_reason = PhaseTelemetryCSVFirst.resolve_initial_disable_reason(context);
            if ~isempty(context.telemetry_disable_reason)
                context.enabled = false;
            end

            context.preflight_status = PhaseTelemetryCSVFirst.prepare_artifact_roots(context);
            PhaseTelemetryCSVFirst.write_launch_manifest(context, 'prepared', struct());
            PhaseTelemetryCSVFirst.emit_preflight_summary(context);

            if ~context.enabled
                if context.telemetry_requested
                    disabled_payload = struct( ...
                        'status', 'hwinfo_not_found', ...
                        'message', 'HWiNFO executable was not found. Phase telemetry disabled for this run.');
                    context.session_response = disabled_payload;
                    PhaseTelemetryCSVFirst.write_launch_manifest(context, 'disabled', disabled_payload);
                end
                return;
            end

            controller_config = struct( ...
                'exe_path', context.executable_path, ...
                'csv_path', context.raw_csv_path, ...
                'session_json_path', context.session_json_path, ...
                'batch_script_path', context.batch_script_path, ...
                'poll_rate_ms', context.poll_rate_ms, ...
                'launch_timeout_s', PhaseTelemetryCSVFirst.pick_numeric(runtime, {'hwinfo_pro_launch_timeout_s'}, 20), ...
                'csv_timeout_s', PhaseTelemetryCSVFirst.pick_numeric(runtime, {'hwinfo_pro_csv_timeout_s'}, 45), ...
                'force_stop_fallback', PhaseTelemetryCSVFirst.pick_logical(runtime, {'hwinfo_pro_force_stop_fallback'}, true), ...
                'write_direct', PhaseTelemetryCSVFirst.pick_logical(runtime, {'hwinfo_pro_log_write_direct'}, true), ...
                'timezone_name', context.host_timezone);
            response = HWiNFOProCLIController.start_session(phase_id, controller_config);
            context.session_response = response;
            PhaseTelemetryCSVFirst.write_launch_manifest(context, 'started', response);
            if ~(isstruct(response) && isfield(response, 'ok') && response.ok)
                if PhaseTelemetryCSVFirst.should_disable_for_missing_executable(response) || ...
                        PhaseTelemetryCSVFirst.should_disable_for_existing_instance(response)
                    context.enabled = false;
                    if PhaseTelemetryCSVFirst.should_disable_for_existing_instance(response)
                        context.telemetry_disable_reason = 'hwinfo_already_running';
                    else
                        context.telemetry_disable_reason = 'hwinfo_not_found';
                    end
                    PhaseTelemetryCSVFirst.write_launch_manifest(context, 'disabled', response);
                    SafeConsoleIO.fprintf('Phase telemetry disabled | mode=%s | reason=%s\n', ...
                        context.control_mode, context.telemetry_disable_reason);
                    return;
                end
                error('PhaseTelemetryCSVFirst:StartFailed', ...
                    'HWiNFO Pro CSV-first preflight failed: %s', ...
                    PhaseTelemetryCSVFirst.pick_text(response, {'message', 'status'}, 'unknown failure'));
            end
            context.active = true;
            SafeConsoleIO.fprintf('Phase telemetry session started | mode=%s | raw CSV=%s\n', ...
                context.control_mode, context.raw_csv_path);
        end

        function context = stop_phase_session(context)
            if nargin < 1 || ~isstruct(context) || ~PhaseTelemetryCSVFirst.pick_logical(context, {'active'}, false)
                return;
            end
            response = HWiNFOProCLIController.stop_session(struct( ...
                'session_json_path', context.session_json_path, ...
                'force_stop_fallback', PhaseTelemetryCSVFirst.pick_logical(context.runtime, {'hwinfo_pro_force_stop_fallback'}, true)));
            context.stop_response = response;
            context.active = false;
            PhaseTelemetryCSVFirst.write_launch_manifest(context, 'stopped', response);
            if ~(isstruct(response) && isfield(response, 'ok') && response.ok)
                error('PhaseTelemetryCSVFirst:StopFailed', ...
                    'HWiNFO Pro CSV-first shutdown failed: %s', ...
                    PhaseTelemetryCSVFirst.pick_text(response, {'message', 'status'}, 'unknown failure'));
            end
            SafeConsoleIO.fprintf('Phase telemetry session stopped | raw CSV=%s | rows=%s\n', ...
                context.raw_csv_path, num2str(PhaseTelemetryCSVFirst.pick_numeric(response, {'csv_row_count'}, NaN)));
        end

        function append_boundary(context, boundary_event, entry)
            if nargin < 3 || ~isstruct(context) || isempty(context.boundary_csv_path)
                return;
            end
            if nargin < 2 || isempty(boundary_event)
                boundary_event = 'start';
            end
            if nargin < 3 || ~isstruct(entry)
                entry = struct();
            end

            row = struct( ...
                'session_time_s', PhaseTelemetryCSVFirst.pick_numeric(entry, {'session_time_s'}, NaN), ...
                'timestamp_utc', PhaseTelemetryCSVFirst.pick_text(entry, {'timestamp_utc'}, ...
                    char(datetime('now', 'TimeZone', 'UTC', 'Format', 'yyyy-MM-dd''T''HH:mm:ss''Z'''))), ...
                'boundary_event', char(string(boundary_event)), ...
                'phase_id', PhaseTelemetryCSVFirst.pick_text(entry, {'phase_id'}, context.phase_id), ...
                'workflow_kind', PhaseTelemetryCSVFirst.pick_text(entry, {'workflow_kind'}, context.workflow_kind), ...
                'stage_id', PhaseTelemetryCSVFirst.pick_text(entry, {'stage_id'}, ''), ...
                'stage_label', PhaseTelemetryCSVFirst.pick_text(entry, {'stage_label'}, ''), ...
                'stage_type', PhaseTelemetryCSVFirst.pick_text(entry, {'stage_type'}, ''), ...
                'substage_id', PhaseTelemetryCSVFirst.pick_text(entry, {'substage_id'}, ''), ...
                'substage_label', PhaseTelemetryCSVFirst.pick_text(entry, {'substage_label'}, ''), ...
                'substage_type', PhaseTelemetryCSVFirst.pick_text(entry, {'substage_type'}, ''), ...
                'stage_method', PhaseTelemetryCSVFirst.pick_text(entry, {'stage_method'}, ''), ...
                'scenario_id', PhaseTelemetryCSVFirst.pick_text(entry, {'scenario_id'}, ''), ...
                'mesh_level', PhaseTelemetryCSVFirst.pick_numeric(entry, {'mesh_level'}, NaN), ...
                'mesh_nx', PhaseTelemetryCSVFirst.pick_numeric(entry, {'mesh_nx'}, NaN), ...
                'mesh_ny', PhaseTelemetryCSVFirst.pick_numeric(entry, {'mesh_ny'}, NaN), ...
                'child_run_index', PhaseTelemetryCSVFirst.pick_numeric(entry, {'child_run_index'}, NaN));
            boundary_row = struct2table(row, 'AsArray', true);
            try
                writetable(boundary_row, context.boundary_csv_path, 'WriteMode', 'append');
            catch
                writetable(boundary_row, context.boundary_csv_path);
            end
        end

        function monitor_series = decorate_monitor_series(monitor_series, context)
            if nargin < 1 || ~isstruct(monitor_series)
                monitor_series = struct();
            end
            if nargin < 2 || ~isstruct(context)
                return;
            end
            if ~isfield(monitor_series, 'raw_log_paths') || ~isstruct(monitor_series.raw_log_paths)
                monitor_series.raw_log_paths = struct();
            end
            if context.enabled
                monitor_series.raw_log_paths.hwinfo = char(string(context.raw_csv_path));
            else
                monitor_series.raw_log_paths.hwinfo = '';
            end
            if ~isfield(monitor_series.raw_log_paths, 'icue')
                monitor_series.raw_log_paths.icue = '';
            end
            if ~isfield(monitor_series, 'collector_status') || ~isstruct(monitor_series.collector_status)
                monitor_series.collector_status = struct();
            end
            monitor_series.collector_status.hwinfo = ternary(context.enabled, 'pro_cli_csv', ...
                PhaseTelemetryCSVFirst.disabled_hwinfo_status(context.telemetry_disable_reason));
            if ~isfield(monitor_series.collector_status, 'icue')
                monitor_series.collector_status.icue = 'disabled';
            end
            if ~isfield(monitor_series, 'collector_probe_details') || ~isstruct(monitor_series.collector_probe_details)
                monitor_series.collector_probe_details = struct();
            end
            detail_payload = struct( ...
                'session_json_path', context.session_json_path, ...
                'raw_csv_path', context.raw_csv_path, ...
                'boundary_csv_path', context.boundary_csv_path, ...
                'dataset_csv_path', context.dataset_csv_path, ...
                'manifest_json_path', context.manifest_json_path, ...
                'control_mode', context.control_mode, ...
                'telemetry_enabled', context.enabled, ...
                'telemetry_disable_reason', context.telemetry_disable_reason, ...
                'poll_rate_ms', context.poll_rate_ms, ...
                'host_timezone', context.host_timezone);
            if isfield(context, 'session_response') && isstruct(context.session_response)
                detail_payload.start_response = context.session_response;
            end
            if isfield(context, 'stop_response') && isstruct(context.stop_response)
                detail_payload.stop_response = context.stop_response;
            end
            monitor_series.collector_probe_details.hwinfo = detail_payload;
            monitor_series.hwinfo_transport = ternary(context.enabled, 'csv', 'none');
            monitor_series.hwinfo_status_reason = ternary(context.enabled, ...
                'Phase workflow used HWiNFO Pro CSV-first logging.', ...
                PhaseTelemetryCSVFirst.telemetry_disabled_text(context.telemetry_disable_reason));
            monitor_series.hwinfo_control_mode = context.control_mode;
            monitor_series.telemetry_enabled = logical(context.enabled);
            monitor_series.telemetry_disable_reason = char(string(context.telemetry_disable_reason));
            monitor_series.host_timezone = context.host_timezone;
        end

        function context = empty_context(paths, phase_id, workflow_kind)
            metrics_root = PhaseTelemetryCSVFirst.pick_text(paths, {'metrics_root'}, '');
            context = struct( ...
                'telemetry_requested', false, ...
                'enabled', false, ...
                'active', false, ...
                'telemetry_disable_reason', '', ...
                'control_mode', 'pro_cli_csv', ...
                'phase_id', char(string(phase_id)), ...
                'workflow_kind', char(string(workflow_kind)), ...
                'host_timezone', '', ...
                'poll_rate_ms', 1000, ...
                'executable_path', '', ...
                'paths', paths, ...
                'runtime', struct(), ...
                'raw_csv_path', PhaseTelemetryCSVFirst.pick_text(paths, {'raw_hwinfo_csv_path'}, ...
                    fullfile(metrics_root, 'HWiNFO_Telemetry.csv')), ...
                'boundary_csv_path', fullfile(metrics_root, 'Telemetry_Stage_Boundaries.csv'), ...
                'dataset_csv_path', fullfile(metrics_root, 'Telemetry_Raw.csv'), ...
                'stage_summary_csv_path', fullfile(metrics_root, 'Telemetry_Stage_Summary.csv'), ...
                'workbook_path', PhaseTelemetryCSVFirst.pick_text(paths, {'run_data_workbook_path'}, ...
                    fullfile(metrics_root, 'Run_Data.xlsx')), ...
                'manifest_json_path', fullfile(metrics_root, 'Telemetry_Launch_Manifest.json'), ...
                'session_json_path', fullfile(metrics_root, 'HWiNFO_Pro_Session.json'), ...
                'save_json', false, ...
                'batch_script_path', fullfile(metrics_root, 'hwinfo_pro_launch.cmd'), ...
                'preflight_status', struct([]), ...
                'session_response', struct(), ...
                'stop_response', struct());
        end
    end

    methods (Static, Access = private)
        function status_rows = prepare_artifact_roots(context)
            status_rows = repmat(struct('path', '', 'status', ''), 1, 0);
            dir_candidates = { ...
                PhaseTelemetryCSVFirst.pick_text(context.paths, {'base'}, ''), ...
                PhaseTelemetryCSVFirst.pick_text(context.paths, {'data'}, ''), ...
                PhaseTelemetryCSVFirst.pick_text(context.paths, {'metrics_root'}, ''), ...
                PhaseTelemetryCSVFirst.pick_text(context.paths, {'visuals_root'}, '')};
            for i = 1:numel(dir_candidates)
                target_dir = char(string(dir_candidates{i}));
                if isempty(target_dir)
                    continue;
                end
                existed = exist(target_dir, 'dir') == 7;
                if exist(target_dir, 'dir') ~= 7
                    mkdir(target_dir);
                end
                status_rows(end + 1) = struct('path', target_dir, 'status', ternary(existed, 'existing', 'created')); %#ok<AGROW>
            end
            boundary_existed = exist(context.boundary_csv_path, 'file') == 2;
            if exist(context.boundary_csv_path, 'file') ~= 2
                writetable(PhaseTelemetryCSVFirst.empty_boundary_table(), context.boundary_csv_path);
            end
            status_rows(end + 1) = struct('path', context.boundary_csv_path, ...
                'status', ternary(boundary_existed, 'existing', 'created')); %#ok<AGROW>
        end

        function write_launch_manifest(context, lifecycle_state, payload)
            if ~PhaseTelemetryCSVFirst.pick_logical(context, {'save_json'}, false)
                return;
            end
            manifest = struct( ...
                'generated_at_utc', char(datetime('now', 'TimeZone', 'UTC', ...
                    'Format', 'yyyy-MM-dd''T''HH:mm:ss''Z''')), ...
                'phase_id', context.phase_id, ...
                'workflow_kind', context.workflow_kind, ...
                'hwinfo_control_mode', context.control_mode, ...
                'raw_csv_path', context.raw_csv_path, ...
                'boundary_csv_path', context.boundary_csv_path, ...
                'dataset_csv_path', context.dataset_csv_path, ...
                'stage_summary_csv_path', context.stage_summary_csv_path, ...
                'run_data_workbook_path', context.workbook_path, ...
                'session_json_path', context.session_json_path, ...
                'batch_script_path', context.batch_script_path, ...
                'poll_rate_ms', context.poll_rate_ms, ...
                'host_timezone', context.host_timezone, ...
                'hwinfo_executable_path', context.executable_path, ...
                'telemetry_enabled', logical(context.enabled), ...
                'telemetry_disable_reason', char(string(context.telemetry_disable_reason)), ...
                'lifecycle_state', char(string(lifecycle_state)), ...
                'payload', payload);
            fid = fopen(context.manifest_json_path, 'w');
            if fid == -1
                warning('PhaseTelemetryCSVFirst:ManifestWriteFailed', ...
                    'Could not write phase telemetry manifest: %s', context.manifest_json_path);
                return;
            end
            cleanup_obj = onCleanup(@() fclose(fid)); %#ok<NASGU>
            fprintf(fid, '%s', jsonencode(manifest));
        end

        function token = sanitize_token(value)
            token = regexprep(char(string(value)), '[^A-Za-z0-9_\-]+', '_');
            if isempty(token)
                token = char(datetime('now', 'Format', 'yyyyMMdd_HHmmss'));
            end
        end

        function emit_preflight_summary(context)
            summary_parts = strings(1, 0);
            status_rows = context.preflight_status;
            for i = 1:numel(status_rows)
                label = char(string(status_rows(i).path));
                if isempty(label)
                    continue;
                end
                summary_parts(end + 1) = string(sprintf('%s=%s', ...
                    char(string(status_rows(i).status)), label)); %#ok<AGROW>
            end
            mode_text = sprintf('enabled:%s', context.control_mode);
            if ~context.enabled
                mode_text = sprintf('disabled:%s', PhaseTelemetryCSVFirst.if_empty(context.telemetry_disable_reason, 'disabled'));
            end
            SafeConsoleIO.fprintf('Phase telemetry preflight | mode=%s | csv=%s | %s\n', ...
                mode_text, context.raw_csv_path, char(strjoin(summary_parts, ' | ')));
        end

        function tf = is_phase_workflow(workflow_kind)
            token = lower(strtrim(char(string(workflow_kind))));
            tf = any(strcmp(token, { ...
                'phase1_periodic_comparison', ...
                'mesh_convergence_study', ...
                'phase2_boundary_condition_study', ...
                'phase3_bathymetry_study'}));
        end

        function path_value = resolve_hwinfo_path(settings)
            path_value = '';
            collector_paths = PhaseTelemetryCSVFirst.pick_struct( ...
                PhaseTelemetryCSVFirst.pick_struct(settings, {'sustainability'}, struct()), ...
                {'collector_paths'}, struct());
            path_value = PhaseTelemetryCSVFirst.pick_text(collector_paths, {'hwinfo'}, '');
            if isempty(path_value)
                default_candidates = ExternalCollectorAdapters.default_paths('hwinfo');
                for i = 1:numel(default_candidates)
                    if exist(default_candidates{i}, 'file') == 2
                        path_value = char(string(default_candidates{i}));
                        return;
                    end
                end
            end
        end

        function reason = resolve_initial_disable_reason(context)
            reason = '';
            if ~PhaseTelemetryCSVFirst.pick_logical(context, {'telemetry_requested'}, false)
                reason = 'disabled_by_settings';
                return;
            end
            exe_path = strtrim(char(string(PhaseTelemetryCSVFirst.pick_text(context, {'executable_path'}, ''))));
            if isempty(exe_path) || exist(exe_path, 'file') ~= 2
                reason = 'hwinfo_not_found';
            end
        end

        function tf = should_disable_for_missing_executable(response)
            tf = false;
            if ~(isstruct(response) && isfield(response, 'status'))
                return;
            end
            status_text = lower(strtrim(char(string(response.status))));
            tf = any(strcmp(status_text, {'hwinfo_executable_missing', 'hwinfo_not_found', 'not_found'}));
        end

        function tf = should_disable_for_existing_instance(response)
            tf = false;
            if ~isstruct(response)
                return;
            end
            status_text = '';
            message_text = '';
            if isfield(response, 'status')
                status_text = lower(strtrim(char(string(response.status))));
            end
            if isfield(response, 'message')
                message_text = lower(strtrim(char(string(response.message))));
            end
            tf = any(strcmp(status_text, {'hwinfo_already_running', 'already_running'})) || ...
                contains(message_text, 'already running');
        end

        function text = telemetry_disabled_text(reason)
            token = lower(strtrim(char(string(reason))));
            switch token
                case 'hwinfo_not_found'
                    text = 'HWiNFO executable not found; phase telemetry disabled.';
                case 'hwinfo_already_running'
                    text = 'HWiNFO is already running; dedicated phase CSV telemetry disabled.';
                case 'disabled_by_settings'
                    text = 'Phase HWiNFO telemetry disabled by settings.';
                otherwise
                    text = 'Phase HWiNFO telemetry disabled.';
            end
        end

        function status_text = disabled_hwinfo_status(reason)
            token = lower(strtrim(char(string(reason))));
            switch token
                case 'hwinfo_not_found'
                    status_text = 'not_found';
                case 'hwinfo_already_running'
                    status_text = 'already_running';
                otherwise
                    status_text = 'disabled';
            end
        end

        function out = if_empty(text_value, fallback)
            out = char(string(text_value));
            if isempty(strtrim(out))
                out = char(string(fallback));
            end
        end

        function poll_rate_ms = resolve_poll_rate_ms(settings, runtime)
            poll_rate_ms = PhaseTelemetryCSVFirst.pick_numeric(runtime, {'hwinfo_pro_poll_rate_ms'}, NaN);
            if isfinite(poll_rate_ms) && poll_rate_ms > 0
                poll_rate_ms = max(100, round(double(poll_rate_ms)));
                return;
            end
            sample_interval = PhaseTelemetryCSVFirst.pick_numeric( ...
                PhaseTelemetryCSVFirst.pick_struct(settings, {'sustainability'}, struct()), ...
                {'sample_interval'}, NaN);
            if ~(isfinite(sample_interval) && sample_interval > 0)
                sample_interval = PhaseTelemetryCSVFirst.pick_numeric(settings, {'sample_interval'}, 1.0);
            end
            poll_rate_ms = max(100, round(double(sample_interval) * 1000));
        end

        function table_out = empty_boundary_table()
            template = struct( ...
                'session_time_s', NaN, ...
                'timestamp_utc', "", ...
                'boundary_event', "", ...
                'phase_id', "", ...
                'workflow_kind', "", ...
                'stage_id', "", ...
                'stage_label', "", ...
                'stage_type', "", ...
                'substage_id', "", ...
                'substage_label', "", ...
                'substage_type', "", ...
                'stage_method', "", ...
                'scenario_id', "", ...
                'mesh_level', NaN, ...
                'mesh_nx', NaN, ...
                'mesh_ny', NaN, ...
                'child_run_index', NaN);
            table_out = struct2table(template, 'AsArray', true);
            table_out(1, :) = [];
        end

        function value = pick_struct(s, keys, fallback)
            value = fallback;
            if nargin < 3
                fallback = struct();
                value = fallback;
            end
            if ~isstruct(s)
                return;
            end
            for i = 1:numel(keys)
                key = keys{i};
                if isfield(s, key) && isstruct(s.(key))
                    value = s.(key);
                    return;
                end
            end
        end

        function value = pick_text(s, keys, fallback)
            value = fallback;
            if ~isstruct(s)
                return;
            end
            for i = 1:numel(keys)
                key = keys{i};
                if isfield(s, key)
                    value = char(string(s.(key)));
                    return;
                end
            end
        end

        function value = pick_numeric(s, keys, fallback)
            value = fallback;
            if ~isstruct(s)
                return;
            end
            for i = 1:numel(keys)
                key = keys{i};
                if isfield(s, key)
                    candidate = double(s.(key));
                    if isscalar(candidate) && isfinite(candidate)
                        value = candidate;
                        return;
                    end
                end
            end
        end

        function value = pick_logical(s, keys, fallback)
            value = fallback;
            if ~isstruct(s)
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
    end
end

function value = ternary(condition, true_value, false_value)
    if condition
        value = true_value;
    else
        value = false_value;
    end
end
