classdef RunArtifactsManager
    % RunArtifactsManager - Centralized post-run artifact finalization.
    %
    % Responsibilities:
    %   - Ensure canonical compact run directories exist.
    %   - Write Run_Settings.txt at the run root.
    %   - Write artifact_manifest.json in Data/.
    %   - Build report_payload.json and HTML/PDF reports in Metrics/.
    %   - Append one row to global sustainability ledger.

    methods (Static)
        function artifact_summary = finalize_minimal(Run_Config, Parameters, Settings, Results, paths)
            paths = RunArtifactsManager.ensure_common_paths(paths);
            run_id = RunArtifactsManager.resolve_run_id(Run_Config, Results);

            run_settings_path = RunArtifactsManager.safe_field(paths, 'run_settings_path', '');
            if isempty(run_settings_path)
                run_settings_path = fullfile(paths.base, 'Run_Settings.txt');
            end
            write_run_settings_text(run_settings_path, ...
                'Run Config', RunArtifactsManager.compact_struct(Run_Config), ...
                'Parameters', RunArtifactsManager.compact_struct(Parameters), ...
                'Settings', RunArtifactsManager.compact_struct(Settings), ...
                'Results', RunArtifactsManager.compact_struct(Results));

            manifest = RunArtifactsManager.build_manifest(run_id, Run_Config, Parameters, Settings, Results, paths);
            manifest.finalization_scope = 'minimal';
            manifest.deferred_rich_artifacts = true;
            manifest_path = RunArtifactsManager.safe_field(paths, 'manifest_path', '');
            if isempty(manifest_path)
                manifest_path = fullfile(paths.config, 'artifact_manifest.json');
            end
            if RunArtifactsManager.json_enabled(Settings)
                RunArtifactsManager.write_json(manifest_path, manifest);
            else
                manifest_path = '';
            end

            payload = RunArtifactsManager.build_report_payload(run_id, Run_Config, Parameters, Results, paths);
            payload_path = '';
            if RunArtifactsManager.json_enabled(Settings)
                payload_path = fullfile(paths.reports, 'report_payload.json');
                if ~exist(paths.reports, 'dir')
                    mkdir(paths.reports);
                end
                RunArtifactsManager.write_json(payload_path, payload);
            end

            artifact_summary = struct();
            artifact_summary.run_id = run_id;
            artifact_summary.run_settings_path = run_settings_path;
            artifact_summary.manifest_path = manifest_path;
            artifact_summary.report_artifacts = struct( ...
                'payload_path', payload_path, ...
                'qmd_path', '', ...
                'html_path', '', ...
                'pdf_path', '', ...
                'engine', 'deferred');
            artifact_summary.sustainability_ledger_path = '';
            artifact_summary.sustainability_row = struct();
            artifact_summary.collector_artifacts = struct();
            artifact_summary.finalization_scope = 'minimal';
            artifact_summary.deferred_rich_artifacts = true;
        end

        function artifact_summary = finalize(Run_Config, Parameters, Settings, Results, paths)
            paths = RunArtifactsManager.ensure_common_paths(paths);
            run_id = RunArtifactsManager.resolve_run_id(Run_Config, Results);

            run_settings_path = RunArtifactsManager.safe_field(paths, 'run_settings_path', '');
            if isempty(run_settings_path)
                run_settings_path = fullfile(paths.base, 'Run_Settings.txt');
            end
            write_run_settings_text(run_settings_path, ...
                'Run Config', RunArtifactsManager.compact_struct(Run_Config), ...
                'Parameters', RunArtifactsManager.compact_struct(Parameters), ...
                'Settings', RunArtifactsManager.compact_struct(Settings), ...
                'Results', RunArtifactsManager.compact_struct(Results));

            manifest = RunArtifactsManager.build_manifest(run_id, Run_Config, Parameters, Settings, Results, paths);
            manifest_path = RunArtifactsManager.safe_field(paths, 'manifest_path', '');
            if isempty(manifest_path)
                manifest_path = fullfile(paths.config, 'artifact_manifest.json');
            end
            if RunArtifactsManager.json_enabled(Settings)
                RunArtifactsManager.write_json(manifest_path, manifest);
            else
                manifest_path = '';
            end

            payload = RunArtifactsManager.build_report_payload(run_id, Run_Config, Parameters, Results, paths);
            reporting_enabled = RunArtifactsManager.reporting_enabled(Settings);
            report_artifacts = struct();
            if reporting_enabled
                report_artifacts = RunReportPipeline.generate(payload, paths, Settings);
            else
                payload_path = '';
                if RunArtifactsManager.json_enabled(Settings)
                    payload_path = fullfile(paths.reports, 'report_payload.json');
                    if ~exist(paths.reports, 'dir')
                        mkdir(paths.reports);
                    end
                    RunArtifactsManager.write_json(payload_path, payload);
                end
                report_artifacts.payload_path = payload_path;
                report_artifacts.engine = 'disabled';
                report_artifacts.html_path = '';
                report_artifacts.pdf_path = '';
            end

            [ledger_path, ledger_row] = SustainabilityLedger.append_run(Run_Config, Parameters, Settings, Results, paths);

            collector_artifacts = struct();
            if exist('ExternalCollectorDispatcher', 'class') == 8 || exist('ExternalCollectorDispatcher', 'file') == 2
                try
                    summary_context = struct( ...
                        'run_id', run_id, ...
                        'run_config', Run_Config, ...
                        'results', Results, ...
                        'paths', paths, ...
                        'workflow_kind', '', ...
                        'monitor_series', struct());
                    collector_artifacts = ExternalCollectorDispatcher.write_run_artifacts(summary_context);
                catch ME
                    collector_artifacts = struct( ...
                        'phase_workbook_path', '', ...
                        'phase_workbook_status', 'failed', ...
                        'phase_workbook_formatting_status', 'failed', ...
                        'artifact_failures', struct( ...
                            'artifact', 'phase_workbook', ...
                            'identifier', char(string(ME.identifier)), ...
                            'message', char(string(ME.message))));
                end
            end

            artifact_summary = struct();
            artifact_summary.run_id = run_id;
            artifact_summary.run_settings_path = run_settings_path;
            artifact_summary.manifest_path = manifest_path;
            artifact_summary.report_artifacts = report_artifacts;
            artifact_summary.sustainability_ledger_path = ledger_path;
            artifact_summary.sustainability_row = ledger_row;
            artifact_summary.collector_artifacts = collector_artifacts;
            artifact_summary.finalization_scope = 'complete';
            artifact_summary.deferred_rich_artifacts = false;
        end
    end

    methods (Static, Access = private)
        function paths = ensure_common_paths(paths)
            defaults = {'base', 'config', 'reports', 'logs', 'sustainability'};
            for i = 1:numel(defaults)
                key = defaults{i};
                if ~isfield(paths, key) || isempty(paths.(key))
                    switch key
                        case 'base'
                            continue;
                        case 'config'
                            paths.(key) = fullfile(paths.base, 'Data');
                        case 'reports'
                            paths.(key) = fullfile(paths.base, 'Metrics');
                        case 'logs'
                            paths.(key) = fullfile(paths.base, 'Metrics', 'Logs');
                        case 'sustainability'
                            paths.(key) = fullfile(paths.base, 'Metrics');
                    end
                end
                target = paths.(key);
                if ~exist(target, 'dir')
                    mkdir(target);
                end
            end

            root_keys = {'matlab_data_root', 'metrics_root', 'visuals_root'};
            for i = 1:numel(root_keys)
                key = root_keys{i};
                if isfield(paths, key) && ~isempty(paths.(key)) && exist(paths.(key), 'dir') ~= 7
                    mkdir(paths.(key));
                end
            end

            settings_path = RunArtifactsManager.safe_field(paths, 'run_settings_path', '');
            if ~isempty(settings_path)
                settings_dir = fileparts(settings_path);
                if ~isempty(settings_dir) && exist(settings_dir, 'dir') ~= 7
                    mkdir(settings_dir);
                end
            end
        end

        function tf = reporting_enabled(Settings)
            tf = false;
            if isfield(Settings, 'save_reports')
                tf = logical(Settings.save_reports);
            end
            if isfield(Settings, 'reporting') && isfield(Settings.reporting, 'enabled')
                tf = logical(Settings.reporting.enabled);
            end
        end

        function tf = json_enabled(Settings)
            tf = false;
            if isstruct(Settings) && isfield(Settings, 'save_json') && ~isempty(Settings.save_json)
                tf = logical(Settings.save_json);
            end
        end

        function run_id = resolve_run_id(Run_Config, Results)
            if isfield(Results, 'run_id') && ~isempty(Results.run_id)
                run_id = char(string(Results.run_id));
                return;
            end
            if isfield(Run_Config, 'run_id') && ~isempty(Run_Config.run_id)
                run_id = char(string(Run_Config.run_id));
                return;
            end
            if isfield(Run_Config, 'study_id') && ~isempty(Run_Config.study_id)
                run_id = char(string(Run_Config.study_id));
                return;
            end
            run_id = RunIDGenerator.generate(Run_Config, struct());
        end

        function manifest = build_manifest(run_id, Run_Config, Parameters, Settings, Results, paths)
            manifest = struct();
            manifest.schema_version = '1.0';
            manifest.generated_at_utc = char(datetime('now', 'TimeZone', 'UTC', 'Format', 'yyyy-MM-dd''T''HH:mm:ss''Z'''));
            manifest.run_id = run_id;
            manifest.method = RunArtifactsManager.safe_field(Run_Config, 'method', 'unknown');
            manifest.mode = RunArtifactsManager.safe_field(Run_Config, 'mode', 'unknown');
            manifest.ic_type = RunArtifactsManager.safe_field(Run_Config, 'ic_type', '');
            manifest.artifact_layout_version = RunArtifactsManager.safe_field(paths, 'artifact_layout_version', '');
            manifest.run_settings_path = RunArtifactsManager.safe_field(paths, 'run_settings_path', '');
            manifest.paths = RunArtifactsManager.compact_struct(paths);
            manifest.parameters = RunArtifactsManager.compact_struct(Parameters);
            manifest.settings = RunArtifactsManager.compact_struct(Settings);
            manifest.results = RunArtifactsManager.compact_struct(Results);
        end

        function payload = build_report_payload(run_id, Run_Config, Parameters, Results, paths)
            payload = struct();
            payload.title = sprintf('Simulation Report: %s', run_id);

            payload.summary = struct();
            payload.summary.run_id = run_id;
            payload.summary.method = RunArtifactsManager.safe_field(Run_Config, 'method', 'unknown');
            payload.summary.mode = RunArtifactsManager.safe_field(Run_Config, 'mode', 'unknown');
            payload.summary.ic_type = RunArtifactsManager.safe_field(Run_Config, 'ic_type', '');
            payload.summary.generated_at_utc = char(datetime('now', 'TimeZone', 'UTC', 'Format', 'yyyy-MM-dd''T''HH:mm:ss''Z'''));

            payload.configuration = struct();
            payload.configuration.Nx = RunArtifactsManager.safe_number(Parameters, 'Nx', NaN);
            payload.configuration.Ny = RunArtifactsManager.safe_number(Parameters, 'Ny', NaN);
            payload.configuration.dt = RunArtifactsManager.safe_number(Parameters, 'dt', NaN);
            payload.configuration.Tfinal = RunArtifactsManager.safe_number(Parameters, 'Tfinal', NaN);
            payload.configuration.nu = RunArtifactsManager.safe_number(Parameters, 'nu', NaN);
            payload.configuration.output_root = RunArtifactsManager.safe_field(Parameters, 'output_root', 'Results');

            payload.metrics = struct();
            payload.metrics.wall_time_s = RunArtifactsManager.safe_number_multi(Results, {'wall_time', 'total_time', 'wall_time_s'});
            payload.metrics.total_steps = RunArtifactsManager.safe_number(Results, 'total_steps', NaN);
            payload.metrics.max_omega = RunArtifactsManager.safe_number(Results, 'max_omega', NaN);
            payload.metrics.final_energy = RunArtifactsManager.safe_number(Results, 'final_energy', NaN);
            payload.metrics.final_enstrophy = RunArtifactsManager.safe_number(Results, 'final_enstrophy', NaN);
            payload.metrics.status = RunArtifactsManager.safe_field(Results, 'status', 'completed');

            payload.paths = struct();
            payload.paths.base = RunArtifactsManager.safe_field(paths, 'base', '');
            payload.paths.data = RunArtifactsManager.safe_field(paths, 'data', '');
            payload.paths.figures_root = RunArtifactsManager.safe_field(paths, 'figures_root', '');
            payload.paths.media = RunArtifactsManager.safe_field(paths, 'media', '');
            payload.paths.reports = RunArtifactsManager.safe_field(paths, 'reports', '');
            payload.paths.logs = RunArtifactsManager.safe_field(paths, 'logs', '');
        end

        function out = compact_struct(in_struct)
            out = struct();
            if ~isstruct(in_struct)
                return;
            end

            fields = fieldnames(in_struct);
            for i = 1:numel(fields)
                key = fields{i};
                value = in_struct.(key);
                if isstruct(value)
                    out.(key) = RunArtifactsManager.compact_struct(value);
                elseif isstring(value) || ischar(value) || islogical(value)
                    out.(key) = value;
                elseif isnumeric(value)
                    if isscalar(value)
                        out.(key) = value;
                    elseif numel(value) <= 16
                        out.(key) = value;
                    else
                        out.(key) = sprintf('[numeric %s]', mat2str(size(value)));
                    end
                elseif iscell(value)
                    out.(key) = sprintf('[cell %s]', mat2str(size(value)));
                else
                    out.(key) = sprintf('[%s]', class(value));
                end
            end
        end

        function value = safe_field(s, field_name, default_value)
            value = default_value;
            if isstruct(s) && isfield(s, field_name) && ~isempty(s.(field_name))
                candidate = s.(field_name);
                if isstring(candidate) && any(ismissing(candidate))
                    value = default_value;
                else
                    value = candidate;
                end
            end
        end

        function value = safe_number(s, field_name, default_value)
            value = default_value;
            if isstruct(s) && isfield(s, field_name)
                candidate = s.(field_name);
                if isnumeric(candidate) && isscalar(candidate)
                    value = candidate;
                end
            end
        end

        function value = safe_number_multi(s, fields)
            value = NaN;
            for i = 1:numel(fields)
                value = RunArtifactsManager.safe_number(s, fields{i}, NaN);
                if ~isnan(value)
                    return;
                end
            end
        end

        function write_json(path_str, payload)
            encoded = jsonencode(payload);
            fid = fopen(path_str, 'w');
            if fid == -1
                error('RunArtifactsManager:WriteFailed', 'Could not write JSON: %s', path_str);
            end
            fprintf(fid, '%s', encoded);
            fclose(fid);
        end

        function out = upper_first(in)
            token = char(string(in));
            if isempty(token)
                out = token;
                return;
            end
            out = [upper(token(1)), token(2:end)];
        end
    end
end
