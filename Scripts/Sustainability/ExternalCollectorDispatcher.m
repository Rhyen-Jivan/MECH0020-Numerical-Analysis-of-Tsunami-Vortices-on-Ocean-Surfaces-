classdef ExternalCollectorDispatcher < handle
    % ExternalCollectorDispatcher - Canonical HWiNFO/iCUE runtime selector.

    properties (Access = private)
        bridge
        settings
        run_id = ''
        is_active = false
        last_sample = struct()
    end

    methods
        function obj = ExternalCollectorDispatcher(settings)
            if nargin < 1 || ~isstruct(settings)
                settings = struct();
            end
            obj.settings = ExternalCollectorDispatcher.normalize_settings(settings);
            obj.last_sample = ExternalCollectorDispatcher.empty_sample();
        end

        function start_live_session(obj, run_id, settings)
            if nargin >= 3 && isstruct(settings)
                obj.settings = ExternalCollectorDispatcher.normalize_settings(settings);
            end
            obj.run_id = char(string(run_id));
            obj.last_sample = ExternalCollectorDispatcher.empty_sample();
            if ~ExternalCollectorDispatcher.any_external_enabled(obj.settings)
                obj.is_active = false;
                return;
            end

            obj.bridge = HardwareMonitorBridge();
            obj.bridge.start_live_session(obj.run_id, obj.settings);
            obj.is_active = true;
        end

        function sample = poll_latest_sample(obj)
            sample = obj.last_sample;
            if ~obj.is_active || isempty(obj.bridge)
                return;
            end
            sample = obj.bridge.poll_latest_sample();
            if ~isstruct(sample) || isempty(fieldnames(sample))
                sample = ExternalCollectorDispatcher.empty_sample();
            end
            sample = ExternalCollectorDispatcher.normalize_collector_payload(sample);
            obj.last_sample = sample;
        end

        function status = get_source_status(obj)
            if obj.is_active && ~isempty(obj.bridge)
                status = obj.bridge.get_source_status();
                return;
            end
            status = ExternalCollectorDispatcher.probe_collectors(obj.settings);
        end

        function summary = stop_live_session(obj)
            summary = struct();
            if obj.is_active && ~isempty(obj.bridge)
                summary = obj.bridge.stop_live_session();
            end
            summary = ExternalCollectorDispatcher.normalize_collector_payload(summary);
            last_sample = ExternalCollectorDispatcher.normalize_collector_payload(obj.last_sample);
            if ExternalCollectorDispatcher.sample_richness_score(last_sample) > ...
                    ExternalCollectorDispatcher.sample_richness_score(summary)
                summary = last_sample;
            elseif ~isempty(fieldnames(summary)) && ~isempty(fieldnames(last_sample))
                summary = ExternalCollectorDispatcher.overlay_sample_fields(summary, last_sample);
            end
            obj.last_sample = summary;
            obj.bridge = [];
            obj.is_active = false;
        end
    end

    methods (Static)
        function probe = probe_collectors(settings)
            settings = ExternalCollectorDispatcher.normalize_settings(settings);
            flags = settings.sustainability.external_collectors;
            paths = settings.sustainability.collector_paths;
            sources = {'hwinfo', 'icue'};

            source_snapshots = struct();
            enabled_count = 0;
            connected_count = 0;
            runtime_cfg = settings.sustainability.collector_runtime;
            for i = 1:numel(sources)
                source = sources{i};
                enabled = isfield(flags, source) && logical(flags.(source));
                snapshot = ExternalCollectorAdapters.extract_snapshot(source, enabled, paths.(source), runtime_cfg);
                source_snapshots.(source) = snapshot;
                enabled_count = enabled_count + double(enabled);
                connected_count = connected_count + double(enabled && snapshot.available);
            end

            probe = struct();
            probe.enabled_external_count = enabled_count;
            probe.connected_external_count = connected_count;
            probe.sources = source_snapshots;
        end

        function settings = normalize_settings(settings)
            if nargin < 1 || ~isstruct(settings)
                settings = struct();
            end
            sustainability_in = struct();
            if isfield(settings, 'sustainability') && isstruct(settings.sustainability)
                sustainability_in = settings.sustainability;
            end

            collectors_in = ExternalCollectorDispatcher.pick_struct_field(sustainability_in, 'external_collectors');
            paths_in = ExternalCollectorDispatcher.pick_struct_field(sustainability_in, 'collector_paths');
            runtime_in = ExternalCollectorDispatcher.pick_struct_field(sustainability_in, 'collector_runtime');

            sample_interval = ExternalCollectorDispatcher.pick_struct_number(sustainability_in, 'sample_interval', NaN);
            if ~isfinite(sample_interval)
                sample_interval = ExternalCollectorDispatcher.pick_struct_number(settings, 'sample_interval', NaN);
            end

            runtime = struct( ...
                'session_output_dir', ExternalCollectorDispatcher.sanitize_text_scalar(runtime_in, 'session_output_dir', ''), ...
                'session_csv_path', ExternalCollectorDispatcher.sanitize_text_scalar(runtime_in, 'session_csv_path', ''), ...
                'collector_launch_root_dir', ExternalCollectorDispatcher.sanitize_text_scalar(runtime_in, 'collector_launch_root_dir', ''), ...
                'collector_launch_csv_file', ExternalCollectorDispatcher.sanitize_text_scalar(runtime_in, 'collector_launch_csv_file', 'launch_telemetry.csv'), ...
                'hwinfo_shared_memory_blob_path', ExternalCollectorDispatcher.sanitize_text_scalar(runtime_in, 'hwinfo_shared_memory_blob_path', ''), ...
                'hwinfo_control_mode', ExternalCollectorDispatcher.sanitize_text_scalar(runtime_in, 'hwinfo_control_mode', 'shared_memory_live'), ...
                'hwinfo_pro_cli_enabled', ExternalCollectorDispatcher.sanitize_logical_scalar(runtime_in, 'hwinfo_pro_cli_enabled', true), ...
                'hwinfo_pro_log_write_direct', ExternalCollectorDispatcher.sanitize_logical_scalar(runtime_in, 'hwinfo_pro_log_write_direct', true), ...
                'hwinfo_pro_poll_rate_ms', ExternalCollectorDispatcher.sanitize_numeric_scalar(runtime_in, 'hwinfo_pro_poll_rate_ms', 1000), ...
                'hwinfo_pro_launch_timeout_s', ExternalCollectorDispatcher.sanitize_numeric_scalar(runtime_in, 'hwinfo_pro_launch_timeout_s', 20), ...
                'hwinfo_pro_csv_timeout_s', ExternalCollectorDispatcher.sanitize_numeric_scalar(runtime_in, 'hwinfo_pro_csv_timeout_s', 45), ...
                'hwinfo_pro_force_stop_fallback', ExternalCollectorDispatcher.sanitize_logical_scalar(runtime_in, 'hwinfo_pro_force_stop_fallback', true), ...
                'hwinfo_pro_csv_path', ExternalCollectorDispatcher.sanitize_text_scalar(runtime_in, 'hwinfo_pro_csv_path', ''), ...
                'hwinfo_transport_mode', ExternalCollectorDispatcher.sanitize_text_scalar(runtime_in, 'hwinfo_transport_mode', 'auto'), ...
                'hwinfo_launch_if_needed', ExternalCollectorDispatcher.sanitize_logical_scalar(runtime_in, 'hwinfo_launch_if_needed', true), ...
                'hwinfo_csv_target_dir', ExternalCollectorDispatcher.sanitize_text_scalar(runtime_in, 'hwinfo_csv_target_dir', ''), ...
                'hwinfo_csv_target_path', ExternalCollectorDispatcher.sanitize_text_scalar(runtime_in, 'hwinfo_csv_target_path', ''), ...
                'collector_panel_profile', ExternalCollectorDispatcher.sanitize_text_scalar(runtime_in, 'collector_panel_profile', ''), ...
                'hwinfo_csv_dir', ExternalCollectorDispatcher.sanitize_text_scalar(runtime_in, 'hwinfo_csv_dir', ''), ...
                'hwinfo_csv_path', ExternalCollectorDispatcher.sanitize_text_scalar(runtime_in, 'hwinfo_csv_path', ''), ...
                'icue_csv_dir', ExternalCollectorDispatcher.sanitize_text_scalar(runtime_in, 'icue_csv_dir', ''), ...
                'icue_csv_path', ExternalCollectorDispatcher.sanitize_text_scalar(runtime_in, 'icue_csv_path', ''), ...
                'icue_user_props_path', ExternalCollectorDispatcher.sanitize_text_scalar(runtime_in, 'icue_user_props_path', ''), ...
                'icue_config_path', ExternalCollectorDispatcher.sanitize_text_scalar(runtime_in, 'icue_config_path', ''), ...
                'icue_devices_path', ExternalCollectorDispatcher.sanitize_text_scalar(runtime_in, 'icue_devices_path', ''));

            sustainability = struct( ...
                'auto_log', ExternalCollectorDispatcher.sanitize_logical_scalar(sustainability_in, 'auto_log', false), ...
                'sample_interval', sample_interval, ...
                'machine_id', ExternalCollectorDispatcher.sanitize_text_scalar(sustainability_in, 'machine_id', ''), ...
                'machine_label', ExternalCollectorDispatcher.sanitize_text_scalar(sustainability_in, 'machine_label', ''), ...
                'collector_panel_profile', ExternalCollectorDispatcher.sanitize_text_scalar(sustainability_in, 'collector_panel_profile', ''), ...
                'environmental_model', ExternalCollectorDispatcher.resolve_environmental_model(sustainability_in), ...
                'external_collectors', struct( ...
                    'hwinfo', ExternalCollectorDispatcher.sanitize_logical_scalar(collectors_in, 'hwinfo', false), ...
                    'icue', ExternalCollectorDispatcher.sanitize_logical_scalar(collectors_in, 'icue', false)), ...
                'collector_paths', struct( ...
                    'hwinfo', ExternalCollectorDispatcher.sanitize_text_scalar(paths_in, 'hwinfo', ''), ...
                    'icue', ExternalCollectorDispatcher.sanitize_text_scalar(paths_in, 'icue', '')), ...
                'collector_runtime', runtime);

            settings = struct('sustainability', sustainability);
        end

        function monitor_series = normalize_collector_payload(monitor_series)
            if nargin < 1 || ~isstruct(monitor_series) || isempty(fieldnames(monitor_series))
                monitor_series = ExternalCollectorDispatcher.empty_sample();
                return;
            end
            if ~isfield(monitor_series, 'collector_status') || ~isstruct(monitor_series.collector_status)
                monitor_series.collector_status = struct('hwinfo', 'disabled', 'icue', 'disabled');
            end
            hwinfo_status = ExternalCollectorDispatcher.pick_struct_text(monitor_series.collector_status, 'hwinfo', '');
            hwinfo_transport = ExternalCollectorDispatcher.pick_text_field(monitor_series, 'hwinfo_transport', 'none');
            requires_integrity = strcmpi(hwinfo_transport, 'shared_memory') && ...
                any(strcmpi(hwinfo_status, {'shared_memory_connected', 'connected'}));
            if ~requires_integrity
                return;
            end

            has_series = ExternalCollectorDispatcher.hwinfo_series_present(monitor_series);
            has_catalog = ExternalCollectorDispatcher.hwinfo_catalog_present(monitor_series);
            if has_series && has_catalog
                return;
            end

            missing_parts = strings(1, 0);
            if ~has_series
                missing_parts(end + 1) = "collector series"; %#ok<AGROW>
            end
            if ~has_catalog
                missing_parts(end + 1) = "metric catalog"; %#ok<AGROW>
            end
            reason_text = sprintf('shared memory connected but HWiNFO %s missing', ...
                char(strjoin(missing_parts, ' and ')));
            monitor_series.collector_status.hwinfo = 'shared_memory_incomplete';
            monitor_series.hwinfo_status_reason = reason_text;
        end

        function table_out = export_phase1_plotting_data_table(summary_context, stage_summary)
            table_out = ExternalCollectorDispatcher.build_phase1_plotting_data_table(summary_context, stage_summary);
        end

        function table_out = export_mesh_convergence_plotting_data_table(summary_context, stage_summary)
            table_out = ExternalCollectorDispatcher.build_mesh_convergence_plotting_data_table(summary_context, stage_summary);
        end

        function score = sample_richness_score(sample)
            score = 0;
            if ~(isstruct(sample) && ~isempty(fieldnames(sample)))
                return;
            end
            status_struct = ExternalCollectorDispatcher.pick_struct_field(sample, 'collector_status');
            hwinfo_status = lower(strtrim(char(string(ExternalCollectorDispatcher.pick_struct_text(status_struct, 'hwinfo', '')))));
            switch hwinfo_status
                case {'shared_memory_connected', 'connected'}
                    score = score + 100;
                case 'csv_fallback'
                    score = score + 80;
                case {'shared_memory_incomplete', 'csv_target_mismatch', 'shared_memory_disabled', ...
                        'shared_memory_expired', 'csv_missing', 'parse_error', 'not_found'}
                    score = score + 60;
            end
            hwinfo_transport = lower(strtrim(char(string(ExternalCollectorDispatcher.pick_text_field(sample, 'hwinfo_transport', 'none')))));
            if ~isempty(hwinfo_transport) && ~strcmp(hwinfo_transport, 'none')
                score = score + 40;
            end
            if ExternalCollectorDispatcher.hwinfo_series_present(sample)
                score = score + 45;
            end
            if ExternalCollectorDispatcher.hwinfo_catalog_present(sample)
                score = score + 35;
            end
            metrics = ExternalCollectorDispatcher.pick_struct_field(sample, 'metrics');
            if isstruct(metrics)
                metric_fields = fieldnames(metrics);
                for i = 1:numel(metric_fields)
                    value = metrics.(metric_fields{i});
                    if isnumeric(value) && ~isempty(value) && any(isfinite(double(value(:))))
                        score = score + 20;
                        break;
                    end
                end
            end
        end

        function merged = overlay_sample_fields(base_sample, overlay_sample)
            merged = base_sample;
            if ~(isstruct(overlay_sample) && ~isempty(fieldnames(overlay_sample)))
                return;
            end
            field_names = {'metrics', 'collector_series', 'collector_status', 'coverage_domains', ...
                'preferred_source', 'raw_log_paths', 'overlay_metrics', 'collector_metric_catalog', ...
                'hwinfo_transport', 'hwinfo_status_reason', 'collector_probe_details'};
            for i = 1:numel(field_names)
                field_name = field_names{i};
                if isfield(overlay_sample, field_name) && ~isempty(overlay_sample.(field_name))
                    merged.(field_name) = overlay_sample.(field_name);
                end
            end
            merged = ExternalCollectorDispatcher.normalize_collector_payload(merged);
        end

        function tf = any_external_enabled(settings)
            settings = ExternalCollectorDispatcher.normalize_settings(settings);
            flags = settings.sustainability.external_collectors;
            tf = logical(flags.hwinfo) || logical(flags.icue);
        end

        function artifact_summary = write_run_artifacts(varargin)
            artifact_summary = struct( ...
                'collector_run_dir', '', ...
                'raw_hwinfo_csv_path', '', ...
                'dataset_csv_path', '', ...
                'curated_dataset_csv_path', '', ...
                'curated_metric_catalog_json_path', '', ...
                'metadata_json_path', '', ...
                'coverage_json_path', '', ...
                'coverage_md_path', '', ...
                'phase_workbook_path', '', ...
                'phase_workbook_root_path', '', ...
                'phase_workbook_status', 'not_requested', ...
                'phase_workbook_formatting_status', 'not_requested', ...
                'phase_sustainability_runtime_plot_png_path', '', ...
                'phase_sustainability_runtime_plot_fig_path', '', ...
                'phase_sustainability_stage_plot_png_path', '', ...
                'phase_sustainability_stage_plot_fig_path', '', ...
                'phase_sustainability_plot_status', 'not_requested', ...
                'phase_sustainability_plot_reason', '', ...
                'stage_boundaries_csv_path', '', ...
                'stage_summary_csv_path', '', ...
                'metric_guide_csv_path', '', ...
                'coverage_rows', struct([]), ...
                'artifact_failures', repmat(struct( ...
                    'artifact', '', ...
                    'identifier', '', ...
                    'message', ''), 1, 0), ...
                'has_failures', false);

            [run_id, monitor_series, paths, summary_context] = ...
                ExternalCollectorDispatcher.parse_artifact_inputs(varargin{:});
            monitor_series = ExternalCollectorDispatcher.normalize_collector_payload(monitor_series);
            monitor_series.environmental_model = ExternalCollectorDispatcher.resolve_environmental_model( ...
                summary_context, monitor_series);
            output_dir = ExternalCollectorDispatcher.resolve_output_dir(paths);
            if isempty(output_dir)
                return;
            end
            compact_v3 = strcmpi(ExternalCollectorDispatcher.pick_struct_text(paths, 'artifact_layout_version', ''), 'compact_v3');
            save_json = ExternalCollectorDispatcher.json_enabled(summary_context, monitor_series, paths);
            artifact_summary.raw_hwinfo_csv_path = ExternalCollectorDispatcher.resolve_phase_raw_hwinfo_csv_path(paths, monitor_series);
            artifact_summary.stage_boundaries_csv_path = ExternalCollectorDispatcher.resolve_phase_boundary_csv_path(paths, monitor_series);

            run_token = ExternalCollectorDispatcher.sanitize_run_id(run_id);
            if isempty(run_token)
                run_token = 'run';
            end
            if compact_v3
                collector_run_dir = output_dir;
            else
                collector_run_dir = fullfile(output_dir, run_token);
            end
            artifact_summary.collector_run_dir = collector_run_dir;
            if exist(collector_run_dir, 'dir') ~= 7
                mkdir(collector_run_dir);
            end

            data_table = table();
            curated_table = table();
            stage_summary = table();
            stage_boundaries = table();
            metric_guide = table();
            csv_first_mode = ExternalCollectorDispatcher.is_phase_csv_first_mode(monitor_series);
            if csv_first_mode
                artifact_summary.dataset_csv_path = ExternalCollectorDispatcher.phase_dataset_csv_path(collector_run_dir, compact_v3);
                try
                    [data_table, artifact_summary.raw_hwinfo_csv_path, stage_boundaries] = ...
                        ExternalCollectorDispatcher.build_dataset_table_from_phase_csv(paths, monitor_series, artifact_summary.dataset_csv_path);
                    monitor_series = ExternalCollectorDispatcher.monitor_series_from_dataset_table(data_table, monitor_series);
                catch ME
                    artifact_summary.artifact_failures = ExternalCollectorDispatcher.record_artifact_failure( ...
                        artifact_summary.artifact_failures, 'collector_dataset_table', ME);
                    data_table = table();
                    stage_boundaries = table();
                    artifact_summary.raw_hwinfo_csv_path = '';
                    artifact_summary.dataset_csv_path = '';
                end
            else
                try
                    data_table = ExternalCollectorDispatcher.build_dataset_table(monitor_series);
                catch ME
                    artifact_summary.artifact_failures = ExternalCollectorDispatcher.record_artifact_failure( ...
                        artifact_summary.artifact_failures, 'collector_dataset_table', ME);
                    data_table = table();
                end
            end
            try
                metric_guide = ExternalCollectorDispatcher.build_metric_guide_table();
            catch ME
                artifact_summary.artifact_failures = ExternalCollectorDispatcher.record_artifact_failure( ...
                    artifact_summary.artifact_failures, 'collector_metric_guide_table', ME);
                metric_guide = table();
            end

            if ~isempty(data_table)
                if ~csv_first_mode
                    try
                        artifact_summary.raw_hwinfo_csv_path = ExternalCollectorDispatcher.write_canonical_hwinfo_csv( ...
                            paths, collector_run_dir, monitor_series, data_table);
                    catch ME
                        artifact_summary.artifact_failures = ExternalCollectorDispatcher.record_artifact_failure( ...
                            artifact_summary.artifact_failures, 'collector_raw_hwinfo_csv', ME);
                        artifact_summary.raw_hwinfo_csv_path = '';
                    end
                    artifact_summary.dataset_csv_path = ExternalCollectorDispatcher.phase_dataset_csv_path(collector_run_dir, compact_v3);
                    try
                        writetable(data_table, artifact_summary.dataset_csv_path);
                    catch ME
                        artifact_summary.artifact_failures = ExternalCollectorDispatcher.record_artifact_failure( ...
                            artifact_summary.artifact_failures, 'collector_dataset_csv', ME);
                        artifact_summary.dataset_csv_path = '';
                    end
                end

                try
                    curated_table = ExternalCollectorDispatcher.build_curated_dataset_table(data_table);
                catch ME
                    artifact_summary.artifact_failures = ExternalCollectorDispatcher.record_artifact_failure( ...
                        artifact_summary.artifact_failures, 'collector_curated_dataset_table', ME);
                    curated_table = table();
                end

                if ~isempty(curated_table)
                    if compact_v3
                        artifact_summary.curated_dataset_csv_path = fullfile(collector_run_dir, 'Telemetry_Curated.csv');
                    else
                        artifact_summary.curated_dataset_csv_path = fullfile(collector_run_dir, 'collector_curated_dataset.csv');
                    end
                    try
                        writetable(curated_table, artifact_summary.curated_dataset_csv_path);
                    catch ME
                        artifact_summary.artifact_failures = ExternalCollectorDispatcher.record_artifact_failure( ...
                            artifact_summary.artifact_failures, 'collector_curated_dataset_csv', ME);
                        artifact_summary.curated_dataset_csv_path = '';
                    end
                end

                try
                    stage_summary = ExternalCollectorDispatcher.build_stage_summary_table(curated_table);
                catch ME
                    artifact_summary.artifact_failures = ExternalCollectorDispatcher.record_artifact_failure( ...
                        artifact_summary.artifact_failures, 'collector_stage_summary_table', ME);
                    stage_summary = table();
                end

                if ~isempty(stage_summary)
                    if compact_v3
                        artifact_summary.stage_summary_csv_path = fullfile(collector_run_dir, 'Telemetry_Stage_Summary.csv');
                    else
                        artifact_summary.stage_summary_csv_path = fullfile(collector_run_dir, 'collector_stage_summary.csv');
                    end
                    try
                        writetable(stage_summary, artifact_summary.stage_summary_csv_path);
                    catch ME
                        artifact_summary.artifact_failures = ExternalCollectorDispatcher.record_artifact_failure( ...
                            artifact_summary.artifact_failures, 'collector_stage_summary_csv', ME);
                        artifact_summary.stage_summary_csv_path = '';
                    end
                end

                if ~csv_first_mode
                    try
                        stage_boundaries = ExternalCollectorDispatcher.build_stage_boundaries_table(monitor_series, data_table);
                    catch ME
                        artifact_summary.artifact_failures = ExternalCollectorDispatcher.record_artifact_failure( ...
                            artifact_summary.artifact_failures, 'collector_stage_boundaries_table', ME);
                        stage_boundaries = table();
                    end
                end
                if ~isempty(stage_boundaries)
                    artifact_summary.stage_boundaries_csv_path = ExternalCollectorDispatcher.phase_stage_boundaries_csv_path( ...
                        paths, collector_run_dir, compact_v3);
                    if ~strcmpi(artifact_summary.stage_boundaries_csv_path, ExternalCollectorDispatcher.pick_struct_text(paths, 'stage_boundaries_csv_path', ''))
                        try
                            writetable(stage_boundaries, artifact_summary.stage_boundaries_csv_path);
                        catch ME
                            artifact_summary.artifact_failures = ExternalCollectorDispatcher.record_artifact_failure( ...
                                artifact_summary.artifact_failures, 'collector_stage_boundaries_csv', ME);
                            artifact_summary.stage_boundaries_csv_path = '';
                        end
                    end
                end
            end

            if ~isempty(metric_guide)
                if compact_v3
                    artifact_summary.metric_guide_csv_path = fullfile(collector_run_dir, 'Metric_Guide.csv');
                else
                    artifact_summary.metric_guide_csv_path = fullfile(collector_run_dir, 'collector_metric_guide.csv');
                end
                try
                    writetable(metric_guide, artifact_summary.metric_guide_csv_path);
                catch ME
                    artifact_summary.artifact_failures = ExternalCollectorDispatcher.record_artifact_failure( ...
                        artifact_summary.artifact_failures, 'collector_metric_guide_csv', ME);
                    artifact_summary.metric_guide_csv_path = '';
                end
            end

            coverage_rows = ExternalCollectorDispatcher.coverage_rows(monitor_series);
            metadata_summary = struct();
            metadata_summary.generated_at_utc = char(datetime('now', 'TimeZone', 'UTC', ...
                'Format', 'yyyy-MM-dd''T''HH:mm:ss''Z'''));
            metadata_summary.run_id = run_token;
            metadata_summary.collector_status = ExternalCollectorDispatcher.pick_struct_field(monitor_series, 'collector_status');
            metadata_summary.coverage_domains = ExternalCollectorDispatcher.pick_struct_field(monitor_series, 'coverage_domains');
            metadata_summary.preferred_source = ExternalCollectorDispatcher.pick_struct_field(monitor_series, 'preferred_source');
            metadata_summary.raw_log_paths = ExternalCollectorDispatcher.pick_struct_field(monitor_series, 'raw_log_paths');
            metadata_summary.collector_metric_catalog = ExternalCollectorDispatcher.metric_catalog(monitor_series);
            metadata_summary.overlay_metrics = ExternalCollectorDispatcher.overlay_metrics(monitor_series);
            metadata_summary.hwinfo_transport = ExternalCollectorDispatcher.pick_text_field(monitor_series, 'hwinfo_transport', 'none');
            metadata_summary.hwinfo_control_mode = ExternalCollectorDispatcher.pick_text_field(monitor_series, 'hwinfo_control_mode', 'shared_memory_live');
            metadata_summary.hwinfo_status_reason = ExternalCollectorDispatcher.pick_text_field(monitor_series, 'hwinfo_status_reason', '');
            metadata_summary.telemetry_enabled = ExternalCollectorDispatcher.telemetry_enabled(monitor_series);
            metadata_summary.telemetry_disable_reason = ExternalCollectorDispatcher.telemetry_disable_reason(monitor_series);
            metadata_summary.collector_probe_details = ExternalCollectorDispatcher.pick_struct_field(monitor_series, 'collector_probe_details');
            metadata_summary.raw_hwinfo_csv_path = artifact_summary.raw_hwinfo_csv_path;
            metadata_summary.dataset_csv_path = artifact_summary.dataset_csv_path;

            if save_json
                if compact_v3
                    artifact_summary.curated_metric_catalog_json_path = fullfile(collector_run_dir, 'Collector_Metric_Catalog.json');
                else
                    artifact_summary.curated_metric_catalog_json_path = fullfile(collector_run_dir, 'collector_curated_metric_catalog.json');
                end
                try
                    ExternalCollectorDispatcher.write_text_file(artifact_summary.curated_metric_catalog_json_path, ...
                        jsonencode(metadata_summary.collector_metric_catalog));
                catch ME
                    artifact_summary.artifact_failures = ExternalCollectorDispatcher.record_artifact_failure( ...
                        artifact_summary.artifact_failures, 'collector_curated_metric_catalog_json', ME);
                    artifact_summary.curated_metric_catalog_json_path = '';
                end
            end

            if save_json
                if compact_v3
                    artifact_summary.metadata_json_path = fullfile(collector_run_dir, 'Collector_Metadata.json');
                else
                    artifact_summary.metadata_json_path = fullfile(collector_run_dir, 'collector_metadata.json');
                end
                try
                    ExternalCollectorDispatcher.write_text_file(artifact_summary.metadata_json_path, jsonencode(metadata_summary));
                catch ME
                    artifact_summary.artifact_failures = ExternalCollectorDispatcher.record_artifact_failure( ...
                        artifact_summary.artifact_failures, 'collector_metadata_json', ME);
                    artifact_summary.metadata_json_path = '';
                end
            end

            coverage_summary = metadata_summary;
            coverage_summary.rows = coverage_rows;

            if save_json
                if compact_v3
                    artifact_summary.coverage_json_path = fullfile(collector_run_dir, 'Collector_Coverage.json');
                else
                    artifact_summary.coverage_json_path = fullfile(collector_run_dir, 'collector_coverage.json');
                end
                try
                    ExternalCollectorDispatcher.write_text_file(artifact_summary.coverage_json_path, jsonencode(coverage_summary));
                catch ME
                    artifact_summary.artifact_failures = ExternalCollectorDispatcher.record_artifact_failure( ...
                        artifact_summary.artifact_failures, 'collector_coverage_json', ME);
                    artifact_summary.coverage_json_path = '';
                end
            end

            if compact_v3
                artifact_summary.coverage_md_path = fullfile(collector_run_dir, 'Collector_Coverage.md');
            else
                artifact_summary.coverage_md_path = fullfile(collector_run_dir, 'collector_coverage.md');
            end
            try
                ExternalCollectorDispatcher.write_text_file(artifact_summary.coverage_md_path, ...
                    ExternalCollectorDispatcher.coverage_markdown(coverage_summary));
            catch ME
                artifact_summary.artifact_failures = ExternalCollectorDispatcher.record_artifact_failure( ...
                    artifact_summary.artifact_failures, 'collector_coverage_md', ME);
                artifact_summary.coverage_md_path = '';
            end
            artifact_summary.coverage_rows = coverage_rows;

            try
                [artifact_summary.phase_workbook_path, artifact_summary.phase_workbook_formatting_status, workbook_summary] = ...
                    ExternalCollectorDispatcher.write_phase_workbook(paths, run_token, summary_context, monitor_series, ...
                    ExternalCollectorDispatcher.safe_table(data_table), ...
                    ExternalCollectorDispatcher.safe_table(curated_table), ...
                    ExternalCollectorDispatcher.safe_table(stage_summary), ...
                    ExternalCollectorDispatcher.safe_table(metric_guide));
                artifact_summary.phase_workbook_root_path = ExternalCollectorDispatcher.pick_struct_text(workbook_summary, 'root_mirror_path', '');
                artifact_summary.phase_workbook_status = ExternalCollectorDispatcher.pick_struct_text(workbook_summary, 'workbook_status', ...
                    artifact_summary.phase_workbook_formatting_status);
                if isfield(workbook_summary, 'failure_message') && ~isempty(strtrim(char(string(workbook_summary.failure_message))))
                    workbook_me = MException('ExternalCollectorDispatcher:WorkbookArtifactFailure', ...
                        '%s', char(string(workbook_summary.failure_message)));
                    artifact_summary.artifact_failures = ExternalCollectorDispatcher.record_artifact_failure( ...
                        artifact_summary.artifact_failures, 'phase_workbook', workbook_me);
                end
            catch ME
                artifact_summary.artifact_failures = ExternalCollectorDispatcher.record_artifact_failure( ...
                    artifact_summary.artifact_failures, 'phase_workbook', ME);
                artifact_summary.phase_workbook_path = '';
                artifact_summary.phase_workbook_root_path = '';
                artifact_summary.phase_workbook_status = 'failed';
                artifact_summary.phase_workbook_formatting_status = 'failed';
            end

            try
                sustainability_plot_summary = ExternalCollectorDispatcher.save_phase1_sustainability_plots( ...
                    paths, summary_context, ExternalCollectorDispatcher.safe_table(stage_summary));
                artifact_summary.phase_sustainability_runtime_plot_png_path = ExternalCollectorDispatcher.pick_struct_text( ...
                    sustainability_plot_summary, 'runtime_png_path', '');
                artifact_summary.phase_sustainability_runtime_plot_fig_path = ExternalCollectorDispatcher.pick_struct_text( ...
                    sustainability_plot_summary, 'runtime_fig_path', '');
                artifact_summary.phase_sustainability_stage_plot_png_path = ExternalCollectorDispatcher.pick_struct_text( ...
                    sustainability_plot_summary, 'stage_png_path', '');
                artifact_summary.phase_sustainability_stage_plot_fig_path = ExternalCollectorDispatcher.pick_struct_text( ...
                    sustainability_plot_summary, 'stage_fig_path', '');
                artifact_summary.phase_sustainability_plot_status = ExternalCollectorDispatcher.pick_struct_text( ...
                    sustainability_plot_summary, 'status', 'created');
                artifact_summary.phase_sustainability_plot_reason = ExternalCollectorDispatcher.pick_struct_text( ...
                    sustainability_plot_summary, 'reason', '');
            catch ME
                artifact_summary.artifact_failures = ExternalCollectorDispatcher.record_artifact_failure( ...
                    artifact_summary.artifact_failures, 'phase1_sustainability_plots', ME);
                artifact_summary.phase_sustainability_plot_status = 'failed';
                artifact_summary.phase_sustainability_plot_reason = char(string(ME.message));
            end
            try
                sustainability_compare_summary = ExternalCollectorDispatcher.save_workflow_sustainability_comparison_plots( ...
                    paths, summary_context, ExternalCollectorDispatcher.safe_table(stage_summary));
                artifact_summary.workflow_sustainability_comparison_png_path = ExternalCollectorDispatcher.pick_struct_text( ...
                    sustainability_compare_summary, 'png_path', '');
                artifact_summary.workflow_sustainability_comparison_fig_path = ExternalCollectorDispatcher.pick_struct_text( ...
                    sustainability_compare_summary, 'fig_path', '');
                artifact_summary.workflow_sustainability_comparison_status = ExternalCollectorDispatcher.pick_struct_text( ...
                    sustainability_compare_summary, 'status', 'not_requested');
                artifact_summary.workflow_sustainability_comparison_reason = ExternalCollectorDispatcher.pick_struct_text( ...
                    sustainability_compare_summary, 'reason', '');
            catch ME
                artifact_summary.artifact_failures = ExternalCollectorDispatcher.record_artifact_failure( ...
                    artifact_summary.artifact_failures, 'workflow_sustainability_comparison_plots', ME);
                artifact_summary.workflow_sustainability_comparison_status = 'failed';
                artifact_summary.workflow_sustainability_comparison_reason = char(string(ME.message));
            end
            metadata_summary.phase_sustainability_plot_status = artifact_summary.phase_sustainability_plot_status;
            metadata_summary.phase_sustainability_plot_reason = artifact_summary.phase_sustainability_plot_reason;
            if save_json && ~isempty(artifact_summary.metadata_json_path)
                try
                    ExternalCollectorDispatcher.write_text_file(artifact_summary.metadata_json_path, jsonencode(metadata_summary));
                catch
                end
            end

            artifact_summary.has_failures = ~isempty(artifact_summary.artifact_failures);
        end

        function tf = json_enabled(varargin)
            tf = false;
            for i = 1:nargin
                source = varargin{i};
                if ~isstruct(source)
                    continue;
                end
                if isfield(source, 'save_json') && ~isempty(source.save_json)
                    tf = logical(source.save_json);
                    return;
                end
                if isfield(source, 'settings') && isstruct(source.settings) && ...
                        isfield(source.settings, 'save_json') && ~isempty(source.settings.save_json)
                    tf = logical(source.settings.save_json);
                    return;
                end
                if isfield(source, 'phase_config') && isstruct(source.phase_config) && ...
                        isfield(source.phase_config, 'save_json') && ~isempty(source.phase_config.save_json)
                    tf = logical(source.phase_config.save_json);
                    return;
                end
                if isfield(source, 'results') && isstruct(source.results)
                    result_payload = source.results;
                    if isfield(result_payload, 'save_json') && ~isempty(result_payload.save_json)
                        tf = logical(result_payload.save_json);
                        return;
                    end
                    if isfield(result_payload, 'phase_config') && isstruct(result_payload.phase_config) && ...
                            isfield(result_payload.phase_config, 'save_json') && ~isempty(result_payload.phase_config.save_json)
                        tf = logical(result_payload.phase_config.save_json);
                        return;
                    end
                end
            end
        end

        function rows = coverage_rows(monitor_series)
            catalog = ExternalCollectorDispatcher.coverage_catalog();
            rows = repmat(struct( ...
                'raw_metric_name', '', ...
                'normalized_metric_key', '', ...
                'domain', '', ...
                'hwinfo_supported', false, ...
                'icue_supported', false, ...
                'preferred_source', '', ...
                'notes', ''), 1, numel(catalog));

            coverage_domains = ExternalCollectorDispatcher.pick_struct_field(monitor_series, 'coverage_domains');
            for i = 1:numel(catalog)
                metric = catalog(i);
                hwinfo_supported = ExternalCollectorDispatcher.source_supports_metric( ...
                    monitor_series, coverage_domains, 'hwinfo', metric.metric_key, metric.domain);
                icue_supported = ExternalCollectorDispatcher.source_supports_metric( ...
                    monitor_series, coverage_domains, 'icue', metric.metric_key, metric.domain);
                note = 'not observed';
                if hwinfo_supported && icue_supported
                    note = 'overlap';
                elseif hwinfo_supported
                    note = 'hwinfo only';
                elseif icue_supported
                    note = 'icue only';
                end
                rows(i) = struct( ...
                    'raw_metric_name', metric.raw_metric_name, ...
                    'normalized_metric_key', metric.metric_key, ...
                    'domain', metric.domain, ...
                    'hwinfo_supported', hwinfo_supported, ...
                    'icue_supported', icue_supported, ...
                    'preferred_source', ExternalCollectorDispatcher.preferred_source_for_domain(metric.domain), ...
                    'notes', note);
            end
        end

        function source = preferred_source_for_domain(domain)
            switch lower(char(string(domain)))
                case {'fan', 'pump', 'battery', 'device'}
                    source = 'icue';
                otherwise
                    source = 'hwinfo';
            end
        end

        function bundle = collector_plot_bundle(monitor_series, analysis, params)
            if nargin < 1 || ~isstruct(monitor_series)
                monitor_series = struct();
            end
            if nargin < 2 || ~isstruct(analysis)
                analysis = struct();
            end
            if nargin < 3 || ~isstruct(params)
                params = struct();
            end

            panel_profile = ExternalCollectorDispatcher.resolve_collector_panel_profile(params, monitor_series);
            catalog = ExternalCollectorDispatcher.collector_panel_catalog(panel_profile);
            metric_catalog = ExternalCollectorDispatcher.metric_catalog(monitor_series);
            panels = repmat(struct( ...
                'id', '', ...
                'title', '', ...
                'ylabel', '', ...
                'traces', struct([]), ...
                'placeholder_text', ''), 1, numel(catalog));
            for i = 1:numel(catalog)
                panel_def = catalog(i);
                switch panel_def.type
                    case 'shared'
                        traces = ExternalCollectorDispatcher.shared_overlay_traces( ...
                            monitor_series, analysis, params, panel_def.metric_key);
                    case 'multi_metric'
                        traces = ExternalCollectorDispatcher.multi_metric_traces( ...
                            monitor_series, analysis, params, panel_def.metric_keys, panel_def.sources);
                    case 'cooling'
                        traces = ExternalCollectorDispatcher.multi_metric_traces( ...
                            monitor_series, analysis, params, ...
                            {'fan_rpm', 'pump_rpm'}, {'hwinfo', 'icue'});
                    otherwise
                        traces = ExternalCollectorDispatcher.single_metric_traces( ...
                            monitor_series, analysis, params, panel_def.metric_key, {'hwinfo', 'icue'});
                end

                panels(i) = struct( ...
                    'id', panel_def.id, ...
                    'title', panel_def.title, ...
                    'ylabel', panel_def.ylabel, ...
                    'traces', traces, ...
                    'placeholder_text', panel_def.placeholder_text);
            end

            bundle = struct( ...
                'panels', panels, ...
                'summary_lines', {ExternalCollectorDispatcher.collector_summary_lines(monitor_series)}, ...
                'coverage_rows', ExternalCollectorDispatcher.coverage_rows(monitor_series), ...
                'metric_catalog', metric_catalog, ...
                'button_specs', ExternalCollectorDispatcher.collector_popup_button_specs(catalog, metric_catalog));
        end

        function lines = collector_summary_lines(monitor_series)
            if nargin < 1 || ~isstruct(monitor_series)
                monitor_series = struct();
            end

            collector_status = ExternalCollectorDispatcher.pick_struct_field(monitor_series, 'collector_status');
            coverage_domains = ExternalCollectorDispatcher.pick_struct_field(monitor_series, 'coverage_domains');
            preferred_source = ExternalCollectorDispatcher.pick_struct_field(monitor_series, 'preferred_source');
            raw_log_paths = ExternalCollectorDispatcher.pick_struct_field(monitor_series, 'raw_log_paths');
            collector_probe_details = ExternalCollectorDispatcher.pick_struct_field(monitor_series, 'collector_probe_details');
            hwinfo_transport = ExternalCollectorDispatcher.pick_text_field(monitor_series, 'hwinfo_transport', 'none');
            hwinfo_reason = ExternalCollectorDispatcher.pick_text_field(monitor_series, 'hwinfo_status_reason', '');
            hwinfo_probe = ExternalCollectorDispatcher.pick_struct_field(collector_probe_details, 'hwinfo');

            lines = { ...
                'Collector Status:' ...
                sprintf('  MATLAB  -> %s', ExternalCollectorDispatcher.humanize_collector_status( ...
                    ExternalCollectorDispatcher.pick_text_field(collector_status, 'matlab', 'connected'))) ...
                sprintf('  HWiNFO  -> %s', ExternalCollectorDispatcher.humanize_collector_status( ...
                    ExternalCollectorDispatcher.pick_text_field(collector_status, 'hwinfo', 'off'))) ...
                sprintf('  iCUE    -> %s', ExternalCollectorDispatcher.humanize_collector_status( ...
                    ExternalCollectorDispatcher.pick_text_field(collector_status, 'icue', 'off'))) ...
                ' ' ...
                'HWiNFO Transport:' ...
                sprintf('  Active transport -> %s', ExternalCollectorDispatcher.humanize_transport(hwinfo_transport)) ...
                sprintf('  Status reason    -> %s', ExternalCollectorDispatcher.if_empty_text(hwinfo_reason, '--')) ...
                sprintf('  CSV target dir   -> %s', ExternalCollectorDispatcher.pick_text_field(hwinfo_probe, 'csv_target_dir', '--')) ...
                sprintf('  CSV target path  -> %s', ExternalCollectorDispatcher.pick_text_field(hwinfo_probe, 'csv_target_path', '--')) ...
                sprintf('  Observed CSV     -> %s', ExternalCollectorDispatcher.pick_text_field(raw_log_paths, 'hwinfo', '--')) ...
                sprintf('  INI path         -> %s', ExternalCollectorDispatcher.pick_text_field(hwinfo_probe, 'ini_path', '--')) ...
                sprintf('  SensorsSM state  -> %s', ExternalCollectorDispatcher.pick_text_field(hwinfo_probe, 'ini_shared_memory_state', '--')) ...
                sprintf('  Target sync      -> %s', ExternalCollectorDispatcher.pick_text_field(hwinfo_probe, 'csv_target_sync_status', '--')) ...
                sprintf('  Shared memory    -> %s', ExternalCollectorDispatcher.bool_text( ...
                    ExternalCollectorDispatcher.pick_nested_logical(collector_probe_details, 'hwinfo', 'shared_memory_available'))) ...
                sprintf('  CSV fallback     -> %s', ExternalCollectorDispatcher.bool_text( ...
                    ExternalCollectorDispatcher.pick_nested_logical(collector_probe_details, 'hwinfo', 'csv_available'))) ...
                ' ' ...
                'Preferred Sources:'};

            preferred_fields = fieldnames(preferred_source);
            if isempty(preferred_fields)
                lines{end + 1} = '  --'; %#ok<AGROW>
            else
                preferred_fields = sort(preferred_fields);
                for i = 1:numel(preferred_fields)
                    key = preferred_fields{i};
                    lines{end + 1} = sprintf('  %s -> %s', ... %#ok<AGROW>
                        ExternalCollectorDispatcher.humanize_metric_name(key), ...
                        ExternalCollectorDispatcher.pick_text_field(preferred_source, key, '--'));
                end
            end

            lines{end + 1} = ' '; %#ok<AGROW>
            lines{end + 1} = 'Coverage Domains:'; %#ok<AGROW>
            lines{end + 1} = sprintf('  HWiNFO -> %s', ... %#ok<AGROW>
                ExternalCollectorDispatcher.join_cellstr(ExternalCollectorDispatcher.pick_domains(coverage_domains, 'hwinfo')));
            lines{end + 1} = sprintf('  iCUE   -> %s', ... %#ok<AGROW>
                ExternalCollectorDispatcher.join_cellstr(ExternalCollectorDispatcher.pick_domains(coverage_domains, 'icue')));

            lines{end + 1} = ' '; %#ok<AGROW>
            lines{end + 1} = 'Raw Log Paths:'; %#ok<AGROW>
            lines{end + 1} = sprintf('  HWiNFO -> %s', ... %#ok<AGROW>
                ExternalCollectorDispatcher.pick_text_field(raw_log_paths, 'hwinfo', '--'));
            lines{end + 1} = sprintf('  iCUE   -> %s', ... %#ok<AGROW>
                ExternalCollectorDispatcher.pick_text_field(raw_log_paths, 'icue', '--'));
        end

        function style = source_plot_style(source, metric_key)
            if nargin < 2
                metric_key = '';
            end

            switch lower(char(string(source)))
                case 'hwinfo'
                    style = struct( ...
                        'rgb', [0.36, 1.00, 0.54], ...
                        'hex', '#5cff8a', ...
                        'line_style', '-', ...
                        'plotly_dash', 'solid');
                case 'icue'
                    style = struct( ...
                        'rgb', [0.95, 0.70, 0.25], ...
                        'hex', '#f2b33f', ...
                        'line_style', '--', ...
                        'plotly_dash', 'dash');
                case 'normalized'
                    style = struct( ...
                        'rgb', [0.36, 1.00, 0.54], ...
                        'hex', '#5cff8a', ...
                        'line_style', '-', ...
                        'plotly_dash', 'solid');
                otherwise
                    style = struct( ...
                        'rgb', [0.36, 0.71, 1.00], ...
                        'hex', '#5bb4ff', ...
                        'line_style', ':', ...
                        'plotly_dash', 'dot');
            end

            [style.marker, style.plotly_marker] = ExternalCollectorDispatcher.metric_marker(metric_key);
        end

        function table_out = launch_session_table(session_id, run_index, metadata, run_config, parameters, monitor_series)
            table_out = ExternalCollectorDispatcher.build_launch_session_table( ...
                session_id, run_index, metadata, run_config, parameters, monitor_series);
        end

        function sample = runtime_probe(settings)
            settings = ExternalCollectorDispatcher.normalize_settings(settings);
            sample = ExternalCollectorDispatcher.empty_sample();
            if ~ExternalCollectorDispatcher.any_external_enabled(settings)
                return;
            end

            probe_dispatcher = ExternalCollectorDispatcher(settings);
            cleanup_dispatcher = onCleanup(@() probe_dispatcher.stop_live_session());
            probe_run_id = sprintf('collector_probe_%s', ...
                char(datetime('now', 'Format', 'yyyyMMdd_HHmmss')));
            probe_dispatcher.start_live_session(probe_run_id, settings);
            pause(0.1);
            probe_sample = probe_dispatcher.poll_latest_sample();
            if isstruct(probe_sample) && ~isempty(fieldnames(probe_sample))
                sample = probe_sample;
            end
            clear cleanup_dispatcher;
        end

        function monitor_series = recover_monitor_series(varargin)
            monitor_series = struct();
            paths = struct();
            summary_context = struct();

            first_arg_is_context = false;
            if nargin >= 3 && isstruct(varargin{1}) && isstruct(varargin{2}) && isstruct(varargin{3})
                first_arg_is_context = true;
            elseif nargin >= 1 && isstruct(varargin{1})
                first_fields = fieldnames(varargin{1});
                context_markers = {'monitor_series', 'results', 'run_config', 'paths', 'workflow_kind', ...
                    'phase_id', 'metadata', 'run_id'};
                first_arg_is_context = any(ismember(context_markers, first_fields));
            end

            if first_arg_is_context
                summary_context = varargin{1};
                if nargin >= 2 && isstruct(varargin{2}) && ~isempty(fieldnames(varargin{2}))
                    monitor_series = varargin{2};
                else
                    monitor_series = ExternalCollectorDispatcher.pick_struct_field(summary_context, 'monitor_series');
                end
                if nargin >= 3 && isstruct(varargin{3}) && ~isempty(fieldnames(varargin{3}))
                    paths = varargin{3};
                else
                    paths = ExternalCollectorDispatcher.pick_struct_field(summary_context, 'paths');
                end
                if isempty(fieldnames(paths))
                    results_struct = ExternalCollectorDispatcher.pick_struct_field(summary_context, 'results');
                    paths = ExternalCollectorDispatcher.pick_struct_field(results_struct, 'paths');
                end
                if ~isfield(summary_context, 'monitor_series') || ~isstruct(summary_context.monitor_series) || ...
                        isempty(fieldnames(summary_context.monitor_series))
                    summary_context.monitor_series = monitor_series;
                end
                if ~isfield(summary_context, 'paths') || ~isstruct(summary_context.paths) || ...
                        isempty(fieldnames(summary_context.paths))
                    summary_context.paths = paths;
                end
            else
                if nargin >= 1 && isstruct(varargin{1})
                    monitor_series = varargin{1};
                end
                if nargin >= 2 && isstruct(varargin{2})
                    paths = varargin{2};
                end
                run_id = '';
                if nargin >= 3 && (ischar(varargin{3}) || isstring(varargin{3}) || ...
                        ((isnumeric(varargin{3}) || islogical(varargin{3})) && isscalar(varargin{3})))
                    run_id = char(string(varargin{3}));
                end
                summary_context = struct( ...
                    'run_id', run_id, ...
                    'monitor_series', monitor_series, ...
                    'paths', paths);
            end

            monitor_series = ExternalCollectorDispatcher.recover_monitor_series_from_summary(summary_context, monitor_series, paths);
        end

        function env_model = resolve_environmental_model_public(varargin)
            env_model = ExternalCollectorDispatcher.resolve_environmental_model(varargin{:});
        end

        function factor = environmental_factor_g_per_wh_public(env_model)
            factor = ExternalCollectorDispatcher.environmental_factor_g_per_wh(env_model);
        end

        function power_vec = resolve_environmental_power_vector_public(table_or_series, env_model)
            power_vec = ExternalCollectorDispatcher.resolve_environmental_power_vector(table_or_series, env_model);
        end
    end

    methods (Static, Access = private)
        function sample = empty_sample()
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
        end

        function panel_profile = resolve_collector_panel_profile(params, monitor_series)
            panel_profile = ExternalCollectorDispatcher.pick_struct_text(params, 'collector_panel_profile', '');
            if isempty(strtrim(panel_profile))
                collectors = ExternalCollectorDispatcher.pick_struct_field(params, 'collectors');
                panel_profile = ExternalCollectorDispatcher.pick_struct_text(collectors, 'panel_profile', '');
            end
            if isempty(strtrim(panel_profile))
                sustainability = ExternalCollectorDispatcher.pick_struct_field(params, 'sustainability');
                panel_profile = ExternalCollectorDispatcher.pick_struct_text(sustainability, 'collector_panel_profile', '');
            end
            if isempty(strtrim(panel_profile))
                runtime = ExternalCollectorDispatcher.pick_struct_field( ...
                    ExternalCollectorDispatcher.pick_struct_field(params, 'sustainability'), 'collector_runtime');
                panel_profile = ExternalCollectorDispatcher.pick_struct_text(runtime, 'collector_panel_profile', '');
            end
            if isempty(strtrim(panel_profile))
                panel_profile = ExternalCollectorDispatcher.pick_struct_text(monitor_series, 'collector_panel_profile', '');
            end
            if isempty(strtrim(panel_profile))
                machine_tag = ExternalCollectorDispatcher.pick_struct_text(params, 'machine_tag', '');
                if isempty(strtrim(machine_tag))
                    collectors = ExternalCollectorDispatcher.pick_struct_field(params, 'collectors');
                    machine_tag = ExternalCollectorDispatcher.pick_struct_text(collectors, 'machine_tag', getenv('COMPUTERNAME'));
                end
                panel_profile = ExternalCollectorDispatcher.infer_collector_panel_profile(machine_tag);
            end
        end

        function panel_profile = infer_collector_panel_profile(machine_tag)
            machine_token = lower(strtrim(char(string(machine_tag))));
            if contains(machine_token, 'zenbook') || any(strcmp(machine_token, {'asus_zenbook', 'laptop', 'notebook'}))
                panel_profile = 'laptop';
            else
                panel_profile = 'desktop';
            end
        end

        function rows = coverage_catalog()
            rows = [ ...
                struct('raw_metric_name', 'CPU usage', 'metric_key', 'cpu_proxy', 'domain', 'cpu'), ...
                struct('raw_metric_name', 'GPU load', 'metric_key', 'gpu_series', 'domain', 'gpu'), ...
                struct('raw_metric_name', 'CPU voltage', 'metric_key', 'cpu_voltage_v', 'domain', 'voltage'), ...
                struct('raw_metric_name', 'GPU voltage', 'metric_key', 'gpu_voltage_v', 'domain', 'voltage'), ...
                struct('raw_metric_name', 'Memory voltage', 'metric_key', 'memory_voltage_v', 'domain', 'voltage'), ...
                struct('raw_metric_name', 'CPU package temperature', 'metric_key', 'cpu_temp_c', 'domain', 'temperature'), ...
                struct('raw_metric_name', 'Package power', 'metric_key', 'power_w', 'domain', 'power'), ...
                struct('raw_metric_name', 'CPU package power', 'metric_key', 'cpu_power_w_hwinfo', 'domain', 'power'), ...
                struct('raw_metric_name', 'GPU power', 'metric_key', 'gpu_power_w_hwinfo', 'domain', 'power'), ...
                struct('raw_metric_name', 'Memory power / demand proxy', 'metric_key', 'memory_power_w_or_proxy', 'domain', 'power'), ...
                struct('raw_metric_name', 'Total system power', 'metric_key', 'system_power_w', 'domain', 'power'), ...
                struct('raw_metric_name', 'Memory usage', 'metric_key', 'memory_series', 'domain', 'memory'), ...
                struct('raw_metric_name', 'Fan speed', 'metric_key', 'fan_rpm', 'domain', 'fan'), ...
                struct('raw_metric_name', 'Pump speed', 'metric_key', 'pump_rpm', 'domain', 'pump'), ...
                struct('raw_metric_name', 'Coolant temperature', 'metric_key', 'coolant_temp_c', 'domain', 'temperature'), ...
                struct('raw_metric_name', 'Device battery level', 'metric_key', 'device_battery_level', 'domain', 'battery') ...
            ];
        end

        function catalog = collector_panel_catalog(panel_profile)
            if nargin < 1
                panel_profile = '';
            end

            panels = [ ...
                struct('id', 'cpu_overlay', 'title', 'CPU Usage Overlay', 'ylabel', 'CPU (%)', ...
                    'type', 'shared', 'metric_key', 'cpu_proxy', ...
                    'metric_keys', {{}}, 'sources', {{}}, ...
                    'placeholder_text', 'No collector data recorded for this metric.'), ...
                struct('id', 'gpu_overlay', 'title', 'GPU Usage Overlay', 'ylabel', 'GPU (%)', ...
                    'type', 'shared', 'metric_key', 'gpu_series', ...
                    'metric_keys', {{}}, 'sources', {{}}, ...
                    'placeholder_text', 'No collector data recorded for this metric.'), ...
                struct('id', 'memory_overlay', 'title', 'Memory Usage Overlay', 'ylabel', 'Memory (%)', ...
                    'type', 'shared', 'metric_key', 'memory_series', ...
                    'metric_keys', {{}}, 'sources', {{}}, ...
                    'placeholder_text', 'No collector data recorded for this metric.'), ...
                struct('id', 'power_overlay', 'title', 'System Power Overlay', 'ylabel', 'Power (W)', ...
                    'type', 'shared', 'metric_key', 'system_power_w', ...
                    'metric_keys', {{}}, 'sources', {{}}, ...
                    'placeholder_text', 'No collector data recorded for this metric.'), ...
                struct('id', 'cpu_temp', 'title', 'CPU Package Temperature', 'ylabel', 'Temp (C)', ...
                    'type', 'shared', 'metric_key', 'cpu_temp_c', ...
                    'metric_keys', {{}}, 'sources', {{}}, ...
                    'placeholder_text', 'No collector data recorded for this metric.'), ...
                struct('id', 'voltage_overlay', 'title', 'Voltage Overlay', 'ylabel', 'Voltage (V)', ...
                    'type', 'multi_metric', 'metric_key', 'cpu_voltage_v', ...
                    'metric_keys', {{'cpu_voltage_v', 'gpu_voltage_v', 'memory_voltage_v'}}, ...
                    'sources', {{'normalized'}}, ...
                    'placeholder_text', 'No voltage telemetry recorded for this run.'), ...
                struct('id', 'cooling_rpm', 'title', 'Cooling RPM', 'ylabel', 'RPM', ...
                    'type', 'cooling', 'metric_key', '', ...
                    'metric_keys', {{}}, 'sources', {{}}, ...
                    'placeholder_text', 'No cooling telemetry recorded for this run.'), ...
                struct('id', 'coolant_temp', 'title', 'Coolant Temperature', 'ylabel', 'Temp (C)', ...
                    'type', 'single_metric', 'metric_key', 'coolant_temp_c', ...
                    'metric_keys', {{}}, 'sources', {{}}, ...
                    'placeholder_text', 'No coolant telemetry recorded for this run.'), ...
                struct('id', 'power_split', 'title', 'Hardware Power Split', 'ylabel', 'Power (W)', ...
                    'type', 'multi_metric', 'metric_key', 'cpu_power_w_hwinfo', ...
                    'metric_keys', {{'cpu_power_w_hwinfo', 'gpu_power_w_hwinfo', 'memory_power_w_or_proxy'}}, ...
                    'sources', {{'normalized'}}, ...
                    'placeholder_text', 'No hardware power telemetry recorded for this run.'), ...
                struct('id', 'energy_cumulative', 'title', 'Cumulative Energy', 'ylabel', 'Energy (Wh)', ...
                    'type', 'single_metric', 'metric_key', 'environmental_energy_wh_cum', ...
                    'metric_keys', {{}}, 'sources', {{'normalized'}}, ...
                    'placeholder_text', 'No cumulative energy telemetry recorded for this run.'), ...
                struct('id', 'co2_cumulative', 'title', 'Cumulative CO2e', 'ylabel', 'CO2e (g)', ...
                    'type', 'single_metric', 'metric_key', 'environmental_co2_g_cum', ...
                    'metric_keys', {{}}, 'sources', {{'normalized'}}, ...
                    'placeholder_text', 'No cumulative CO2e telemetry recorded for this run.'), ...
                struct('id', 'battery_level', 'title', 'Device Battery Level', 'ylabel', 'Battery (%)', ...
                    'type', 'single_metric', 'metric_key', 'device_battery_level', ...
                    'metric_keys', {{}}, 'sources', {{'icue', 'normalized'}}, ...
                    'placeholder_text', 'No device battery telemetry recorded for this run.') ...
            ];

            switch lower(strtrim(char(string(panel_profile))))
                case 'laptop'
                    panel_order = {'cpu_overlay', 'gpu_overlay', 'memory_overlay', 'power_overlay', ...
                        'cpu_temp', 'voltage_overlay', 'power_split', 'battery_level'};
                otherwise
                    panel_order = {'cpu_overlay', 'gpu_overlay', 'memory_overlay', 'power_overlay', ...
                        'cpu_temp', 'voltage_overlay', 'cooling_rpm', 'coolant_temp'};
            end

            ordered_idx = zeros(1, 0);
            for i = 1:numel(panel_order)
                idx = find(strcmp({panels.id}, panel_order{i}), 1, 'first');
                if ~isempty(idx)
                    ordered_idx(end + 1) = idx; %#ok<AGROW>
                end
            end
            remaining_idx = setdiff(1:numel(panels), ordered_idx, 'stable');
            catalog = panels([ordered_idx, remaining_idx]);
        end

        function catalog = metric_catalog(monitor_series)
            raw_catalog = ExternalCollectorDispatcher.empty_metric_catalog();
            if isstruct(monitor_series) && isfield(monitor_series, 'collector_metric_catalog') && ...
                    ~isempty(monitor_series.collector_metric_catalog)
                raw_catalog = ExternalCollectorDispatcher.normalize_metric_catalog_array( ...
                    monitor_series.collector_metric_catalog);
            end
            if isempty(raw_catalog)
                raw_catalog = ExternalCollectorDispatcher.derive_metric_catalog_from_series(monitor_series);
            end
            catalog = ExternalCollectorDispatcher.curated_metric_catalog(raw_catalog, monitor_series);
        end

        function specs = collector_popup_button_specs(panel_catalog, metric_catalog)
            specs = repmat(struct( ...
                'id', '', ...
                'label', '', ...
                'panel_id', '', ...
                'default_title', '', ...
                'default_x_id', 'session_time_s', ...
                'default_y_id', '', ...
                'default_xlabel', 'Time (s)', ...
                'default_ylabel', ''), 1, numel(panel_catalog));
            for i = 1:numel(panel_catalog)
                panel = panel_catalog(i);
                default_y_id = ExternalCollectorDispatcher.resolve_popup_default_metric_id( ...
                    panel.metric_key, metric_catalog, panel.id);
                specs(i) = struct( ...
                    'id', sprintf('%s_popup_button', panel.id), ...
                    'label', sprintf('Open %s', panel.title), ...
                    'panel_id', panel.id, ...
                    'default_title', panel.title, ...
                    'default_x_id', 'session_time_s', ...
                    'default_y_id', default_y_id, ...
                    'default_xlabel', 'Time (s)', ...
                    'default_ylabel', panel.ylabel);
            end
        end

        function traces = shared_overlay_traces(monitor_series, analysis, params, metric_key)
            supported_metrics = {'cpu_proxy', 'gpu_series', 'memory_series', 'cpu_temp_c', 'power_w', 'system_power_w'};
            overlay_metrics = ExternalCollectorDispatcher.overlay_metrics(monitor_series);
            traces = struct([]);
            if ~any(strcmpi(supported_metrics, metric_key)) || ...
                    ~any(strcmpi(overlay_metrics, metric_key))
                return;
            end
            if any(strcmpi(metric_key, {'cpu_proxy', 'gpu_series', 'memory_series', 'cpu_temp_c', 'system_power_w'}))
                traces = ExternalCollectorDispatcher.single_metric_traces( ...
                    monitor_series, analysis, params, metric_key, {'normalized'});
                return;
            end
            traces = ExternalCollectorDispatcher.single_metric_traces( ...
                monitor_series, analysis, params, metric_key, {'matlab', 'hwinfo', 'icue'});
        end

        function traces = single_metric_traces(monitor_series, analysis, params, metric_key, sources)
            traces = ExternalCollectorDispatcher.empty_trace_array();
            for i = 1:numel(sources)
                display_source = ExternalCollectorDispatcher.resolve_effective_source( ...
                    monitor_series, sources{i}, metric_key);
                trace = ExternalCollectorDispatcher.make_trace( ...
                    monitor_series, analysis, params, sources{i}, metric_key, ...
                    ExternalCollectorDispatcher.source_label(display_source));
                if ~isempty(trace)
                    traces(end + 1) = trace; %#ok<AGROW>
                end
            end
        end

        function traces = multi_metric_traces(monitor_series, analysis, params, metric_keys, sources)
            traces = ExternalCollectorDispatcher.empty_trace_array();
            for si = 1:numel(sources)
                source = sources{si};
                for mi = 1:numel(metric_keys)
                    metric_key = metric_keys{mi};
                    display_source = ExternalCollectorDispatcher.resolve_effective_source( ...
                        monitor_series, source, metric_key);
                    trace_label = sprintf('%s %s', ...
                        ExternalCollectorDispatcher.source_label(display_source), ...
                        ExternalCollectorDispatcher.metric_trace_label(metric_key));
                    trace = ExternalCollectorDispatcher.make_trace( ...
                        monitor_series, analysis, params, source, metric_key, trace_label);
                    if ~isempty(trace)
                        traces(end + 1) = trace; %#ok<AGROW>
                    end
                end
            end
        end

        function trace = make_trace(monitor_series, analysis, params, source, metric_key, label)
            trace = struct([]);
            values = ExternalCollectorDispatcher.source_metric_vector(monitor_series, source, metric_key);
            if isempty(values) || ~any(isfinite(values))
                return;
            end

            x = ExternalCollectorDispatcher.resolve_trace_timebase(monitor_series, values, analysis, params);
            if isempty(x) || numel(x) ~= numel(values)
                return;
            end

            display_source = ExternalCollectorDispatcher.resolve_effective_source( ...
                monitor_series, source, metric_key);
            style = ExternalCollectorDispatcher.source_plot_style(display_source, metric_key);
            trace = struct( ...
                'source', display_source, ...
                'metric_key', metric_key, ...
                'label', char(string(label)), ...
                'x', reshape(double(x), 1, []), ...
                'y', reshape(double(values), 1, []), ...
                'color_rgb', style.rgb, ...
                'color_hex', style.hex, ...
                'line_style', style.line_style, ...
                'plotly_dash', style.plotly_dash, ...
                'marker', style.marker, ...
                'plotly_marker', style.plotly_marker);
        end

        function x = resolve_trace_timebase(monitor_series, values, analysis, params)
            n = numel(values);
            x = zeros(1, 0);
            if n <= 0
                return;
            end

            candidates = {'elapsed_wall_time', 't'};
            for i = 1:numel(candidates)
                key = candidates{i};
                if isfield(monitor_series, key) && isnumeric(monitor_series.(key))
                    candidate = reshape(double(monitor_series.(key)), 1, []);
                    if numel(candidate) == n
                        x = candidate;
                        return;
                    end
                end
            end

            if isstruct(analysis) && isfield(analysis, 'time_vec') && isnumeric(analysis.time_vec)
                candidate = reshape(double(analysis.time_vec), 1, []);
                if numel(candidate) == n
                    x = candidate;
                    return;
                end
            end

            span = ExternalCollectorDispatcher.resolve_run_span(analysis, params);
            if n == 1
                x = 0;
            elseif isfinite(span) && span > 0
                x = linspace(0, span, n);
            else
                x = 0:(n - 1);
            end
        end

        function span = resolve_run_span(analysis, params)
            span = NaN;
            if isstruct(analysis) && isfield(analysis, 'time_vec') && isnumeric(analysis.time_vec)
                values = double(analysis.time_vec(:));
                values = values(isfinite(values));
                if ~isempty(values)
                    span = max(values) - min(values);
                    if span <= 0
                        span = max(values);
                    end
                end
            end
            if (~isfinite(span) || span <= 0) && isstruct(params)
                if isfield(params, 'Tfinal') && isnumeric(params.Tfinal) && isscalar(params.Tfinal)
                    span = double(params.Tfinal);
                elseif isfield(params, 't_final') && isnumeric(params.t_final) && isscalar(params.t_final)
                    span = double(params.t_final);
                end
            end
            if ~isfinite(span) || span <= 0
                span = NaN;
            end
        end

        function values = source_metric_vector(monitor_series, source, metric_key)
            values = zeros(1, 0);
            if strcmpi(char(string(source)), 'normalized')
                if isstruct(monitor_series) && isfield(monitor_series, metric_key) && isnumeric(monitor_series.(metric_key))
                    values = reshape(double(monitor_series.(metric_key)), 1, []);
                    if any(isfinite(values))
                        return;
                    end
                end
                fallback_sources = {'hwinfo', 'icue', 'matlab'};
                for i = 1:numel(fallback_sources)
                    candidate = ExternalCollectorDispatcher.source_metric_vector(monitor_series, fallback_sources{i}, metric_key);
                    if ~isempty(candidate) && any(isfinite(candidate))
                        values = candidate;
                        return;
                    end
                end
                return;
            end
            if ~isfield(monitor_series, 'collector_series') || ~isstruct(monitor_series.collector_series)
                return;
            end
            if ~isfield(monitor_series.collector_series, source) || ...
                    ~isstruct(monitor_series.collector_series.(source))
                return;
            end
            source_struct = monitor_series.collector_series.(source);
            if ~isfield(source_struct, metric_key) || ~isnumeric(source_struct.(metric_key))
                return;
            end
            values = reshape(double(source_struct.(metric_key)), 1, []);
        end

        function source = resolve_effective_source(monitor_series, source, metric_key)
            source = lower(char(string(source)));
            if ~strcmpi(source, 'normalized')
                return;
            end

            fallback_sources = {'hwinfo', 'icue', 'matlab'};
            for i = 1:numel(fallback_sources)
                candidate = fallback_sources{i};
                if ExternalCollectorDispatcher.has_finite_source_metric(monitor_series, candidate, metric_key)
                    source = candidate;
                    return;
                end
            end
        end

        function traces = empty_trace_array()
            traces = repmat(struct( ...
                'source', '', ...
                'metric_key', '', ...
                'label', '', ...
                'x', zeros(1, 0), ...
                'y', zeros(1, 0), ...
                'color_rgb', [0, 0, 0], ...
                'color_hex', '', ...
                'line_style', '-', ...
                'plotly_dash', 'solid', ...
                'marker', 'none', ...
                'plotly_marker', 'circle'), 1, 0);
        end

        function catalog = empty_metric_catalog()
            catalog = repmat(struct( ...
                'id', '', ...
                'display_name', '', ...
                'unit', '', ...
                'source', '', ...
                'metric_key', '', ...
                'default_title', '', ...
                'default_xlabel', 'Time (s)', ...
                'default_ylabel', '', ...
                'raw_header', '', ...
                'column_index', 0, ...
                'origin', '', ...
                'table_column', '', ...
                'meaning', ''), 1, 0);
        end

        function catalog = normalize_metric_catalog_array(raw_catalog)
            catalog = ExternalCollectorDispatcher.empty_metric_catalog();
            if isempty(raw_catalog)
                return;
            end
            if iscell(raw_catalog)
                items = raw_catalog;
            else
                items = num2cell(raw_catalog);
            end
            seen = containers.Map('KeyType', 'char', 'ValueType', 'logical');
            for i = 1:numel(items)
                item = items{i};
                if ~isstruct(item)
                    continue;
                end
                if ~all(isfield(item, {'id', 'display_name', 'source', 'metric_key'}))
                    continue;
                end
                item_id = char(string(item.id));
                if isempty(item_id) || isKey(seen, item_id)
                    continue;
                end
                seen(item_id) = true;
                catalog(end + 1) = struct( ... %#ok<AGROW>
                    'id', item_id, ...
                    'display_name', char(string(item.display_name)), ...
                    'unit', char(string(ExternalCollectorDispatcher.pick_struct_text(item, 'unit', ''))), ...
                    'source', lower(char(string(item.source))), ...
                    'metric_key', lower(char(string(item.metric_key))), ...
                    'default_title', char(string(ExternalCollectorDispatcher.pick_struct_text(item, 'default_title', item.display_name))), ...
                    'default_xlabel', char(string(ExternalCollectorDispatcher.pick_struct_text(item, 'default_xlabel', 'Time (s)'))), ...
                    'default_ylabel', char(string(ExternalCollectorDispatcher.pick_struct_text(item, 'default_ylabel', item.display_name))), ...
                    'raw_header', char(string(ExternalCollectorDispatcher.pick_struct_text(item, 'raw_header', ''))), ...
                    'column_index', double(ExternalCollectorDispatcher.pick_struct_number(item, 'column_index', 0)), ...
                    'origin', char(string(ExternalCollectorDispatcher.pick_struct_text(item, 'origin', ''))), ...
                    'table_column', char(string(ExternalCollectorDispatcher.pick_struct_text(item, 'table_column', ''))), ...
                    'meaning', char(string(ExternalCollectorDispatcher.pick_struct_text(item, 'meaning', ''))));
            end
        end

        function catalog = derive_metric_catalog_from_series(monitor_series)
            catalog = ExternalCollectorDispatcher.empty_metric_catalog();
            metric_meta = ExternalCollectorDispatcher.coverage_catalog();
            sources = {'hwinfo', 'icue', 'matlab'};
            source_labels = struct('hwinfo', 'HWiNFO', 'icue', 'iCUE', 'matlab', 'MATLAB');
            for si = 1:numel(sources)
                source = sources{si};
                for mi = 1:numel(metric_meta)
                    key = metric_meta(mi).metric_key;
                    if ~ExternalCollectorDispatcher.has_finite_source_metric(monitor_series, source, key)
                        continue;
                    end
                    display = sprintf('%s | %s', source_labels.(source), metric_meta(mi).raw_metric_name);
                    unit = ExternalCollectorDispatcher.metric_unit(key);
                    if ~isempty(unit)
                        display = sprintf('%s (%s)', display, unit);
                    end
                    catalog(end + 1) = struct( ... %#ok<AGROW>
                        'id', sprintf('%s__%s', source, key), ...
                        'display_name', display, ...
                        'unit', unit, ...
                        'source', source, ...
                        'metric_key', key, ...
                        'default_title', metric_meta(mi).raw_metric_name, ...
                        'default_xlabel', 'Time (s)', ...
                        'default_ylabel', ExternalCollectorDispatcher.metric_ylabel(metric_meta(mi).raw_metric_name, unit), ...
                        'raw_header', '', ...
                        'column_index', 0, ...
                        'origin', 'normalized_series', ...
                        'table_column', matlab.lang.makeValidName(sprintf('%s_%s', source, key)), ...
                        'meaning', metric_meta(mi).raw_metric_name);
                end
            end
        end

        function values = overlay_metrics(monitor_series)
            supported_metrics = {'cpu_proxy', 'gpu_series', 'memory_series', 'cpu_temp_c', 'power_w', 'system_power_w'};
            values = supported_metrics;
            if isstruct(monitor_series) && isfield(monitor_series, 'overlay_metrics') && ...
                    ~isempty(monitor_series.overlay_metrics)
                values = ExternalCollectorDispatcher.normalize_cellstr(monitor_series.overlay_metrics);
                values = values(cellfun(@(v) any(strcmpi(supported_metrics, v)), values));
                if isempty(values)
                    values = {};
                end
            end
        end

        function label = source_label(source)
            switch lower(char(string(source)))
                case 'hwinfo'
                    label = 'HWiNFO';
                case 'icue'
                    label = 'iCUE';
                case 'normalized'
                    label = 'Telemetry';
                otherwise
                    label = 'MATLAB';
            end
        end

        function label = metric_trace_label(metric_key)
            switch lower(char(string(metric_key)))
                case 'fan_rpm'
                    label = 'Fan';
                case 'pump_rpm'
                    label = 'Pump';
                case 'coolant_temp_c'
                    label = 'Coolant';
                case 'device_battery_level'
                    label = 'Battery';
                otherwise
                    label = ExternalCollectorDispatcher.humanize_metric_name(metric_key);
            end
        end

        function [marker, plotly_marker] = metric_marker(metric_key)
            switch lower(char(string(metric_key)))
                case 'fan_rpm'
                    marker = 'o';
                    plotly_marker = 'circle';
                case 'pump_rpm'
                    marker = 's';
                    plotly_marker = 'square';
                case 'coolant_temp_c'
                    marker = 'd';
                    plotly_marker = 'diamond';
                case 'device_battery_level'
                    marker = '^';
                    plotly_marker = 'triangle-up';
                otherwise
                    marker = 'none';
                    plotly_marker = 'circle';
            end
        end

        function tf = source_supports_metric(monitor_series, coverage_domains, source, metric_key, domain)
            tf = ExternalCollectorDispatcher.has_finite_source_metric(monitor_series, source, metric_key);
            if tf
                return;
            end
            domains = {};
            if isstruct(coverage_domains) && isfield(coverage_domains, source)
                domains = ExternalCollectorDispatcher.normalize_cellstr(coverage_domains.(source));
            end
            tf = any(strcmpi(domains, domain));
        end

        function tf = has_finite_source_metric(monitor_series, source, metric_key)
            tf = false;
            if ~isfield(monitor_series, 'collector_series') || ~isstruct(monitor_series.collector_series)
                return;
            end
            if ~isfield(monitor_series.collector_series, source) || ...
                    ~isstruct(monitor_series.collector_series.(source)) || ...
                    ~isfield(monitor_series.collector_series.(source), metric_key)
                return;
            end
            values = monitor_series.collector_series.(source).(metric_key);
            tf = isnumeric(values) && any(isfinite(values));
        end

        function tf = has_finite_metric(monitor_series, source, metric_key)
            tf = false;
            if strcmpi(char(string(source)), 'normalized')
                values = ExternalCollectorDispatcher.source_metric_vector(monitor_series, source, metric_key);
                tf = ~isempty(values) && any(isfinite(values));
                return;
            end
            tf = ExternalCollectorDispatcher.has_finite_source_metric(monitor_series, source, metric_key);
        end

        function catalog = curated_metric_catalog(raw_catalog, monitor_series)
            specs = ExternalCollectorDispatcher.curated_metric_specs();
            catalog = ExternalCollectorDispatcher.empty_metric_catalog();
            if nargin < 1 || isempty(raw_catalog)
                raw_catalog = ExternalCollectorDispatcher.empty_metric_catalog();
            end
            for i = 1:numel(specs)
                spec = specs(i);
                if ~ExternalCollectorDispatcher.has_finite_metric(monitor_series, spec.source, spec.metric_key)
                    continue;
                end
                catalog(end + 1) = struct( ... %#ok<AGROW>
                    'id', spec.id, ...
                    'display_name', spec.display_name, ...
                    'unit', spec.unit, ...
                    'source', spec.source, ...
                    'metric_key', spec.metric_key, ...
                    'default_title', spec.default_title, ...
                    'default_xlabel', 'Time (s)', ...
                    'default_ylabel', spec.default_ylabel, ...
                    'raw_header', '', ...
                    'column_index', 0, ...
                    'origin', spec.origin, ...
                    'table_column', spec.table_column, ...
                    'meaning', spec.meaning);
            end
            if isempty(catalog)
                catalog = raw_catalog;
            end
        end

        function table_out = build_curated_dataset_table(data_table)
            if nargin < 1 || isempty(data_table)
                table_out = table();
                return;
            end
            metadata_cols = { ...
                'workflow_kind', 'phase_id', 'stage_id', 'stage_label', 'stage_type', ...
                'stage_method', 'substage_id', 'substage_label', 'substage_type', ...
                'scenario_id', 'mesh_level', 'mesh_index', 'child_run_index', ...
                't', 'elapsed_wall_time', 'wall_clock_time', 'iteration', 'iter_rate', 'iter_completion_pct', ...
                'mesh_nx', 'mesh_ny', 'stage_wall_time_meta'};
            specs = ExternalCollectorDispatcher.curated_metric_specs();
            metric_cols = cell(1, 0);
            for i = 1:numel(specs)
                metric_cols{end + 1} = char(string(specs(i).table_column)); %#ok<AGROW>
            end
            desired_cols = unique([metadata_cols, metric_cols], 'stable');
            keep = intersect(desired_cols, data_table.Properties.VariableNames, 'stable');
            table_out = data_table(:, keep);

            if all(ismember({'cpu_power_w_hwinfo', 'gpu_power_w_hwinfo', 'memory_power_w_or_proxy'}, ...
                    table_out.Properties.VariableNames))
                table_out.component_power_w_total = table_out.cpu_power_w_hwinfo + ...
                    table_out.gpu_power_w_hwinfo + table_out.memory_power_w_or_proxy;
            end
            if all(ismember({'environmental_energy_wh_cum', 'iteration'}, table_out.Properties.VariableNames))
                denom = max(table_out.iteration, 1);
                table_out.energy_per_iteration_wh = table_out.environmental_energy_wh_cum ./ denom;
            end
            if all(ismember({'environmental_co2_g_cum', 'iteration'}, table_out.Properties.VariableNames))
                denom = max(table_out.iteration, 1);
                table_out.co2e_per_iteration_g = table_out.environmental_co2_g_cum ./ denom;
            end
            if all(ismember({'environmental_energy_wh_cum', 't'}, table_out.Properties.VariableNames))
                denom = max(table_out.t, eps);
                table_out.energy_per_sim_second_wh = table_out.environmental_energy_wh_cum ./ denom;
            end
            if all(ismember({'environmental_co2_g_cum', 't'}, table_out.Properties.VariableNames))
                denom = max(table_out.t, eps);
                table_out.co2e_per_sim_second_g = table_out.environmental_co2_g_cum ./ denom;
            end
            if all(ismember({'environmental_energy_wh_cum', 'iteration', 'mesh_nx', 'mesh_ny'}, table_out.Properties.VariableNames))
                cell_steps = max(table_out.iteration .* max(table_out.mesh_nx, 1) .* max(table_out.mesh_ny, 1), 1);
                table_out.energy_per_cell_step_wh = table_out.environmental_energy_wh_cum ./ cell_steps;
            end
            if all(ismember({'environmental_co2_g_cum', 'iteration', 'mesh_nx', 'mesh_ny'}, table_out.Properties.VariableNames))
                cell_steps = max(table_out.iteration .* max(table_out.mesh_nx, 1) .* max(table_out.mesh_ny, 1), 1);
                table_out.co2e_per_cell_step_g = table_out.environmental_co2_g_cum ./ cell_steps;
            end
        end

        function table_out = build_stage_summary_table(curated_table)
            if nargin < 1 || isempty(curated_table)
                table_out = table();
                return;
            end
            stage_ids = string(curated_table.stage_id);
            if isempty(stage_ids)
                table_out = table();
                return;
            end
            substage_ids = strings(size(stage_ids));
            if ismember('substage_id', curated_table.Properties.VariableNames)
                substage_ids = string(curated_table.substage_id);
            end
            group_keys = stage_ids + "||" + substage_ids;
            [groups, ~, group_idx] = unique(group_keys, 'stable');
            rows = repmat(struct( ...
                'stage_id', "", ...
                'stage_label', "", ...
                'stage_type', "", ...
                'stage_method', "", ...
                'substage_id', "", ...
                'substage_label', "", ...
                'substage_type', "", ...
                'scenario_id', "", ...
                'mesh_level', NaN, ...
                'mesh_index', NaN, ...
                'child_run_index', NaN, ...
                'wall_time_s', NaN, ...
                'mean_total_power_w', NaN, ...
                'peak_total_power_w', NaN, ...
                'energy_wh_total', NaN, ...
                'co2_g_total', NaN, ...
                'mean_cpu_temp_c', NaN, ...
                'peak_cpu_temp_c', NaN, ...
                'mean_gpu_load_pct', NaN, ...
                'mean_memory_pct', NaN, ...
                'energy_per_iteration_wh', NaN, ...
                'energy_per_sim_second_wh', NaN, ...
                'energy_per_cell_step_wh', NaN, ...
                'carbon_intensity_gco2e_per_kwh', NaN, ...
                'electricity_scope', ""), numel(groups), 1);
            for gi = 1:numel(groups)
                mask = group_idx == gi;
                block = curated_table(mask, :);
                total_power = ExternalCollectorDispatcher.table_column_or_fallback(block, 'system_power_w', ...
                    ExternalCollectorDispatcher.table_sum_columns(block, {'cpu_power_w_hwinfo', 'gpu_power_w_hwinfo', 'memory_power_w_or_proxy'}));
                wall_time_s = ExternalCollectorDispatcher.stage_wall_time(block);
                energy_wh_total = ExternalCollectorDispatcher.stage_terminal_value(block, 'environmental_energy_wh_cum');
                co2_g_total = ExternalCollectorDispatcher.stage_terminal_value(block, 'environmental_co2_g_cum');
                energy_per_iteration_wh = NaN;
                if ismember('iteration', block.Properties.VariableNames)
                    iter_final = ExternalCollectorDispatcher.stage_terminal_value(block, 'iteration');
                    if isfinite(iter_final) && iter_final > 0 && isfinite(energy_wh_total)
                        energy_per_iteration_wh = energy_wh_total / iter_final;
                    end
                end
                energy_per_sim_second_wh = NaN;
                if ismember('t', block.Properties.VariableNames)
                    sim_final = ExternalCollectorDispatcher.stage_terminal_value(block, 't');
                    if isfinite(sim_final) && sim_final > 0 && isfinite(energy_wh_total)
                        energy_per_sim_second_wh = energy_wh_total / sim_final;
                    end
                end
                energy_per_cell_step_wh = NaN;
                if all(ismember({'iteration', 'mesh_nx', 'mesh_ny'}, block.Properties.VariableNames))
                    iter_final = ExternalCollectorDispatcher.stage_terminal_value(block, 'iteration');
                    nx = ExternalCollectorDispatcher.stage_terminal_value(block, 'mesh_nx');
                    ny = ExternalCollectorDispatcher.stage_terminal_value(block, 'mesh_ny');
                    denom = iter_final * max(nx, 1) * max(ny, 1);
                    if isfinite(denom) && denom > 0 && isfinite(energy_wh_total)
                        energy_per_cell_step_wh = energy_wh_total / denom;
                    end
                end
                env_model = ExternalCollectorDispatcher.resolve_environmental_model(block);
                rows(gi) = struct( ...
                    'stage_id', string(ExternalCollectorDispatcher.first_text_value(block, 'stage_id')), ...
                    'stage_label', string(ExternalCollectorDispatcher.first_text_value(block, 'stage_label')), ...
                    'stage_type', string(ExternalCollectorDispatcher.first_text_value(block, 'stage_type')), ...
                    'stage_method', string(ExternalCollectorDispatcher.first_text_value(block, 'stage_method')), ...
                    'substage_id', string(ExternalCollectorDispatcher.first_text_value(block, 'substage_id')), ...
                    'substage_label', string(ExternalCollectorDispatcher.first_text_value(block, 'substage_label')), ...
                    'substage_type', string(ExternalCollectorDispatcher.first_text_value(block, 'substage_type')), ...
                    'scenario_id', string(ExternalCollectorDispatcher.first_text_value(block, 'scenario_id')), ...
                    'mesh_level', ExternalCollectorDispatcher.stage_terminal_value(block, 'mesh_level'), ...
                    'mesh_index', ExternalCollectorDispatcher.stage_terminal_value(block, 'mesh_index'), ...
                    'child_run_index', ExternalCollectorDispatcher.stage_terminal_value(block, 'child_run_index'), ...
                    'wall_time_s', wall_time_s, ...
                    'mean_total_power_w', ExternalCollectorDispatcher.nanmean_safe(total_power), ...
                    'peak_total_power_w', ExternalCollectorDispatcher.nanmax_safe(total_power), ...
                    'energy_wh_total', energy_wh_total, ...
                    'co2_g_total', co2_g_total, ...
                    'mean_cpu_temp_c', ExternalCollectorDispatcher.nanmean_safe(ExternalCollectorDispatcher.table_column(block, 'cpu_temp_c')), ...
                    'peak_cpu_temp_c', ExternalCollectorDispatcher.nanmax_safe(ExternalCollectorDispatcher.table_column(block, 'cpu_temp_c')), ...
                    'mean_gpu_load_pct', ExternalCollectorDispatcher.nanmean_safe(ExternalCollectorDispatcher.table_column(block, 'gpu_series')), ...
                    'mean_memory_pct', ExternalCollectorDispatcher.nanmean_safe(ExternalCollectorDispatcher.table_column(block, 'memory_series')), ...
                    'energy_per_iteration_wh', energy_per_iteration_wh, ...
                    'energy_per_sim_second_wh', energy_per_sim_second_wh, ...
                    'energy_per_cell_step_wh', energy_per_cell_step_wh, ...
                    'carbon_intensity_gco2e_per_kwh', ExternalCollectorDispatcher.environmental_factor_g_per_wh(env_model) * 1000, ...
                    'electricity_scope', string(ExternalCollectorDispatcher.pick_struct_text(env_model, 'electricity_scope', 'consumed_location_based')));
            end
            table_out = struct2table(rows);
        end

        function table_out = build_metric_guide_table()
            specs = ExternalCollectorDispatcher.curated_metric_specs();
            rows = repmat(struct( ...
                'metric_id', "", ...
                'display_name', "", ...
                'table_column', "", ...
                'unit', "", ...
                'source', "", ...
                'meaning', ""), numel(specs), 1);
            for i = 1:numel(specs)
                rows(i) = struct( ...
                    'metric_id', string(specs(i).id), ...
                    'display_name', string(specs(i).display_name), ...
                    'table_column', string(specs(i).table_column), ...
                    'unit', string(specs(i).unit), ...
                    'source', string(specs(i).source), ...
                    'meaning', string(specs(i).meaning));
            end
            table_out = struct2table(rows);
        end

        function table_out = build_dataset_table(monitor_series)
            t = ExternalCollectorDispatcher.series_column(monitor_series, 't', []);
            if isempty(t)
                table_out = table();
                return;
            end
            n = numel(t);
            table_out = table( ...
                ExternalCollectorDispatcher.text_series_column(monitor_series, {'workflow_kind_series'}, ...
                    ExternalCollectorDispatcher.pick_text_field(monitor_series, 'workflow_kind', ''), n), ...
                ExternalCollectorDispatcher.text_series_column(monitor_series, {'workflow_phase_id_series'}, ...
                    ExternalCollectorDispatcher.pick_text_field(monitor_series, 'workflow_phase_id', ''), n), ...
                ExternalCollectorDispatcher.text_series_column(monitor_series, {'workflow_stage_id_series'}, ...
                    ExternalCollectorDispatcher.pick_text_field(monitor_series, 'workflow_stage_id', 'single_run'), n), ...
                ExternalCollectorDispatcher.text_series_column(monitor_series, {'workflow_stage_label_series'}, ...
                    ExternalCollectorDispatcher.pick_text_field(monitor_series, 'workflow_stage_label', 'Single Run'), n), ...
                ExternalCollectorDispatcher.text_series_column(monitor_series, {'workflow_stage_type_series'}, ...
                    ExternalCollectorDispatcher.pick_text_field(monitor_series, 'workflow_stage_type', ''), n), ...
                ExternalCollectorDispatcher.text_series_column(monitor_series, {'workflow_method_series'}, ...
                    ExternalCollectorDispatcher.pick_text_field(monitor_series, 'workflow_method', ''), n), ...
                ExternalCollectorDispatcher.text_series_column(monitor_series, {'workflow_substage_id_series'}, '', n), ...
                ExternalCollectorDispatcher.text_series_column(monitor_series, {'workflow_substage_label_series'}, '', n), ...
                ExternalCollectorDispatcher.text_series_column(monitor_series, {'workflow_substage_type_series'}, '', n), ...
                ExternalCollectorDispatcher.text_series_column(monitor_series, {'workflow_scenario_id_series'}, ...
                    ExternalCollectorDispatcher.pick_text_field(monitor_series, 'workflow_scenario_id', ''), n), ...
                ExternalCollectorDispatcher.numeric_series_column(monitor_series, {'workflow_mesh_level_series'}, NaN, n), ...
                ExternalCollectorDispatcher.numeric_series_column(monitor_series, {'workflow_mesh_index_series'}, NaN, n), ...
                ExternalCollectorDispatcher.numeric_series_column(monitor_series, {'workflow_child_run_index_series'}, NaN, n), ...
                t, ...
                'VariableNames', {'workflow_kind', 'phase_id', 'stage_id', 'stage_label', 'stage_type', ...
                    'stage_method', 'substage_id', 'substage_label', 'substage_type', ...
                    'scenario_id', 'mesh_level', 'mesh_index', 'child_run_index', 't'});
            table_out.elapsed_wall_time = ExternalCollectorDispatcher.series_column(monitor_series, 'elapsed_wall_time', nan(n, 1));
            table_out.wall_clock_time = ExternalCollectorDispatcher.series_column(monitor_series, 'wall_clock_time', nan(n, 1));
            table_out.iteration = ExternalCollectorDispatcher.series_column(monitor_series, 'iters', nan(n, 1));
            table_out.iter_rate = ExternalCollectorDispatcher.series_column(monitor_series, 'iter_rate', nan(n, 1));
            table_out.iter_completion_pct = ExternalCollectorDispatcher.series_column(monitor_series, 'iter_completion_pct', nan(n, 1));
            table_out.mesh_nx = ExternalCollectorDispatcher.numeric_series_column(monitor_series, ...
                {'workflow_child_mesh_nx_series'}, ExternalCollectorDispatcher.pick_struct_number(monitor_series, 'workflow_child_mesh_nx', NaN), n);
            table_out.mesh_ny = ExternalCollectorDispatcher.numeric_series_column(monitor_series, ...
                {'workflow_child_mesh_ny_series'}, ExternalCollectorDispatcher.pick_struct_number(monitor_series, 'workflow_child_mesh_ny', NaN), n);

            unified_keys = {'cpu_proxy', 'gpu_series', 'memory_series', 'cpu_temp_c', 'power_w', ...
                'cpu_voltage_v', 'gpu_voltage_v', 'memory_voltage_v', ...
                'cpu_power_w_hwinfo', 'gpu_power_w_hwinfo', 'memory_power_w_or_proxy', ...
                'system_power_w', 'environmental_energy_wh_cum', 'environmental_co2_g_cum', ...
                'fan_rpm', 'pump_rpm', 'coolant_temp_c', 'device_battery_level'};
            for i = 1:numel(unified_keys)
                key = unified_keys{i};
                table_out.(key) = ExternalCollectorDispatcher.series_column(monitor_series, key, nan(n, 1));
            end

            source_metric_map = ExternalCollectorDispatcher.source_metric_map(monitor_series);
            sources = fieldnames(source_metric_map);
            for si = 1:numel(sources)
                source = sources{si};
                keys = source_metric_map.(source);
                for i = 1:numel(keys)
                    key = keys{i};
                    var_name = matlab.lang.makeValidName(sprintf('%s_%s', source, key));
                    table_out.(var_name) = ExternalCollectorDispatcher.source_series_column(monitor_series, source, key, n);
                end
            end
            table_out.stage_wall_time_meta = ExternalCollectorDispatcher.numeric_series_column(monitor_series, ...
                {'workflow_stage_wall_time_series'}, NaN, n);
            collector_status = ExternalCollectorDispatcher.pick_struct_field(monitor_series, 'collector_status');
            table_out.hwinfo_status = repmat(string(ExternalCollectorDispatcher.pick_struct_text(collector_status, 'hwinfo', '')), n, 1);
            table_out.hwinfo_transport = repmat(string(ExternalCollectorDispatcher.pick_text_field(monitor_series, 'hwinfo_transport', 'none')), n, 1);
            table_out = ExternalCollectorDispatcher.ensure_environmental_impact_columns(table_out, monitor_series);
        end

        function [data_table, raw_csv_path, boundary_table] = build_dataset_table_from_phase_csv(paths, monitor_series, output_csv_path)
            data_table = table();
            raw_csv_path = ExternalCollectorDispatcher.resolve_phase_raw_hwinfo_csv_path(paths, monitor_series);
            boundary_csv_path = ExternalCollectorDispatcher.resolve_phase_boundary_csv_path(paths, monitor_series);
            boundary_table = table();
            if isempty(raw_csv_path) || exist(raw_csv_path, 'file') ~= 2
                error('ExternalCollectorDispatcher:PhaseRawCSVUnavailable', ...
                    'Phase raw HWiNFO CSV was not found: %s', raw_csv_path);
            end
            if isempty(boundary_csv_path) || exist(boundary_csv_path, 'file') ~= 2
                error('ExternalCollectorDispatcher:PhaseBoundaryCSVUnavailable', ...
                    'Phase boundary CSV was not found: %s', boundary_csv_path);
            end

            host_timezone = ExternalCollectorDispatcher.resolve_phase_host_timezone(monitor_series);
            normalize_response = HWiNFOProCLIController.normalize_csv_dataset(struct( ...
                'raw_csv_path', raw_csv_path, ...
                'boundary_csv_path', boundary_csv_path, ...
                'output_csv_path', output_csv_path, ...
                'timezone_name', host_timezone));
            if ~(isstruct(normalize_response) && isfield(normalize_response, 'ok') && normalize_response.ok)
                error('ExternalCollectorDispatcher:PhaseCSVNormalizeFailed', ...
                    'HWiNFO Pro CSV normalization failed: %s', ...
                    ExternalCollectorDispatcher.pick_struct_text(normalize_response, 'message', 'unknown failure'));
            end

            data_table = readtable(output_csv_path);
            data_table = ExternalCollectorDispatcher.ensure_environmental_impact_columns(data_table, monitor_series);
            boundary_table = readtable(boundary_csv_path);
        end

        function csv_path = phase_dataset_csv_path(collector_run_dir, compact_v3)
            if compact_v3
                csv_path = fullfile(collector_run_dir, 'Telemetry_Raw.csv');
            else
                csv_path = fullfile(collector_run_dir, 'collector_dataset.csv');
            end
        end

        function csv_path = phase_stage_boundaries_csv_path(paths, collector_run_dir, compact_v3)
            csv_path = ExternalCollectorDispatcher.pick_struct_text(paths, 'stage_boundaries_csv_path', '');
            if ~isempty(csv_path)
                return;
            end
            if compact_v3
                csv_path = fullfile(collector_run_dir, 'Telemetry_Stage_Boundaries.csv');
            else
                csv_path = fullfile(collector_run_dir, 'collector_stage_boundaries.csv');
            end
        end

        function tf = is_phase_csv_first_mode(monitor_series)
            if ~strcmpi(ExternalCollectorDispatcher.pick_text_field(monitor_series, 'hwinfo_control_mode', ''), 'pro_cli_csv')
                tf = false;
                return;
            end
            if isfield(monitor_series, 'telemetry_enabled')
                tf = logical(monitor_series.telemetry_enabled);
                return;
            end
            tf = strcmpi(ExternalCollectorDispatcher.pick_text_field( ...
                ExternalCollectorDispatcher.pick_struct_field(monitor_series, 'collector_status'), 'hwinfo', ''), 'pro_cli_csv');
        end

        function csv_path = resolve_phase_raw_hwinfo_csv_path(paths, monitor_series)
            csv_path = ExternalCollectorDispatcher.pick_struct_text(paths, 'raw_hwinfo_csv_path', '');
            if ~isempty(csv_path)
                return;
            end
            probe_hwinfo = ExternalCollectorDispatcher.pick_struct_field( ...
                ExternalCollectorDispatcher.pick_struct_field(monitor_series, 'collector_probe_details'), 'hwinfo');
            csv_path = ExternalCollectorDispatcher.pick_struct_text(probe_hwinfo, 'raw_csv_path', '');
            if isempty(csv_path)
                csv_path = ExternalCollectorDispatcher.pick_struct_text( ...
                    ExternalCollectorDispatcher.pick_struct_field(monitor_series, 'raw_log_paths'), 'hwinfo', '');
            end
        end

        function csv_path = resolve_phase_boundary_csv_path(paths, monitor_series)
            csv_path = ExternalCollectorDispatcher.pick_struct_text(paths, 'stage_boundaries_csv_path', '');
            if ~isempty(csv_path)
                return;
            end
            sustainability_root = ExternalCollectorDispatcher.pick_struct_text(paths, 'sustainability_collectors', '');
            if isempty(sustainability_root)
                sustainability_root = ExternalCollectorDispatcher.pick_struct_text(paths, 'sustainability', '');
            end
            if ~isempty(sustainability_root)
                csv_path = fullfile(sustainability_root, 'Telemetry_Stage_Boundaries.csv');
                return;
            end
            metrics_root = ExternalCollectorDispatcher.pick_struct_text(paths, 'metrics_root', '');
            if ~isempty(metrics_root)
                csv_path = fullfile(metrics_root, 'Telemetry_Stage_Boundaries.csv');
                return;
            end
            probe_hwinfo = ExternalCollectorDispatcher.pick_struct_field( ...
                ExternalCollectorDispatcher.pick_struct_field(monitor_series, 'collector_probe_details'), 'hwinfo');
            csv_path = ExternalCollectorDispatcher.pick_struct_text(probe_hwinfo, 'boundary_csv_path', '');
        end

        function timezone_name = resolve_phase_host_timezone(monitor_series)
            timezone_name = ExternalCollectorDispatcher.pick_text_field(monitor_series, 'host_timezone', '');
            if isempty(timezone_name)
                probe_hwinfo = ExternalCollectorDispatcher.pick_struct_field( ...
                    ExternalCollectorDispatcher.pick_struct_field(monitor_series, 'collector_probe_details'), 'hwinfo');
                timezone_name = ExternalCollectorDispatcher.pick_struct_text(probe_hwinfo, 'host_timezone', '');
            end
            if isempty(timezone_name)
                timezone_name = 'UTC';
            end
        end

        function monitor_series = monitor_series_from_dataset_table(data_table, template_series)
            if nargin < 2 || ~isstruct(template_series)
                template_series = struct();
            end
            monitor_series = ExternalCollectorDispatcher.normalize_collector_payload(template_series);
            if isempty(data_table) || ~istable(data_table) || ~ismember('t', data_table.Properties.VariableNames)
                return;
            end
            if isempty(fieldnames(monitor_series))
                monitor_series = ExternalCollectorDispatcher.empty_sample();
            end
            monitor_series.t = reshape(double(data_table.t), 1, []);
            if ismember('elapsed_wall_time', data_table.Properties.VariableNames)
                monitor_series.elapsed_wall_time = reshape(double(data_table.elapsed_wall_time), 1, []);
            else
                monitor_series.elapsed_wall_time = monitor_series.t;
            end
            if ismember('wall_clock_time', data_table.Properties.VariableNames)
                monitor_series.wall_clock_time = reshape(double(data_table.wall_clock_time), 1, []);
            end
            if ~isfield(monitor_series, 'collector_series') || ~isstruct(monitor_series.collector_series)
                monitor_series.collector_series = ExternalCollectorDispatcher.empty_sample().collector_series;
            end
            if ~isfield(monitor_series.collector_series, 'hwinfo') || ~isstruct(monitor_series.collector_series.hwinfo)
                monitor_series.collector_series.hwinfo = struct();
            end
            metric_keys = {'cpu_proxy', 'gpu_series', 'memory_series', 'cpu_temp_c', 'power_w', ...
                'cpu_voltage_v', 'gpu_voltage_v', 'memory_voltage_v', 'cpu_power_w_hwinfo', ...
                'gpu_power_w_hwinfo', 'memory_power_w_or_proxy', 'system_power_w', ...
                'environmental_energy_wh_cum', 'environmental_co2_g_cum', 'fan_rpm', ...
                'pump_rpm', 'coolant_temp_c', 'device_battery_level'};
            for i = 1:numel(metric_keys)
                key = metric_keys{i};
                if ~ismember(key, data_table.Properties.VariableNames)
                    continue;
                end
                values = reshape(double(data_table.(key)), 1, []);
                monitor_series.collector_series.hwinfo.(key) = values;
                monitor_series.(key) = values;
            end
            if ~isfield(monitor_series, 'collector_status') || ~isstruct(monitor_series.collector_status)
                monitor_series.collector_status = struct('hwinfo', 'disabled', 'icue', 'disabled');
            end
            monitor_series.collector_status.hwinfo = ExternalCollectorDispatcher.table_last_text(data_table, 'hwinfo_status', 'pro_cli_csv');
            monitor_series.hwinfo_transport = ExternalCollectorDispatcher.table_last_text(data_table, 'hwinfo_transport', 'csv');
            if ~isfield(monitor_series, 'collector_metric_catalog') || isempty(monitor_series.collector_metric_catalog)
                monitor_series.collector_metric_catalog = ExternalCollectorDispatcher.derive_metric_catalog_from_series(monitor_series);
            end
            monitor_series = ExternalCollectorDispatcher.normalize_collector_payload(monitor_series);
        end

        function csv_path = write_canonical_hwinfo_csv(paths, collector_run_dir, monitor_series, data_table)
            csv_path = '';
            target_path = ExternalCollectorDispatcher.pick_struct_text(paths, 'raw_hwinfo_csv_path', '');
            if isempty(target_path)
                target_path = fullfile(char(string(collector_run_dir)), 'HWiNFO_Telemetry.csv');
            end

            raw_table = ExternalCollectorDispatcher.build_canonical_hwinfo_table(monitor_series, data_table);
            if isempty(raw_table) || ~istable(raw_table) || height(raw_table) < 1
                return;
            end

            target_dir = fileparts(target_path);
            if ~isempty(target_dir) && exist(target_dir, 'dir') ~= 7
                mkdir(target_dir);
            end
            writetable(raw_table, target_path);
            csv_path = target_path;
        end

        function raw_table = build_canonical_hwinfo_table(monitor_series, data_table)
            raw_table = table();
            manifest = ExternalCollectorDispatcher.pick_struct_field(monitor_series, 'workflow_segment_manifest');
            [raw_table, ~] = ExternalCollectorDispatcher.merge_hwinfo_csv_manifest(manifest);
            if ~isempty(raw_table)
                return;
            end
            raw_table = ExternalCollectorDispatcher.build_hwinfo_table_from_dataset(data_table);
        end

        function [merged, boundary_table] = merge_hwinfo_csv_manifest(manifest)
            merged = table();
            boundary_table = table();
            if ~isstruct(manifest) || isempty(manifest)
                return;
            end
            seen_paths = strings(1, 0);
            time_offset = 0;
            gap = 1.0e-6;
            boundary_rows = repmat(struct( ...
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
                'child_run_index', NaN), 1, 0);
            for i = 1:numel(manifest)
                csv_path = string(ExternalCollectorDispatcher.pick_struct_text(manifest(i), 'raw_hwinfo_csv_path', ''));
                if strlength(strtrim(csv_path)) == 0 || any(strcmpi(seen_paths, csv_path)) || exist(csv_path, 'file') ~= 2
                    continue;
                end
                try
                    block = readtable(char(csv_path));
                catch
                    continue;
                end
                if isempty(block) || ~ismember('session_time_s', block.Properties.VariableNames)
                    continue;
                end
                session_time = double(block.session_time_s);
                if isempty(session_time)
                    continue;
                end
                session_time = session_time(:);
                session_time = session_time - session_time(1);
                if height(merged) > 0
                    session_time = session_time + time_offset + gap;
                end
                block.session_time_s = session_time;
                block.phase_id = repmat(string(ExternalCollectorDispatcher.pick_struct_text(manifest(i), 'phase_id', '')), height(block), 1);
                block.workflow_kind = repmat(string(ExternalCollectorDispatcher.pick_struct_text(manifest(i), 'workflow_kind', '')), height(block), 1);
                block.stage_id = repmat(string(ExternalCollectorDispatcher.pick_struct_text(manifest(i), 'stage_id', '')), height(block), 1);
                block.stage_label = repmat(string(ExternalCollectorDispatcher.pick_struct_text(manifest(i), 'stage_label', '')), height(block), 1);
                block.stage_type = repmat(string(ExternalCollectorDispatcher.pick_struct_text(manifest(i), 'stage_type', '')), height(block), 1);
                block.substage_id = repmat(string(ExternalCollectorDispatcher.pick_struct_text(manifest(i), 'substage_id', '')), height(block), 1);
                block.substage_label = repmat(string(ExternalCollectorDispatcher.pick_struct_text(manifest(i), 'substage_label', '')), height(block), 1);
                block.substage_type = repmat(string(ExternalCollectorDispatcher.pick_struct_text(manifest(i), 'substage_type', '')), height(block), 1);
                block.stage_method = repmat(string(ExternalCollectorDispatcher.pick_struct_text(manifest(i), 'stage_method', '')), height(block), 1);
                block.scenario_id = repmat(string(ExternalCollectorDispatcher.pick_struct_text(manifest(i), 'scenario_id', '')), height(block), 1);
                block.mesh_level = repmat(double(ExternalCollectorDispatcher.pick_struct_number(manifest(i), 'mesh_level', NaN)), height(block), 1);
                block.mesh_nx = repmat(double(ExternalCollectorDispatcher.pick_struct_number(manifest(i), 'mesh_nx', NaN)), height(block), 1);
                block.mesh_ny = repmat(double(ExternalCollectorDispatcher.pick_struct_number(manifest(i), 'mesh_ny', NaN)), height(block), 1);
                block.child_run_index = repmat(double(ExternalCollectorDispatcher.pick_struct_number(manifest(i), 'child_run_index', NaN)), height(block), 1);
                boundary_rows(end + 1) = ExternalCollectorDispatcher.boundary_row_from_manifest_block(manifest(i), block, 1, "start"); %#ok<AGROW>
                boundary_rows(end + 1) = ExternalCollectorDispatcher.boundary_row_from_manifest_block(manifest(i), block, height(block), "end"); %#ok<AGROW>
                if isempty(merged)
                    merged = block;
                else
                    merged = [merged; block]; %#ok<AGROW>
                end
                seen_paths(end + 1) = csv_path; %#ok<AGROW>
                time_offset = block.session_time_s(end);
            end
            if ~isempty(boundary_rows)
                boundary_table = struct2table(boundary_rows);
            end
        end

        function raw_table = build_hwinfo_table_from_dataset(data_table)
            raw_table = table();
            if isempty(data_table) || ~istable(data_table) || height(data_table) < 1
                return;
            end
            base_columns = {'t', 'wall_clock_time', 'hwinfo_status', 'hwinfo_transport', ...
                'cpu_proxy', 'gpu_series', 'memory_series', 'cpu_temp_c', 'system_power_w', ...
                'cpu_voltage_v', 'gpu_voltage_v', 'memory_voltage_v', 'cpu_power_w_hwinfo', ...
                'gpu_power_w_hwinfo', 'memory_power_w_or_proxy', 'environmental_energy_wh_cum', ...
                'environmental_co2_g_cum', 'fan_rpm', 'pump_rpm', 'coolant_temp_c', 'device_battery_level', ...
                'phase_id', 'workflow_kind', 'stage_id', 'stage_label', 'stage_type', ...
                'substage_id', 'substage_label', 'substage_type', 'stage_method', 'scenario_id', ...
                'mesh_level', 'mesh_nx', 'mesh_ny', 'child_run_index'};
            keep = intersect(base_columns, data_table.Properties.VariableNames, 'stable');
            if isempty(keep)
                return;
            end
            raw_table = data_table(:, keep);
            session_idx = find(strcmp(raw_table.Properties.VariableNames, 't'), 1, 'first');
            if ~isempty(session_idx)
                raw_table.Properties.VariableNames{session_idx} = 'session_time_s';
            end
            if ismember('wall_clock_time', raw_table.Properties.VariableNames)
                raw_table.timestamp_utc = ExternalCollectorDispatcher.posix_to_utc_string_column(raw_table.wall_clock_time);
                raw_table = movevars(raw_table, 'timestamp_utc', 'After', 'session_time_s');
                raw_table = removevars(raw_table, 'wall_clock_time');
            else
                raw_table.timestamp_utc = repmat("", height(raw_table), 1);
                raw_table = movevars(raw_table, 'timestamp_utc', 'After', 'session_time_s');
            end
            raw_table = movevars(raw_table, {'hwinfo_status', 'hwinfo_transport'}, 'After', 'timestamp_utc');
        end

        function table_out = build_stage_boundaries_table(monitor_series, data_table)
            table_out = table();
            manifest = ExternalCollectorDispatcher.pick_struct_field(monitor_series, 'workflow_segment_manifest');
            [~, manifest_boundaries] = ExternalCollectorDispatcher.merge_hwinfo_csv_manifest(manifest);
            if ~isempty(manifest_boundaries)
                table_out = manifest_boundaries;
                return;
            end
            if isempty(data_table) || ~istable(data_table) || height(data_table) < 1 || ...
                    ~ismember('t', data_table.Properties.VariableNames)
                return;
            end
            stage_id = ExternalCollectorDispatcher.table_text_column(data_table, 'stage_id');
            substage_id = ExternalCollectorDispatcher.table_text_column(data_table, 'substage_id');
            child_run = ExternalCollectorDispatcher.table_numeric_column(data_table, 'child_run_index');
            key = stage_id + "||" + substage_id + "||" + string(child_run);
            groups = zeros(height(data_table), 1);
            groups(1) = 1;
            for i = 2:height(data_table)
                groups(i) = groups(i - 1) + double(key(i) ~= key(i - 1));
            end
            rows = repmat(struct( ...
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
                'child_run_index', NaN), 1, 0);
            for gi = 1:max(groups)
                mask = groups == gi;
                block = data_table(mask, :);
                rows(end + 1) = ExternalCollectorDispatcher.boundary_row_from_block(block, 1, "start"); %#ok<AGROW>
                rows(end + 1) = ExternalCollectorDispatcher.boundary_row_from_block(block, height(block), "end"); %#ok<AGROW>
            end
            table_out = struct2table(rows);
        end

        function row = boundary_row_from_manifest_block(manifest_entry, block, row_index, boundary_event)
            timestamp_utc = "";
            if ismember('timestamp_utc', block.Properties.VariableNames)
                timestamp_utc = string(ExternalCollectorDispatcher.table_row_text(block, row_index, 'timestamp_utc', ''));
            elseif ismember('wall_clock_time', block.Properties.VariableNames)
                wall_clock = ExternalCollectorDispatcher.table_row_number(block, row_index, 'wall_clock_time', NaN);
                if isfinite(wall_clock)
                    timestamp_utc = string(ExternalCollectorDispatcher.posix_scalar_to_utc(wall_clock));
                end
            end
            row = struct( ...
                'session_time_s', ExternalCollectorDispatcher.table_row_number(block, row_index, 'session_time_s', NaN), ...
                'timestamp_utc', timestamp_utc, ...
                'boundary_event', string(boundary_event), ...
                'phase_id', string(ExternalCollectorDispatcher.pick_struct_text(manifest_entry, 'phase_id', '')), ...
                'workflow_kind', string(ExternalCollectorDispatcher.pick_struct_text(manifest_entry, 'workflow_kind', '')), ...
                'stage_id', string(ExternalCollectorDispatcher.pick_struct_text(manifest_entry, 'stage_id', '')), ...
                'stage_label', string(ExternalCollectorDispatcher.pick_struct_text(manifest_entry, 'stage_label', '')), ...
                'stage_type', string(ExternalCollectorDispatcher.pick_struct_text(manifest_entry, 'stage_type', '')), ...
                'substage_id', string(ExternalCollectorDispatcher.pick_struct_text(manifest_entry, 'substage_id', '')), ...
                'substage_label', string(ExternalCollectorDispatcher.pick_struct_text(manifest_entry, 'substage_label', '')), ...
                'substage_type', string(ExternalCollectorDispatcher.pick_struct_text(manifest_entry, 'substage_type', '')), ...
                'stage_method', string(ExternalCollectorDispatcher.pick_struct_text(manifest_entry, 'stage_method', '')), ...
                'scenario_id', string(ExternalCollectorDispatcher.pick_struct_text(manifest_entry, 'scenario_id', '')), ...
                'mesh_level', double(ExternalCollectorDispatcher.pick_struct_number(manifest_entry, 'mesh_level', NaN)), ...
                'mesh_nx', double(ExternalCollectorDispatcher.pick_struct_number(manifest_entry, 'mesh_nx', NaN)), ...
                'mesh_ny', double(ExternalCollectorDispatcher.pick_struct_number(manifest_entry, 'mesh_ny', NaN)), ...
                'child_run_index', double(ExternalCollectorDispatcher.pick_struct_number(manifest_entry, 'child_run_index', NaN)));
        end

        function row = boundary_row_from_block(block, row_index, boundary_event)
            timestamp_utc = "";
            wall_clock = ExternalCollectorDispatcher.table_row_number(block, row_index, 'wall_clock_time', NaN);
            if isfinite(wall_clock)
                timestamp_utc = string(ExternalCollectorDispatcher.posix_scalar_to_utc(wall_clock));
            end
            row = struct( ...
                'session_time_s', ExternalCollectorDispatcher.table_row_number(block, row_index, 't', NaN), ...
                'timestamp_utc', timestamp_utc, ...
                'boundary_event', string(boundary_event), ...
                'phase_id', string(ExternalCollectorDispatcher.table_row_text(block, row_index, 'phase_id', '')), ...
                'workflow_kind', string(ExternalCollectorDispatcher.table_row_text(block, row_index, 'workflow_kind', '')), ...
                'stage_id', string(ExternalCollectorDispatcher.table_row_text(block, row_index, 'stage_id', '')), ...
                'stage_label', string(ExternalCollectorDispatcher.table_row_text(block, row_index, 'stage_label', '')), ...
                'stage_type', string(ExternalCollectorDispatcher.table_row_text(block, row_index, 'stage_type', '')), ...
                'substage_id', string(ExternalCollectorDispatcher.table_row_text(block, row_index, 'substage_id', '')), ...
                'substage_label', string(ExternalCollectorDispatcher.table_row_text(block, row_index, 'substage_label', '')), ...
                'substage_type', string(ExternalCollectorDispatcher.table_row_text(block, row_index, 'substage_type', '')), ...
                'stage_method', string(ExternalCollectorDispatcher.table_row_text(block, row_index, 'stage_method', '')), ...
                'scenario_id', string(ExternalCollectorDispatcher.table_row_text(block, row_index, 'scenario_id', '')), ...
                'mesh_level', ExternalCollectorDispatcher.table_row_number(block, row_index, 'mesh_level', NaN), ...
                'mesh_nx', ExternalCollectorDispatcher.table_row_number(block, row_index, 'mesh_nx', NaN), ...
                'mesh_ny', ExternalCollectorDispatcher.table_row_number(block, row_index, 'mesh_ny', NaN), ...
                'child_run_index', ExternalCollectorDispatcher.table_row_number(block, row_index, 'child_run_index', NaN));
        end

        function utc_values = posix_to_utc_string_column(posix_values)
            utc_values = repmat("", numel(posix_values), 1);
            if isempty(posix_values)
                return;
            end
            for i = 1:numel(posix_values)
                utc_values(i) = string(ExternalCollectorDispatcher.posix_scalar_to_utc(posix_values(i)));
            end
        end

        function text = posix_scalar_to_utc(posix_value)
            text = '';
            if ~(isnumeric(posix_value) && isscalar(posix_value) && isfinite(posix_value))
                return;
            end
            try
                text = char(datetime(posix_value, 'ConvertFrom', 'posixtime', 'TimeZone', 'UTC', ...
                    'Format', 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z'''));
            catch
                text = '';
            end
        end

        function table_out = build_launch_session_table(session_id, run_index, metadata, run_config, parameters, monitor_series)
            table_out = ExternalCollectorDispatcher.build_dataset_table(monitor_series);
            if isempty(table_out)
                return;
            end

            n = height(table_out);
            run_id = ExternalCollectorDispatcher.pick_struct_text(metadata, 'run_id', '');
            if isempty(run_id)
                run_id = ExternalCollectorDispatcher.pick_struct_text(run_config, 'run_id', '');
            end
            method = ExternalCollectorDispatcher.pick_struct_text(run_config, 'method', '');
            mode = ExternalCollectorDispatcher.pick_struct_text(run_config, 'mode', '');
            ic_type = ExternalCollectorDispatcher.pick_struct_text(run_config, 'ic_type', '');
            bc_case = ExternalCollectorDispatcher.pick_struct_text(parameters, 'boundary_condition_case', '');
            workflow_kind = ExternalCollectorDispatcher.pick_text_field(monitor_series, 'workflow_kind', '');
            phase_id = ExternalCollectorDispatcher.pick_text_field(monitor_series, 'workflow_phase_id', '');
            hwinfo_transport = ExternalCollectorDispatcher.pick_text_field(monitor_series, 'hwinfo_transport', 'none');
            collector_status = ExternalCollectorDispatcher.pick_struct_field(monitor_series, 'collector_status');
            raw_log_paths = ExternalCollectorDispatcher.pick_struct_field(monitor_series, 'raw_log_paths');

            meta_table = table( ...
                repmat(string(session_id), n, 1), ...
                repmat(double(run_index), n, 1), ...
                repmat(string(run_id), n, 1), ...
                repmat(string(method), n, 1), ...
                repmat(string(mode), n, 1), ...
                repmat(string(workflow_kind), n, 1), ...
                repmat(string(phase_id), n, 1), ...
                repmat(string(ic_type), n, 1), ...
                repmat(string(bc_case), n, 1), ...
                repmat(double(ExternalCollectorDispatcher.pick_struct_number(parameters, 'Nx', NaN)), n, 1), ...
                repmat(double(ExternalCollectorDispatcher.pick_struct_number(parameters, 'Ny', NaN)), n, 1), ...
                repmat(double(ExternalCollectorDispatcher.pick_struct_number(parameters, 'dt', NaN)), n, 1), ...
                repmat(double(ExternalCollectorDispatcher.pick_struct_number(parameters, 'Tfinal', NaN)), n, 1), ...
                repmat(double(ExternalCollectorDispatcher.pick_struct_number(parameters, 'sample_interval', NaN)), n, 1), ...
                repmat(string(hwinfo_transport), n, 1), ...
                repmat(string(ExternalCollectorDispatcher.pick_struct_text(collector_status, 'hwinfo', '')), n, 1), ...
                repmat(string(ExternalCollectorDispatcher.pick_struct_text(collector_status, 'icue', '')), n, 1), ...
                repmat(string(ExternalCollectorDispatcher.pick_struct_text(raw_log_paths, 'hwinfo', '')), n, 1), ...
                repmat(string(ExternalCollectorDispatcher.pick_struct_text(raw_log_paths, 'icue', '')), n, 1), ...
                'VariableNames', { ...
                    'ui_launch_session_id', 'run_index', 'run_id', 'method', 'mode', ...
                    'workflow_kind_meta', 'phase_id_meta', ...
                    'ic_type', 'bc_case', 'Nx', 'Ny', 'dt', 'Tfinal', 'sample_interval', ...
                    'hwinfo_transport_meta', 'hwinfo_status_meta', 'icue_status_meta', ...
                    'hwinfo_raw_log_path', 'icue_raw_log_path'});
            table_out = [meta_table, table_out];
        end

        function col = series_column(monitor_series, field_name, fallback)
            if nargin < 3
                fallback = [];
            end
            if isfield(monitor_series, field_name) && isnumeric(monitor_series.(field_name))
                vec = reshape(double(monitor_series.(field_name)), [], 1);
                fallback_col = reshape(double(fallback), [], 1);
                target_n = numel(fallback_col);
                if target_n <= 0
                    col = vec;
                    return;
                end
                col = fallback_col;
                if isempty(vec)
                    return;
                end
                if numel(vec) == 1
                    col(:) = vec;
                    return;
                end
                m = min(target_n, numel(vec));
                col(1:m) = vec(1:m);
                return;
            end
            col = reshape(double(fallback), [], 1);
        end

        function col = numeric_series_column(monitor_series, field_names, fallback, n)
            if nargin < 4 || ~isfinite(n) || n < 1
                col = zeros(0, 1);
                return;
            end
            if nargin < 3
                fallback = NaN;
            end
            if ~iscell(field_names)
                field_names = {field_names};
            end
            col = nan(n, 1);
            for i = 1:numel(field_names)
                key = field_names{i};
                if isfield(monitor_series, key) && isnumeric(monitor_series.(key))
                    vec = reshape(double(monitor_series.(key)), [], 1);
                    if numel(vec) == 1
                        col(:) = vec;
                        return;
                    end
                    m = min(n, numel(vec));
                    col(1:m) = vec(1:m);
                    return;
                end
            end
            col(:) = double(fallback);
        end

        function col = text_series_column(monitor_series, field_names, fallback, n)
            if nargin < 4 || ~isfinite(n) || n < 1
                col = strings(0, 1);
                return;
            end
            if nargin < 3
                fallback = "";
            end
            if ~iscell(field_names)
                field_names = {field_names};
            end
            col = repmat(string(fallback), n, 1);
            for i = 1:numel(field_names)
                key = field_names{i};
                if ~isfield(monitor_series, key) || isempty(monitor_series.(key))
                    continue;
                end
                candidate = monitor_series.(key);
                if ischar(candidate)
                    col(:) = string(candidate);
                    return;
                end
                if isstring(candidate)
                    candidate = reshape(string(candidate), [], 1);
                    if numel(candidate) == 1
                        col(:) = candidate;
                    else
                        m = min(n, numel(candidate));
                        col(1:m) = candidate(1:m);
                        if m < n && m >= 1
                            col(m + 1:end) = candidate(m);
                        end
                    end
                    return;
                end
                if iscell(candidate)
                    candidate = string(candidate(:));
                    m = min(n, numel(candidate));
                    col(1:m) = candidate(1:m);
                    if m < n && m >= 1
                        col(m + 1:end) = candidate(m);
                    end
                    return;
                end
            end
        end

        function col = source_series_column(monitor_series, source, metric_key, n)
            col = nan(n, 1);
            if ~isfield(monitor_series, 'collector_series') || ~isstruct(monitor_series.collector_series)
                return;
            end
            if ~isfield(monitor_series.collector_series, source) || ~isstruct(monitor_series.collector_series.(source))
                return;
            end
            source_struct = monitor_series.collector_series.(source);
            if ~isfield(source_struct, metric_key) || ~isnumeric(source_struct.(metric_key))
                return;
            end
            vec = reshape(double(source_struct.(metric_key)), [], 1);
            m = min(n, numel(vec));
            col(1:m) = vec(1:m);
        end

        function metric_map = source_metric_map(monitor_series)
            metric_map = struct( ...
                'matlab', {{'cpu_proxy', 'gpu_series', 'memory_series', 'cpu_temp_c', 'power_w'}}, ...
                'hwinfo', {{'cpu_proxy', 'gpu_series', 'memory_series', 'cpu_temp_c', 'power_w', ...
                    'cpu_voltage_v', 'gpu_voltage_v', 'memory_voltage_v', ...
                    'cpu_power_w_hwinfo', 'gpu_power_w_hwinfo', 'memory_power_w_or_proxy', ...
                    'system_power_w', 'fan_rpm', 'pump_rpm'}}, ...
                'icue', {{'cpu_proxy', 'gpu_series', 'cpu_temp_c', 'power_w', ...
                    'fan_rpm', 'pump_rpm', 'coolant_temp_c', 'device_battery_level'}});
            if ~(isstruct(monitor_series) && isfield(monitor_series, 'collector_metric_catalog') && ...
                    ~isempty(monitor_series.collector_metric_catalog))
                return;
            end
            catalog = ExternalCollectorDispatcher.normalize_metric_catalog_array(monitor_series.collector_metric_catalog);
            for i = 1:numel(catalog)
                source = lower(char(string(catalog(i).source)));
                key = lower(char(string(catalog(i).metric_key)));
                if isempty(source) || isempty(key) || ~isfield(metric_map, source)
                    continue;
                end
                if ~any(strcmpi(metric_map.(source), key))
                    metric_map.(source){end + 1} = key; %#ok<AGROW>
                end
            end
        end

        function tf = hwinfo_series_present(monitor_series)
            tf = false;
            collector_series = ExternalCollectorDispatcher.pick_struct_field(monitor_series, 'collector_series');
            hwinfo_series = ExternalCollectorDispatcher.pick_struct_field(collector_series, 'hwinfo');
            if ~isstruct(hwinfo_series) || isempty(fieldnames(hwinfo_series))
                return;
            end
            series_fields = fieldnames(hwinfo_series);
            for i = 1:numel(series_fields)
                value = hwinfo_series.(series_fields{i});
                if isnumeric(value) || islogical(value)
                    if ~isempty(value) && any(isfinite(double(value(:))))
                        tf = true;
                        return;
                    end
                elseif iscell(value) || isstring(value) || ischar(value) || isstruct(value)
                    if ~isempty(value)
                        tf = true;
                        return;
                    end
                end
            end
        end

        function tf = hwinfo_catalog_present(monitor_series)
            tf = false;
            raw_catalog = ExternalCollectorDispatcher.pick_struct_field(monitor_series, 'collector_metric_catalog');
            if isempty(raw_catalog)
                return;
            end
            catalog = ExternalCollectorDispatcher.normalize_metric_catalog_array(raw_catalog);
            for i = 1:numel(catalog)
                if strcmpi(char(string(catalog(i).source)), 'hwinfo')
                    tf = true;
                    return;
                end
            end
        end

        function out_dir = resolve_output_dir(paths)
            out_dir = '';
            if nargin < 1 || ~isstruct(paths)
                return;
            end
            layout_version = lower(strtrim(char(string(ExternalCollectorDispatcher.pick_struct_text(paths, 'artifact_layout_version', '')))));
            compact_v3 = strcmp(layout_version, 'compact_v3');
            if isfield(paths, 'sustainability_collectors') && ~isempty(paths.sustainability_collectors)
                out_dir = char(string(paths.sustainability_collectors));
                return;
            end
            if isfield(paths, 'sustainability') && ~isempty(paths.sustainability)
                out_dir = char(string(paths.sustainability));
                return;
            end
            if isfield(paths, 'metrics_root') && ~isempty(paths.metrics_root)
                out_dir = char(string(paths.metrics_root));
                if ~compact_v3
                    out_dir = fullfile(out_dir, 'Collectors');
                end
                return;
            end
            if isfield(paths, 'reports') && ~isempty(paths.reports)
                out_dir = char(string(paths.reports));
                if ~compact_v3
                    out_dir = fullfile(out_dir, 'Collectors');
                end
                return;
            end
            if isfield(paths, 'base') && ~isempty(paths.base)
                out_dir = fullfile(char(string(paths.base)), 'Metrics');
                if ~compact_v3
                    out_dir = fullfile(out_dir, 'Collectors');
                end
            end
        end

        function [workbook_path, formatting_status, workbook_summary] = write_phase_workbook(paths, run_token, summary_context, monitor_series, data_table, curated_table, stage_summary, metric_guide)
            workbook_path = '';
            formatting_status = 'not_requested';
            workbook_summary = struct( ...
                'canonical_path', '', ...
                'root_mirror_path', '', ...
                'workbook_status', 'not_requested', ...
                'failure_message', '');
            workflow_kind = ExternalCollectorDispatcher.resolve_summary_workflow_kind(summary_context, monitor_series);
            layout_version = lower(strtrim(char(string(ExternalCollectorDispatcher.pick_struct_text(paths, 'artifact_layout_version', '')))));
            compact_v3 = strcmp(layout_version, 'compact_v3');
            is_phase_workflow = startsWith(lower(strtrim(workflow_kind)), 'phase');
            if ~compact_v3 && ~is_phase_workflow
                return;
            end
            sustainability_root = '';
            if isstruct(paths) && isfield(paths, 'run_data_workbook_path') && ~isempty(paths.run_data_workbook_path)
                workbook_path = char(string(paths.run_data_workbook_path));
                sustainability_root = fileparts(workbook_path);
            elseif isstruct(paths) && isfield(paths, 'sustainability') && ~isempty(paths.sustainability)
                sustainability_root = char(string(paths.sustainability));
            elseif isstruct(paths) && isfield(paths, 'sustainability_collectors') && ~isempty(paths.sustainability_collectors)
                sustainability_root = char(string(paths.sustainability_collectors));
            elseif isstruct(paths) && isfield(paths, 'metrics_root') && ~isempty(paths.metrics_root)
                sustainability_root = char(string(paths.metrics_root));
            elseif isstruct(paths) && isfield(paths, 'base') && ~isempty(paths.base)
                sustainability_root = fullfile(char(string(paths.base)), 'Metrics');
            end
            if isempty(sustainability_root)
                return;
            end
            if exist(sustainability_root, 'dir') ~= 7
                mkdir(sustainability_root);
            end

            if isempty(workbook_path)
                if is_phase_workflow
                    workbook_path = fullfile(sustainability_root, 'Run_Data.xlsx');
                elseif compact_v3
                    workbook_path = fullfile(sustainability_root, 'Run_Data.xlsx');
                else
                    workbook_path = fullfile(fullfile(sustainability_root, 'Collectors'), sprintf('%s_sustainability.xlsx', run_token));
                end
            end
            if ~(is_phase_workflow || compact_v3)
                workbook_parent = fileparts(workbook_path);
                if exist(workbook_parent, 'dir') ~= 7
                    mkdir(workbook_parent);
                end
            end
            workbook_summary.canonical_path = workbook_path;
            try
                if compact_v3 && strcmpi(workflow_kind, 'phase1_periodic_comparison')
                    phase1_sheets = ExternalCollectorDispatcher.build_phase1_workbook_sheet_defs( ...
                        summary_context, monitor_series, data_table, curated_table, stage_summary);
                    support_sheets = strings(1, numel(phase1_sheets));
                    for sheet_index = 1:numel(phase1_sheets)
                        ExternalCollectorDispatcher.write_phase_workbook_sheet(workbook_path, phase1_sheets(sheet_index));
                        support_sheets(sheet_index) = string(phase1_sheets(sheet_index).name);
                    end
                    formatting_status = ExternalCollectorDispatcher.try_style_phase_workbook( ...
                        workbook_path, phase1_sheets, 0, cellstr(support_sheets));
                elseif compact_v3 && strcmpi(workflow_kind, 'mesh_convergence_study')
                    mesh_sheets = ExternalCollectorDispatcher.build_mesh_convergence_workbook_sheet_defs( ...
                        summary_context, monitor_series, data_table, curated_table, stage_summary);
                    support_sheets = strings(1, numel(mesh_sheets) + 2);
                    for sheet_index = 1:numel(mesh_sheets)
                        ExternalCollectorDispatcher.write_phase_workbook_sheet(workbook_path, mesh_sheets(sheet_index));
                        support_sheets(sheet_index) = string(mesh_sheets(sheet_index).name);
                    end
                    if isempty(metric_guide)
                        writecell({'No metric guide rows available.'}, workbook_path, 'Sheet', 'Metric Guide');
                    else
                        writetable(metric_guide, workbook_path, 'Sheet', 'Metric Guide');
                    end
                    if isempty(data_table)
                        telemetry_status_table = ExternalCollectorDispatcher.phase_telemetry_status_raw_table( ...
                            monitor_series, ExternalCollectorDispatcher.resolve_phase_raw_hwinfo_csv_path(paths, monitor_series));
                        if isempty(telemetry_status_table)
                            writecell({'No normalized telemetry rows available.'}, workbook_path, 'Sheet', 'Telemetry Raw');
                        else
                            writetable(telemetry_status_table, workbook_path, 'Sheet', 'Telemetry Raw');
                        end
                    else
                        writetable(data_table, workbook_path, 'Sheet', 'Telemetry Raw');
                    end
                    support_sheets(end - 1) = "Metric Guide";
                    support_sheets(end) = "Telemetry Raw";
                    formatting_status = ExternalCollectorDispatcher.try_style_phase_workbook( ...
                        workbook_path, mesh_sheets, 0, cellstr(support_sheets));
                else
                    summary_cells = ExternalCollectorDispatcher.build_workbook_summary_cells(summary_context, monitor_series, curated_table, stage_summary);
                    telemetry_sheets = ExternalCollectorDispatcher.build_phase_workbook_sheets(summary_context, monitor_series, curated_table);
                    summary_sheet_name = 'Run Summary';
                    if ~is_phase_workflow
                        summary_sheet_name = 'Overview';
                    end
                    if isempty(summary_cells)
                        writecell({'No run summary rows available.'}, workbook_path, 'Sheet', summary_sheet_name);
                    else
                        writecell(summary_cells, workbook_path, 'Sheet', summary_sheet_name);
                    end

                    support_sheets = {summary_sheet_name, 'Metric Guide', 'Telemetry Raw'};
                    if compact_v3 && strcmpi(workflow_kind, 'phase1_periodic_comparison')
                        convergence_cells = ExternalCollectorDispatcher.build_phase1_convergence_sheet(summary_context);
                        writecell(convergence_cells, workbook_path, 'Sheet', 'Convergence');
                        support_sheets{end + 1} = 'Convergence'; %#ok<AGROW>
                    elseif ~compact_v3
                        if isempty(stage_summary)
                            writecell({'No stage summary rows available.'}, workbook_path, 'Sheet', 'Stage Summary');
                        else
                            writetable(stage_summary, workbook_path, 'Sheet', 'Stage Summary');
                        end
                        support_sheets{end + 1} = 'Stage Summary'; %#ok<AGROW>
                    end

                    for sheet_index = 1:numel(telemetry_sheets)
                        writecell(telemetry_sheets(sheet_index).cells, workbook_path, 'Sheet', telemetry_sheets(sheet_index).name);
                        if ~isempty(summary_cells)
                            writecell(summary_cells, workbook_path, 'Sheet', telemetry_sheets(sheet_index).name, ...
                                'Range', sprintf('%s1', ExternalCollectorDispatcher.excel_column_name(telemetry_sheets(sheet_index).summary_start_col)));
                        end
                    end
                    if isempty(metric_guide)
                        writecell({'No metric guide rows available.'}, workbook_path, 'Sheet', 'Metric Guide');
                    else
                        writetable(metric_guide, workbook_path, 'Sheet', 'Metric Guide');
                    end
                    if isempty(data_table)
                        telemetry_status_table = ExternalCollectorDispatcher.phase_telemetry_status_raw_table( ...
                            monitor_series, ExternalCollectorDispatcher.resolve_phase_raw_hwinfo_csv_path(paths, monitor_series));
                        if isempty(telemetry_status_table)
                            writecell({'No normalized telemetry rows available.'}, workbook_path, 'Sheet', 'Telemetry Raw');
                        else
                            writetable(telemetry_status_table, workbook_path, 'Sheet', 'Telemetry Raw');
                        end
                    else
                        writetable(data_table, workbook_path, 'Sheet', 'Telemetry Raw');
                    end
                    if ~compact_v3
                        if isempty(data_table)
                            writecell({'No normalized telemetry rows available.'}, workbook_path, 'Sheet', 'Normalized Telemetry');
                        else
                            writetable(data_table, workbook_path, 'Sheet', 'Normalized Telemetry');
                        end
                        support_sheets{end + 1} = 'Normalized Telemetry'; %#ok<AGROW>
                    end

                    formatting_status = ExternalCollectorDispatcher.try_style_phase_workbook( ...
                        workbook_path, telemetry_sheets, size(summary_cells, 2), support_sheets);
                end
                workbook_summary.workbook_status = 'created';
                if exist(workbook_path, 'file') ~= 2
                    error('ExternalCollectorDispatcher:WorkbookMissingAfterWrite', ...
                        'Workbook write finished without creating %s.', workbook_path);
                end
                if ~compact_v3 && (isempty(layout_version) || ~(strcmp(layout_version, 'phase_compact_v1') || strcmp(layout_version, 'compact_v2')))
                    phase_root = '';
                    if isstruct(paths) && isfield(paths, 'base') && ~isempty(paths.base)
                        phase_root = char(string(paths.base));
                    end
                    if ~isempty(phase_root)
                        workbook_summary.root_mirror_path = fullfile(phase_root, sprintf('%s_sustainability.xlsx', run_token));
                        if ~strcmpi(workbook_summary.root_mirror_path, workbook_path)
                            try
                                copyfile(workbook_path, workbook_summary.root_mirror_path, 'f');
                            catch ME
                                workbook_summary.root_mirror_path = '';
                                workbook_summary.failure_message = char(string(ME.message));
                            end
                        end
                    end
                end
            catch ME
                workbook_summary.workbook_status = 'failed';
                workbook_summary.failure_message = char(string(getReport(ME, 'extended', 'hyperlinks', 'off')));
                workbook_path = '';
                formatting_status = 'failed';
            end
        end

        function sheet_defs = build_phase_workbook_sheets(summary_context, monitor_series, curated_table)
            %#ok<INUSD>
            sheet_defs = repmat(struct( ...
                'name', '', ...
                'cells', {{}}, ...
                'style', repmat(struct('kind', '', 'method', ''), 1, 0), ...
                'summary_start_col', 3), 1, 0);
            workflow_kind = lower(strtrim(char(string( ...
                ExternalCollectorDispatcher.resolve_summary_workflow_kind(summary_context, monitor_series)))));

            switch workflow_kind
                case 'phase1_periodic_comparison'
                    sheet_defs(end + 1) = ExternalCollectorDispatcher.build_phase_method_workbook_sheet(curated_table, 'fd', 'FD'); %#ok<AGROW>
                    sheet_defs(end + 1) = ExternalCollectorDispatcher.build_phase_method_workbook_sheet(curated_table, 'spectral', 'SM'); %#ok<AGROW>
                case {'phase2_boundary_condition_study', 'phase3_bathymetry_study'}
                    scenario_ids = ExternalCollectorDispatcher.phase_workbook_scenario_ids(summary_context, curated_table);
                    used_names = strings(1, 0);
                    for i = 1:numel(scenario_ids)
                        sheet_name = ExternalCollectorDispatcher.resolve_phase_scenario_sheet_name(summary_context, scenario_ids(i), i);
                        sheet_name = ExternalCollectorDispatcher.sanitize_excel_sheet_name(sheet_name, sprintf('Scenario_%d', i), used_names);
                        used_names(end + 1) = string(sheet_name); %#ok<AGROW>
                        sheet_defs(end + 1) = ExternalCollectorDispatcher.build_phase_scenario_workbook_sheet(curated_table, scenario_ids(i), sheet_name); %#ok<AGROW>
                    end
                otherwise
                    sheet_defs(end + 1) = ExternalCollectorDispatcher.build_default_run_workbook_sheet(summary_context, curated_table); %#ok<AGROW>
            end

            if isempty(sheet_defs)
                sheet_defs = ExternalCollectorDispatcher.build_default_run_workbook_sheet(summary_context, curated_table);
            end
        end

        function sheet_defs = build_phase1_workbook_sheet_defs(summary_context, monitor_series, data_table, curated_table, stage_summary)
            comparison_cells = ExternalCollectorDispatcher.build_phase1_comparison_sheet_cells(summary_context);
            convergence_table = ExternalCollectorDispatcher.build_phase1_convergence_workbook_table(summary_context, stage_summary);
            fd_summary_table = ExternalCollectorDispatcher.build_phase1_ic_summary_workbook_table(summary_context, 'fd');
            sm_summary_table = ExternalCollectorDispatcher.build_phase1_ic_summary_workbook_table(summary_context, 'spectral');
            raw_csv_path = ExternalCollectorDispatcher.resolve_phase_raw_hwinfo_csv_path( ...
                ExternalCollectorDispatcher.pick_struct_field(summary_context, 'paths'), monitor_series);
            sustainability_raw = data_table;
            if isempty(sustainability_raw)
                sustainability_raw = ExternalCollectorDispatcher.phase_telemetry_status_raw_table(monitor_series, raw_csv_path);
            end
            sustainability_processed = ExternalCollectorDispatcher.build_phase1_sustainability_processed_table(stage_summary, monitor_series, raw_csv_path);
            plotting_data_table = ExternalCollectorDispatcher.build_phase1_plotting_data_table(summary_context, stage_summary);

            sheet_defs = repmat(struct( ...
                'name', '', ...
                'cells', {{}}, ...
                'style', repmat(struct('kind', '', 'method', ''), 1, 0), ...
                'summary_start_col', 0), 1, 0);
            sheet_defs(end + 1) = ExternalCollectorDispatcher.build_workbook_sheet_from_table('plotting_data', plotting_data_table, 'No plotting-data rows available.'); %#ok<AGROW>
            sheet_defs(end + 1) = ExternalCollectorDispatcher.build_workbook_sheet_from_cells('comparison data', comparison_cells); %#ok<AGROW>
            sheet_defs(end + 1) = ExternalCollectorDispatcher.build_workbook_sheet_from_table('convergence', convergence_table, 'No convergence rows available.'); %#ok<AGROW>
            sheet_defs(end + 1) = ExternalCollectorDispatcher.build_workbook_sheet_from_table('FD summary', fd_summary_table, 'No FD IC-study rows available.'); %#ok<AGROW>
            sheet_defs(end + 1) = ExternalCollectorDispatcher.build_workbook_sheet_from_table('SM summary', sm_summary_table, 'No SM IC-study rows available.'); %#ok<AGROW>
            sheet_defs(end + 1) = ExternalCollectorDispatcher.build_workbook_sheet_from_table('sustainability_raw', sustainability_raw, 'No raw telemetry rows available.'); %#ok<AGROW>
            sheet_defs(end + 1) = ExternalCollectorDispatcher.build_workbook_sheet_from_table('sustainability_processed', sustainability_processed, 'No processed sustainability rows available.'); %#ok<AGROW>
        end

        function sheet_defs = build_mesh_convergence_workbook_sheet_defs(summary_context, monitor_series, data_table, curated_table, stage_summary)
            %#ok<INUSD>
            plotting_data_table = ExternalCollectorDispatcher.build_mesh_convergence_plotting_data_table(summary_context, stage_summary);
            convergence_table = ExternalCollectorDispatcher.build_phase1_convergence_workbook_table(summary_context, stage_summary);
            runtime_table = ExternalCollectorDispatcher.build_mesh_convergence_runtime_resolution_table(summary_context, stage_summary);
            adaptive_table = ExternalCollectorDispatcher.build_mesh_convergence_adaptive_timestep_table(summary_context);
            fd_summary_table = ExternalCollectorDispatcher.build_mesh_convergence_selected_summary_table(summary_context, 'fd', stage_summary);
            sm_summary_table = ExternalCollectorDispatcher.build_mesh_convergence_selected_summary_table(summary_context, 'spectral', stage_summary);
            comparison_sheet = ExternalCollectorDispatcher.build_mesh_convergence_comparison_sheet_def(summary_context, stage_summary);
            raw_csv_path = ExternalCollectorDispatcher.resolve_phase_raw_hwinfo_csv_path( ...
                ExternalCollectorDispatcher.pick_struct_field(summary_context, 'paths'), monitor_series);
            sustainability_processed = ExternalCollectorDispatcher.build_mesh_convergence_sustainability_processed_table( ...
                stage_summary, monitor_series, raw_csv_path);

            sheet_defs = repmat(struct( ...
                'name', '', ...
                'cells', {{}}, ...
                'style', repmat(struct('kind', '', 'method', ''), 1, 0), ...
                'summary_start_col', 0), 1, 0);
            sheet_defs(end + 1) = ExternalCollectorDispatcher.build_workbook_sheet_from_table_with_style( ...
                'plotting_data', plotting_data_table, 'No plotting-data rows available.', ...
                ExternalCollectorDispatcher.build_mesh_generic_table_style(plotting_data_table, '')); %#ok<AGROW>
            sheet_defs(end + 1) = comparison_sheet; %#ok<AGROW>
            sheet_defs(end + 1) = ExternalCollectorDispatcher.build_workbook_sheet_from_table_with_style( ...
                'convergence', convergence_table, 'No convergence rows available.', ...
                ExternalCollectorDispatcher.build_mesh_convergence_table_style(convergence_table)); %#ok<AGROW>
            sheet_defs(end + 1) = ExternalCollectorDispatcher.build_workbook_sheet_from_table_with_style( ...
                'runtime_vs_resolution', runtime_table, 'No runtime-vs-resolution rows available.', ...
                ExternalCollectorDispatcher.build_mesh_method_table_style(runtime_table)); %#ok<AGROW>
            sheet_defs(end + 1) = ExternalCollectorDispatcher.build_workbook_sheet_from_table_with_style( ...
                'adaptive_timestep', adaptive_table, 'No adaptive-timestep rows available.', ...
                ExternalCollectorDispatcher.build_mesh_method_table_style(adaptive_table)); %#ok<AGROW>
            sheet_defs(end + 1) = ExternalCollectorDispatcher.build_workbook_sheet_from_table_with_style( ...
                'FD summary', fd_summary_table, 'No FD selected-mesh summary rows available.', ...
                ExternalCollectorDispatcher.build_mesh_summary_table_style(fd_summary_table, 'fd')); %#ok<AGROW>
            sheet_defs(end + 1) = ExternalCollectorDispatcher.build_workbook_sheet_from_table_with_style( ...
                'SM summary', sm_summary_table, 'No SM selected-mesh summary rows available.', ...
                ExternalCollectorDispatcher.build_mesh_summary_table_style(sm_summary_table, 'spectral')); %#ok<AGROW>
            sheet_defs(end + 1) = ExternalCollectorDispatcher.build_workbook_sheet_from_table( ...
                'sustainability_processed', sustainability_processed, 'No processed sustainability rows available.'); %#ok<AGROW>
        end

        function sheet_def = build_mesh_convergence_comparison_sheet_def(summary_context, stage_summary)
            [cells_out, style_rows] = ExternalCollectorDispatcher.build_mesh_convergence_comparison_sheet_cells(summary_context, stage_summary);
            sheet_def = ExternalCollectorDispatcher.build_workbook_sheet_from_cells('comparison data', cells_out, style_rows, 0);
        end

        function [cells_out, style_rows] = build_mesh_convergence_comparison_sheet_cells(summary_context, stage_summary)
            results_struct = ExternalCollectorDispatcher.pick_struct_field(summary_context, 'results');
            metrics_struct = ExternalCollectorDispatcher.pick_struct_field(results_struct, 'metrics');
            summary_metrics = ExternalCollectorDispatcher.pick_struct_field(metrics_struct, 'summary');
            error_payload = ExternalCollectorDispatcher.pick_struct_field(results_struct, 'error_vs_time');
            if isempty(fieldnames(error_payload))
                error_payload = ExternalCollectorDispatcher.pick_struct_field(results_struct, 'rmse_vs_time');
            end
            children = ExternalCollectorDispatcher.pick_struct_field(results_struct, 'children');
            fd_child = ExternalCollectorDispatcher.pick_struct_field(children, 'fd');
            sm_child = ExternalCollectorDispatcher.pick_struct_field(children, 'spectral');
            fd_mesh = ExternalCollectorDispatcher.pick_struct_field(fd_child, 'selected_mesh');
            sm_mesh = ExternalCollectorDispatcher.pick_struct_field(sm_child, 'selected_mesh');
            fd_stage = ExternalCollectorDispatcher.phase1_find_convergence_stage_row(stage_summary, 'FD', ...
                ExternalCollectorDispatcher.pick_struct_number(fd_mesh, 'mesh_level_index', NaN), ...
                ExternalCollectorDispatcher.pick_struct_number(fd_mesh, 'Nx', NaN), ...
                ExternalCollectorDispatcher.pick_struct_number(fd_mesh, 'Ny', NaN));
            sm_stage = ExternalCollectorDispatcher.phase1_find_convergence_stage_row(stage_summary, 'Spectral', ...
                ExternalCollectorDispatcher.pick_struct_number(sm_mesh, 'mesh_level_index', NaN), ...
                ExternalCollectorDispatcher.pick_struct_number(sm_mesh, 'Nx', NaN), ...
                ExternalCollectorDispatcher.pick_struct_number(sm_mesh, 'Ny', NaN));

            cells_out = { ...
                'Mesh convergence comparison data', '', '', '', '', '', '', ''; ...
                '', '', '', '', '', '', '', ''; ...
                'Selected mesh summary', '', '', '', '', '', '', ''; ...
                'Method', 'Selected mesh', 'Verdict', 'Tolerance (%)', 'xi_L2 (%)', 'xi_peak (%)', 'Selection reason', 'Fallback used'; ...
                'FD', ExternalCollectorDispatcher.phase1_mesh_label(fd_mesh), ...
                    ExternalCollectorDispatcher.pick_struct_text(fd_mesh, 'verdict', ''), ...
                    ExternalCollectorDispatcher.pick_struct_number(fd_mesh, 'tolerance', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(fd_mesh, 'final_relative_change', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(fd_mesh, 'final_peak_error', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_text(fd_mesh, 'selection_reason', ''), ...
                    ExternalCollectorDispatcher.yes_no_text(ExternalCollectorDispatcher.pick_struct_value(fd_mesh, 'fallback_used', false)); ...
                'SM', ExternalCollectorDispatcher.phase1_mesh_label(sm_mesh), ...
                    ExternalCollectorDispatcher.pick_struct_text(sm_mesh, 'verdict', ''), ...
                    ExternalCollectorDispatcher.pick_struct_number(sm_mesh, 'tolerance', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(sm_mesh, 'final_relative_change', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(sm_mesh, 'final_peak_error', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_text(sm_mesh, 'selection_reason', ''), ...
                    ExternalCollectorDispatcher.yes_no_text(ExternalCollectorDispatcher.pick_struct_value(sm_mesh, 'fallback_used', false)); ...
                '', '', '', '', '', '', '', ''; ...
                'Runtime and telemetry summary', '', '', '', '', '', '', ''; ...
                'Method', 'Selected mesh', 'Runtime (s)', 'Mean power (W)', 'Peak power (W)', 'Energy (Wh)', 'CO2e (g)', 'Source path'; ...
                'FD', ExternalCollectorDispatcher.phase1_mesh_label(fd_mesh), ...
                    ExternalCollectorDispatcher.table_row_number(fd_stage, 1, 'wall_time_s', NaN), ...
                    ExternalCollectorDispatcher.table_row_number(fd_stage, 1, 'mean_total_power_w', NaN), ...
                    ExternalCollectorDispatcher.table_row_number(fd_stage, 1, 'peak_total_power_w', NaN), ...
                    ExternalCollectorDispatcher.table_row_number(fd_stage, 1, 'energy_wh_total', NaN), ...
                    ExternalCollectorDispatcher.table_row_number(fd_stage, 1, 'co2_g_total', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_text(fd_mesh, 'source_path', ''); ...
                'SM', ExternalCollectorDispatcher.phase1_mesh_label(sm_mesh), ...
                    ExternalCollectorDispatcher.table_row_number(sm_stage, 1, 'wall_time_s', NaN), ...
                    ExternalCollectorDispatcher.table_row_number(sm_stage, 1, 'mean_total_power_w', NaN), ...
                    ExternalCollectorDispatcher.table_row_number(sm_stage, 1, 'peak_total_power_w', NaN), ...
                    ExternalCollectorDispatcher.table_row_number(sm_stage, 1, 'energy_wh_total', NaN), ...
                    ExternalCollectorDispatcher.table_row_number(sm_stage, 1, 'co2_g_total', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_text(sm_mesh, 'source_path', ''); ...
                '', '', '', '', '', '', '', ''; ...
                'Adaptive timestep summary at selected mesh', '', '', '', '', '', '', ''; ...
                'Method', 'dt_advection', 'dt_diffusion', 'dt_CFL', 'Observed CFL', 'Terminal CFL', 'Convergence status', 'Mesh level'; ...
                'FD', ...
                    ExternalCollectorDispatcher.pick_struct_number(fd_mesh, 'dt_adv', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(fd_mesh, 'dt_diff', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(fd_mesh, 'dt_final', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(fd_mesh, 'cfl_observed', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(fd_mesh, 'cfl', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_text(fd_mesh, 'convergence_status', ''), ...
                    ExternalCollectorDispatcher.pick_struct_text(fd_mesh, 'mesh_level_label', ''); ...
                'SM', ...
                    ExternalCollectorDispatcher.pick_struct_number(sm_mesh, 'dt_adv', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(sm_mesh, 'dt_diff', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(sm_mesh, 'dt_final', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(sm_mesh, 'cfl_observed', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(sm_mesh, 'cfl', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_text(sm_mesh, 'convergence_status', ''), ...
                    ExternalCollectorDispatcher.pick_struct_text(sm_mesh, 'mesh_level_label', ''); ...
                '', '', '', '', '', '', '', ''; ...
                'Final invariant summary at selected mesh', '', '', '', '', '', '', ''; ...
                'Method', 'Final kinetic energy', 'Final enstrophy', 'Final circulation', 'Joint tolerance met', 'Fallback used', 'Selection reason', 'Source path'; ...
                'FD', ...
                    ExternalCollectorDispatcher.pick_struct_number(fd_mesh, 'final_energy', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(fd_mesh, 'final_enstrophy', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(fd_mesh, 'final_circulation', NaN), ...
                    ExternalCollectorDispatcher.yes_no_text(ExternalCollectorDispatcher.pick_struct_value(fd_mesh, 'joint_tolerance_met', false)), ...
                    ExternalCollectorDispatcher.yes_no_text(ExternalCollectorDispatcher.pick_struct_value(fd_mesh, 'fallback_used', false)), ...
                    ExternalCollectorDispatcher.pick_struct_text(fd_mesh, 'selection_reason', ''), ...
                    ExternalCollectorDispatcher.pick_struct_text(fd_mesh, 'source_path', ''); ...
                'SM', ...
                    ExternalCollectorDispatcher.pick_struct_number(sm_mesh, 'final_energy', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(sm_mesh, 'final_enstrophy', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(sm_mesh, 'final_circulation', NaN), ...
                    ExternalCollectorDispatcher.yes_no_text(ExternalCollectorDispatcher.pick_struct_value(sm_mesh, 'joint_tolerance_met', false)), ...
                    ExternalCollectorDispatcher.yes_no_text(ExternalCollectorDispatcher.pick_struct_value(sm_mesh, 'fallback_used', false)), ...
                    ExternalCollectorDispatcher.pick_struct_text(sm_mesh, 'selection_reason', ''), ...
                    ExternalCollectorDispatcher.pick_struct_text(sm_mesh, 'source_path', '')};

            selected_metric_rows = { ...
                {'Vorticity relative L2 mismatch', 'mean_cross_method_mismatch_l2'}; ...
                {'Streamfunction relative L2 mismatch', 'mean_cross_method_streamfunction_mismatch_l2'}; ...
                {'Speed relative L2 mismatch', 'mean_cross_method_speed_mismatch_l2'}; ...
                {'Velocity-vector relative L2 mismatch', 'mean_cross_method_velocity_vector_mismatch_l2'}; ...
                {'Streamline-direction relative L2 mismatch', 'mean_cross_method_streamline_direction_mismatch_l2'}};
            cells_out(end + 1, 1:8) = {'', '', '', '', '', '', '', ''}; %#ok<AGROW>
            cells_out(end + 1, 1:8) = {'Selected-mesh cross-method field mismatch', '', '', '', '', '', '', ''}; %#ok<AGROW>
            cells_out(end + 1, 1:8) = {'Metric', 'Mean / Aggregate', 'FD', 'SM', '', '', '', ''}; %#ok<AGROW>
            fd_metrics = ExternalCollectorDispatcher.pick_struct_field(metrics_struct, 'FD');
            sm_metrics = ExternalCollectorDispatcher.pick_struct_field(metrics_struct, 'Spectral');
            fd_fields = { ...
                'cross_method_mismatch_l2', ...
                'cross_method_streamfunction_relative_l2_mismatch', ...
                'cross_method_speed_relative_l2_mismatch', ...
                'cross_method_velocity_vector_relative_l2_mismatch', ...
                'cross_method_streamline_direction_relative_l2_mismatch'};
            sm_fields = fd_fields;
            for i = 1:numel(selected_metric_rows)
                cells_out(end + 1, 1:8) = { ... %#ok<AGROW>
                    selected_metric_rows{i}{1}, ...
                    ExternalCollectorDispatcher.pick_struct_number(summary_metrics, selected_metric_rows{i}{2}, NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(fd_metrics, fd_fields{i}, NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(sm_metrics, sm_fields{i}, NaN), ...
                    '', '', '', ''};
            end

            error_times = reshape(double(ExternalCollectorDispatcher.pick_struct_value(error_payload, 'time_s', [])), 1, []);
            vorticity_l2 = reshape(double(ExternalCollectorDispatcher.pick_struct_value(error_payload, ...
                'vorticity_vector_relative_l2_mismatch', ExternalCollectorDispatcher.pick_struct_value(error_payload, 'relative_l2_mismatch', []))), 1, []);
            streamfunction_l2 = reshape(double(ExternalCollectorDispatcher.pick_struct_value(error_payload, 'streamfunction_relative_l2_mismatch', [])), 1, []);
            speed_l2 = reshape(double(ExternalCollectorDispatcher.pick_struct_value(error_payload, 'speed_relative_l2_mismatch', [])), 1, []);
            velocity_vector_l2 = reshape(double(ExternalCollectorDispatcher.pick_struct_value(error_payload, 'velocity_vector_relative_l2_mismatch', [])), 1, []);
            streamline_direction_l2 = reshape(double(ExternalCollectorDispatcher.pick_struct_value(error_payload, 'streamline_direction_relative_l2_mismatch', [])), 1, []);
            circulation_series = reshape(double(ExternalCollectorDispatcher.pick_struct_value(error_payload, 'circulation_relative_error', [])), 1, []);
            kinetic_energy_series = reshape(double(ExternalCollectorDispatcher.pick_struct_value(error_payload, 'kinetic_energy_relative_error', [])), 1, []);
            error_count = max([numel(error_times), numel(vorticity_l2), numel(streamfunction_l2), numel(speed_l2), ...
                numel(velocity_vector_l2), numel(streamline_direction_l2), numel(circulation_series), numel(kinetic_energy_series), 0]);
            if error_count > 0
                cells_out(end + 1, 1:8) = {'', '', '', '', '', '', '', ''}; %#ok<AGROW>
                cells_out(end + 1, 1:8) = {'Selected-mesh error vs time', '', '', '', '', '', '', ''}; %#ok<AGROW>
                cells_out(end + 1, 1:8) = {'Time (s)', 'Vorticity L2', 'Streamfunction L2', 'Speed L2', ...
                    'Velocity-vector L2', 'Streamline-direction L2', 'Circulation', 'Kinetic Energy'}; %#ok<AGROW>
                for i = 1:error_count
                    cells_out(end + 1, 1:8) = { ... %#ok<AGROW>
                        ExternalCollectorDispatcher.numeric_cell(error_times, i), ...
                        ExternalCollectorDispatcher.numeric_cell(vorticity_l2, i), ...
                        ExternalCollectorDispatcher.numeric_cell(streamfunction_l2, i), ...
                        ExternalCollectorDispatcher.numeric_cell(speed_l2, i), ...
                        ExternalCollectorDispatcher.numeric_cell(velocity_vector_l2, i), ...
                        ExternalCollectorDispatcher.numeric_cell(streamline_direction_l2, i), ...
                        ExternalCollectorDispatcher.numeric_cell(circulation_series, i), ...
                        ExternalCollectorDispatcher.numeric_cell(kinetic_energy_series, i)};
                end
            end

            style_rows = repmat(struct('kind', 'data', 'method', ''), 1, size(cells_out, 1));
            style_rows(1) = struct('kind', 'section_header', 'method', '');
            style_rows(2) = struct('kind', 'spacer', 'method', '');
            style_rows(3) = struct('kind', 'mesh_block_a_header', 'method', '');
            style_rows(4) = struct('kind', 'header', 'method', '');
            style_rows(5) = struct('kind', 'mesh_block_a_data', 'method', 'fd');
            style_rows(6) = struct('kind', 'mesh_block_a_data', 'method', 'spectral');
            style_rows(7) = struct('kind', 'spacer', 'method', '');
            style_rows(8) = struct('kind', 'mesh_block_b_header', 'method', '');
            style_rows(9) = struct('kind', 'header', 'method', '');
            style_rows(10) = struct('kind', 'mesh_block_b_data', 'method', 'fd');
            style_rows(11) = struct('kind', 'mesh_block_b_data', 'method', 'spectral');
            style_rows(12) = struct('kind', 'spacer', 'method', '');
            style_rows(13) = struct('kind', 'mesh_block_c_header', 'method', '');
            style_rows(14) = struct('kind', 'header', 'method', '');
            style_rows(15) = struct('kind', 'mesh_block_c_data', 'method', 'fd');
            style_rows(16) = struct('kind', 'mesh_block_c_data', 'method', 'spectral');
            style_rows(17) = struct('kind', 'spacer', 'method', '');
            style_rows(18) = struct('kind', 'mesh_block_d_header', 'method', '');
            style_rows(19) = struct('kind', 'header', 'method', '');
            style_rows(20) = struct('kind', 'mesh_block_d_data', 'method', 'fd');
            style_rows(21) = struct('kind', 'mesh_block_d_data', 'method', 'spectral');
            row_cursor = 22;
            if numel(style_rows) >= row_cursor + 7
                style_rows(row_cursor) = struct('kind', 'spacer', 'method', '');
                style_rows(row_cursor + 1) = struct('kind', 'mesh_block_e_header', 'method', '');
                style_rows(row_cursor + 2) = struct('kind', 'header', 'method', '');
                for i = (row_cursor + 3):(row_cursor + 7)
                    style_rows(i) = struct('kind', 'mesh_block_e_data', 'method', '');
                end
                row_cursor = row_cursor + 8;
            end
            if numel(style_rows) >= row_cursor + 3
                style_rows(row_cursor) = struct('kind', 'spacer', 'method', '');
                style_rows(row_cursor + 1) = struct('kind', 'mesh_block_f_header', 'method', '');
                style_rows(row_cursor + 2) = struct('kind', 'header', 'method', '');
                for i = (row_cursor + 3):numel(style_rows)
                    style_rows(i) = struct('kind', 'mesh_block_f_data', 'method', '');
                end
            end
        end

        function table_out = build_mesh_convergence_runtime_resolution_table(summary_context, stage_summary)
            if nargin < 2
                stage_summary = table();
            end
            rows = repmat(struct( ...
                'Method', "", ...
                'Level', "", ...
                'MeshLabel', "", ...
                'Nx', NaN, ...
                'Ny', NaN, ...
                'Runtime_s', NaN, ...
                'xi_L2_pct', NaN, ...
                'xi_peak_pct', NaN, ...
                'FinalEnergy', NaN, ...
                'FinalEnstrophy', NaN, ...
                'FinalCirculation', NaN, ...
                'MeanTotalPower_W', NaN, ...
                'Energy_Wh', NaN, ...
                'CO2_g', NaN, ...
                'SelectedMesh', ""), 1, 0);
            results_struct = ExternalCollectorDispatcher.pick_struct_field(summary_context, 'results');
            children = ExternalCollectorDispatcher.pick_struct_field(results_struct, 'children');
            method_specs = {struct('key', 'fd', 'label', 'FD'), struct('key', 'spectral', 'label', 'SM')};
            for mi = 1:numel(method_specs)
                child = ExternalCollectorDispatcher.pick_struct_field(children, method_specs{mi}.key);
                convergence_output = ExternalCollectorDispatcher.pick_struct_field(child, 'convergence_output');
                convergence_results = ExternalCollectorDispatcher.pick_struct_field(convergence_output, 'results');
                run_records = ExternalCollectorDispatcher.pick_struct_value(convergence_results, 'run_records', struct([]));
                if ~isstruct(run_records)
                    continue;
                end
                for ri = 1:numel(run_records)
                    record = run_records(ri);
                    stage_row = ExternalCollectorDispatcher.phase1_find_convergence_stage_row( ...
                        ExternalCollectorDispatcher.safe_table(stage_summary), ...
                        method_specs{mi}.label, ...
                        ExternalCollectorDispatcher.pick_struct_number(record, 'mesh_level_index', ri), ...
                        ExternalCollectorDispatcher.pick_struct_number(record, 'Nx', NaN), ...
                        ExternalCollectorDispatcher.pick_struct_number(record, 'Ny', NaN));
                    rows(end + 1) = struct( ... %#ok<AGROW>
                        'Method', string(method_specs{mi}.label), ...
                        'Level', string(ExternalCollectorDispatcher.pick_struct_text(record, 'mesh_level_label', sprintf('L%02d', ri))), ...
                        'MeshLabel', string(sprintf('%s / %dx%d', ...
                            ExternalCollectorDispatcher.pick_struct_text(record, 'mesh_level_label', sprintf('L%02d', ri)), ...
                            round(ExternalCollectorDispatcher.pick_struct_number(record, 'Nx', NaN)), ...
                            round(ExternalCollectorDispatcher.pick_struct_number(record, 'Ny', NaN)))), ...
                        'Nx', ExternalCollectorDispatcher.pick_struct_number(record, 'Nx', NaN), ...
                        'Ny', ExternalCollectorDispatcher.pick_struct_number(record, 'Ny', NaN), ...
                        'Runtime_s', ExternalCollectorDispatcher.pick_struct_number(record, 'runtime_wall_s', NaN), ...
                        'xi_L2_pct', ExternalCollectorDispatcher.pick_struct_number(record, 'xi', NaN), ...
                        'xi_peak_pct', ExternalCollectorDispatcher.pick_struct_number(record, 'max_vorticity_rel_error_pct', NaN), ...
                        'FinalEnergy', ExternalCollectorDispatcher.pick_struct_number(record, 'final_energy', NaN), ...
                        'FinalEnstrophy', ExternalCollectorDispatcher.pick_struct_number(record, 'final_enstrophy', NaN), ...
                        'FinalCirculation', ExternalCollectorDispatcher.pick_struct_number(record, 'final_circulation', NaN), ...
                        'MeanTotalPower_W', ExternalCollectorDispatcher.table_row_number(stage_row, 1, 'mean_total_power_w', NaN), ...
                        'Energy_Wh', ExternalCollectorDispatcher.table_row_number(stage_row, 1, 'energy_wh_total', NaN), ...
                        'CO2_g', ExternalCollectorDispatcher.table_row_number(stage_row, 1, 'co2_g_total', NaN), ...
                        'SelectedMesh', string(ExternalCollectorDispatcher.yes_no_text(ExternalCollectorDispatcher.pick_struct_value(record, 'selected_level', false))));
                end
            end
            table_out = struct2table(rows);
        end

        function table_out = build_mesh_convergence_adaptive_timestep_table(summary_context)
            rows = repmat(struct( ...
                'Method', "", ...
                'Level', "", ...
                'MeshLabel', "", ...
                'dt_advection', NaN, ...
                'dt_diffusion', NaN, ...
                'dt_cfl', NaN, ...
                'dt_used', NaN, ...
                'cfl_observed', NaN, ...
                'cfl_terminal', NaN, ...
                'FinalEnergy', NaN, ...
                'FinalEnstrophy', NaN, ...
                'FinalCirculation', NaN, ...
                'SelectedMesh', ""), 1, 0);
            results_struct = ExternalCollectorDispatcher.pick_struct_field(summary_context, 'results');
            children = ExternalCollectorDispatcher.pick_struct_field(results_struct, 'children');
            method_specs = {struct('key', 'fd', 'label', 'FD'), struct('key', 'spectral', 'label', 'SM')};
            for mi = 1:numel(method_specs)
                child = ExternalCollectorDispatcher.pick_struct_field(children, method_specs{mi}.key);
                convergence_output = ExternalCollectorDispatcher.pick_struct_field(child, 'convergence_output');
                convergence_results = ExternalCollectorDispatcher.pick_struct_field(convergence_output, 'results');
                run_records = ExternalCollectorDispatcher.pick_struct_value(convergence_results, 'run_records', struct([]));
                if ~isstruct(run_records)
                    continue;
                end
                for ri = 1:numel(run_records)
                    record = run_records(ri);
                    rows(end + 1) = struct( ... %#ok<AGROW>
                        'Method', string(method_specs{mi}.label), ...
                        'Level', string(ExternalCollectorDispatcher.pick_struct_text(record, 'mesh_level_label', sprintf('L%02d', ri))), ...
                        'MeshLabel', string(sprintf('%s / %dx%d', ...
                            ExternalCollectorDispatcher.pick_struct_text(record, 'mesh_level_label', sprintf('L%02d', ri)), ...
                            round(ExternalCollectorDispatcher.pick_struct_number(record, 'Nx', NaN)), ...
                            round(ExternalCollectorDispatcher.pick_struct_number(record, 'Ny', NaN)))), ...
                        'dt_advection', ExternalCollectorDispatcher.pick_struct_number(record, 'dt_adv', NaN), ...
                        'dt_diffusion', ExternalCollectorDispatcher.pick_struct_number(record, 'dt_diff', NaN), ...
                        'dt_cfl', ExternalCollectorDispatcher.pick_struct_number(record, 'dt_final', NaN), ...
                        'dt_used', ExternalCollectorDispatcher.pick_struct_number(record, 'dt_used', NaN), ...
                        'cfl_observed', ExternalCollectorDispatcher.pick_struct_number(record, 'cfl_observed', NaN), ...
                        'cfl_terminal', ExternalCollectorDispatcher.pick_struct_number(record, 'cfl', NaN), ...
                        'FinalEnergy', ExternalCollectorDispatcher.pick_struct_number(record, 'final_energy', NaN), ...
                        'FinalEnstrophy', ExternalCollectorDispatcher.pick_struct_number(record, 'final_enstrophy', NaN), ...
                        'FinalCirculation', ExternalCollectorDispatcher.pick_struct_number(record, 'final_circulation', NaN), ...
                        'SelectedMesh', string(ExternalCollectorDispatcher.yes_no_text(ExternalCollectorDispatcher.pick_struct_value(record, 'selected_level', false))));
                end
            end
            table_out = struct2table(rows);
        end

        function table_out = build_mesh_convergence_selected_summary_table(summary_context, method_key, stage_summary)
            results_struct = ExternalCollectorDispatcher.pick_struct_field(summary_context, 'results');
            children = ExternalCollectorDispatcher.pick_struct_field(results_struct, 'children');
            child = ExternalCollectorDispatcher.pick_struct_field(children, char(string(method_key)));
            selected_mesh = ExternalCollectorDispatcher.pick_struct_field(child, 'selected_mesh');
            method_label = 'FD';
            stage_method = 'FD';
            if strcmpi(char(string(method_key)), 'spectral')
                method_label = 'SM';
                stage_method = 'Spectral';
            end
            stage_row = ExternalCollectorDispatcher.phase1_find_convergence_stage_row(stage_summary, stage_method, ...
                ExternalCollectorDispatcher.pick_struct_number(selected_mesh, 'mesh_level_index', NaN), ...
                ExternalCollectorDispatcher.pick_struct_number(selected_mesh, 'Nx', NaN), ...
                ExternalCollectorDispatcher.pick_struct_number(selected_mesh, 'Ny', NaN));
            row = struct( ...
                'Method', string(method_label), ...
                'SelectedMesh', string(ExternalCollectorDispatcher.phase1_mesh_label(selected_mesh)), ...
                'Verdict', string(ExternalCollectorDispatcher.pick_struct_text(selected_mesh, 'verdict', '')), ...
                'Tolerance_pct', ExternalCollectorDispatcher.pick_struct_number(selected_mesh, 'tolerance', NaN), ...
                'xi_L2_pct', ExternalCollectorDispatcher.pick_struct_number(selected_mesh, 'final_relative_change', NaN), ...
                'xi_peak_pct', ExternalCollectorDispatcher.pick_struct_number(selected_mesh, 'final_peak_error', NaN), ...
                'SelectionReason', string(ExternalCollectorDispatcher.pick_struct_text(selected_mesh, 'selection_reason', '')), ...
                'FallbackUsed', string(ExternalCollectorDispatcher.yes_no_text(ExternalCollectorDispatcher.pick_struct_value(selected_mesh, 'fallback_used', false))), ...
                'ContinuedAfterUnconverged', string(ExternalCollectorDispatcher.yes_no_text(ExternalCollectorDispatcher.pick_struct_value(selected_mesh, 'continued_after_unconverged_mesh', false))), ...
                'Runtime_s', ExternalCollectorDispatcher.table_row_number(stage_row, 1, 'wall_time_s', NaN), ...
                'MeanTotalPower_W', ExternalCollectorDispatcher.table_row_number(stage_row, 1, 'mean_total_power_w', NaN), ...
                'Energy_Wh', ExternalCollectorDispatcher.table_row_number(stage_row, 1, 'energy_wh_total', NaN), ...
                'CO2_g', ExternalCollectorDispatcher.table_row_number(stage_row, 1, 'co2_g_total', NaN), ...
                'ObservedCFL', ExternalCollectorDispatcher.pick_struct_number(selected_mesh, 'cfl_observed', NaN), ...
                'TerminalCFL', ExternalCollectorDispatcher.pick_struct_number(selected_mesh, 'cfl', NaN), ...
                'FinalEnergy', ExternalCollectorDispatcher.pick_struct_number(selected_mesh, 'final_energy', NaN), ...
                'FinalEnstrophy', ExternalCollectorDispatcher.pick_struct_number(selected_mesh, 'final_enstrophy', NaN), ...
                'FinalCirculation', ExternalCollectorDispatcher.pick_struct_number(selected_mesh, 'final_circulation', NaN), ...
                'SourcePath', string(ExternalCollectorDispatcher.pick_struct_text(selected_mesh, 'source_path', '')));
            table_out = struct2table(row);
        end

        function style_rows = build_mesh_generic_table_style(table_in, method_hint)
            if isempty(table_in) || ~istable(table_in) || height(table_in) < 1
                style_rows = struct('kind', 'empty', 'method', char(string(method_hint)));
                return;
            end
            style_rows = repmat(struct('kind', 'data', 'method', char(string(method_hint))), 1, height(table_in) + 1);
            style_rows(1) = struct('kind', 'header', 'method', '');
            if isempty(method_hint) && ismember('Method', table_in.Properties.VariableNames)
                for i = 1:height(table_in)
                    style_rows(i + 1).method = ExternalCollectorDispatcher.normalize_method_family( ...
                        ExternalCollectorDispatcher.table_row_text(table_in, i, 'Method', ''));
                end
            end
        end

        function style_rows = build_mesh_method_table_style(table_in)
            style_rows = ExternalCollectorDispatcher.build_mesh_generic_table_style(table_in, '');
            if isempty(table_in) || ~istable(table_in) || height(table_in) < 1 || ...
                    ~ismember('SelectedMesh', table_in.Properties.VariableNames)
                return;
            end
            for i = 1:height(table_in)
                if strcmpi(ExternalCollectorDispatcher.table_row_text(table_in, i, 'SelectedMesh', ''), 'Yes')
                    style_rows(i + 1).kind = 'selected_mesh';
                end
            end
        end

        function style_rows = build_mesh_convergence_table_style(table_in)
            style_rows = ExternalCollectorDispatcher.build_mesh_method_table_style(table_in);
        end

        function style_rows = build_mesh_summary_table_style(table_in, method_hint)
            style_rows = ExternalCollectorDispatcher.build_mesh_generic_table_style(table_in, method_hint);
            if numel(style_rows) >= 2
                style_rows(2).kind = 'selected_mesh';
            end
        end

        function cells_out = build_phase1_comparison_sheet_cells(summary_context)
            results_struct = ExternalCollectorDispatcher.pick_struct_field(summary_context, 'results');
            metrics_struct = ExternalCollectorDispatcher.pick_struct_field(results_struct, 'metrics');
            fd_metrics = ExternalCollectorDispatcher.pick_struct_field(metrics_struct, 'FD');
            sm_metrics = ExternalCollectorDispatcher.pick_struct_field(metrics_struct, 'Spectral');
            summary_metrics = ExternalCollectorDispatcher.pick_struct_field(metrics_struct, 'summary');
            error_payload = ExternalCollectorDispatcher.pick_struct_field(results_struct, 'error_vs_time');
            if isempty(fieldnames(error_payload))
                error_payload = ExternalCollectorDispatcher.pick_struct_field(results_struct, 'rmse_vs_time');
            end
            ic_study = ExternalCollectorDispatcher.pick_struct_field(results_struct, 'ic_study');
            children = ExternalCollectorDispatcher.pick_struct_field(results_struct, 'children');
            fd_child = ExternalCollectorDispatcher.pick_struct_field(children, 'fd');
            sm_child = ExternalCollectorDispatcher.pick_struct_field(children, 'spectral');
            baseline_label = ExternalCollectorDispatcher.phase1_baseline_display_label(ic_study);
            baseline_group = ExternalCollectorDispatcher.pick_struct_text(ic_study, 'selected_group', 'baseline');

            cells_out = { ...
                'Phase 1 comparison data', '', '', '', '', '', ''; ...
                '', '', '', '', '', '', ''; ...
                'Cross-method mismatch', '', '', '', '', '', ''; ...
                'Metric', 'FD', 'SM', 'Mean / Aggregate', '', '', ''; ...
                'Relative L2 mismatch', ...
                    ExternalCollectorDispatcher.pick_struct_number(summary_metrics, 'fd_vs_spectral_mismatch_l2', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(summary_metrics, 'spectral_vs_fd_mismatch_l2', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(summary_metrics, 'mean_cross_method_mismatch_l2', NaN), ...
                    '', '', ''; ...
                'Relative L_inf mismatch', ...
                    ExternalCollectorDispatcher.pick_struct_number(summary_metrics, 'fd_vs_spectral_mismatch_linf', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(summary_metrics, 'spectral_vs_fd_mismatch_linf', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(summary_metrics, 'mean_cross_method_mismatch_linf', NaN), ...
                    '', '', ''; ...
                'Streamfunction relative L2 mismatch', ...
                    ExternalCollectorDispatcher.pick_struct_number(summary_metrics, 'fd_vs_spectral_streamfunction_mismatch_l2', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(summary_metrics, 'spectral_vs_fd_streamfunction_mismatch_l2', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(summary_metrics, 'mean_cross_method_streamfunction_mismatch_l2', NaN), ...
                    '', '', ''; ...
                'Speed relative L2 mismatch', ...
                    ExternalCollectorDispatcher.pick_struct_number(summary_metrics, 'fd_vs_spectral_speed_mismatch_l2', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(summary_metrics, 'spectral_vs_fd_speed_mismatch_l2', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(summary_metrics, 'mean_cross_method_speed_mismatch_l2', NaN), ...
                    '', '', ''; ...
                'Velocity-vector relative L2 mismatch', ...
                    ExternalCollectorDispatcher.pick_struct_number(summary_metrics, 'fd_vs_spectral_velocity_vector_mismatch_l2', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(summary_metrics, 'spectral_vs_fd_velocity_vector_mismatch_l2', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(summary_metrics, 'mean_cross_method_velocity_vector_mismatch_l2', NaN), ...
                    '', '', ''; ...
                'Streamline-direction relative L2 mismatch', ...
                    ExternalCollectorDispatcher.pick_struct_number(summary_metrics, 'fd_vs_spectral_streamline_direction_mismatch_l2', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(summary_metrics, 'spectral_vs_fd_streamline_direction_mismatch_l2', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(summary_metrics, 'mean_cross_method_streamline_direction_mismatch_l2', NaN), ...
                    '', '', ''; ...
                '', '', '', '', '', '', ''; ...
                'Convergence selection summary', '', '', '', '', '', ''; ...
                'Method', 'Selected mesh', 'xi_L2 (%)', 'xi_peak (%)', 'Tolerance (%)', 'Selection reason', 'Fallback used'; ...
                'FD', ...
                    ExternalCollectorDispatcher.phase1_mesh_label(ExternalCollectorDispatcher.pick_nested_struct(results_struct, 'children', 'fd', 'selected_mesh')), ...
                    ExternalCollectorDispatcher.pick_struct_number(fd_metrics, 'mesh_final_successive_vorticity_error', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(fd_metrics, 'mesh_final_peak_vorticity_error', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(fd_metrics, 'mesh_tolerance', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_text(ExternalCollectorDispatcher.pick_nested_struct(results_struct, 'children', 'fd', 'selected_mesh'), 'selection_reason', ''), ...
                    ExternalCollectorDispatcher.yes_no_text(ExternalCollectorDispatcher.pick_struct_value(fd_metrics, 'mesh_fallback_used', false)); ...
                'SM', ...
                    ExternalCollectorDispatcher.phase1_mesh_label(ExternalCollectorDispatcher.pick_nested_struct(results_struct, 'children', 'spectral', 'selected_mesh')), ...
                    ExternalCollectorDispatcher.pick_struct_number(sm_metrics, 'mesh_final_successive_vorticity_error', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(sm_metrics, 'mesh_final_peak_vorticity_error', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(sm_metrics, 'mesh_tolerance', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_text(ExternalCollectorDispatcher.pick_nested_struct(results_struct, 'children', 'spectral', 'selected_mesh'), 'selection_reason', ''), ...
                    ExternalCollectorDispatcher.yes_no_text(ExternalCollectorDispatcher.pick_struct_value(sm_metrics, 'mesh_fallback_used', false)); ...
                '', '', '', '', '', '', ''; ...
                'Cross-method mismatch by initial condition', '', '', '', '', '', ''; ...
                'Case', 'FD Rel L2', 'SM Rel L2', 'FD Rel L_inf', 'SM Rel L_inf', 'Group', ''; ...
                baseline_label, ...
                    ExternalCollectorDispatcher.pick_struct_number(fd_metrics, 'cross_method_mismatch_l2', ...
                        ExternalCollectorDispatcher.pick_struct_number(summary_metrics, 'fd_vs_spectral_mismatch_l2', NaN)), ...
                    ExternalCollectorDispatcher.pick_struct_number(sm_metrics, 'cross_method_mismatch_l2', ...
                        ExternalCollectorDispatcher.pick_struct_number(summary_metrics, 'spectral_vs_fd_mismatch_l2', NaN)), ...
                    ExternalCollectorDispatcher.pick_struct_number(fd_metrics, 'cross_method_mismatch_linf', ...
                        ExternalCollectorDispatcher.pick_struct_number(summary_metrics, 'fd_vs_spectral_mismatch_linf', NaN)), ...
                    ExternalCollectorDispatcher.pick_struct_number(sm_metrics, 'cross_method_mismatch_linf', ...
                        ExternalCollectorDispatcher.pick_struct_number(summary_metrics, 'spectral_vs_fd_mismatch_linf', NaN)), ...
                    baseline_group, ''; ...
                '', '', '', '', '', '', ''; ...
                'Conservation drift by initial condition', '', '', '', '', '', ''; ...
                'Case', 'Metric', 'FD', 'SM', 'Group', '', ''; ...
                baseline_label, 'Kinetic energy', ...
                    ExternalCollectorDispatcher.pick_struct_number(fd_metrics, 'kinetic_energy_drift', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(sm_metrics, 'kinetic_energy_drift', NaN), baseline_group, '', ''; ...
                baseline_label, 'Enstrophy', ...
                    ExternalCollectorDispatcher.pick_struct_number(fd_metrics, 'enstrophy_drift', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(sm_metrics, 'enstrophy_drift', NaN), baseline_group, '', ''; ...
                baseline_label, 'Circulation', ...
                    ExternalCollectorDispatcher.pick_struct_number(fd_metrics, 'circulation_drift', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(sm_metrics, 'circulation_drift', NaN), baseline_group, '', ''; ...
                '', '', '', '', '', '', ''; ...
                'Vortex preservation error', '', '', '', '', '', ''; ...
                'Metric', 'FD', 'SM', '', '', '', ''; ...
                'Peak ratio error', ...
                    ExternalCollectorDispatcher.pick_struct_number(fd_metrics, 'peak_vorticity_ratio_error', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(sm_metrics, 'peak_vorticity_ratio_error', NaN), '', '', '', ''; ...
                'Centroid drift', ...
                    ExternalCollectorDispatcher.pick_struct_number(fd_metrics, 'centroid_drift', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(sm_metrics, 'centroid_drift', NaN), '', '', '', ''; ...
                '', '', '', '', '', '', ''; ...
                'Error vs Time for Different Metrics', '', '', '', '', '', ''; ...
                'Time (s)', 'MSE', 'RMSE', 'Vorticity L2', 'Peak Vorticity', 'Circulation', 'Kinetic Energy'};
            case_entries = ExternalCollectorDispatcher.order_phase1_ic_cases( ...
                ExternalCollectorDispatcher.pick_struct_value(ic_study, 'cases', struct([])));
            if isstruct(case_entries)
                for i = 1:numel(case_entries)
                    case_label = ExternalCollectorDispatcher.phase1_case_display_label(case_entries(i), sprintf('Case %d', i));
                    group_label = ExternalCollectorDispatcher.pick_struct_text(case_entries(i), 'group_label', '');
                    fd_case = ExternalCollectorDispatcher.pick_struct_field(case_entries(i), 'fd');
                    sm_case = ExternalCollectorDispatcher.pick_struct_field(case_entries(i), 'spectral');
                    fd_case_metrics = ExternalCollectorDispatcher.pick_struct_field(fd_case, 'metrics');
                    sm_case_metrics = ExternalCollectorDispatcher.pick_struct_field(sm_case, 'metrics');
                    cells_out(end + 1, 1:7) = { ... %#ok<AGROW>
                        case_label, ...
                        ExternalCollectorDispatcher.pick_struct_number(fd_case_metrics, 'cross_method_mismatch_l2', NaN), ...
                        ExternalCollectorDispatcher.pick_struct_number(sm_case_metrics, 'cross_method_mismatch_l2', NaN), ...
                        ExternalCollectorDispatcher.pick_struct_number(fd_case_metrics, 'cross_method_mismatch_linf', NaN), ...
                        ExternalCollectorDispatcher.pick_struct_number(sm_case_metrics, 'cross_method_mismatch_linf', NaN), ...
                        group_label, ''};
                    cells_out(end + 1, 1:7) = {case_label, 'Kinetic energy', ...
                        ExternalCollectorDispatcher.pick_struct_number(fd_case_metrics, 'kinetic_energy_drift', NaN), ...
                        ExternalCollectorDispatcher.pick_struct_number(sm_case_metrics, 'kinetic_energy_drift', NaN), ...
                        group_label, '', ''}; %#ok<AGROW>
                    cells_out(end + 1, 1:7) = {case_label, 'Enstrophy', ...
                        ExternalCollectorDispatcher.pick_struct_number(fd_case_metrics, 'enstrophy_drift', NaN), ...
                        ExternalCollectorDispatcher.pick_struct_number(sm_case_metrics, 'enstrophy_drift', NaN), ...
                        group_label, '', ''}; %#ok<AGROW>
                    cells_out(end + 1, 1:7) = {case_label, 'Circulation', ...
                        ExternalCollectorDispatcher.pick_struct_number(fd_case_metrics, 'circulation_drift', NaN), ...
                        ExternalCollectorDispatcher.pick_struct_number(sm_case_metrics, 'circulation_drift', NaN), ...
                        group_label, '', ''}; %#ok<AGROW>
                end
            end
            cells_out(end + 1, 1:7) = {'', '', '', '', '', '', ''}; %#ok<AGROW>
            cells_out(end + 1, 1:7) = {'Cross-method field mismatch by initial condition', '', '', '', '', '', ''}; %#ok<AGROW>
            cells_out(end + 1, 1:7) = {'Case', 'Metric', 'FD', 'SM', 'Group', '', ''}; %#ok<AGROW>
            field_metric_specs = { ...
                {'Vorticity relative L2 mismatch', 'cross_method_mismatch_l2'}; ...
                {'Streamfunction relative L2 mismatch', 'cross_method_streamfunction_relative_l2_mismatch'}; ...
                {'Speed relative L2 mismatch', 'cross_method_speed_relative_l2_mismatch'}; ...
                {'Velocity-vector relative L2 mismatch', 'cross_method_velocity_vector_relative_l2_mismatch'}; ...
                {'Streamline-direction relative L2 mismatch', 'cross_method_streamline_direction_relative_l2_mismatch'}};
            baseline_metric_sources = {fd_metrics, sm_metrics};
            for i = 1:numel(field_metric_specs)
                cells_out(end + 1, 1:7) = { ... %#ok<AGROW>
                    baseline_label, field_metric_specs{i}{1}, ...
                    ExternalCollectorDispatcher.pick_struct_number(baseline_metric_sources{1}, field_metric_specs{i}{2}, NaN), ...
                    ExternalCollectorDispatcher.pick_struct_number(baseline_metric_sources{2}, field_metric_specs{i}{2}, NaN), ...
                    baseline_group, '', ''};
            end
            if isstruct(case_entries)
                for ci = 1:numel(case_entries)
                    case_label = ExternalCollectorDispatcher.phase1_case_display_label(case_entries(ci), sprintf('Case %d', ci));
                    group_label = ExternalCollectorDispatcher.pick_struct_text(case_entries(ci), 'group_label', '');
                    fd_case_metrics = ExternalCollectorDispatcher.pick_struct_field( ...
                        ExternalCollectorDispatcher.pick_struct_field(case_entries(ci), 'fd'), 'metrics');
                    sm_case_metrics = ExternalCollectorDispatcher.pick_struct_field( ...
                        ExternalCollectorDispatcher.pick_struct_field(case_entries(ci), 'spectral'), 'metrics');
                    for i = 1:numel(field_metric_specs)
                        cells_out(end + 1, 1:7) = { ... %#ok<AGROW>
                            case_label, field_metric_specs{i}{1}, ...
                            ExternalCollectorDispatcher.pick_struct_number(fd_case_metrics, field_metric_specs{i}{2}, NaN), ...
                            ExternalCollectorDispatcher.pick_struct_number(sm_case_metrics, field_metric_specs{i}{2}, NaN), ...
                            group_label, '', ''};
                    end
                end
            end
            error_times = reshape(double(ExternalCollectorDispatcher.pick_struct_value(error_payload, 'time_s', [])), 1, []);
            mse_series = reshape(double(ExternalCollectorDispatcher.pick_struct_value(error_payload, 'mse', [])), 1, []);
            rmse_series = reshape(double(ExternalCollectorDispatcher.pick_struct_value(error_payload, 'rmse', ...
                ExternalCollectorDispatcher.pick_struct_value(error_payload, 'relative_rmse', []))), 1, []);
            vorticity_l2 = reshape(double(ExternalCollectorDispatcher.pick_struct_value(error_payload, ...
                'vorticity_vector_relative_l2_mismatch', ExternalCollectorDispatcher.pick_struct_value(error_payload, 'relative_l2_mismatch', []))), 1, []);
            streamfunction_l2 = reshape(double(ExternalCollectorDispatcher.pick_struct_value(error_payload, 'streamfunction_relative_l2_mismatch', [])), 1, []);
            speed_l2 = reshape(double(ExternalCollectorDispatcher.pick_struct_value(error_payload, 'speed_relative_l2_mismatch', [])), 1, []);
            velocity_vector_l2 = reshape(double(ExternalCollectorDispatcher.pick_struct_value(error_payload, 'velocity_vector_relative_l2_mismatch', [])), 1, []);
            streamline_direction_l2 = reshape(double(ExternalCollectorDispatcher.pick_struct_value(error_payload, 'streamline_direction_relative_l2_mismatch', [])), 1, []);
            peak_series = reshape(double(ExternalCollectorDispatcher.pick_struct_value(error_payload, 'peak_vorticity_relative_error', [])), 1, []);
            circulation_series = reshape(double(ExternalCollectorDispatcher.pick_struct_value(error_payload, 'circulation_relative_error', [])), 1, []);
            kinetic_energy_series = reshape(double(ExternalCollectorDispatcher.pick_struct_value(error_payload, 'kinetic_energy_relative_error', [])), 1, []);
            enstrophy_series = reshape(double(ExternalCollectorDispatcher.pick_struct_value(error_payload, 'enstrophy_relative_error', [])), 1, []);
            error_count = max([numel(error_times), numel(mse_series), numel(rmse_series), numel(vorticity_l2), ...
                numel(peak_series), numel(circulation_series), numel(kinetic_energy_series), numel(enstrophy_series), 0]);
            for i = 1:error_count
                cells_out(end + 1, 1:7) = { ... %#ok<AGROW>
                    ExternalCollectorDispatcher.numeric_cell(error_times, i), ...
                    ExternalCollectorDispatcher.numeric_cell(mse_series, i), ...
                    ExternalCollectorDispatcher.numeric_cell(rmse_series, i), ...
                    ExternalCollectorDispatcher.numeric_cell(vorticity_l2, i), ...
                    ExternalCollectorDispatcher.numeric_cell(peak_series, i), ...
                    ExternalCollectorDispatcher.numeric_cell(circulation_series, i), ...
                    ExternalCollectorDispatcher.numeric_cell(kinetic_energy_series, i)};
            end
            cells_out(end + 1, 1:7) = {'', '', '', '', '', '', ''}; %#ok<AGROW>
            cells_out(end + 1, 1:7) = {'Cross-method evolution field mismatch', '', '', '', '', '', ''}; %#ok<AGROW>
            cells_out(end + 1, 1:7) = {'Time (s)', 'Streamfunction L2', 'Speed L2', 'Velocity-vector L2', 'Streamline-direction L2', 'Enstrophy', ''}; %#ok<AGROW>
            error_count = max([numel(error_times), numel(streamfunction_l2), numel(speed_l2), numel(velocity_vector_l2), ...
                numel(streamline_direction_l2), numel(enstrophy_series), 0]);
            for i = 1:error_count
                cells_out(end + 1, 1:7) = { ... %#ok<AGROW>
                    ExternalCollectorDispatcher.numeric_cell(error_times, i), ...
                    ExternalCollectorDispatcher.numeric_cell(streamfunction_l2, i), ...
                    ExternalCollectorDispatcher.numeric_cell(speed_l2, i), ...
                    ExternalCollectorDispatcher.numeric_cell(velocity_vector_l2, i), ...
                    ExternalCollectorDispatcher.numeric_cell(streamline_direction_l2, i), ...
                    ExternalCollectorDispatcher.numeric_cell(enstrophy_series, i), ...
                    ''};
            end
            cells_out(end + 1, 1:7) = {'', '', '', '', '', '', ''}; %#ok<AGROW>
            cells_out(end + 1, 1:7) = {'IC-study runtime by case', '', '', '', '', '', ''}; %#ok<AGROW>
            cells_out(end + 1, 1:4) = {'Case', 'FD runtime (s)', 'SM runtime (s)', 'Group'}; %#ok<AGROW>
            cells_out(end + 1, 1:4) = { ... %#ok<AGROW>
                baseline_label, ...
                ExternalCollectorDispatcher.pick_struct_number(fd_metrics, 'runtime_wall_s', ...
                    ExternalCollectorDispatcher.pick_struct_number(fd_child, 'runtime_wall_s', NaN)), ...
                ExternalCollectorDispatcher.pick_struct_number(sm_metrics, 'runtime_wall_s', ...
                    ExternalCollectorDispatcher.pick_struct_number(sm_child, 'runtime_wall_s', NaN)), ...
                baseline_group};
            if isstruct(case_entries)
                for i = 1:numel(case_entries)
                    fd_case = ExternalCollectorDispatcher.pick_struct_field(case_entries(i), 'fd');
                    sm_case = ExternalCollectorDispatcher.pick_struct_field(case_entries(i), 'spectral');
                    cells_out(end + 1, 1:4) = { ... %#ok<AGROW>
                        ExternalCollectorDispatcher.phase1_case_display_label(case_entries(i), sprintf('Case %d', i)), ...
                        ExternalCollectorDispatcher.pick_struct_number(fd_case, 'runtime_wall_s', NaN), ...
                        ExternalCollectorDispatcher.pick_struct_number(sm_case, 'runtime_wall_s', NaN), ...
                        ExternalCollectorDispatcher.pick_struct_text(case_entries(i), 'group_label', '')};
                end
            end
        end

        function table_out = build_phase1_convergence_workbook_table(summary_context, stage_summary)
            rows = repmat(struct( ...
                'Method', "", ...
                'Level', "", ...
                'Nx', NaN, ...
                'Ny', NaN, ...
                'delta', NaN, ...
                'xi_L2_pct', NaN, ...
                'xi_peak_pct', NaN, ...
                'Tolerance_pct', NaN, ...
                'L2ToleranceMet', "", ...
                'PeakToleranceMet', "", ...
                'JointToleranceMet', "", ...
                'SelectedMesh', "", ...
                'FallbackSelected', "", ...
                'SelectionReason', "", ...
                'dt_advection', NaN, ...
                'dt_diffusion', NaN, ...
                'dt_cfl', NaN, ...
                'dt_used', NaN, ...
                'cfl_advection', NaN, ...
                'cfl_diffusion', NaN, ...
                'cfl_observed', NaN, ...
                'cfl_terminal', NaN, ...
                'Runtime_s', NaN, ...
                'KineticEnergyDrift', NaN, ...
                'EnstrophyDrift', NaN, ...
                'CirculationDrift', NaN, ...
                'FinalEnergy', NaN, ...
                'FinalEnstrophy', NaN, ...
                'FinalCirculation', NaN, ...
                'MeanTotalPower_W', NaN, ...
                'PeakTotalPower_W', NaN, ...
                'Energy_Wh', NaN, ...
                'CO2_g', NaN, ...
                'SourcePath', ""), 1, 0);
            results_struct = ExternalCollectorDispatcher.pick_struct_field(summary_context, 'results');
            children = ExternalCollectorDispatcher.pick_struct_field(results_struct, 'children');
            method_specs = { ...
                struct('key', 'fd', 'label', 'FD'), ...
                struct('key', 'spectral', 'label', 'SM')};
            for mi = 1:numel(method_specs)
                child = ExternalCollectorDispatcher.pick_struct_field(children, method_specs{mi}.key);
                convergence_output = ExternalCollectorDispatcher.pick_struct_field(child, 'convergence_output');
                convergence_results = ExternalCollectorDispatcher.pick_struct_field(convergence_output, 'results');
                run_records = ExternalCollectorDispatcher.pick_struct_value(convergence_results, 'run_records', struct([]));
                if ~isstruct(run_records)
                    continue;
                end
                for ri = 1:numel(run_records)
                    record = run_records(ri);
                    stage_row = ExternalCollectorDispatcher.phase1_find_convergence_stage_row( ...
                        stage_summary, method_specs{mi}.label, ...
                        ExternalCollectorDispatcher.pick_struct_number(record, 'mesh_level_index', ri), ...
                        ExternalCollectorDispatcher.pick_struct_number(record, 'Nx', NaN), ...
                        ExternalCollectorDispatcher.pick_struct_number(record, 'Ny', NaN));
                    rows(end + 1) = struct( ... %#ok<AGROW>
                        'Method', string(method_specs{mi}.label), ...
                        'Level', string(ExternalCollectorDispatcher.pick_struct_text(record, 'mesh_level_label', sprintf('L%02d', ri))), ...
                        'Nx', ExternalCollectorDispatcher.pick_struct_number(record, 'Nx', NaN), ...
                        'Ny', ExternalCollectorDispatcher.pick_struct_number(record, 'Ny', NaN), ...
                        'delta', ExternalCollectorDispatcher.pick_struct_number(record, 'delta', NaN), ...
                        'xi_L2_pct', ExternalCollectorDispatcher.pick_struct_number(record, 'xi', NaN), ...
                        'xi_peak_pct', ExternalCollectorDispatcher.pick_struct_number(record, 'max_vorticity_rel_error_pct', NaN), ...
                        'Tolerance_pct', ExternalCollectorDispatcher.pick_struct_number(record, 'xi_tol', NaN), ...
                        'L2ToleranceMet', string(ExternalCollectorDispatcher.yes_no_text(ExternalCollectorDispatcher.pick_struct_value(record, 'xi_l2_tol_met', false))), ...
                        'PeakToleranceMet', string(ExternalCollectorDispatcher.yes_no_text(ExternalCollectorDispatcher.pick_struct_value(record, 'xi_peak_tol_met', false))), ...
                        'JointToleranceMet', string(ExternalCollectorDispatcher.yes_no_text(ExternalCollectorDispatcher.pick_struct_value(record, 'joint_tolerance_met', false))), ...
                        'SelectedMesh', string(ExternalCollectorDispatcher.yes_no_text(ExternalCollectorDispatcher.pick_struct_value(record, 'selected_level', false))), ...
                        'FallbackSelected', string(ExternalCollectorDispatcher.yes_no_text(ExternalCollectorDispatcher.pick_struct_value(record, 'fallback_selected', false))), ...
                        'SelectionReason', string(ExternalCollectorDispatcher.pick_struct_text(record, 'selection_reason', '')), ...
                        'dt_advection', ExternalCollectorDispatcher.pick_struct_number(record, 'dt_adv', NaN), ...
                        'dt_diffusion', ExternalCollectorDispatcher.pick_struct_number(record, 'dt_diff', NaN), ...
                        'dt_cfl', ExternalCollectorDispatcher.pick_struct_number(record, 'dt_final', NaN), ...
                        'dt_used', ExternalCollectorDispatcher.pick_struct_number(record, 'dt_used', NaN), ...
                        'cfl_advection', ExternalCollectorDispatcher.pick_struct_number(record, 'cfl_adv', NaN), ...
                        'cfl_diffusion', ExternalCollectorDispatcher.pick_struct_number(record, 'cfl_diff', NaN), ...
                        'cfl_observed', ExternalCollectorDispatcher.pick_struct_number(record, 'cfl_observed', NaN), ...
                        'cfl_terminal', ExternalCollectorDispatcher.pick_struct_number(record, 'cfl', NaN), ...
                        'Runtime_s', ExternalCollectorDispatcher.pick_struct_number(record, 'runtime_wall_s', NaN), ...
                        'KineticEnergyDrift', ExternalCollectorDispatcher.pick_struct_number(record, 'kinetic_energy_drift', NaN), ...
                        'EnstrophyDrift', ExternalCollectorDispatcher.pick_struct_number(record, 'enstrophy_drift', NaN), ...
                        'CirculationDrift', ExternalCollectorDispatcher.pick_struct_number(record, 'circulation_drift', NaN), ...
                        'FinalEnergy', ExternalCollectorDispatcher.pick_struct_number(record, 'final_energy', NaN), ...
                        'FinalEnstrophy', ExternalCollectorDispatcher.pick_struct_number(record, 'final_enstrophy', NaN), ...
                        'FinalCirculation', ExternalCollectorDispatcher.pick_struct_number(record, 'final_circulation', NaN), ...
                        'MeanTotalPower_W', ExternalCollectorDispatcher.table_row_number(stage_row, 1, 'mean_total_power_w', NaN), ...
                        'PeakTotalPower_W', ExternalCollectorDispatcher.table_row_number(stage_row, 1, 'peak_total_power_w', NaN), ...
                        'Energy_Wh', ExternalCollectorDispatcher.table_row_number(stage_row, 1, 'energy_wh_total', NaN), ...
                        'CO2_g', ExternalCollectorDispatcher.table_row_number(stage_row, 1, 'co2_g_total', NaN), ...
                        'SourcePath', string(ExternalCollectorDispatcher.pick_struct_text(record, 'mesh_level_summary_path', ExternalCollectorDispatcher.pick_struct_text(record, 'data_path', ''))));
                end
            end
            table_out = struct2table(rows);
        end

        function table_out = build_phase1_ic_summary_workbook_table(summary_context, method_key)
            results_struct = ExternalCollectorDispatcher.pick_struct_field(summary_context, 'results');
            ic_study = ExternalCollectorDispatcher.pick_struct_field(results_struct, 'ic_study');
            case_entries = ExternalCollectorDispatcher.order_phase1_ic_cases( ...
                ExternalCollectorDispatcher.pick_struct_value(ic_study, 'cases', struct([])));
            children = ExternalCollectorDispatcher.pick_struct_field(results_struct, 'children');
            method_struct = ExternalCollectorDispatcher.pick_struct_field(children, char(string(method_key)));
            metrics_struct = ExternalCollectorDispatcher.pick_struct_field(results_struct, 'metrics');
            metric_field = 'FD';
            if strcmpi(char(string(method_key)), 'spectral')
                metric_field = 'Spectral';
            end
            baseline_metrics = ExternalCollectorDispatcher.pick_struct_field(metrics_struct, metric_field);
            baseline_label = ExternalCollectorDispatcher.phase1_baseline_display_label(ic_study);
            baseline_group = ExternalCollectorDispatcher.pick_struct_text(ic_study, 'selected_group', baseline_label);
            rows = repmat(struct( ...
                'Case', "", ...
                'Group', "", ...
                'SelectedMesh', "", ...
                'Runtime_s', NaN, ...
                'TotalSteps', NaN, ...
                'PeakVorticityRatio', NaN, ...
                'CentroidDrift', NaN, ...
                'KineticEnergyDrift', NaN, ...
                'EnstrophyDrift', NaN, ...
                'CirculationDrift', NaN, ...
                'FinalEnergy', NaN, ...
                'FinalEnstrophy', NaN, ...
                'FinalCirculation', NaN, ...
                'RunID', ""), 1, 0);
            rows(end + 1) = struct( ... %#ok<AGROW>
                'Case', string(baseline_label), ...
                'Group', string(baseline_group), ...
                'SelectedMesh', string(ExternalCollectorDispatcher.pick_struct_text(baseline_metrics, 'selected_mesh_label', ...
                    ExternalCollectorDispatcher.phase1_mesh_label(ExternalCollectorDispatcher.pick_struct_field(method_struct, 'selected_mesh')))), ...
                'Runtime_s', ExternalCollectorDispatcher.pick_struct_number(baseline_metrics, 'runtime_wall_s', ...
                    ExternalCollectorDispatcher.pick_struct_number(method_struct, 'runtime_wall_s', NaN)), ...
                'TotalSteps', ExternalCollectorDispatcher.pick_struct_number(baseline_metrics, 'total_steps', NaN), ...
                'PeakVorticityRatio', ExternalCollectorDispatcher.pick_struct_number(baseline_metrics, 'peak_vorticity_ratio', NaN), ...
                'CentroidDrift', ExternalCollectorDispatcher.pick_struct_number(baseline_metrics, 'centroid_drift', NaN), ...
                'KineticEnergyDrift', ExternalCollectorDispatcher.pick_struct_number(baseline_metrics, 'kinetic_energy_drift', NaN), ...
                'EnstrophyDrift', ExternalCollectorDispatcher.pick_struct_number(baseline_metrics, 'enstrophy_drift', NaN), ...
                'CirculationDrift', ExternalCollectorDispatcher.pick_struct_number(baseline_metrics, 'circulation_drift', NaN), ...
                'FinalEnergy', ExternalCollectorDispatcher.pick_struct_number(baseline_metrics, 'final_energy', NaN), ...
                'FinalEnstrophy', ExternalCollectorDispatcher.pick_struct_number(baseline_metrics, 'final_enstrophy', NaN), ...
                'FinalCirculation', ExternalCollectorDispatcher.pick_struct_number(baseline_metrics, 'final_circulation', NaN), ...
                'RunID', string(ExternalCollectorDispatcher.pick_struct_text( ...
                    ExternalCollectorDispatcher.pick_struct_field(method_struct, 'evolution_output'), 'run_id', ...
                    ExternalCollectorDispatcher.pick_struct_text(method_struct, 'run_id', ''))));
            if isstruct(case_entries)
                for i = 1:numel(case_entries)
                    method_struct = ExternalCollectorDispatcher.pick_struct_field(case_entries(i), char(string(method_key)));
                    metrics = ExternalCollectorDispatcher.pick_struct_field(method_struct, 'metrics');
                    rows(end + 1) = struct( ... %#ok<AGROW>
                        'Case', string(ExternalCollectorDispatcher.phase1_case_display_label(case_entries(i), sprintf('Case %d', i))), ...
                        'Group', string(ExternalCollectorDispatcher.pick_struct_text(case_entries(i), 'group_label', '')), ...
                        'SelectedMesh', string(ExternalCollectorDispatcher.pick_struct_text(metrics, 'selected_mesh_label', '')), ...
                        'Runtime_s', ExternalCollectorDispatcher.pick_struct_number(metrics, 'runtime_wall_s', ExternalCollectorDispatcher.pick_struct_number(method_struct, 'runtime_wall_s', NaN)), ...
                        'TotalSteps', ExternalCollectorDispatcher.pick_struct_number(metrics, 'total_steps', NaN), ...
                        'PeakVorticityRatio', ExternalCollectorDispatcher.pick_struct_number(metrics, 'peak_vorticity_ratio', NaN), ...
                        'CentroidDrift', ExternalCollectorDispatcher.pick_struct_number(metrics, 'centroid_drift', NaN), ...
                        'KineticEnergyDrift', ExternalCollectorDispatcher.pick_struct_number(metrics, 'kinetic_energy_drift', NaN), ...
                        'EnstrophyDrift', ExternalCollectorDispatcher.pick_struct_number(metrics, 'enstrophy_drift', NaN), ...
                        'CirculationDrift', ExternalCollectorDispatcher.pick_struct_number(metrics, 'circulation_drift', NaN), ...
                        'FinalEnergy', ExternalCollectorDispatcher.pick_struct_number(metrics, 'final_energy', NaN), ...
                        'FinalEnstrophy', ExternalCollectorDispatcher.pick_struct_number(metrics, 'final_enstrophy', NaN), ...
                        'FinalCirculation', ExternalCollectorDispatcher.pick_struct_number(metrics, 'final_circulation', NaN), ...
                        'RunID', string(ExternalCollectorDispatcher.pick_struct_text(method_struct, 'run_id', '')));
                end
            end
            table_out = struct2table(rows);
        end

        function table_out = build_phase1_sustainability_processed_table(stage_summary, monitor_series, raw_csv_path)
            table_out = ExternalCollectorDispatcher.build_workflow_sustainability_processed_table( ...
                stage_summary, monitor_series, raw_csv_path, 'Phase 1 total');
        end

        function table_out = build_mesh_convergence_sustainability_processed_table(stage_summary, monitor_series, raw_csv_path)
            table_out = ExternalCollectorDispatcher.build_workflow_sustainability_processed_table( ...
                stage_summary, monitor_series, raw_csv_path, 'Mesh Convergence total');
        end

        function table_out = build_workflow_sustainability_processed_table(stage_summary, monitor_series, raw_csv_path, overall_label)
            if nargin < 2 || ~isstruct(monitor_series)
                monitor_series = struct();
            end
            if nargin < 3
                raw_csv_path = '';
            end
            if nargin < 4 || strlength(string(overall_label)) == 0
                overall_label = 'Workflow total';
            end
            rows = repmat(struct( ...
                'scope', "", ...
                'stage_id', "", ...
                'stage_label', "", ...
                'stage_method', "", ...
                'stage_type', "", ...
                'substage_id', "", ...
                'substage_label', "", ...
                'substage_type', "", ...
                'wall_time_s', NaN, ...
                'mean_total_power_w', NaN, ...
                'peak_total_power_w', NaN, ...
                'energy_wh_total', NaN, ...
                'co2_g_total', NaN, ...
                'energy_per_iteration_wh', NaN, ...
                'energy_per_sim_second_wh', NaN, ...
                'energy_per_cell_step_wh', NaN, ...
                'telemetry_enabled', "", ...
                'telemetry_disable_reason', "", ...
                'hwinfo_control_mode', "", ...
                'raw_hwinfo_csv_path', "", ...
                'note', ""), 1, 0);
            safe_stage_summary = ExternalCollectorDispatcher.safe_table(stage_summary);
            if ~isempty(safe_stage_summary)
                for i = 1:height(safe_stage_summary)
                    rows(end + 1) = struct( ... %#ok<AGROW>
                        'scope', "stage", ...
                        'stage_id', string(ExternalCollectorDispatcher.table_row_text(safe_stage_summary, i, 'stage_id', '')), ...
                        'stage_label', string(ExternalCollectorDispatcher.table_row_text(safe_stage_summary, i, 'stage_label', '')), ...
                        'stage_method', string(ExternalCollectorDispatcher.table_row_text(safe_stage_summary, i, 'stage_method', '')), ...
                        'stage_type', string(ExternalCollectorDispatcher.table_row_text(safe_stage_summary, i, 'stage_type', '')), ...
                        'substage_id', string(ExternalCollectorDispatcher.table_row_text(safe_stage_summary, i, 'substage_id', '')), ...
                        'substage_label', string(ExternalCollectorDispatcher.table_row_text(safe_stage_summary, i, 'substage_label', '')), ...
                        'substage_type', string(ExternalCollectorDispatcher.table_row_text(safe_stage_summary, i, 'substage_type', '')), ...
                        'wall_time_s', ExternalCollectorDispatcher.table_row_number(safe_stage_summary, i, 'wall_time_s', NaN), ...
                        'mean_total_power_w', ExternalCollectorDispatcher.table_row_number(safe_stage_summary, i, 'mean_total_power_w', NaN), ...
                        'peak_total_power_w', ExternalCollectorDispatcher.table_row_number(safe_stage_summary, i, 'peak_total_power_w', NaN), ...
                        'energy_wh_total', ExternalCollectorDispatcher.table_row_number(safe_stage_summary, i, 'energy_wh_total', NaN), ...
                        'co2_g_total', ExternalCollectorDispatcher.table_row_number(safe_stage_summary, i, 'co2_g_total', NaN), ...
                        'energy_per_iteration_wh', ExternalCollectorDispatcher.table_row_number(safe_stage_summary, i, 'energy_per_iteration_wh', NaN), ...
                        'energy_per_sim_second_wh', ExternalCollectorDispatcher.table_row_number(safe_stage_summary, i, 'energy_per_sim_second_wh', NaN), ...
                        'energy_per_cell_step_wh', ExternalCollectorDispatcher.table_row_number(safe_stage_summary, i, 'energy_per_cell_step_wh', NaN), ...
                        'telemetry_enabled', "", ...
                        'telemetry_disable_reason', "", ...
                        'hwinfo_control_mode', "", ...
                        'raw_hwinfo_csv_path', "", ...
                        'note', "");
                end
                methods = unique(string(safe_stage_summary.stage_method));
                methods = methods(strlength(strtrim(methods)) > 0);
                for i = 1:numel(methods)
                    mask = strcmpi(string(safe_stage_summary.stage_method), methods(i));
                    block = safe_stage_summary(mask, :);
                    rows(end + 1) = ExternalCollectorDispatcher.aggregate_phase1_sustainability_row("method_total", ...
                        "", sprintf('%s total', char(methods(i))), char(methods(i)), "", block); %#ok<AGROW>
                end
                rows(end + 1) = ExternalCollectorDispatcher.aggregate_phase1_sustainability_row("overall_total", ...
                    "", char(string(overall_label)), "", "", safe_stage_summary); %#ok<AGROW>
            elseif ~ExternalCollectorDispatcher.telemetry_enabled(monitor_series)
                rows(end + 1) = ExternalCollectorDispatcher.phase1_sustainability_disabled_row( ...
                    monitor_series, raw_csv_path); %#ok<AGROW>
            end
            table_out = struct2table(rows);
        end

        function table_out = build_phase1_plotting_data_table(summary_context, stage_summary)
            rows = ExternalCollectorDispatcher.build_phase1_plotting_data_rows(summary_context, stage_summary);
            if isempty(rows)
                table_out = table();
                return;
            end
            table_out = struct2table(rows);
            if isempty(table_out)
                return;
            end
            keep_idx = ~contains(lower(string(table_out.figure_id)), "3x3");
            table_out = table_out(keep_idx, :);
            sort_vars = {'figure_id', 'stage_type', 'x_order', 'series_id', 'x_value', 'x_category'};
            sort_vars = sort_vars(ismember(sort_vars, table_out.Properties.VariableNames));
            if ~isempty(sort_vars)
                table_out = sortrows(table_out, sort_vars);
            end
        end

        function table_out = build_mesh_convergence_plotting_data_table(summary_context, stage_summary)
            rows = ExternalCollectorDispatcher.build_mesh_convergence_plotting_data_rows(summary_context, stage_summary);
            if isempty(rows)
                table_out = table();
                return;
            end
            table_out = struct2table(rows);
            sort_vars = {'figure_id', 'stage_type', 'x_order', 'series_id', 'x_value', 'x_category'};
            sort_vars = sort_vars(ismember(sort_vars, table_out.Properties.VariableNames));
            if ~isempty(sort_vars)
                table_out = sortrows(table_out, sort_vars);
            end
        end

        function rows = build_mesh_convergence_plotting_data_rows(summary_context, stage_summary)
            rows = repmat(ExternalCollectorDispatcher.empty_plotting_row(), 1, 0);
            results_struct = ExternalCollectorDispatcher.pick_struct_field(summary_context, 'results');
            if isempty(fieldnames(results_struct))
                return;
            end

            colors = ResultsPlotDispatcher.default_light_colors();
            sm_abs_color = [0.08, 0.48, 0.44];
            if isfield(colors, 'quaternary') && isnumeric(colors.quaternary) && numel(colors.quaternary) == 3
                sm_abs_color = double(reshape(colors.quaternary, 1, 3));
            elseif isfield(colors, 'tertiary') && isnumeric(colors.tertiary) && numel(colors.tertiary) == 3
                sm_abs_color = max(0, double(reshape(colors.tertiary, 1, 3)) - 0.12);
            end

            children = ExternalCollectorDispatcher.pick_struct_field(results_struct, 'children');
            method_specs = { ...
                struct('child_key', 'fd', 'label', 'FD', 'series_prefix', 'fd', 'l2_color', colors.primary, 'peak_color', colors.secondary), ...
                struct('child_key', 'spectral', 'label', 'SM', 'series_prefix', 'sm', 'l2_color', colors.tertiary, 'peak_color', sm_abs_color)};
            for mi = 1:numel(method_specs)
                child = ExternalCollectorDispatcher.pick_struct_field(children, method_specs{mi}.child_key);
                convergence_output = ExternalCollectorDispatcher.pick_struct_field(child, 'convergence_output');
                convergence_results = ExternalCollectorDispatcher.pick_struct_field(convergence_output, 'results');
                records = ExternalCollectorDispatcher.pick_struct_value(convergence_results, 'run_records', struct([]));
                if ~isstruct(records)
                    continue;
                end
                for ri = 1:numel(records)
                    record = records(ri);
                    mesh_n = ExternalCollectorDispatcher.pick_struct_number(record, 'Nx', NaN);
                    mesh_label = ExternalCollectorDispatcher.pick_struct_text(record, 'mesh_level_label', sprintf('L%02d', ri));
                    mesh_category = sprintf('%s / %d', mesh_label, round(mesh_n));
                    rows = ExternalCollectorDispatcher.append_plotting_line_row(rows, ...
                        'mesh_convergence_comparison', 'Mesh Convergence Comparison', 'line', ...
                        sprintf('%s_l2', method_specs{mi}.series_prefix), sprintf('%s L2', method_specs{mi}.label), ...
                        method_specs{mi}.label, '', 'convergence', mesh_n, ...
                        ExternalCollectorDispatcher.pick_struct_number(record, 'xi', NaN), ...
                        'Mesh N', 'Convergence error (%)', '', '%', ...
                        ExternalCollectorDispatcher.rgb_to_hex(method_specs{mi}.l2_color), 'o', '-', ...
                        sprintf('Results.children.%s.convergence_output.results.run_records(%d).xi', method_specs{mi}.child_key, ri), ...
                        'joint_tolerance_metric');
                    rows = ExternalCollectorDispatcher.append_plotting_line_row(rows, ...
                        'mesh_convergence_comparison', 'Mesh Convergence Comparison', 'line', ...
                        sprintf('%s_peak', method_specs{mi}.series_prefix), sprintf('%s peak', method_specs{mi}.label), ...
                        method_specs{mi}.label, '', 'convergence', mesh_n, ...
                        ExternalCollectorDispatcher.pick_struct_number(record, 'max_vorticity_rel_error_pct', NaN), ...
                        'Mesh N', 'Convergence error (%)', '', '%', ...
                        ExternalCollectorDispatcher.rgb_to_hex(method_specs{mi}.peak_color), 's', '--', ...
                        sprintf('Results.children.%s.convergence_output.results.run_records(%d).max_vorticity_rel_error_pct', method_specs{mi}.child_key, ri), ...
                        'joint_tolerance_metric');
                    rows = ExternalCollectorDispatcher.append_plotting_category_row(rows, ...
                        'mesh_convergence_adaptive_timestep', 'Mesh Convergence Adaptive Timestep', 'grouped_bar', ...
                        sprintf('%s_dt_adv', method_specs{mi}.series_prefix), sprintf('%s dt_advection', method_specs{mi}.label), ...
                        method_specs{mi}.label, '', 'convergence', mesh_category, ri, ...
                        ExternalCollectorDispatcher.pick_struct_number(record, 'dt_adv', NaN), ...
                        'Mesh level / N', 'Timestep value (s)', '', 's', ...
                        ExternalCollectorDispatcher.rgb_to_hex(colors.primary), 'none', '-', ...
                        sprintf('Results.children.%s.convergence_output.results.run_records(%d).dt_adv', method_specs{mi}.child_key, ri), ...
                        'stability_snapshot');
                    rows = ExternalCollectorDispatcher.append_plotting_category_row(rows, ...
                        'mesh_convergence_adaptive_timestep', 'Mesh Convergence Adaptive Timestep', 'grouped_bar', ...
                        sprintf('%s_dt_diff', method_specs{mi}.series_prefix), sprintf('%s dt_diffusion', method_specs{mi}.label), ...
                        method_specs{mi}.label, '', 'convergence', mesh_category, ri, ...
                        ExternalCollectorDispatcher.pick_struct_number(record, 'dt_diff', NaN), ...
                        'Mesh level / N', 'Timestep value (s)', '', 's', ...
                        ExternalCollectorDispatcher.rgb_to_hex(colors.secondary), 'none', '-', ...
                        sprintf('Results.children.%s.convergence_output.results.run_records(%d).dt_diff', method_specs{mi}.child_key, ri), ...
                        'stability_snapshot');
                    rows = ExternalCollectorDispatcher.append_plotting_category_row(rows, ...
                        'mesh_convergence_adaptive_timestep', 'Mesh Convergence Adaptive Timestep', 'grouped_bar', ...
                        sprintf('%s_dt_cfl', method_specs{mi}.series_prefix), sprintf('%s dt_CFL', method_specs{mi}.label), ...
                        method_specs{mi}.label, '', 'convergence', mesh_category, ri, ...
                        ExternalCollectorDispatcher.pick_struct_number(record, 'dt_final', NaN), ...
                        'Mesh level / N', 'Timestep value (s)', '', 's', ...
                        ExternalCollectorDispatcher.rgb_to_hex(colors.tertiary), 'none', '-', ...
                        sprintf('Results.children.%s.convergence_output.results.run_records(%d).dt_final', method_specs{mi}.child_key, ri), ...
                        'stability_snapshot');
                    rows = ExternalCollectorDispatcher.append_plotting_category_row(rows, ...
                        'mesh_convergence_runtime_vs_resolution', 'Mesh Convergence Runtime vs Resolution', 'grouped_bar', ...
                        sprintf('%s_runtime', method_specs{mi}.series_prefix), sprintf('%s runtime', method_specs{mi}.label), ...
                        method_specs{mi}.label, '', 'convergence', sprintf('%d^2', round(mesh_n)), ri, ...
                        ExternalCollectorDispatcher.pick_struct_number(record, 'runtime_wall_s', NaN), ...
                        'Grid Resolution', 'Computational time (s)', '', 's', ...
                        ExternalCollectorDispatcher.rgb_to_hex(method_specs{mi}.l2_color), 'none', '-', ...
                        sprintf('Results.children.%s.convergence_output.results.run_records(%d).runtime_wall_s', method_specs{mi}.child_key, ri), ...
                        'convergence_runtime');
                end
            end

            metrics_struct = ExternalCollectorDispatcher.pick_struct_field(results_struct, 'metrics');
            summary_metrics = ExternalCollectorDispatcher.pick_struct_field(metrics_struct, 'summary');
            cross_method_specs = { ...
                {'mean_cross_method_mismatch_l2', 'Vorticity relative L2 mismatch'}; ...
                {'mean_cross_method_streamfunction_mismatch_l2', 'Streamfunction relative L2 mismatch'}; ...
                {'mean_cross_method_speed_mismatch_l2', 'Speed relative L2 mismatch'}; ...
                {'mean_cross_method_velocity_vector_mismatch_l2', 'Velocity-vector relative L2 mismatch'}; ...
                {'mean_cross_method_streamline_direction_mismatch_l2', 'Streamline-direction relative L2 mismatch'}};
            for i = 1:numel(cross_method_specs)
                rows = ExternalCollectorDispatcher.append_plotting_category_row(rows, ...
                    'mesh_convergence_cross_method_fields', 'Mesh Convergence Selected-Mesh Cross-Method Field Mismatch', 'bar', ...
                    cross_method_specs{i}{1}, cross_method_specs{i}{2}, 'FD_vs_SM', '', 'comparison', cross_method_specs{i}{2}, i, ...
                    ExternalCollectorDispatcher.pick_struct_number(summary_metrics, cross_method_specs{i}{1}, NaN), ...
                    'Field comparison metric', 'Mean relative L2 mismatch', '', '', ...
                    ExternalCollectorDispatcher.rgb_to_hex(colors.primary), 'none', '-', ...
                    sprintf('Results.metrics.summary.%s', cross_method_specs{i}{1}), 'selected_mesh_cross_method');
            end

            error_payload = ExternalCollectorDispatcher.pick_struct_field(results_struct, 'error_vs_time');
            if isempty(fieldnames(error_payload))
                error_payload = ExternalCollectorDispatcher.pick_struct_field(results_struct, 'rmse_vs_time');
            end
            error_times = reshape(double(ExternalCollectorDispatcher.pick_struct_value(error_payload, 'time_s', [])), 1, []);
            error_specs = { ...
                {'vorticity_vector_relative_l2_mismatch', 'Vorticity L2 mismatch', colors.primary, '-'}, ...
                {'streamfunction_relative_l2_mismatch', 'Streamfunction L2 mismatch', colors.tertiary, '--'}, ...
                {'speed_relative_l2_mismatch', 'Speed L2 mismatch', colors.secondary, '-.'}, ...
                {'velocity_vector_relative_l2_mismatch', 'Velocity-vector L2 mismatch', colors.primary, ':'}, ...
                {'streamline_direction_relative_l2_mismatch', 'Streamline-direction L2 mismatch', colors.tertiary, '-'}};
            for si = 1:numel(error_specs)
                values = reshape(double(ExternalCollectorDispatcher.pick_struct_value(error_payload, error_specs{si}{1}, [])), 1, []);
                for i = 1:max([numel(error_times), numel(values), 0])
                    rows = ExternalCollectorDispatcher.append_plotting_line_row(rows, ...
                        'mesh_convergence_selected_mesh_error_vs_time', 'Mesh Convergence Selected-Mesh Error vs Time', 'line', ...
                        error_specs{si}{1}, error_specs{si}{2}, 'FD_vs_SM', '', 'error_vs_time', ...
                        ExternalCollectorDispatcher.numeric_cell(error_times, i), ExternalCollectorDispatcher.numeric_cell(values, i), ...
                        'Evolution snapshot time (s)', 'Relative error', 's', '', ...
                        ExternalCollectorDispatcher.rgb_to_hex(error_specs{si}{3}), 'none', error_specs{si}{4}, ...
                        sprintf('Results.error_vs_time.%s', error_specs{si}{1}), 'selected_mesh_snapshot_alignment');
                end
            end

            rows = [rows, ExternalCollectorDispatcher.clone_figure_rows(rows, 'mesh_convergence_comparison', 'mesh_convergence_overview_triptych', 'Mesh Convergence Overview Triptych', 'overview_left')]; %#ok<AGROW>
            rows = [rows, ExternalCollectorDispatcher.clone_figure_rows(rows, 'mesh_convergence_runtime_vs_resolution', 'mesh_convergence_overview_triptych', 'Mesh Convergence Overview Triptych', 'overview_middle')]; %#ok<AGROW>

            for mi = 1:numel(method_specs)
                child = ExternalCollectorDispatcher.pick_struct_field(children, method_specs{mi}.child_key);
                selected_mesh = ExternalCollectorDispatcher.pick_struct_field(child, 'selected_mesh');
                rows = ExternalCollectorDispatcher.append_plotting_category_row(rows, ...
                    'mesh_convergence_overview_triptych', 'Mesh Convergence Overview Triptych', 'grouped_bar', ...
                    sprintf('%s_selected_dt_adv', method_specs{mi}.series_prefix), sprintf('%s selected dt_advection', method_specs{mi}.label), ...
                    method_specs{mi}.label, '', 'convergence', 'dt_advection', mi, ...
                    ExternalCollectorDispatcher.pick_struct_number(selected_mesh, 'dt_adv', NaN), ...
                    'Selected-mesh timestep metric', 'Timestep value (s)', '', 's', ...
                    ExternalCollectorDispatcher.rgb_to_hex(colors.primary), 'none', '-', ...
                    sprintf('Results.children.%s.selected_mesh.dt_adv', method_specs{mi}.child_key), ...
                    'overview_right');
                rows = ExternalCollectorDispatcher.append_plotting_category_row(rows, ...
                    'mesh_convergence_overview_triptych', 'Mesh Convergence Overview Triptych', 'grouped_bar', ...
                    sprintf('%s_selected_dt_diff', method_specs{mi}.series_prefix), sprintf('%s selected dt_diffusion', method_specs{mi}.label), ...
                    method_specs{mi}.label, '', 'convergence', 'dt_diffusion', mi + 2, ...
                    ExternalCollectorDispatcher.pick_struct_number(selected_mesh, 'dt_diff', NaN), ...
                    'Selected-mesh timestep metric', 'Timestep value (s)', '', 's', ...
                    ExternalCollectorDispatcher.rgb_to_hex(colors.secondary), 'none', '-', ...
                    sprintf('Results.children.%s.selected_mesh.dt_diff', method_specs{mi}.child_key), ...
                    'overview_right');
                rows = ExternalCollectorDispatcher.append_plotting_category_row(rows, ...
                    'mesh_convergence_overview_triptych', 'Mesh Convergence Overview Triptych', 'grouped_bar', ...
                    sprintf('%s_selected_dt_cfl', method_specs{mi}.series_prefix), sprintf('%s selected dt_CFL', method_specs{mi}.label), ...
                    method_specs{mi}.label, '', 'convergence', 'dt_CFL', mi + 4, ...
                    ExternalCollectorDispatcher.pick_struct_number(selected_mesh, 'dt_final', NaN), ...
                    'Selected-mesh timestep metric', 'Timestep value (s)', '', 's', ...
                    ExternalCollectorDispatcher.rgb_to_hex(colors.tertiary), 'none', '-', ...
                    sprintf('Results.children.%s.selected_mesh.dt_final', method_specs{mi}.child_key), ...
                    'overview_right');
            end
        end

        function rows = build_phase1_plotting_data_rows(summary_context, stage_summary)
            rows = repmat(ExternalCollectorDispatcher.empty_plotting_row(), 1, 0);
            results_struct = ExternalCollectorDispatcher.pick_struct_field(summary_context, 'results');
            if isempty(fieldnames(results_struct))
                return;
            end

            colors = ResultsPlotDispatcher.default_light_colors();
            sm_abs_color = [0.08, 0.48, 0.44];
            if isfield(colors, 'quaternary') && isnumeric(colors.quaternary) && numel(colors.quaternary) == 3
                sm_abs_color = double(reshape(colors.quaternary, 1, 3));
            elseif isfield(colors, 'tertiary') && isnumeric(colors.tertiary) && numel(colors.tertiary) == 3
                sm_abs_color = max(0, double(reshape(colors.tertiary, 1, 3)) - 0.12);
            end

            metrics_struct = ExternalCollectorDispatcher.pick_struct_field(results_struct, 'metrics');
            fd_metrics = ExternalCollectorDispatcher.pick_struct_field(metrics_struct, 'FD');
            sm_metrics = ExternalCollectorDispatcher.pick_struct_field(metrics_struct, 'Spectral');
            summary_metrics = ExternalCollectorDispatcher.pick_struct_field(metrics_struct, 'summary');
            ic_study = ExternalCollectorDispatcher.pick_struct_field(results_struct, 'ic_study');
            baseline_label = ExternalCollectorDispatcher.phase1_baseline_display_label(ic_study);
            baseline_case_id = ExternalCollectorDispatcher.pick_struct_text(ic_study, 'baseline_case_id', 'baseline_stretched_single');

            rows = ExternalCollectorDispatcher.append_plotting_category_row(rows, ...
                'phase1_cross_method_mismatch', 'Phase 1 Cross-Method Vorticity Mismatch', 'grouped_bar', ...
                'fd_l2', 'FD Relative L2 mismatch', 'FD', baseline_case_id, 'comparison', baseline_label, 1, ...
                ExternalCollectorDispatcher.pick_struct_number(fd_metrics, 'cross_method_mismatch_l2', ...
                    ExternalCollectorDispatcher.pick_struct_number(summary_metrics, 'fd_vs_spectral_mismatch_l2', NaN)), ...
                'Initial Condition Case', 'Relative L2 mismatch', '', '', ...
                ExternalCollectorDispatcher.rgb_to_hex(colors.primary), 'none', '-', ...
                'Results.metrics.FD.cross_method_mismatch_l2', 'cross_method_case_mismatch');
            rows = ExternalCollectorDispatcher.append_plotting_category_row(rows, ...
                'phase1_cross_method_mismatch', 'Phase 1 Cross-Method Vorticity Mismatch', 'grouped_bar', ...
                'sm_l2', 'SM Relative L2 mismatch', 'SM', baseline_case_id, 'comparison', baseline_label, 1, ...
                ExternalCollectorDispatcher.pick_struct_number(sm_metrics, 'cross_method_mismatch_l2', ...
                    ExternalCollectorDispatcher.pick_struct_number(summary_metrics, 'spectral_vs_fd_mismatch_l2', NaN)), ...
                'Initial Condition Case', 'Relative L2 mismatch', '', '', ...
                ExternalCollectorDispatcher.rgb_to_hex(colors.tertiary), 'none', '-', ...
                'Results.metrics.Spectral.cross_method_mismatch_l2', 'cross_method_case_mismatch');
            baseline_field_specs = { ...
                {'cross_method_streamfunction_relative_l2_mismatch', 'Streamfunction relative L2 mismatch'}; ...
                {'cross_method_speed_relative_l2_mismatch', 'Speed relative L2 mismatch'}; ...
                {'cross_method_velocity_vector_relative_l2_mismatch', 'Velocity-vector relative L2 mismatch'}; ...
                {'cross_method_streamline_direction_relative_l2_mismatch', 'Streamline-direction relative L2 mismatch'}};
            for i = 1:numel(baseline_field_specs)
                rows = ExternalCollectorDispatcher.append_plotting_category_row(rows, ...
                    'phase1_cross_method_mismatch', 'Phase 1 Cross-Method Field Mismatch', 'grouped_bar', ...
                    sprintf('fd_%s', baseline_field_specs{i}{1}), sprintf('FD %s', baseline_field_specs{i}{2}), ...
                    'FD', baseline_case_id, 'comparison', baseline_label, i + 1, ...
                    ExternalCollectorDispatcher.pick_struct_number(fd_metrics, baseline_field_specs{i}{1}, NaN), ...
                    'Initial Condition Case', 'Relative L2 mismatch', '', '', ...
                    ExternalCollectorDispatcher.rgb_to_hex(colors.primary), 'none', '-', ...
                    sprintf('Results.metrics.FD.%s', baseline_field_specs{i}{1}), 'cross_method_case_mismatch');
                rows = ExternalCollectorDispatcher.append_plotting_category_row(rows, ...
                    'phase1_cross_method_mismatch', 'Phase 1 Cross-Method Field Mismatch', 'grouped_bar', ...
                    sprintf('sm_%s', baseline_field_specs{i}{1}), sprintf('SM %s', baseline_field_specs{i}{2}), ...
                    'SM', baseline_case_id, 'comparison', baseline_label, i + 1, ...
                    ExternalCollectorDispatcher.pick_struct_number(sm_metrics, baseline_field_specs{i}{1}, NaN), ...
                    'Initial Condition Case', 'Relative L2 mismatch', '', '', ...
                    ExternalCollectorDispatcher.rgb_to_hex(colors.tertiary), 'none', '-', ...
                    sprintf('Results.metrics.Spectral.%s', baseline_field_specs{i}{1}), 'cross_method_case_mismatch');
            end

            children = ExternalCollectorDispatcher.pick_struct_field(results_struct, 'children');
            method_specs = { ...
                struct('child_key', 'fd', 'label', 'FD', 'series_prefix', 'fd', 'l2_color', colors.primary, 'peak_color', colors.secondary), ...
                struct('child_key', 'spectral', 'label', 'SM', 'series_prefix', 'sm', 'l2_color', colors.tertiary, 'peak_color', sm_abs_color)};
            for mi = 1:numel(method_specs)
                child = ExternalCollectorDispatcher.pick_struct_field(children, method_specs{mi}.child_key);
                convergence_output = ExternalCollectorDispatcher.pick_struct_field(child, 'convergence_output');
                convergence_results = ExternalCollectorDispatcher.pick_struct_field(convergence_output, 'results');
                records = ExternalCollectorDispatcher.pick_struct_value(convergence_results, 'run_records', struct([]));
                if ~isstruct(records)
                    continue;
                end
                for ri = 1:numel(records)
                    record = records(ri);
                    mesh_n = ExternalCollectorDispatcher.pick_struct_number(record, 'Nx', NaN);
                    mesh_label = ExternalCollectorDispatcher.pick_struct_text(record, 'mesh_level_label', sprintf('L%02d', ri));
                    mesh_category = sprintf('%s / %d', mesh_label, round(mesh_n));
                    rows = ExternalCollectorDispatcher.append_plotting_line_row(rows, ...
                        'phase1_convergence_comparison', 'Phase 1 Convergence Comparison', 'line', ...
                        sprintf('%s_l2', method_specs{mi}.series_prefix), sprintf('%s L2', method_specs{mi}.label), ...
                        method_specs{mi}.label, '', 'convergence', mesh_n, ...
                        ExternalCollectorDispatcher.pick_struct_number(record, 'xi', NaN), ...
                        'Mesh N', 'Convergence error (%)', '', '%', ...
                        ExternalCollectorDispatcher.rgb_to_hex(method_specs{mi}.l2_color), 'o', '-', ...
                        sprintf('Results.children.%s.convergence_output.results.run_records(%d).xi', method_specs{mi}.child_key, ri), ...
                        'joint_tolerance_metric');
                    rows = ExternalCollectorDispatcher.append_plotting_line_row(rows, ...
                        'phase1_convergence_comparison', 'Phase 1 Convergence Comparison', 'line', ...
                        sprintf('%s_peak', method_specs{mi}.series_prefix), sprintf('%s absolute', method_specs{mi}.label), ...
                        method_specs{mi}.label, '', 'convergence', mesh_n, ...
                        ExternalCollectorDispatcher.pick_struct_number(record, 'max_vorticity_rel_error_pct', NaN), ...
                        'Mesh N', 'Convergence error (%)', '', '%', ...
                        ExternalCollectorDispatcher.rgb_to_hex(method_specs{mi}.peak_color), 's', '--', ...
                        sprintf('Results.children.%s.convergence_output.results.run_records(%d).max_vorticity_rel_error_pct', method_specs{mi}.child_key, ri), ...
                        'joint_tolerance_metric');
                    rows = ExternalCollectorDispatcher.append_plotting_category_row(rows, ...
                        'phase1_adaptive_timestep_convergence', 'Phase 1 Stability Timestep Recommendations During Mesh Convergence', 'grouped_bar', ...
                        sprintf('%s_dt_adv', method_specs{mi}.series_prefix), sprintf('%s dt_advection', method_specs{mi}.label), ...
                        method_specs{mi}.label, '', 'convergence', mesh_category, ri, ...
                        ExternalCollectorDispatcher.pick_struct_number(record, 'dt_adv', NaN), ...
                        'Mesh level / N', 'Timestep value (s)', '', 's', ...
                        ExternalCollectorDispatcher.rgb_to_hex(colors.primary), 'none', '-', ...
                        sprintf('Results.children.%s.convergence_output.results.run_records(%d).dt_adv', method_specs{mi}.child_key, ri), ...
                        'stability_snapshot');
                    rows = ExternalCollectorDispatcher.append_plotting_category_row(rows, ...
                        'phase1_adaptive_timestep_convergence', 'Phase 1 Stability Timestep Recommendations During Mesh Convergence', 'grouped_bar', ...
                        sprintf('%s_dt_diff', method_specs{mi}.series_prefix), sprintf('%s dt_diffusion', method_specs{mi}.label), ...
                        method_specs{mi}.label, '', 'convergence', mesh_category, ri, ...
                        ExternalCollectorDispatcher.pick_struct_number(record, 'dt_diff', NaN), ...
                        'Mesh level / N', 'Timestep value (s)', '', 's', ...
                        ExternalCollectorDispatcher.rgb_to_hex(colors.secondary), 'none', '-', ...
                        sprintf('Results.children.%s.convergence_output.results.run_records(%d).dt_diff', method_specs{mi}.child_key, ri), ...
                        'stability_snapshot');
                    rows = ExternalCollectorDispatcher.append_plotting_category_row(rows, ...
                        'phase1_adaptive_timestep_convergence', 'Phase 1 Stability Timestep Recommendations During Mesh Convergence', 'grouped_bar', ...
                        sprintf('%s_dt_cfl', method_specs{mi}.series_prefix), sprintf('%s dt_CFL', method_specs{mi}.label), ...
                        method_specs{mi}.label, '', 'convergence', mesh_category, ri, ...
                        ExternalCollectorDispatcher.pick_struct_number(record, 'dt_final', NaN), ...
                        'Mesh level / N', 'Timestep value (s)', '', 's', ...
                        ExternalCollectorDispatcher.rgb_to_hex(colors.tertiary), 'none', '-', ...
                        sprintf('Results.children.%s.convergence_output.results.run_records(%d).dt_final', method_specs{mi}.child_key, ri), ...
                        'stability_snapshot');
                    rows = ExternalCollectorDispatcher.append_plotting_category_row(rows, ...
                        'phase1_runtime_vs_resolution', 'Computational Time vs Resolution', 'grouped_bar', ...
                        sprintf('%s_runtime', method_specs{mi}.series_prefix), sprintf('%s runtime', method_specs{mi}.label), ...
                        method_specs{mi}.label, '', 'convergence', sprintf('%d^2', round(mesh_n)), ri, ...
                        ExternalCollectorDispatcher.pick_struct_number(record, 'runtime_wall_s', NaN), ...
                        'Grid Resolution', 'Computational time (s)', '', 's', ...
                        ExternalCollectorDispatcher.rgb_to_hex(method_specs{mi}.l2_color), 'none', '-', ...
                        sprintf('Results.children.%s.convergence_output.results.run_records(%d).runtime_wall_s', method_specs{mi}.child_key, ri), ...
                        'convergence_runtime');
                end
            end

            case_entries = ExternalCollectorDispatcher.order_phase1_ic_cases( ...
                ExternalCollectorDispatcher.pick_struct_value(ic_study, 'cases', struct([])));
            if isstruct(case_entries)
                for i = 1:numel(case_entries)
                    case_id = ExternalCollectorDispatcher.pick_struct_text(case_entries(i), 'case_id', sprintf('case_%02d', i));
                    case_label = ExternalCollectorDispatcher.phase1_case_display_label(case_entries(i), sprintf('Case %d', i));
                    fd_case_metrics = ExternalCollectorDispatcher.pick_struct_field( ...
                        ExternalCollectorDispatcher.pick_struct_field(case_entries(i), 'fd'), 'metrics');
                    sm_case_metrics = ExternalCollectorDispatcher.pick_struct_field( ...
                        ExternalCollectorDispatcher.pick_struct_field(case_entries(i), 'spectral'), 'metrics');
                    rows = ExternalCollectorDispatcher.append_plotting_category_row(rows, ...
                        'phase1_cross_method_mismatch', 'Phase 1 Cross-Method Vorticity Mismatch', 'grouped_bar', ...
                        'fd_l2', 'FD Relative L2 mismatch', 'FD', case_id, 'comparison', case_label, i + 1, ...
                        ExternalCollectorDispatcher.pick_struct_number(fd_case_metrics, 'cross_method_mismatch_l2', NaN), ...
                        'Initial Condition Case', 'Relative L2 mismatch', '', '', ...
                        ExternalCollectorDispatcher.rgb_to_hex(colors.primary), 'none', '-', ...
                        sprintf('Results.ic_study.cases(%d).fd.metrics.cross_method_mismatch_l2', i), 'cross_method_case_mismatch');
                    rows = ExternalCollectorDispatcher.append_plotting_category_row(rows, ...
                        'phase1_cross_method_mismatch', 'Phase 1 Cross-Method Vorticity Mismatch', 'grouped_bar', ...
                        'sm_l2', 'SM Relative L2 mismatch', 'SM', case_id, 'comparison', case_label, i + 1, ...
                        ExternalCollectorDispatcher.pick_struct_number(sm_case_metrics, 'cross_method_mismatch_l2', NaN), ...
                        'Initial Condition Case', 'Relative L2 mismatch', '', '', ...
                        ExternalCollectorDispatcher.rgb_to_hex(colors.tertiary), 'none', '-', ...
                        sprintf('Results.ic_study.cases(%d).spectral.metrics.cross_method_mismatch_l2', i), 'cross_method_case_mismatch');
                    for fi = 1:numel(baseline_field_specs)
                        rows = ExternalCollectorDispatcher.append_plotting_category_row(rows, ...
                            'phase1_cross_method_mismatch', 'Phase 1 Cross-Method Field Mismatch', 'grouped_bar', ...
                            sprintf('fd_%s', baseline_field_specs{fi}{1}), sprintf('FD %s', baseline_field_specs{fi}{2}), ...
                            'FD', case_id, 'comparison', case_label, fi + 1, ...
                            ExternalCollectorDispatcher.pick_struct_number(fd_case_metrics, baseline_field_specs{fi}{1}, NaN), ...
                            'Initial Condition Case', 'Relative L2 mismatch', '', '', ...
                            ExternalCollectorDispatcher.rgb_to_hex(colors.primary), 'none', '-', ...
                            sprintf('Results.ic_study.cases(%d).fd.metrics.%s', i, baseline_field_specs{fi}{1}), 'cross_method_case_mismatch');
                        rows = ExternalCollectorDispatcher.append_plotting_category_row(rows, ...
                            'phase1_cross_method_mismatch', 'Phase 1 Cross-Method Field Mismatch', 'grouped_bar', ...
                            sprintf('sm_%s', baseline_field_specs{fi}{1}), sprintf('SM %s', baseline_field_specs{fi}{2}), ...
                            'SM', case_id, 'comparison', case_label, fi + 1, ...
                            ExternalCollectorDispatcher.pick_struct_number(sm_case_metrics, baseline_field_specs{fi}{1}, NaN), ...
                            'Initial Condition Case', 'Relative L2 mismatch', '', '', ...
                            ExternalCollectorDispatcher.rgb_to_hex(colors.tertiary), 'none', '-', ...
                            sprintf('Results.ic_study.cases(%d).spectral.metrics.%s', i, baseline_field_specs{fi}{1}), 'cross_method_case_mismatch');
                    end
                end
            end

            conservation_specs = { ...
                {'Kinetic energy', 'kinetic_energy_drift'}, ...
                {'Enstrophy', 'enstrophy_drift'}, ...
                {'Circulation', 'circulation_drift'}};
            for i = 1:numel(conservation_specs)
                rows = ExternalCollectorDispatcher.append_plotting_category_row(rows, ...
                    'phase1_conservation_drift', 'Phase 1 Conservation Drift', 'grouped_bar', ...
                    sprintf('fd_%s', conservation_specs{i}{2}), sprintf('FD %s', conservation_specs{i}{1}), ...
                    'FD', baseline_case_id, 'comparison', baseline_label, i, ...
                    ExternalCollectorDispatcher.pick_struct_number(fd_metrics, conservation_specs{i}{2}, NaN), ...
                    'Initial Condition Case', 'Relative drift', '', '', ...
                    ExternalCollectorDispatcher.rgb_to_hex(colors.primary), 'none', '-', ...
                    sprintf('Results.metrics.FD.%s', conservation_specs{i}{2}), 'conservation_metric');
                rows = ExternalCollectorDispatcher.append_plotting_category_row(rows, ...
                    'phase1_conservation_drift', 'Phase 1 Conservation Drift', 'grouped_bar', ...
                    sprintf('sm_%s', conservation_specs{i}{2}), sprintf('SM %s', conservation_specs{i}{1}), ...
                    'SM', baseline_case_id, 'comparison', baseline_label, i, ...
                    ExternalCollectorDispatcher.pick_struct_number(sm_metrics, conservation_specs{i}{2}, NaN), ...
                    'Initial Condition Case', 'Relative drift', '', '', ...
                    ExternalCollectorDispatcher.rgb_to_hex(colors.tertiary), 'none', '-', ...
                    sprintf('Results.metrics.Spectral.%s', conservation_specs{i}{2}), 'conservation_metric');
            end
            if isstruct(case_entries)
                for ci = 1:numel(case_entries)
                    case_id = ExternalCollectorDispatcher.pick_struct_text(case_entries(ci), 'case_id', sprintf('case_%02d', ci));
                    case_label = ExternalCollectorDispatcher.phase1_case_display_label(case_entries(ci), sprintf('Case %d', ci));
                    fd_case_metrics = ExternalCollectorDispatcher.pick_struct_field( ...
                        ExternalCollectorDispatcher.pick_struct_field(case_entries(ci), 'fd'), 'metrics');
                    sm_case_metrics = ExternalCollectorDispatcher.pick_struct_field( ...
                        ExternalCollectorDispatcher.pick_struct_field(case_entries(ci), 'spectral'), 'metrics');
                    for i = 1:numel(conservation_specs)
                        rows = ExternalCollectorDispatcher.append_plotting_category_row(rows, ...
                            'phase1_conservation_drift', 'Phase 1 Conservation Drift', 'grouped_bar', ...
                            sprintf('fd_%s', conservation_specs{i}{2}), sprintf('FD %s', conservation_specs{i}{1}), ...
                            'FD', case_id, 'comparison', case_label, ci + 1, ...
                            ExternalCollectorDispatcher.pick_struct_number(fd_case_metrics, conservation_specs{i}{2}, NaN), ...
                            'Initial Condition Case', 'Relative drift', '', '', ...
                            ExternalCollectorDispatcher.rgb_to_hex(colors.primary), 'none', '-', ...
                            sprintf('Results.ic_study.cases(%d).fd.metrics.%s', ci, conservation_specs{i}{2}), 'conservation_metric');
                        rows = ExternalCollectorDispatcher.append_plotting_category_row(rows, ...
                            'phase1_conservation_drift', 'Phase 1 Conservation Drift', 'grouped_bar', ...
                            sprintf('sm_%s', conservation_specs{i}{2}), sprintf('SM %s', conservation_specs{i}{1}), ...
                            'SM', case_id, 'comparison', case_label, ci + 1, ...
                            ExternalCollectorDispatcher.pick_struct_number(sm_case_metrics, conservation_specs{i}{2}, NaN), ...
                            'Initial Condition Case', 'Relative drift', '', '', ...
                            ExternalCollectorDispatcher.rgb_to_hex(colors.tertiary), 'none', '-', ...
                            sprintf('Results.ic_study.cases(%d).spectral.metrics.%s', ci, conservation_specs{i}{2}), 'conservation_metric');
                    end
                end
            end

            vortex_specs = { ...
                {'Peak ratio', 'peak_vorticity_ratio'}, ...
                {'Centroid drift', 'centroid_drift'}, ...
                {'Core anisotropy', 'core_anisotropy_final'}};
            for i = 1:numel(vortex_specs)
                rows = ExternalCollectorDispatcher.append_plotting_category_row(rows, ...
                    'phase1_vortex_preservation', 'Phase 1 vortex preservation', 'grouped_bar', ...
                    sprintf('fd_%s', vortex_specs{i}{2}), sprintf('FD %s', vortex_specs{i}{1}), ...
                    'FD', '', 'comparison', vortex_specs{i}{1}, i, ...
                    ExternalCollectorDispatcher.pick_struct_number(fd_metrics, vortex_specs{i}{2}, NaN), ...
                    'Method', 'Metric value', '', '', ...
                    ExternalCollectorDispatcher.rgb_to_hex(colors.primary), 'none', '-', ...
                    sprintf('Results.metrics.FD.%s', vortex_specs{i}{2}), 'vortex_preservation');
                rows = ExternalCollectorDispatcher.append_plotting_category_row(rows, ...
                    'phase1_vortex_preservation', 'Phase 1 vortex preservation', 'grouped_bar', ...
                    sprintf('sm_%s', vortex_specs{i}{2}), sprintf('SM %s', vortex_specs{i}{1}), ...
                    'SM', '', 'comparison', vortex_specs{i}{1}, i, ...
                    ExternalCollectorDispatcher.pick_struct_number(sm_metrics, vortex_specs{i}{2}, NaN), ...
                    'Method', 'Metric value', '', '', ...
                    ExternalCollectorDispatcher.rgb_to_hex(colors.tertiary), 'none', '-', ...
                    sprintf('Results.metrics.Spectral.%s', vortex_specs{i}{2}), 'vortex_preservation');
            end

            [case_labels, runtime_matrix] = ExternalCollectorDispatcher.phase1_ic_runtime_matrix(results_struct);
            for i = 1:numel(case_labels)
                case_id = ExternalCollectorDispatcher.phase1_case_id_from_label(results_struct, case_labels{i});
                rows = ExternalCollectorDispatcher.append_plotting_category_row(rows, ...
                    'phase1_runtime_per_ic_case', 'Method Comparison: Time per Initial Condition', 'grouped_bar', ...
                    'fd_runtime_ic', 'FD runtime', 'FD', case_id, 'ic_case', case_labels{i}, i, runtime_matrix(i, 1), ...
                    'Initial Condition Case', 'Computational time (s)', '', 's', ...
                    ExternalCollectorDispatcher.rgb_to_hex(colors.primary), 'none', '-', 'Results.ic_study', 'ic_runtime_case');
                rows = ExternalCollectorDispatcher.append_plotting_category_row(rows, ...
                    'phase1_runtime_per_ic_case', 'Method Comparison: Time per Initial Condition', 'grouped_bar', ...
                    'sm_runtime_ic', 'SM runtime', 'SM', case_id, 'ic_case', case_labels{i}, i, runtime_matrix(i, 2), ...
                    'Initial Condition Case', 'Computational time (s)', '', 's', ...
                    ExternalCollectorDispatcher.rgb_to_hex(colors.tertiary), 'none', '-', 'Results.ic_study', 'ic_runtime_case');
            end

            error_payload = ExternalCollectorDispatcher.pick_struct_field(results_struct, 'error_vs_time');
            if isempty(fieldnames(error_payload))
                error_payload = ExternalCollectorDispatcher.pick_struct_field(results_struct, 'rmse_vs_time');
            end
            error_times = reshape(double(ExternalCollectorDispatcher.pick_struct_value(error_payload, 'time_s', [])), 1, []);
            error_specs = { ...
                {'mse', 'MSE', 'MSE', 'Error', colors.primary, '-'}, ...
                {'rmse', 'RMSE', 'RMSE', 'Error', colors.secondary, '-'}, ...
                {'vorticity_vector_relative_l2_mismatch', 'Vorticity L2 mismatch', 'Relative error', 'Relative error', colors.tertiary, '-'}, ...
                {'streamfunction_relative_l2_mismatch', 'Streamfunction L2 mismatch', 'Relative error', 'Relative error', colors.primary, '--'}, ...
                {'speed_relative_l2_mismatch', 'Speed L2 mismatch', 'Relative error', 'Relative error', colors.secondary, '-.'}, ...
                {'velocity_vector_relative_l2_mismatch', 'Velocity-vector L2 mismatch', 'Relative error', 'Relative error', colors.primary, ':'}, ...
                {'streamline_direction_relative_l2_mismatch', 'Streamline-direction L2 mismatch', 'Relative error', 'Relative error', colors.tertiary, '-'}, ...
                {'peak_vorticity_relative_error', 'Peak vorticity', 'Relative error', 'Relative error', colors.secondary, '--'}, ...
                {'circulation_relative_error', 'Circulation', 'Relative error', 'Relative error', colors.primary, ':'}, ...
                {'kinetic_energy_relative_error', 'Kinetic energy', 'Relative error', 'Relative error', colors.secondary, '-.'}, ...
                {'enstrophy_relative_error', 'Enstrophy', 'Relative error', 'Relative error', colors.tertiary, '--'}};
            for si = 1:numel(error_specs)
                values = reshape(double(ExternalCollectorDispatcher.pick_struct_value(error_payload, error_specs{si}{1}, [])), 1, []);
                for i = 1:max([numel(error_times), numel(values), 0])
                    rows = ExternalCollectorDispatcher.append_plotting_line_row(rows, ...
                        'phase1_error_vs_time', 'Error vs Time for Different Metrics', 'line', ...
                        error_specs{si}{1}, error_specs{si}{2}, '', '', 'error_vs_time', ...
                        ExternalCollectorDispatcher.numeric_cell(error_times, i), ExternalCollectorDispatcher.numeric_cell(values, i), ...
                        'Evolution snapshot time (s)', error_specs{si}{3}, 's', '', ...
                        ExternalCollectorDispatcher.rgb_to_hex(error_specs{si}{5}), 'none', error_specs{si}{6}, ...
                        sprintf('Results.error_vs_time.%s', error_specs{si}{1}), 'snapshot_time_basis');
                end
            end

            for i = 1:max([numel(error_times), 0])
                rows = ExternalCollectorDispatcher.append_plotting_line_row(rows, ...
                    'phase1_error_vs_time', 'Error vs Time for Different Metrics', 'line', ...
                    'time_basis', 'Evolution snapshot time', '', '', 'error_vs_time', ...
                    ExternalCollectorDispatcher.numeric_cell(error_times, i), ExternalCollectorDispatcher.numeric_cell(error_times, i), ...
                    'Evolution snapshot time (s)', 'Time (s)', 's', 's', ...
                    ExternalCollectorDispatcher.rgb_to_hex(colors.primary), 'none', '-', ...
                    'Results.error_vs_time.time_s', 'snapshot_time_basis');
            end

            rows = [rows, ExternalCollectorDispatcher.clone_figure_rows(rows, 'phase1_convergence_comparison', 'phase1_overview_triptych', 'Phase 1 Overview Triptych', 'overview_left')]; %#ok<AGROW>
            rows = [rows, ExternalCollectorDispatcher.clone_figure_rows(rows, 'phase1_conservation_drift', 'phase1_overview_triptych', 'Phase 1 Overview Triptych', 'overview_middle')]; %#ok<AGROW>
            rows = [rows, ExternalCollectorDispatcher.clone_figure_rows(rows, 'phase1_error_vs_time', 'phase1_overview_triptych', 'Phase 1 Overview Triptych', 'overview_right')]; %#ok<AGROW>

            safe_stage_summary = ExternalCollectorDispatcher.safe_table(stage_summary);
            if ~isempty(safe_stage_summary)
                metric_defs = { ...
                    {'energy_wh_total', 'Energy (Wh)'}, ...
                    {'mean_total_power_w', 'Mean power (W)'}, ...
                    {'co2_g_total', 'CO2e (g)'}};
                for i = 1:height(safe_stage_summary)
                    stage_label = ExternalCollectorDispatcher.table_row_text(safe_stage_summary, i, 'substage_label', ...
                        ExternalCollectorDispatcher.table_row_text(safe_stage_summary, i, 'stage_label', 'Stage'));
                    stage_method = ExternalCollectorDispatcher.table_row_text(safe_stage_summary, i, 'stage_method', '');
                    for md = 1:numel(metric_defs)
                        rows = ExternalCollectorDispatcher.append_plotting_line_row(rows, ...
                            'phase1_sustainability_vs_runtime', 'Phase 1 Sustainability vs Runtime', 'scatter_line', ...
                            sprintf('%s_%s_runtime', lower(stage_method), metric_defs{md}{1}), ...
                            sprintf('%s %s', stage_method, metric_defs{md}{2}), stage_method, ...
                            ExternalCollectorDispatcher.table_row_text(safe_stage_summary, i, 'substage_id', ''), 'sustainability', ...
                            ExternalCollectorDispatcher.table_row_number(safe_stage_summary, i, 'wall_time_s', NaN), ...
                            ExternalCollectorDispatcher.table_row_number(safe_stage_summary, i, metric_defs{md}{1}, NaN), ...
                            'Total Phase 1 runtime', metric_defs{md}{2}, 's', '', ...
                            ExternalCollectorDispatcher.rgb_to_hex(ExternalCollectorDispatcher.phase1_method_color(stage_method, colors, sm_abs_color)), ...
                            'o', '-', ...
                            sprintf('Collector.stage_summary.%s', metric_defs{md}{1}), 'stage_segment');
                        rows = ExternalCollectorDispatcher.append_plotting_category_row(rows, ...
                            'phase1_sustainability_stage_breakdown', 'Phase 1 Sustainability Stage Breakdown', 'grouped_bar', ...
                            sprintf('%s_%s_stage', lower(stage_method), metric_defs{md}{1}), ...
                            sprintf('%s %s', stage_method, metric_defs{md}{2}), stage_method, ...
                            ExternalCollectorDispatcher.table_row_text(safe_stage_summary, i, 'substage_id', ''), 'sustainability', ...
                            stage_label, i, ExternalCollectorDispatcher.table_row_number(safe_stage_summary, i, metric_defs{md}{1}, NaN), ...
                            'Stage / substage', metric_defs{md}{2}, '', '', ...
                            ExternalCollectorDispatcher.rgb_to_hex(ExternalCollectorDispatcher.phase1_method_color(stage_method, colors, sm_abs_color)), ...
                            'none', '-', ...
                            sprintf('Collector.stage_summary.%s', metric_defs{md}{1}), 'stage_segment');
                    end
                end
            end
        end

        function row = empty_plotting_row()
            row = struct( ...
                'figure_id', "", ...
                'figure_title', "", ...
                'plot_kind', "", ...
                'series_id', "", ...
                'series_label', "", ...
                'method', "", ...
                'case_id', "", ...
                'stage_type', "", ...
                'x_value', NaN, ...
                'x_category', "", ...
                'x_order', NaN, ...
                'y_value', NaN, ...
                'x_label', "", ...
                'y_label', "", ...
                'x_unit', "", ...
                'y_unit', "", ...
                'series_color_hex', "", ...
                'series_marker', "", ...
                'series_line_style', "", ...
                'source_struct_path', "", ...
                'source_notes', "");
        end

        function rows = append_plotting_line_row(rows, figure_id, figure_title, plot_kind, series_id, series_label, method, case_id, stage_type, x_value, y_value, x_label, y_label, x_unit, y_unit, color_hex, marker, line_style, source_path, source_notes)
            row = ExternalCollectorDispatcher.empty_plotting_row();
            row.figure_id = string(figure_id);
            row.figure_title = string(figure_title);
            row.plot_kind = string(plot_kind);
            row.series_id = string(series_id);
            row.series_label = string(series_label);
            row.method = string(method);
            row.case_id = string(case_id);
            row.stage_type = string(stage_type);
            row.x_value = double(x_value);
            row.y_value = double(y_value);
            row.x_label = string(x_label);
            row.y_label = string(y_label);
            row.x_unit = string(x_unit);
            row.y_unit = string(y_unit);
            row.series_color_hex = string(color_hex);
            row.series_marker = string(marker);
            row.series_line_style = string(line_style);
            row.source_struct_path = string(source_path);
            row.source_notes = string(source_notes);
            rows(end + 1) = row; %#ok<AGROW>
        end

        function rows = append_plotting_category_row(rows, figure_id, figure_title, plot_kind, series_id, series_label, method, case_id, stage_type, x_category, x_order, y_value, x_label, y_label, x_unit, y_unit, color_hex, marker, line_style, source_path, source_notes)
            row = ExternalCollectorDispatcher.empty_plotting_row();
            row.figure_id = string(figure_id);
            row.figure_title = string(figure_title);
            row.plot_kind = string(plot_kind);
            row.series_id = string(series_id);
            row.series_label = string(series_label);
            row.method = string(method);
            row.case_id = string(case_id);
            row.stage_type = string(stage_type);
            row.x_category = string(x_category);
            row.x_order = double(x_order);
            row.y_value = double(y_value);
            row.x_label = string(x_label);
            row.y_label = string(y_label);
            row.x_unit = string(x_unit);
            row.y_unit = string(y_unit);
            row.series_color_hex = string(color_hex);
            row.series_marker = string(marker);
            row.series_line_style = string(line_style);
            row.source_struct_path = string(source_path);
            row.source_notes = string(source_notes);
            rows(end + 1) = row; %#ok<AGROW>
        end

        function cloned_rows = clone_figure_rows(rows, source_figure_id, target_figure_id, target_title, source_notes)
            cloned_rows = repmat(ExternalCollectorDispatcher.empty_plotting_row(), 1, 0);
            if isempty(rows)
                return;
            end
            for i = 1:numel(rows)
                if ~strcmp(string(rows(i).figure_id), string(source_figure_id))
                    continue;
                end
                row = rows(i);
                row.figure_id = string(target_figure_id);
                row.figure_title = string(target_title);
                row.source_notes = string(source_notes);
                cloned_rows(end + 1) = row; %#ok<AGROW>
            end
        end

        function color = phase1_method_color(method_label, colors, sm_abs_color)
            method_text = lower(strtrim(char(string(method_label))));
            if contains(method_text, 'spectral') || strcmp(method_text, 'sm')
                color = sm_abs_color;
            else
                color = colors.primary;
            end
        end

        function text = rgb_to_hex(rgb)
            text = "";
            if ~(isnumeric(rgb) && numel(rgb) == 3)
                return;
            end
            rgb = max(0, min(1, double(reshape(rgb, 1, 3))));
            text = string(sprintf('#%02X%02X%02X', round(255 * rgb(1)), round(255 * rgb(2)), round(255 * rgb(3))));
        end

        function [labels, runtime_matrix] = phase1_ic_runtime_matrix(results_struct)
            labels = {};
            runtime_matrix = [];
            if ~isstruct(results_struct)
                return;
            end
            metrics_struct = ExternalCollectorDispatcher.pick_struct_field(results_struct, 'metrics');
            ic_study = ExternalCollectorDispatcher.pick_struct_field(results_struct, 'ic_study');
            baseline_label = ExternalCollectorDispatcher.phase1_baseline_display_label(ic_study);
            labels = {baseline_label};
            runtime_matrix = [ ...
                ExternalCollectorDispatcher.pick_struct_number(ExternalCollectorDispatcher.pick_struct_field(metrics_struct, 'FD'), 'runtime_wall_s', NaN), ...
                ExternalCollectorDispatcher.pick_struct_number(ExternalCollectorDispatcher.pick_struct_field(metrics_struct, 'Spectral'), 'runtime_wall_s', NaN)];
            case_entries = ExternalCollectorDispatcher.order_phase1_ic_cases( ...
                ExternalCollectorDispatcher.pick_struct_value(ic_study, 'cases', struct([])));
            if isstruct(case_entries)
                for i = 1:numel(case_entries)
                    labels{end + 1} = ExternalCollectorDispatcher.phase1_case_display_label(case_entries(i), sprintf('Case %d', i)); %#ok<AGROW>
                    runtime_matrix(end + 1, :) = [ ... %#ok<AGROW>
                        ExternalCollectorDispatcher.pick_struct_number(ExternalCollectorDispatcher.pick_struct_field(case_entries(i), 'fd'), 'runtime_wall_s', NaN), ...
                        ExternalCollectorDispatcher.pick_struct_number(ExternalCollectorDispatcher.pick_struct_field(case_entries(i), 'spectral'), 'runtime_wall_s', NaN)];
                end
            end
        end

        function case_id = phase1_case_id_from_label(results_struct, case_label)
            case_id = '';
            ic_study = ExternalCollectorDispatcher.pick_struct_field(results_struct, 'ic_study');
            baseline_label = ExternalCollectorDispatcher.phase1_baseline_display_label(ic_study);
            if strcmpi(char(string(case_label)), baseline_label) || ...
                    strcmpi(char(string(case_label)), ExternalCollectorDispatcher.pick_struct_text(ic_study, 'baseline_label', ''))
                case_id = ExternalCollectorDispatcher.pick_struct_text(ic_study, 'baseline_case_id', 'baseline_stretched_single');
                return;
            end
            case_entries = ExternalCollectorDispatcher.pick_struct_value(ic_study, 'cases', struct([]));
            if ~isstruct(case_entries)
                return;
            end
            for i = 1:numel(case_entries)
                if strcmpi(ExternalCollectorDispatcher.phase1_case_display_label(case_entries(i), ''), char(string(case_label))) || ...
                        strcmpi(ExternalCollectorDispatcher.pick_struct_text(case_entries(i), 'label', ''), char(string(case_label)))
                    case_id = ExternalCollectorDispatcher.pick_struct_text(case_entries(i), 'case_id', '');
                    return;
                end
            end
        end

        function label = phase1_baseline_display_label(ic_study)
            baseline_case_id = ExternalCollectorDispatcher.pick_struct_text(ic_study, 'baseline_case_id', '');
            baseline_raw = ExternalCollectorDispatcher.pick_struct_text(ic_study, 'baseline_label', 'Stretched Gaussian');
            switch lower(strtrim(char(string(baseline_raw))))
                case {'stretched gaussian', 'stretched gaussian (default)', 'stretched single', ...
                        'stretched_gaussian', 'baseline_stretched_single'}
                    label = 'Stretched Gaussian';
                case {''}
                    if strcmpi(baseline_case_id, 'baseline_elliptic_single')
                        label = 'Elliptic';
                    else
                        label = 'Stretched Gaussian';
                    end
                    return;
                case {'elliptic gaussian', 'elliptic gaussian (default)', 'elliptic single', ...
                        'elliptical vortex', 'elliptic vortex', 'baseline_elliptic_single'}
                    label = 'Elliptic';
                otherwise
                    label = char(string(baseline_raw));
            end
        end

        function label = phase1_case_display_label(case_entry, fallback_label)
            if nargin < 2
                fallback_label = '';
            end
            case_id = '';
            display_label = '';
            raw_label = '';
            if isstruct(case_entry)
                case_id = ExternalCollectorDispatcher.pick_struct_text(case_entry, 'case_id', '');
                display_label = ExternalCollectorDispatcher.pick_struct_text(case_entry, 'display_label', '');
                raw_label = ExternalCollectorDispatcher.pick_struct_text(case_entry, 'label', '');
            else
                case_id = char(string(case_entry));
            end
            if ~isempty(strtrim(display_label))
                label = display_label;
                return;
            end
            switch lower(strtrim(case_id))
                case {'baseline_stretched_single', 'stretched', 'stretched_gaussian'}
                    label = 'Stretched Gaussian';
                    return;
                case {'', 'baseline_elliptic_single', 'elliptic', 'elliptical_vortex', 'elliptic_vortex'}
                    label = 'Elliptic';
                    return;
                case {'taylor_green', 'taylorgreen'}
                    label = 'Taylor-Green';
                    return;
            end
            if ~isempty(strtrim(raw_label))
                label = raw_label;
            elseif ~isempty(strtrim(fallback_label))
                label = char(string(fallback_label));
            else
                label = 'Case';
            end
        end

        function case_entries = order_phase1_ic_cases(case_entries)
            if ~isstruct(case_entries) || isempty(case_entries)
                return;
            end
            canonical_labels = ["Taylor-Green", "Polar Opposites", "Ring Train", "Random Pack"];
            rank = inf(1, numel(case_entries));
            for i = 1:numel(case_entries)
                label = string(ExternalCollectorDispatcher.pick_struct_text(case_entries(i), 'label', ''));
                idx = find(strcmpi(label, canonical_labels), 1, 'first');
                if ~isempty(idx)
                    rank(i) = idx;
                end
            end
            [~, order] = sort(rank, 'ascend');
            case_entries = case_entries(order);
        end

        function row = aggregate_phase1_sustainability_row(scope, stage_id, stage_label, stage_method, stage_type, block)
            wall_time = ExternalCollectorDispatcher.table_numeric_column(block, 'wall_time_s');
            mean_power = ExternalCollectorDispatcher.table_numeric_column(block, 'mean_total_power_w');
            row = struct( ...
                'scope', string(scope), ...
                'stage_id', string(stage_id), ...
                'stage_label', string(stage_label), ...
                'stage_method', string(stage_method), ...
                'stage_type', string(stage_type), ...
                'substage_id', "", ...
                'substage_label', "", ...
                'substage_type', "", ...
                'wall_time_s', nansum(wall_time), ...
                'mean_total_power_w', ExternalCollectorDispatcher.weighted_average(mean_power, wall_time), ...
                'peak_total_power_w', max(ExternalCollectorDispatcher.table_numeric_column(block, 'peak_total_power_w'), [], 'omitnan'), ...
                'energy_wh_total', nansum(ExternalCollectorDispatcher.table_numeric_column(block, 'energy_wh_total')), ...
                'co2_g_total', nansum(ExternalCollectorDispatcher.table_numeric_column(block, 'co2_g_total')), ...
                'energy_per_iteration_wh', NaN, ...
                'energy_per_sim_second_wh', NaN, ...
                'energy_per_cell_step_wh', NaN, ...
                'telemetry_enabled', "", ...
                'telemetry_disable_reason', "", ...
                'hwinfo_control_mode', "", ...
                'raw_hwinfo_csv_path', "", ...
                'note', "");
        end

        function tf = telemetry_enabled(monitor_series)
            if isstruct(monitor_series) && isfield(monitor_series, 'telemetry_enabled')
                tf = logical(monitor_series.telemetry_enabled);
                return;
            end
            status_text = lower(strtrim(char(string(ExternalCollectorDispatcher.pick_text_field( ...
                ExternalCollectorDispatcher.pick_struct_field(monitor_series, 'collector_status'), 'hwinfo', '')))));
            transport_text = lower(strtrim(char(string(ExternalCollectorDispatcher.pick_text_field(monitor_series, 'hwinfo_transport', '')))));
            tf = any(strcmp(status_text, {'pro_cli_csv', 'shared_memory_connected', 'connected', 'csv_fallback', 'csv_target_mismatch'})) || ...
                any(strcmp(transport_text, {'csv', 'shared_memory'}));
        end

        function reason = telemetry_disable_reason(monitor_series)
            reason = ExternalCollectorDispatcher.pick_text_field(monitor_series, 'telemetry_disable_reason', '');
            if ~isempty(strtrim(reason))
                return;
            end
            status_text = lower(strtrim(char(string(ExternalCollectorDispatcher.pick_text_field( ...
                ExternalCollectorDispatcher.pick_struct_field(monitor_series, 'collector_status'), 'hwinfo', '')))));
            switch status_text
                case 'not_found'
                    reason = 'hwinfo_not_found';
                case 'disabled'
                    reason = 'disabled_by_settings';
                otherwise
                    reason = '';
            end
        end

        function note_text = phase_telemetry_status_note(monitor_series)
            reason = lower(strtrim(char(string(ExternalCollectorDispatcher.telemetry_disable_reason(monitor_series)))));
            switch reason
                case 'hwinfo_not_found'
                    note_text = 'HWiNFO executable not found; telemetry was disabled for this phase run.';
                case 'disabled_by_settings'
                    note_text = 'Telemetry was disabled by settings for this phase run.';
                otherwise
                    note_text = 'No telemetry data was available for this phase run.';
            end
        end

        function table_out = phase_telemetry_status_raw_table(monitor_series, raw_csv_path)
            if nargin < 2
                raw_csv_path = '';
            end
            if ExternalCollectorDispatcher.telemetry_enabled(monitor_series)
                table_out = table();
                return;
            end
            row = struct( ...
                'telemetry_enabled', "No", ...
                'telemetry_disable_reason', string(ExternalCollectorDispatcher.telemetry_disable_reason(monitor_series)), ...
                'hwinfo_control_mode', string(ExternalCollectorDispatcher.pick_text_field(monitor_series, 'hwinfo_control_mode', 'pro_cli_csv')), ...
                'hwinfo_status', string(ExternalCollectorDispatcher.pick_text_field( ...
                    ExternalCollectorDispatcher.pick_struct_field(monitor_series, 'collector_status'), 'hwinfo', 'disabled')), ...
                'hwinfo_transport', string(ExternalCollectorDispatcher.pick_text_field(monitor_series, 'hwinfo_transport', 'none')), ...
                'raw_hwinfo_csv_path', string(raw_csv_path), ...
                'note', string(ExternalCollectorDispatcher.phase_telemetry_status_note(monitor_series)));
            table_out = struct2table(row, 'AsArray', true);
        end

        function row = phase1_sustainability_disabled_row(monitor_series, raw_csv_path)
            if nargin < 2
                raw_csv_path = '';
            end
            row = struct( ...
                'scope', "telemetry_status", ...
                'stage_id', "", ...
                'stage_label', "Telemetry unavailable", ...
                'stage_method', "", ...
                'stage_type', "", ...
                'substage_id', "", ...
                'substage_label', "", ...
                'substage_type', "", ...
                'wall_time_s', NaN, ...
                'mean_total_power_w', NaN, ...
                'peak_total_power_w', NaN, ...
                'energy_wh_total', NaN, ...
                'co2_g_total', NaN, ...
                'energy_per_iteration_wh', NaN, ...
                'energy_per_sim_second_wh', NaN, ...
                'energy_per_cell_step_wh', NaN, ...
                'telemetry_enabled', "No", ...
                'telemetry_disable_reason', string(ExternalCollectorDispatcher.telemetry_disable_reason(monitor_series)), ...
                'hwinfo_control_mode', string(ExternalCollectorDispatcher.pick_text_field(monitor_series, 'hwinfo_control_mode', 'pro_cli_csv')), ...
                'raw_hwinfo_csv_path', string(raw_csv_path), ...
                'note', string(ExternalCollectorDispatcher.phase_telemetry_status_note(monitor_series)));
        end

        function sheet_def = build_workbook_sheet_from_cells(sheet_name, cells_out, style_rows, summary_start_col)
            if nargin < 2 || isempty(cells_out)
                cells_out = {'No sheet rows available.'};
            end
            if nargin < 3 || isempty(style_rows)
                style_rows = repmat(struct('kind', 'header', 'method', ''), 1, size(cells_out, 1));
                if numel(style_rows) >= 2
                    for i = 2:numel(style_rows)
                        style_rows(i) = struct('kind', 'data', 'method', '');
                    end
                end
            end
            if nargin < 4
                summary_start_col = 0;
            end
            sheet_def = struct( ...
                'name', char(string(sheet_name)), ...
                'cells', {cells_out}, ...
                'style', style_rows, ...
                'summary_start_col', summary_start_col);
        end

        function sheet_def = build_workbook_sheet_from_table(sheet_name, table_in, empty_message)
            if nargin < 3
                empty_message = 'No table rows available.';
            end
            cells_out = ExternalCollectorDispatcher.table_to_workbook_cells(table_in, empty_message);
            sheet_def = ExternalCollectorDispatcher.build_workbook_sheet_from_cells(sheet_name, cells_out);
        end

        function sheet_def = build_workbook_sheet_from_table_with_style(sheet_name, table_in, empty_message, style_rows)
            if nargin < 3
                empty_message = 'No table rows available.';
            end
            cells_out = ExternalCollectorDispatcher.table_to_workbook_cells(table_in, empty_message);
            if nargin < 4 || isempty(style_rows)
                style_rows = [];
            end
            sheet_def = ExternalCollectorDispatcher.build_workbook_sheet_from_cells(sheet_name, cells_out, style_rows, 0);
        end

        function write_phase_workbook_sheet(workbook_path, sheet_def)
            if isempty(sheet_def.cells)
                writecell({'No sheet rows available.'}, workbook_path, 'Sheet', sheet_def.name);
            else
                writecell(sheet_def.cells, workbook_path, 'Sheet', sheet_def.name);
            end
        end

        function cells_out = table_to_workbook_cells(table_in, empty_message)
            if nargin < 2
                empty_message = 'No table rows available.';
            end
            if isempty(table_in) || ~istable(table_in) || height(table_in) < 1
                cells_out = {char(string(empty_message))};
                return;
            end
            headers = reshape(cellstr(string(table_in.Properties.VariableNames)), 1, []);
            cells_out = [headers; table2cell(table_in)];
        end

        function row = phase1_find_convergence_stage_row(stage_summary, method_label, mesh_level_index, mesh_nx, mesh_ny)
            row = table();
            if isempty(stage_summary) || height(stage_summary) < 1
                return;
            end
            method_family = ExternalCollectorDispatcher.normalize_method_family(method_label);
            stage_type = lower(string(ExternalCollectorDispatcher.table_text_column(stage_summary, 'stage_type')));
            stage_method = lower(string(ExternalCollectorDispatcher.table_text_column(stage_summary, 'stage_method')));
            mesh_level = ExternalCollectorDispatcher.table_numeric_column(stage_summary, 'mesh_level');
            stage_mesh_nx = ExternalCollectorDispatcher.table_numeric_column(stage_summary, 'mesh_nx');
            stage_mesh_ny = ExternalCollectorDispatcher.table_numeric_column(stage_summary, 'mesh_ny');
            stage_type = stage_type(:);
            stage_method = stage_method(:);
            mesh_level = mesh_level(:);
            stage_mesh_nx = stage_mesh_nx(:);
            stage_mesh_ny = stage_mesh_ny(:);
            row_count = min([height(stage_summary), numel(stage_type), numel(stage_method), ...
                numel(mesh_level), numel(stage_mesh_nx), numel(stage_mesh_ny)]);
            if row_count < 1
                return;
            end
            stage_summary = stage_summary(1:row_count, :);
            stage_type = stage_type(1:row_count);
            stage_method = stage_method(1:row_count);
            mesh_level = mesh_level(1:row_count);
            stage_mesh_nx = stage_mesh_nx(1:row_count);
            stage_mesh_ny = stage_mesh_ny(1:row_count);
            mask = contains(stage_type, 'convergence') & strcmpi(stage_method, method_family);
            if isfinite(mesh_level_index)
                mask = mask & mesh_level == mesh_level_index;
            end
            if ~any(mask) && isfinite(mesh_nx) && isfinite(mesh_ny)
                mask = contains(stage_type, 'convergence') & strcmpi(stage_method, method_family) & ...
                    stage_mesh_nx == mesh_nx & stage_mesh_ny == mesh_ny;
            end
            idx = find(mask, 1, 'first');
            if ~isempty(idx)
                row = stage_summary(idx, :);
            end
        end

        function summary = save_phase1_sustainability_plots(paths, summary_context, stage_summary)
            summary = struct('runtime_png_path', '', 'runtime_fig_path', '', 'stage_png_path', '', 'stage_fig_path', '', ...
                'status', 'not_requested', 'reason', '');
            workflow_kind = ExternalCollectorDispatcher.resolve_summary_workflow_kind(summary_context, struct());
            if ~strcmpi(workflow_kind, 'phase1_periodic_comparison')
                return;
            end
            monitor_series = ExternalCollectorDispatcher.pick_struct_field(summary_context, 'monitor_series');
            if ~ExternalCollectorDispatcher.telemetry_enabled(monitor_series)
                summary.status = 'skipped_no_telemetry';
                summary.reason = ExternalCollectorDispatcher.phase_telemetry_status_note(monitor_series);
                return;
            end
            safe_stage_summary = ExternalCollectorDispatcher.safe_table(stage_summary);
            if isempty(safe_stage_summary) || height(safe_stage_summary) < 1 || ...
                    ~isfield(paths, 'visuals_root') || isempty(paths.visuals_root)
                summary.status = 'skipped_no_stage_summary';
                summary.reason = 'No processed sustainability stage summary was available.';
                return;
            end

            plot_root = fullfile(char(string(paths.visuals_root)), 'Collectors');
            if exist(plot_root, 'dir') ~= 7
                mkdir(plot_root);
            end
            save_settings = struct('save_png', true, 'save_fig', true, 'save_pdf', false, 'figure_dpi', 180);

            fig = figure('Visible', 'off', 'Color', 'w', 'Units', 'pixels', 'Position', [120 120 1280 360]);
            layout = tiledlayout(fig, 1, 3, 'TileSpacing', 'compact', 'Padding', 'compact');
            title(layout, 'Phase 1 Sustainability vs Runtime');
            ExternalCollectorDispatcher.plot_phase1_runtime_metric_tile(nexttile(layout, 1), safe_stage_summary, 'energy_wh_total', 'Energy (Wh)');
            ExternalCollectorDispatcher.plot_phase1_runtime_metric_tile(nexttile(layout, 2), safe_stage_summary, 'mean_total_power_w', 'Mean power (W)');
            ExternalCollectorDispatcher.plot_phase1_runtime_metric_tile(nexttile(layout, 3), safe_stage_summary, 'co2_g_total', 'CO2e (g)');
            outputs = ResultsPlotDispatcher.save_figure_bundle(fig, fullfile(plot_root, 'phase1_sustainability_vs_runtime'), save_settings);
            summary.runtime_png_path = ExternalCollectorDispatcher.pick_struct_text(outputs, 'png_path', ...
                ResultsPlotDispatcher.primary_output_path(outputs, ''));
            summary.runtime_fig_path = outputs.fig_path;

            fig = figure('Visible', 'off', 'Color', 'w', 'Units', 'pixels', 'Position', [120 120 1380 420]);
            layout = tiledlayout(fig, 1, 3, 'TileSpacing', 'compact', 'Padding', 'compact');
            title(layout, 'Phase 1 Sustainability Stage Breakdown');
            ExternalCollectorDispatcher.plot_phase1_stage_breakdown_tile(nexttile(layout, 1), safe_stage_summary, 'energy_wh_total', 'Energy (Wh)');
            ExternalCollectorDispatcher.plot_phase1_stage_breakdown_tile(nexttile(layout, 2), safe_stage_summary, 'mean_total_power_w', 'Mean power (W)');
            ExternalCollectorDispatcher.plot_phase1_stage_breakdown_tile(nexttile(layout, 3), safe_stage_summary, 'co2_g_total', 'CO2e (g)');
            outputs = ResultsPlotDispatcher.save_figure_bundle(fig, fullfile(plot_root, 'phase1_sustainability_stage_breakdown'), save_settings);
            summary.stage_png_path = ExternalCollectorDispatcher.pick_struct_text(outputs, 'png_path', ...
                ResultsPlotDispatcher.primary_output_path(outputs, ''));
            summary.stage_fig_path = outputs.fig_path;
            summary.status = 'created';
            summary.reason = '';
        end

        function summary = save_workflow_sustainability_comparison_plots(paths, summary_context, stage_summary)
            summary = struct('png_path', '', 'fig_path', '', 'status', 'not_requested', 'reason', '');
            workflow_kind = ExternalCollectorDispatcher.resolve_summary_workflow_kind(summary_context, struct());
            if ~any(strcmpi(workflow_kind, {'phase1_periodic_comparison', 'phase2_boundary_condition_study'}))
                return;
            end
            monitor_series = ExternalCollectorDispatcher.pick_struct_field(summary_context, 'monitor_series');
            if ~ExternalCollectorDispatcher.telemetry_enabled(monitor_series)
                summary.status = 'skipped_no_telemetry';
                summary.reason = ExternalCollectorDispatcher.phase_telemetry_status_note(monitor_series);
                return;
            end
            safe_stage_summary = ExternalCollectorDispatcher.safe_table(stage_summary);
            if isempty(safe_stage_summary) || height(safe_stage_summary) < 1 || ...
                    ~isfield(paths, 'visuals_root') || isempty(paths.visuals_root)
                summary.status = 'skipped_no_stage_summary';
                summary.reason = 'No processed sustainability stage summary was available.';
                return;
            end

            if strcmpi(workflow_kind, 'phase1_periodic_comparison')
                plot_table = ExternalCollectorDispatcher.phase1_ic_comparison_table(safe_stage_summary);
                plot_title = 'Phase 1 IC Sustainability Comparison';
                output_stem = 'phase1_hwinfo_ic_comparison';
            else
                plot_table = ExternalCollectorDispatcher.phase2_bc_comparison_table(safe_stage_summary);
                plot_title = 'Phase 2 BC Sustainability Comparison';
                output_stem = 'phase2_hwinfo_bc_comparison';
            end
            if isempty(plot_table) || height(plot_table) < 1
                summary.status = 'skipped_no_comparison_rows';
                summary.reason = 'No comparable telemetry rows were available for grouped sustainability plots.';
                return;
            end

            plot_root = fullfile(char(string(paths.visuals_root)), 'Collectors');
            if exist(plot_root, 'dir') ~= 7
                mkdir(plot_root);
            end
            save_settings = struct('save_png', true, 'save_fig', true, 'save_pdf', false, 'figure_dpi', 180);
            fig = figure('Visible', 'off', 'Color', 'w', 'Units', 'pixels', 'Position', [120 120 1320 420]);
            layout = tiledlayout(fig, 1, 3, 'TileSpacing', 'compact', 'Padding', 'compact');
            title(layout, plot_title);
            ExternalCollectorDispatcher.plot_workflow_comparison_metric_tile(nexttile(layout, 1), plot_table, 'mean_total_power_w', 'Mean power (W)');
            ExternalCollectorDispatcher.plot_workflow_comparison_metric_tile(nexttile(layout, 2), plot_table, 'energy_wh_total', 'Cumulative energy (Wh)');
            ExternalCollectorDispatcher.plot_workflow_comparison_metric_tile(nexttile(layout, 3), plot_table, 'co2_g_total', 'Cumulative CO2e (g)');
            outputs = ResultsPlotDispatcher.save_figure_bundle(fig, fullfile(plot_root, output_stem), save_settings);
            summary.png_path = ExternalCollectorDispatcher.pick_struct_text(outputs, 'png_path', ...
                ResultsPlotDispatcher.primary_output_path(outputs, ''));
            summary.fig_path = outputs.fig_path;
            summary.status = 'created';
        end

        function table_out = phase1_ic_comparison_table(stage_summary)
            table_out = stage_summary;
            if isempty(table_out) || height(table_out) < 1
                return;
            end
            keep = false(height(table_out), 1);
            category = strings(height(table_out), 1);
            method = strings(height(table_out), 1);
            for i = 1:height(table_out)
                method_label = lower(strtrim(char(string(ExternalCollectorDispatcher.table_row_text(table_out, i, 'stage_method', '')))));
                if isempty(method_label)
                    continue;
                end
                stage_id = lower(strtrim(char(string(ExternalCollectorDispatcher.table_row_text(table_out, i, 'stage_id', '')))));
                if contains(stage_id, 'convergence')
                    continue;
                end
                raw_label = ExternalCollectorDispatcher.table_row_text(table_out, i, 'substage_label', ...
                    ExternalCollectorDispatcher.table_row_text(table_out, i, 'stage_label', 'Stage'));
                raw_label = regexprep(char(string(raw_label)), '^(FD|SM|Spectral)\s*\|\s*', '', 'ignorecase');
                raw_label = regexprep(raw_label, '[_\s]+', ' ');
                raw_label = strtrim(raw_label);
                if isempty(raw_label)
                    continue;
                end
                keep(i) = true;
                category(i) = string(raw_label);
                method(i) = string(upper(method_label));
            end
            table_out = table_out(keep, :);
            if isempty(table_out)
                return;
            end
            table_out.category_label = category(keep);
            table_out.series_label = method(keep);
        end

        function table_out = phase2_bc_comparison_table(stage_summary)
            table_out = stage_summary;
            if isempty(table_out) || height(table_out) < 1
                return;
            end
            category = strings(height(table_out), 1);
            keep = false(height(table_out), 1);
            for i = 1:height(table_out)
                raw_label = ExternalCollectorDispatcher.table_row_text(table_out, i, 'stage_label', sprintf('Scenario %d', i));
                raw_label = regexprep(char(string(raw_label)), '[_\s]+', ' ');
                raw_label = strtrim(raw_label);
                if isempty(raw_label)
                    continue;
                end
                keep(i) = true;
                category(i) = string(raw_label);
            end
            table_out = table_out(keep, :);
            if isempty(table_out)
                return;
            end
            table_out.category_label = category(keep);
            table_out.series_label = repmat("FD", height(table_out), 1);
        end

        function plot_workflow_comparison_metric_tile(ax, plot_table, metric_name, ylabel_text)
            if isempty(plot_table) || height(plot_table) < 1
                text(ax, 0.5, 0.5, 'No telemetry data available.', 'HorizontalAlignment', 'center', 'VerticalAlignment', 'middle');
                axis(ax, 'off');
                return;
            end
            categories = unique(string(plot_table.category_label), 'stable');
            series = unique(string(plot_table.series_label), 'stable');
            value_matrix = nan(numel(categories), numel(series));
            for i = 1:numel(categories)
                for j = 1:numel(series)
                    mask = strcmpi(string(plot_table.category_label), categories(i)) & ...
                        strcmpi(string(plot_table.series_label), series(j));
                    if any(mask)
                        value_matrix(i, j) = ExternalCollectorDispatcher.weighted_average( ...
                            ExternalCollectorDispatcher.table_numeric_column(plot_table(mask, :), metric_name), ...
                            max(ExternalCollectorDispatcher.table_numeric_column(plot_table(mask, :), 'wall_time_s'), 1));
                    end
                end
            end
            bars = bar(ax, value_matrix, 'grouped');
            if numel(series) == 1 && ~isempty(bars)
                bars(1).FaceColor = [0.10 0.34 0.78];
            elseif numel(bars) >= 2
                bars(1).FaceColor = [0.10 0.34 0.78];
                bars(2).FaceColor = [0.87 0.43 0.10];
            end
            ax.XTick = 1:numel(categories);
            ax.XTickLabel = cellstr(categories);
            ax.XTickLabelRotation = 20;
            ylabel(ax, ylabel_text);
            xlabel(ax, 'Comparison group');
            title(ax, erase(ylabel_text, 'Cumulative '));
            grid(ax, 'on');
            box(ax, 'on');
            legend(ax, cellstr(series), 'Location', 'best');
        end

        function plot_phase1_runtime_metric_tile(ax, stage_summary, metric_name, ylabel_text)
            hold(ax, 'on');
            methods = {'fd', 'spectral'};
            labels = {'FD', 'SM'};
            styles = {[0.10 0.34 0.78], [0.87 0.43 0.10]};
            for i = 1:numel(methods)
                mask = strcmpi(string(stage_summary.stage_method), methods{i});
                if ~any(mask)
                    continue;
                end
                block = stage_summary(mask, :);
                runtime = cumsum(ExternalCollectorDispatcher.table_numeric_column(block, 'wall_time_s'));
                metric_values = ExternalCollectorDispatcher.table_numeric_column(block, metric_name);
                plot(ax, runtime, metric_values, '-o', 'LineWidth', 1.5, ...
                    'Color', styles{i}, 'MarkerFaceColor', styles{i}, 'DisplayName', labels{i});
            end
            hold(ax, 'off');
            xlabel(ax, 'Cumulative runtime (s)');
            ylabel(ax, ylabel_text);
            title(ax, erase(ylabel_text, ' (W)'));
            grid(ax, 'on');
            box(ax, 'on');
            legend(ax, 'Location', 'best');
        end

        function plot_phase1_stage_breakdown_tile(ax, stage_summary, metric_name, ylabel_text)
            metric_values = ExternalCollectorDispatcher.table_numeric_column(stage_summary, metric_name);
            labels = strings(height(stage_summary), 1);
            bar_colors = zeros(height(stage_summary), 3);
            for i = 1:height(stage_summary)
                stage_label = ExternalCollectorDispatcher.table_row_text(stage_summary, i, 'substage_label', ...
                    ExternalCollectorDispatcher.table_row_text(stage_summary, i, 'stage_label', sprintf('Stage %d', i)));
                stage_label = regexprep(char(string(stage_label)), '[_\s]+', ' ');
                method_label = ExternalCollectorDispatcher.table_row_text(stage_summary, i, 'stage_method', '');
                method_display = upper(strtrim(char(string(method_label))));
                if isempty(method_display)
                    labels(i) = string(strtrim(stage_label));
                else
                    labels(i) = string(sprintf('%s | %s', method_display, strtrim(stage_label)));
                end
                if contains(lower(method_label), 'spectral') || strcmpi(method_display, 'SM')
                    bar_colors(i, :) = [0.87 0.43 0.10];
                else
                    bar_colors(i, :) = [0.10 0.34 0.78];
                end
            end
            bars = bar(ax, metric_values, 0.72, 'FaceColor', 'flat');
            bars.CData = bar_colors;
            ax.XTick = 1:numel(labels);
            ax.XTickLabel = labels;
            ax.XTickLabelRotation = 25;
            xlabel(ax, 'Phase 1 stage');
            ylabel(ax, ylabel_text);
            title(ax, erase(ylabel_text, ' (W)'));
            grid(ax, 'on');
            box(ax, 'on');
        end

        function value = weighted_average(values, weights)
            value = NaN;
            if isempty(values)
                return;
            end
            values = double(values(:));
            weights = double(weights(:));
            valid = isfinite(values) & isfinite(weights) & weights > 0;
            if any(valid)
                value = sum(values(valid) .* weights(valid)) / sum(weights(valid));
                return;
            end
            finite_values = values(isfinite(values));
            if ~isempty(finite_values)
                value = mean(finite_values);
            end
        end

        function value = numeric_cell(values, index)
            value = NaN;
            if index >= 1 && index <= numel(values)
                value = values(index);
            end
        end

        function text = phase1_mesh_label(mesh_struct)
            text = '--';
            nx = ExternalCollectorDispatcher.pick_struct_number(mesh_struct, 'Nx', NaN);
            ny = ExternalCollectorDispatcher.pick_struct_number(mesh_struct, 'Ny', nx);
            if isfinite(nx) && isfinite(ny)
                text = sprintf('%dx%d', round(nx), round(ny));
            end
        end

        function value = table_row_number(tbl, row_idx_or_name, column_name, fallback)
            value = fallback;
            if nargin < 4
                fallback = NaN;
                value = fallback;
            end
            if isempty(tbl) || ~istable(tbl) || ~ismember(column_name, tbl.Properties.VariableNames)
                return;
            end
            if isnumeric(row_idx_or_name)
                row_idx = row_idx_or_name;
            else
                row_idx = 1;
            end
            if row_idx < 1 || row_idx > height(tbl)
                return;
            end
            raw = tbl.(column_name);
            if isnumeric(raw) && numel(raw) >= row_idx && isfinite(raw(row_idx))
                value = double(raw(row_idx));
            end
        end

        function text = table_row_text(tbl, row_idx, column_name, fallback)
            text = fallback;
            if nargin < 4
                fallback = '';
                text = fallback;
            end
            if isempty(tbl) || ~istable(tbl) || row_idx < 1 || row_idx > height(tbl) || ...
                    ~ismember(column_name, tbl.Properties.VariableNames)
                return;
            end
            raw = tbl.(column_name);
            if iscell(raw)
                text = char(string(raw{row_idx}));
            else
                text = char(string(raw(row_idx)));
            end
        end

        function values = table_numeric_column(tbl, column_name)
            values = nan(0, 1);
            if isempty(tbl) || ~istable(tbl) || ~ismember(column_name, tbl.Properties.VariableNames)
                return;
            end
            raw = tbl.(column_name);
            if isnumeric(raw)
                values = double(raw(:));
            else
                values = nan(height(tbl), 1);
            end
        end

        function item = pick_nested_struct(root_struct, varargin)
            item = struct();
            current = root_struct;
            for i = 1:numel(varargin)
                if ~(isstruct(current) && isfield(current, varargin{i}) && isstruct(current.(varargin{i})))
                    return;
                end
                current = current.(varargin{i});
            end
            item = current;
        end

        function sheet_def = build_default_run_workbook_sheet(summary_context, curated_table)
            mode_name = ExternalCollectorDispatcher.first_nonempty_text( ...
                ExternalCollectorDispatcher.pick_struct_text(summary_context, 'mode', ''), ...
                ExternalCollectorDispatcher.pick_struct_text(ExternalCollectorDispatcher.pick_struct_field(summary_context, 'run_config'), 'mode', ''), ...
                'Mode');
            sheet_name = ExternalCollectorDispatcher.sanitize_excel_sheet_name(mode_name, 'Mode', strings(1, 0));
            [cells_out, style_rows, summary_start_col] = ExternalCollectorDispatcher.build_workbook_telemetry_cells(curated_table);
            sheet_def = struct( ...
                'name', char(string(sheet_name)), ...
                'cells', {cells_out}, ...
                'style', style_rows, ...
                'summary_start_col', summary_start_col);
        end

        function cells_out = build_phase1_convergence_sheet(summary_context)
            headers = {'Method', 'Level', 'Grid', 'Converged', 'Selected Mesh', 'Successive Error', 'Tolerance', 'Runtime (s)', 'Source Path'};
            cells_out = headers;
            results_struct = ExternalCollectorDispatcher.pick_struct_field(summary_context, 'results');
            children = ExternalCollectorDispatcher.pick_struct_field(results_struct, 'children');
            method_specs = { ...
                struct('key', 'fd', 'label', 'FD'), ...
                struct('key', 'spectral', 'label', 'SM')};
            wrote_rows = false;
            for mi = 1:numel(method_specs)
                child = ExternalCollectorDispatcher.pick_struct_field(children, method_specs{mi}.key);
                convergence_output = ExternalCollectorDispatcher.pick_struct_field(child, 'convergence_output');
                convergence_results = ExternalCollectorDispatcher.pick_struct_field(convergence_output, 'results');
                run_records = ExternalCollectorDispatcher.pick_struct_value(convergence_results, 'run_records', struct([]));
                selected_mesh = ExternalCollectorDispatcher.pick_struct_field(child, 'selected_mesh');
                if ~isstruct(run_records) || isempty(run_records)
                    continue;
                end
                for ri = 1:numel(run_records)
                    nx = ExternalCollectorDispatcher.pick_struct_number(run_records(ri), 'Nx', NaN);
                    ny = ExternalCollectorDispatcher.pick_struct_number(run_records(ri), 'Ny', nx);
                    grid_label = '--';
                    if isfinite(nx) && isfinite(ny)
                        grid_label = sprintf('%03dx%03d', round(nx), round(ny));
                    end
                    selected_flag = false;
                    selected_nx = ExternalCollectorDispatcher.pick_struct_number(selected_mesh, 'Nx', NaN);
                    selected_ny = ExternalCollectorDispatcher.pick_struct_number(selected_mesh, 'Ny', selected_nx);
                    if isfinite(nx) && isfinite(ny) && isfinite(selected_nx) && isfinite(selected_ny)
                        selected_flag = round(nx) == round(selected_nx) && round(ny) == round(selected_ny);
                    end
                    converged_flag = ExternalCollectorDispatcher.pick_struct_value(run_records(ri), 'tolerance_met', []);
                    if islogical(converged_flag) || (isnumeric(converged_flag) && isscalar(converged_flag))
                        converged_text = ExternalCollectorDispatcher.yes_no_text(logical(converged_flag));
                    else
                        converged_text = '--';
                    end
                    cells_out(end + 1, 1:numel(headers)) = { ... %#ok<AGROW>
                        method_specs{mi}.label, ...
                        sprintf('L%02d', ri), ...
                        grid_label, ...
                        converged_text, ...
                        ExternalCollectorDispatcher.yes_no_text(selected_flag), ...
                        ExternalCollectorDispatcher.pick_struct_number(run_records(ri), 'relative_change', NaN), ...
                        ExternalCollectorDispatcher.pick_struct_number(selected_mesh, 'tolerance', NaN), ...
                        ExternalCollectorDispatcher.pick_struct_number(run_records(ri), 'runtime_wall_s', NaN), ...
                        ExternalCollectorDispatcher.pick_struct_text(run_records(ri), 'data_path', '')};
                    wrote_rows = true;
                end
            end
            if ~wrote_rows
                cells_out = {'No convergence rows available.'};
            end
        end

        function sheet_def = build_phase_method_workbook_sheet(curated_table, method_family, sheet_name)
            if nargin < 3 || strlength(string(strtrim(sheet_name))) == 0
                sheet_name = upper(char(string(method_family)));
            end
            sections = repmat(struct('title', '', 'table', table(), 'method', ''), 1, 0);
            sections(end + 1) = struct( ... %#ok<AGROW>
                'title', 'Mesh Convergence', ...
                'table', ExternalCollectorDispatcher.filter_curated_table(curated_table, ...
                    @(tbl) ExternalCollectorDispatcher.method_mask(tbl, method_family) & ...
                    ExternalCollectorDispatcher.stage_type_mask(tbl, {'convergence'})), ...
                'method', method_family);
            sections(end + 1) = struct( ... %#ok<AGROW>
                'title', 'Evolution', ...
                'table', ExternalCollectorDispatcher.filter_curated_table(curated_table, ...
                    @(tbl) ExternalCollectorDispatcher.method_mask(tbl, method_family) & ...
                    ExternalCollectorDispatcher.stage_type_mask(tbl, {'evolution'})), ...
                'method', method_family);
            [cells_out, style_rows, summary_start_col] = ExternalCollectorDispatcher.build_workbook_cells_from_sections(curated_table, sections);
            sheet_def = struct( ...
                'name', char(string(sheet_name)), ...
                'cells', {cells_out}, ...
                'style', style_rows, ...
                'summary_start_col', summary_start_col);
        end

        function sheet_def = build_phase_scenario_workbook_sheet(curated_table, scenario_id, sheet_name)
            scenario_token = char(string(scenario_id));
            if nargin < 3 || strlength(string(strtrim(sheet_name))) == 0
                sheet_name = scenario_token;
            end
            sections = repmat(struct('title', '', 'table', table(), 'method', ''), 1, 0);
            sections(end + 1) = struct( ... %#ok<AGROW>
                'title', 'FD', ...
                'table', ExternalCollectorDispatcher.filter_curated_table(curated_table, ...
                    @(tbl) ExternalCollectorDispatcher.scenario_mask(tbl, scenario_token) & ...
                    ExternalCollectorDispatcher.method_mask(tbl, 'fd')), ...
                'method', 'fd');
            sections(end + 1) = struct( ... %#ok<AGROW>
                'title', 'SM', ...
                'table', ExternalCollectorDispatcher.filter_curated_table(curated_table, ...
                    @(tbl) ExternalCollectorDispatcher.scenario_mask(tbl, scenario_token) & ...
                    ExternalCollectorDispatcher.method_mask(tbl, 'spectral')), ...
                'method', 'spectral');
            [cells_out, style_rows, summary_start_col] = ExternalCollectorDispatcher.build_workbook_cells_from_sections(curated_table, sections);
            sheet_def = struct( ...
                'name', char(string(sheet_name)), ...
                'cells', {cells_out}, ...
                'style', style_rows, ...
                'summary_start_col', summary_start_col);
        end

        function [cells_out, style_rows, summary_start_col] = build_workbook_cells_from_sections(curated_table, sections)
            if nargin < 1 || isempty(curated_table)
                cells_out = {'No telemetry rows available.'};
                style_rows = struct('kind', 'empty', 'method', '');
                summary_start_col = 3;
                return;
            end
            headers = curated_table.Properties.VariableNames;
            cells_out = cell(1, numel(headers));
            cells_out(1, :) = reshape(cellstr(string(headers)), 1, []);
            style_rows = struct('kind', 'header', 'method', '');
            for section_index = 1:numel(sections)
                section = sections(section_index);
                if section_index > 1
                    cells_out(end + 1, 1:numel(headers)) = {''}; %#ok<AGROW>
                    style_rows(end + 1) = struct('kind', 'spacer', 'method', char(string(section.method))); %#ok<AGROW>
                end
                cells_out(end + 1, 1:numel(headers)) = {''}; %#ok<AGROW>
                cells_out(end, 1) = {char(string(section.title))};
                style_rows(end + 1) = struct('kind', 'section_header', 'method', char(string(section.method))); %#ok<AGROW>
                if isempty(section.table)
                    cells_out(end + 1, 1:numel(headers)) = {''}; %#ok<AGROW>
                    cells_out(end, 1:2) = {'Status', 'No telemetry rows available.'};
                    style_rows(end + 1) = struct('kind', 'empty_block', 'method', char(string(section.method))); %#ok<AGROW>
                    continue;
                end
                [cells_out, style_rows] = ExternalCollectorDispatcher.append_workbook_block_rows(cells_out, style_rows, section.table);
            end
            summary_start_col = numel(headers) + 2;
        end

        function [cells_out, style_rows] = append_workbook_block_rows(cells_out, style_rows, curated_block)
            stage_ids = string(ExternalCollectorDispatcher.table_text_column(curated_block, 'stage_id'));
            last_stage = "";
            last_mesh_signature = "";
            headers = curated_block.Properties.VariableNames;
            for i = 1:height(curated_block)
                current_stage = stage_ids(i);
                current_method = ExternalCollectorDispatcher.telemetry_row_method(curated_block, i);
                current_mesh_signature = ExternalCollectorDispatcher.telemetry_mesh_signature(curated_block, i);
                if i == 1 || current_stage ~= last_stage
                    label = string(ExternalCollectorDispatcher.row_text(curated_block, i, 'stage_label'));
                    if strlength(label) == 0
                        label = current_stage;
                    end
                    stage_type = ExternalCollectorDispatcher.row_text(curated_block, i, 'stage_type');
                    method_text = ExternalCollectorDispatcher.row_text(curated_block, i, 'stage_method');
                    cells_out(end + 1, 1:numel(headers)) = {''}; %#ok<AGROW>
                    cells_out(end, 1:4) = { ...
                        sprintf('Stage: %s', char(label)), ...
                        char(current_stage), ...
                        char(stage_type), ...
                        char(method_text)};
                    style_rows(end + 1) = struct('kind', 'stage_header', 'method', current_method); %#ok<AGROW>
                    last_mesh_signature = "";
                end
                if strlength(current_mesh_signature) > 0 && current_mesh_signature ~= last_mesh_signature
                    cells_out(end + 1, 1:numel(headers)) = {''}; %#ok<AGROW>
                    cells_out(end, 1:4) = { ...
                        sprintf('Iteration block: %s', char(current_mesh_signature)), ...
                        char(ExternalCollectorDispatcher.row_text(curated_block, i, 'scenario_id')), ...
                        char(ExternalCollectorDispatcher.row_text(curated_block, i, 'stage_method')), ...
                        char(ExternalCollectorDispatcher.row_text(curated_block, i, 'stage_type'))};
                    style_rows(end + 1) = struct('kind', 'mesh_header', 'method', current_method); %#ok<AGROW>
                end
                cells_out(end + 1, 1:numel(headers)) = table2cell(curated_block(i, :)); %#ok<AGROW>
                style_rows(end + 1) = struct('kind', 'data', 'method', current_method); %#ok<AGROW>
                last_stage = current_stage;
                last_mesh_signature = current_mesh_signature;
            end
        end

        function scenario_ids = phase_workbook_scenario_ids(summary_context, curated_table)
            scenario_ids = strings(1, 0);
            if ~isempty(curated_table) && ismember('scenario_id', curated_table.Properties.VariableNames)
                values = unique(string(curated_table.scenario_id));
                values = values(strlength(strtrim(values)) > 0);
                scenario_ids = values(:).';
            end
            if ~isempty(scenario_ids)
                return;
            end
            results_struct = ExternalCollectorDispatcher.pick_struct_field(summary_context, 'results');
            scenarios = ExternalCollectorDispatcher.pick_struct_value(results_struct, 'scenarios', struct([]));
            if ~isstruct(scenarios)
                return;
            end
            ids = strings(1, numel(scenarios));
            for i = 1:numel(scenarios)
                ids(i) = string(ExternalCollectorDispatcher.first_nonempty_text( ...
                    ExternalCollectorDispatcher.pick_struct_text(scenarios(i), 'scenario_id', ''), ...
                    ExternalCollectorDispatcher.pick_struct_text(scenarios(i), 'id', '')));
            end
            ids = ids(strlength(strtrim(ids)) > 0);
            scenario_ids = unique(ids, 'stable');
        end

        function sheet_name = resolve_phase_scenario_sheet_name(summary_context, scenario_id, fallback_index)
            if nargin < 3
                fallback_index = 1;
            end
            scenario_token = char(string(scenario_id));
            sheet_name = ExternalCollectorDispatcher.first_nonempty_text(scenario_token, sprintf('Scenario %d', fallback_index));
            results_struct = ExternalCollectorDispatcher.pick_struct_field(summary_context, 'results');
            scenarios = ExternalCollectorDispatcher.pick_struct_value(results_struct, 'scenarios', struct([]));
            if ~isstruct(scenarios)
                return;
            end
            for i = 1:numel(scenarios)
                current_id = ExternalCollectorDispatcher.first_nonempty_text( ...
                    ExternalCollectorDispatcher.pick_struct_text(scenarios(i), 'scenario_id', ''), ...
                    ExternalCollectorDispatcher.pick_struct_text(scenarios(i), 'id', ''));
                if ~strcmpi(current_id, scenario_token)
                    continue;
                end
                sheet_name = ExternalCollectorDispatcher.first_nonempty_text( ...
                    ExternalCollectorDispatcher.pick_struct_text(scenarios(i), 'scenario_label', ''), ...
                    ExternalCollectorDispatcher.pick_struct_text(scenarios(i), 'label', ''), ...
                    sheet_name);
                return;
            end
        end

        function sheet_name = sanitize_excel_sheet_name(sheet_name, fallback_name, used_names)
            if nargin < 2 || strlength(string(strtrim(fallback_name))) == 0
                fallback_name = 'Sheet';
            end
            if nargin < 3
                used_names = strings(1, 0);
            end
            sheet_name = char(string(sheet_name));
            sheet_name = regexprep(sheet_name, '[:\\/\?\*\[\]]', '_');
            sheet_name = strtrim(sheet_name);
            if isempty(sheet_name)
                sheet_name = char(string(fallback_name));
            end
            if strlength(string(sheet_name)) > 31
                sheet_name = extractBefore(string(sheet_name), 32);
                sheet_name = char(sheet_name);
            end
            candidate = string(sheet_name);
            suffix_index = 1;
            while any(strcmpi(candidate, used_names))
                suffix = sprintf('_%d', suffix_index);
                max_base = max(1, 31 - strlength(string(suffix)));
                base_name = extractBefore(candidate, max_base + 1);
                candidate = base_name + suffix;
                suffix_index = suffix_index + 1;
            end
            sheet_name = char(candidate);
        end

        function block = filter_curated_table(curated_table, mask_fn)
            block = table();
            if nargin < 2 || isempty(curated_table)
                return;
            end
            mask = false(height(curated_table), 1);
            if isa(mask_fn, 'function_handle')
                mask = logical(mask_fn(curated_table));
            end
            if numel(mask) ~= height(curated_table)
                mask = false(height(curated_table), 1);
            end
            block = curated_table(mask, :);
        end

        function mask = method_mask(curated_table, method_family)
            method_tokens = string(ExternalCollectorDispatcher.table_text_column(curated_table, 'stage_method'));
            mask = false(height(curated_table), 1);
            for i = 1:height(curated_table)
                mask(i) = strcmpi(ExternalCollectorDispatcher.normalize_method_family(method_tokens(i)), ...
                    char(string(method_family)));
            end
        end

        function mask = stage_type_mask(curated_table, stage_tokens)
            if nargin < 2 || ~iscell(stage_tokens)
                stage_tokens = {char(string(stage_tokens))};
            end
            stage_values = lower(string(ExternalCollectorDispatcher.table_text_column(curated_table, 'stage_type')));
            mask = false(height(curated_table), 1);
            for i = 1:numel(stage_tokens)
                token = lower(char(string(stage_tokens{i})));
                mask = mask | contains(stage_values, token);
            end
        end

        function mask = scenario_mask(curated_table, scenario_id)
            scenario_values = string(ExternalCollectorDispatcher.table_text_column(curated_table, 'scenario_id'));
            mask = strcmpi(scenario_values, string(scenario_id));
        end

        function [cells_out, style_rows, summary_start_col] = build_workbook_telemetry_cells(curated_table)
            if nargin < 1 || isempty(curated_table)
                cells_out = {'No telemetry rows available.'};
                style_rows = struct('kind', 'empty', 'method', '');
                summary_start_col = 3;
                return;
            end
            headers = curated_table.Properties.VariableNames;
            cells_out = cell(1, numel(headers));
            cells_out(1, :) = reshape(cellstr(string(headers)), 1, []);
            style_rows = repmat(struct('kind', '', 'method', ''), 1, 0);
            style_rows(end + 1) = struct('kind', 'header', 'method', ''); %#ok<AGROW>
            stage_ids = string(ExternalCollectorDispatcher.table_text_column(curated_table, 'stage_id'));
            last_stage = "";
            last_mesh_signature = "";
            for i = 1:height(curated_table)
                current_stage = stage_ids(i);
                current_method = ExternalCollectorDispatcher.telemetry_row_method(curated_table, i);
                current_mesh_signature = ExternalCollectorDispatcher.telemetry_mesh_signature(curated_table, i);
                if i == 1 || current_stage ~= last_stage
                    label = string(ExternalCollectorDispatcher.row_text(curated_table, i, 'stage_label'));
                    if strlength(label) == 0
                        label = current_stage;
                    end
                    stage_type = ExternalCollectorDispatcher.row_text(curated_table, i, 'stage_type');
                    method_text = ExternalCollectorDispatcher.row_text(curated_table, i, 'stage_method');
                    cells_out(end + 1, 1:numel(headers)) = {''}; %#ok<AGROW>
                    cells_out(end, 1:4) = { ...
                        sprintf('Stage: %s', char(label)), ...
                        char(current_stage), ...
                        char(stage_type), ...
                        char(method_text)};
                    style_rows(end + 1) = struct('kind', 'stage_header', 'method', current_method); %#ok<AGROW>
                    last_mesh_signature = "";
                end
                if strlength(current_mesh_signature) > 0 && current_mesh_signature ~= last_mesh_signature
                    cells_out(end + 1, 1:numel(headers)) = {''}; %#ok<AGROW>
                    cells_out(end, 1:4) = { ...
                        sprintf('Iteration block: %s', char(current_mesh_signature)), ...
                        char(ExternalCollectorDispatcher.row_text(curated_table, i, 'scenario_id')), ...
                        char(ExternalCollectorDispatcher.row_text(curated_table, i, 'stage_method')), ...
                        char(ExternalCollectorDispatcher.row_text(curated_table, i, 'stage_type'))};
                    style_rows(end + 1) = struct('kind', 'mesh_header', 'method', current_method); %#ok<AGROW>
                end
                row_cells = table2cell(curated_table(i, :));
                cells_out(end + 1, 1:numel(headers)) = row_cells; %#ok<AGROW>
                style_rows(end + 1) = struct('kind', 'data', 'method', current_method); %#ok<AGROW>
                last_stage = current_stage;
                last_mesh_signature = current_mesh_signature;
            end
            summary_start_col = numel(headers) + 2;
        end

        function cells_out = build_workbook_summary_cells(summary_context, monitor_series, curated_table, stage_summary)
            cells_out = {};
            total_energy = ExternalCollectorDispatcher.stage_terminal_value(curated_table, 'environmental_energy_wh_cum');
            if ~isfinite(total_energy)
                total_energy = ExternalCollectorDispatcher.monitor_series_terminal_value(monitor_series, 'environmental_energy_wh_cum');
            end
            total_co2 = ExternalCollectorDispatcher.stage_terminal_value(curated_table, 'environmental_co2_g_cum');
            if ~isfinite(total_co2)
                total_co2 = ExternalCollectorDispatcher.monitor_series_terminal_value(monitor_series, 'environmental_co2_g_cum');
            end
            total_power = ExternalCollectorDispatcher.table_column_or_fallback(curated_table, 'system_power_w', ...
                ExternalCollectorDispatcher.table_sum_columns(curated_table, {'cpu_power_w_hwinfo', 'gpu_power_w_hwinfo', 'memory_power_w_or_proxy'}));
            if isempty(total_power)
                total_power = ExternalCollectorDispatcher.series_column(monitor_series, 'system_power_w', []);
                if isempty(total_power)
                    total_power = ExternalCollectorDispatcher.series_column(monitor_series, 'power_w', []);
                end
            end
            wall_time_s = ExternalCollectorDispatcher.stage_wall_time(curated_table);
            if ~isfinite(wall_time_s)
                wall_time_s = ExternalCollectorDispatcher.monitor_series_wall_time(monitor_series);
            end
            collector_status_text = ExternalCollectorDispatcher.collector_status_summary_text(monitor_series);
            preferred_source_text = ExternalCollectorDispatcher.preferred_source_summary_text(monitor_series);
            raw_path_text = ExternalCollectorDispatcher.raw_paths_summary_text(monitor_series);
            parameter_rows = ExternalCollectorDispatcher.build_parameter_summary_rows(summary_context);
            results_struct = ExternalCollectorDispatcher.pick_struct_field(summary_context, 'results');
            rmse_payload = ExternalCollectorDispatcher.pick_struct_field(results_struct, 'rmse_vs_time');
            if isempty(fieldnames(rmse_payload))
                rmse_payload = ExternalCollectorDispatcher.pick_struct_field( ...
                    ExternalCollectorDispatcher.pick_struct_field(results_struct, 'combined'), 'rmse_vs_time');
            end
            stage_count = height(ExternalCollectorDispatcher.safe_table(stage_summary));
            if stage_count < 1
                stage_count = numel(parameter_rows);
            end

            cells_out = { ...
                'Phase Summary', '', '', ''; ...
                'Workflow kind', ExternalCollectorDispatcher.resolve_summary_workflow_kind(summary_context, monitor_series), '', ''; ...
                'Phase ID', ExternalCollectorDispatcher.resolve_summary_phase_id(summary_context, monitor_series), '', ''; ...
                'Run ID', ExternalCollectorDispatcher.resolve_summary_run_id(summary_context, monitor_series), '', ''; ...
                'Collector transport', ExternalCollectorDispatcher.pick_text_field(monitor_series, 'hwinfo_transport', 'none'), '', ''; ...
                'Collector status', collector_status_text, '', ''; ...
                'Preferred source', preferred_source_text, '', ''; ...
                'Raw collector paths', raw_path_text, '', ''; ...
                'Wall time (s)', wall_time_s, '', ''; ...
                'Mean total power (W)', ExternalCollectorDispatcher.nanmean_safe(total_power), '', ''; ...
                'Peak total power (W)', ExternalCollectorDispatcher.nanmax_safe(total_power), '', ''; ...
                'Energy total (Wh)', total_energy, '', ''; ...
                'CO2e total (g)', total_co2, '', ''; ...
                'CPU temp peak (C)', ExternalCollectorDispatcher.nanmax_or_fallback( ...
                    ExternalCollectorDispatcher.table_column(curated_table, 'cpu_temp_c'), ...
                    ExternalCollectorDispatcher.series_column(monitor_series, 'cpu_temp_c', [])), '', ''; ...
                'Coolant temp peak (C)', ExternalCollectorDispatcher.nanmax_or_fallback( ...
                    ExternalCollectorDispatcher.table_column(curated_table, 'coolant_temp_c'), ...
                    ExternalCollectorDispatcher.series_column(monitor_series, 'coolant_temp_c', [])), '', ''; ...
                'Stage count', stage_count, '', ''; ...
                '', '', '', ''; ...
                'Subphase breakdown', '', '', ''; ...
                'Stage ID', 'Label / Method', 'Wall time (s)', 'Energy / CO2e'};
            if ~isempty(stage_summary)
                for i = 1:height(stage_summary)
                    cells_out(end + 1, 1:4) = { ... %#ok<AGROW>
                        char(string(stage_summary.stage_id(i))), ...
                        sprintf('%s | %s', ...
                            char(string(stage_summary.stage_label(i))), ...
                            char(string(stage_summary.stage_method(i)))), ...
                        stage_summary.wall_time_s(i), ...
                        sprintf('%s Wh | %s g', ...
                            ExternalCollectorDispatcher.numeric_or_dash(stage_summary.energy_wh_total(i), '%.4g'), ...
                            ExternalCollectorDispatcher.numeric_or_dash(stage_summary.co2_g_total(i), '%.4g'))};
                end
            else
                rows = ExternalCollectorDispatcher.build_subphase_breakdown_rows(summary_context);
                if isempty(rows)
                    cells_out(end + 1, 1:4) = {'--', 'No stage summary rows captured.', '', ''}; %#ok<AGROW>
                else
                    for i = 1:numel(rows)
                        cells_out(end + 1, 1:4) = { ... %#ok<AGROW>
                            rows(i).stage_id, ...
                            rows(i).label_and_method, ...
                            rows(i).wall_time_s, ...
                            rows(i).energy_and_co2};
                    end
                end
            end
            if ~isempty(fieldnames(rmse_payload))
                cells_out(end + 1, 1:4) = {'', '', '', ''}; %#ok<AGROW>
                cells_out(end + 1, 1:4) = {'Phase 1 RMSE summary', '', '', ''}; %#ok<AGROW>
                cells_out(end + 1, 1:4) = {'Metric', 'Value', 'Grid', 'Interpolation'}; %#ok<AGROW>
                cells_out(end + 1, 1:4) = {'Absolute RMSE mean', ...
                    ExternalCollectorDispatcher.pick_struct_number(rmse_payload, 'abs_rmse_mean', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_text(rmse_payload, 'comparison_grid_label', '--'), ...
                    ExternalCollectorDispatcher.pick_struct_text(rmse_payload, 'spatial_interpolation', '--')}; %#ok<AGROW>
                cells_out(end + 1, 1:4) = {'Absolute RMSE peak', ...
                    ExternalCollectorDispatcher.pick_struct_number(rmse_payload, 'abs_rmse_peak', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_text(rmse_payload, 'comparison_grid_label', '--'), ...
                    ExternalCollectorDispatcher.pick_struct_text(rmse_payload, 'temporal_interpolation', '--')}; %#ok<AGROW>
                cells_out(end + 1, 1:4) = {'Relative RMSE mean', ...
                    ExternalCollectorDispatcher.pick_struct_number(rmse_payload, 'rel_rmse_mean', NaN), ...
                    ExternalCollectorDispatcher.pick_struct_text(rmse_payload, 'fd_mesh_label', '--'), ...
                    ExternalCollectorDispatcher.pick_struct_text(rmse_payload, 'spectral_mesh_label', '--')}; %#ok<AGROW>
            end
            cells_out(end + 1, 1:4) = {'', '', '', ''}; %#ok<AGROW>
            cells_out(end + 1, 1:4) = {'Subphase parameter summary', '', '', ''}; %#ok<AGROW>
            cells_out(end + 1, 1:4) = {'Stage', 'Problem setup', 'Runtime params', 'Frames'}; %#ok<AGROW>
            if isempty(parameter_rows)
                cells_out(end + 1, 1:4) = {'--', 'No saved workflow parameter blocks were available.', '', ''}; %#ok<AGROW>
            else
                for i = 1:numel(parameter_rows)
                    cells_out(end + 1, 1:4) = { ... %#ok<AGROW>
                        parameter_rows(i).stage_label, ...
                        parameter_rows(i).problem_setup, ...
                        parameter_rows(i).runtime_setup, ...
                        parameter_rows(i).frame_setup};
                end
            end
        end

        function status = try_style_phase_workbook(workbook_path, telemetry_sheets, summary_width, support_sheets)
            status = 'unstyled';
            if nargin < 4 || isempty(support_sheets)
                support_sheets = {'Stage Summary', 'Metric Guide', 'Normalized Telemetry'};
            end
            try
                excel = actxserver('Excel.Application');
            catch
                return;
            end
            cleanup_excel = onCleanup(@() ExternalCollectorDispatcher.close_excel(excel)); %#ok<NASGU>
            workbook = [];
            workbook_open = false;
            try
                workbook = excel.Workbooks.Open(workbook_path);
                workbook_open = true;
                for sheet_index = 1:numel(telemetry_sheets)
                    try
                        sheet = workbook.Worksheets.Item(telemetry_sheets(sheet_index).name);
                    catch
                        continue;
                    end
                    sheet_cells = telemetry_sheets(sheet_index).cells;
                    sheet_style = telemetry_sheets(sheet_index).style;
                    if isempty(sheet_cells)
                        continue;
                    end
                    header_range = sprintf('A1:%s1', ExternalCollectorDispatcher.excel_column_name(size(sheet_cells, 2)));
                    sheet.Range(header_range).Font.Bold = true;
                    sheet.Range(header_range).Interior.Color = 14540253;
                    last_col_name = ExternalCollectorDispatcher.excel_column_name(size(sheet_cells, 2));
                    for r = 1:min(numel(sheet_style), size(sheet_cells, 1))
                        row_kind = char(string(sheet_style(r).kind));
                        method_family = char(string(sheet_style(r).method));
                        row_range = sprintf('A%d:%s%d', r, last_col_name, r);
                        switch row_kind
                            case 'section_header'
                                sheet.Range(row_range).Font.Bold = true;
                                sheet.Range(row_range).Interior.Color = 14281213;
                            case 'selected_mesh'
                                sheet.Range(row_range).Font.Bold = true;
                                sheet.Range(row_range).Interior.Color = 13434828;
                            case 'stage_header'
                                sheet.Range(row_range).Font.Bold = true;
                                sheet.Range(row_range).Interior.Color = 15790320;
                            case 'mesh_header'
                                sheet.Range(row_range).Font.Bold = true;
                                sheet.Range(row_range).Interior.Color = 15198183;
                            case 'mesh_block_a_header'
                                sheet.Range(row_range).Font.Bold = true;
                                sheet.Range(row_range).Interior.Color = 15790320;
                            case 'mesh_block_b_header'
                                sheet.Range(row_range).Font.Bold = true;
                                sheet.Range(row_range).Interior.Color = 15198183;
                            case 'mesh_block_c_header'
                                sheet.Range(row_range).Font.Bold = true;
                                sheet.Range(row_range).Interior.Color = 14281213;
                            case 'mesh_block_a_data'
                                sheet.Range(row_range).Interior.Color = 15921906;
                            case 'mesh_block_b_data'
                                sheet.Range(row_range).Interior.Color = 15198183;
                            case 'mesh_block_c_data'
                                sheet.Range(row_range).Interior.Color = 14544639;
                            case 'empty_block'
                                sheet.Range(row_range).Font.Italic = true;
                                sheet.Range(row_range).Interior.Color = 15658734;
                            case 'data'
                                switch method_family
                                    case 'fd'
                                        sheet.Range(row_range).Interior.Color = 15921906;
                                    case 'spectral'
                                        sheet.Range(row_range).Interior.Color = 14544639;
                                end
                        end
                    end
                    if nargin >= 3 && summary_width >= 1 && isfield(telemetry_sheets(sheet_index), 'summary_start_col')
                        summary_start_col = telemetry_sheets(sheet_index).summary_start_col;
                        if isfinite(summary_start_col) && summary_start_col >= 1
                            summary_end_col = summary_start_col + summary_width - 1;
                            summary_header_range = sprintf('%s1:%s1', ...
                                ExternalCollectorDispatcher.excel_column_name(summary_start_col), ...
                                ExternalCollectorDispatcher.excel_column_name(summary_end_col));
                            sheet.Range(summary_header_range).Font.Bold = true;
                            sheet.Range(summary_header_range).Interior.Color = 14281213;
                        end
                    end
                    sheet.Columns.AutoFit();
                end
                for i = 1:numel(support_sheets)
                    try
                        support_sheet = workbook.Worksheets.Item(support_sheets{i});
                        support_sheet.Range('A1:Z1').Font.Bold = true;
                        support_sheet.Range('A1:Z1').Interior.Color = 14540253;
                        support_sheet.Columns.AutoFit();
                    catch
                    end
                end
                workbook.Save();
                status = 'styled_excel';
            catch
                status = 'unstyled';
            end
            if workbook_open
                try
                    workbook.Close(true);
                catch
                end
            end
        end

        function [run_id, monitor_series, paths, summary_context] = parse_artifact_inputs(varargin)
            run_id = '';
            monitor_series = struct();
            paths = struct();
            summary_context = struct();

            first_arg_is_context = false;
            if nargin >= 1 && isstruct(varargin{1})
                first_fields = fieldnames(varargin{1});
                context_markers = {'monitor_series', 'results', 'run_config', 'paths', 'workflow_kind', ...
                    'phase_id', 'metadata', 'run_id'};
                first_arg_is_context = any(ismember(context_markers, first_fields));
            end

            if first_arg_is_context
                summary_context = varargin{1};
                if nargin >= 2 && isstruct(varargin{2}) && ~isempty(fieldnames(varargin{2}))
                    monitor_series = varargin{2};
                else
                    monitor_series = ExternalCollectorDispatcher.pick_struct_field(summary_context, 'monitor_series');
                end
                if nargin >= 3 && isstruct(varargin{3}) && ~isempty(fieldnames(varargin{3}))
                    paths = varargin{3};
                else
                    paths = ExternalCollectorDispatcher.pick_struct_field(summary_context, 'paths');
                end
                if isempty(fieldnames(paths))
                    results_struct = ExternalCollectorDispatcher.pick_struct_field(summary_context, 'results');
                    paths = ExternalCollectorDispatcher.pick_struct_field(results_struct, 'paths');
                end
                if ~isfield(summary_context, 'monitor_series') || ~isstruct(summary_context.monitor_series) || ...
                        isempty(fieldnames(summary_context.monitor_series))
                    summary_context.monitor_series = monitor_series;
                end
                if ~isfield(summary_context, 'paths') || ~isstruct(summary_context.paths) || ...
                        isempty(fieldnames(summary_context.paths))
                    summary_context.paths = paths;
                end
                run_id = ExternalCollectorDispatcher.resolve_summary_run_id(summary_context, monitor_series);
                monitor_series = ExternalCollectorDispatcher.recover_monitor_series_from_summary(summary_context, monitor_series, paths);
                return;
            end

            if nargin >= 1 && isstruct(varargin{1})
                monitor_series = varargin{1};
            end
            if nargin >= 2 && isstruct(varargin{2})
                paths = varargin{2};
            end
            if nargin >= 3
                run_id = char(string(varargin{3}));
            end
            summary_context = struct( ...
                'run_id', run_id, ...
                'monitor_series', monitor_series, ...
                'paths', paths);
            monitor_series = ExternalCollectorDispatcher.recover_monitor_series_from_summary(summary_context, monitor_series, paths);
        end

        function monitor_series = recover_monitor_series_from_summary(summary_context, monitor_series, paths)
            if nargin < 2 || ~isstruct(monitor_series)
                monitor_series = struct();
            end
            if nargin < 3 || ~isstruct(paths)
                paths = struct();
            end
            monitor_series = ExternalCollectorDispatcher.normalize_collector_payload(monitor_series);
            if ExternalCollectorDispatcher.monitor_series_has_timebase(monitor_series) && ...
                    (ExternalCollectorDispatcher.hwinfo_series_present(monitor_series) || ...
                    ExternalCollectorDispatcher.hwinfo_catalog_present(monitor_series))
                return;
            end

            results_struct = ExternalCollectorDispatcher.pick_struct_field(summary_context, 'results');
            recovered = ExternalCollectorDispatcher.monitor_series_from_raw_hwinfo_csv(results_struct, paths, monitor_series, summary_context);
            if ExternalCollectorDispatcher.monitor_series_has_timebase(recovered)
                monitor_series = recovered;
                return;
            end

            recovered = ExternalCollectorDispatcher.synthesize_monitor_series_from_results(results_struct, monitor_series, summary_context);
            if ExternalCollectorDispatcher.monitor_series_has_timebase(recovered) || ...
                    ExternalCollectorDispatcher.hwinfo_series_present(recovered) || ...
                    ExternalCollectorDispatcher.hwinfo_catalog_present(recovered)
                monitor_series = recovered;
            end
        end

        function tf = monitor_series_has_timebase(monitor_series)
            tf = isstruct(monitor_series) && isfield(monitor_series, 't') && ...
                isnumeric(monitor_series.t) && ~isempty(monitor_series.t);
        end

        function monitor_series = monitor_series_from_raw_hwinfo_csv(results_struct, paths, fallback_monitor_series, summary_context)
            monitor_series = struct();
            csv_path = ExternalCollectorDispatcher.resolve_hwinfo_csv_path(results_struct, paths, fallback_monitor_series);
            if isempty(csv_path) || exist(csv_path, 'file') ~= 2
                return;
            end
            try
                data_table = readtable(csv_path);
            catch
                return;
            end
            time_var = '';
            if ismember('session_time_s', data_table.Properties.VariableNames)
                time_var = 'session_time_s';
            elseif ismember('t', data_table.Properties.VariableNames)
                time_var = 't';
            end
            if isempty(data_table) || isempty(time_var)
                return;
            end

            metric_keys = {'cpu_proxy', 'gpu_series', 'memory_series', 'cpu_temp_c', 'system_power_w', ...
                'cpu_voltage_v', 'gpu_voltage_v', 'memory_voltage_v', 'cpu_power_w_hwinfo', ...
                'gpu_power_w_hwinfo', 'memory_power_w_or_proxy', 'environmental_energy_wh_cum', ...
                'environmental_co2_g_cum', 'fan_rpm', 'pump_rpm', 'coolant_temp_c', 'device_battery_level'};
            sample = ExternalCollectorDispatcher.empty_sample();
            sample.collector_status.hwinfo = ExternalCollectorDispatcher.table_last_text(data_table, 'hwinfo_status', 'shared_memory_connected');
            sample.hwinfo_transport = ExternalCollectorDispatcher.table_last_text(data_table, 'hwinfo_transport', 'shared_memory');
            sample.raw_log_paths.hwinfo = csv_path;

            monitor_series = sample;
            monitor_series.t = reshape(double(data_table.(time_var)), 1, []);
            monitor_series.elapsed_wall_time = monitor_series.t;
            if ismember('timestamp_utc', data_table.Properties.VariableNames)
                monitor_series.wall_clock_time = ExternalCollectorDispatcher.utc_series_to_posix(data_table.timestamp_utc);
            end

            monitor_series.collector_series.hwinfo = struct();
            for i = 1:numel(metric_keys)
                key = metric_keys{i};
                if ~ismember(key, data_table.Properties.VariableNames)
                    continue;
                end
                values = reshape(double(data_table.(key)), 1, []);
                monitor_series.collector_series.hwinfo.(key) = values;
                monitor_series.(key) = values;
            end

            results_catalog = ExternalCollectorDispatcher.pick_struct_field(results_struct, 'collector_metric_catalog');
            fallback_catalog = ExternalCollectorDispatcher.pick_struct_field(fallback_monitor_series, 'collector_metric_catalog');
            if ~isempty(results_catalog)
                monitor_series.collector_metric_catalog = results_catalog;
            elseif ~isempty(fallback_catalog)
                monitor_series.collector_metric_catalog = fallback_catalog;
            end
            if ~isfield(monitor_series, 'collector_metric_catalog') || isempty(monitor_series.collector_metric_catalog)
                monitor_series.collector_metric_catalog = ExternalCollectorDispatcher.derive_metric_catalog_from_series(monitor_series);
            end

            monitor_series.workflow_kind = ExternalCollectorDispatcher.resolve_summary_workflow_kind(summary_context, monitor_series);
            monitor_series.workflow_phase_id = ExternalCollectorDispatcher.resolve_summary_phase_id(summary_context, monitor_series);
            monitor_series = ExternalCollectorDispatcher.normalize_collector_payload(monitor_series);
        end

        function monitor_series = synthesize_monitor_series_from_results(results_struct, fallback_monitor_series, summary_context)
            monitor_series = struct();
            if ~isstruct(results_struct)
                return;
            end

            sample = struct();
            if isfield(results_struct, 'collector_last_sample') && isstruct(results_struct.collector_last_sample)
                sample = ExternalCollectorDispatcher.normalize_collector_payload(results_struct.collector_last_sample);
            end
            if isfield(results_struct, 'collector_session') && isstruct(results_struct.collector_session)
                session_sample = ExternalCollectorDispatcher.normalize_collector_payload(results_struct.collector_session);
                if isempty(fieldnames(sample)) || ...
                        (~ExternalCollectorDispatcher.hwinfo_series_present(sample) && ExternalCollectorDispatcher.hwinfo_series_present(session_sample)) || ...
                        (~ExternalCollectorDispatcher.hwinfo_catalog_present(sample) && ExternalCollectorDispatcher.hwinfo_catalog_present(session_sample))
                    sample = session_sample;
                end
            end
            if isempty(fieldnames(sample))
                return;
            end

            metric_keys = {'cpu_proxy', 'gpu_series', 'memory_series', 'cpu_temp_c', 'power_w', ...
                'cpu_voltage_v', 'gpu_voltage_v', 'memory_voltage_v', 'cpu_power_w_hwinfo', ...
                'gpu_power_w_hwinfo', 'memory_power_w_or_proxy', 'system_power_w', ...
                'environmental_energy_wh_cum', 'environmental_co2_g_cum', 'fan_rpm', ...
                'pump_rpm', 'coolant_temp_c', 'device_battery_level'};
            monitor_series = sample;
            monitor_series.t = [0, 1];
            monitor_series.elapsed_wall_time = [0, 1];

            if ~isfield(monitor_series, 'collector_series') || ~isstruct(monitor_series.collector_series)
                monitor_series.collector_series = ExternalCollectorDispatcher.empty_sample().collector_series;
            end

            for i = 1:numel(metric_keys)
                key = metric_keys{i};
                if isfield(monitor_series, key) && isnumeric(monitor_series.(key)) && ~isempty(monitor_series.(key))
                    values = reshape(double(monitor_series.(key)), 1, []);
                    if numel(values) == 1
                        values = [values, values];
                    end
                    monitor_series.(key) = values;
                    continue;
                end
                if ~isfield(sample, 'metrics') || ~isstruct(sample.metrics) || ~isfield(sample.metrics, key)
                    continue;
                end
                value = sample.metrics.(key);
                if ~(isnumeric(value) && isscalar(value) && isfinite(value))
                    continue;
                end
                values = [double(value), double(value)];
                monitor_series.(key) = values;
            end

            if (~isfield(monitor_series, 'collector_metric_catalog') || isempty(monitor_series.collector_metric_catalog)) && ...
                    isstruct(fallback_monitor_series) && isfield(fallback_monitor_series, 'collector_metric_catalog') && ...
                    ~isempty(fallback_monitor_series.collector_metric_catalog)
                monitor_series.collector_metric_catalog = fallback_monitor_series.collector_metric_catalog;
            end
            monitor_series.workflow_kind = ExternalCollectorDispatcher.resolve_summary_workflow_kind(summary_context, monitor_series);
            monitor_series.workflow_phase_id = ExternalCollectorDispatcher.resolve_summary_phase_id(summary_context, monitor_series);
            monitor_series = ExternalCollectorDispatcher.normalize_collector_payload(monitor_series);
        end

        function csv_path = resolve_hwinfo_csv_path(results_struct, paths, fallback_monitor_series)
            csv_path = '';
            candidate_paths = { ...
                ExternalCollectorDispatcher.pick_struct_text( ...
                    ExternalCollectorDispatcher.pick_struct_field(results_struct, 'collector_artifacts'), 'dataset_csv_path', ''), ...
                ExternalCollectorDispatcher.pick_struct_text(paths, 'raw_hwinfo_csv_path', ''), ...
                ExternalCollectorDispatcher.pick_struct_text(ExternalCollectorDispatcher.pick_struct_field(fallback_monitor_series, 'raw_log_paths'), 'hwinfo', '')};
            if isstruct(results_struct)
                last_raw = ExternalCollectorDispatcher.pick_struct_field(ExternalCollectorDispatcher.pick_struct_field(results_struct, 'collector_last_sample'), 'raw_log_paths');
                session_raw = ExternalCollectorDispatcher.pick_struct_field(ExternalCollectorDispatcher.pick_struct_field(results_struct, 'collector_session'), 'raw_log_paths');
                candidate_paths = [{ ...
                    ExternalCollectorDispatcher.pick_struct_text(last_raw, 'hwinfo', ''), ...
                    ExternalCollectorDispatcher.pick_struct_text(session_raw, 'hwinfo', '')}, candidate_paths];
            end
            for i = 1:numel(candidate_paths)
                candidate = char(string(candidate_paths{i}));
                if ~isempty(strtrim(candidate)) && exist(candidate, 'file') == 2
                    csv_path = candidate;
                    return;
                end
            end
        end

        function text = table_last_text(tbl, column_name, fallback)
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

        function posix_values = utc_series_to_posix(values_in)
            posix_values = nan(1, numel(values_in));
            try
                dt = datetime(string(values_in), 'TimeZone', 'UTC', 'InputFormat', 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z''');
            catch
                try
                    dt = datetime(string(values_in), 'TimeZone', 'UTC');
                catch
                    return;
                end
            end
            posix_values = reshape(posixtime(dt), 1, []);
        end

        function workflow_kind = resolve_summary_workflow_kind(summary_context, monitor_series)
            workflow_kind = '';
            candidates = { ...
                ExternalCollectorDispatcher.pick_struct_text(summary_context, 'workflow_kind', ''), ...
                ExternalCollectorDispatcher.pick_struct_text(ExternalCollectorDispatcher.pick_struct_field(summary_context, 'run_config'), 'workflow_kind', ''), ...
                ExternalCollectorDispatcher.pick_struct_text(ExternalCollectorDispatcher.pick_struct_field(summary_context, 'results'), 'workflow_kind', ''), ...
                ExternalCollectorDispatcher.pick_text_field(monitor_series, 'workflow_kind', '')};
            workflow_kind = ExternalCollectorDispatcher.first_nonempty_text(candidates{:});
        end

        function phase_id = resolve_summary_phase_id(summary_context, monitor_series)
            phase_id = '';
            results_struct = ExternalCollectorDispatcher.pick_struct_field(summary_context, 'results');
            run_cfg = ExternalCollectorDispatcher.pick_struct_field(summary_context, 'run_config');
            candidates = { ...
                ExternalCollectorDispatcher.pick_struct_text(summary_context, 'phase_id', ''), ...
                ExternalCollectorDispatcher.pick_struct_text(run_cfg, 'phase_id', ''), ...
                ExternalCollectorDispatcher.pick_struct_text(results_struct, 'phase_id', ''), ...
                ExternalCollectorDispatcher.pick_text_field(monitor_series, 'workflow_phase_id', '')};
            phase_id = ExternalCollectorDispatcher.first_nonempty_text(candidates{:});
        end

        function run_id = resolve_summary_run_id(summary_context, monitor_series)
            run_id = '';
            results_struct = ExternalCollectorDispatcher.pick_struct_field(summary_context, 'results');
            run_cfg = ExternalCollectorDispatcher.pick_struct_field(summary_context, 'run_config');
            candidates = { ...
                ExternalCollectorDispatcher.pick_struct_text(summary_context, 'run_id', ''), ...
                ExternalCollectorDispatcher.pick_struct_text(run_cfg, 'run_id', ''), ...
                ExternalCollectorDispatcher.pick_struct_text(results_struct, 'run_id', ''), ...
                ExternalCollectorDispatcher.resolve_summary_phase_id(summary_context, monitor_series)};
            run_id = ExternalCollectorDispatcher.first_nonempty_text(candidates{:});
        end

        function text = first_nonempty_text(varargin)
            text = '';
            for i = 1:nargin
                candidate = char(string(varargin{i}));
                if ~isempty(strtrim(candidate))
                    text = candidate;
                    return;
                end
            end
        end

        function values = table_text_column(tbl, column_name)
            values = strings(0, 1);
            if isempty(tbl) || ~ismember(column_name, tbl.Properties.VariableNames)
                return;
            end
            raw = tbl.(column_name);
            if isstring(raw)
                values = raw(:);
            elseif iscell(raw)
                values = string(raw(:));
            elseif ischar(raw)
                values = repmat(string(raw), height(tbl), 1);
            else
                values = string(raw(:));
            end
        end

        function text = row_text(tbl, row_idx, column_name)
            text = '';
            values = ExternalCollectorDispatcher.table_text_column(tbl, column_name);
            if isempty(values) || row_idx < 1 || row_idx > numel(values)
                return;
            end
            text = char(strtrim(values(row_idx)));
        end

        function method_family = telemetry_row_method(curated_table, row_idx)
            method_text = ExternalCollectorDispatcher.row_text(curated_table, row_idx, 'stage_method');
            if isempty(method_text)
                method_text = ExternalCollectorDispatcher.row_text(curated_table, row_idx, 'stage_id');
            end
            method_family = ExternalCollectorDispatcher.normalize_method_family(method_text);
        end

        function method_family = normalize_method_family(method_text)
            text = lower(strtrim(char(string(method_text))));
            if contains(text, 'spectral') || strcmp(text, 'sm')
                method_family = 'spectral';
            elseif contains(text, 'fd') || contains(text, 'finite difference')
                method_family = 'fd';
            else
                method_family = 'other';
            end
        end

        function signature = telemetry_mesh_signature(curated_table, row_idx)
            parts = {};
            scenario_id = ExternalCollectorDispatcher.row_text(curated_table, row_idx, 'scenario_id');
            if ~isempty(scenario_id)
                parts{end + 1} = scenario_id; %#ok<AGROW>
            end
            mesh_level = ExternalCollectorDispatcher.row_numeric(curated_table, row_idx, 'mesh_level');
            if isfinite(mesh_level)
                parts{end + 1} = sprintf('level %d', round(mesh_level)); %#ok<AGROW>
            end
            mesh_nx = ExternalCollectorDispatcher.row_numeric(curated_table, row_idx, 'mesh_nx');
            mesh_ny = ExternalCollectorDispatcher.row_numeric(curated_table, row_idx, 'mesh_ny');
            if isfinite(mesh_nx) && isfinite(mesh_ny)
                parts{end + 1} = sprintf('%dx%d', round(mesh_nx), round(mesh_ny)); %#ok<AGROW>
            end
            child_index = ExternalCollectorDispatcher.row_numeric(curated_table, row_idx, 'child_run_index');
            if isfinite(child_index)
                parts{end + 1} = sprintf('child %d', round(child_index)); %#ok<AGROW>
            end
            signature = string(strjoin(parts, ' | '));
        end

        function value = row_numeric(tbl, row_idx, column_name)
            value = NaN;
            if isempty(tbl) || ~ismember(column_name, tbl.Properties.VariableNames)
                return;
            end
            raw = tbl.(column_name);
            if ~isnumeric(raw) || row_idx < 1 || row_idx > numel(raw)
                return;
            end
            raw = double(raw(:));
            value = raw(row_idx);
        end

        function text = collector_status_summary_text(monitor_series)
            status_struct = ExternalCollectorDispatcher.pick_struct_field(monitor_series, 'collector_status');
            hwinfo_status = ExternalCollectorDispatcher.humanize_collector_status( ...
                ExternalCollectorDispatcher.pick_struct_text(status_struct, 'hwinfo', 'disabled'));
            icue_status = ExternalCollectorDispatcher.humanize_collector_status( ...
                ExternalCollectorDispatcher.pick_struct_text(status_struct, 'icue', 'disabled'));
            text = sprintf('HWiNFO=%s | iCUE=%s', hwinfo_status, icue_status);
        end

        function text = preferred_source_summary_text(monitor_series)
            preferred = ExternalCollectorDispatcher.pick_struct_field(monitor_series, 'preferred_source');
            if isempty(fieldnames(preferred))
                text = '--';
                return;
            end
            fields = fieldnames(preferred);
            parts = cell(1, 0);
            for i = 1:numel(fields)
                value = char(string(preferred.(fields{i})));
                if isempty(strtrim(value))
                    continue;
                end
                parts{end + 1} = sprintf('%s=%s', fields{i}, value); %#ok<AGROW>
            end
            if isempty(parts)
                text = '--';
            else
                text = strjoin(parts, ' | ');
            end
        end

        function text = raw_paths_summary_text(monitor_series)
            raw_paths = ExternalCollectorDispatcher.pick_struct_field(monitor_series, 'raw_log_paths');
            if isempty(fieldnames(raw_paths))
                text = '--';
                return;
            end
            text = sprintf('HWiNFO=%s | iCUE=%s', ...
                ExternalCollectorDispatcher.if_empty_text(ExternalCollectorDispatcher.pick_struct_text(raw_paths, 'hwinfo', ''), '--'), ...
                ExternalCollectorDispatcher.if_empty_text(ExternalCollectorDispatcher.pick_struct_text(raw_paths, 'icue', ''), '--'));
        end

        function rows = build_parameter_summary_rows(summary_context)
            rows = repmat(struct( ...
                'stage_label', '', ...
                'problem_setup', '', ...
                'runtime_setup', '', ...
                'frame_setup', ''), 1, 0);
            if ~isstruct(summary_context) || isempty(fieldnames(summary_context))
                return;
            end

            results_struct = ExternalCollectorDispatcher.pick_struct_field(summary_context, 'results');
            workflow_kind = ExternalCollectorDispatcher.resolve_summary_workflow_kind(summary_context, struct());
            parent_parameters = ExternalCollectorDispatcher.pick_struct_field(results_struct, 'parent_parameters');
            if isempty(fieldnames(parent_parameters))
                parent_parameters = ExternalCollectorDispatcher.pick_struct_field(summary_context, 'parameters');
            end
            rows = ExternalCollectorDispatcher.append_parameter_summary_row(rows, 'Top-level workflow parameters', parent_parameters);
            rows = ExternalCollectorDispatcher.append_phase_config_summary_row(rows, 'Phase configuration', ...
                ExternalCollectorDispatcher.pick_struct_field(results_struct, 'phase_config'));

            switch lower(char(string(workflow_kind)))
                case 'phase1_periodic_comparison'
                    children = ExternalCollectorDispatcher.pick_struct_field(results_struct, 'children');
                    rows = ExternalCollectorDispatcher.append_child_parameter_rows(rows, children, 'fd', 'FD');
                    rows = ExternalCollectorDispatcher.append_child_parameter_rows(rows, children, 'spectral', 'Spectral');
                case {'phase2_boundary_condition_study', 'phase3_bathymetry_study'}
                    scenarios = ExternalCollectorDispatcher.pick_struct_value(results_struct, 'scenarios', struct([]));
                    if isstruct(scenarios)
                        for i = 1:numel(scenarios)
                            scenario_label = ExternalCollectorDispatcher.first_nonempty_text( ...
                                ExternalCollectorDispatcher.pick_struct_text(scenarios(i), 'scenario_id', ''), ...
                                ExternalCollectorDispatcher.pick_struct_text(scenarios(i), 'scenario_label', ''), ...
                                sprintf('Scenario %d', i));
                            rows = ExternalCollectorDispatcher.append_scenario_parameter_row(rows, scenarios(i), 'fd', ...
                                sprintf('FD | %s', scenario_label));
                            rows = ExternalCollectorDispatcher.append_scenario_parameter_row(rows, scenarios(i), 'spectral', ...
                                sprintf('Spectral | %s', scenario_label));
                        end
                    end
                otherwise
                    rows = ExternalCollectorDispatcher.append_parameter_summary_row(rows, 'Run parameters', ...
                        ExternalCollectorDispatcher.pick_struct_field(summary_context, 'parameters'));
            end

            rows = rows(~arrayfun(@(r) isempty(strtrim(r.stage_label)), rows));
        end

        function rows = append_child_parameter_rows(rows, children, child_key, label_text)
            if ~isstruct(children) || ~isfield(children, child_key) || ~isstruct(children.(child_key))
                return;
            end
            child = children.(child_key);
            if isfield(child, 'convergence_output') && isstruct(child.convergence_output)
                rows = ExternalCollectorDispatcher.append_parameter_summary_row(rows, ...
                    sprintf('%s Convergence', label_text), ...
                    ExternalCollectorDispatcher.pick_struct_field(child.convergence_output, 'parameters'));
            end
            if isfield(child, 'evolution_output') && isstruct(child.evolution_output)
                rows = ExternalCollectorDispatcher.append_parameter_summary_row(rows, ...
                    sprintf('%s Evolution', label_text), ...
                    ExternalCollectorDispatcher.pick_struct_field(child.evolution_output, 'parameters'));
            end
        end

        function rows = append_scenario_parameter_row(rows, scenario_struct, method_key, label_text)
            if ~isstruct(scenario_struct) || ~isfield(scenario_struct, method_key) || ...
                    ~isstruct(scenario_struct.(method_key))
                return;
            end
            method_struct = scenario_struct.(method_key);
            if isfield(method_struct, 'output') && isstruct(method_struct.output)
                rows = ExternalCollectorDispatcher.append_parameter_summary_row(rows, label_text, ...
                    ExternalCollectorDispatcher.pick_struct_field(method_struct.output, 'parameters'));
            end
        end

        function rows = append_phase_config_summary_row(rows, label_text, phase_cfg)
            if ~isstruct(phase_cfg) || isempty(fieldnames(phase_cfg))
                return;
            end
            problem_parts = {};
            runtime_parts = {};
            bc_case = ExternalCollectorDispatcher.pick_struct_text(phase_cfg, 'force_bc_case', '');
            bathy = ExternalCollectorDispatcher.pick_struct_text(phase_cfg, 'force_bathymetry', '');
            if ~isempty(strtrim(bc_case))
                problem_parts{end + 1} = sprintf('BC=%s', bc_case); %#ok<AGROW>
            end
            if ~isempty(strtrim(bathy))
                problem_parts{end + 1} = sprintf('Bathy=%s', bathy); %#ok<AGROW>
            end
            tol = ExternalCollectorDispatcher.pick_struct_number(phase_cfg, 'convergence_tolerance', NaN);
            if isfinite(tol)
                runtime_parts{end + 1} = sprintf('tol=%s', ExternalCollectorDispatcher.numeric_or_dash(tol, '%.4g')); %#ok<AGROW>
            end
            mesh_levels = ExternalCollectorDispatcher.pick_struct_number(phase_cfg, 'mesh_level_count', NaN);
            if isfinite(mesh_levels)
                runtime_parts{end + 1} = sprintf('levels=%d', round(mesh_levels)); %#ok<AGROW>
            end
            rows(end + 1) = struct( ... %#ok<AGROW>
                'stage_label', label_text, ...
                'problem_setup', strjoin(problem_parts, ' | '), ...
                'runtime_setup', strjoin(runtime_parts, ' | '), ...
                'frame_setup', '');
        end

        function rows = append_parameter_summary_row(rows, stage_label, params)
            if ~isstruct(params) || isempty(fieldnames(params))
                return;
            end
            bc_case = ExternalCollectorDispatcher.first_nonempty_text( ...
                ExternalCollectorDispatcher.pick_struct_text(params, 'boundary_condition_case', ''), ...
                ExternalCollectorDispatcher.pick_struct_text(params, 'bc_case', ''), ...
                ExternalCollectorDispatcher.pick_struct_text(params, 'force_bc_case', ''));
            bathy = ExternalCollectorDispatcher.first_nonempty_text( ...
                ExternalCollectorDispatcher.pick_struct_text(params, 'bathymetry_scenario', ''), ...
                ExternalCollectorDispatcher.pick_struct_text(params, 'force_bathymetry', ''));
            ic_type = ExternalCollectorDispatcher.pick_struct_text(params, 'ic_type', '');
            mesh_nx = ExternalCollectorDispatcher.pick_struct_number(params, 'Nx', NaN);
            mesh_ny = ExternalCollectorDispatcher.pick_struct_number(params, 'Ny', NaN);
            dt = ExternalCollectorDispatcher.pick_struct_number(params, 'dt', NaN);
            tfinal = ExternalCollectorDispatcher.pick_struct_number(params, 'Tfinal', NaN);
            plot_frames = ExternalCollectorDispatcher.pick_struct_number(params, 'num_plot_snapshots', NaN);
            animation_frames = ExternalCollectorDispatcher.pick_struct_number(params, 'animation_num_frames', NaN);
            if ~isfinite(animation_frames)
                animation_frames = ExternalCollectorDispatcher.pick_struct_number(params, 'num_animation_frames', NaN);
            end
            runtime_frames = ExternalCollectorDispatcher.pick_struct_number(params, 'num_snapshots', NaN);

            problem_parts = {};
            runtime_parts = {};
            frame_parts = {};
            if ~isempty(strtrim(ic_type))
                problem_parts{end + 1} = sprintf('IC=%s', ic_type); %#ok<AGROW>
            end
            if ~isempty(strtrim(bc_case))
                problem_parts{end + 1} = sprintf('BC=%s', bc_case); %#ok<AGROW>
            end
            if ~isempty(strtrim(bathy))
                problem_parts{end + 1} = sprintf('Bathy=%s', bathy); %#ok<AGROW>
            end
            if isfinite(mesh_nx) && isfinite(mesh_ny)
                runtime_parts{end + 1} = sprintf('mesh=%dx%d', round(mesh_nx), round(mesh_ny)); %#ok<AGROW>
            end
            if isfinite(dt)
                runtime_parts{end + 1} = sprintf('dt=%s', ExternalCollectorDispatcher.numeric_or_dash(dt, '%.4g')); %#ok<AGROW>
            end
            if isfinite(tfinal)
                runtime_parts{end + 1} = sprintf('T=%s', ExternalCollectorDispatcher.numeric_or_dash(tfinal, '%.4g')); %#ok<AGROW>
            end
            if isfinite(plot_frames)
                frame_parts{end + 1} = sprintf('plots=%d', round(plot_frames)); %#ok<AGROW>
            end
            if isfinite(animation_frames)
                frame_parts{end + 1} = sprintf('anim=%d', round(animation_frames)); %#ok<AGROW>
            end
            if isfinite(runtime_frames)
                frame_parts{end + 1} = sprintf('runtime=%d', round(runtime_frames)); %#ok<AGROW>
            end

            rows(end + 1) = struct( ... %#ok<AGROW>
                'stage_label', char(string(stage_label)), ...
                'problem_setup', strjoin(problem_parts, ' | '), ...
                'runtime_setup', strjoin(runtime_parts, ' | '), ...
                'frame_setup', strjoin(frame_parts, ' | '));
        end

        function value = numeric_or_dash(number_value, format_spec)
            if nargin < 2 || isempty(format_spec)
                format_spec = '%.4g';
            end
            if isnumeric(number_value) && isscalar(number_value) && isfinite(number_value)
                value = sprintf(format_spec, double(number_value));
            else
                value = '--';
            end
        end

        function value = pick_struct_value(item, field_name, fallback)
            value = fallback;
            if isstruct(item) && isfield(item, field_name)
                value = item.(field_name);
            end
        end

        function close_excel(excel)
            try
                excel.Quit();
            catch
            end
            try
                delete(excel);
            catch
            end
        end

        function name = excel_column_name(index)
            name = '';
            idx = max(1, round(double(index)));
            while idx > 0
                rem_idx = mod(idx - 1, 26);
                name = [char(65 + rem_idx), name]; %#ok<AGROW>
                idx = floor((idx - 1) / 26);
            end
        end

        function value = stage_wall_time(block)
            value = NaN;
            elapsed = ExternalCollectorDispatcher.table_column(block, 'elapsed_wall_time');
            if ~isempty(elapsed)
                finite_vals = elapsed(isfinite(elapsed));
                if ~isempty(finite_vals)
                    value = finite_vals(end) - finite_vals(1);
                    if value < 0
                        value = finite_vals(end);
                    end
                end
            end
            if ~(isfinite(value) && value > 0)
                meta_vals = ExternalCollectorDispatcher.table_column(block, 'stage_wall_time_meta');
                meta_vals = meta_vals(isfinite(meta_vals) & meta_vals > 0);
                if ~isempty(meta_vals)
                    value = meta_vals(end);
                end
            end
        end

        function env_model = resolve_environmental_model(varargin)
            env_model = ExternalCollectorDispatcher.default_environmental_model();
            for i = 1:nargin
                candidate = varargin{i};
                if ~isstruct(candidate)
                    continue;
                end
                if isfield(candidate, 'sustainability') && isstruct(candidate.sustainability)
                    candidate = candidate.sustainability;
                end
                if isfield(candidate, 'environmental_model') && isstruct(candidate.environmental_model)
                    env_in = candidate.environmental_model;
                else
                    env_in = candidate;
                end
                candidate_fields = fieldnames(env_in);
                for fi = 1:numel(candidate_fields)
                    field_name = candidate_fields{fi};
                    env_model.(field_name) = env_in.(field_name);
                end
            end
            factor_g_per_kwh = ExternalCollectorDispatcher.pick_struct_number(env_model, ...
                'grid_carbon_intensity_gco2e_per_kwh', NaN);
            if ~(isfinite(factor_g_per_kwh) && factor_g_per_kwh >= 0)
                factor_kg_per_kwh = ExternalCollectorDispatcher.pick_struct_number(env_model, ...
                    'grid_carbon_intensity_kgco2e_per_kwh', NaN);
                if isfinite(factor_kg_per_kwh) && factor_kg_per_kwh >= 0
                    factor_g_per_kwh = 1000 * factor_kg_per_kwh;
                end
            end
            if ~(isfinite(factor_g_per_kwh) && factor_g_per_kwh >= 0)
                factor_g_per_kwh = 1000 * ExternalCollectorDispatcher.pick_struct_number( ...
                    ExternalCollectorDispatcher.default_environmental_model(), ...
                    'grid_carbon_intensity_kgco2e_per_kwh', 0.19553);
            end
            env_model.grid_carbon_intensity_gco2e_per_kwh = factor_g_per_kwh;
            env_model.grid_carbon_intensity_kgco2e_per_kwh = factor_g_per_kwh / 1000;
        end

        function env_model = default_environmental_model()
            env_model = struct( ...
                'electricity_scope', 'consumed_location_based', ...
                'grid_carbon_intensity_kgco2e_per_kwh', 0.19553, ...
                'grid_carbon_intensity_gco2e_per_kwh', 195.53, ...
                'factor_year', 2025, ...
                'factor_region', 'UK', ...
                'factor_source_tag', 'uk_gov_2025_consumed_2023data', ...
                'factor_source_url', 'https://www.gov.uk/government/publications/greenhouse-gas-reporting-conversion-factors-2025', ...
                'line_losses_included', true, ...
                'allow_component_power_sum_fallback', true, ...
                'detect_apu_power_duplication', true);
        end

        function factor = environmental_factor_g_per_wh(env_model)
            env_model = ExternalCollectorDispatcher.resolve_environmental_model(env_model);
            factor = ExternalCollectorDispatcher.pick_struct_number(env_model, ...
                'grid_carbon_intensity_gco2e_per_kwh', NaN) / 1000;
            if ~(isfinite(factor) && factor >= 0)
                factor = 0.19553;
            end
        end

        function power_vec = resolve_environmental_power_vector(table_or_series, env_model)
            if nargin < 2
                env_model = struct();
            end
            env_model = ExternalCollectorDispatcher.resolve_environmental_model(env_model);
            power_vec = zeros(0, 1);
            if istable(table_or_series)
                system_power = ExternalCollectorDispatcher.table_column(table_or_series, 'system_power_w');
                if any(isfinite(system_power))
                    power_vec = system_power;
                    return;
                end
                fallback_power = ExternalCollectorDispatcher.table_column(table_or_series, 'power_w');
                if any(isfinite(fallback_power))
                    power_vec = fallback_power;
                    return;
                end
                if ~ExternalCollectorDispatcher.pick_struct_value(env_model, 'allow_component_power_sum_fallback', true)
                    power_vec = system_power;
                    return;
                end
                cpu_power = ExternalCollectorDispatcher.table_column(table_or_series, 'cpu_power_w_hwinfo');
                gpu_power = ExternalCollectorDispatcher.table_column(table_or_series, 'gpu_power_w_hwinfo');
                memory_power = ExternalCollectorDispatcher.table_column(table_or_series, 'memory_power_w_or_proxy');
                power_vec = ExternalCollectorDispatcher.combine_component_power_vectors(cpu_power, gpu_power, memory_power, env_model);
                return;
            end
            if isstruct(table_or_series)
                system_power = ExternalCollectorDispatcher.series_column(table_or_series, 'system_power_w', []);
                if any(isfinite(system_power))
                    power_vec = reshape(double(system_power(:)), [], 1);
                    return;
                end
                fallback_power = ExternalCollectorDispatcher.series_column(table_or_series, 'power_w', []);
                if any(isfinite(fallback_power))
                    power_vec = reshape(double(fallback_power(:)), [], 1);
                    return;
                end
                if ~ExternalCollectorDispatcher.pick_struct_value(env_model, 'allow_component_power_sum_fallback', true)
                    power_vec = reshape(double(system_power(:)), [], 1);
                    return;
                end
                cpu_power = reshape(double(ExternalCollectorDispatcher.series_column(table_or_series, 'cpu_power_w_hwinfo', [])), [], 1);
                gpu_power = reshape(double(ExternalCollectorDispatcher.series_column(table_or_series, 'gpu_power_w_hwinfo', [])), [], 1);
                memory_power = reshape(double(ExternalCollectorDispatcher.series_column(table_or_series, 'memory_power_w_or_proxy', [])), [], 1);
                power_vec = ExternalCollectorDispatcher.combine_component_power_vectors(cpu_power, gpu_power, memory_power, env_model);
            end
        end

        function power_vec = combine_component_power_vectors(cpu_power, gpu_power, memory_power, env_model)
            lengths = [numel(cpu_power), numel(gpu_power), numel(memory_power)];
            n = max([lengths, 0]);
            if n < 1
                power_vec = zeros(0, 1);
                return;
            end
            cpu_power = ExternalCollectorDispatcher.resize_numeric_vector(cpu_power, n);
            gpu_power = ExternalCollectorDispatcher.resize_numeric_vector(gpu_power, n);
            memory_power = ExternalCollectorDispatcher.resize_numeric_vector(memory_power, n);
            cpu_power(~isfinite(cpu_power)) = 0;
            gpu_power(~isfinite(gpu_power)) = 0;
            memory_power(~isfinite(memory_power)) = 0;
            power_vec = cpu_power + gpu_power + memory_power;
            if ExternalCollectorDispatcher.pick_struct_value(env_model, 'detect_apu_power_duplication', true)
                zero_mask = power_vec <= 0;
                power_vec(zero_mask) = nan;
            end
        end

        function table_out = ensure_environmental_impact_columns(table_out, monitor_series)
            if nargin < 1 || isempty(table_out) || ~istable(table_out)
                return;
            end
            if nargin < 2
                monitor_series = struct();
            end
            required_cols = {'environmental_energy_wh_cum', 'environmental_co2_g_cum'};
            if ~all(ismember(required_cols, table_out.Properties.VariableNames))
                return;
            end
            energy_vals = ExternalCollectorDispatcher.table_column(table_out, 'environmental_energy_wh_cum');
            co2_vals = ExternalCollectorDispatcher.table_column(table_out, 'environmental_co2_g_cum');
            if any(isfinite(energy_vals)) && any(isfinite(co2_vals))
                return;
            end

            time_s = ExternalCollectorDispatcher.table_column(table_out, 'elapsed_wall_time');
            if isempty(time_s)
                time_s = ExternalCollectorDispatcher.table_column(table_out, 't');
            end
            if isempty(time_s)
                return;
            end

            env_model = ExternalCollectorDispatcher.resolve_environmental_model(monitor_series, table_out);
            source_power = ExternalCollectorDispatcher.resolve_environmental_power_vector(table_out, env_model);
            if ~any(isfinite(source_power))
                source_power = ExternalCollectorDispatcher.resolve_environmental_power_vector(monitor_series, env_model);
            end

            n = min(numel(time_s), numel(source_power));
            if n <= 0
                return;
            end

            time_s = reshape(double(time_s(1:n)), 1, []);
            source_power = reshape(double(source_power(1:n)), 1, []);
            energy_wh = zeros(1, n);
            for i = 2:n
                dt_h = max(time_s(i) - time_s(i - 1), 0) / 3600;
                p_prev = max(source_power(i - 1), 0);
                p_now = max(source_power(i), 0);
                if ~(isfinite(dt_h) && isfinite(p_prev) && isfinite(p_now))
                    energy_wh(i) = energy_wh(i - 1);
                    continue;
                end
                energy_wh(i) = energy_wh(i - 1) + 0.5 * (p_prev + p_now) * dt_h;
            end
            co2_g = ExternalCollectorDispatcher.environmental_factor_g_per_wh(env_model) * energy_wh;

            energy_col = nan(height(table_out), 1);
            co2_col = nan(height(table_out), 1);
            energy_col(1:n) = reshape(energy_wh, [], 1);
            co2_col(1:n) = reshape(co2_g, [], 1);

            if ~any(isfinite(energy_vals))
                table_out.environmental_energy_wh_cum = energy_col;
            end
            if ~any(isfinite(co2_vals))
                table_out.environmental_co2_g_cum = co2_col;
            end
        end

        function vec = resize_numeric_vector(vec, n)
            vec = reshape(double(vec), [], 1);
            if nargin < 2 || ~isfinite(n) || n < 1
                vec = zeros(0, 1);
                return;
            end
            if isempty(vec)
                vec = nan(n, 1);
                return;
            end
            if numel(vec) == n
                return;
            end
            resized = nan(n, 1);
            m = min(n, numel(vec));
            resized(1:m) = vec(1:m);
            if m < n
                resized(m + 1:end) = vec(m);
            end
            vec = resized;
        end

        function value = stage_terminal_value(block, column_name)
            value = NaN;
            vec = ExternalCollectorDispatcher.table_column(block, column_name);
            finite_vals = vec(isfinite(vec));
            if isempty(finite_vals)
                return;
            end
            value = finite_vals(end);
        end

        function value = first_text_value(block, column_name)
            value = '';
            if isempty(block) || ~ismember(column_name, block.Properties.VariableNames)
                return;
            end
            col = string(block.(column_name));
            idx = find(strlength(strtrim(col)) > 0, 1, 'first');
            if ~isempty(idx)
                value = char(col(idx));
            end
        end

        function vec = table_column(block, column_name)
            vec = zeros(0, 1);
            if isempty(block) || ~ismember(column_name, block.Properties.VariableNames)
                return;
            end
            values = block.(column_name);
            if isnumeric(values)
                vec = double(values(:));
            end
        end

        function vec = table_sum_columns(block, column_names)
            vec = zeros(height(block), 1);
            has_any = false;
            for i = 1:numel(column_names)
                if ismember(column_names{i}, block.Properties.VariableNames)
                    values = block.(column_names{i});
                    if isnumeric(values)
                        vec = vec + double(values(:));
                        has_any = true;
                    end
                end
            end
            if ~has_any
                vec = nan(height(block), 1);
            end
        end

        function vec = table_column_or_fallback(block, column_name, fallback)
            vec = ExternalCollectorDispatcher.table_column(block, column_name);
            if isempty(vec) || ~any(isfinite(vec))
                vec = fallback;
            end
        end

        function value = nanmean_safe(vec)
            value = NaN;
            finite_vals = vec(isfinite(vec));
            if ~isempty(finite_vals)
                value = mean(finite_vals);
            end
        end

        function value = nanmax_safe(vec)
            value = NaN;
            finite_vals = vec(isfinite(vec));
            if ~isempty(finite_vals)
                value = max(finite_vals);
            end
        end

        function value = nanmax_or_fallback(primary_vec, fallback_vec)
            value = ExternalCollectorDispatcher.nanmax_safe(primary_vec);
            if ~isfinite(value)
                value = ExternalCollectorDispatcher.nanmax_safe(fallback_vec);
            end
        end

        function value = monitor_series_terminal_value(monitor_series, field_name)
            value = NaN;
            vec = ExternalCollectorDispatcher.series_column(monitor_series, field_name, []);
            finite_vals = vec(isfinite(vec));
            if ~isempty(finite_vals)
                value = finite_vals(end);
            end
        end

        function value = monitor_series_wall_time(monitor_series)
            value = NaN;
            elapsed = ExternalCollectorDispatcher.series_column(monitor_series, 'elapsed_wall_time', []);
            finite_vals = elapsed(isfinite(elapsed));
            if ~isempty(finite_vals)
                value = finite_vals(end) - finite_vals(1);
                if value < 0
                    value = finite_vals(end);
                end
                return;
            end
            value = ExternalCollectorDispatcher.pick_struct_number(monitor_series, 'wall_time', NaN);
        end

        function rows = build_subphase_breakdown_rows(summary_context)
            rows = repmat(struct( ...
                'stage_id', '', ...
                'label_and_method', '', ...
                'wall_time_s', NaN, ...
                'energy_and_co2', '--'), 1, 0);
            if ~isstruct(summary_context) || isempty(fieldnames(summary_context))
                return;
            end
            results_struct = ExternalCollectorDispatcher.pick_struct_field(summary_context, 'results');
            workflow_kind = ExternalCollectorDispatcher.resolve_summary_workflow_kind(summary_context, struct());
            switch lower(char(string(workflow_kind)))
                case 'phase1_periodic_comparison'
                    children = ExternalCollectorDispatcher.pick_struct_field(results_struct, 'children');
                    rows = ExternalCollectorDispatcher.append_subphase_breakdown_row(rows, ...
                        'fd_convergence', 'FD Convergence | FD', ...
                        ExternalCollectorDispatcher.pick_struct_number( ...
                            ExternalCollectorDispatcher.pick_struct_field( ...
                                ExternalCollectorDispatcher.pick_struct_field(children, 'fd'), 'convergence_output'), ...
                            'wall_time', NaN));
                    rows = ExternalCollectorDispatcher.append_subphase_breakdown_row(rows, ...
                        'fd_baseline_selected', 'FD Baseline | selected convergence level', ...
                        ExternalCollectorDispatcher.pick_struct_number( ...
                            ExternalCollectorDispatcher.pick_struct_field( ...
                                ExternalCollectorDispatcher.pick_struct_field(children, 'fd'), 'metrics'), ...
                            'runtime_wall_s', NaN));
                    rows = ExternalCollectorDispatcher.append_subphase_breakdown_row(rows, ...
                        'spectral_convergence', 'Spectral Convergence | Spectral', ...
                        ExternalCollectorDispatcher.pick_struct_number( ...
                            ExternalCollectorDispatcher.pick_struct_field( ...
                                ExternalCollectorDispatcher.pick_struct_field(children, 'spectral'), 'convergence_output'), ...
                            'wall_time', NaN));
                    rows = ExternalCollectorDispatcher.append_subphase_breakdown_row(rows, ...
                        'spectral_baseline_selected', 'Spectral Baseline | selected convergence level', ...
                        ExternalCollectorDispatcher.pick_struct_number( ...
                            ExternalCollectorDispatcher.pick_struct_field( ...
                                ExternalCollectorDispatcher.pick_struct_field(children, 'spectral'), 'metrics'), ...
                            'runtime_wall_s', NaN));
                otherwise
                    parameter_rows = ExternalCollectorDispatcher.build_parameter_summary_rows(summary_context);
                    for i = 1:numel(parameter_rows)
                        rows = ExternalCollectorDispatcher.append_subphase_breakdown_row(rows, ...
                            matlab.lang.makeValidName(parameter_rows(i).stage_label), ...
                            parameter_rows(i).stage_label, NaN);
                    end
            end
        end

        function rows = append_subphase_breakdown_row(rows, stage_id, label_text, wall_time_s)
            rows(end + 1) = struct( ... %#ok<AGROW>
                'stage_id', char(string(stage_id)), ...
                'label_and_method', char(string(label_text)), ...
                'wall_time_s', wall_time_s, ...
                'energy_and_co2', '--');
        end

        function failures = record_artifact_failure(failures, artifact_name, ME)
            if nargin < 1 || ~isstruct(failures)
                failures = repmat(struct('artifact', '', 'identifier', '', 'message', ''), 1, 0);
            end
            if nargin < 3 || ~isa(ME, 'MException')
                ME = MException('ExternalCollectorDispatcher:UnknownArtifactFailure', ...
                    'Unknown collector artifact failure.');
            end
            failures(end + 1) = struct( ... %#ok<AGROW>
                'artifact', char(string(artifact_name)), ...
                'identifier', char(string(ME.identifier)), ...
                'message', char(string(ME.message)));
        end

        function table_out = safe_table(value)
            if nargin < 1 || ~istable(value)
                table_out = table();
                return;
            end
            table_out = value;
        end

        function specs = curated_metric_specs()
            specs = [ ...
                ExternalCollectorDispatcher.make_curated_metric_spec('normalized__system_power_w', 'System Power', 'system_power_w', 'W', 'normalized', 'Power Overlay', 'System Power (W)', 'Estimated total system power draw.'), ...
                ExternalCollectorDispatcher.make_curated_metric_spec('normalized__cpu_proxy', 'CPU Load', 'cpu_proxy', '%', 'normalized', 'CPU Usage Overlay', 'CPU Load (%)', 'Representative CPU load used in the monitor overlay.'), ...
                ExternalCollectorDispatcher.make_curated_metric_spec('normalized__gpu_series', 'GPU Load', 'gpu_series', '%', 'normalized', 'GPU Usage Overlay', 'GPU Load (%)', 'Representative GPU load used in the monitor overlay.'), ...
                ExternalCollectorDispatcher.make_curated_metric_spec('normalized__memory_series', 'Memory Usage', 'memory_series', '%', 'normalized', 'Memory Usage', 'Memory Usage (%)', 'Representative memory load/proxy.'), ...
                ExternalCollectorDispatcher.make_curated_metric_spec('normalized__cpu_temp_c', 'CPU Temperature', 'cpu_temp_c', 'C', 'normalized', 'CPU Package Temperature', 'CPU Temperature (C)', 'CPU package temperature.'), ...
                ExternalCollectorDispatcher.make_curated_metric_spec('normalized__coolant_temp_c', 'Coolant Temperature', 'coolant_temp_c', 'C', 'normalized', 'Coolant Temperature', 'Coolant Temperature (C)', 'Coolant temperature when available.'), ...
                ExternalCollectorDispatcher.make_curated_metric_spec('normalized__fan_rpm', 'Fan Speed', 'fan_rpm', 'RPM', 'normalized', 'Cooling RPM', 'Fan Speed (RPM)', 'Cooling fan speed.'), ...
                ExternalCollectorDispatcher.make_curated_metric_spec('normalized__pump_rpm', 'Pump Speed', 'pump_rpm', 'RPM', 'normalized', 'Cooling RPM', 'Pump Speed (RPM)', 'Cooling pump speed.'), ...
                ExternalCollectorDispatcher.make_curated_metric_spec('normalized__cpu_voltage_v', 'CPU Voltage', 'cpu_voltage_v', 'V', 'normalized', 'CPU Voltage', 'CPU Voltage (V)', 'CPU package voltage.'), ...
                ExternalCollectorDispatcher.make_curated_metric_spec('normalized__gpu_voltage_v', 'GPU Voltage', 'gpu_voltage_v', 'V', 'normalized', 'GPU Voltage', 'GPU Voltage (V)', 'GPU voltage.'), ...
                ExternalCollectorDispatcher.make_curated_metric_spec('normalized__memory_voltage_v', 'Memory Voltage', 'memory_voltage_v', 'V', 'normalized', 'Memory Voltage', 'Memory Voltage (V)', 'Memory voltage or proxy.'), ...
                ExternalCollectorDispatcher.make_curated_metric_spec('normalized__cpu_power_w_hwinfo', 'CPU Power', 'cpu_power_w_hwinfo', 'W', 'normalized', 'CPU Power', 'CPU Power (W)', 'CPU power draw from HWiNFO when available.'), ...
                ExternalCollectorDispatcher.make_curated_metric_spec('normalized__gpu_power_w_hwinfo', 'GPU Power', 'gpu_power_w_hwinfo', 'W', 'normalized', 'GPU Power', 'GPU Power (W)', 'GPU power draw from HWiNFO when available.'), ...
                ExternalCollectorDispatcher.make_curated_metric_spec('normalized__memory_power_w_or_proxy', 'Memory Power / Proxy', 'memory_power_w_or_proxy', 'W', 'normalized', 'Memory Power', 'Memory Power / Proxy (W)', 'Memory power or derived proxy.'), ...
                ExternalCollectorDispatcher.make_curated_metric_spec('normalized__environmental_energy_wh_cum', 'Cumulative Energy', 'environmental_energy_wh_cum', 'Wh', 'normalized', 'Cumulative Energy', 'Energy (Wh)', 'Cumulative energy estimate.'), ...
                ExternalCollectorDispatcher.make_curated_metric_spec('normalized__environmental_co2_g_cum', 'Cumulative CO2e', 'environmental_co2_g_cum', 'g', 'normalized', 'Cumulative CO2e', 'CO2e (g)', 'Cumulative location-based CO2e estimate derived from electricity consumption.') ...
            ];
        end

        function spec = make_curated_metric_spec(id, display_name, table_column, unit, source, default_title, default_ylabel, meaning)
            spec = struct( ...
                'id', id, ...
                'display_name', display_name, ...
                'metric_key', table_column, ...
                'table_column', table_column, ...
                'unit', unit, ...
                'source', source, ...
                'default_title', default_title, ...
                'default_ylabel', default_ylabel, ...
                'origin', 'curated_dataset', ...
                'meaning', meaning);
        end

        function token = sanitize_run_id(run_id)
            token = char(string(run_id));
            token = regexprep(token, '[^A-Za-z0-9_\-]+', '_');
            token = regexprep(token, '_+', '_');
            token = strtrim(token);
        end

        function out = pick_struct_field(s, field_name)
            out = struct();
            if isstruct(s) && isfield(s, field_name) && isstruct(s.(field_name))
                out = s.(field_name);
            end
        end

        function out = pick_text_field(s, field_name, fallback)
            if nargin < 3
                fallback = '--';
            end
            out = fallback;
            if isstruct(s) && isfield(s, field_name) && ~isempty(s.(field_name))
                out = char(string(s.(field_name)));
            end
            if isempty(strtrim(out))
                out = fallback;
            end
        end

        function out = if_empty_text(value, fallback)
            out = char(string(value));
            if nargin < 2
                fallback = '--';
            end
            if isempty(strtrim(out))
                out = char(string(fallback));
            end
        end

        function tf = pick_nested_logical(s, field_name, nested_name)
            tf = false;
            if isstruct(s) && isfield(s, field_name) && isstruct(s.(field_name)) && ...
                    isfield(s.(field_name), nested_name)
                candidate = s.(field_name).(nested_name);
                if islogical(candidate) && isscalar(candidate)
                    tf = candidate;
                elseif isnumeric(candidate) && isscalar(candidate)
                    tf = logical(candidate);
                end
            end
        end

        function domains = pick_domains(coverage_domains, source)
            domains = {};
            if isstruct(coverage_domains) && isfield(coverage_domains, source)
                domains = ExternalCollectorDispatcher.normalize_cellstr(coverage_domains.(source));
            end
        end

        function cells = normalize_cellstr(value)
            if isempty(value)
                cells = {};
                return;
            end
            if iscell(value)
                cells = cell(1, 0);
                for i = 1:numel(value)
                    text = ExternalCollectorDispatcher.safe_text_value(value{i}, '');
                    if ~isempty(strtrim(text))
                        cells{end + 1} = text; %#ok<AGROW>
                    end
                end
                return;
            end
            if isstring(value)
                cells = cellstr(value(:).');
                return;
            end
            text = ExternalCollectorDispatcher.safe_text_value(value, '');
            if isempty(strtrim(text))
                cells = {};
            else
                cells = {text};
            end
        end

        function text = join_cellstr(value)
            cells = ExternalCollectorDispatcher.normalize_cellstr(value);
            if isempty(cells)
                text = '--';
            else
                text = strjoin(cells, ', ');
            end
        end

        function text = humanize_metric_name(value)
            text = ExternalCollectorDispatcher.safe_text_value(value, '');
            text = strrep(text, '_', ' ');
            parts = strsplit(lower(strtrim(text)), ' ');
            for i = 1:numel(parts)
                if isempty(parts{i})
                    continue;
                end
                parts{i}(1) = upper(parts{i}(1));
            end
            text = strjoin(parts, ' ');
        end

        function text = humanize_collector_status(value)
            switch lower(strtrim(char(string(value))))
                case 'shared_memory_connected'
                    text = 'shared memory connected';
                case 'shared_memory_incomplete'
                    text = 'shared memory missing data';
                case 'csv_fallback'
                    text = 'csv fallback active';
                case 'csv_target_mismatch'
                    text = 'csv target mismatch';
                case 'shared_memory_disabled'
                    text = 'shared memory disabled';
                case 'shared_memory_expired'
                    text = 'shared memory expired';
                case 'csv_missing'
                    text = 'csv logging missing';
                case 'parse_error'
                    text = 'parse error';
                case 'not_found'
                    text = 'not found';
                case 'disabled'
                    text = 'off';
                otherwise
                    text = char(string(value));
            end
        end

        function text = humanize_transport(value)
            switch lower(strtrim(char(string(value))))
                case 'shared_memory'
                    text = 'shared memory';
                case 'csv'
                    text = 'csv fallback';
                otherwise
                    text = 'none';
            end
        end

        function text = yes_no_text(value)
            if islogical(value) || (isnumeric(value) && isscalar(value))
                if logical(value)
                    text = 'Yes';
                else
                    text = 'No';
                end
            else
                text = '--';
            end
        end

        function unit = metric_unit(metric_key)
            switch lower(char(string(metric_key)))
                case {'cpu_proxy', 'gpu_series', 'memory_series', 'device_battery_level'}
                    unit = '%';
                case {'cpu_voltage_v', 'gpu_voltage_v', 'memory_voltage_v'}
                    unit = 'V';
                case 'cpu_temp_c'
                    unit = 'C';
                case {'power_w', 'cpu_power_w_hwinfo', 'gpu_power_w_hwinfo', 'system_power_w'}
                    unit = 'W';
                case {'fan_rpm', 'pump_rpm'}
                    unit = 'RPM';
                case 'coolant_temp_c'
                    unit = 'C';
                case {'environmental_energy_wh_cum'}
                    unit = 'Wh';
                case {'environmental_co2_g_cum'}
                    unit = 'g';
                otherwise
                    unit = '';
            end
        end

        function ylabel = metric_ylabel(base_name, unit)
            ylabel = char(string(base_name));
            if nargin >= 2 && ~isempty(unit)
                ylabel = sprintf('%s (%s)', ylabel, char(string(unit)));
            end
        end

        function value = pick_struct_text(item, field_name, fallback)
            value = fallback;
            if isstruct(item) && isfield(item, field_name) && ~isempty(item.(field_name))
                value = item.(field_name);
            end
        end

        function value = pick_struct_number(item, field_name, fallback)
            value = fallback;
            if isstruct(item) && isfield(item, field_name) && isnumeric(item.(field_name)) && ...
                    isscalar(item.(field_name)) && isfinite(item.(field_name))
                value = double(item.(field_name));
            end
        end

        function value = sanitize_text_scalar(item, field_name, fallback)
            value = char(string(fallback));
            if ~(isstruct(item) && isfield(item, field_name))
                return;
            end
            candidate = item.(field_name);
            if isempty(candidate)
                return;
            end
            try
                if ischar(candidate)
                    text = candidate;
                elseif isstring(candidate)
                    text = char(join(candidate(:).', '|'));
                elseif isa(candidate, 'datetime') || isa(candidate, 'duration') || isa(candidate, 'calendarDuration')
                    text = char(join(string(candidate(:).'), '|'));
                else
                    text = char(string(candidate));
                end
            catch
                text = char(string(fallback));
            end
            text = strtrim(text);
            if ~isempty(text)
                value = text;
            end
        end

        function value = sanitize_logical_scalar(item, field_name, fallback)
            value = logical(fallback);
            if ~(isstruct(item) && isfield(item, field_name))
                return;
            end
            candidate = item.(field_name);
            if (islogical(candidate) || isnumeric(candidate)) && isscalar(candidate)
                value = logical(candidate);
                return;
            end
            try
                text = lower(strtrim(char(string(candidate))));
            catch
                text = '';
            end
            if any(strcmp(text, {'true', 'on', 'yes', 'enabled', '1'}))
                value = true;
            elseif any(strcmp(text, {'false', 'off', 'no', 'disabled', '0'}))
                value = false;
            end
        end

        function value = sanitize_numeric_scalar(item, field_name, fallback)
            value = double(fallback);
            if ~(isstruct(item) && isfield(item, field_name))
                return;
            end
            candidate = item.(field_name);
            if isnumeric(candidate) && isscalar(candidate) && isfinite(candidate)
                value = double(candidate);
                return;
            end
            try
                numeric_value = double(candidate);
            catch
                numeric_value = NaN;
            end
            if isscalar(numeric_value) && isfinite(numeric_value)
                value = numeric_value;
            end
        end

        function text = safe_text_value(value, fallback)
            if nargin < 2
                fallback = '';
            end
            text = char(string(fallback));
            if nargin < 1 || isempty(value)
                return;
            end

            try
                if ischar(value)
                    candidate = value;
                elseif isstring(value)
                    candidate = char(join(value(:).', '|'));
                elseif isnumeric(value) || islogical(value)
                    if isscalar(value)
                        candidate = char(string(value));
                    else
                        candidate = sprintf('[%s %s]', class(value), mat2str(size(value)));
                    end
                elseif isstruct(value)
                    if isscalar(value)
                        field_names = fieldnames(value);
                        preview = strjoin(field_names(1:min(numel(field_names), 3)), ',');
                        if numel(field_names) > 3
                            preview = sprintf('%s,+%d', preview, numel(field_names) - 3);
                        end
                        candidate = sprintf('{%s}', preview);
                    else
                        candidate = sprintf('[struct %s]', mat2str(size(value)));
                    end
                elseif iscell(value)
                    if isscalar(value)
                        candidate = ExternalCollectorDispatcher.safe_text_value(value{1}, fallback);
                    else
                        candidate = sprintf('[cell %s]', mat2str(size(value)));
                    end
                else
                    candidate = char(string(value));
                end
            catch
                candidate = char(string(fallback));
            end

            candidate = strtrim(candidate);
            if ~isempty(candidate)
                text = candidate;
            end
        end

        function metric_id = resolve_popup_default_metric_id(metric_key, metric_catalog, panel_id)
            metric_id = '';
            preferred_sources = {'hwinfo', 'icue', 'matlab'};
            switch lower(char(string(panel_id)))
                case {'cooling_rpm', 'coolant_temp', 'device_battery'}
                    preferred_sources = {'icue', 'hwinfo', 'matlab'};
            end
            for si = 1:numel(preferred_sources)
                source = preferred_sources{si};
                idx = find(strcmpi({metric_catalog.metric_key}, metric_key) & ...
                    strcmpi({metric_catalog.source}, source), 1, 'first');
                if ~isempty(idx)
                    metric_id = metric_catalog(idx).id;
                    return;
                end
            end
            idx = find(strcmpi({metric_catalog.metric_key}, metric_key), 1, 'first');
            if ~isempty(idx)
                metric_id = metric_catalog(idx).id;
            end
        end

        function text = coverage_markdown(summary)
            lines = { ...
                '# Collector Coverage Comparison', ...
                '', ...
                sprintf('- Run ID: `%s`', char(string(summary.run_id))), ...
                sprintf('- Generated: `%s`', char(string(summary.generated_at_utc))), ...
                '', ...
                '## Status', ...
                '', ...
                sprintf('- HWiNFO: `%s`', ExternalCollectorDispatcher.status_field(summary.collector_status, 'hwinfo')), ...
                sprintf('- iCUE: `%s`', ExternalCollectorDispatcher.status_field(summary.collector_status, 'icue')), ...
                '', ...
                '## Coverage', ...
                '', ...
                '| Metric | Domain | HWiNFO | iCUE | Preferred | Notes |', ...
                '|---|---|---|---|---|---|'};
            rows = summary.rows;
            for i = 1:numel(rows)
                lines{end + 1} = sprintf('| %s | %s | %s | %s | %s | %s |', ...
                    rows(i).raw_metric_name, rows(i).domain, ...
                    ExternalCollectorDispatcher.bool_token(rows(i).hwinfo_supported), ...
                    ExternalCollectorDispatcher.bool_token(rows(i).icue_supported), ...
                    rows(i).preferred_source, rows(i).notes); %#ok<AGROW>
            end
            text = strjoin(lines, newline);
        end

        function value = status_field(status_struct, field_name)
            value = 'disabled';
            if isstruct(status_struct) && isfield(status_struct, field_name)
                value = char(string(status_struct.(field_name)));
            end
        end

        function token = bool_token(flag)
            if flag
                token = 'yes';
            else
                token = 'no';
            end
        end

        function text = bool_text(flag)
            if flag
                text = 'available';
            else
                text = 'inactive';
            end
        end

        function write_text_file(path_str, text)
            fid = fopen(path_str, 'w');
            if fid == -1
                error('ExternalCollectorDispatcher:WriteFailed', ...
                    'Could not write file: %s', path_str);
            end
            cleaner = onCleanup(@() fclose(fid));
            fprintf(fid, '%s', text);
            clear cleaner;
        end
    end
end
