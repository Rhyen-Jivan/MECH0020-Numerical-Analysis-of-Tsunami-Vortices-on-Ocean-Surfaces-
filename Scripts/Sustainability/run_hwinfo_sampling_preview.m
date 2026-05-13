function output = run_hwinfo_sampling_preview(duration_seconds, interval_seconds, hwinfo_source, require_shared_memory, figure_visible)
% run_hwinfo_sampling_preview  Live HWiNFO SHM proof harness via the canonical bridge.
%
% This utility exercises the same dispatcher/bridge path used by the emulator and
% renders a live 3x3 diagnostic:
%   1. CPU Usage
%   2. GPU Usage
%   3. CPU Package Temperature
%   4. CPU Package Power
%   5. Memory Usage
%   6. Fan RPM
%   7. Pump RPM
%   8. Transport Status
%   9. Sample Metadata
%
% Usage:
%   run_hwinfo_sampling_preview()
%   run_hwinfo_sampling_preview(5, 0.5)
%   run_hwinfo_sampling_preview(5, 0.25, 'C:\path\hwinfo_blob.bin', true)
%   run_hwinfo_sampling_preview(5, 0.25, 'C:\path\hwinfo.csv', false, false)

    if nargin < 1 || ~isnumeric(duration_seconds) || ~isfinite(duration_seconds)
        duration_seconds = 5;
    end
    if nargin < 2 || ~isnumeric(interval_seconds) || ~isfinite(interval_seconds)
        interval_seconds = 0.5;
    end
    if nargin < 3
        hwinfo_source = '';
    end
    if nargin < 4 || isempty(require_shared_memory)
        require_shared_memory = false;
    end
    if nargin < 5 || isempty(figure_visible)
        figure_visible = true;
    end

    duration_seconds = max(1, double(duration_seconds));
    interval_seconds = max(0.1, double(interval_seconds));
    require_shared_memory = logical(require_shared_memory);
    figure_visible = logical(figure_visible);

    addpath(genpath(fullfile(fileparts(mfilename('fullpath')), '..')));

    params = create_default_parameters();
    settings = Settings();
    settings.sample_interval = interval_seconds;
    settings.sustainability.sample_interval = interval_seconds;
    settings.sustainability.external_collectors.hwinfo = true;
    settings.sustainability.external_collectors.icue = false;
    settings.sustainability.collector_runtime.hwinfo_shared_memory_blob_path = '';
    settings.sustainability.collector_runtime.hwinfo_transport_mode = 'auto';
    settings.sustainability.collector_runtime.hwinfo_csv_dir = '';
    settings.sustainability.collector_runtime.hwinfo_csv_path = '';
    if require_shared_memory
        settings.sustainability.collector_runtime.hwinfo_transport_mode = 'shared_memory';
    end
    settings = local_apply_hwinfo_source(settings, hwinfo_source);

    dispatcher = ExternalCollectorDispatcher(settings);
    run_id = sprintf('hwinfo_preview_%s', char(datetime('now', 'Format', 'yyyyMMdd_HHmmss')));
    dispatcher.start_live_session(run_id, settings);
    cleanup_dispatcher = onCleanup(@() dispatcher.stop_live_session());

    monitor_series = local_empty_monitor_series();
    sample_count = max(2, ceil(duration_seconds / interval_seconds) + 1);
    analysis = struct('time_vec', monitor_series.t);
    panel_defs = local_preview_panel_defs();
    source_label = 'HWiNFO';
    last_sample = struct();
    shared_memory_confirmed = false;

    fig = figure( ...
        'Name', 'HWiNFO SHM Sampling Preview', ...
        'Color', [0.11 0.12 0.15], ...
        'Visible', local_visible_token(figure_visible), ...
        'NumberTitle', 'off');
    tile_layout = tiledlayout(fig, 3, 3, 'TileSpacing', 'compact', 'Padding', 'compact');
    ax = gobjects(1, 9);
    for idx = 1:9
        ax(idx) = nexttile(tile_layout, idx);
    end

    t0 = tic;
    for idx = 1:sample_count
        last_sample = dispatcher.poll_latest_sample();
        elapsed = toc(t0);
        monitor_series = local_append_sample(monitor_series, last_sample, elapsed, idx);
        analysis.time_vec = monitor_series.t;

        if strcmpi(local_pick_text(last_sample, {'collector_status', 'hwinfo'}, ''), 'shared_memory_connected') && ...
                strcmpi(local_pick_text(last_sample, {'hwinfo_transport'}, 'none'), 'shared_memory')
            shared_memory_confirmed = true;
        end

        for panel_index = 1:numel(panel_defs)
            local_render_metric_panel(ax(panel_index), monitor_series, analysis, params, ...
                panel_defs(panel_index), source_label);
        end
        local_render_status_panel(ax(8), last_sample, require_shared_memory);
        local_render_metadata_panel(ax(9), last_sample, monitor_series, idx, sample_count, elapsed, run_id);
        drawnow limitrate;

        if idx < sample_count
            pause(interval_seconds);
        end
    end

    collector_bundle = ExternalCollectorDispatcher.collector_plot_bundle(monitor_series, analysis, params);
    output = struct( ...
        'monitor_series', monitor_series, ...
        'bundle', collector_bundle, ...
        'figure', fig, ...
        'last_sample', last_sample, ...
        'run_id', run_id, ...
        'shared_memory_confirmed', shared_memory_confirmed);

    if require_shared_memory && ~shared_memory_confirmed
        detail = local_pick_text(last_sample, {'hwinfo_status_reason'}, 'shared memory proof failed');
        if ~figure_visible && ishghandle(fig)
            close(fig);
        end
        error('run_hwinfo_sampling_preview:SharedMemoryRequired', ...
            'Expected HWiNFO shared memory, but transport resolved to %s (%s).', ...
            local_pick_text(last_sample, {'collector_status', 'hwinfo'}, 'unknown'), detail);
    end
end

function settings = local_apply_hwinfo_source(settings, hwinfo_source)
    source_text = char(string(hwinfo_source));
    if isempty(strtrim(source_text))
        return;
    end

    if isfolder(source_text)
        settings.sustainability.collector_runtime.hwinfo_transport_mode = 'csv';
        settings.sustainability.collector_runtime.hwinfo_csv_dir = source_text;
        return;
    end

    [source_dir, ~, ext] = fileparts(source_text);
    switch lower(ext)
        case '.bin'
            settings.sustainability.collector_runtime.hwinfo_transport_mode = 'shared_memory';
            settings.sustainability.collector_runtime.hwinfo_shared_memory_blob_path = source_text;
        otherwise
            settings.sustainability.collector_runtime.hwinfo_transport_mode = 'csv';
            settings.sustainability.collector_runtime.hwinfo_csv_path = source_text;
            if exist(source_dir, 'dir') == 7
                settings.sustainability.collector_runtime.hwinfo_csv_dir = source_dir;
            end
    end
end

function token = local_visible_token(is_visible)
    if is_visible
        token = 'on';
    else
        token = 'off';
    end
end

function panel_defs = local_preview_panel_defs()
    panel_defs = [ ...
        struct('metric_key', 'cpu_proxy', 'fallback_title', 'CPU Usage', 'fallback_ylabel', 'CPU (%)'), ...
        struct('metric_key', 'gpu_series', 'fallback_title', 'GPU Usage', 'fallback_ylabel', 'GPU (%)'), ...
        struct('metric_key', 'cpu_temp_c', 'fallback_title', 'CPU Package Temperature', 'fallback_ylabel', 'Temp (C)'), ...
        struct('metric_key', 'power_w', 'fallback_title', 'CPU Package Power', 'fallback_ylabel', 'Power (W)'), ...
        struct('metric_key', 'memory_series', 'fallback_title', 'Memory Usage', 'fallback_ylabel', 'Memory'), ...
        struct('metric_key', 'fan_rpm', 'fallback_title', 'Fan RPM', 'fallback_ylabel', 'RPM'), ...
        struct('metric_key', 'pump_rpm', 'fallback_title', 'Pump RPM', 'fallback_ylabel', 'RPM') ...
    ];
end

function monitor_series = local_empty_monitor_series()
    base_metrics = local_base_metrics();
    monitor_series = struct( ...
        't', zeros(1, 0), ...
        'elapsed_wall_time', zeros(1, 0), ...
        'collector_series', struct('matlab', base_metrics, 'hwinfo', base_metrics, 'icue', base_metrics), ...
        'collector_status', struct('matlab', 'connected', 'hwinfo', 'disabled', 'icue', 'disabled'), ...
        'coverage_domains', struct('hwinfo', {{}}, 'icue', {{}}), ...
        'preferred_source', struct(), ...
        'raw_log_paths', struct('hwinfo', '', 'icue', ''), ...
        'overlay_metrics', {{'cpu_proxy', 'gpu_series', 'memory_series', 'system_power_w', 'cpu_temp_c'}}, ...
        'collector_metric_catalog', struct([]), ...
        'hwinfo_transport', 'none', ...
        'hwinfo_status_reason', '', ...
        'collector_probe_details', struct('hwinfo', struct(), 'icue', struct()));
end

function metrics = local_base_metrics()
    metrics = struct( ...
        'cpu_proxy', zeros(1, 0), ...
        'gpu_series', zeros(1, 0), ...
        'cpu_temp_c', zeros(1, 0), ...
        'power_w', zeros(1, 0), ...
        'memory_series', zeros(1, 0), ...
        'fan_rpm', zeros(1, 0), ...
        'pump_rpm', zeros(1, 0), ...
        'coolant_temp_c', zeros(1, 0), ...
        'device_battery_level', zeros(1, 0));
end

function monitor_series = local_append_sample(monitor_series, sample, elapsed, idx)
    monitor_series.t(idx) = elapsed;
    monitor_series.elapsed_wall_time(idx) = elapsed;

    if isfield(sample, 'collector_series') && isstruct(sample.collector_series)
        monitor_series.collector_series = local_store_series(monitor_series.collector_series, sample.collector_series, idx);
    else
        monitor_series.collector_series = local_store_series(monitor_series.collector_series, struct(), idx);
    end

    if isfield(sample, 'collector_status'), monitor_series.collector_status = sample.collector_status; end
    if isfield(sample, 'coverage_domains'), monitor_series.coverage_domains = sample.coverage_domains; end
    if isfield(sample, 'preferred_source'), monitor_series.preferred_source = sample.preferred_source; end
    if isfield(sample, 'raw_log_paths'), monitor_series.raw_log_paths = sample.raw_log_paths; end
    if isfield(sample, 'overlay_metrics'), monitor_series.overlay_metrics = sample.overlay_metrics; end
    if isfield(sample, 'collector_metric_catalog'), monitor_series.collector_metric_catalog = sample.collector_metric_catalog; end
    if isfield(sample, 'hwinfo_transport'), monitor_series.hwinfo_transport = char(string(sample.hwinfo_transport)); end
    if isfield(sample, 'hwinfo_status_reason'), monitor_series.hwinfo_status_reason = char(string(sample.hwinfo_status_reason)); end
    if isfield(sample, 'collector_probe_details'), monitor_series.collector_probe_details = sample.collector_probe_details; end
end

function series = local_store_series(series, payload_series, idx)
    known_sources = unique([fieldnames(series); fieldnames(payload_series)]);
    base_metrics = local_base_metrics();

    for source_index = 1:numel(known_sources)
        source = known_sources{source_index};
        if ~isfield(series, source) || ~isstruct(series.(source))
            series.(source) = base_metrics;
        end

        source_struct = series.(source);
        existing_keys = fieldnames(source_struct);
        for key_index = 1:numel(existing_keys)
            key = existing_keys{key_index};
            if numel(source_struct.(key)) < idx
                source_struct.(key)(end + 1:idx) = nan;
            end
        end

        if isfield(payload_series, source) && isstruct(payload_series.(source))
            payload_keys = fieldnames(payload_series.(source));
            for key_index = 1:numel(payload_keys)
                key = payload_keys{key_index};
                if ~isfield(source_struct, key)
                    source_struct.(key) = nan(1, idx);
                end
                value = payload_series.(source).(key);
                if isnumeric(value) && isscalar(value) && isfinite(value)
                    source_struct.(key)(idx) = double(value);
                else
                    source_struct.(key)(idx) = nan;
                end
            end
        end

        series.(source) = source_struct;
    end
end

function local_render_metric_panel(ax, monitor_series, analysis, params, panel_def, source_label)
    cla(ax, 'reset');
    hold(ax, 'on');
    trace = local_make_hwinfo_trace(monitor_series, analysis, params, panel_def.metric_key, source_label);
    panel_meta = local_panel_metadata(monitor_series, panel_def.metric_key, ...
        panel_def.fallback_title, panel_def.fallback_ylabel);

    if ~isempty(trace)
        plot(ax, trace.x, trace.y, ...
            'LineWidth', 1.8, ...
            'Color', trace.color_rgb, ...
            'LineStyle', trace.line_style, ...
            'DisplayName', trace.label);
        legend(ax, 'show', 'Interpreter', 'none', 'Location', 'best');
    else
        xlim(ax, [0, 1]);
        ylim(ax, [0, 1]);
        text(ax, 0.5, 0.5, 'No live telemetry yet.', ...
            'Color', [0.72 0.72 0.72], ...
            'HorizontalAlignment', 'center', ...
            'FontSize', 10, ...
            'Interpreter', 'none');
    end
    hold(ax, 'off');
    title(ax, panel_meta.title, 'Interpreter', 'none', 'Color', [0.95 0.95 0.95]);
    xlabel(ax, panel_meta.xlabel, 'Interpreter', 'none', 'Color', [0.88 0.88 0.88]);
    ylabel(ax, panel_meta.ylabel, 'Interpreter', 'none', 'Color', [0.88 0.88 0.88]);
    grid(ax, 'on');
    ax.XColor = [0.85 0.85 0.85];
    ax.YColor = [0.85 0.85 0.85];
    ax.GridColor = [0.35 0.35 0.35];
    ax.Color = [0.11 0.12 0.15];
end

function trace = local_make_hwinfo_trace(monitor_series, analysis, params, metric_key, label)
    trace = struct([]);
    if ~isfield(monitor_series, 'collector_series') || ~isstruct(monitor_series.collector_series) || ...
            ~isfield(monitor_series.collector_series, 'hwinfo') || ...
            ~isstruct(monitor_series.collector_series.hwinfo) || ...
            ~isfield(monitor_series.collector_series.hwinfo, metric_key)
        return;
    end

    values = reshape(double(monitor_series.collector_series.hwinfo.(metric_key)), 1, []);
    finite_mask = isfinite(values);
    if isempty(values) || ~any(finite_mask)
        return;
    end

    x = local_resolve_timebase(monitor_series, analysis, params, numel(values));
    if isempty(x) || numel(x) ~= numel(values)
        return;
    end

    trace = struct( ...
        'x', x, ...
        'y', values, ...
        'label', label, ...
        'color_rgb', [0.18 0.78 0.36], ...
        'line_style', '-');
end

function x = local_resolve_timebase(monitor_series, analysis, params, n)
    x = zeros(1, 0);
    if nargin < 4 || n <= 0
        return;
    end

    if isfield(monitor_series, 't') && isnumeric(monitor_series.t)
        candidate = reshape(double(monitor_series.t), 1, []);
        if numel(candidate) == n
            x = candidate;
            return;
        end
    end

    if isfield(monitor_series, 'elapsed_wall_time') && isnumeric(monitor_series.elapsed_wall_time)
        candidate = reshape(double(monitor_series.elapsed_wall_time), 1, []);
        if numel(candidate) == n
            x = candidate;
            return;
        end
    end

    if isstruct(analysis) && isfield(analysis, 'time_vec') && isnumeric(analysis.time_vec)
        candidate = reshape(double(analysis.time_vec), 1, []);
        if numel(candidate) == n
            x = candidate;
            return;
        end
    end

    span = NaN;
    if isstruct(params)
        if isfield(params, 'Tfinal') && isnumeric(params.Tfinal) && isscalar(params.Tfinal)
            span = double(params.Tfinal);
        elseif isfield(params, 't_final') && isnumeric(params.t_final) && isscalar(params.t_final)
            span = double(params.t_final);
        end
    end

    if n == 1
        x = 0;
    elseif isfinite(span) && span > 0
        x = linspace(0, span, n);
    else
        x = 0:(n - 1);
    end
end

function panel_meta = local_panel_metadata(monitor_series, metric_key, fallback_title, fallback_ylabel)
    catalog = local_metric_catalog(monitor_series);
    panel_meta = struct('title', fallback_title, 'xlabel', 'Time (s)', 'ylabel', fallback_ylabel);
    if isempty(catalog)
        return;
    end
    for idx = 1:numel(catalog)
        item = catalog(idx);
        if strcmpi(char(string(item.source)), 'hwinfo') && strcmpi(char(string(item.metric_key)), metric_key)
            title_text = char(string(item.default_title));
            ylabel_text = char(string(item.default_ylabel));
            xlabel_text = char(string(item.default_xlabel));
            if ~isempty(title_text)
                panel_meta.title = title_text;
            end
            if ~isempty(ylabel_text)
                panel_meta.ylabel = ylabel_text;
            end
            if ~isempty(xlabel_text)
                panel_meta.xlabel = xlabel_text;
            end
            return;
        end
    end
end

function local_render_status_panel(ax, sample, require_shared_memory)
    cla(ax, 'reset');
    axis(ax, 'off');
    ax.Color = [0.11 0.12 0.15];
    title(ax, 'Transport Status', 'Interpreter', 'none', 'Color', [0.95 0.95 0.95]);

    hwinfo_probe = local_pick_struct(sample, {'collector_probe_details', 'hwinfo'});
    lines = { ...
        sprintf('Collector Status: %s', local_pick_text(sample, {'collector_status', 'hwinfo'}, 'unknown')) ...
        sprintf('Transport: %s', local_pick_text(sample, {'hwinfo_transport'}, 'none')) ...
        sprintf('Reason: %s', local_pick_text(sample, {'hwinfo_status_reason'}, '')) ...
        sprintf('Require SHM: %s', char(string(require_shared_memory))) ...
        sprintf('Process Running: %s', char(string(local_pick_logical(hwinfo_probe, {'process_running'}, false)))) ...
        sprintf('SensorsSM: %s', local_pick_text(hwinfo_probe, {'ini_shared_memory_state'}, 'unknown')) ...
        sprintf('SHM Available: %s', char(string(local_pick_logical(hwinfo_probe, {'shared_memory_available'}, false)))) ...
        sprintf('SHM Detail: %s', local_pick_text(hwinfo_probe, {'shared_memory_status_detail'}, '')) ...
        sprintf('CSV Path: %s', local_pick_text(hwinfo_probe, {'csv_path'}, '--')) ...
    };
    local_render_text_panel(ax, lines);
end

function local_render_metadata_panel(ax, sample, monitor_series, sample_index, sample_count, elapsed, run_id)
    cla(ax, 'reset');
    axis(ax, 'off');
    ax.Color = [0.11 0.12 0.15];
    title(ax, 'Sample Metadata', 'Interpreter', 'none', 'Color', [0.95 0.95 0.95]);

    hwinfo_probe = local_pick_struct(sample, {'collector_probe_details', 'hwinfo'});
    metric_catalog = local_metric_catalog(monitor_series);
    lines = { ...
        sprintf('Run ID: %s', run_id) ...
        sprintf('Sample: %d / %d', sample_index, sample_count) ...
        sprintf('Elapsed: %.2f s', elapsed) ...
        sprintf('Timestamp: %s', local_pick_text(sample, {'timestamp_utc'}, '--')) ...
        sprintf('Metric Catalog Entries: %d', numel(metric_catalog)) ...
        sprintf('Raw Log Path: %s', local_pick_text(sample, {'raw_log_paths', 'hwinfo'}, '--')) ...
        sprintf('SHM Object: %s', local_pick_text(hwinfo_probe, {'shared_memory_name'}, '--')) ...
        sprintf('Mutex Opened: %s', char(string(local_pick_logical(hwinfo_probe, {'shared_memory_mutex_opened'}, false)))) ...
        sprintf('Map Opened: %s', char(string(local_pick_logical(hwinfo_probe, {'shared_memory_map_opened'}, false)))) ...
        sprintf('View Opened: %s', char(string(local_pick_logical(hwinfo_probe, {'shared_memory_view_opened'}, false)))) ...
    };
    local_render_text_panel(ax, lines);
end

function catalog = local_metric_catalog(monitor_series)
    catalog = struct([]);
    if ~isstruct(monitor_series) || ~isfield(monitor_series, 'collector_metric_catalog') || ...
            isempty(monitor_series.collector_metric_catalog)
        return;
    end
    raw_catalog = monitor_series.collector_metric_catalog;
    if iscell(raw_catalog)
        raw_catalog = [raw_catalog{:}];
    end
    if ~isstruct(raw_catalog)
        return;
    end
    catalog = raw_catalog;
end

function local_render_text_panel(ax, lines)
    y = 0.97;
    dy = 0.1;
    for idx = 1:numel(lines)
        text(ax, 0.02, y, char(string(lines{idx})), ...
            'Units', 'normalized', ...
            'Color', [0.88 0.88 0.88], ...
            'HorizontalAlignment', 'left', ...
            'VerticalAlignment', 'top', ...
            'Interpreter', 'none', ...
            'FontName', 'Consolas', ...
            'FontSize', 9);
        y = y - dy;
    end
end

function out = local_pick_struct(input_struct, path)
    out = struct();
    value = local_pick_value(input_struct, path, struct());
    if isstruct(value)
        out = value;
    end
end

function out = local_pick_text(input_struct, path, fallback)
    value = local_pick_value(input_struct, path, fallback);
    out = char(string(value));
end

function out = local_pick_logical(input_struct, path, fallback)
    value = local_pick_value(input_struct, path, fallback);
    out = logical(value);
end

function value = local_pick_value(input_struct, path, fallback)
    value = fallback;
    current = input_struct;
    for idx = 1:numel(path)
        key = path{idx};
        if ~isstruct(current) || ~isfield(current, key)
            return;
        end
        current = current.(key);
    end
    value = current;
end
