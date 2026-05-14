function [fig, output_path, metric_rows] = export_phase1_ic_error_comparison_figure(results_source, output_path)
% export_phase1_ic_error_comparison_figure  Create a 3-panel Phase 1 IC error comparison figure.

    [Results, results_path] = load_phase1_results_local(results_source);
    [case_labels, metric_rows] = build_metric_rows_local(Results);

    fig = figure('Visible', 'off', 'Color', 'w', 'Units', 'pixels', 'Position', [140 140 1380 420]);
    layout = tiledlayout(fig, 1, 3, 'TileSpacing', 'compact', 'Padding', 'compact');
    title(layout, 'Phase 1 FD-SM Error Comparison by Initial Condition');

    metric_specs = { ...
        {'cross_method_speed_relative_l2_mismatch', '(A) Velocity magnitude', 'Relative L2 mismatch'}, ...
        {'cross_method_streamfunction_relative_l2_mismatch', '(B) Streamfunction', 'Relative L2 mismatch'}, ...
        {'cross_method_mismatch_l2', '(C) Vorticity', 'Relative L2 mismatch'}};

    bar_colors = [0.08 0.36 0.78; 0.88 0.32 0.10];
    for i = 1:numel(metric_specs)
        ax = nexttile(layout, i);
        values = extract_metric_matrix_local(metric_rows, metric_specs{i}{1});
        bars = bar(ax, values, 'grouped');
        for j = 1:min(numel(bars), size(bar_colors, 1))
            bars(j).FaceColor = bar_colors(j, :);
        end
        if numel(bars) >= 1
            bars(1).DisplayName = 'FD';
        end
        if numel(bars) >= 2
            bars(2).DisplayName = 'SM';
        end
        ax.XTick = 1:numel(case_labels);
        ax.XTickLabel = case_labels;
        ax.XTickLabelRotation = 16;
        xlabel(ax, 'Initial condition');
        ylabel(ax, metric_specs{i}{3});
        title(ax, metric_specs{i}{2});
        grid(ax, 'on');
        box(ax, 'on');
        if i == 1
            legend(ax, 'Location', 'northwest');
        end
        annotate_grouped_bars_local(ax, bars, values);
    end

    if nargin < 2 || strlength(string(output_path)) == 0
        output_path = default_output_path_local(results_path);
    end
    if strlength(string(output_path)) > 0
        output_path = char(string(output_path));
        output_dir = fileparts(output_path);
        if strlength(string(output_dir)) > 0 && exist(output_dir, 'dir') ~= 7
            mkdir(output_dir);
        end
        exportgraphics(fig, output_path, 'Resolution', 300);
    else
        output_path = '';
    end
end

function [Results, results_path] = load_phase1_results_local(results_source)
    results_path = '';
    if ischar(results_source) || isstring(results_source)
        results_path = char(string(results_source));
        loaded_data = load(results_path);
        if isfield(loaded_data, 'ResultsForSave')
            Results = loaded_data.ResultsForSave;
            return;
        end
        if isfield(loaded_data, 'Results')
            Results = loaded_data.Results;
            return;
        end
        error('export_phase1_ic_error_comparison_figure:InvalidResultsFile', ...
            'The Phase 1 results file did not contain ResultsForSave or Results.');
    end
    if isstruct(results_source)
        Results = results_source;
        return;
    end
    error('export_phase1_ic_error_comparison_figure:InvalidInput', ...
        'results_source must be a Phase 1 results struct or a .mat file path.');
end

function [case_labels, metric_rows] = build_metric_rows_local(Results)
    ic_study = pick_struct_local(Results, {'ic_study'}, struct());
    metrics = pick_struct_local(Results, {'metrics'}, struct());
    fd_metrics = pick_struct_local(metrics, {'FD'}, struct());
    sm_metrics = pick_struct_local(metrics, {'Spectral'}, struct());
    baseline_case_id = pick_text_local(ic_study, {'baseline_case_id'}, 'baseline_elliptic_single');
    baseline_label = pretty_case_label_local(baseline_case_id, pick_text_local(ic_study, {'baseline_label'}, 'Elliptic'));

    metric_rows = repmat(struct('label', '', 'fd', struct(), 'sm', struct()), 1, 1);
    metric_rows(1).label = baseline_label;
    metric_rows(1).fd = apply_case_metric_aliases_local(fd_metrics);
    metric_rows(1).sm = apply_case_metric_aliases_local(sm_metrics);

    case_labels = {baseline_label};
    cases = pick_value_local(ic_study, 'cases', struct([]));
    if ~(isstruct(cases) && ~isempty(cases))
        return;
    end

    for i = 1:numel(cases)
        case_label = pretty_case_label_local( ...
            pick_text_local(cases(i), {'case_id'}, sprintf('case_%02d', i)), ...
            pick_text_local(cases(i), {'display_label', 'label'}, sprintf('Case %d', i)));
        case_labels{end + 1} = case_label; %#ok<AGROW>
        metric_rows(end + 1).label = case_label; %#ok<AGROW>
        metric_rows(end).fd = resolve_phase1_ic_case_metrics(cases(i), 'fd');
        metric_rows(end).sm = resolve_phase1_ic_case_metrics(cases(i), 'spectral');
    end
end

function metric_matrix = extract_metric_matrix_local(metric_rows, metric_name)
    metric_matrix = nan(numel(metric_rows), 2);
    for i = 1:numel(metric_rows)
        metric_matrix(i, 1) = pick_numeric_local(metric_rows(i).fd, {metric_name}, NaN);
        metric_matrix(i, 2) = pick_numeric_local(metric_rows(i).sm, {metric_name}, NaN);
    end
end

function annotate_grouped_bars_local(ax, bars, values)
    if isempty(bars)
        return;
    end
    for i = 1:numel(bars)
        x_endpoints = bars(i).XEndPoints;
        y_endpoints = bars(i).YEndPoints;
        for j = 1:min(numel(x_endpoints), size(values, 1))
            value = values(j, i);
            if ~isfinite(value)
                continue;
            end
            text(ax, x_endpoints(j), y_endpoints(j), sprintf('%.3g', value), ...
                'HorizontalAlignment', 'center', 'VerticalAlignment', 'bottom', ...
                'FontSize', 9, 'Interpreter', 'none');
        end
    end
end

function metrics = apply_case_metric_aliases_local(metrics)
    metrics.relative_vorticity_error_L2 = pick_numeric_local(metrics, ...
        {'relative_vorticity_error_L2'}, pick_numeric_local(metrics, {'cross_method_mismatch_l2'}, NaN));
    metrics.relative_vorticity_error_Linf = pick_numeric_local(metrics, ...
        {'relative_vorticity_error_Linf'}, pick_numeric_local(metrics, {'cross_method_mismatch_linf'}, NaN));
end

function label = pretty_case_label_local(case_id, fallback_label)
    case_id = lower(strtrim(char(string(case_id))));
    switch case_id
        case {'', 'baseline_elliptic_single', 'elliptic', 'elliptical_vortex', 'elliptic_vortex'}
            label = 'Elliptic Gaussian';
        case {'taylor_green', 'taylorgreen'}
            label = 'Taylor-Green';
        otherwise
            label = char(string(fallback_label));
            if strlength(string(label)) == 0
                label = strrep(case_id, '_', ' ');
            end
    end
end

function output_path = default_output_path_local(results_path)
    output_path = '';
    if strlength(string(results_path)) == 0
        return;
    end
    data_dir = fileparts(char(string(results_path)));
    run_root = fileparts(data_dir);
    output_path = fullfile(run_root, 'Visuals', 'Comparisons', 'phase1_ic_error_comparison.png');
end

function value = pick_numeric_local(source, field_names, fallback)
    if nargin < 3
        fallback = NaN;
    end
    value = fallback;
    if ~isstruct(source)
        return;
    end
    for i = 1:numel(field_names)
        field_name = char(string(field_names{i}));
        if isfield(source, field_name)
            candidate = source.(field_name);
            if isnumeric(candidate) && isscalar(candidate) && isfinite(double(candidate))
                value = double(candidate);
                return;
            end
        end
    end
end

function value = pick_text_local(source, field_names, fallback)
    if nargin < 3
        fallback = '';
    end
    value = char(string(fallback));
    if ~isstruct(source)
        return;
    end
    for i = 1:numel(field_names)
        field_name = char(string(field_names{i}));
        if isfield(source, field_name) && strlength(string(source.(field_name))) > 0
            value = char(string(source.(field_name)));
            return;
        end
    end
end

function value = pick_struct_local(source, field_names, fallback)
    if nargin < 3
        fallback = struct();
    end
    value = fallback;
    if ~isstruct(source)
        return;
    end
    for i = 1:numel(field_names)
        field_name = char(string(field_names{i}));
        if isfield(source, field_name) && isstruct(source.(field_name))
            value = source.(field_name);
            return;
        end
    end
end

function value = pick_value_local(source, field_name, fallback)
    if nargin < 3
        fallback = [];
    end
    value = fallback;
    if isstruct(source) && isfield(source, field_name)
        value = source.(field_name);
    end
end
