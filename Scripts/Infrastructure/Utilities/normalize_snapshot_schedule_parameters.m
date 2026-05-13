function params = normalize_snapshot_schedule_parameters(params)
% normalize_snapshot_schedule_parameters
% Keep snapshot-count metadata and derived time vectors internally
% consistent without inflating runtime snapshots from animation frames.

    if nargin < 1 || ~isstruct(params)
        params = struct();
        return;
    end

    plot_count = pick_numeric_scalar(params, {'num_plot_snapshots', 'num_snapshots'}, 9);
    runtime_count = pick_numeric_scalar(params, {'num_snapshots'}, plot_count);
    animation_count = pick_numeric_scalar(params, {'animation_num_frames', 'num_animation_frames'}, runtime_count);

    plot_count = max(1, round(plot_count));
    runtime_count = max(plot_count, round(runtime_count));
    animation_count = max(2, round(animation_count));

    params.num_plot_snapshots = plot_count;
    params.num_snapshots = runtime_count;
    params.animation_num_frames = animation_count;
    params.num_animation_frames = animation_count;

    if isfield(params, 'Tfinal') && isnumeric(params.Tfinal) && isscalar(params.Tfinal) && isfinite(params.Tfinal)
        tfinal = double(params.Tfinal);
        if ~has_valid_time_vector(params, 'snap_times', runtime_count)
            params.snap_times = linspace(0, tfinal, runtime_count);
        end
        if ~has_valid_time_vector(params, 'plot_snap_times', plot_count)
            params.plot_snap_times = linspace(0, tfinal, plot_count);
        end
        if ~has_valid_time_vector(params, 'animation_times', animation_count)
            params.animation_times = linspace(0, tfinal, animation_count);
        end
    end

    params.snapshot_plot_indices = unique(max(1, min(runtime_count, ...
        round(linspace(1, runtime_count, plot_count)))), 'stable');
end

function value = pick_numeric_scalar(source, field_names, fallback)
    value = fallback;
    if ~isstruct(source)
        return;
    end
    for i = 1:numel(field_names)
        field_name = field_names{i};
        if isfield(source, field_name) && isnumeric(source.(field_name)) && ...
                isscalar(source.(field_name)) && isfinite(source.(field_name))
            value = double(source.(field_name));
            return;
        end
    end
end

function tf = has_valid_time_vector(source, field_name, expected_length)
    tf = false;
    if ~isstruct(source) || ~isfield(source, field_name) || ~isnumeric(source.(field_name))
        return;
    end
    values = source.(field_name);
    tf = isvector(values) && numel(values) == expected_length && all(isfinite(values(:)));
end
