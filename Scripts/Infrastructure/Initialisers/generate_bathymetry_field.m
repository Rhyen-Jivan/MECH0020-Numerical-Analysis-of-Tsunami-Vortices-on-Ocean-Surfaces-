function [bath, meta] = generate_bathymetry_field(X, Y, scenario_id, params)
% GENERATE_BATHYMETRY_FIELD Canonical equation-driven bathymetry generator.
%
% Inputs:
%   X, Y        - meshgrid arrays
%   scenario_id - catalog token (flat_2d, wavebed_2d, ..., reef_lagoon_3d)
%   params      - optional struct of scalar controls
%
% Outputs:
%   bath - bathymetry field (depth-like scalar map)
%   meta - struct with display_name, dimension, and irl_label

    if nargin < 4 || ~isstruct(params)
        params = struct();
    end
    if ~isnumeric(X) || ~isnumeric(Y) || ~isequal(size(X), size(Y))
        error('generate_bathymetry_field:InvalidGrid', ...
            'X and Y must be numeric arrays of identical size.');
    end

    scenario = normalize_bathymetry_scenario_token(scenario_id);
    if isempty(scenario)
        error('generate_bathymetry_field:MissingScenario', ...
            'scenario_id must be a non-empty token.');
    end
    active_params = resolve_active_bathymetry_params(params, scenario);

    x_min = min(X(:));
    x_max = max(X(:));
    y_min = min(Y(:));
    y_max = max(Y(:));
    Lx = max(x_max - x_min, eps);
    Ly = max(y_max - y_min, eps);

    z0 = pick_param(active_params, {'bathymetry_depth_offset', 'z0', 'depth_offset'}, 1000.0);
    amp = pick_param(active_params, {'bathymetry_relief_amplitude', 'bathymetry_amplitude', 'amplitude', 'A'}, 180.0);
    slope = pick_param(active_params, {'bathymetry_bed_slope', 'bed_slope', 'slope'}, 0.03);
    control_resolution = round(pick_param(active_params, {'interpolation_resolution', 'bathymetry_resolution', 'resolution'}, 96));
    control_resolution = max(24, min(256, control_resolution));

    x_norm = (X - x_min) / Lx;
    y_norm = (Y - y_min) / Ly;
    x_c = x_min + 0.5 * Lx;
    y_c = y_min + 0.5 * Ly;
    dy_min = resolve_min_spacing(Y(:, 1), Ly);
    y_floor = y_min + dy_min;
    y_ceiling = y_max - dy_min;
    y_span = max(y_ceiling - y_floor, eps);
    relief_scale = min(max(abs(slope) * max(Lx, Ly), 0.15 * dy_min), 0.70 * y_span);
    slope_sign = sign(slope);
    if slope_sign == 0
        slope_sign = 1;
    end
    profile_x = X(1, :);
    control_points = struct('enabled', {}, 'x', {}, 'y', {}, 'elevation', {});

    meta = struct( ...
        'scenario_id', scenario, ...
        'display_name', scenario, ...
        'dimension', '2d', ...
        'irl_label', 'Idealized ocean basin', ...
        'profile_x', profile_x, ...
        'profile_y', y_floor + 0.0 * profile_x, ...
        'control_points', control_points, ...
        'blocks_periodic_bc', logical(bathymetry_blocks_periodic_bc(scenario)), ...
        'uses_custom_points', false);

    switch scenario
        case 'flat_2d'
            profile = y_floor + 0.0 * profile_x;
            bath = replicate_profile(profile, size(Y, 1));
            meta.display_name = 'Flat';
            meta.dimension = '2d';
            meta.irl_label = 'IRL analog: open basin with nearly flat lower boundary';
            meta.profile_y = profile;

        case 'linear_elevation_2d'
            if slope_sign >= 0
                profile = y_floor + relief_scale * x_norm(1, :);
            else
                profile = y_floor + relief_scale * (1.0 - x_norm(1, :));
            end
            profile = clamp_profile(profile, y_floor, y_ceiling);
            bath = replicate_profile(profile, size(Y, 1));
            meta.display_name = 'Linear Elevation';
            meta.dimension = '2d';
            meta.irl_label = 'IRL analog: gently rising seabed plane';
            meta.profile_y = profile;

        case 'wave_profile_2d'
            wave_height_scale = pick_param(active_params, {'wave_height_scale'}, 1.0);
            wave_cycles = max(0.25, pick_param(active_params, {'wave_cycles'}, 1.0));
            profile = y_floor + 0.5 * wave_height_scale * relief_scale * ...
                (1.0 - cos(2.0 * pi * wave_cycles * x_norm(1, :)));
            profile = clamp_profile(profile, y_floor, y_ceiling);
            bath = replicate_profile(profile, size(Y, 1));
            meta.display_name = 'Wave Profile';
            meta.dimension = '2d';
            meta.irl_label = 'IRL analog: long undulating seabed profile';
            meta.profile_y = profile;

        case 'custom_points_2d'
            [bath, profile, control_points] = build_custom_points_field( ...
                X, Y, active_params, x_min, x_max, y_min, y_max, y_floor, y_ceiling, control_resolution);
            meta.display_name = 'Custom Points';
            meta.dimension = '2d';
            meta.irl_label = 'IRL analog: survey-informed custom seabed edits';
            meta.profile_y = profile;
            meta.control_points = control_points;
            meta.uses_custom_points = true;

        case 'reef_2d'
            reef_pos = pick_param(active_params, {'reef_crest_position'}, 0.74);
            reef_width = max(0.01, pick_param(active_params, {'reef_crest_width'}, 0.07));
            reef_height_scale = pick_param(active_params, {'reef_height_scale'}, 0.88);
            profile = y_floor + 0.22 * relief_scale * x_norm(1, :);
            profile = profile + reef_height_scale * relief_scale * ...
                exp(-((x_norm(1, :) - reef_pos) .^ 2) / max(reef_width ^ 2, eps));
            profile = clamp_profile(profile, y_floor, y_ceiling);
            bath = replicate_profile(profile, size(Y, 1));
            meta.display_name = 'Reef';
            meta.dimension = '2d';
            meta.irl_label = 'IRL analog: reef crest rising off the lower surface';
            meta.profile_y = profile;

        case 'recess_2d'
            recess_pos = pick_param(active_params, {'recess_position'}, 0.38);
            recess_width = max(0.01, pick_param(active_params, {'recess_width'}, 0.092));
            recess_depth_scale = pick_param(active_params, {'recess_depth_scale'}, 0.78);
            profile = y_floor + 0.30 * relief_scale + 0.18 * relief_scale * x_norm(1, :);
            profile = profile - recess_depth_scale * relief_scale * ...
                exp(-((x_norm(1, :) - recess_pos) .^ 2) / max(recess_width ^ 2, eps));
            profile = clamp_profile(profile, y_floor, y_ceiling);
            bath = replicate_profile(profile, size(Y, 1));
            meta.display_name = 'Recess';
            meta.dimension = '2d';
            meta.irl_label = 'IRL analog: recessed pocket or local seabed depression';
            meta.profile_y = profile;

        case 'shore_runup_2d'
            transition_pos = pick_param(active_params, {'shore_transition_position'}, 0.72);
            transition_width = max(0.01, pick_param(active_params, {'shore_transition_width'}, 0.08));
            runup_height_scale = pick_param(active_params, {'runup_height_scale'}, 0.96);
            transition = 0.5 * (1.0 + tanh((x_norm(1, :) - transition_pos) / transition_width));
            profile = y_floor + 0.12 * relief_scale * x_norm(1, :) + runup_height_scale * relief_scale * transition;
            profile = clamp_profile(profile, y_floor, y_ceiling);
            bath = replicate_profile(profile, size(Y, 1));
            meta.display_name = 'Shore Runup';
            meta.dimension = '2d';
            meta.irl_label = 'IRL analog: nearshore run-up ramp toward the coastline';
            meta.profile_y = profile;

        case 'tsunami_runup_composite_2d'
            reef_pos = pick_param(active_params, {'composite_reef_position'}, 0.24);
            reef_width = max(0.01, pick_param(active_params, {'composite_reef_width'}, 0.05));
            reef_scale = pick_param(active_params, {'composite_reef_scale'}, 0.75);
            ravine_pos = pick_param(active_params, {'composite_ravine_position'}, 0.58);
            ravine_width = max(0.01, pick_param(active_params, {'composite_ravine_width'}, 0.06));
            ravine_scale = pick_param(active_params, {'composite_ravine_scale'}, 1.05);
            undercut_pos = pick_param(active_params, {'composite_undercut_position'}, 0.72);
            undercut_width = max(0.005, pick_param(active_params, {'composite_undercut_width'}, 0.025));
            undercut_scale = pick_param(active_params, {'composite_undercut_scale'}, 0.52);
            wave_cycles = max(0.5, pick_param(active_params, {'composite_wave_cycles'}, 3.0));
            wave_scale = pick_param(active_params, {'composite_wave_scale'}, 0.18);
            shore_pos = pick_param(active_params, {'composite_shore_position'}, 0.82);
            shore_width = max(0.01, pick_param(active_params, {'composite_shore_width'}, 0.06));
            shore_scale = pick_param(active_params, {'composite_shore_scale'}, 0.95);
            base_profile = y_floor + 0.10 * relief_scale + 0.16 * relief_scale * x_norm(1, :);
            reef_component = reef_scale * relief_scale * exp(-((x_norm(1, :) - reef_pos) .^ 2) / max(reef_width ^ 2, eps));
            uneven_component = wave_scale * relief_scale * sin(2.0 * pi * wave_cycles * x_norm(1, :)) .* ...
                exp(-0.5 * ((x_norm(1, :) - 0.55) / 0.28) .^ 2);
            ravine_component = -ravine_scale * relief_scale * exp(-((x_norm(1, :) - ravine_pos) .^ 2) / max(ravine_width ^ 2, eps));
            undercut_component = undercut_scale * relief_scale * tanh((x_norm(1, :) - undercut_pos) / undercut_width);
            shore_component = shore_scale * relief_scale * 0.5 * (1.0 + tanh((x_norm(1, :) - shore_pos) / shore_width));
            profile = base_profile + reef_component + uneven_component + ravine_component + 0.32 * undercut_component + shore_component;
            profile = clamp_profile(profile, y_floor, y_ceiling);
            bath = replicate_profile(profile, size(Y, 1));
            meta.display_name = 'Composite Tsunami Run-up';
            meta.dimension = '2d';
            meta.irl_label = 'IRL analog: reef + undercut + uneven bed + ravine + shore run-up composite';
            meta.profile_y = profile;

        case 'tohoku_profile_2d'
            smoothing_strength = max(0.0, pick_param(active_params, {'tohoku_smoothing_strength'}, 1.5));
            depth_scale = max(0.25, pick_param(active_params, {'tohoku_depth_scale'}, 1.0));
            depth_offset = pick_param(active_params, {'tohoku_depth_offset'}, 0.0);
            stretch = max(0.5, pick_param(active_params, {'tohoku_horizontal_stretch'}, 1.0));
            crop_center = min(max(pick_param(active_params, {'tohoku_crop_center'}, 0.50), 0.05), 0.95);
            crop_width = min(max(pick_param(active_params, {'tohoku_crop_width'}, 1.0), 0.2), 1.0);
            [profile_x_norm, profile_depth_norm] = load_tohoku_profile_data();
            [profile_x_norm, profile_depth_norm] = reshape_profile_with_crop(profile_x_norm, profile_depth_norm, crop_center, crop_width, stretch);
            if smoothing_strength > 0
                profile_depth_norm = smooth_profile(profile_depth_norm, smoothing_strength);
            end
            profile_depth_norm = profile_depth_norm - min(profile_depth_norm);
            denom = max(max(profile_depth_norm) - min(profile_depth_norm), eps);
            profile_depth_norm = profile_depth_norm ./ denom;
            profile = y_floor + y_span * (0.04 + 0.82 * min(max(depth_offset + depth_scale * profile_depth_norm, 0.0), 1.0));
            profile = interp1(profile_x_norm, profile, x_norm(1, :), 'pchip', 'extrap');
            profile = clamp_profile(profile, y_floor, y_ceiling);
            bath = replicate_profile(profile, size(Y, 1));
            meta.display_name = 'Tohoku Profile';
            meta.dimension = '2d';
            meta.irl_label = 'IRL analog: extracted Tohoku shelf-slope-trench transect (ETOPO-style profile)';
            meta.profile_y = profile;

        case 'flat_3d'
            bath = z0 + 0.0 * X;
            meta.display_name = 'Flat Plane (3D)';
            meta.dimension = '3d';
            meta.irl_label = 'IRL analog: basin floor with low relief';

        case 'wavebed_3d'
            x_wave_count = max(1, round(pick_param(active_params, {'x_wave_count'}, 1.0)));
            y_wave_count = max(1, round(pick_param(active_params, {'y_wave_count'}, 1.0)));
            bath = z0 + amp * (sin(2.0 * pi * x_wave_count * x_norm) + ...
                sin(2.0 * pi * y_wave_count * y_norm));
            meta.display_name = 'Wave Bed (3D)';
            meta.dimension = '3d';
            meta.irl_label = 'IRL analog: intersecting seabed sandwave systems';

        case 'seamount_basin_3d'
            seamount_pos_x = pick_param(active_params, {'seamount_position_x'}, 0.50);
            seamount_pos_y = pick_param(active_params, {'seamount_position_y'}, 0.50);
            seamount_width = max(0.03, pick_param(active_params, {'seamount_width'}, 0.18));
            seamount_height_scale = pick_param(active_params, {'seamount_height_scale'}, 1.15);
            basin = z0 + slope * (X - x_min) + 0.25 * slope * (Y - y_min);
            seamount_x = x_min + seamount_pos_x * Lx;
            seamount_y = y_min + seamount_pos_y * Ly;
            r2 = ((X - seamount_x) .^ 2) + ((Y - seamount_y) .^ 2);
            sigma_m = max((seamount_width * min(Lx, Ly)) ^ 2, eps);
            seamount = seamount_height_scale * amp * exp(-r2 / sigma_m);
            bath = basin + seamount;
            meta.display_name = 'Seamount Basin (3D)';
            meta.dimension = '3d';
            meta.irl_label = 'IRL analog: volcanic seamount rising from deep basin';

        case 'canyon_undercut_3d'
            canyon_pos_x = pick_param(active_params, {'canyon_position_x'}, 0.48);
            canyon_pos_y = pick_param(active_params, {'canyon_position_y'}, 0.52);
            canyon_width_x = max(0.02, pick_param(active_params, {'canyon_width_x'}, 0.11));
            canyon_width_y = max(0.02, pick_param(active_params, {'canyon_width_y'}, 0.18));
            canyon_depth_scale = pick_param(active_params, {'canyon_depth_scale'}, 1.20);
            undercut_pos = pick_param(active_params, {'undercut_position'}, 0.82);
            undercut_width = max(0.005, pick_param(active_params, {'undercut_width'}, 0.025));
            undercut_height_scale = pick_param(active_params, {'undercut_height_scale'}, 0.70);
            shelf = z0 + slope * (X - x_min) + 0.30 * amp * (0.5 * (1 + tanh((X - (x_min + 0.70 * Lx)) / (0.07 * Lx))));
            canyon_center_x = x_min + canyon_pos_x * Lx;
            canyon_center_y = y_min + canyon_pos_y * Ly;
            sx2 = max((canyon_width_x * Lx) ^ 2, eps);
            sy2 = max((canyon_width_y * Ly) ^ 2, eps);
            canyon = -canyon_depth_scale * amp * exp(-((X - canyon_center_x) .^ 2) / sx2 - ((Y - canyon_center_y) .^ 2) / sy2);
            undercut = undercut_height_scale * amp * tanh((X - (x_min + undercut_pos * Lx)) / max(undercut_width * Lx, eps));
            bath = shelf + canyon + undercut;
            meta.display_name = 'Canyon + Undercut (3D)';
            meta.dimension = '3d';
            meta.irl_label = 'IRL analog: submarine canyon with sharp shelf undercut';

        case 'reef_lagoon_3d'
            reef_radius = pick_param(active_params, {'reef_radius'}, 0.62);
            reef_ring_width = max(0.01, pick_param(active_params, {'reef_ring_width'}, 0.10));
            reef_height_scale = pick_param(active_params, {'reef_height_scale'}, 1.05);
            lagoon_radius = max(0.05, pick_param(active_params, {'lagoon_radius'}, 0.42));
            lagoon_depth_scale = pick_param(active_params, {'lagoon_depth_scale'}, 0.75);
            shelf = z0 + slope * (X - x_min) + 0.15 * slope * (Y - y_min);
            r = sqrt(((X - x_c) / max(0.40 * Lx, eps)) .^ 2 + ((Y - y_c) / max(0.40 * Ly, eps)) .^ 2);
            reef_ring = reef_height_scale * amp * exp(-((r - reef_radius) .^ 2) / max(reef_ring_width ^ 2, eps));
            lagoon_core = -lagoon_depth_scale * amp * exp(-(r .^ 2) / max(lagoon_radius ^ 2, eps));
            bath = shelf + reef_ring + lagoon_core;
            meta.display_name = 'Reef Lagoon (3D)';
            meta.dimension = '3d';
            meta.irl_label = 'IRL analog: fringing reef ring around a lagoon interior';

        otherwise
            error('generate_bathymetry_field:UnknownScenario', ...
                'Unknown bathymetry scenario token: %s', scenario);
    end

    if any(~isfinite(bath), 'all')
        error('generate_bathymetry_field:NonFiniteField', ...
            'Bathymetry scenario "%s" produced non-finite values.', scenario);
    end
end

function active_params = resolve_active_bathymetry_params(params, scenario)
    active_params = params;
    if ~isstruct(params) || ~isfield(params, 'bathymetry_dynamic_params') || ...
            ~isstruct(params.bathymetry_dynamic_params) || ...
            ~isfield(params.bathymetry_dynamic_params, scenario) || ...
            ~isstruct(params.bathymetry_dynamic_params.(scenario))
        return;
    end

    scenario_params = params.bathymetry_dynamic_params.(scenario);
    keys = fieldnames(scenario_params);
    for i = 1:numel(keys)
        active_params.(keys{i}) = scenario_params.(keys{i});
    end
end

function value = pick_param(params, keys, fallback)
    value = fallback;
    for i = 1:numel(keys)
        key = keys{i};
        if isfield(params, key)
            candidate = double(params.(key));
            if isfinite(candidate)
                value = candidate;
                return;
            end
        end
    end
end

function spacing = resolve_min_spacing(vec, fallback_span)
    vec = double(vec(:));
    vec = vec(isfinite(vec));
    spacing = max(fallback_span / 128.0, eps);
    if numel(vec) < 2
        return;
    end
    diffs = diff(unique(vec));
    diffs = diffs(isfinite(diffs) & diffs > 0);
    if ~isempty(diffs)
        spacing = min(diffs);
    end
end

function profile = clamp_profile(profile, y_floor, y_ceiling)
    profile = max(y_floor, min(y_ceiling, double(profile)));
end

function bath = replicate_profile(profile, ny)
    bath = repmat(reshape(profile, 1, []), ny, 1);
end

function [bath, profile, points] = build_custom_points_field(X, Y, params, x_min, x_max, y_min, y_max, y_floor, y_ceiling, control_resolution)
    points = resolve_custom_points(params, x_min, x_max, y_min, y_max, y_floor, y_ceiling);
    x_pts = [points.x].';
    y_pts = [points.y].';
    e_pts = [points.elevation].';

    x_aux = linspace(x_min, x_max, control_resolution);
    y_aux = linspace(y_min, y_max, control_resolution);
    [X_aux, Y_aux] = meshgrid(x_aux, y_aux);

    interpolant = scatteredInterpolant(x_pts, y_pts, e_pts, 'linear', 'nearest');
    bath_aux = interpolant(X_aux, Y_aux);
    bath_aux = clamp_profile(bath_aux, y_floor, y_ceiling);
    bath = interp2(X_aux, Y_aux, bath_aux, X, Y, 'linear');
    nan_mask = ~isfinite(bath);
    if any(nan_mask, 'all')
        bath(nan_mask) = interp2(X_aux, Y_aux, bath_aux, X(nan_mask), Y(nan_mask), 'nearest');
    end
    bath = clamp_profile(bath, y_floor, y_ceiling);
    profile = max(bath_aux, [], 1);
    profile = clamp_profile(profile, y_floor, y_ceiling);
end

function points = resolve_custom_points(params, x_min, x_max, y_min, y_max, y_floor, y_ceiling)
    if isfield(params, 'bathymetry_custom_points')
        raw_points = params.bathymetry_custom_points;
    elseif isfield(params, 'custom_points')
        raw_points = params.custom_points;
    else
        raw_points = [];
    end

    if isempty(raw_points)
        error('generate_bathymetry_field:MissingCustomPoints', ...
            'custom_points_2d requires bathymetry_custom_points or custom_points data.');
    end
    if ~isstruct(raw_points)
        error('generate_bathymetry_field:InvalidCustomPoints', ...
            'Bathymetry custom points must be provided as a struct array.');
    end

    points = struct('enabled', {}, 'x', {}, 'y', {}, 'elevation', {});
    for idx = 1:numel(raw_points)
        entry = raw_points(idx);
        enabled = true;
        if isfield(entry, 'enabled') && ~isempty(entry.enabled)
            enabled = logical(entry.enabled);
        end
        if ~enabled
            continue;
        end
        if ~(isfield(entry, 'x') && isfield(entry, 'y') && isfield(entry, 'elevation'))
            error('generate_bathymetry_field:InvalidCustomPoints', ...
                'Each bathymetry control point must contain enabled/x/y/elevation fields.');
        end
        point = struct();
        point.enabled = true;
        point.x = min(max(double(entry.x), x_min), x_max);
        point.y = min(max(double(entry.y), y_min), y_max);
        point.elevation = min(max(double(entry.elevation), y_floor), y_ceiling);
        if ~(isfinite(point.x) && isfinite(point.y) && isfinite(point.elevation))
            error('generate_bathymetry_field:InvalidCustomPoints', ...
                'Bathymetry control points must be finite after clamping.');
        end
        points(end + 1) = point; %#ok<AGROW>
    end

    if numel(points) < 3
        error('generate_bathymetry_field:InsufficientCustomPoints', ...
            'custom_points_2d requires at least three enabled control points.');
    end
end

function [x_norm, depth_norm] = load_tohoku_profile_data()
    data_path = fullfile(fileparts(mfilename('fullpath')), 'Data', 'tohoku_profile_2d.csv');
    if exist(data_path, 'file') ~= 2
        error('generate_bathymetry_field:MissingTohokuProfileData', ...
            'Missing checked-in Tohoku profile data at %s.', data_path);
    end
    data = readmatrix(data_path);
    if ~isnumeric(data) || size(data, 2) < 2
        error('generate_bathymetry_field:InvalidTohokuProfileData', ...
            'Tohoku profile data must contain at least two numeric columns.');
    end
    x_norm = double(data(:, 1));
    depth_norm = double(data(:, 2));
    finite_mask = isfinite(x_norm) & isfinite(depth_norm);
    x_norm = x_norm(finite_mask);
    depth_norm = depth_norm(finite_mask);
    if numel(x_norm) < 4
        error('generate_bathymetry_field:InsufficientTohokuProfileData', ...
            'Tohoku profile data must contain at least four finite sample points.');
    end
    x_norm = min(max(x_norm(:).', 0.0), 1.0);
    depth_norm = max(depth_norm(:).', 0.0);
    [x_norm, unique_idx] = unique(x_norm, 'stable');
    depth_norm = depth_norm(unique_idx);
end

function [x_out, profile_out] = reshape_profile_with_crop(x_in, profile_in, crop_center, crop_width, stretch)
    half_width = 0.5 * crop_width;
    left_edge = max(0.0, crop_center - half_width);
    right_edge = min(1.0, crop_center + half_width);
    if right_edge - left_edge < 0.05
        left_edge = max(0.0, crop_center - 0.025);
        right_edge = min(1.0, crop_center + 0.025);
    end
    x_shifted = 0.5 + (x_in - crop_center) ./ max(stretch, eps);
    keep = x_shifted >= left_edge & x_shifted <= right_edge;
    if nnz(keep) < 4
        keep = true(size(x_shifted));
    end
    x_crop = x_shifted(keep);
    profile_crop = profile_in(keep);
    x_crop = (x_crop - min(x_crop)) ./ max(max(x_crop) - min(x_crop), eps);
    x_out = linspace(0.0, 1.0, max(numel(x_crop), 32));
    profile_out = interp1(x_crop, profile_crop, x_out, 'pchip', 'extrap');
end

function profile = smooth_profile(profile_in, smoothing_strength)
    profile = reshape(double(profile_in), 1, []);
    window = max(1, 2 * round(double(smoothing_strength)) + 1);
    if window <= 1 || numel(profile) <= 2
        return;
    end
    kernel = ones(1, window) ./ window;
    pad = floor(window / 2);
    padded = [repmat(profile(1), 1, pad), profile, repmat(profile(end), 1, pad)];
    profile = conv(padded, kernel, 'valid');
end
