function geometry = build_bathymetry_geometry(Parameters, X, Y, method_token, varargin)
% build_bathymetry_geometry Build active solver geometry from bathymetry scenarios.
%
% This helper turns the existing bathymetry scenario generator into
% solver-facing geometry metadata for active vorticity-streamfunction runs.
%
% Inputs:
%   Parameters   - runtime/config struct with bathymetry_* fields
%   X, Y         - horizontal meshgrid arrays
%   method_token - 'fd' or 'fv'
%   varargin     - optional struct with method-specific fields (for example
%                  Nz/Lz/dz/z for FV)
%
% Output:
%   geometry     - struct describing active wet/solid geometry, masks, and
%                  boundary metadata for solver/runtime + plotting.

    narginchk(4, 5);
    if nargin < 5 || ~isstruct(varargin{1})
        extra = struct();
    else
        extra = varargin{1};
    end

    if ~isnumeric(X) || ~isnumeric(Y) || ~isequal(size(X), size(Y))
        error('build_bathymetry_geometry:InvalidGrid', ...
            'X and Y must be numeric arrays with identical size.');
    end

    method_name = lower(strtrim(char(string(method_token))));
    method_name = strrep(method_name, '-', '_');
    method_name = strrep(method_name, ' ', '_');

    scenario = 'flat_2d';
    if any(strcmp(method_name, {'fv', 'finite_volume', 'finitevolume'}))
        scenario = 'flat_3d';
    end
    if isfield(Parameters, 'bathymetry_scenario') && ~isempty(Parameters.bathymetry_scenario)
        scenario = normalize_bathymetry_scenario_token(Parameters.bathymetry_scenario);
    end

    use_dry_mask = true;
    if isfield(Parameters, 'bathymetry_use_dry_mask') && ~isempty(Parameters.bathymetry_use_dry_mask)
        use_dry_mask = logical(Parameters.bathymetry_use_dry_mask);
    end

    bath_params = struct( ...
        'bed_slope', pick_bathymetry_param(Parameters, {'bathymetry_bed_slope', 'bed_slope'}, 0.03), ...
        'bathymetry_resolution', round(pick_bathymetry_param(Parameters, {'bathymetry_resolution'}, 96)), ...
        'z0', pick_bathymetry_param(Parameters, {'bathymetry_depth_offset', 'bathymetry_z0', 'z0'}, 1000.0), ...
        'amplitude', pick_bathymetry_param(Parameters, {'bathymetry_relief_amplitude', 'bathymetry_amplitude', 'amplitude'}, 180.0));
    if isfield(Parameters, 'bathymetry_custom_points') && ~isempty(Parameters.bathymetry_custom_points)
        bath_params.bathymetry_custom_points = Parameters.bathymetry_custom_points;
    end
    if isfield(Parameters, 'bathymetry_dynamic_params') && isstruct(Parameters.bathymetry_dynamic_params)
        bath_params.bathymetry_dynamic_params = Parameters.bathymetry_dynamic_params;
    end

    [bath_field, meta] = generate_bathymetry_field(X, Y, scenario, bath_params);

    geometry = struct();
    geometry.method = method_name;
    geometry.enabled = false;
    geometry.use_dry_mask = use_dry_mask;
    geometry.scenario = scenario;
    geometry.dimension = char(string(meta.dimension));
    geometry.is_flat = any(strcmpi(scenario, {'flat_2d', 'flat_3d'}));
    geometry.bathymetry_field = double(bath_field);
    geometry.generator_meta = meta;
    geometry.profile_x = [];
    geometry.profile_y = [];
    geometry.wet_mask = [];
    geometry.fluid_mask = [];
    geometry.wall_mask = [];
    geometry.solid_mask = [];
    geometry.boundary_mask = [];
    geometry.interior_mask = [];

    switch method_name
        case {'fd', 'finite_difference', 'finite_difference_method'}
            geometry = build_fd_geometry(geometry, meta, X, Y);
        case {'fv', 'finite_volume', 'finitevolume'}
            geometry = build_fv_geometry(geometry, meta, X, Y, extra);
        otherwise
            error('build_bathymetry_geometry:UnknownMethod', ...
                'Unsupported method token: %s', method_name);
    end

    geometry.enabled = logical(use_dry_mask && ~geometry.is_flat);
end

function geometry = build_fd_geometry(geometry, meta, X, Y)
    [Ny, Nx] = size(X);
    x_vec = double(X(1, :));
    y_vec = double(Y(:, 1));
    dx = estimate_spacing(x_vec);
    dy = estimate_spacing(y_vec);

    if isfield(meta, 'profile_y') && ~isempty(meta.profile_y)
        profile_y = reshape(double(meta.profile_y), 1, []);
    else
        mid_row = max(1, round(size(geometry.bathymetry_field, 1) / 2));
        profile_y = reshape(double(geometry.bathymetry_field(mid_row, :)), 1, []);
    end
    if numel(profile_y) ~= Nx
        profile_y = interp1(linspace(0, 1, numel(profile_y)), profile_y, linspace(0, 1, Nx), 'linear', 'extrap');
    end

    % Keep at least one active interior row above the bathymetry boundary.
    y_cap = y_vec(max(1, Ny - 1)) - 0.25 * dy;
    profile_y = min(profile_y, y_cap);

    wet_mask = Y >= repmat(profile_y, Ny, 1) - 1.0e-12;
    first_wet_row = zeros(1, Nx);
    first_fluid_row = zeros(1, Nx);
    first_fluid_valid = false(1, Nx);
    boundary_mask = false(Ny, Nx);
    wall_mask = false(Ny, Nx);
    first_fluid_distance = dy * ones(1, Nx);
    tangent_x = ones(1, Nx);
    tangent_y = zeros(1, Nx);
    drive_scale = ones(1, Nx);

    dprofile_dx = gradient(profile_y, dx);
    [bathymetry_slope_x, bathymetry_slope_y] = gradient(double(geometry.bathymetry_field), dx, dy);
    tan_norm = sqrt(1 + dprofile_dx .^ 2);
    tangent_x = 1 ./ tan_norm;
    tangent_y = dprofile_dx ./ tan_norm;
    drive_scale = tangent_x;

    for col = 1:Nx
        row = find(wet_mask(:, col), 1, 'first');
        if isempty(row)
            row = Ny;
            wet_mask(row, col) = true;
        end
        row = min(max(1, row), Ny);
        first_wet_row(col) = row;
        wall_mask(row, col) = true;
        boundary_mask(row, col) = true;
        if row < Ny
            first_fluid_row(col) = row + 1;
            first_fluid_valid(col) = true;
            first_fluid_distance(col) = max(abs(y_vec(row + 1) - profile_y(col)), 0.5 * dy);
        else
            first_fluid_row(col) = row;
            first_fluid_valid(col) = false;
            first_fluid_distance(col) = dy;
        end
    end

    % Embedded bottom-wall endpoints are wall-wall corner contacts with the
    % outer vertical boundaries, so they are excluded from the per-column
    % first-fluid closure contract.
    if Nx >= 1
        first_fluid_valid(1) = false;
        first_fluid_row(1) = first_wet_row(1);
    end
    if Nx >= 2
        first_fluid_valid(end) = false;
        first_fluid_row(end) = first_wet_row(end);
    end

    solid_mask = ~wet_mask;
    fluid_mask = wet_mask & ~wall_mask;
    boundary_mask(end, wet_mask(end, :)) = true;
    boundary_mask(wet_mask(:, 1), 1) = true;
    boundary_mask(wet_mask(:, end), end) = true;
    interior_mask = fluid_mask & ~boundary_mask;

    geometry.profile_x = x_vec;
    geometry.profile_y = profile_y;
    geometry.dx = dx;
    geometry.dy = dy;
    geometry.wet_mask = wet_mask;
    geometry.fluid_mask = fluid_mask;
    geometry.wall_mask = wall_mask;
    geometry.solid_mask = solid_mask;
    geometry.boundary_mask = boundary_mask;
    geometry.interior_mask = interior_mask;
    geometry.bottom_boundary_mask = wall_mask;
    geometry.first_wet_row = first_wet_row;
    geometry.first_fluid_row = first_fluid_row;
    geometry.first_fluid_valid = first_fluid_valid;
    geometry.first_fluid_distance = first_fluid_distance;
    geometry.bottom_tangent_x = tangent_x;
    geometry.bottom_tangent_y = tangent_y;
    geometry.bottom_drive_scale = drive_scale;
    geometry.bottom_drive_u = tangent_x .* drive_scale;
    geometry.bottom_drive_v = tangent_y .* drive_scale;
    geometry.cell_averaged_bathymetry = double(geometry.bathymetry_field);
    geometry.bathymetry_slope_x = bathymetry_slope_x;
    geometry.bathymetry_slope_y = bathymetry_slope_y;
end

function geometry = build_fv_geometry(geometry, ~, X, Y, extra)
    [Ny, Nx] = size(X);
    x_vec = double(X(1, :));
    y_vec = double(Y(:, 1));
    dx = estimate_spacing(x_vec);
    dy = estimate_spacing(y_vec);

    required = {'Nz', 'Lz'};
    for i = 1:numel(required)
        if ~isfield(extra, required{i})
            error('build_bathymetry_geometry:MissingFVField', ...
                'FV geometry requires extra.%s.', required{i});
        end
    end
    Nz = round(double(extra.Nz));
    Lz = double(extra.Lz);
    if Nz <= 0 || ~(isfinite(Lz) && Lz > 0)
        error('build_bathymetry_geometry:InvalidFVGrid', ...
            'FV geometry requires Nz > 0 and finite positive Lz.');
    end

    if isfield(extra, 'dz') && isfinite(extra.dz) && extra.dz > 0
        dz = double(extra.dz);
    else
        dz = Lz / Nz;
    end
    if isfield(extra, 'z') && ~isempty(extra.z)
        z_vec = reshape(double(extra.z), 1, []);
    else
        z_vec = linspace(0, Lz - dz, Nz);
    end

    bath_field = double(geometry.bathymetry_field);
    bath_min = min(bath_field(:));
    bath_max = max(bath_field(:));
    if ~isfinite(bath_min) || ~isfinite(bath_max)
        error('build_bathymetry_geometry:NonFiniteBathymetry', ...
            'Bathymetry field must be finite for FV geometry normalization.');
    end
    if abs(bath_max - bath_min) <= 1.0e-12 * max(1.0, abs(bath_max))
        floor_height = zeros(Ny, Nx);
    else
        floor_height = (bath_field - bath_min) / (bath_max - bath_min);
        floor_height = (Lz - dz) * floor_height;
    end
    floor_height = max(0.0, min(Lz - dz, floor_height));

    Z3 = reshape(z_vec, 1, 1, []);
    wet_mask = Z3 >= floor_height;
    boundary_mask = false(Ny, Nx, Nz);
    first_wet_k = ones(Ny, Nx);

    for row = 1:Ny
        for col = 1:Nx
            k = find(squeeze(wet_mask(row, col, :)), 1, 'first');
            if isempty(k)
                k = Nz;
                wet_mask(row, col, k) = true;
            end
            first_wet_k(row, col) = k;
            boundary_mask(row, col, k) = true;
        end
    end

    solid_mask = ~wet_mask;
    interior_mask = wet_mask & ~boundary_mask;

    [dfloor_dx, dfloor_dy] = gradient(floor_height, dx, dy);
    normal_x = -dfloor_dx;
    normal_y = -dfloor_dy;
    normal_z = ones(size(floor_height));
    normal_norm = sqrt(normal_x .^ 2 + normal_y .^ 2 + normal_z .^ 2);
    normal_x = normal_x ./ normal_norm;
    normal_y = normal_y ./ normal_norm;
    normal_z = normal_z ./ normal_norm;

    proj_x = 1 - normal_x .^ 2;
    proj_y = -normal_x .* normal_y;
    proj_h = hypot(proj_x, proj_y);
    proj_h(proj_h <= 1.0e-12) = 1.0;
    drive_u = proj_x ./ proj_h;
    drive_v = proj_y ./ proj_h;
    drive_scale = hypot(proj_x, proj_y);

    interface_floor_height_x = zeros(Ny, Nx + 1);
    interface_floor_height_x(:, 2:Nx) = 0.5 * (floor_height(:, 1:Nx-1) + floor_height(:, 2:Nx));
    interface_floor_height_x(:, 1) = floor_height(:, 1);
    interface_floor_height_x(:, end) = floor_height(:, end);

    interface_floor_height_y = zeros(Ny + 1, Nx);
    interface_floor_height_y(2:Ny, :) = 0.5 * (floor_height(1:Ny-1, :) + floor_height(2:Ny, :));
    interface_floor_height_y(1, :) = floor_height(1, :);
    interface_floor_height_y(end, :) = floor_height(end, :);

    geometry.dx = dx;
    geometry.dy = dy;
    geometry.dz = dz;
    geometry.Nz = Nz;
    geometry.Lz = Lz;
    geometry.z = z_vec;
    geometry.floor_height = floor_height;
    geometry.wet_mask = wet_mask;
    geometry.solid_mask = solid_mask;
    geometry.boundary_mask = boundary_mask;
    geometry.interior_mask = interior_mask;
    geometry.seabed_boundary_mask = boundary_mask;
    geometry.first_wet_k = first_wet_k;
    geometry.bottom_drive_scale = drive_scale;
    geometry.bottom_drive_u = drive_u .* drive_scale;
    geometry.bottom_drive_v = drive_v .* drive_scale;
    geometry.surface_normal_x = normal_x;
    geometry.surface_normal_y = normal_y;
    geometry.surface_normal_z = normal_z;
    geometry.cell_averaged_bathymetry = bath_field;
    geometry.bathymetry_slope_x = dfloor_dx;
    geometry.bathymetry_slope_y = dfloor_dy;
    geometry.interface_floor_height_x = interface_floor_height_x;
    geometry.interface_floor_height_y = interface_floor_height_y;
end

function value = pick_bathymetry_param(source, keys, fallback)
    value = fallback;
    for i = 1:numel(keys)
        key = keys{i};
        if isfield(source, key)
            candidate = double(source.(key));
            if isfinite(candidate)
                value = candidate;
                return;
            end
        end
    end
end

function spacing = estimate_spacing(vec)
    vec = double(vec(:));
    vec = vec(isfinite(vec));
    if numel(vec) < 2
        spacing = 1.0;
        return;
    end
    diffs = diff(vec);
    diffs = diffs(isfinite(diffs) & abs(diffs) > 0);
    if isempty(diffs)
        spacing = 1.0;
    else
        spacing = abs(median(diffs));
    end
    spacing = max(spacing, eps);
end
