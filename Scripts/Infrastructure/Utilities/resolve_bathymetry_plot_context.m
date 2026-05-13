function plot_context = resolve_bathymetry_plot_context(analysis, ny, nx)
% resolve_bathymetry_plot_context Normalize bathymetry masks/outlines for results plotting.

    plot_context = struct( ...
        'plot_mask', [], ...
        'fluid_mask', [], ...
        'wall_mask', [], ...
        'solid_mask', [], ...
        'profile_x', [], ...
        'profile_y', [], ...
        'is_fd_bathymetry_2d', false, ...
        'is_fd_wall_domain_2d', false, ...
        'requires_velocity_snapshots', false);

    if nargin < 3 || ~isstruct(analysis) || ...
            ~(isnumeric(ny) && isnumeric(nx) && isfinite(ny) && isfinite(nx))
        return;
    end

    ny = round(double(ny));
    nx = round(double(nx));

    fluid_mask = extract_mask_2d(analysis, 'bathymetry_fluid_mask_2d', ny, nx);
    wall_mask = extract_mask_2d(analysis, 'bathymetry_wall_mask_2d', ny, nx);
    solid_mask = extract_mask_2d(analysis, 'bathymetry_solid_mask_2d', ny, nx);
    wet_mask = extract_mask_2d(analysis, 'bathymetry_wet_mask_2d', ny, nx);
    fd_fluid_mask = extract_mask_2d(analysis, 'fd_fluid_mask_2d', ny, nx);
    fd_wall_mask = extract_mask_2d(analysis, 'fd_wall_mask_2d', ny, nx);

    if isempty(fluid_mask) && ~isempty(fd_fluid_mask)
        fluid_mask = fd_fluid_mask;
    end
    if isempty(wall_mask) && ~isempty(fd_wall_mask)
        wall_mask = fd_wall_mask;
    end

    if isempty(fluid_mask) && ~isempty(wet_mask) && ~isempty(wall_mask)
        fluid_mask = wet_mask & ~wall_mask;
    end
    if isempty(fluid_mask) && ~isempty(wet_mask)
        fluid_mask = wet_mask;
    end
    if isempty(solid_mask) && ~isempty(fluid_mask)
        solid_mask = ~fluid_mask;
    end

    if isfield(analysis, 'bathymetry_wet_mask_3d') && ~isempty(analysis.bathymetry_wet_mask_3d)
        candidate = logical(analysis.bathymetry_wet_mask_3d);
        if ndims(candidate) == 3 && size(candidate, 1) == ny && size(candidate, 2) == nx
            fluid_mask = any(candidate, 3);
            plot_context.plot_mask = fluid_mask;
        end
    end

    if isfield(analysis, 'bathymetry_profile_x') && isfield(analysis, 'bathymetry_profile_2d')
        profile_x = double(analysis.bathymetry_profile_x(:));
        profile_y = double(analysis.bathymetry_profile_2d(:));
        if numel(profile_x) == numel(profile_y) && numel(profile_x) >= 2
            plot_context.profile_x = profile_x;
            plot_context.profile_y = profile_y;
        end
    end

    plot_context.is_fd_bathymetry_2d = ~isempty(plot_context.profile_x) && ...
        ~isempty(fluid_mask) && (~isempty(wall_mask) || ~isempty(wet_mask));
    plot_context.is_fd_wall_domain_2d = ~plot_context.is_fd_bathymetry_2d && ...
        ~isempty(fd_wall_mask) && any(fd_wall_mask(:)) && ~isempty(fd_fluid_mask);

    if isempty(plot_context.plot_mask)
        plot_context.plot_mask = fluid_mask;
    end
    if plot_context.is_fd_wall_domain_2d
        plot_context.plot_mask = erode_fd_wall_display_mask(plot_context.plot_mask);
    end
    plot_context.fluid_mask = fluid_mask;
    plot_context.wall_mask = wall_mask;
    plot_context.solid_mask = solid_mask;
    plot_context.requires_velocity_snapshots = plot_context.is_fd_bathymetry_2d || ...
        plot_context.is_fd_wall_domain_2d;
end

function mask = extract_mask_2d(analysis, field_name, ny, nx)
    mask = [];
    if ~isfield(analysis, field_name) || isempty(analysis.(field_name))
        return;
    end
    candidate = logical(analysis.(field_name));
    if isequal(size(candidate), [ny, nx])
        mask = candidate;
    end
end

function cropped = erode_fd_wall_display_mask(mask)
    cropped = [];
    if isempty(mask)
        return;
    end

    mask = logical(mask);
    if size(mask, 1) < 3 || size(mask, 2) < 3
        cropped = mask;
        return;
    end

    cropped = false(size(mask));
    cropped(2:end-1, 2:end-1) = ...
        mask(2:end-1, 2:end-1) & ...
        mask(1:end-2, 2:end-1) & ...
        mask(3:end, 2:end-1) & ...
        mask(2:end-1, 1:end-2) & ...
        mask(2:end-1, 3:end);

    if ~any(cropped(:))
        cropped = mask;
    end
end
