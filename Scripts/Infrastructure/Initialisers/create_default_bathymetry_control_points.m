function points = create_default_bathymetry_control_points(Lx, Ly)
% create_default_bathymetry_control_points Canonical default 2D bathymetry control points.

    if nargin < 1 || ~isnumeric(Lx) || ~isscalar(Lx) || ~isfinite(Lx) || Lx <= 0
        Lx = 10.0;
    end
    if nargin < 2 || ~isnumeric(Ly) || ~isscalar(Ly) || ~isfinite(Ly) || Ly <= 0
        Ly = 10.0;
    end

    x_vals = [-0.42, -0.14, 0.10, 0.34, 0.45] * (Lx / 2);
    y_vals = [-0.32, 0.08, 0.26, -0.18, 0.34] * (Ly / 2);
    e_vals = (-Ly / 2) + [0.12, 0.22, 0.31, 0.18, 0.27] * Ly;

    points = repmat(struct('enabled', true, 'x', 0.0, 'y', 0.0, 'elevation', 0.0), 1, numel(x_vals));
    for idx = 1:numel(x_vals)
        points(idx).enabled = true;
        points(idx).x = x_vals(idx);
        points(idx).y = y_vals(idx);
        points(idx).elevation = e_vals(idx);
    end
end
