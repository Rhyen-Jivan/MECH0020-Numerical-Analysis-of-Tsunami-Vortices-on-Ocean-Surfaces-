classdef ICDispatcher
% ICDISPATCHER  Canonical initial-condition source for all methods.
%
% Usage:
%   omega = ICDispatcher.resolve(X, Y, Parameters)
%   omega = ICDispatcher.resolve(X, Y, Parameters, method)
%
% Notes:
%   - Centralizes IC catalog formulas so methods never implement IC logic.
%   - Legacy wrappers (initialise_omega.m / ic_factory.m) should forward here.

    methods(Static)

        function omega = resolve(X, Y, Parameters, varargin)
            method_name = 'fd';
            if ~isempty(varargin)
                method_name = lower(char(string(varargin{1}))); %#ok<NASGU>
            end

            ic_type = ICDispatcher.extract_ic_type(Parameters);
            multi_rows = ICDispatcher.extract_multi_vortex_rows(Parameters);
            if ICDispatcher.should_use_multi_vortex_rows(multi_rows, Parameters, ic_type)
                omega = ICDispatcher.evaluate_row_catalog(X, Y, multi_rows, Parameters, ic_type);
                return;
            end
            ic_coeff = ICDispatcher.extract_ic_coeff(Parameters, ic_type);
            omega = ICDispatcher.evaluate_catalog(X, Y, ic_type, ic_coeff, Parameters);
        end

        function ic_type = extract_ic_type(Parameters)
            if isfield(Parameters, 'ic_type') && ~isempty(Parameters.ic_type)
                ic_type = char(string(Parameters.ic_type));
            else
                ic_type = 'stretched_gaussian';
            end
        end

        function ic_coeff = extract_ic_coeff(Parameters, ic_type)
            if nargin < 2 || isempty(ic_type)
                ic_type = ICDispatcher.extract_ic_type(Parameters);
            end

            ic_coeff = ICDispatcher.extract_dynamic_ic_coeff(Parameters, ic_type);
            if ~isempty(ic_coeff)
                return;
            end

            if isfield(Parameters, 'ic_coeff') && ~isempty(Parameters.ic_coeff)
                ic_coeff = double(Parameters.ic_coeff(:).');
            else
                ic_coeff = [];
            end
        end

        function rows = extract_multi_vortex_rows(Parameters)
            rows = struct([]);
            if isfield(Parameters, 'ic_multi_vortex_rows') && ~isempty(Parameters.ic_multi_vortex_rows)
                rows = Parameters.ic_multi_vortex_rows;
            end
        end

        function omega = evaluate_catalog(X, Y, ic_type_raw, ic_coeff, Parameters)
            ic_type = lower(char(string(ic_type_raw)));
            ic_type = strrep(ic_type, '-', '_');
            ic_type = strrep(ic_type, ' ', '_');

            if nargin < 5 || ~isstruct(Parameters)
                Parameters = struct();
            end

            if ~isnumeric(ic_coeff) || isempty(ic_coeff)
                ic_coeff = [];
            end

            params = ICDispatcher.ic_coeff_to_params(ic_type, ic_coeff);

            switch ic_type
                case 'lamb_oseen'
                    Gamma = params.circulation;
                    nu = params.nu;
                    t0 = max(params.virtual_time, 1.0e-6);
                    x0 = params.center_x;
                    y0 = params.center_y;
                    R2 = (X - x0).^2 + (Y - y0).^2;
                    omega = (Gamma / (4 * pi * nu * t0)) * exp(-R2 / (4 * nu * t0));

                case 'rankine'
                    omega0 = params.core_vorticity;
                    rc = params.core_radius;
                    x0 = params.center_x;
                    y0 = params.center_y;
                    R = sqrt((X - x0).^2 + (Y - y0).^2);
                    omega = zeros(size(X));
                    omega(R <= rc) = omega0;

                case 'lamb_dipole'
                    U = params.translation_speed;
                    a = max(params.dipole_radius, 1.0e-6);
                    x0 = params.center_x;
                    y0 = params.center_y;
                    r1_2 = (X - x0 + a/2).^2 + (Y - y0).^2;
                    r2_2 = (X - x0 - a/2).^2 + (Y - y0).^2;
                    a2 = a^2;
                    omega = (U/(pi*a2)) * (exp(-r1_2/a2) - exp(-r2_2/a2));

                case 'taylor_green'
                    mode_count = params.mode_count;
                    G = params.strength;
                    x0 = params.center_x;
                    y0 = params.center_y;
                    Lx = ICDispatcher.resolve_domain_length(Parameters, X, 'Lx');
                    Ly = ICDispatcher.resolve_domain_length(Parameters, Y, 'Ly');
                    kx = 2 * pi * mode_count / max(Lx, eps);
                    ky = 2 * pi * mode_count / max(Ly, eps);
                    omega = G * (kx + ky) .* ...
                        sin(kx * (X - x0)) .* ...
                        sin(ky * (Y - y0));

                case 'random_turbulence'
                    alpha = params.spectrum_exp;
                    E0 = params.energy_level;
                    seed = params.seed;
                    rng(seed);
                    kmax = 4;
                    omega = zeros(size(X));
                    for k = 1:kmax
                        omega = omega + (E0 / k^(alpha/2)) * sin(k * X) .* cos(k * Y);
                    end

                case 'elliptical_vortex'
                    w0 = params.peak_vorticity;
                    sx = max(params.width_x, 1.0e-6);
                    sy = max(params.width_y, 1.0e-6);
                    theta = params.rotation_angle;
                    x0 = params.center_x;
                    y0 = params.center_y;
                    Xc = X - x0;
                    Yc = Y - y0;
                    xr = cos(theta) * Xc + sin(theta) * Yc;
                    yr = -sin(theta) * Xc + cos(theta) * Yc;
                    omega = w0 * exp(-(xr.^2 / (2*sx^2) + yr.^2 / (2*sy^2)));

                case 'no_initial_condition'
                    omega = zeros(size(X));

                case {'stretched_gaussian', 'gaussian'}
                    if isempty(ic_coeff) || numel(ic_coeff) < 2
                        x_coeff = -1.0;
                        y_coeff = -1.0;
                    else
                        x_coeff = -ic_coeff(1);
                        y_coeff = -ic_coeff(2);
                    end
                    amplitude = 1.0;
                    if numel(ic_coeff) >= 3 && isfinite(ic_coeff(3))
                        amplitude = ic_coeff(3);
                    end
                    x0 = 0;
                    y0 = 0;
                    if numel(ic_coeff) >= 6
                        x0 = ic_coeff(5);
                        y0 = ic_coeff(6);
                    end
                    omega = amplitude .* exp(x_coeff*(X-x0).^2 + y_coeff*(Y-y0).^2);

                case 'vortex_blob_gaussian'
                    if numel(ic_coeff) < 4
                        error('ICDispatcher:InvalidCoeff', ...
                            'vortex_blob_gaussian requires [Circulation, Radius, x0, y0].');
                    end
                    circulation = ic_coeff(1);
                    radius = max(ic_coeff(2), 1e-6);
                    x0 = ic_coeff(3);
                    y0 = ic_coeff(4);
                    omega = circulation/(2 * pi * radius^2) * exp(-((X-x0).^2 + (Y-y0).^2)/(2*radius^2));

                case 'vortex_pair'
                    if numel(ic_coeff) < 6
                        error('ICDispatcher:InvalidCoeff', ...
                            'vortex_pair requires [Gamma1, R1, x1, y1, Gamma2, x2].');
                    end
                    Gamma1 = ic_coeff(1);
                    R1 = max(ic_coeff(2), 1e-6);
                    x1 = ic_coeff(3);
                    y1 = ic_coeff(4);
                    Gamma2 = ic_coeff(5);
                    x2 = ic_coeff(6);
                    y2 = 10 - y1;
                    vort1 = Gamma1/(2*pi*R1^2) * exp(-((X-x1).^2 + (Y-y1).^2)/(2*R1^2));
                    vort2 = Gamma2/(2*pi*R1^2) * exp(-((X-x2).^2 + (Y-y2).^2)/(2*R1^2));
                    omega = vort1 + vort2;

                case 'multi_vortex'
                    n_coeff = numel(ic_coeff);
                    if n_coeff < 4
                        error('ICDispatcher:InvalidCoeff', ...
                            'multi_vortex requires at least one [G, R, x, y] tuple.');
                    end
                    omega = zeros(size(X));
                    n_vort = floor(n_coeff / 4);
                    for vi = 1:n_vort
                        idx = (vi - 1) * 4;
                        Gi = ic_coeff(idx + 1);
                        Ri = max(ic_coeff(idx + 2), 1e-6);
                        xi = ic_coeff(idx + 3);
                        yi = ic_coeff(idx + 4);
                        omega = omega + Gi/(2*pi*Ri^2) * exp(-((X-xi).^2 + (Y-yi).^2)/(2*Ri^2));
                    end

                case 'counter_rotating_pair'
                    if numel(ic_coeff) < 8
                        error('ICDispatcher:InvalidCoeff', ...
                            'counter_rotating_pair requires [G1,R1,x1,y1,G2,R2,x2,y2].');
                    end
                    G1 = ic_coeff(1); R1 = max(ic_coeff(2), 1e-6); x1 = ic_coeff(3); y1 = ic_coeff(4);
                    G2 = ic_coeff(5); R2 = max(ic_coeff(6), 1e-6); x2 = ic_coeff(7); y2 = ic_coeff(8);
                    vort1 = G1/(2*pi*R1^2) * exp(-((X-x1).^2 + (Y-y1).^2)/(2*R1^2));
                    vort2 = G2/(2*pi*R2^2) * exp(-((X-x2).^2 + (Y-y2).^2)/(2*R2^2));
                    omega = vort1 + vort2;

                case 'placeholder2'
                    omega = zeros(size(X));

                case 'kutz'
                    omega = sin(X) .* cos(Y);

                otherwise
                    error('ICDispatcher:UnknownType', 'Unknown ic_type: %s', ic_type);
            end
        end

    end

    methods(Static, Access = private)

        function tf = should_use_multi_vortex_rows(rows, Parameters, ~)
            tf = false;
            if isempty(rows) || ~isstruct(rows)
                return;
            end
            is_experimental = false;
            if isfield(Parameters, 'ic_multi_vortex_experimental')
                is_experimental = logical(Parameters.ic_multi_vortex_experimental);
            end
            tf = is_experimental;
        end

        function omega = evaluate_row_catalog(X, Y, rows, Parameters, fallback_ic_type)
            rows = ICDispatcher.normalize_rows(rows, Parameters, fallback_ic_type);
            omega = zeros(size(X));
            if isempty(rows)
                return;
            end
            enabled_mask = [rows.enabled];
            if ~any(enabled_mask)
                enabled_mask = true(size(enabled_mask));
            end
            rows = rows(enabled_mask);
            for i = 1:numel(rows)
                coeff = ICDispatcher.row_to_coeff(rows(i), Parameters);
                omega = omega + ICDispatcher.evaluate_catalog(X, Y, rows(i).ic_type, coeff, Parameters);
            end
        end

        function rows = normalize_rows(rows, ~, fallback_ic_type)
            if nargin < 3 || isempty(fallback_ic_type)
                fallback_ic_type = 'stretched_gaussian';
            end
            if isempty(rows) || ~isstruct(rows)
                rows = struct([]);
                return;
            end
            for i = 1:numel(rows)
                rows(i).row_id = ICDispatcher.pick_row_field(rows(i), 'row_id', i);
                rows(i).enabled = logical(ICDispatcher.pick_row_field(rows(i), 'enabled', true));
                rows(i).ic_type = ICDispatcher.normalize_ic_type(ICDispatcher.pick_row_field(rows(i), 'ic_type', fallback_ic_type));
                rows(i).center_x = double(ICDispatcher.pick_row_field(rows(i), 'center_x', 0.0));
                rows(i).center_y = double(ICDispatcher.pick_row_field(rows(i), 'center_y', 0.0));
                rows(i).scale = max(1.0e-6, double(ICDispatcher.pick_row_field(rows(i), 'scale', 1.0)));
                rows(i).amplitude = max(1.0e-6, double(ICDispatcher.pick_row_field(rows(i), 'amplitude', 1.0)));
                rows(i).charge = double(ICDispatcher.pick_row_field(rows(i), 'charge', 1.0));
                rows(i).dynamic_values = ICDispatcher.normalize_row_dynamic_values( ...
                    ICDispatcher.pick_row_field(rows(i), 'dynamic_values', struct()), rows(i).ic_type);
                rows(i).preset_tag = char(string(ICDispatcher.pick_row_field(rows(i), 'preset_tag', 'Manual')));
            end
        end

        function coeff = row_to_coeff(row, Parameters)
            ic_type = ICDispatcher.normalize_ic_type(row.ic_type);
            dyn = row.dynamic_values;
            x0 = row.center_x;
            y0 = row.center_y;
            scale = row.scale;
            amp = row.amplitude;
            charge = row.charge;
            Lx = ICDispatcher.pick_parameter(Parameters, 'Lx', 10.0);
            Ly = ICDispatcher.pick_parameter(Parameters, 'Ly', 10.0);
            switch ic_type
                case {'stretched_gaussian', 'gaussian'}
                    a = ICDispatcher.pick_dyn(dyn, 'stretch_x', 2.15);
                    b = ICDispatcher.pick_dyn(dyn, 'stretch_y', 0.18);
                    coeff = [max(a, 1.0e-8), max(b, 1.0e-8), max(scale, 1.0e-8) * amp * charge, 0, x0, y0];
                case 'vortex_blob_gaussian'
                    gamma = ICDispatcher.pick_dyn(dyn, 'circulation', 1.0) * scale * charge;
                    radius = ICDispatcher.pick_dyn(dyn, 'radius', 0.35);
                    coeff = [gamma, max(radius, 1.0e-6), x0, y0, x0, y0];
                case 'vortex_pair'
                    gamma1 = ICDispatcher.pick_dyn(dyn, 'gamma1', 1.0) * scale * charge;
                    sep = max(ICDispatcher.pick_dyn(dyn, 'separation', 2.0), 1.0e-6);
                    rad = max(ICDispatcher.pick_dyn(dyn, 'core_radius', 0.4), 1.0e-6);
                    gamma2 = -abs(gamma1);
                    coeff = [gamma1, rad, x0 - sep / 2, y0, gamma2, x0 + sep / 2];
                case 'multi_vortex'
                    n_vort = max(1, round(ICDispatcher.pick_parameter(Parameters, 'ic_count', 1)));
                    pattern = char(string(ICDispatcher.pick_parameter(Parameters, 'ic_arrangement', 'single')));
                    [centers_x, centers_y] = disperse_vortices(n_vort, pattern, Lx, Ly);
                    centers_x = centers_x(:) + x0;
                    centers_y = centers_y(:) + y0;
                    gamma = ICDispatcher.pick_dyn(dyn, 'gamma', 1.0) * scale * charge;
                    radius = max(ICDispatcher.pick_dyn(dyn, 'core_radius', 0.3), 1.0e-6);
                    coeff = zeros(1, 4 * n_vort);
                    for vi = 1:n_vort
                        idx = (vi - 1) * 4;
                        coeff(idx + 1) = gamma;
                        coeff(idx + 2) = radius;
                        coeff(idx + 3) = centers_x(vi);
                        coeff(idx + 4) = centers_y(vi);
                    end
                case 'lamb_oseen'
                    gamma = ICDispatcher.pick_dyn(dyn, 'circulation', 1.0) * scale * charge;
                    t0 = ICDispatcher.pick_dyn(dyn, 'virtual_time', 100.0);
                    nu = ICDispatcher.pick_dyn(dyn, 'nu', 1.0e-3);
                    coeff = [gamma, max(t0, 1.0e-6), max(nu, 1.0e-8), 0, x0, y0];
                case 'rankine'
                    omega0 = ICDispatcher.pick_dyn(dyn, 'core_vorticity', 1.0) * scale * charge;
                    rc = ICDispatcher.pick_dyn(dyn, 'core_radius', 1.0);
                    coeff = [omega0, max(rc, 1.0e-6), 0, 0, x0, y0];
                case 'lamb_dipole'
                    U = ICDispatcher.pick_dyn(dyn, 'translation_speed', 0.5) * scale;
                    a = ICDispatcher.pick_dyn(dyn, 'dipole_radius', 1.0);
                    coeff = [U, max(a, 1.0e-6), 0, 0, x0, y0];
                case 'taylor_green'
                    k = max(1, round(ICDispatcher.pick_dyn(dyn, 'wavenumber', 2.0)));
                    G = ICDispatcher.pick_dyn(dyn, 'strength', 2.0) * scale;
                    coeff = [k, G, 0, 0, x0, y0];
                case 'random_turbulence'
                    alpha = ICDispatcher.pick_dyn(dyn, 'spectrum_exponent', 5/3);
                    e0 = ICDispatcher.pick_dyn(dyn, 'energy_level', 1.0) * scale;
                    seed = round(ICDispatcher.pick_dyn(dyn, 'seed', 0.0));
                    coeff = [max(alpha, 0.1), e0, seed, 0, 0, 0];
                case 'elliptical_vortex'
                    w0 = ICDispatcher.pick_dyn(dyn, 'peak_vorticity', 1.0) * scale * charge;
                    sx = ICDispatcher.pick_dyn(dyn, 'sigma_x', 1.0 / sqrt(2.0 * 2.15));
                    sy = ICDispatcher.pick_dyn(dyn, 'sigma_y', 1.0 / sqrt(2.0 * 0.18));
                    theta = ICDispatcher.pick_dyn(dyn, 'rotation_theta', 0.0);
                    coeff = [w0, max(sx, 1.0e-6), max(sy, 1.0e-6), theta, x0, y0];
                otherwise
                    error('ICDispatcher:UnknownRowType', ...
                        'Unknown ic_type in structured row payload: %s', ic_type);
            end
        end

        function coeff = extract_dynamic_ic_coeff(Parameters, ic_type)
            coeff = [];
            if ~isstruct(Parameters) || ...
                    ~isfield(Parameters, 'ic_dynamic_values') || ...
                    ~isstruct(Parameters.ic_dynamic_values) || ...
                    isempty(fieldnames(Parameters.ic_dynamic_values))
                return;
            end

            row = struct( ...
                'ic_type', ICDispatcher.normalize_ic_type(ic_type), ...
                'center_x', double(ICDispatcher.pick_parameter(Parameters, 'ic_center_x', 0.0)), ...
                'center_y', double(ICDispatcher.pick_parameter(Parameters, 'ic_center_y', 0.0)), ...
                'scale', max(1.0e-6, double(ICDispatcher.pick_parameter(Parameters, 'ic_scale', 1.0))), ...
                'amplitude', max(1.0e-6, double(ICDispatcher.pick_parameter(Parameters, 'ic_amplitude', 1.0))), ...
                'charge', 1.0, ...
                'dynamic_values', Parameters.ic_dynamic_values);
            row.dynamic_values = ICDispatcher.normalize_row_dynamic_values(row.dynamic_values, row.ic_type);
            coeff = ICDispatcher.row_to_coeff(row, Parameters);
        end

        function dyn = normalize_row_dynamic_values(dyn, ic_type)
            defaults = ICDispatcher.default_dynamic_values(ic_type);
            if ~isstruct(dyn)
                dyn = defaults;
                return;
            end
            names = fieldnames(defaults);
            for i = 1:numel(names)
                key = names{i};
                if ~isfield(dyn, key) || isempty(dyn.(key))
                    dyn.(key) = defaults.(key);
                end
            end
        end

        function defaults = default_dynamic_values(ic_type)
            switch ICDispatcher.normalize_ic_type(ic_type)
                case 'no_initial_condition'
                    defaults = struct();
                case 'stretched_gaussian'
                    defaults = struct('stretch_x', 2.15, 'stretch_y', 0.18);
                case 'vortex_blob_gaussian'
                    defaults = struct('circulation', 1.0, 'radius', 0.35);
                case 'vortex_pair'
                    defaults = struct('gamma1', 1.0, 'separation', 2.0, 'core_radius', 0.4);
                case 'multi_vortex'
                    defaults = struct('gamma', 1.0, 'core_radius', 0.3);
                case 'lamb_oseen'
                    defaults = struct('circulation', 1.0, 'virtual_time', 100.0, 'nu', 1.0e-3);
                case 'rankine'
                    defaults = struct('core_vorticity', 1.0, 'core_radius', 1.0);
                case 'lamb_dipole'
                    defaults = struct('translation_speed', 0.5, 'dipole_radius', 1.0);
                case 'taylor_green'
                    defaults = struct('wavenumber', 2.0, 'strength', 2.0);
                case 'random_turbulence'
                    defaults = struct('spectrum_exponent', 5/3, 'energy_level', 1.0, 'seed', 0.0);
                case 'elliptical_vortex'
                    defaults = struct('peak_vorticity', 1.0, 'sigma_x', 1.0 / sqrt(2.0 * 2.15), 'sigma_y', 1.0 / sqrt(2.0 * 0.18), 'rotation_theta', 0.0);
                otherwise
                    error('ICDispatcher:UnknownDynamicDefaultsType', ...
                        'No dynamic default schema exists for ic_type: %s', ic_type);
            end
        end

        function value = pick_row_field(row, field_name, fallback)
            value = fallback;
            if isstruct(row) && isfield(row, field_name)
                candidate = row.(field_name);
                if ~isempty(candidate)
                    value = candidate;
                end
            end
        end

        function value = pick_parameter(Parameters, field_name, fallback)
            value = fallback;
            if isstruct(Parameters) && isfield(Parameters, field_name) && ~isempty(Parameters.(field_name))
                value = Parameters.(field_name);
            end
        end

        function value = pick_dyn(dyn, field_name, fallback)
            value = fallback;
            if isstruct(dyn) && isfield(dyn, field_name) && ~isempty(dyn.(field_name))
                value = double(dyn.(field_name));
            end
        end

        function ic_type = normalize_ic_type(ic_type_raw)
            ic_type = lower(char(string(ic_type_raw)));
            ic_type = strrep(ic_type, '-', '_');
            ic_type = strrep(ic_type, ' ', '_');
            switch ic_type
                case 'vortex_blob'
                    ic_type = 'vortex_blob_gaussian';
                case 'no_initial_condition'
                    ic_type = 'no_initial_condition';
            end
        end

        function params = ic_coeff_to_params(ic_type, ic_coeff)
            params = struct();

            switch ic_type
                case 'lamb_oseen'
                    params.circulation = ICDispatcher.get_coeff(ic_coeff, 1, 1.0);
                    params.virtual_time = ICDispatcher.get_coeff(ic_coeff, 2, 100.0);
                    params.nu = ICDispatcher.get_coeff(ic_coeff, 3, 0.001);

                case 'rankine'
                    params.core_vorticity = ICDispatcher.get_coeff(ic_coeff, 1, 1.0);
                    params.core_radius = ICDispatcher.get_coeff(ic_coeff, 2, 1.0);

                case 'lamb_dipole'
                    params.translation_speed = ICDispatcher.get_coeff(ic_coeff, 1, 0.5);
                    params.dipole_radius = ICDispatcher.get_coeff(ic_coeff, 2, 1.0);

                case 'taylor_green'
                    params.mode_count = max(1, round(ICDispatcher.get_coeff(ic_coeff, 1, 2.0)));
                    params.wavenumber = params.mode_count;
                    params.strength = ICDispatcher.get_coeff(ic_coeff, 2, 2.0);

                case 'random_turbulence'
                    params.spectrum_exp = ICDispatcher.get_coeff(ic_coeff, 1, 5/3);
                    params.energy_level = ICDispatcher.get_coeff(ic_coeff, 2, 1.0);
                    params.seed = ICDispatcher.get_coeff(ic_coeff, 3, 0);

                case 'elliptical_vortex'
                    params.peak_vorticity = ICDispatcher.get_coeff(ic_coeff, 1, 1.0);
                    params.width_x = ICDispatcher.get_coeff(ic_coeff, 2, 1.0 / sqrt(2.0 * 2.15));
                    params.width_y = ICDispatcher.get_coeff(ic_coeff, 3, 1.0 / sqrt(2.0 * 0.18));
                    params.rotation_angle = ICDispatcher.get_coeff(ic_coeff, 4, 0.0);
            end

            if ~isempty(fieldnames(params))
                params.center_x = ICDispatcher.get_coeff(ic_coeff, 5, 0.0);
                params.center_y = ICDispatcher.get_coeff(ic_coeff, 6, 0.0);
            end
        end

        function val = get_coeff(ic_coeff, index, default)
            if numel(ic_coeff) >= index
                val = ic_coeff(index);
            else
                val = default;
            end
        end

        function L = resolve_domain_length(Parameters, axis_grid, field_name)
            L = ICDispatcher.pick_parameter(Parameters, field_name, NaN);
            if ~(isnumeric(L) && isscalar(L) && isfinite(L) && L > 0)
                axis_values = double(axis_grid(:));
                axis_values = axis_values(isfinite(axis_values));
                if isempty(axis_values)
                    L = 1.0;
                    return;
                end
                L = max(axis_values) - min(axis_values);
                if ~(isfinite(L) && L > 0)
                    L = 1.0;
                end
            end
        end

    end
end
