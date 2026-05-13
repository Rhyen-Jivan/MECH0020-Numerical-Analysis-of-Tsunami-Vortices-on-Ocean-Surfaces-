function [params, meta] = apply_taylor_green_timestep_cap(params, adaptive_cfg, context_label)
% apply_taylor_green_timestep_cap
% Clamp Taylor-Green workflow timesteps to conservative stability limits
% derived from the active mesh spacing, viscosity, and TG strength.

    if nargin < 1 || ~isstruct(params)
        params = struct();
    end
    if nargin < 2 || ~isstruct(adaptive_cfg)
        adaptive_cfg = struct();
    end
    if nargin < 3
        context_label = '';
    end

    meta = struct( ...
        'applied', false, ...
        'honor_fixed_dt', false, ...
        'context', char(string(context_label)), ...
        'dt_original', pick_numeric_local(params, 'dt', NaN), ...
        'dt_capped', pick_numeric_local(params, 'dt', NaN), ...
        'dt_adv_limit', NaN, ...
        'dt_diff_limit', NaN, ...
        'delta', NaN, ...
        'strength', NaN, ...
        'nu', pick_numeric_local(params, 'nu', NaN), ...
        'C_adv', pick_numeric_local(adaptive_cfg, 'C_adv', 0.5), ...
        'C_diff', pick_numeric_local(adaptive_cfg, 'C_diff', 0.25));

    ic_type = lower(strtrim(char(string(pick_text_local(params, 'ic_type', '')))));
    if ~strcmp(ic_type, 'taylor_green')
        return;
    end

    nx = max(1, round(pick_numeric_local(params, 'Nx', NaN)));
    ny = max(1, round(pick_numeric_local(params, 'Ny', NaN)));
    lx = pick_numeric_local(params, 'Lx', NaN);
    ly = pick_numeric_local(params, 'Ly', NaN);
    if ~(isfinite(nx) && isfinite(ny) && nx > 0 && ny > 0 && ...
            isfinite(lx) && lx > 0 && isfinite(ly) && ly > 0)
        return;
    end

    coeff = [];
    try
        coeff = ICDispatcher.extract_ic_coeff(params, 'taylor_green');
    catch
        coeff = [];
    end
    if isnumeric(coeff) && numel(coeff) >= 2 && isfinite(coeff(2))
        tg_strength = abs(double(coeff(2)));
    else
        tg_strength = abs(pick_numeric_local(pick_struct_local(params, 'ic_dynamic_values', struct()), 'strength', 2.0));
        tg_strength = tg_strength * max(eps, pick_numeric_local(params, 'ic_scale', 1.0));
    end
    if ~(isfinite(tg_strength) && tg_strength > eps)
        return;
    end

    c_adv = pick_numeric_local(adaptive_cfg, 'C_adv', 0.5);
    c_diff = pick_numeric_local(adaptive_cfg, 'C_diff', 0.25);
    if ~(isfinite(c_adv) && c_adv > 0)
        c_adv = 0.5;
    end
    if ~(isfinite(c_diff) && c_diff > 0)
        c_diff = 0.25;
    end

    delta = max(lx / nx, ly / ny);
    if ~(isfinite(delta) && delta > 0)
        return;
    end

    dt_adv = c_adv * delta / tg_strength;
    nu = pick_numeric_local(params, 'nu', NaN);
    if isfinite(nu) && nu > 0
        dt_diff = c_diff * delta^2 / (2 * nu);
    else
        dt_diff = inf;
    end
    dt_requested = pick_numeric_local(params, 'dt', NaN);
    if ~(isfinite(dt_requested) && dt_requested > 0)
        return;
    end

    honor_fixed_dt = pick_logical_local(params, 'taylor_green_honor_fixed_dt', false);
    meta.honor_fixed_dt = honor_fixed_dt;
    meta.dt_original = double(dt_requested);
    meta.dt_adv_limit = double(dt_adv);
    meta.dt_diff_limit = double(dt_diff);
    meta.delta = double(delta);
    meta.strength = double(tg_strength);
    meta.nu = double(nu);
    meta.C_adv = double(c_adv);
    meta.C_diff = double(c_diff);
    if honor_fixed_dt
        params.taylor_green_dt_cap_applied = false;
        params.taylor_green_dt_cap_context = char(string(context_label));
        params.taylor_green_dt_original = double(dt_requested);
        params.taylor_green_dt_adv_limit = double(dt_adv);
        params.taylor_green_dt_diff_limit = double(dt_diff);
        params.taylor_green_dt_capped = double(dt_requested);
        params.taylor_green_velocity_scale = double(tg_strength);
        meta.dt_capped = double(dt_requested);
        return;
    end

    dt_capped = min([dt_requested, dt_adv, dt_diff]);
    if ~(isfinite(dt_capped) && dt_capped > 0)
        return;
    end

    params.dt = double(dt_capped);
    params.taylor_green_dt_cap_applied = logical(dt_capped < dt_requested - 1.0e-12);
    params.taylor_green_dt_cap_context = char(string(context_label));
    params.taylor_green_dt_original = double(dt_requested);
    params.taylor_green_dt_adv_limit = double(dt_adv);
    params.taylor_green_dt_diff_limit = double(dt_diff);
    params.taylor_green_dt_capped = double(dt_capped);
    params.taylor_green_velocity_scale = double(tg_strength);

    meta.applied = logical(params.taylor_green_dt_cap_applied);
    meta.dt_capped = double(dt_capped);
    meta.dt_adv_limit = double(dt_adv);
    meta.dt_diff_limit = double(dt_diff);
    meta.delta = double(delta);
    meta.strength = double(tg_strength);
    meta.nu = double(nu);
    meta.C_adv = double(c_adv);
    meta.C_diff = double(c_diff);
end

function value = pick_numeric_local(source, field_name, fallback)
    value = fallback;
    if isstruct(source) && isfield(source, field_name)
        candidate = source.(field_name);
        if isnumeric(candidate) && isscalar(candidate) && isfinite(candidate)
            value = double(candidate);
        end
    end
end

function value = pick_text_local(source, field_name, fallback)
    value = fallback;
    if isstruct(source) && isfield(source, field_name) && ~isempty(source.(field_name))
        value = char(string(source.(field_name)));
    end
end

function value = pick_logical_local(source, field_name, fallback)
    value = fallback;
    if isstruct(source) && isfield(source, field_name) && ~isempty(source.(field_name))
        value = logical(source.(field_name));
    end
end

function value = pick_struct_local(source, field_name, fallback)
    value = fallback;
    if isstruct(source) && isfield(source, field_name) && isstruct(source.(field_name))
        value = source.(field_name);
    end
end
