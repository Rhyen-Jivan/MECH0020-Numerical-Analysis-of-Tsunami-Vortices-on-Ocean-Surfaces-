function scaling = resolve_result_plot_scaling(data, plot_context, params)
% resolve_result_plot_scaling Shared plot-mask and color-limit policy.
%
% Returns a struct with:
%   plot_mask
%   fluid_mask
%   interior_mask
%   boundary_strip_mask
%   trim_layers
%   limit_mode
%   percentile_band
%   include_boundary_diagnostics
%   cmin
%   cmax

    if nargin < 2 || isempty(plot_context)
        plot_context = struct();
    end
    if nargin < 3 || isempty(params)
        params = struct();
    end

    cfg = resolve_scaling_config(params);
    plot_mask = extract_mask(plot_context, 'plot_mask', size(data));
    fluid_mask = extract_mask(plot_context, 'fluid_mask', size(data));

    if isempty(fluid_mask)
        fluid_mask = plot_mask;
    end
    if isempty(plot_mask)
        plot_mask = fluid_mask;
    end

    if isempty(fluid_mask)
        fluid_mask = true(size(data, 1), size(data, 2));
    end
    if isempty(plot_mask)
        plot_mask = fluid_mask;
    end

    interior_mask = trim_mask_layers(fluid_mask, cfg.trim_layers);
    if ~any(interior_mask(:))
        interior_mask = fluid_mask;
    end
    if ~any(interior_mask(:))
        interior_mask = plot_mask;
    end

    boundary_strip_mask = fluid_mask & ~interior_mask;

    switch cfg.limit_mode
        case 'full_field_extrema'
            value_mask = plot_mask;
        case 'robust_percentile'
            value_mask = interior_mask;
        otherwise
            value_mask = interior_mask;
    end

    finite_vals = extract_masked_values(data, value_mask);
    if isempty(finite_vals) && ~isequal(value_mask, plot_mask)
        finite_vals = extract_masked_values(data, plot_mask);
    end
    if isempty(finite_vals)
        finite_vals = data(isfinite(data));
    end

    [cmin, cmax] = resolve_limits(finite_vals, cfg.limit_mode, cfg.percentile_band);

    scaling = struct();
    scaling.plot_mask = plot_mask;
    scaling.fluid_mask = fluid_mask;
    scaling.interior_mask = interior_mask;
    scaling.boundary_strip_mask = boundary_strip_mask;
    scaling.trim_layers = cfg.trim_layers;
    scaling.limit_mode = cfg.limit_mode;
    scaling.percentile_band = cfg.percentile_band;
    scaling.include_boundary_diagnostics = cfg.include_boundary_diagnostics;
    scaling.cmin = cmin;
    scaling.cmax = cmax;
end

function cfg = resolve_scaling_config(params)
    persistent default_cfg;
    if isempty(default_cfg)
        defaults = create_default_parameters();
        default_cfg = struct( ...
            'trim_layers', double(defaults.plot_trim_layers), ...
            'limit_mode', char(string(defaults.plot_limit_mode)), ...
            'percentile_band', double(defaults.plot_percentile_band(:).'), ...
            'include_boundary_diagnostics', logical(defaults.plot_include_boundary_diagnostics));
    end

    cfg = default_cfg;
    if isfield(params, 'results_plot_scaling') && isstruct(params.results_plot_scaling)
        source = params.results_plot_scaling;
    else
        source = params;
    end

    if isfield(source, 'trim_layers') && isnumeric(source.trim_layers) && isscalar(source.trim_layers)
        cfg.trim_layers = max(0, round(double(source.trim_layers)));
    elseif isfield(source, 'plot_trim_layers') && isnumeric(source.plot_trim_layers) && isscalar(source.plot_trim_layers)
        cfg.trim_layers = max(0, round(double(source.plot_trim_layers)));
    end

    if isfield(source, 'limit_mode') && ~isempty(source.limit_mode)
        cfg.limit_mode = lower(char(string(source.limit_mode)));
    elseif isfield(source, 'plot_limit_mode') && ~isempty(source.plot_limit_mode)
        cfg.limit_mode = lower(char(string(source.plot_limit_mode)));
    end

    if isfield(source, 'percentile_band') && isnumeric(source.percentile_band) && numel(source.percentile_band) == 2
        cfg.percentile_band = sort(double(source.percentile_band(:).'));
    elseif isfield(source, 'plot_percentile_band') && isnumeric(source.plot_percentile_band) && numel(source.plot_percentile_band) == 2
        cfg.percentile_band = sort(double(source.plot_percentile_band(:).'));
    end

    if isfield(source, 'include_boundary_diagnostics')
        cfg.include_boundary_diagnostics = logical(source.include_boundary_diagnostics);
    elseif isfield(source, 'plot_include_boundary_diagnostics')
        cfg.include_boundary_diagnostics = logical(source.plot_include_boundary_diagnostics);
    end

    cfg.percentile_band(1) = max(0.0, min(100.0, cfg.percentile_band(1)));
    cfg.percentile_band(2) = max(cfg.percentile_band(1), min(100.0, cfg.percentile_band(2)));
    if ~any(strcmp(cfg.limit_mode, {'trimmed_interior_extrema', 'robust_percentile', 'full_field_extrema'}))
        cfg.limit_mode = default_cfg.limit_mode;
    end
end

function mask = extract_mask(plot_context, field_name, data_size)
    mask = [];
    if islogical(plot_context)
        candidate = plot_context;
    elseif isstruct(plot_context) && isfield(plot_context, field_name)
        candidate = plot_context.(field_name);
    else
        return;
    end

    if isempty(candidate)
        return;
    end
    candidate = logical(candidate);

    if isequal(size(candidate), data_size)
        mask = candidate;
    elseif ismatrix(candidate) && numel(data_size) == 3 && ...
            size(candidate, 1) == data_size(1) && size(candidate, 2) == data_size(2)
        mask = candidate;
    end
end

function mask = trim_mask_layers(mask, trim_layers)
    mask = logical(mask);
    trim_layers = max(0, round(double(trim_layers)));
    for layer = 1:trim_layers
        if size(mask, 1) < 3 || size(mask, 2) < 3
            break;
        end
        eroded = false(size(mask));
        eroded(2:end-1, 2:end-1) = ...
            mask(2:end-1, 2:end-1) & ...
            mask(1:end-2, 2:end-1) & ...
            mask(3:end, 2:end-1) & ...
            mask(2:end-1, 1:end-2) & ...
            mask(2:end-1, 3:end);
        if ~any(eroded(:))
            break;
        end
        mask = eroded;
    end
end

function vals = extract_masked_values(data, mask)
    if isempty(mask)
        vals = data(isfinite(data));
        return;
    end

    if isequal(size(mask), size(data))
        vals = data(mask & isfinite(data));
        return;
    end

    if ismatrix(mask) && ndims(data) == 3 && size(mask, 1) == size(data, 1) && size(mask, 2) == size(data, 2)
        mask3 = repmat(mask, 1, 1, size(data, 3));
        vals = data(mask3 & isfinite(data));
        return;
    end

    vals = data(isfinite(data));
end

function [cmin, cmax] = resolve_limits(vals, limit_mode, percentile_band)
    if isempty(vals)
        cmin = -1;
        cmax = 1;
        return;
    end

    switch limit_mode
        case 'robust_percentile'
            cmin = prctile(vals, percentile_band(1));
            cmax = prctile(vals, percentile_band(2));
        otherwise
            cmin = min(vals);
            cmax = max(vals);
    end

    if ~(isfinite(cmin) && isfinite(cmax)) || cmax <= cmin
        center = 0.0;
        if ~isempty(vals)
            center = mean(vals, 'omitnan');
        end
        if ~isfinite(center)
            center = 0.0;
        end
        span = max(1.0, max(abs(vals)));
        cmin = center - span;
        cmax = center + span;
    end
end
