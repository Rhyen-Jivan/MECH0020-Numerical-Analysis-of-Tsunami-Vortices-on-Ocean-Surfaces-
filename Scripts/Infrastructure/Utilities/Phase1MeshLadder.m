function [levels, meta] = Phase1MeshLadder(phase1_cfg)
%Phase1MeshLadder Resolve the canonical Phase 1 convergence mesh ladder.

    if nargin < 1 || ~isstruct(phase1_cfg)
        error('Phase1MeshLadder:InvalidConfig', ...
            'Phase1MeshLadder requires a Phase 1 config struct.');
    end

    meta = struct();
    meta.mode = normalize_mode(local_pick_text(phase1_cfg, 'mesh_ladder_mode', 'bounded'));
    meta.start_n = resolve_bound(phase1_cfg, 'start', 32);
    meta.final_n = max(meta.start_n, resolve_bound(phase1_cfg, 'final', 768));
    meta.requested_count = max(2, round(local_pick_numeric(phase1_cfg, 'mesh_level_count', 7)));
    meta.spacing_count = max(2, round(local_pick_numeric(phase1_cfg, 'mesh_spacing_level_count', meta.requested_count)));
    meta.powers_of_two_max_n = max(8, round(local_pick_numeric(phase1_cfg, 'mesh_powers_of_two_max_n', 1024)));
    meta.required_levels = resolve_required_levels(phase1_cfg, meta.start_n, meta.final_n);

    switch meta.mode
        case 'bounded'
            levels = build_bounded_ladder(meta.start_n, meta.final_n, meta.spacing_count);
        case 'powers_of_2'
            levels = build_powers_of_two_ladder(meta.start_n, meta.final_n, meta.powers_of_two_max_n);
        otherwise
            error('Phase1MeshLadder:UnsupportedMode', ...
                'Unsupported Phase 1 mesh ladder mode "%s".', meta.mode);
    end

    levels = double(reshape(levels, 1, []));
    if ~isempty(meta.required_levels)
        levels = sort(unique([levels, meta.required_levels]));
    end
    meta.level_count = double(numel(levels));
    meta.capped_final_n = double(min(meta.final_n, meta.powers_of_two_max_n));
end

function mode = normalize_mode(mode_value)
    mode = lower(strtrim(char(string(mode_value))));
    switch mode
        case {'bounded', 'bounds'}
            mode = 'bounded';
        case {'powers_of_2', 'powers_of_two', 'powers of 2', 'pow2'}
            mode = 'powers_of_2';
        otherwise
            error('Phase1MeshLadder:UnsupportedMode', ...
                'Unsupported Phase 1 mesh ladder mode "%s".', char(string(mode_value)));
    end
end

function bound_n = resolve_bound(cfg_struct, which_side, fallback_n)
    equal_field = sprintf('mesh_%s_equal_xy', which_side);
    n_field = sprintf('mesh_%s_n', which_side);
    nx_field = sprintf('mesh_%s_nx', which_side);
    ny_field = sprintf('mesh_%s_ny', which_side);

    use_equal = true;
    if isfield(cfg_struct, equal_field)
        use_equal = logical(cfg_struct.(equal_field));
    end

    if use_equal
        bound_n = round(local_pick_numeric(cfg_struct, n_field, fallback_n));
    else
        candidates = [];
        if isfield(cfg_struct, nx_field) && isnumeric(cfg_struct.(nx_field)) && isscalar(cfg_struct.(nx_field)) ...
                && isfinite(cfg_struct.(nx_field))
            candidates(end + 1) = double(cfg_struct.(nx_field)); %#ok<AGROW>
        end
        if isfield(cfg_struct, ny_field) && isnumeric(cfg_struct.(ny_field)) && isscalar(cfg_struct.(ny_field)) ...
                && isfinite(cfg_struct.(ny_field))
            candidates(end + 1) = double(cfg_struct.(ny_field)); %#ok<AGROW>
        end
        if isempty(candidates)
            bound_n = fallback_n;
        else
            bound_n = round(max(candidates));
        end
    end

    bound_n = max(8, round(double(bound_n)));
end

function levels = build_bounded_ladder(start_n, final_n, count)
    start_n = max(8, round(double(start_n)));
    final_n = max(start_n, round(double(final_n)));
    count = max(2, round(double(count)));

    if (final_n - start_n + 1) < count
        error('Phase1MeshLadder:RangeTooNarrow', ...
            'Phase 1 mesh range [%d, %d] cannot supply %d unique integer mesh levels.', ...
            start_n, final_n, count);
    end

    levels = round(linspace(start_n, final_n, count));
    levels(1) = start_n;
    levels(end) = final_n;

    for i = 2:count
        min_allowed = levels(i - 1) + 1;
        max_allowed = final_n - (count - i);
        levels(i) = min(max(levels(i), min_allowed), max_allowed);
    end

    for i = count-1:-1:1
        min_allowed = start_n + (i - 1);
        max_allowed = levels(i + 1) - 1;
        levels(i) = max(min(levels(i), max_allowed), min_allowed);
    end

    levels(1) = start_n;
    levels(end) = final_n;
end

function levels = build_powers_of_two_ladder(start_n, final_n, max_n)
    start_n = max(8, round(double(start_n)));
    final_n = max(start_n, round(double(final_n)));
    max_n = max(8, round(double(max_n)));
    capped_final_n = min(final_n, max_n);

    min_exp = ceil(log2(double(start_n)));
    max_exp = floor(log2(double(capped_final_n)));
    if max_exp < min_exp
        levels = [];
    else
        levels = 2 .^ (min_exp:max_exp);
        levels = levels(levels >= start_n & levels <= capped_final_n);
    end

    if isempty(levels)
        error('Phase1MeshLadder:NoPowersOfTwoInRange', ...
            'Phase 1 powers-of-two mesh range [%d, %d] contains no valid 2^n levels at or below %d.', ...
            start_n, final_n, max_n);
    end
end

function levels = resolve_required_levels(cfg_struct, start_n, final_n)
    levels = zeros(1, 0);
    if ~isstruct(cfg_struct) || ~isfield(cfg_struct, 'required_mesh_levels')
        return;
    end
    raw = cfg_struct.required_mesh_levels;
    if ~(isnumeric(raw) || islogical(raw))
        return;
    end
    levels = round(double(reshape(raw, 1, [])));
    levels = levels(isfinite(levels) & levels >= start_n & levels <= final_n);
    levels = unique(levels, 'stable');
end

function value = local_pick_numeric(source, field_name, fallback)
    value = fallback;
    if isstruct(source) && isfield(source, field_name) && isnumeric(source.(field_name)) ...
            && isscalar(source.(field_name)) && isfinite(source.(field_name))
        value = double(source.(field_name));
    end
end

function value = local_pick_text(source, field_name, fallback)
    value = fallback;
    if isstruct(source) && isfield(source, field_name) && ~isempty(source.(field_name))
        value = char(string(source.(field_name)));
    end
end
