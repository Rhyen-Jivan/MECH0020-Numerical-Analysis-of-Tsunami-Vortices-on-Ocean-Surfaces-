function merged = merge_structs(base, override)
% merge_structs Recursively overlay scalar struct fields from override onto base.

    if ~builtin('isstruct', base) || ~builtin('isstruct', override) || ...
            ~isscalar(base) || ~isscalar(override)
        merged = override;
        return;
    end

    merged = base;
    keys = fieldnames(override);
    for i = 1:numel(keys)
        key = keys{i};
        override_value = override.(key);
        if isfield(merged, key)
            base_value = merged.(key);
        else
            base_value = [];
        end
        if isfield(merged, key) && builtin('isstruct', base_value) && ...
                builtin('isstruct', override_value) && isscalar(base_value) && isscalar(override_value)
            merged.(key) = merge_structs(base_value, override_value);
        else
            merged.(key) = override_value;
        end
    end
end
