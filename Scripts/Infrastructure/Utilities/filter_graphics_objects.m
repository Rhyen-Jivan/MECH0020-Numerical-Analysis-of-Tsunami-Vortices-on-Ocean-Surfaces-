function cleaned = filter_graphics_objects(s)
    % FILTER_GRAPHICS_OBJECTS - Recursively removes graphics objects from structs
    %
    % Syntax:
    %   cleaned = filter_graphics_objects(s)
    %
    % Purpose:
    %   Removes graphics objects, UI components, and function handles from
    %   a struct to prevent warnings when saving to .mat files
    %
    % Input:
    %   s - Input struct (can be nested)
    %
    % Output:
    %   cleaned - Struct with graphics objects removed
    %
    % Filters out:
    %   - Figure handles (matlab.ui.Figure)
    %   - Axes handles (matlab.graphics.axis.Axes)
    %   - UI controls (GridLayout, UIAxes, UIControl, etc.)
    %   - Graphics objects detected by isgraphics()
    %   - Function handles
    %
    % Example:
    %   config.data = [1 2 3];
    %   config.fig = figure();
    %   config_clean = filter_graphics_objects(config);
    %   % config_clean will have 'data' but not 'fig'

    if ~isstruct(s)
        cleaned = s;
        return;
    end

    if isempty(s)
        cleaned = s;
        return;
    end

    if numel(s) > 1
        cleaned_cells = cell(size(s));
        field_union = cell(1, 0);
        for idx = 1:numel(s)
            cleaned_cells{idx} = filter_graphics_objects(s(idx));
            field_union = union(field_union, fieldnames(cleaned_cells{idx}), 'stable');
        end
        template = struct();
        for field_idx = 1:numel(field_union)
            template.(field_union{field_idx}) = [];
        end
        cleaned = repmat(template, size(s));
        for idx = 1:numel(s)
            element = cleaned_cells{idx};
            for field_idx = 1:numel(field_union)
                field_name = field_union{field_idx};
                if isfield(element, field_name)
                    cleaned(idx).(field_name) = element.(field_name);
                end
            end
        end
        return;
    end

    cleaned = struct();
    fields = fieldnames(s);

    for i = 1:length(fields)
        field_name = fields{i};
        field_value = s.(field_name);

        % Check if this field should be skipped (graphics/UI objects)
        % IMPORTANT: Only filter based on object TYPE, not numeric values
        % (numeric arrays might contain values that happen to be graphics handles)
        skip_field = false;

        try
            % Check for graphics/UI object types using isa()
            % Do NOT use isgraphics() on numeric data - it can give false positives!
            if isa(field_value, 'matlab.ui.Figure') || ...
               isa(field_value, 'matlab.ui.container.GridLayout') || ...
               isa(field_value, 'matlab.ui.container.Tab') || ...
               isa(field_value, 'matlab.ui.container.TabGroup') || ...
               isa(field_value, 'matlab.ui.container.Panel') || ...
               isa(field_value, 'matlab.ui.control.UIControl') || ...
               isa(field_value, 'matlab.ui.control.UIAxes') || ...
               isa(field_value, 'matlab.ui.control.Button') || ...
               isa(field_value, 'matlab.ui.control.Label') || ...
               isa(field_value, 'matlab.graphics.axis.Axes') || ...
               isa(field_value, 'matlab.graphics.chart.Chart') || ...
               isa(field_value, 'matlab.graphics.primitive.Line') || ...
               isa(field_value, 'matlab.graphics.primitive.Patch') || ...
               isa(field_value, 'matlab.graphics.primitive.Surface') || ...
               isa(field_value, 'matlab.graphics.Graphics') || ...
               isa(field_value, 'function_handle')
                skip_field = true;
            end
        catch ME
            warn_once('filter_graphics_objects:TypeProbeFailed', ...
                'Type probe failed while filtering graphics objects; defaulting to keep field: %s', ME.message);
            % If any check fails, assume it's not a graphics object
            skip_field = false;
        end

        if skip_field
            continue;  % Skip this field
        end

        % Recursively clean nested structs
        if isstruct(field_value)
            cleaned.(field_name) = filter_graphics_objects(field_value);
        else
            cleaned.(field_name) = field_value;
        end
    end
end

function warn_once(identifier, message, varargin)
    persistent emitted_ids;
    if isempty(emitted_ids)
        emitted_ids = containers.Map('KeyType', 'char', 'ValueType', 'logical');
    end
    id = char(string(identifier));
    if isKey(emitted_ids, id)
        return;
    end
    emitted_ids(id) = true;
    warning(id, message, varargin{:});
end
