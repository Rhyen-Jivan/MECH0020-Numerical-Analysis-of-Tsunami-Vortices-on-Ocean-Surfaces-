function settings_path = write_run_settings_text(settings_path, varargin)
% write_run_settings_text - Write a compact human-readable settings summary.
%
% Usage:
%   write_run_settings_text(path, 'Section Name', payload, ...)

    if nargin < 1 || isempty(settings_path)
        error('write_run_settings_text:MissingPath', ...
            'A target settings path is required.');
    end
    if mod(numel(varargin), 2) ~= 0
        error('write_run_settings_text:InvalidSections', ...
            'Sections must be supplied as name/payload pairs.');
    end

    settings_path = char(string(settings_path));
    parent_dir = fileparts(settings_path);
    if ~isempty(parent_dir) && exist(parent_dir, 'dir') ~= 7
        mkdir(parent_dir);
    end

    fid = fopen(settings_path, 'w');
    if fid < 0
        error('write_run_settings_text:OpenFailed', ...
            'Could not open %s for writing.', settings_path);
    end
    cleaner = onCleanup(@() fclose(fid));

    fprintf(fid, 'Run Settings\n');
    fprintf(fid, 'Generated: %s\n\n', char(datetime('now', 'Format', 'yyyy-MM-dd HH:mm:ss')));
    for i = 1:2:numel(varargin)
        section_name = char(string(varargin{i}));
        payload = local_filter_payload(varargin{i + 1});
        fprintf(fid, '[%s]\n', section_name);
        section_text = strtrim(evalc('disp(payload)'));
        if isempty(section_text)
            section_text = '(empty)';
        end
        fprintf(fid, '%s\n\n', section_text);
    end
    clear cleaner
end

function payload = local_filter_payload(payload)
    if exist('filter_graphics_objects', 'file') == 2
        try
            payload = filter_graphics_objects(payload);
        catch
            % Leave payload as-is if the graphics filter is unavailable.
        end
    end
end
