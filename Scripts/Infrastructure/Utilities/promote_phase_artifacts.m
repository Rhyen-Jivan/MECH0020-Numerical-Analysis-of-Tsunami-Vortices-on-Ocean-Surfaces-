function quick_access = promote_phase_artifacts(source_paths, quick_root)
% promote_phase_artifacts - Publish direct-access phase child artifacts.
%
% This helper mirrors the most useful files from a nested child run root
% into the queue-level phase run folder so users can inspect compact
% Data, Visuals, and Metrics outputs without drilling through
% Method/Mode/RunId subfolders.

    quick_access = struct( ...
        'root', char(string(quick_root)), ...
        'data', '', ...
        'figures', '', ...
        'reports', '', ...
        'sustainability', '');

    if ~isstruct(source_paths) || isempty(fieldnames(source_paths))
        return;
    end

    quick_root = char(string(quick_root));
    if isempty(quick_root)
        return;
    end

    quick_access.data = local_promote_tree(local_pick_path(source_paths, 'data'), ...
        fullfile(quick_root, 'Data'));
    quick_access.figures = local_promote_tree(local_pick_path(source_paths, 'figures_root'), ...
        fullfile(quick_root, 'Visuals'));
    quick_access.reports = local_promote_tree(local_pick_path(source_paths, 'reports'), ...
        fullfile(quick_root, 'Metrics'));
    quick_access.sustainability = local_promote_tree(local_pick_path(source_paths, 'sustainability'), ...
        fullfile(quick_root, 'Metrics', 'Collectors'));
end

function promoted_dir = local_promote_tree(source_dir, target_dir)
    promoted_dir = '';
    source_dir = char(string(source_dir));
    target_dir = char(string(target_dir));
    if isempty(source_dir) || exist(source_dir, 'dir') ~= 7
        return;
    end

    source_norm = local_normalize_path(source_dir);
    target_norm = local_normalize_path(target_dir);
    if strcmpi(source_norm, target_norm)
        promoted_dir = target_dir;
        return;
    end
    if local_is_child_path(target_norm, source_norm)
        %//NOTE Avoid mirroring a directory into one of its own children.
        % This occurs when a selected Phase 1 mesh is promoted in place and
        % its Metrics root is already the final quick-access destination.
        promoted_dir = source_dir;
        return;
    end

    file_entries = dir(fullfile(source_dir, '**', '*'));
    if isempty(file_entries)
        return;
    end
    file_entries = file_entries(~[file_entries.isdir]);
    if isempty(file_entries)
        return;
    end

    if exist(target_dir, 'dir') ~= 7
        mkdir(target_dir);
    end

    for i = 1:numel(file_entries)
        source_file = fullfile(file_entries(i).folder, file_entries(i).name);
        rel_dir = erase(char(string(file_entries(i).folder)), [source_norm, filesep]);
        rel_dir = strrep(rel_dir, '/', filesep);
        rel_dir = strrep(rel_dir, '\', filesep);
        candidate_name = file_entries(i).name;
        target_file = fullfile(target_dir, candidate_name);
        if exist(target_file, 'file') == 2
            rel_token = regexprep(rel_dir, '[\\/]+', '__');
            rel_token = regexprep(rel_token, '[^a-zA-Z0-9_]+', '_');
            rel_token = regexprep(rel_token, '^_+|_+$', '');
            if ~isempty(rel_token)
                candidate_name = sprintf('%s__%s', rel_token, file_entries(i).name);
                target_file = fullfile(target_dir, candidate_name);
            end
        end
        copyfile(source_file, target_file, 'f');
    end

    promoted_dir = target_dir;
end

function value = local_pick_path(source_paths, field_name)
    value = '';
    if isstruct(source_paths) && isfield(source_paths, field_name) && ~isempty(source_paths.(field_name))
        value = source_paths.(field_name);
    end
end

function norm_path = local_normalize_path(path_value)
    norm_path = char(string(path_value));
    norm_path = strrep(norm_path, '/', filesep);
    norm_path = strrep(norm_path, '\', filesep);
    while endsWith(norm_path, filesep)
        norm_path = extractBefore(string(norm_path), strlength(string(norm_path)));
        norm_path = char(norm_path);
    end
end

function tf = local_is_child_path(candidate_path, parent_path)
    tf = false;
    candidate_path = char(string(candidate_path));
    parent_path = char(string(parent_path));
    if isempty(candidate_path) || isempty(parent_path)
        return;
    end

    candidate_cmp = lower(strrep(candidate_path, '/', filesep));
    parent_cmp = lower(strrep(parent_path, '/', filesep));
    tf = startsWith(candidate_cmp, [parent_cmp, filesep]);
end
