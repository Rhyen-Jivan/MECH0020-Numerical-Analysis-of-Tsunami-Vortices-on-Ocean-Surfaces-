function safe_save_mat(target_path, payload, mat_version)
%safe_save_mat Atomically persist a non-empty struct payload to a MAT file.

    target_path = char(string(target_path));
    if nargin < 2 || ~isstruct(payload) || isempty(fieldnames(payload))
        error('safe_save_mat:InvalidSavePayload', ...
            'safe_save_mat requires a non-empty struct payload.');
    end
    if nargin < 3 || isempty(mat_version)
        mat_version = '-v7';
    end
    mat_version = char(string(mat_version));

    target_dir = fileparts(target_path);
    if ~isempty(target_dir) && exist(target_dir, 'dir') ~= 7
        mkdir(target_dir);
    end
    if exist(target_path, 'file') == 2
        delete(target_path);
    end

    tmp_candidates = local_temp_candidates(target_path, target_dir);
    last_error = [];
    last_tmp_path = '';

    for i = 1:numel(tmp_candidates)
        tmp_path = tmp_candidates{i};
        last_tmp_path = tmp_path;
        local_delete_if_present(tmp_path);
        try
            save(tmp_path, '-struct', 'payload', mat_version);
        catch ME
            local_delete_if_present(tmp_path);
            last_error = ME;
            if i < numel(tmp_candidates) && local_should_retry_save(ME)
                continue;
            end
            local_throw_save_error(target_path, tmp_path, ME);
        end

        [ok_move, move_msg] = movefile(tmp_path, target_path, 'f');
        if ok_move
            return;
        end

        local_delete_if_present(tmp_path);
        last_error = MException('safe_save_mat:SaveMoveFailed', ...
            'Failed finalizing MAT save to "%s" via "%s": %s', target_path, tmp_path, move_msg);
        if i < numel(tmp_candidates)
            continue;
        end
        throw(last_error);
    end

    if ~isempty(last_error)
        local_throw_save_error(target_path, last_tmp_path, last_error);
    end
end

function candidates = local_temp_candidates(target_path, target_dir)
    [~, ~, ext] = fileparts(target_path);
    if isempty(ext)
        ext = '.mat';
    end

    hash_token = local_short_hash(target_path);
    candidates = {
        fullfile(target_dir, ['t', ext]), ...
        fullfile(target_dir, sprintf('s_%s%s', hash_token, ext)), ...
        fullfile(tempdir, sprintf('tsu_%s%s', hash_token, ext))};
    candidates = unique(candidates, 'stable');
end

function tf = local_should_retry_save(ME)
    identifier = char(string(ME.identifier));
    message = lower(char(string(ME.message)));
    tf = strcmp(identifier, 'MATLAB:save:unableToWriteToMatFile') || ...
        strcmp(identifier, 'MATLAB:save:cantWriteFile') || ...
        contains(message, 'appears to be corrupt') || ...
        contains(message, 'could not be closed') || ...
        contains(message, 'permission denied') || ...
        contains(message, 'unable to write file');
end

function local_throw_save_error(target_path, tmp_path, ME)
    target_len = strlength(string(target_path));
    tmp_len = strlength(string(tmp_path));
    if ispc && (target_len >= 240 || tmp_len >= 240)
        error('safe_save_mat:SavePathTooLong', ...
            ['Failed writing MAT file because the save path exceeded the Windows guard. ', ...
             'Target length %d: %s | Temp length %d: %s'], ...
            target_len, target_path, tmp_len, tmp_path);
    end
    rethrow(ME);
end

function local_delete_if_present(path_str)
    if exist(path_str, 'file') == 2
        delete(path_str);
    end
end

function token = local_short_hash(path_str)
    path_str = char(string(path_str));
    hash_value = uint32(2166136261);
    prime = uint32(16777619);
    for i = 1:numel(path_str)
        hash_value = bitxor(hash_value, uint32(path_str(i)));
        hash_value = uint32(mod(double(hash_value) * double(prime), 2^32));
    end
    token = lower(dec2hex(double(hash_value), 8));
end
