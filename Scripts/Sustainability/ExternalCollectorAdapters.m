classdef ExternalCollectorAdapters
    % ExternalCollectorAdapters - Probe/extraction adapters for external monitors.
    %
    % Supported sources:
    %   - hwinfo
    %   - icue

    methods (Static)
        function snapshot = extract_snapshot(source, enabled, preferred_path, runtime_cfg)
            source_token = lower(char(string(source)));
            if nargin < 4 || ~isstruct(runtime_cfg)
                runtime_cfg = struct();
            end
            snapshot = struct();
            snapshot.source = source_token;
            snapshot.enabled = logical(enabled);
            snapshot.available = false;
            snapshot.path = '';
            snapshot.status = 'disabled';
            snapshot.transport = 'none';
            snapshot.message = 'collector disabled';
            snapshot.status_reason = snapshot.message;
            snapshot.version = '';
            snapshot.csv_path = '';
            snapshot.csv_dir = '';
            snapshot.csv_target_path = '';
            snapshot.csv_target_dir = '';
            snapshot.probe_details = struct();
            snapshot.timestamp_utc = char(datetime('now', 'TimeZone', 'UTC', ...
                'Format', 'yyyy-MM-dd''T''HH:mm:ss''Z'''));

            if ~snapshot.enabled
                return;
            end

            candidates = ExternalCollectorAdapters.collect_candidates(source_token, preferred_path);
            if isempty(candidates)
                snapshot.status = 'not_configured';
                snapshot.message = 'no candidate paths available';
                snapshot.status_reason = snapshot.message;
                return;
            end

            for i = 1:numel(candidates)
                candidate = char(string(candidates{i}));
                if isempty(candidate)
                    continue;
                end
                if exist(candidate, 'file') == 2
                    snapshot.path = candidate;
                    snapshot.status = 'executable_found';
                    snapshot.message = 'collector executable found but live transport is not yet verified';
                    snapshot.status_reason = snapshot.message;
                    snapshot.version = ExternalCollectorAdapters.read_file_version(candidate);
                    break;
                end
            end

            if isempty(snapshot.path)
                snapshot.available = false;
                snapshot.path = char(string(candidates{1}));
                snapshot.status = 'not_found';
                snapshot.message = 'collector executable not found';
                snapshot.status_reason = snapshot.message;
            end

            if strcmpi(source_token, 'hwinfo')
                runtime_cfg = ExternalCollectorAdapters.normalize_hwinfo_runtime_cfg(runtime_cfg);
                snapshot.csv_target_path = runtime_cfg.hwinfo_csv_target_path;
                snapshot.csv_target_dir = runtime_cfg.hwinfo_csv_target_dir;

                ini_state = ExternalCollectorAdapters.resolve_hwinfo_ini_state(snapshot.path);
                [~, ~, ~, csv_probe] = ExternalCollectorAdapters.discover_recent_csv_transport(source_token, runtime_cfg);
                snapshot.csv_path = csv_probe.csv_path;
                snapshot.csv_dir = csv_probe.csv_dir;
                snapshot.probe_details = struct( ...
                    'csv_path', csv_probe.csv_path, ...
                    'csv_dir', csv_probe.csv_dir, ...
                    'csv_status', csv_probe.csv_status, ...
                    'csv_target_path', csv_probe.csv_target_path, ...
                    'csv_target_dir', csv_probe.csv_target_dir, ...
                    'csv_target_resolved_path', csv_probe.csv_target_resolved_path, ...
                    'csv_target_available', csv_probe.csv_target_available, ...
                    'csv_target_configured', csv_probe.csv_target_configured, ...
                    'csv_observed_path', csv_probe.csv_observed_path, ...
                    'csv_observed_available', csv_probe.csv_observed_available, ...
                    'csv_target_mismatch', csv_probe.csv_target_mismatch, ...
                    'ini_path', ini_state.ini_path, ...
                    'ini_shared_memory_enabled', ini_state.ini_shared_memory_enabled, ...
                    'ini_shared_memory_state', ini_state.ini_shared_memory_state, ...
                    'csv_target_sync_status', ini_state.csv_target_sync_status);

                switch lower(char(string(csv_probe.csv_status)))
                    case 'csv_fallback_ready'
                        snapshot.status = 'csv_fallback';
                        snapshot.transport = 'csv';
                        snapshot.available = true;
                        snapshot.message = 'CSV fallback active';
                    case 'csv_target_mismatch'
                        snapshot.status = 'csv_target_mismatch';
                        snapshot.transport = 'csv';
                        snapshot.available = true;
                        snapshot.message = 'CSV target configured, but HWiNFO is logging elsewhere';
                    case 'csv_fallback_stale'
                        snapshot.status = 'csv_missing';
                        snapshot.transport = 'none';
                        snapshot.available = false;
                        if csv_probe.csv_target_configured
                            snapshot.message = 'shared memory unavailable; configured CSV target is not active';
                        else
                            snapshot.message = 'shared memory unavailable; CSV fallback is stale';
                        end
                    otherwise
                        if ~isempty(csv_probe.csv_status)
                            snapshot.status = csv_probe.csv_status;
                        elseif ~isempty(snapshot.path) && exist(snapshot.path, 'file') == 2
                            if isequal(ini_state.ini_shared_memory_enabled, false)
                                snapshot.status = 'shared_memory_disabled';
                                snapshot.message = 'shared memory disabled; CSV logging not active';
                            else
                                snapshot.status = 'csv_missing';
                                snapshot.message = 'shared memory unavailable; CSV logging not active';
                            end
                        end
                end
                snapshot.status_reason = snapshot.message;
                snapshot = ExternalCollectorAdapters.apply_hwinfo_live_shared_memory_probe(snapshot, runtime_cfg);
            else
                [csv_path, csv_dir, csv_status] = ExternalCollectorAdapters.discover_recent_csv_transport(source_token, runtime_cfg);
                snapshot.csv_path = csv_path;
                snapshot.csv_dir = csv_dir;
                snapshot.probe_details = struct( ...
                    'csv_path', csv_path, ...
                    'csv_dir', csv_dir, ...
                    'csv_status', csv_status);
            end
        end

        function [available, resolved_path, status] = probe(source, enabled, preferred_path, runtime_cfg)
            if nargin < 4
                runtime_cfg = struct();
            end
            snapshot = ExternalCollectorAdapters.extract_snapshot(source, enabled, preferred_path, runtime_cfg);
            available = snapshot.available;
            resolved_path = snapshot.path;
            status = snapshot.status;
        end

        function paths = default_paths(source)
            switch lower(char(string(source)))
                case 'hwinfo'
                    paths = { ...
                        'C:\Program Files\HWiNFO64\HWiNFO64.exe', ...
                        'C:\Program Files\HWiNFO32\HWiNFO32.exe', ...
                        'C:\Program Files\HWiNFO\HWiNFO.exe' ...
                    };
                case 'icue'
                    paths = { ...
                        'C:\Program Files\Corsair\Corsair iCUE5 Software\iCUE.exe', ...
                        'C:\Program Files\CORSAIR\Corsair iCUE5 Software\iCUE.exe', ...
                        'C:\Program Files\CORSAIR\CORSAIR iCUE 4 Software\iCUE.exe', ...
                        'C:\Program Files\Corsair\CORSAIR iCUE 3 Software\iCUE.exe', ...
                        'C:\Program Files\Corsair\CORSAIR iCUE Software\iCUE.exe' ...
                    };
                otherwise
                    paths = {};
            end
        end
    end

    methods (Static, Access = private)
        function candidates = collect_candidates(source_token, preferred_path)
            candidates = {};

            preferred_cells = ExternalCollectorAdapters.normalize_path_input(preferred_path);
            for i = 1:numel(preferred_cells)
                token = char(string(preferred_cells{i}));
                if ~isempty(token)
                    candidates{end + 1} = token; %#ok<AGROW>
                end
            end

            defaults = ExternalCollectorAdapters.default_paths(source_token);
            for i = 1:numel(defaults)
                token = char(string(defaults{i}));
                if isempty(token)
                    continue;
                end
                if ~any(strcmpi(candidates, token))
                    candidates{end + 1} = token; %#ok<AGROW>
                end
            end
        end

        function cells = normalize_path_input(path_input)
            cells = {};
            if nargin < 1 || isempty(path_input)
                return;
            end
            if ischar(path_input) || isstring(path_input)
                token = char(string(path_input));
                if ~isempty(token)
                    cells = {token};
                end
                return;
            end
            if iscell(path_input)
                cells = path_input(:).';
            end
        end

        function version = read_file_version(path_str)
            version = '';
            if ~ispc
                return;
            end
            try
                info = System.Diagnostics.FileVersionInfo.GetVersionInfo(path_str);
                version = char(string(info.FileVersion));
            catch ME
                ExternalCollectorAdapters.warn_once('ExternalCollectorAdapters:VersionProbeFailed', ...
                    'File version probe failed for external collector binary: %s', ME.message);
                version = '';
            end
        end

        function [csv_path, csv_dir, csv_status, probe] = discover_recent_csv_transport(source_token, runtime_cfg)
            csv_path = '';
            csv_dir = '';
            csv_status = '';
            probe = struct( ...
                'csv_path', '', ...
                'csv_dir', '', ...
                'csv_status', '', ...
                'csv_target_path', '', ...
                'csv_target_dir', '', ...
                'csv_target_resolved_path', '', ...
                'csv_target_available', false, ...
                'csv_target_configured', false, ...
                'csv_observed_path', '', ...
                'csv_observed_available', false, ...
                'csv_target_mismatch', false);
            if nargin < 2 || ~isstruct(runtime_cfg)
                runtime_cfg = struct();
            end
            if ~strcmpi(source_token, 'hwinfo')
                return;
            end

            runtime_cfg = ExternalCollectorAdapters.normalize_hwinfo_runtime_cfg(runtime_cfg);
            persistent hwinfo_csv_cache;
            if isempty(hwinfo_csv_cache)
                hwinfo_csv_cache = struct( ...
                    'timestamp', datetime.empty, ...
                    'csv_path', '', ...
                    'csv_dir', '', ...
                    'csv_status', '', ...
                    'csv_target_path', '', ...
                    'csv_target_dir', '', ...
                    'csv_target_resolved_path', '', ...
                    'csv_target_available', false, ...
                    'csv_target_configured', false, ...
                    'csv_observed_path', '', ...
                    'csv_observed_available', false, ...
                    'csv_target_mismatch', false);
            end

            now_utc = datetime('now', 'TimeZone', 'UTC');
            cache_matches_target = strcmpi(hwinfo_csv_cache.csv_target_path, runtime_cfg.hwinfo_csv_target_path) && ...
                strcmpi(hwinfo_csv_cache.csv_target_dir, runtime_cfg.hwinfo_csv_target_dir);
            if cache_matches_target && ~isempty(hwinfo_csv_cache.timestamp) && ...
                    seconds(now_utc - hwinfo_csv_cache.timestamp) < 30
                csv_path = hwinfo_csv_cache.csv_path;
                csv_dir = hwinfo_csv_cache.csv_dir;
                csv_status = hwinfo_csv_cache.csv_status;
                probe = hwinfo_csv_cache;
                return;
            end

            target_file = ExternalCollectorAdapters.resolve_target_csv_file(runtime_cfg);
            target_is_explicit = ~isempty(runtime_cfg.hwinfo_csv_target_path) && ...
                strcmpi(char(string(target_file)), char(string(runtime_cfg.hwinfo_csv_target_path)));
            target_available = ExternalCollectorAdapters.is_csv_available(target_file, target_is_explicit, now_utc);
            observed_file = '';
            observed_available = false;
            observed_datenum = -inf;
            candidate_dirs = ExternalCollectorAdapters.hwinfo_candidate_csv_dirs();
            for i = 1:numel(candidate_dirs)
                directory = candidate_dirs{i};
                if exist(directory, 'dir') ~= 7
                    continue;
                end
                files = dir(fullfile(directory, '*.csv'));
                if isempty(files)
                    continue;
                end
                [~, idx] = max([files.datenum]);
                if files(idx).datenum > observed_datenum
                    observed_datenum = files(idx).datenum;
                    observed_file = fullfile(files(idx).folder, files(idx).name);
                end
            end
            if ~isempty(observed_file)
                observed_available = ExternalCollectorAdapters.is_csv_available(observed_file, false, now_utc);
            end

            csv_target_configured = ~isempty(runtime_cfg.hwinfo_csv_target_path) || ~isempty(runtime_cfg.hwinfo_csv_target_dir);
            csv_target_mismatch = false;
            if target_available
                csv_path = target_file;
                csv_dir = fileparts(target_file);
                csv_status = 'csv_fallback_ready';
            elseif csv_target_configured && observed_available && ~isempty(observed_file)
                csv_path = observed_file;
                csv_dir = fileparts(observed_file);
                csv_status = 'csv_target_mismatch';
                csv_target_mismatch = true;
            elseif observed_available
                csv_path = observed_file;
                csv_dir = fileparts(observed_file);
                csv_status = 'csv_fallback_ready';
            elseif ~isempty(observed_file)
                csv_path = observed_file;
                csv_dir = fileparts(observed_file);
                csv_status = 'csv_fallback_stale';
            end

            hwinfo_csv_cache.timestamp = now_utc;
            hwinfo_csv_cache.csv_path = csv_path;
            hwinfo_csv_cache.csv_dir = csv_dir;
            hwinfo_csv_cache.csv_status = csv_status;
            hwinfo_csv_cache.csv_target_path = runtime_cfg.hwinfo_csv_target_path;
            hwinfo_csv_cache.csv_target_dir = runtime_cfg.hwinfo_csv_target_dir;
            hwinfo_csv_cache.csv_target_resolved_path = target_file;
            hwinfo_csv_cache.csv_target_available = target_available;
            hwinfo_csv_cache.csv_target_configured = csv_target_configured;
            hwinfo_csv_cache.csv_observed_path = observed_file;
            hwinfo_csv_cache.csv_observed_available = observed_available;
            hwinfo_csv_cache.csv_target_mismatch = csv_target_mismatch;
            probe = hwinfo_csv_cache;
        end

        function dirs_out = hwinfo_candidate_csv_dirs()
            dirs_out = {};
            user_home = getenv('USERPROFILE');
            if isempty(user_home)
                return;
            end

            roots = {fullfile(user_home, 'Documents')};
            onedrive_roots = dir(fullfile(user_home, 'OneDrive*'));
            for i = 1:numel(onedrive_roots)
                if ~onedrive_roots(i).isdir
                    continue;
                end
                roots{end + 1} = fullfile(onedrive_roots(i).folder, onedrive_roots(i).name); %#ok<AGROW>
                roots{end + 1} = fullfile(roots{end}, 'Documents'); %#ok<AGROW>
            end

            search_patterns = {'PC Metrics', '*HWiNFO*'};
            for i = 1:numel(roots)
                root = roots{i};
                if exist(root, 'dir') ~= 7
                    continue;
                end
                if ~any(strcmpi(dirs_out, root))
                    dirs_out{end + 1} = root; %#ok<AGROW>
                end
                for p = 1:numel(search_patterns)
                    matches = dir(fullfile(root, '**', search_patterns{p}));
                    matches = matches([matches.isdir]);
                    for j = 1:numel(matches)
                        candidate = fullfile(matches(j).folder, matches(j).name);
                        if ~any(strcmpi(dirs_out, candidate))
                            dirs_out{end + 1} = candidate; %#ok<AGROW>
                        end
                    end
                end
            end
        end

        function runtime_cfg = normalize_hwinfo_runtime_cfg(runtime_cfg)
            if nargin < 1 || ~isstruct(runtime_cfg)
                runtime_cfg = struct();
            end
            if ~isfield(runtime_cfg, 'hwinfo_shared_memory_blob_path')
                runtime_cfg.hwinfo_shared_memory_blob_path = '';
            end
            if ~isfield(runtime_cfg, 'hwinfo_transport_mode')
                runtime_cfg.hwinfo_transport_mode = 'auto';
            end
            if ~isfield(runtime_cfg, 'hwinfo_launch_if_needed')
                runtime_cfg.hwinfo_launch_if_needed = true;
            end
            if ~isfield(runtime_cfg, 'hwinfo_csv_target_dir')
                runtime_cfg.hwinfo_csv_target_dir = '';
            end
            if ~isfield(runtime_cfg, 'hwinfo_csv_target_path')
                runtime_cfg.hwinfo_csv_target_path = '';
            end
        end

        function snapshot = apply_hwinfo_live_shared_memory_probe(snapshot, runtime_cfg)
            runtime_cfg = ExternalCollectorAdapters.normalize_hwinfo_runtime_cfg(runtime_cfg);
            if strcmpi(char(string(runtime_cfg.hwinfo_transport_mode)), 'csv')
                return;
            end
            if (isempty(snapshot.path) || exist(snapshot.path, 'file') ~= 2) && ...
                    (isempty(runtime_cfg.hwinfo_shared_memory_blob_path) || ...
                    exist(runtime_cfg.hwinfo_shared_memory_blob_path, 'file') ~= 2)
                return;
            end

            live_sample = ExternalCollectorAdapters.probe_hwinfo_runtime_sample(snapshot.path, runtime_cfg);
            if ~isstruct(live_sample) || isempty(fieldnames(live_sample))
                return;
            end
            if ~isfield(live_sample, 'collector_status') || ~isstruct(live_sample.collector_status) || ...
                    ~isfield(live_sample.collector_status, 'hwinfo')
                return;
            end

            live_status = char(string(live_sample.collector_status.hwinfo));
            live_transport = char(string(ExternalCollectorAdapters.pick_field(live_sample, {'hwinfo_transport'}, 'none')));
            if ~strcmpi(live_status, 'shared_memory_connected') || ~strcmpi(live_transport, 'shared_memory')
                return;
            end

            snapshot.status = live_status;
            snapshot.transport = live_transport;
            snapshot.available = true;
            snapshot.message = char(string(ExternalCollectorAdapters.pick_field(live_sample, {'hwinfo_status_reason'}, 'shared memory connected')));
            snapshot.status_reason = snapshot.message;
            live_details = ExternalCollectorAdapters.pick_field(live_sample, {'collector_probe_details', 'hwinfo'}, struct());
            snapshot.probe_details = ExternalCollectorAdapters.merge_structs(snapshot.probe_details, live_details);
            snapshot.probe_details.shared_memory_runtime_verified = true;
            if strcmpi(char(string(ExternalCollectorAdapters.pick_field(live_details, {'shared_memory_source'}, 'none'))), 'blob_fixture')
                snapshot.csv_path = '';
                snapshot.csv_dir = '';
            end
        end

        function sample = probe_hwinfo_runtime_sample(exe_path, runtime_cfg)
            sample = struct();
            runtime_cfg = ExternalCollectorAdapters.normalize_hwinfo_runtime_cfg(runtime_cfg);
            runtime_cfg.hwinfo_launch_if_needed = false;
            settings = struct( ...
                'sustainability', struct( ...
                    'external_collectors', struct('hwinfo', true, 'icue', false), ...
                    'collector_paths', struct('hwinfo', char(string(exe_path)), 'icue', ''), ...
                    'collector_runtime', runtime_cfg));
            try
                sample = ExternalCollectorDispatcher.runtime_probe(settings);
            catch ME
                ExternalCollectorAdapters.warn_once('ExternalCollectorAdapters:LiveHWiNFOProbeFailed', ...
                    'Live HWiNFO shared-memory probe failed: %s', ME.message);
                sample = struct();
            end
        end

        function value = pick_field(source, path, default_value)
            value = default_value;
            if nargin < 3
                default_value = [];
                value = default_value;
            end
            current = source;
            for i = 1:numel(path)
                key = path{i};
                if isstruct(current) && isfield(current, key)
                    current = current.(key);
                else
                    return;
                end
            end
            value = current;
        end

        function merged = merge_structs(base, override)
            merged = base;
            if ~isstruct(merged)
                merged = struct();
            end
            if ~isstruct(override)
                return;
            end
            keys = fieldnames(override);
            for i = 1:numel(keys)
                merged.(keys{i}) = override.(keys{i});
            end
        end

        function file_path = resolve_target_csv_file(runtime_cfg)
            file_path = '';
            runtime_cfg = ExternalCollectorAdapters.normalize_hwinfo_runtime_cfg(runtime_cfg);
            if ~isempty(runtime_cfg.hwinfo_csv_target_path) && exist(runtime_cfg.hwinfo_csv_target_path, 'file') == 2
                file_path = char(string(runtime_cfg.hwinfo_csv_target_path));
                return;
            end
            if ~isempty(runtime_cfg.hwinfo_csv_target_dir) && exist(runtime_cfg.hwinfo_csv_target_dir, 'dir') == 7
                files = dir(fullfile(runtime_cfg.hwinfo_csv_target_dir, '*.csv'));
                if ~isempty(files)
                    [~, idx] = max([files.datenum]);
                    file_path = fullfile(files(idx).folder, files(idx).name);
                end
            end
        end

        function tf = is_csv_available(file_path, is_explicit, now_utc)
            tf = false;
            if nargin < 3 || isempty(now_utc)
                now_utc = datetime('now', 'TimeZone', 'UTC');
            end
            if nargin < 2
                is_explicit = false;
            end
            if isempty(file_path) || exist(file_path, 'file') ~= 2
                return;
            end
            if is_explicit
                tf = true;
                return;
            end
            file_info = dir(file_path);
            if isempty(file_info)
                return;
            end
            file_time = datetime(file_info.datenum, 'ConvertFrom', 'datenum', 'TimeZone', '');
            file_time.TimeZone = 'UTC';
            tf = minutes(now_utc - file_time) <= 30;
        end

        function ini_state = resolve_hwinfo_ini_state(exe_path)
            ini_state = struct( ...
                'ini_path', '', ...
                'ini_shared_memory_enabled', [], ...
                'ini_shared_memory_state', 'unknown', ...
                'csv_target_sync_status', 'not_configured');
            candidates = ExternalCollectorAdapters.hwinfo_ini_candidates(exe_path);
            for i = 1:numel(candidates)
                candidate = candidates{i};
                if exist(candidate, 'file') ~= 2
                    continue;
                end
                ini_state.ini_path = candidate;
                text = ExternalCollectorAdapters.read_text_file(candidate);
                if isempty(text)
                    return;
                end
                token = regexp(text, '(?im)^\s*SensorsSM\s*=\s*(\d+)\s*$', 'tokens', 'once');
                if ~isempty(token)
                    ini_state.ini_shared_memory_enabled = strcmp(strtrim(token{1}), '1');
                end
                if isequal(ini_state.ini_shared_memory_enabled, true)
                    ini_state.ini_shared_memory_state = 'enabled';
                elseif isequal(ini_state.ini_shared_memory_enabled, false)
                    ini_state.ini_shared_memory_state = 'disabled';
                end
                return;
            end
        end

        function candidates = hwinfo_ini_candidates(exe_path)
            candidates = {};
            if nargin < 1
                exe_path = '';
            end
            if ~isempty(exe_path)
                exe = string(exe_path);
                [folder, stem] = fileparts(char(exe));
                candidates = { ...
                    fullfile(folder, [stem '.INI']), ...
                    fullfile(folder, 'HWiNFO64.INI'), ...
                    fullfile(folder, 'HWiNFO32.INI'), ...
                    fullfile(folder, 'HWiNFO.INI')};
            end
            appdata = getenv('APPDATA');
            if ~isempty(appdata)
                candidates = [candidates, { ... %#ok<AGROW>
                    fullfile(appdata, 'HWiNFO64.INI'), ...
                    fullfile(appdata, 'HWiNFO32.INI'), ...
                    fullfile(appdata, 'HWiNFO.INI')}];
            end
            if isempty(candidates)
                return;
            end
            [~, idx] = unique(lower(string(candidates)), 'stable');
            candidates = candidates(sort(idx));
        end

        function text = read_text_file(path_str)
            text = '';
            encodings = {'UTF-8', 'windows-1252', 'ISO-8859-1'};
            for i = 1:numel(encodings)
                try
                    text = fileread(path_str);
                    return;
                catch
                end
                fid = fopen(path_str, 'r', 'n', encodings{i});
                if fid == -1
                    continue;
                end
                cleaner = onCleanup(@() fclose(fid)); %#ok<NASGU>
                raw = fread(fid, '*char')';
                if ~isempty(raw)
                    text = raw;
                    return;
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
            SafeConsoleIO.warning(id, message, varargin{:});
        end
    end
end
