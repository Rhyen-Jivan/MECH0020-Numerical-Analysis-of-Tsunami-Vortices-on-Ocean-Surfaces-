classdef ExecutionResourcePlanner
    % ExecutionResourcePlanner - Canonical host-aware planning for workers/threads/GPU.
    %
    % This planner is the single source of truth for runtime resource policy
    % used by both UI previews and execution paths.

    methods (Static)
        function [plan, host_probe] = plan(parameters, settings, varargin)
            p = inputParser;
            addParameter(p, 'HostProbe', struct(), @isstruct);
            addParameter(p, 'ProfilesPath', '', @(x) ischar(x) || isstring(x));
            addParameter(p, 'ModeToken', '', @(x) ischar(x) || isstring(x));
            addParameter(p, 'MethodToken', '', @(x) ischar(x) || isstring(x));
            parse(p, varargin{:});
            opts = p.Results;

            host_probe = ExecutionResourcePlanner.resolve_host_probe(opts.HostProbe);
            [mode_token, method_token] = ExecutionResourcePlanner.resolve_context_tokens( ...
                parameters, settings, opts.ModeToken, opts.MethodToken);

            plan = ExecutionResourcePlanner.default_plan();
            plan = ExecutionResourcePlanner.overlay_user_inputs(plan, parameters, settings);
            plan = ExecutionResourcePlanner.clamp_plan(plan);

            note_lines = {};
            if plan.planner_use_host_profiles
                profiles_path = char(string(opts.ProfilesPath));
                if isempty(strtrim(profiles_path))
                    profiles_path = ExecutionResourcePlanner.default_profiles_path();
                end
                [plan, profile_note] = ExecutionResourcePlanner.apply_host_profile(plan, host_probe, profiles_path);
                if ~isempty(profile_note)
                    note_lines{end + 1} = profile_note; %#ok<AGROW>
                end
            end

            [plan, policy_notes] = ExecutionResourcePlanner.compute_effective_policy( ...
                plan, parameters, host_probe, mode_token, method_token);
            note_lines = [note_lines, policy_notes]; %#ok<AGROW>

            plan.mode_token = mode_token;
            plan.method_token = method_token;
            plan.pool_workers = plan.pool_workers_effective;
            plan.target_pool_workers = plan.pool_workers_effective;
            plan.max_pool_workers = plan.pool_workers_effective;
            plan.max_threads = plan.thread_cap;
            plan.monitor_period_seconds = 1 / plan.monitor_hz;
            plan.cpu_probe_interval_seconds = max(0.5, plan.monitor_period_seconds);
            plan.worker_topology = ExecutionResourcePlanner.resolve_worker_topology(plan.pool_workers_effective);

            if isempty(note_lines)
                note_lines = {'planner resolved default policy'};
            end
            plan.planner_notes = note_lines;
        end

        function host_probe = probe_host()
            host_probe = struct();
            host_probe.hostname = char(string(ExecutionResourcePlanner.first_nonempty( ...
                {getenv('COMPUTERNAME'), getenv('HOSTNAME')}, 'unknown_host')));
            host_probe.cpu_cores = ExecutionResourcePlanner.resolve_available_threads();
            host_probe.local_worker_capacity = ExecutionResourcePlanner.resolve_local_worker_capacity();
            host_probe.has_gpu_api = (exist('gpuDevice', 'file') == 2) || (exist('gpuDevice', 'builtin') > 0);
            host_probe.gpu_device_available = false;
            host_probe.gpu_total_memory_bytes = NaN;
            host_probe.gpu_name = '';

            if host_probe.has_gpu_api
                try
                    g = gpuDevice();
                    host_probe.gpu_device_available = logical(g.DeviceAvailable);
                    if isprop(g, 'TotalMemory')
                        host_probe.gpu_total_memory_bytes = double(g.TotalMemory);
                    end
                    if isprop(g, 'Name')
                        host_probe.gpu_name = char(string(g.Name));
                    end
                catch
                    host_probe.gpu_device_available = false;
                    host_probe.gpu_total_memory_bytes = NaN;
                    host_probe.gpu_name = '';
                end
            end
        end

        function host_probe = resolve_host_probe(candidate)
            if nargin >= 1 && isstruct(candidate) && ~isempty(fieldnames(candidate))
                host_probe = candidate;
            else
                host_probe = ExecutionResourcePlanner.probe_host();
            end

            if ~isfield(host_probe, 'hostname') || isempty(host_probe.hostname)
                host_probe.hostname = char(string(ExecutionResourcePlanner.first_nonempty( ...
                    {getenv('COMPUTERNAME'), getenv('HOSTNAME')}, 'unknown_host')));
            end
            if ~isfield(host_probe, 'cpu_cores') || ~isfinite(double(host_probe.cpu_cores))
                host_probe.cpu_cores = ExecutionResourcePlanner.resolve_available_threads();
            else
                host_probe.cpu_cores = max(1, round(double(host_probe.cpu_cores)));
            end
            if ~isfield(host_probe, 'local_worker_capacity') || ~isfinite(double(host_probe.local_worker_capacity))
                host_probe.local_worker_capacity = ExecutionResourcePlanner.resolve_local_worker_capacity();
            else
                host_probe.local_worker_capacity = max(1, round(double(host_probe.local_worker_capacity)));
            end
            if ~isfield(host_probe, 'has_gpu_api')
                host_probe.has_gpu_api = (exist('gpuDevice', 'file') == 2) || (exist('gpuDevice', 'builtin') > 0);
            end
            if ~isfield(host_probe, 'gpu_device_available')
                host_probe.gpu_device_available = false;
            end
            if ~isfield(host_probe, 'gpu_total_memory_bytes')
                host_probe.gpu_total_memory_bytes = NaN;
            end
            if ~isfield(host_probe, 'gpu_name')
                host_probe.gpu_name = '';
            end
        end

        function profile_data = load_profiles(profiles_path)
            profile_data = struct('schema_version', '1.0', 'profiles', []);
            path_str = char(string(profiles_path));
            if isempty(strtrim(path_str)) || exist(path_str, 'file') ~= 2
                return;
            end

            try
                raw = fileread(path_str);
                decoded = jsondecode(raw);
            catch ME
                error('ExecutionResourcePlanner:InvalidProfileFile', ...
                    'Failed to parse resource profile file (%s): %s', path_str, ME.message);
            end

            if isfield(decoded, 'schema_version')
                profile_data.schema_version = char(string(decoded.schema_version));
            end
            if isfield(decoded, 'profiles')
                profile_data.profiles = decoded.profiles;
            end
        end

        function path_str = default_profiles_path()
            util_dir = fileparts(mfilename('fullpath')); % Scripts/Infrastructure/Utilities
            repo_root = fileparts(fileparts(fileparts(util_dir))); % repo root
            path_str = fullfile(repo_root, 'settings', 'resource_profiles.json');
        end
    end

    methods (Static, Access = private)
        function plan = default_plan()
            plan = struct( ...
                'cpu_target_pct', 80, ...
                'memory_target_pct', 70, ...
                'gpu_target_pct', 80, ...
                'monitor_hz', 2, ...
                'progress_hz', 6, ...
                'pool_workers', 1, ...
                'resource_strategy', 'mode_adaptive', ...
                'gpu_policy', 'auto', ...
                'planner_use_host_profiles', true, ...
                'interactive_pool_workers_max', 2, ...
                'throughput_pool_workers_min', 2, ...
                'interactive_thread_headroom_pct', 75, ...
                'gpu_min_workload_cells_steps', 0, ...
                'gpu_memory_guard_pct', 80, ...
                'pool_workers_max_override', 0, ...
                'pool_workers_requested', 1, ...
                'pool_workers_effective', 1, ...
                'thread_cap', 1, ...
                'gpu_enabled_effective', false, ...
                'host_profile_id', 'none', ...
                'planner_notes', {{}});
        end

        function plan = overlay_user_inputs(plan, parameters, settings)
            if nargin < 2 || ~isstruct(parameters)
                parameters = struct();
            end
            if nargin < 3 || ~isstruct(settings)
                settings = struct();
            end

            user = struct();
            if isfield(settings, 'resource_allocation') && isstruct(settings.resource_allocation)
                user = settings.resource_allocation;
            end

            keys = fieldnames(plan);
            for i = 1:numel(keys)
                key = keys{i};
                if isfield(user, key)
                    plan.(key) = user.(key);
                elseif isfield(parameters, key)
                    plan.(key) = parameters.(key);
                end
            end

            if isfield(user, 'pool_workers')
                plan.pool_workers = user.pool_workers;
            elseif isfield(parameters, 'pool_workers')
                plan.pool_workers = parameters.pool_workers;
            end
        end

        function plan = clamp_plan(plan)
            plan.cpu_target_pct = ExecutionResourcePlanner.clamp_percent(plan.cpu_target_pct, 5, 100, 80);
            plan.memory_target_pct = ExecutionResourcePlanner.clamp_percent(plan.memory_target_pct, 10, 95, 70);
            plan.gpu_target_pct = ExecutionResourcePlanner.clamp_percent(plan.gpu_target_pct, 0, 100, 80);
            plan.monitor_hz = ExecutionResourcePlanner.clamp_positive(plan.monitor_hz, 2, 0.5, 20);
            plan.progress_hz = ExecutionResourcePlanner.clamp_positive(plan.progress_hz, 6, 0.5, 30);
            plan.pool_workers = ExecutionResourcePlanner.clamp_int(plan.pool_workers, 1, 1, 1024);
            plan.interactive_pool_workers_max = ExecutionResourcePlanner.clamp_int(plan.interactive_pool_workers_max, 2, 1, 1024);
            plan.throughput_pool_workers_min = ExecutionResourcePlanner.clamp_int(plan.throughput_pool_workers_min, 2, 1, 1024);
            plan.interactive_thread_headroom_pct = ExecutionResourcePlanner.clamp_percent(plan.interactive_thread_headroom_pct, 0, 99, 75);
            plan.gpu_min_workload_cells_steps = max(0, double(plan.gpu_min_workload_cells_steps));
            if ~isfinite(plan.gpu_min_workload_cells_steps)
                plan.gpu_min_workload_cells_steps = 0;
            end
            plan.gpu_memory_guard_pct = ExecutionResourcePlanner.clamp_percent(plan.gpu_memory_guard_pct, 1, 99, 80);

            if ~isfinite(double(plan.pool_workers_max_override))
                plan.pool_workers_max_override = 0;
            end
            plan.pool_workers_max_override = round(double(plan.pool_workers_max_override));
            if plan.pool_workers_max_override < 0
                plan.pool_workers_max_override = 0;
            end

            plan.resource_strategy = ExecutionResourcePlanner.normalize_strategy(plan.resource_strategy);
            plan.gpu_policy = ExecutionResourcePlanner.normalize_gpu_policy(plan.gpu_policy);
            plan.planner_use_host_profiles = logical(plan.planner_use_host_profiles);
        end

        function [mode_token, method_token] = resolve_context_tokens(parameters, settings, mode_hint, method_hint)
            mode_token = char(string(mode_hint));
            if isempty(strtrim(mode_token)) && isstruct(parameters)
                mode_token = char(string(ExecutionResourcePlanner.pick_field(parameters, ...
                    {'run_mode_internal', 'mode'}, '')));
            end
            if isempty(strtrim(mode_token)) && isstruct(settings)
                mode_token = char(string(ExecutionResourcePlanner.pick_field(settings, ...
                    {'run_mode_internal', 'mode'}, '')));
            end
            mode_token = lower(strtrim(mode_token));
            if isempty(mode_token)
                mode_token = 'evolution';
            end

            method_token = char(string(method_hint));
            if isempty(strtrim(method_token)) && isstruct(parameters)
                method_token = char(string(ExecutionResourcePlanner.pick_field(parameters, ...
                    {'method', 'analysis_method'}, '')));
            end
            if isempty(strtrim(method_token)) && isstruct(settings)
                method_token = char(string(ExecutionResourcePlanner.pick_field(settings, ...
                    {'method', 'analysis_method'}, '')));
            end
            method_token = ExecutionResourcePlanner.normalize_method_token(method_token);
        end

        function [plan, note_lines] = compute_effective_policy(plan, parameters, host_probe, mode_token, method_token)
            note_lines = {};

            worker_cap = max(1, round(double(host_probe.local_worker_capacity)));
            strategy_requested = ExecutionResourcePlanner.strategy_requested_workers(plan, mode_token);
            requested = max(1, round(double(plan.pool_workers)));
            if strcmp(plan.resource_strategy, 'throughput_first')
                requested = max(requested, strategy_requested);
            elseif strcmp(plan.resource_strategy, 'ui_responsive')
                requested = min(requested, strategy_requested);
            else
                if ExecutionResourcePlanner.is_interactive_mode(mode_token)
                    requested = min(requested, strategy_requested);
                else
                    requested = max(requested, strategy_requested);
                end
            end

            requested = max(1, requested);
            effective_cap = worker_cap;
            if plan.pool_workers_max_override > 0
                effective_cap = min(effective_cap, max(1, plan.pool_workers_max_override));
                note_lines{end + 1} = sprintf('worker cap override applied: %d', effective_cap); %#ok<AGROW>
            end

            effective = min(requested, effective_cap);
            plan.pool_workers_requested = requested;
            plan.pool_workers_effective = max(1, effective);

            cpu_cores = max(1, round(double(host_probe.cpu_cores)));
            if ExecutionResourcePlanner.is_interactive_mode(mode_token) && ...
                    ~strcmp(plan.resource_strategy, 'throughput_first')
                run_fraction = max(0.01, (100 - plan.interactive_thread_headroom_pct) / 100);
                total_thread_budget = max(1, floor(cpu_cores * run_fraction));
            else
                total_thread_budget = max(1, round(cpu_cores * plan.cpu_target_pct / 100));
            end
            plan.thread_cap = max(1, floor(total_thread_budget / plan.pool_workers_effective));

            requested_gpu = false;
            if isstruct(parameters) && isfield(parameters, 'use_gpu')
                requested_gpu = logical(parameters.use_gpu);
            end

            [gpu_effective, gpu_notes] = ExecutionResourcePlanner.resolve_gpu_effective( ...
                requested_gpu, plan, parameters, host_probe, method_token);
            plan.gpu_enabled_effective = gpu_effective;
            note_lines = [note_lines, gpu_notes]; %#ok<AGROW>

            if plan.pool_workers_effective < plan.pool_workers_requested
                note_lines{end + 1} = sprintf('workers clamped by host cap: %d -> %d', ...
                    plan.pool_workers_requested, plan.pool_workers_effective); %#ok<AGROW>
            end
            note_lines{end + 1} = sprintf('thread cap per worker: %d', plan.thread_cap); %#ok<AGROW>
        end

        function [gpu_effective, notes] = resolve_gpu_effective(requested_gpu, plan, parameters, host_probe, method_token)
            notes = {};
            gpu_effective = false;

            if ~requested_gpu
                notes{end + 1} = 'gpu disabled by user toggle'; %#ok<AGROW>
                return;
            end

            method_gpu_supported = strcmp(method_token, 'finite_difference');
            if ~method_gpu_supported
                if strcmp(plan.gpu_policy, 'strict')
                    error('ExecutionResourcePlanner:StrictGpuUnsupportedMethod', ...
                        'gpu_policy=strict requires an FD run; method token was "%s".', method_token);
                end
                notes{end + 1} = sprintf('gpu auto-disabled: method "%s" has no GPU path', method_token); %#ok<AGROW>
                return;
            end

            if ~logical(host_probe.has_gpu_api) || ~logical(host_probe.gpu_device_available)
                if strcmp(plan.gpu_policy, 'strict')
                    error('ExecutionResourcePlanner:StrictGpuUnavailable', ...
                        'gpu_policy=strict requested GPU but no compatible gpuDevice is available.');
                end
                notes{end + 1} = 'gpu auto-disabled: gpuDevice unavailable'; %#ok<AGROW>
                return;
            end

            [bc_supported, bc_reason] = ExecutionResourcePlanner.fd_gpu_boundary_supported(parameters);
            if ~bc_supported
                if strcmp(plan.gpu_policy, 'strict')
                    error('ExecutionResourcePlanner:StrictGpuUnsupportedBoundaryCondition', ...
                        'gpu_policy=strict requested GPU but %s.', bc_reason);
                end
                notes{end + 1} = sprintf('gpu auto-disabled: %s', bc_reason); %#ok<AGROW>
                return;
            end

            workload = ExecutionResourcePlanner.estimate_workload_cells_steps(parameters);
            if isfinite(workload) && workload < plan.gpu_min_workload_cells_steps
                if strcmp(plan.gpu_policy, 'strict')
                    gpu_effective = true;
                    notes{end + 1} = sprintf('strict GPU retained despite low workload %.3g', workload); %#ok<AGROW>
                else
                    notes{end + 1} = sprintf('gpu auto-disabled: workload %.3g < threshold %.3g', ...
                        workload, plan.gpu_min_workload_cells_steps); %#ok<AGROW>
                    return;
                end
            end

            gpu_total = double(host_probe.gpu_total_memory_bytes);
            if isfinite(gpu_total) && gpu_total > 0
                est_bytes = ExecutionResourcePlanner.estimate_runtime_memory_bytes(parameters);
                gpu_budget = gpu_total * plan.gpu_memory_guard_pct / 100;
                if isfinite(est_bytes) && est_bytes > gpu_budget
                    if strcmp(plan.gpu_policy, 'strict')
                        error('ExecutionResourcePlanner:StrictGpuMemoryBudgetExceeded', ...
                            ['gpu_policy=strict requested GPU but estimated memory %.3g GiB exceeds ' ...
                             'guard budget %.3g GiB (guard=%g%%).'], ...
                            est_bytes / 1024^3, gpu_budget / 1024^3, plan.gpu_memory_guard_pct);
                    end
                    notes{end + 1} = 'gpu auto-disabled: estimated memory exceeds guard budget'; %#ok<AGROW>
                    return;
                end
            end

            gpu_effective = true;
            notes{end + 1} = 'gpu enabled'; %#ok<AGROW>
        end

        function [tf, reason] = fd_gpu_boundary_supported(parameters)
            tf = true;
            reason = '';
            if ~isstruct(parameters)
                return;
            end

            bc_case = ExecutionResourcePlanner.normalize_bc_case( ...
                ExecutionResourcePlanner.pick_field(parameters, {'bc_case', 'boundary_condition_case'}, 'periodic'));
            if ~isempty(bc_case) && ~strcmp(bc_case, 'user_defined')
                if ~strcmp(bc_case, 'periodic')
                    tf = false;
                    reason = sprintf('FD GPU execution currently requires periodic boundaries on all sides (bc_case=%s)', bc_case);
                    return;
                end
            end

            normalized = { ...
                ExecutionResourcePlanner.resolve_side_bc_token(parameters, 'bc_top_math', 'bc_top'), ...
                ExecutionResourcePlanner.resolve_side_bc_token(parameters, 'bc_bottom_math', 'bc_bottom'), ...
                ExecutionResourcePlanner.resolve_side_bc_token(parameters, 'bc_left_math', 'bc_left'), ...
                ExecutionResourcePlanner.resolve_side_bc_token(parameters, 'bc_right_math', 'bc_right')};
            if all(cellfun(@(token) strcmp(token, 'periodic'), normalized))
                return;
            end

            tf = false;
            reason = sprintf(['FD GPU execution currently requires periodic boundaries on all sides ' ...
                '(top=%s, bottom=%s, left=%s, right=%s)'], normalized{1}, normalized{2}, normalized{3}, normalized{4});
        end

        function requested = strategy_requested_workers(plan, mode_token)
            switch plan.resource_strategy
                case 'throughput_first'
                    requested = max(1, round(double(plan.throughput_pool_workers_min)));
                case 'ui_responsive'
                    requested = max(1, round(double(plan.interactive_pool_workers_max)));
                otherwise
                    if ExecutionResourcePlanner.is_interactive_mode(mode_token)
                        requested = max(1, round(double(plan.interactive_pool_workers_max)));
                    else
                        requested = max(1, round(double(plan.throughput_pool_workers_min)));
                    end
            end
        end

        function tf = is_interactive_mode(mode_token)
            token = lower(strtrim(char(string(mode_token))));
            tf = any(strcmp(token, {'evolution', 'plotting', 'animation'}));
        end

        function [plan, profile_note] = apply_host_profile(plan, host_probe, profiles_path)
            profile_note = '';
            profile_data = ExecutionResourcePlanner.load_profiles(profiles_path);
            profiles = profile_data.profiles;
            if isempty(profiles)
                return;
            end

            hostname = lower(char(string(host_probe.hostname)));
            for i = 1:numel(profiles)
                p = profiles(i);
                if ~ExecutionResourcePlanner.profile_matches_hostname(p, hostname)
                    continue;
                end

                if ~isfield(p, 'overrides') || ~isstruct(p.overrides)
                    continue;
                end

                override_keys = fieldnames(p.overrides);
                for k = 1:numel(override_keys)
                    key = override_keys{k};
                    if isfield(plan, key)
                        plan.(key) = p.overrides.(key);
                    end
                end

                if isfield(p, 'id') && ~isempty(p.id)
                    plan.host_profile_id = char(string(p.id));
                else
                    plan.host_profile_id = sprintf('profile_%d', i);
                end
                profile_note = sprintf('host profile matched: %s', plan.host_profile_id);
                return;
            end
        end

        function tf = profile_matches_hostname(profile_entry, hostname)
            tf = false;
            if ~isfield(profile_entry, 'hostname_patterns')
                return;
            end

            patterns = profile_entry.hostname_patterns;
            if ischar(patterns) || isstring(patterns)
                patterns = {char(string(patterns))};
            elseif iscell(patterns)
                patterns = patterns(:).';
            else
                patterns = {};
            end

            for i = 1:numel(patterns)
                pattern = char(string(patterns{i}));
                if isempty(pattern)
                    continue;
                end
                rx = regexptranslate('wildcard', lower(pattern));
                if ~isempty(regexp(hostname, ['^', rx, '$'], 'once'))
                    tf = true;
                    return;
                end
            end
        end

        function method_token = normalize_method_token(raw)
            token = lower(strtrim(char(string(raw))));
            token = strrep(token, '-', '_');
            token = strrep(token, ' ', '_');
            switch token
                case {'fd', 'finite_difference', 'finite_difference_method'}
                    method_token = 'finite_difference';
                case {'spectral', 'fft', 'pseudo_spectral'}
                    method_token = 'spectral';
                case {'fv', 'finite_volume', 'finitevolume'}
                    method_token = 'finite_volume';
                otherwise
                    method_token = token;
            end
        end

        function strategy = normalize_strategy(raw)
            token = lower(strtrim(char(string(raw))));
            switch token
                case {'mode_adaptive', 'adaptive'}
                    strategy = 'mode_adaptive';
                case {'throughput_first', 'throughput'}
                    strategy = 'throughput_first';
                case {'ui_responsive', 'interactive'}
                    strategy = 'ui_responsive';
                otherwise
                    strategy = 'mode_adaptive';
            end
        end

        function policy = normalize_gpu_policy(raw)
            token = lower(strtrim(char(string(raw))));
            switch token
                case 'strict'
                    policy = 'strict';
                otherwise
                    policy = 'auto';
            end
        end

        function token = normalize_bc_token(raw)
            token = lower(strtrim(char(string(raw))));
            token = strrep(token, '_', '-');
            token = regexprep(token, '\s+', '-');
            if contains(token, 'periodic')
                token = 'periodic';
            elseif contains(token, 'no-slip') || contains(token, 'noslip') || contains(token, 'dirichlet')
                token = 'no-slip';
            elseif contains(token, 'driven')
                token = 'driven';
            elseif contains(token, 'neumann')
                token = 'neumann';
            elseif isempty(token)
                token = 'unknown';
            end
        end

        function token = normalize_bc_case(raw)
            token = lower(strtrim(char(string(raw))));
            token = strrep(token, '-', '_');
            token = regexprep(token, '\s+', '_');
            token = regexprep(token, '^case_*', '');
            switch token
                case {'', 'unknown'}
                    token = '';
                case {'1'}
                    token = 'lid_driven_cavity';
                case {'2'}
                    token = 'driven_channel_flow';
                case {'3', 'lid_and_bottom_driven_cavity', 'enclosed_shear', 'shear_layer_enclosed', 'opposed_wall_shear'}
                    token = 'enclosed_shear_layer';
                case {'4'}
                    token = 'enclosed_cavity';
                case {'5', 'periodic_all'}
                    token = 'periodic';
                case {'6'}
                    token = 'user_defined';
                case {'pinned_dirichlet_box', 'dirichlet_box'}
                    token = 'pinned_box';
                case {'noflux_box', 'neumann_box'}
                    token = 'no_flux_box';
            end
        end

        function token = resolve_side_bc_token(parameters, math_field, display_field)
            math_token = ExecutionResourcePlanner.normalize_bc_token( ...
                ExecutionResourcePlanner.pick_field(parameters, {math_field}, ''));
            display_token = ExecutionResourcePlanner.normalize_bc_token( ...
                ExecutionResourcePlanner.pick_field(parameters, {display_field}, ''));
            if ~isempty(display_token) && ~strcmp(display_token, 'unknown') && ~strcmp(display_token, 'periodic')
                token = display_token;
                return;
            end
            if ~isempty(math_token) && ~strcmp(math_token, 'unknown')
                token = math_token;
                return;
            end
            if ~isempty(display_token) && ~strcmp(display_token, 'unknown')
                token = display_token;
                return;
            end
            token = 'periodic';
        end

        function value = pick_field(s, keys, fallback)
            value = fallback;
            if ~isstruct(s)
                return;
            end
            for i = 1:numel(keys)
                key = keys{i};
                if isfield(s, key)
                    value = s.(key);
                    return;
                end
            end
        end

        function n = resolve_available_threads()
            try
                n = max(1, feature('numcores'));
            catch
                n = 1;
            end
        end

        function n = resolve_local_worker_capacity()
            n = 1;
            try
                c = parcluster('local');
                if isprop(c, 'NumWorkers')
                    n = max(1, round(double(c.NumWorkers)));
                end
            catch
                n = ExecutionResourcePlanner.resolve_available_threads();
            end
        end

        function topology = resolve_worker_topology(worker_count)
            w = max(1, round(double(worker_count)));
            if w >= 3
                topology = 'adaptive_3worker_split';
            elseif w == 2
                topology = 'adaptive_2worker_combined';
            else
                topology = 'single_worker_background';
            end
        end

        function out = clamp_percent(val, low, high, fallback)
            out = fallback;
            x = double(val);
            if isfinite(x)
                out = min(max(x, low), high);
            end
        end

        function out = clamp_positive(val, fallback, low, high)
            out = fallback;
            x = double(val);
            if isfinite(x)
                out = min(max(x, low), high);
            end
        end

        function out = clamp_int(val, fallback, low, high)
            out = fallback;
            x = round(double(val));
            if isfinite(x)
                out = min(max(x, low), high);
            end
        end

        function workload = estimate_workload_cells_steps(parameters)
            workload = NaN;
            if ~isstruct(parameters)
                return;
            end
            if ~isfield(parameters, 'Nx') || ~isfield(parameters, 'Ny')
                return;
            end
            Nx = max(1, round(double(parameters.Nx)));
            Ny = max(1, round(double(parameters.Ny)));

            Nt = NaN;
            if isfield(parameters, 'Nt')
                Nt = double(parameters.Nt);
            elseif isfield(parameters, 'Tfinal') && isfield(parameters, 'dt')
                Tf = double(parameters.Tfinal);
                dt = double(parameters.dt);
                if isfinite(Tf) && isfinite(dt) && dt > 0
                    Nt = round(Tf / dt);
                end
            end
            if ~isfinite(Nt) || Nt < 1
                Nt = 1;
            end

            workload = double(Nx) * double(Ny) * double(Nt);
        end

        function est_bytes = estimate_runtime_memory_bytes(parameters)
            est_bytes = NaN;
            if ~isstruct(parameters)
                return;
            end
            if ~isfield(parameters, 'Nx') || ~isfield(parameters, 'Ny')
                return;
            end

            Nx = max(1, round(double(parameters.Nx)));
            Ny = max(1, round(double(parameters.Ny)));
            n_snap = 1;
            if isfield(parameters, 'num_snapshots')
                n_snap = max(n_snap, round(double(parameters.num_snapshots)));
            end
            if isfield(parameters, 'num_plot_snapshots')
                n_snap = max(n_snap, round(double(parameters.num_plot_snapshots)));
            end

            bytes_per_double = 8;
            core_field_count = 18;
            snapshot_field_count = 2;
            est_bytes = 1.25 * Nx * Ny * bytes_per_double * (core_field_count + snapshot_field_count * n_snap);
        end

        function out = first_nonempty(candidates, fallback)
            out = fallback;
            for i = 1:numel(candidates)
                candidate = char(string(candidates{i}));
                if ~isempty(candidate)
                    out = candidate;
                    return;
                end
            end
        end
    end
end
