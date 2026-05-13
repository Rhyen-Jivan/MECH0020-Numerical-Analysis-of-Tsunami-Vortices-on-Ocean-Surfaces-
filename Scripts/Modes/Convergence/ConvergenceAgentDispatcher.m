classdef ConvergenceAgentDispatcher
    % ConvergenceAgentDispatcher Contract wrapper for convergence agent dispatch.
    %
    % Purpose:
    %   - Resolve dispatch strategy/objective from convergence settings.
    %   - Build an explicit, serializable request payload for agent runs.
    %   - Execute the agent-guided branch through a single entrypoint.

    methods (Static)
        function info = resolve_dispatch_info(parameters)
            info = struct();
            info.strategy = 'standard';
            info.progress_telemetry = true;
            info.objective_mode = 'balanced';

            has_explicit_strategy = false;
            if isfield(parameters, 'convergence') && isstruct(parameters.convergence)
                conv_cfg = parameters.convergence;
                if isfield(conv_cfg, 'dispatch') && isstruct(conv_cfg.dispatch)
                    dispatch_cfg = conv_cfg.dispatch;
                    if isfield(dispatch_cfg, 'strategy') && ~isempty(dispatch_cfg.strategy)
                        info.strategy = lower(strtrim(char(string(dispatch_cfg.strategy))));
                        has_explicit_strategy = true;
                    end
                    if isfield(dispatch_cfg, 'progress_telemetry') && ~isempty(dispatch_cfg.progress_telemetry)
                        info.progress_telemetry = logical(dispatch_cfg.progress_telemetry);
                    end
                end
                if isfield(conv_cfg, 'agent') && isstruct(conv_cfg.agent)
                    agent_cfg = conv_cfg.agent;
                    if isfield(agent_cfg, 'objective_mode') && ~isempty(agent_cfg.objective_mode)
                        info.objective_mode = lower(strtrim(char(string(agent_cfg.objective_mode))));
                    end
                    if ~has_explicit_strategy && isfield(agent_cfg, 'enabled') && logical(agent_cfg.enabled) && strcmp(info.strategy, 'standard')
                        info.strategy = 'agent_guided';
                    end
                end
            elseif isfield(parameters, 'convergence_agent_enabled') && logical(parameters.convergence_agent_enabled)
                info.strategy = 'agent_guided';
            end

            if any(strcmp(info.strategy, {'adaptive', 'agent'}))
                info.strategy = 'agent_guided';
            end
            if ~any(strcmp(info.strategy, {'standard', 'agent_guided'}))
                error('ConvergenceAgentDispatcher:UnsupportedDispatchStrategy', ...
                    'Unsupported convergence dispatch strategy ''%s''.', info.strategy);
            end
        end

        function tf = is_agent_guided(dispatch_info)
            tf = isstruct(dispatch_info) && isfield(dispatch_info, 'strategy') && ...
                strcmpi(char(string(dispatch_info.strategy)), 'agent_guided');
        end

        function request = build_dispatch_request(run_config, parameters, settings, paths, dispatch_info)
            if nargin < 5 || isempty(dispatch_info)
                dispatch_info = ConvergenceAgentDispatcher.resolve_dispatch_info(parameters);
            end

            run_id = '';
            if isfield(run_config, 'study_id') && ~isempty(run_config.study_id)
                run_id = char(string(run_config.study_id));
            elseif isfield(run_config, 'run_id') && ~isempty(run_config.run_id)
                run_id = char(string(run_config.run_id));
            end

            [n_coarse, n_max] = ConvergenceAgentDispatcher.resolve_mesh_bounds(parameters);
            request = struct();
            request.channel = 'convergence_dispatch';
            request.dispatch_strategy = char(string(dispatch_info.strategy));
            request.objective_mode = char(string(dispatch_info.objective_mode));
            request.progress_telemetry = logical(dispatch_info.progress_telemetry);
            request.run_id = run_id;
            request.method = char(string(run_config.method));
            request.mode = 'convergence';
            request.ic_type = char(string(ConvergenceAgentDispatcher.pick_field(parameters, {'ic_type'}, 'Stretched Gaussian')));
            request.target_tolerance = ConvergenceAgentDispatcher.resolve_tolerance(parameters);
            request.mesh_bounds = struct('n_coarse', n_coarse, 'n_max', n_max);
            request.runtime_hints = struct( ...
                'pool_workers', ConvergenceAgentDispatcher.pick_field(parameters, {'pool_workers'}, NaN), ...
                'gpu_enabled', ConvergenceAgentDispatcher.pick_field(parameters, {'gpu_enabled'}, false), ...
                'cpu_allocation_pct', ConvergenceAgentDispatcher.pick_field(parameters, {'cpu_allocation_pct'}, NaN), ...
                'memory_allocation_pct', ConvergenceAgentDispatcher.pick_field(parameters, {'memory_allocation_pct'}, NaN), ...
                'gpu_allocation_pct', ConvergenceAgentDispatcher.pick_field(parameters, {'gpu_allocation_pct'}, NaN), ...
                'thread_count', maxNumCompThreads);
            request.paths = struct( ...
                'data', ConvergenceAgentDispatcher.pick_field(paths, {'data'}, ''), ...
                'figures_convergence', ConvergenceAgentDispatcher.pick_field(paths, {'figures_convergence'}, ''), ...
                'reports', ConvergenceAgentDispatcher.pick_field(paths, {'reports'}, ''));
            request.settings = struct( ...
                'save_data', logical(ConvergenceAgentDispatcher.pick_field(settings, {'save_data'}, false)), ...
                'save_figures', logical(ConvergenceAgentDispatcher.pick_field(settings, {'save_figures'}, false)), ...
                'save_reports', logical(ConvergenceAgentDispatcher.pick_field(settings, {'save_reports'}, false)));
            request.timestamp_utc = char(datetime('now', 'TimeZone', 'UTC', 'Format', 'yyyy-MM-dd''T''HH:mm:ss''Z'''));
        end

        function [results, request] = dispatch(run_config, parameters, settings, paths, dispatch_info)
            if nargin < 5 || isempty(dispatch_info)
                dispatch_info = ConvergenceAgentDispatcher.resolve_dispatch_info(parameters);
            end
            if ~ConvergenceAgentDispatcher.is_agent_guided(dispatch_info)
                error('ConvergenceAgentDispatcher:DispatchNotAgentGuided', ...
                    'Dispatch requested for non-agent strategy ''%s''.', char(string(dispatch_info.strategy)));
            end

            request = ConvergenceAgentDispatcher.build_dispatch_request( ...
                run_config, parameters, settings, paths, dispatch_info);
            results = run_agent_guided_convergence( ...
                run_config, parameters, settings, paths, dispatch_info, request);
        end
    end

    methods (Static, Access = private)
        function [n_coarse, n_max] = resolve_mesh_bounds(parameters)
            n_coarse = ConvergenceAgentDispatcher.pick_field(parameters, {'convergence_N_coarse'}, 32);
            n_max = ConvergenceAgentDispatcher.pick_field(parameters, {'convergence_N_max'}, 128);
            if isfield(parameters, 'convergence') && isstruct(parameters.convergence) && ...
                    isfield(parameters.convergence, 'study') && isstruct(parameters.convergence.study)
                study = parameters.convergence.study;
                n_coarse = ConvergenceAgentDispatcher.pick_field(study, {'N_coarse'}, n_coarse);
                n_max = ConvergenceAgentDispatcher.pick_field(study, {'N_max'}, n_max);
            end
            n_coarse = max(8, round(double(n_coarse)));
            n_max = max(n_coarse, round(double(n_max)));
        end

        function tol = resolve_tolerance(parameters)
            tol = ConvergenceAgentDispatcher.pick_field(parameters, {'convergence_tol'}, 5e-2);
            if isfield(parameters, 'convergence') && isstruct(parameters.convergence) && ...
                    isfield(parameters.convergence, 'study') && isstruct(parameters.convergence.study)
                tol = ConvergenceAgentDispatcher.pick_field(parameters.convergence.study, {'tolerance'}, tol);
            end
            if ~isnumeric(tol) || ~isscalar(tol) || ~isfinite(tol) || tol <= 0
                tol = 5e-2;
            end
            tol = double(tol);
        end

        function out = pick_field(source, keys, fallback)
            out = fallback;
            if ~isstruct(source) || isempty(keys)
                return;
            end
            for i = 1:numel(keys)
                key = char(string(keys{i}));
                if isfield(source, key)
                    value = source.(key);
                    if ~isempty(value)
                        out = value;
                        return;
                    end
                end
            end
        end
    end
end
