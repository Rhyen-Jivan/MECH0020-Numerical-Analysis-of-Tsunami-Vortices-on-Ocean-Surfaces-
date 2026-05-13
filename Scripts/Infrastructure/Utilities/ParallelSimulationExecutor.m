classdef ParallelSimulationExecutor < handle
    % PARALLELSIMULATIONEXECUTOR - Runs simulations in parallel with UI monitoring
    %
    % Purpose:
    %   Executes tsunami simulations in a background worker using parfeval
    %   Allows UI to remain responsive during simulation
    %   Provides independent monitoring timer for metrics collection
    %
    % Usage:
    %   executor = ParallelSimulationExecutor(ui_callback);
    %   executor.start(run_config, parameters, settings);
    %   ... UI remains responsive ...
    %   [results, paths] = executor.wait_for_completion();
    %   [results, paths] = executor.wait_for_completion(@() pump_ui_updates());

    properties
        ui_progress_callback    % Callback for UI updates
        monitor_timer          % Timer for independent monitoring
        future_obj             % Future object from parfeval
        progress_data_queue    % Worker -> client live progress channel
        pending_ui_payloads    % Buffered UI payloads drained on the host wait path
        is_running             % Simulation running flag
        start_time             % Simulation start timestamp
        last_update_time       % Last UI update timestamp
        system_metrics         % Collected system metrics
        shared_data            % Shared data between workers
        resource_policy        % CPU/GPU/memory allocation policy
        latest_progress_pct    % Latest worker-reported progress percentage
        last_cpu_probe_tic     % CPU probe timer reference
        payload_sequence_id    % Monotonic sequence across solver/metrics/report payloads
        collector_dispatcher   % Host-side external collector dispatcher
        cleanup_in_progress    % Idempotent cleanup guard
        synchronous_execution  % True when the active run is on the host thread
        last_monitor_poll_tic  % Manual polling cadence for synchronous runs
        current_run_config     % Active run configuration snapshot
        current_parameters     % Active parameter snapshot
        current_settings       % Active settings snapshot used for publication
        terminal_state         % Final executor state remembered across cleanup
        terminal_failure_error % Wrapped worker failure preserved across cleanup
        raw_hwinfo_csv_session_path % Host-side staged HWiNFO CSV during the run
        raw_hwinfo_csv_initialized  % True once the staged CSV header exists
        latest_collector_sample % Richest normalized collector sample seen during the run
    end

    methods
        function obj = ParallelSimulationExecutor(ui_callback)
            % Constructor
            obj.ui_progress_callback = ui_callback;
            obj.is_running = false;
            obj.system_metrics = struct();
            obj.system_metrics.cpu_usage = [];
            obj.system_metrics.memory_usage = [];
            obj.system_metrics.timestamps = [];
            obj.shared_data = struct();
            obj.resource_policy = ParallelSimulationExecutor.default_resource_policy();
            obj.latest_progress_pct = 0;
            obj.last_cpu_probe_tic = tic;
            obj.payload_sequence_id = 0;
            obj.pending_ui_payloads = {};
            obj.collector_dispatcher = [];
            obj.cleanup_in_progress = false;
            obj.synchronous_execution = false;
            obj.last_monitor_poll_tic = tic;
            obj.current_run_config = struct();
            obj.current_parameters = struct();
            obj.current_settings = struct();
            obj.terminal_state = '';
            obj.terminal_failure_error = [];
            obj.raw_hwinfo_csv_session_path = '';
            obj.raw_hwinfo_csv_initialized = false;
            obj.latest_collector_sample = struct();
        end

        function start(obj, run_config, parameters, settings)
            % Start simulation in background worker

            if obj.is_running
                error('ParallelSimulationExecutor:AlreadyRunning', ...
                    'A simulation is already running in this executor instance.');
            end

            % Initialize state
            obj.is_running = true;
            obj.start_time = datetime('now');
            obj.last_update_time = tic;
            obj.last_cpu_probe_tic = tic;
            obj.latest_progress_pct = 0;
            obj.payload_sequence_id = 0;
            obj.synchronous_execution = false;
            obj.last_monitor_poll_tic = tic;
            obj.shared_data = struct();
            obj.current_run_config = run_config;
            obj.current_parameters = parameters;
            obj.current_settings = struct();
            obj.terminal_state = '';
            obj.terminal_failure_error = [];
            obj.pending_ui_payloads = {};
            obj.raw_hwinfo_csv_session_path = ParallelSimulationExecutor.resolve_raw_hwinfo_session_csv_path(settings, run_config);
            obj.raw_hwinfo_csv_initialized = false;
            obj.latest_collector_sample = struct();
            obj.resource_policy = ParallelSimulationExecutor.resolve_resource_policy(parameters, settings, run_config);
            settings.resource_allocation = obj.resource_policy;
            if isfield(obj.resource_policy, 'gpu_enabled_effective')
                parameters.use_gpu = logical(obj.resource_policy.gpu_enabled_effective);
            end
            obj.current_parameters = parameters;
            obj.current_settings = ParallelSimulationExecutor.sanitize_settings_for_publication(settings);

            if ParallelSimulationExecutor.phase_csv_mode_requested(settings, run_config)
                obj.collector_dispatcher = [];
            else
                try
                    obj.collector_dispatcher = ExternalCollectorDispatcher(settings);
                    obj.collector_dispatcher.start_live_session( ...
                        ParallelSimulationExecutor.resolve_run_id(run_config), settings);
                    obj.prime_collector_sample();
                catch ME
                    obj.collector_dispatcher = [];
                    ParallelSimulationExecutor.warn_once('ParallelSimulationExecutor:CollectorStartFailed', ...
                        'External collector dispatcher could not start: %s', ME.message);
                end
            end

            % Fail fast when requested memory budget cannot safely accommodate run buffers.
            ParallelSimulationExecutor.apply_memory_guard(parameters, obj.resource_policy);

            requested_workers = 1;
            if isfield(obj.resource_policy, 'pool_workers_effective')
                requested_workers = max(1, round(double(obj.resource_policy.pool_workers_effective)));
            elseif isfield(obj.resource_policy, 'target_pool_workers')
                requested_workers = max(1, round(double(obj.resource_policy.target_pool_workers)));
            end

            force_synchronous = false;
            if isfield(settings, 'force_synchronous_execution')
                force_synchronous = logical(settings.force_synchronous_execution);
            elseif isfield(obj.resource_policy, 'force_synchronous_execution')
                force_synchronous = logical(obj.resource_policy.force_synchronous_execution);
            end
            if force_synchronous
                obj.emit_runtime_log('Synchronous execution explicitly requested. Running on the host thread.', 'warning');
                obj.synchronous_execution = true;
                obj.start_monitor_timer();
                obj.run_synchronous(run_config, parameters, settings);
                return;
            end

            % Check for parallel pool
            try
                pool = obj.ensure_pool();
            catch ME
                obj.emit_runtime_log(sprintf('Parallel pool unavailable (%s). Running synchronously.', ME.message), 'warning');
                obj.synchronous_execution = true;
                obj.start_monitor_timer();
                obj.run_synchronous(run_config, parameters, settings);
                return;
            end

            if requested_workers <= 1
                obj.emit_runtime_log('Single-worker execution selected. Running via background worker.', 'info');
            end

            % Strip non-serializable fields (closures capturing UI
            % handles) before sending to the worker.
            worker_settings = settings;
            if isfield(worker_settings, 'ui_progress_callback')
                worker_settings = rmfield(worker_settings, 'ui_progress_callback');
            end

            % Dedicated worker -> UI channel for live progress payloads.
            obj.progress_data_queue = parallel.pool.PollableDataQueue;
            worker_settings.progress_data_queue = obj.progress_data_queue;
            worker_settings.resource_allocation = obj.resource_policy;

            % Start simulation in background using the canonical dispatcher.
            try
                obj.future_obj = parfeval(pool, ...
                    @RunDispatcher, 2, ...
                    run_config, parameters, worker_settings);
            catch ME
                obj.emit_runtime_log(sprintf('Error starting parallel simulation: %s', ME.message), 'error');
                obj.is_running = false;
                rethrow(ME);
            end

            obj.start_monitor_timer();
        end

        function [results, paths] = wait_for_completion(obj, pump_callback)
            % Wait for simulation to complete and return results
            if nargin < 2 || isempty(pump_callback)
                pump_callback = [];
            end

            try
                while true
                    completion = obj.poll_completion(pump_callback);
                    if completion.done
                        results = completion.results;
                        paths = completion.paths;
                        switch completion.state
                            case 'finished'
                                return;
                            case 'failed'
                                throw(completion.error);
                            case 'cancelled'
                                error('ParallelSimulationExecutor:Cancelled', ...
                                    'Simulation was cancelled before completion.');
                            otherwise
                                error('ParallelSimulationExecutor:UnexpectedCompletionState', ...
                                    'Unexpected completion state %s.', char(string(completion.state)));
                        end
                    end
                    pause(0.02);
                    drawnow limitrate;
                end
            catch ME
                % Clean up on error
                obj.cleanup();
                rethrow(ME);
            end
        end

        function completion = poll_completion(obj, pump_callback)
            % Non-blocking completion probe used by async UI launch paths.
            if nargin < 2 || isempty(pump_callback)
                pump_callback = [];
            end

            completion = struct( ...
                'done', false, ...
                'state', 'running', ...
                'results', struct(), ...
                'paths', struct(), ...
                'error', []);

            obj.drain_progress_queue();
            obj.dispatch_pending_ui_payloads();
            obj.invoke_pump_callback(pump_callback);

            % Synchronous fallback path may complete before the host polls it.
            if ~obj.is_running && isfield(obj.shared_data, 'results') && isfield(obj.shared_data, 'paths')
                [completion.results, completion.paths] = obj.finalize_results_payload( ...
                    obj.shared_data.results, obj.shared_data.paths, obj.shared_data);
                obj.terminal_state = 'finished';
                obj.terminal_failure_error = [];
                obj.shared_data = struct();
                obj.cleanup();
                obj.dispatch_pending_ui_payloads();
                obj.invoke_pump_callback(pump_callback);
                completion.done = true;
                completion.state = 'finished';
                return;
            end

            if ~obj.is_running
                terminal_state = lower(char(string(obj.terminal_state)));
                switch terminal_state
                    case 'failed'
                        completion.done = true;
                        completion.state = 'failed';
                        if isempty(obj.terminal_failure_error)
                            completion.error = MException( ...
                                'ParallelSimulationExecutor:MissingFailureError', ...
                                'Background worker failed without a captured terminal error.');
                        else
                            completion.error = obj.terminal_failure_error;
                        end
                        return;
                    case 'cancelled'
                        completion.done = true;
                        completion.state = 'cancelled';
                        return;
                    otherwise
                        error('ParallelSimulationExecutor:NoRunningSimulation', ...
                            'No simulation is currently running.');
                end
            end

            if isempty(obj.future_obj)
                error('ParallelSimulationExecutor:MissingFuture', ...
                    'Background future handle is missing.');
            end

            state = lower(char(string(obj.future_obj.State)));
            switch state
                case 'finished'
                    failure_error = [];
                    try
                        failure_error = obj.future_obj.Error;
                    catch
                        failure_error = [];
                    end
                    if ~isempty(failure_error)
                        obj.emit_failure_payload(failure_error);
                        wrapped_error = ParallelSimulationExecutor.wrap_worker_failure(failure_error);
                        obj.terminal_state = 'failed';
                        obj.terminal_failure_error = wrapped_error;
                        obj.cleanup();
                        obj.dispatch_pending_ui_payloads();
                        obj.invoke_pump_callback(pump_callback);
                        completion.done = true;
                        completion.state = 'failed';
                        completion.error = wrapped_error;
                        return;
                    end

                    obj.terminal_state = 'finished';
                    obj.terminal_failure_error = [];
                    try
                        [results, paths] = fetchOutputs(obj.future_obj);
                    catch ME
                        failure_error = ME;
                        try
                            worker_error = obj.future_obj.Error;
                            if ~isempty(worker_error)
                                failure_error = worker_error;
                            end
                        catch
                        end
                        obj.emit_failure_payload(failure_error);
                        wrapped_error = ParallelSimulationExecutor.wrap_worker_failure(failure_error);
                        obj.terminal_state = 'failed';
                        obj.terminal_failure_error = wrapped_error;
                        obj.cleanup();
                        obj.dispatch_pending_ui_payloads();
                        obj.invoke_pump_callback(pump_callback);
                        completion.done = true;
                        completion.state = 'failed';
                        completion.error = wrapped_error;
                        return;
                    end
                    [completion.results, completion.paths] = obj.finalize_results_payload(results, paths);
                    obj.cleanup();
                    obj.dispatch_pending_ui_payloads();
                    obj.invoke_pump_callback(pump_callback);
                    completion.done = true;
                    completion.state = 'finished';
                case 'failed'
                    failure_error = obj.future_obj.Error;
                    obj.emit_failure_payload(failure_error);
                    wrapped_error = ParallelSimulationExecutor.wrap_worker_failure(failure_error);
                    obj.terminal_state = 'failed';
                    obj.terminal_failure_error = wrapped_error;
                    obj.cleanup();
                    obj.dispatch_pending_ui_payloads();
                    obj.invoke_pump_callback(pump_callback);
                    completion.done = true;
                    completion.state = 'failed';
                    completion.error = wrapped_error;
                case 'cancelled'
                    obj.terminal_state = 'cancelled';
                    obj.terminal_failure_error = [];
                    obj.cleanup();
                    obj.dispatch_pending_ui_payloads();
                    obj.invoke_pump_callback(pump_callback);
                    completion.done = true;
                    completion.state = 'cancelled';
                otherwise
                    completion.state = 'running';
            end
        end

        function cancel(obj)
            % Cancel running simulation

            if ~obj.is_running
                return;
            end

            try
                if ~isempty(obj.future_obj)
                    cancel(obj.future_obj);
                end
            catch ME
                ParallelSimulationExecutor.warn_once('ParallelSimulationExecutor:CancelFailed', ...
                    'Non-fatal future cancellation error during cancel(): %s', ME.message);
            end

            obj.cleanup();
        end

        function progress = get_progress(obj)
            % Get current simulation progress (0 to 1)

            if ~obj.is_running
                if isfield(obj.shared_data, 'results')
                    progress = 1;
                else
                    progress = 0;
                end
                return;
            end

            progress = max(0, min(1, obj.latest_progress_pct / 100));
        end

        function delete(obj)
            try
                obj.cleanup();
            catch
            end
        end

        function cleanup(obj)
            obj.cleanup_internal();
        end
    end

    methods (Access = private)
        function [results, paths] = finalize_results_payload(obj, results, paths, shared_data)
            if nargin < 4 || ~isstruct(shared_data)
                shared_data = struct();
            end
            if isfield(shared_data, 'collector_samples')
                results.collector_samples = shared_data.collector_samples;
                if isstruct(shared_data.collector_samples) && ...
                        isfield(shared_data.collector_samples, 'post') && ...
                        isstruct(shared_data.collector_samples.post)
                    results.collector_last_sample = shared_data.collector_samples.post;
                end
            end
            results = obj.attach_collector_summary(results);
            [paths, results] = obj.promote_raw_hwinfo_csv(paths, results);
            obj.emit_report_payload(results, paths);
            obj.emit_final_collector_payload(results);
        end

        function pool = ensure_pool(obj)
            % Ensure a pool exists and honor minimum worker policy.
            target_workers = 1;
            max_workers = inf;
            if isfield(obj.resource_policy, 'pool_workers_effective')
                target_workers = max(1, round(double(obj.resource_policy.pool_workers_effective)));
            elseif isfield(obj.resource_policy, 'target_pool_workers')
                target_workers = max(1, round(double(obj.resource_policy.target_pool_workers)));
            end
            if isfield(obj.resource_policy, 'max_pool_workers')
                max_workers = round(double(obj.resource_policy.max_pool_workers));
                if isfinite(max_workers) && max_workers >= 1
                    target_workers = min(target_workers, max_workers);
                end
            end
            pool = ParallelSimulationExecutor.ensure_pool_for_target(target_workers, max_workers);
        end

        function handle_worker_progress(obj, payload)
            % Receive runtime progress payloads and relay to the UI.
            obj.relay_progress_payload(payload);
        end

        function drain_progress_queue(obj, max_payloads)
            if nargin < 2 || isempty(max_payloads)
                max_payloads = inf;
            end
            if isempty(obj.progress_data_queue)
                return;
            end
            delivered = 0;
            while delivered < max_payloads
                payload = poll(obj.progress_data_queue, 0);
                if isempty(payload)
                    break;
                end
                obj.handle_worker_progress(payload);
                delivered = delivered + 1;
            end
        end

        function relay_progress_payload(obj, payload)
            if ~isstruct(payload)
                return;
            end

            if ~isfield(payload, 'channel') || isempty(payload.channel)
                payload.channel = 'solver';
            end
            payload = obj.stamp_payload(payload);

            if isfield(payload, 'progress_pct')
                progress_pct = double(payload.progress_pct);
                if isfinite(progress_pct)
                    obj.latest_progress_pct = min(max(progress_pct, 0), 100);
                end
            end
            if isfield(payload, 'workflow_overall_progress_pct')
                progress_pct = double(payload.workflow_overall_progress_pct);
                if isfinite(progress_pct)
                    obj.latest_progress_pct = min(max(progress_pct, 0), 100);
                end
            end
            if isfield(payload, 'iteration') && isfield(payload, 'total_iterations')
                iter = double(payload.iteration);
                total_iter = double(payload.total_iterations);
                if isfinite(iter) && isfinite(total_iter) && total_iter > 0
                    obj.latest_progress_pct = 100 * min(max(iter / total_iter, 0), 1);
                end
            end

            obj.queue_ui_payload(payload);

            if obj.synchronous_execution
                obj.dispatch_pending_ui_payloads();
                obj.maybe_poll_synchronous_metrics();
                drawnow limitrate;
            end
        end

        function emit_runtime_log(obj, message, msg_type)
            if nargin < 3 || strlength(string(msg_type)) == 0
                msg_type = 'info';
            end
            message = char(string(message));
            if isempty(strtrim(message))
                return;
            end

            if ~isempty(obj.ui_progress_callback)
                obj.relay_progress_payload(struct( ...
                    'channel', 'log', ...
                    'log_type', char(string(msg_type)), ...
                    'log_message', message));
                return;
            end

            fprintf('%s\n', message);
        end

        function monitor_callback(obj)
            % Called by timer to update UI and collect metrics

            if ~obj.is_running
                return;
            end

            % In async UI mode, the host timer is the only place that can
            % continuously drain worker progress and push it into the live
            % monitor before completion.
            obj.drain_progress_queue(256);

            % Collect system metrics
            try
                % Get memory usage
                mem_info = memory;
                mem_used_mb = mem_info.MemUsedMATLAB / 1024^2;

                % Get CPU usage (Windows-specific, sampled at a lower cadence).
                if ispc
                    cpu_usage = NaN;
                    if toc(obj.last_cpu_probe_tic) >= obj.resource_policy.cpu_probe_interval_seconds
                        [~, cpu_str] = system('wmic cpu get loadpercentage');
                        cpu_lines = strsplit(strtrim(cpu_str), '\n');
                        if numel(cpu_lines) >= 2
                            cpu_usage = str2double(cpu_lines{2});
                        end
                        obj.last_cpu_probe_tic = tic;
                    end
                else
                    cpu_usage = NaN;  % Platform-specific implementation needed
                end

                % Store metrics
                obj.system_metrics.cpu_usage(end+1) = cpu_usage;
                obj.system_metrics.memory_usage(end+1) = mem_used_mb;
                obj.system_metrics.timestamps(end+1) = toc(obj.last_update_time);

                collector_sample = struct();
                if ~isempty(obj.collector_dispatcher)
                    collector_sample = obj.collector_dispatcher.poll_latest_sample();
                    collector_sample = ExternalCollectorDispatcher.normalize_collector_payload(collector_sample);
                    obj.remember_collector_sample(collector_sample);
                    collector_sample = ParallelSimulationExecutor.prefer_richer_collector_sample( ...
                        collector_sample, obj.latest_collector_sample);
                    obj.append_raw_hwinfo_csv_row(collector_sample);
                end

                gpu_proxy = ParallelSimulationExecutor.gpu_usage_proxy();
                estimated_power = ParallelSimulationExecutor.estimate_power_from_cpu(cpu_usage);
                unified_cpu = ParallelSimulationExecutor.pick_collector_metric_or_series(collector_sample, 'cpu_proxy', cpu_usage);
                unified_gpu = ParallelSimulationExecutor.pick_collector_metric_or_series(collector_sample, 'gpu_series', gpu_proxy);
                unified_temp = ParallelSimulationExecutor.pick_collector_metric_or_series(collector_sample, 'cpu_temp_c', NaN);
                unified_power = ParallelSimulationExecutor.pick_collector_metric_or_series(collector_sample, 'power_w', estimated_power);
                unified_memory = ParallelSimulationExecutor.pick_collector_metric_or_series(collector_sample, 'memory_series', mem_used_mb);

                % Create metrics payload for UI
                payload = struct();
                payload.channel = 'metrics';
                payload.cpu_usage = unified_cpu;
                payload.memory_usage = unified_memory;
                payload.gpu_usage = unified_gpu;
                payload.cpu_temp_c = unified_temp;
                payload.power_w = unified_power;
                payload.matlab_cpu_usage = cpu_usage;
                payload.matlab_memory_usage = mem_used_mb;
                payload.matlab_gpu_usage = gpu_proxy;
                payload.matlab_power_w = estimated_power;
                payload.elapsed_time = seconds(datetime('now') - obj.start_time);
                payload.wall_clock_time = posixtime(datetime('now', 'TimeZone', 'UTC'));
                payload.is_background_update = true;  % Flag for UI to know this is a timer update
                payload.resource_policy = obj.resource_policy;
                payload.progress_pct = obj.latest_progress_pct;
                if isstruct(collector_sample) && ~isempty(fieldnames(collector_sample))
                    if isfield(collector_sample, 'collector_series')
                        payload.collector_series = collector_sample.collector_series;
                    end
                    if isfield(collector_sample, 'collector_status')
                        payload.collector_status = collector_sample.collector_status;
                    end
                    if isfield(collector_sample, 'coverage_domains')
                        payload.coverage_domains = collector_sample.coverage_domains;
                    end
                    if isfield(collector_sample, 'preferred_source')
                        payload.preferred_source = collector_sample.preferred_source;
                    end
                    if isfield(collector_sample, 'raw_log_paths')
                        payload.raw_log_paths = collector_sample.raw_log_paths;
                    end
                    if isfield(collector_sample, 'overlay_metrics')
                        payload.overlay_metrics = collector_sample.overlay_metrics;
                    end
                    if isfield(collector_sample, 'collector_metric_catalog')
                        payload.collector_metric_catalog = collector_sample.collector_metric_catalog;
                    end
                    if isfield(collector_sample, 'hwinfo_transport')
                        payload.hwinfo_transport = collector_sample.hwinfo_transport;
                    end
                    if isfield(collector_sample, 'hwinfo_status_reason')
                        payload.hwinfo_status_reason = collector_sample.hwinfo_status_reason;
                    end
                    if isfield(collector_sample, 'collector_probe_details')
                        payload.collector_probe_details = collector_sample.collector_probe_details;
                    end
                end
                payload = obj.stamp_payload(payload);

                obj.queue_ui_payload(payload);
                obj.dispatch_pending_ui_payloads();

            catch ME
                ParallelSimulationExecutor.warn_once('ParallelSimulationExecutor:MonitorCallbackFailed', ...
                    'Monitoring callback failed: %s', ME.message);
            end
        end

        function queue_ui_payload(obj, payload)
            if isempty(obj.ui_progress_callback) || ~isstruct(payload)
                return;
            end
            if ~iscell(obj.pending_ui_payloads)
                obj.pending_ui_payloads = {};
            end
            obj.pending_ui_payloads{end + 1} = payload;
        end

        function dispatch_pending_ui_payloads(obj)
            if isempty(obj.ui_progress_callback)
                obj.pending_ui_payloads = {};
                return;
            end
            if ~iscell(obj.pending_ui_payloads) || isempty(obj.pending_ui_payloads)
                return;
            end

            payloads = obj.pending_ui_payloads;
            obj.pending_ui_payloads = {};
            for idx = 1:numel(payloads)
                payload = payloads{idx};
                if ~isstruct(payload)
                    continue;
                end
                try
                    invoke_runtime_progress_callback(obj.ui_progress_callback, payload);
                catch ME
                    ParallelSimulationExecutor.warn_once('ParallelSimulationExecutor:UIProgressRelayFailed', ...
                        'UI progress relay failed: %s', ME.message);
                end
            end
        end

        function handle_timer_error(obj)
            % Handle timer errors
            ParallelSimulationExecutor.warn_once('ParallelSimulationExecutor:MonitorTimerError', ...
                'Monitor timer error occurred');
            obj.cleanup();
        end

        function invoke_pump_callback(~, pump_callback)
            if isempty(pump_callback)
                return;
            end

            try
                pump_callback();
            catch ME
                ParallelSimulationExecutor.warn_once('ParallelSimulationExecutor:UIPumpFailed', ...
                    'UI pump callback failed: %s', ME.message);
            end
        end

        function cleanup_internal(obj)
            % Clean up resources
            if obj.cleanup_in_progress
                return;
            end
            obj.cleanup_in_progress = true;
            cleanup_guard = onCleanup(@() set_cleanup_flag(obj, false)); %#ok<NASGU>
            obj.is_running = false;

            if ~isempty(obj.monitor_timer) && isvalid(obj.monitor_timer)
                try
                    stop(obj.monitor_timer);
                    delete(obj.monitor_timer);
                catch ME
                    ParallelSimulationExecutor.warn_once('ParallelSimulationExecutor:MonitorTimerCleanupFailed', ...
                        'Non-fatal monitor timer cleanup error: %s', ME.message);
                end
                obj.monitor_timer = [];
            end

            if ~isempty(obj.future_obj)
                future_obj = obj.future_obj;
                try
                    future_state = lower(char(string(future_obj.State)));
                catch
                    future_state = '';
                end
                if isempty(obj.terminal_state) && any(strcmp(future_state, {'finished', 'failed', 'cancelled'}))
                    obj.terminal_state = future_state;
                end
                if any(strcmp(future_state, {'finished', 'failed'})) && isempty(obj.terminal_failure_error)
                    try
                        if ~isempty(future_obj.Error)
                            obj.terminal_state = 'failed';
                            obj.terminal_failure_error = ParallelSimulationExecutor.wrap_worker_failure(future_obj.Error);
                        end
                    catch
                    end
                end
                try
                    if ~any(strcmp(future_state, {'finished', 'failed', 'cancelled'}))
                        cancel(future_obj);
                    end
                catch ME
                    ParallelSimulationExecutor.warn_once('ParallelSimulationExecutor:FutureCleanupCancelFailed', ...
                        'Non-fatal future cancellation error during cleanup: %s', ME.message);
                end
                wait_started = tic;
                while toc(wait_started) < 2.0
                    try
                        future_state = lower(char(string(future_obj.State)));
                    catch
                        break;
                    end
                    if any(strcmp(future_state, {'finished', 'failed', 'cancelled'}))
                        break;
                    end
                    pause(0.02);
                end
                obj.future_obj = [];
            end

            obj.progress_data_queue = [];
            obj.payload_sequence_id = 0;
            obj.synchronous_execution = false;
            obj.last_monitor_poll_tic = tic;
            obj.current_run_config = struct();
            obj.current_parameters = struct();
            obj.current_settings = struct();
            if ~isempty(obj.collector_dispatcher)
                try
                    obj.collector_dispatcher.stop_live_session();
                catch ME
                    ParallelSimulationExecutor.warn_once('ParallelSimulationExecutor:CollectorStopFailed', ...
                        'External collector dispatcher cleanup failed: %s', ME.message);
                end
                obj.collector_dispatcher = [];
            end
            obj.raw_hwinfo_csv_session_path = '';
            obj.raw_hwinfo_csv_initialized = false;
            obj.latest_collector_sample = struct();
        end

        function payload = stamp_payload(obj, payload)
            % Add a monotonic sequence id for client-side ordering.
            obj.payload_sequence_id = obj.payload_sequence_id + 1;
            payload.sequence_id = obj.payload_sequence_id;
            if isfield(obj.resource_policy, 'worker_topology')
                payload.worker_topology = obj.resource_policy.worker_topology;
            end
        end

        function emit_report_payload(obj, results, paths)
            % Emit one completion payload for report/UI channels.
            if isempty(obj.ui_progress_callback) || ~isstruct(results)
                return;
            end

            if nargin < 3 || ~isstruct(paths)
                paths = struct();
            end
            [paths, results] = obj.promote_raw_hwinfo_csv(paths, results);

            summary = struct( ...
                'mode', char(string(ParallelSimulationExecutor.pick_struct_field(obj.current_run_config, {'mode'}, ''))), ...
                'run_config', obj.current_run_config, ...
                'parameters', obj.current_parameters, ...
                'settings', obj.current_settings, ...
                'results', results, ...
                'paths', paths, ...
                'wall_time', double(ParallelSimulationExecutor.pick_struct_field(results, {'wall_time'}, NaN)));
            if isfield(results, 'analysis') && isstruct(results.analysis)
                summary.analysis = results.analysis;
            end
            if isfield(obj.current_run_config, 'workflow_kind')
                summary.workflow_kind = obj.current_run_config.workflow_kind;
            elseif isfield(results, 'workflow_kind')
                summary.workflow_kind = results.workflow_kind;
            end
            if isfield(obj.current_run_config, 'result_layout_kind')
                summary.result_layout_kind = obj.current_run_config.result_layout_kind;
            elseif isfield(results, 'result_layout_kind')
                summary.result_layout_kind = results.result_layout_kind;
            end
            payload = struct();
            payload.channel = 'report';
            payload.phase = 'completion';
            payload.progress_pct = 100;
            payload.summary = summary;
            payload.results = results;
            payload.paths = paths;
            payload.run_config = obj.current_run_config;
            payload.parameters = obj.current_parameters;
            payload.result_publication_mode = char(string(ParallelSimulationExecutor.pick_struct_field( ...
                obj.current_run_config, {'result_publication_mode'}, 'manual')));
            payload.workflow_kind = char(string(ParallelSimulationExecutor.pick_struct_field( ...
                obj.current_run_config, {'workflow_kind'}, ParallelSimulationExecutor.pick_struct_field(results, {'workflow_kind'}, ''))));
            payload.result_layout_kind = char(string(ParallelSimulationExecutor.pick_struct_field( ...
                obj.current_run_config, {'result_layout_kind'}, ParallelSimulationExecutor.pick_struct_field(results, {'result_layout_kind'}, ''))));
            if isfield(results, 'run_id')
                payload.run_id = results.run_id;
            end
            if isfield(results, 'wall_time')
                payload.wall_time = results.wall_time;
            end
            if isfield(results, 'max_omega')
                payload.max_vorticity = results.max_omega;
            end
            payload = obj.stamp_payload(payload);

            obj.queue_ui_payload(payload);
        end

        function emit_final_collector_payload(obj, results)
            if isempty(obj.ui_progress_callback) || ~isstruct(results)
                return;
            end
            if ~isfield(results, 'collector_last_sample') || ~isstruct(results.collector_last_sample) || ...
                    isempty(fieldnames(results.collector_last_sample))
                return;
            end

            sample = ExternalCollectorDispatcher.normalize_collector_payload(results.collector_last_sample);
            if isempty(fieldnames(sample))
                return;
            end

            payload = struct();
            payload.channel = 'metrics';
            payload.is_background_update = true;
            payload.progress_pct = 100;
            payload.elapsed_time = double(ParallelSimulationExecutor.pick_struct_field(results, {'wall_time'}, ...
                seconds(datetime('now') - obj.start_time)));
            payload.wall_clock_time = posixtime(datetime('now', 'TimeZone', 'UTC'));
            payload.resource_policy = obj.resource_policy;
            payload.cpu_usage = ParallelSimulationExecutor.pick_collector_metric_or_series(sample, 'cpu_proxy', NaN);
            payload.memory_usage = ParallelSimulationExecutor.pick_collector_metric_or_series(sample, 'memory_series', NaN);
            payload.gpu_usage = ParallelSimulationExecutor.pick_collector_metric_or_series(sample, 'gpu_series', NaN);
            payload.cpu_temp_c = ParallelSimulationExecutor.pick_collector_metric_or_series(sample, 'cpu_temp_c', NaN);
            payload.power_w = ParallelSimulationExecutor.pick_collector_metric_or_series(sample, 'power_w', NaN);
            payload.matlab_cpu_usage = NaN;
            payload.matlab_memory_usage = NaN;
            payload.matlab_gpu_usage = NaN;
            payload.matlab_power_w = NaN;

            collector_fields = {'collector_series', 'collector_status', 'coverage_domains', 'preferred_source', ...
                'raw_log_paths', 'overlay_metrics', 'collector_metric_catalog', 'hwinfo_transport', ...
                'hwinfo_status_reason', 'collector_probe_details'};
            for i = 1:numel(collector_fields)
                field_name = collector_fields{i};
                if isfield(sample, field_name)
                    payload.(field_name) = sample.(field_name);
                end
            end

            payload = obj.stamp_payload(payload);
            obj.queue_ui_payload(payload);

            if ParallelSimulationExecutor.hwinfo_status_is_incomplete(sample)
                obj.emit_runtime_log(sprintf('Collector warning: HWiNFO payload incomplete at completion (%s).', ...
                    char(string(ParallelSimulationExecutor.pick_struct_field(sample, {'hwinfo_status_reason'}, ...
                    'shared memory bundle incomplete')))), 'warning');
            end
        end

        function results = attach_collector_summary(obj, results)
            if nargin < 2 || ~isstruct(results)
                results = struct();
            end
            if isempty(obj.collector_dispatcher)
                return;
            end
            latest_sample = struct();
            session_summary = struct();
            cached_sample = ExternalCollectorDispatcher.normalize_collector_payload(obj.latest_collector_sample);
            try
                latest_sample = obj.collector_dispatcher.poll_latest_sample();
                latest_sample = ExternalCollectorDispatcher.normalize_collector_payload(latest_sample);
                latest_sample = ParallelSimulationExecutor.prefer_richer_collector_sample(latest_sample, cached_sample);
                obj.append_raw_hwinfo_csv_row(latest_sample);
                if isstruct(latest_sample) && ~isempty(fieldnames(latest_sample))
                    results.collector_last_sample = latest_sample;
                    if isfield(latest_sample, 'collector_status')
                        results.collector_status = latest_sample.collector_status;
                    end
                    if isfield(latest_sample, 'collector_metric_catalog')
                        results.collector_metric_catalog = latest_sample.collector_metric_catalog;
                    end
                end
            catch ME
                ParallelSimulationExecutor.warn_once('ParallelSimulationExecutor:CollectorLatestSampleFailed', ...
                    'External collector final sample capture failed: %s', ME.message);
            end

            try
                session_summary = obj.collector_dispatcher.stop_live_session();
                session_summary = ExternalCollectorDispatcher.normalize_collector_payload(session_summary);
            catch ME
                ParallelSimulationExecutor.warn_once('ParallelSimulationExecutor:CollectorSummaryFailed', ...
                    'External collector summary capture failed: %s', ME.message);
            end
            obj.collector_dispatcher = [];

            session_summary = ParallelSimulationExecutor.prefer_richer_collector_sample(session_summary, latest_sample);
            if isstruct(session_summary) && ~isempty(fieldnames(session_summary))
                results.collector_session = session_summary;
                if isfield(session_summary, 'collector_status')
                    results.collector_status = session_summary.collector_status;
                end
                if (~isfield(results, 'collector_metric_catalog') || isempty(results.collector_metric_catalog)) && ...
                        isfield(session_summary, 'collector_metric_catalog')
                    results.collector_metric_catalog = session_summary.collector_metric_catalog;
                end
            end
        end

        function remember_collector_sample(obj, sample)
            if ~(isstruct(sample) && ~isempty(fieldnames(sample)))
                return;
            end
            sample = ExternalCollectorDispatcher.normalize_collector_payload(sample);
            obj.latest_collector_sample = ParallelSimulationExecutor.prefer_richer_collector_sample( ...
                obj.latest_collector_sample, sample);
        end

        function prime_collector_sample(obj)
            if isempty(obj.collector_dispatcher)
                return;
            end

            max_attempts = 4;
            pause_seconds = 0.2;
            for attempt = 1:max_attempts
                sample = struct();
                try
                    sample = obj.collector_dispatcher.poll_latest_sample();
                    sample = ExternalCollectorDispatcher.normalize_collector_payload(sample);
                catch ME
                    ParallelSimulationExecutor.warn_once('ParallelSimulationExecutor:CollectorPrimeFailed', ...
                        'Initial collector sample capture failed: %s', ME.message);
                    return;
                end
                obj.remember_collector_sample(sample);
                richer_sample = ParallelSimulationExecutor.prefer_richer_collector_sample(sample, obj.latest_collector_sample);
                obj.append_raw_hwinfo_csv_row(richer_sample);
                if ParallelSimulationExecutor.collector_payload_score(richer_sample) >= 140
                    return;
                end
                if attempt < max_attempts
                    pause(pause_seconds);
                end
            end
        end

        function emit_failure_payload(obj, failure_error)
            % Emit one failure payload for report/UI channels.
            if isempty(obj.ui_progress_callback) || isempty(failure_error)
                return;
            end

            payload = struct();
            payload.channel = 'report';
            payload.phase = 'failure';
            payload.progress_pct = obj.latest_progress_pct;
            payload.failure_identifier = char(string(failure_error.identifier));
            payload.failure_message = char(string(failure_error.message));
            try
                payload.failure_report = getReport(failure_error, 'extended', 'hyperlinks', 'off');
            catch
                payload.failure_report = char(string(failure_error.message));
            end
            if isprop(failure_error, 'stack')
                payload.failure_stack = failure_error.stack;
            else
                payload.failure_stack = [];
            end
            payload = obj.stamp_payload(payload);

            obj.queue_ui_payload(payload);
        end

        function append_raw_hwinfo_csv_row(obj, sample)
            if ~isstruct(sample) || isempty(fieldnames(sample))
                return;
            end
            csv_path = char(string(obj.raw_hwinfo_csv_session_path));
            if isempty(csv_path)
                return;
            end
            status_struct = ParallelSimulationExecutor.pick_struct_field(sample, {'collector_status'}, struct());
            hwinfo_status = char(string(ParallelSimulationExecutor.pick_struct_field(status_struct, {'hwinfo'}, '')));
            if strlength(string(strtrim(hwinfo_status))) == 0 || strcmpi(hwinfo_status, 'disabled')
                return;
            end
            csv_dir = fileparts(csv_path);
            if ~isempty(csv_dir) && exist(csv_dir, 'dir') ~= 7
                mkdir(csv_dir);
            end
            session_time_s = double(ParallelSimulationExecutor.pick_struct_field(sample, {'elapsed_wall_time_s'}, NaN));
            if ~(isfinite(session_time_s) && session_time_s >= 0)
                try
                    session_time_s = seconds(datetime('now') - obj.start_time);
                catch
                    session_time_s = NaN;
                end
            end
            row = table( ...
                session_time_s, ...
                string(char(datetime('now', 'TimeZone', 'UTC', 'Format', 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z'''))), ...
                string(char(string(hwinfo_status))), ...
                string(char(string(ParallelSimulationExecutor.pick_struct_field(sample, {'hwinfo_transport'}, 'none')))), ...
                double(ParallelSimulationExecutor.pick_collector_metric_or_series(sample, 'cpu_proxy', NaN)), ...
                double(ParallelSimulationExecutor.pick_collector_metric_or_series(sample, 'gpu_series', NaN)), ...
                double(ParallelSimulationExecutor.pick_collector_metric_or_series(sample, 'memory_series', NaN)), ...
                double(ParallelSimulationExecutor.pick_collector_metric_or_series(sample, 'cpu_temp_c', NaN)), ...
                double(ParallelSimulationExecutor.pick_collector_metric_or_series(sample, 'system_power_w', NaN)), ...
                double(ParallelSimulationExecutor.pick_collector_metric_or_series(sample, 'cpu_voltage_v', NaN)), ...
                double(ParallelSimulationExecutor.pick_collector_metric_or_series(sample, 'gpu_voltage_v', NaN)), ...
                double(ParallelSimulationExecutor.pick_collector_metric_or_series(sample, 'memory_voltage_v', NaN)), ...
                double(ParallelSimulationExecutor.pick_collector_metric_or_series(sample, 'cpu_power_w_hwinfo', NaN)), ...
                double(ParallelSimulationExecutor.pick_collector_metric_or_series(sample, 'gpu_power_w_hwinfo', NaN)), ...
                double(ParallelSimulationExecutor.pick_collector_metric_or_series(sample, 'memory_power_w_or_proxy', NaN)), ...
                double(ParallelSimulationExecutor.pick_collector_metric_or_series(sample, 'environmental_energy_wh_cum', NaN)), ...
                double(ParallelSimulationExecutor.pick_collector_metric_or_series(sample, 'environmental_co2_g_cum', NaN)), ...
                double(ParallelSimulationExecutor.pick_collector_metric_or_series(sample, 'fan_rpm', NaN)), ...
                double(ParallelSimulationExecutor.pick_collector_metric_or_series(sample, 'pump_rpm', NaN)), ...
                double(ParallelSimulationExecutor.pick_collector_metric_or_series(sample, 'coolant_temp_c', NaN)), ...
                double(ParallelSimulationExecutor.pick_collector_metric_or_series(sample, 'device_battery_level', NaN)), ...
                'VariableNames', {'session_time_s', 'timestamp_utc', 'hwinfo_status', 'hwinfo_transport', ...
                'cpu_proxy', 'gpu_series', 'memory_series', 'cpu_temp_c', 'system_power_w', ...
                'cpu_voltage_v', 'gpu_voltage_v', 'memory_voltage_v', 'cpu_power_w_hwinfo', ...
                'gpu_power_w_hwinfo', 'memory_power_w_or_proxy', 'environmental_energy_wh_cum', ...
                'environmental_co2_g_cum', 'fan_rpm', 'pump_rpm', 'coolant_temp_c', 'device_battery_level'});
            try
                if obj.raw_hwinfo_csv_initialized && exist(csv_path, 'file') == 2
                    writetable(row, csv_path, 'WriteMode', 'append');
                else
                    writetable(row, csv_path);
                    obj.raw_hwinfo_csv_initialized = true;
                end
            catch ME
                ParallelSimulationExecutor.warn_once('ParallelSimulationExecutor:RawHWiNFOCsvWriteFailed', ...
                    'HWiNFO CSV logging failed: %s', ME.message);
            end
        end

        function [paths, results] = promote_raw_hwinfo_csv(obj, paths, results)
            if nargin < 2 || ~isstruct(paths)
                paths = struct();
            end
            if nargin < 3 || ~isstruct(results)
                results = struct();
            end
            source_path = char(string(obj.raw_hwinfo_csv_session_path));
            target_path = char(string(ParallelSimulationExecutor.pick_struct_field(paths, {'raw_hwinfo_csv_path'}, '')));
            if isempty(source_path) || exist(source_path, 'file') ~= 2 || isempty(target_path)
                return;
            end
            target_dir = fileparts(target_path);
            if ~isempty(target_dir) && exist(target_dir, 'dir') ~= 7
                mkdir(target_dir);
            end
            try
                copyfile(source_path, target_path, 'f');
            catch ME
                ParallelSimulationExecutor.warn_once('ParallelSimulationExecutor:RawHWiNFOCsvPromoteFailed', ...
                    'Could not promote HWiNFO CSV to the final results package: %s', ME.message);
                return;
            end
            if ~isfield(results, 'collector_last_sample') || ~isstruct(results.collector_last_sample)
                results.collector_last_sample = struct();
            end
            if ~isfield(results.collector_last_sample, 'raw_log_paths') || ~isstruct(results.collector_last_sample.raw_log_paths)
                results.collector_last_sample.raw_log_paths = struct();
            end
            results.collector_last_sample.raw_log_paths.hwinfo = target_path;
            if isfield(results, 'collector_session') && isstruct(results.collector_session)
                if ~isfield(results.collector_session, 'raw_log_paths') || ~isstruct(results.collector_session.raw_log_paths)
                    results.collector_session.raw_log_paths = struct();
                end
                results.collector_session.raw_log_paths.hwinfo = target_path;
            end
        end

        function run_synchronous(obj, run_config, parameters, settings)
            % Fallback to synchronous execution if parallel fails

            try
                sync_settings = settings;
                sync_settings.ui_progress_callback = struct( ...
                    'callback_kind', 'parallel_executor_relay', ...
                    'executor', obj);
                if isfield(sync_settings, 'progress_data_queue')
                    sync_settings = rmfield(sync_settings, 'progress_data_queue');
                end
                collector_samples = struct('pre', struct(), 'post', struct());
                if ~isempty(obj.collector_dispatcher)
                    collector_samples.pre = obj.collector_dispatcher.poll_latest_sample();
                    obj.remember_collector_sample(collector_samples.pre);
                end
                % Run simulation directly
                [results, paths] = RunDispatcher(run_config, parameters, sync_settings);
                if ~isempty(obj.collector_dispatcher)
                    collector_samples.post = obj.collector_dispatcher.poll_latest_sample();
                    obj.remember_collector_sample(collector_samples.post);
                end

                % Store results for retrieval
                obj.shared_data = struct('results', results, 'paths', paths, ...
                    'collector_samples', collector_samples);
                obj.is_running = false;
                obj.latest_progress_pct = 100;

            catch ME
                obj.is_running = false;
                rethrow(ME);
            end
        end

        function start_monitor_timer(obj)
            if ParallelSimulationExecutor.should_skip_host_monitor_timer()
                return;
            end
            if ~isempty(obj.monitor_timer) && isvalid(obj.monitor_timer)
                return;
            end
            obj.monitor_timer = timer(...
                'ExecutionMode', 'fixedRate', ...
                'Period', obj.resource_policy.monitor_period_seconds, ...
                'TimerFcn', {@parallel_executor_timer_monitor_callback, obj}, ...
                'ErrorFcn', {@parallel_executor_timer_error_callback, obj});
            start(obj.monitor_timer);
        end

        function maybe_poll_synchronous_metrics(obj)
            if ~obj.synchronous_execution || ~obj.is_running
                return;
            end
            period_s = 0.5;
            if isfield(obj.resource_policy, 'monitor_period_seconds')
                period_s = max(0.1, double(obj.resource_policy.monitor_period_seconds));
            end
            if toc(obj.last_monitor_poll_tic) < period_s
                return;
            end
            obj.last_monitor_poll_tic = tic;
            obj.monitor_callback();
        end
    end

    methods (Static)
        function pool = prewarm_pool(target_workers, max_workers)
            if nargin < 1 || ~(isnumeric(target_workers) && isscalar(target_workers) && isfinite(target_workers))
                target_workers = 1;
            end
            if nargin < 2 || ~(isnumeric(max_workers) && isscalar(max_workers) && isfinite(max_workers))
                max_workers = inf;
            end
            pool = ParallelSimulationExecutor.ensure_pool_for_target(target_workers, max_workers);
        end

        function persist_results_package(results, paths)
            if nargin < 1 || ~isstruct(results) || nargin < 2 || ~isstruct(paths)
                return;
            end

            persisted_results = ParallelSimulationExecutor.prepare_results_for_persistence(results, paths);
            save_json = ParallelSimulationExecutor.json_saving_enabled(results, paths);
            specs = ParallelSimulationExecutor.result_package_specs(results, paths);
            if isempty(specs)
                if save_json
                    ParallelSimulationExecutor.persist_initial_artifact_manifest(paths, persisted_results);
                end
                return;
            end

            for i = 1:numel(specs)
                spec = specs(i);
                if isempty(spec.mat_path) || exist(spec.mat_path, 'file') ~= 2
                    continue;
                end
                try
                    loaded = load(spec.mat_path, spec.var_name);
                    if ~isfield(loaded, spec.var_name) || ~isstruct(loaded.(spec.var_name))
                        continue;
                    end
                    merged = ParallelSimulationExecutor.overlay_persisted_results(loaded.(spec.var_name), persisted_results, paths);
                    save_payload = struct();
                    save_payload.(spec.var_name) = filter_graphics_objects(merged);
                    safe_save_mat(spec.mat_path, save_payload, '-v7.3');
                    if save_json && ~isempty(spec.json_path)
                        ParallelSimulationExecutor.write_json_file(spec.json_path, filter_graphics_objects(merged));
                    end
                catch ME
                    ParallelSimulationExecutor.warn_once('ParallelSimulationExecutor:PersistFinalResultsPackageFailed', ...
                        'Could not persist final collector/runtime results back into the saved package: %s', ME.message);
                end
            end

            if save_json
                ParallelSimulationExecutor.persist_initial_artifact_manifest(paths, persisted_results);
            end
        end
    end

    methods (Static, Access = private)
        function pool = ensure_pool_for_target(target_workers, max_workers)
            target_workers = max(1, round(double(target_workers)));
            if nargin >= 2 && isfinite(max_workers) && max_workers >= 1
                target_workers = min(target_workers, round(double(max_workers)));
            end

            pool = gcp('nocreate');
            if isempty(pool)
                pool = ParallelSimulationExecutor.open_local_pool(target_workers);
            elseif pool.NumWorkers < target_workers
                delete(pool);
                pool = ParallelSimulationExecutor.open_local_pool(target_workers);
            end
            ParallelSimulationExecutor.disable_pool_idle_timeout(pool);
        end

        function pool = open_local_pool(target_workers)
            pool = [];
            try
                evalc('pool = parpool(''local'', target_workers, ''SpmdEnabled'', false, ''IdleTimeout'', Inf);'); %#ok<EVLC>
            catch first_error
                pool = gcp('nocreate');
                if ~isempty(pool)
                    ParallelSimulationExecutor.disable_pool_idle_timeout(pool);
                    return;
                end
                try
                    evalc('pool = parpool(''local'', target_workers, ''SpmdEnabled'', false);'); %#ok<EVLC>
                catch
                    rethrow(first_error);
                end
            end
            ParallelSimulationExecutor.disable_pool_idle_timeout(pool);
        end

        function disable_pool_idle_timeout(pool)
            if isempty(pool) || ~isvalid(pool)
                return;
            end
            try
                if isprop(pool, 'IdleTimeout')
                    try
                        pool.IdleTimeout = Inf;
                    catch
                        % Older profiles can reject Inf after creation; keep
                        % the pool alive for a practical full-day session.
                        pool.IdleTimeout = 24 * 60;
                    end
                end
            catch
            end
        end

        function run_id = resolve_run_id(run_config)
            run_id = '';
            if isstruct(run_config)
                if isfield(run_config, 'run_id') && ~isempty(run_config.run_id)
                    run_id = char(string(run_config.run_id));
                    return;
                end
                if isfield(run_config, 'study_id') && ~isempty(run_config.study_id)
                    run_id = char(string(run_config.study_id));
                    return;
                end
                if isfield(run_config, 'phase_id') && ~isempty(run_config.phase_id)
                    run_id = char(string(run_config.phase_id));
                    return;
                end
            end
            run_id = char(datetime('now', 'Format', 'yyyyMMdd_HHmmss'));
        end

        function csv_path = resolve_raw_hwinfo_session_csv_path(settings, run_config)
            csv_path = '';
            if ParallelSimulationExecutor.phase_csv_mode_requested(settings, run_config)
                return;
            end
            sustainability_cfg = ParallelSimulationExecutor.pick_struct_field(settings, {'sustainability'}, struct());
            external_cfg = ParallelSimulationExecutor.pick_struct_field(sustainability_cfg, {'external_collectors'}, struct());
            hwinfo_enabled = logical(ParallelSimulationExecutor.pick_struct_field(external_cfg, {'hwinfo'}, false));
            if ~hwinfo_enabled
                return;
            end
            runtime_cfg = ParallelSimulationExecutor.pick_struct_field(sustainability_cfg, {'collector_runtime'}, struct());
            session_output_dir = char(string(ParallelSimulationExecutor.pick_struct_field(runtime_cfg, {'session_output_dir'}, '')));
            if isempty(session_output_dir)
                session_output_dir = ParallelSimulationExecutor.resolve_canonical_collector_session_dir(settings, run_config);
            end
            if isempty(session_output_dir)
                session_output_dir = fullfile(tempdir, 'tsunami_hwinfo_runtime');
            end
            run_id = ParallelSimulationExecutor.resolve_run_id(run_config);
            run_token = regexprep(char(string(run_id)), '[^A-Za-z0-9_\-]+', '_');
            csv_path = fullfile(session_output_dir, sprintf('%s_hwinfo_runtime.csv', run_token));
        end

        function session_output_dir = resolve_canonical_collector_session_dir(settings, run_config)
            session_output_dir = '';
            if ~(exist('PathBuilder', 'class') == 8 || exist('PathBuilder', 'file') == 2)
                return;
            end

            output_root = char(string(ParallelSimulationExecutor.pick_struct_field(settings, {'output_root'}, 'Results')));
            workflow_kind = lower(strtrim(char(string(ParallelSimulationExecutor.pick_struct_field(run_config, {'workflow_kind'}, '')))));
            try
                switch workflow_kind
                    case 'phase1_periodic_comparison'
                        phase_id = char(string(ParallelSimulationExecutor.pick_struct_field(run_config, {'phase_id'}, '')));
                        if isempty(phase_id)
                            return;
                        end
                        phase_paths = PathBuilder.get_phase_paths('Phase1', phase_id, output_root);
                        session_output_dir = char(string(ParallelSimulationExecutor.pick_struct_field(phase_paths, {'metrics_root'}, '')));
                    case 'phase2_boundary_condition_study'
                        phase_id = char(string(ParallelSimulationExecutor.pick_struct_field(run_config, {'phase_id'}, '')));
                        if isempty(phase_id)
                            return;
                        end
                        phase_paths = PathBuilder.get_phase_paths('Phase2', phase_id, output_root);
                        session_output_dir = char(string(ParallelSimulationExecutor.pick_struct_field(phase_paths, {'metrics_root'}, '')));
                    case 'phase3_bathymetry_study'
                        phase_id = char(string(ParallelSimulationExecutor.pick_struct_field(run_config, {'phase_id'}, '')));
                        if isempty(phase_id)
                            return;
                        end
                        phase_paths = PathBuilder.get_phase_paths('Phase3', phase_id, output_root);
                        session_output_dir = char(string(ParallelSimulationExecutor.pick_struct_field(phase_paths, {'metrics_root'}, '')));
                    otherwise
                        method_name = char(string(ParallelSimulationExecutor.pick_struct_field(run_config, {'method'}, '')));
                        mode_name = char(string(ParallelSimulationExecutor.pick_struct_field(run_config, {'mode'}, 'Evolution')));
                        run_id = ParallelSimulationExecutor.resolve_run_id(run_config);
                        run_paths = PathBuilder.get_run_paths(method_name, mode_name, run_id, output_root);
                        session_output_dir = char(string(ParallelSimulationExecutor.pick_struct_field(run_paths, {'metrics_root'}, '')));
                end
            catch
                session_output_dir = '';
            end
        end

        function tf = phase_csv_mode_requested(settings, run_config)
            tf = false;
            if ~(isstruct(settings) && isstruct(run_config))
                return;
            end
            workflow_kind = char(string(ParallelSimulationExecutor.pick_struct_field(run_config, {'workflow_kind'}, '')));
            if isempty(strtrim(workflow_kind))
                return;
            end
            if ~(exist('PhaseTelemetryCSVFirst', 'class') == 8 || exist('PhaseTelemetryCSVFirst', 'file') == 2)
                return;
            end
            try
                tf = PhaseTelemetryCSVFirst.phase_csv_mode_enabled(settings, workflow_kind);
            catch
                tf = false;
            end
        end

        function persisted_results = prepare_results_for_persistence(results, paths)
            persisted_results = results;
            if ~isstruct(persisted_results)
                persisted_results = struct();
            end
            if nargin >= 2 && isstruct(paths) && ~isempty(fieldnames(paths))
                persisted_results.paths = paths;
                layout_version = char(string(ParallelSimulationExecutor.pick_struct_field(paths, {'artifact_layout_version'}, '')));
                if ~isempty(layout_version)
                    persisted_results.artifact_layout_version = layout_version;
                end
            end
        end

        function tf = json_saving_enabled(varargin)
            tf = false;
            for i = 1:nargin
                source = varargin{i};
                if ~isstruct(source)
                    continue;
                end
                if isfield(source, 'save_json') && ~isempty(source.save_json)
                    tf = logical(source.save_json);
                    return;
                end
                if isfield(source, 'phase_config') && isstruct(source.phase_config) && ...
                        isfield(source.phase_config, 'save_json') && ~isempty(source.phase_config.save_json)
                    tf = logical(source.phase_config.save_json);
                    return;
                end
                if isfield(source, 'mesh_convergence') && isstruct(source.mesh_convergence) && ...
                        isfield(source.mesh_convergence, 'save_json') && ~isempty(source.mesh_convergence.save_json)
                    tf = logical(source.mesh_convergence.save_json);
                    return;
                end
                if isfield(source, 'paths') && isstruct(source.paths) && ...
                        isfield(source.paths, 'save_json') && ~isempty(source.paths.save_json)
                    tf = logical(source.paths.save_json);
                    return;
                end
            end
        end

        function specs = result_package_specs(results, paths)
            specs = repmat(struct('mat_path', '', 'json_path', '', 'var_name', ''), 1, 0);
            data_roots = {};
            matlab_data_root = char(string(ParallelSimulationExecutor.pick_struct_field(paths, {'matlab_data_root', 'data'}, '')));
            if ~isempty(matlab_data_root)
                data_roots{end + 1} = matlab_data_root; %#ok<AGROW>
            end
            data_root = char(string(ParallelSimulationExecutor.pick_struct_field(paths, {'data'}, '')));
            if ~isempty(data_root) && ~any(strcmpi(data_roots, data_root))
                data_roots{end + 1} = data_root; %#ok<AGROW>
            end
            if isempty(data_roots)
                return;
            end

            workflow_kind = lower(char(string(ParallelSimulationExecutor.pick_struct_field(results, {'workflow_kind'}, ''))));
            file_stems = {'results'};
            switch workflow_kind
                case 'mesh_convergence_study'
                    file_stems = {'mesh_convergence_results', 'results'};
                case 'phase1_periodic_comparison'
                    file_stems = {'phase1_results', 'results'};
                case 'phase2_boundary_condition_study'
                    file_stems = {'phase2_results', 'results'};
                case 'phase3_bathymetry_study'
                    file_stems = {'phase3_results', 'results'};
            end

            for ri = 1:numel(data_roots)
                for fi = 1:numel(file_stems)
                    stem = file_stems{fi};
                    mat_path = fullfile(data_roots{ri}, sprintf('%s.mat', stem));
                    if exist(mat_path, 'file') ~= 2
                        continue;
                    end
                    var_name = 'Results';
                    if startsWith(stem, 'phase') || strcmp(stem, 'mesh_convergence_results')
                        var_name = 'ResultsForSave';
                    end
                    json_path = fullfile(data_roots{ri}, sprintf('%s.json', stem));
                    specs(end + 1) = struct( ... %#ok<AGROW>
                        'mat_path', mat_path, ...
                        'json_path', json_path, ...
                        'var_name', var_name);
                end
            end
        end

        function merged = overlay_persisted_results(existing_results, runtime_results, paths)
            merged = existing_results;
            if ~isstruct(merged)
                merged = struct();
            end
            if ~isstruct(runtime_results)
                runtime_results = struct();
            end

            direct_fields = {'collector_samples', 'collector_last_sample', 'collector_session', ...
                'collector_status', 'collector_metric_catalog'};
            for i = 1:numel(direct_fields)
                field_name = direct_fields{i};
                if isfield(runtime_results, field_name) && ~isempty(runtime_results.(field_name))
                    merged.(field_name) = runtime_results.(field_name);
                end
            end

            if nargin >= 3 && isstruct(paths) && ~isempty(fieldnames(paths))
                merged.paths = paths;
                layout_version = char(string(ParallelSimulationExecutor.pick_struct_field(paths, {'artifact_layout_version'}, '')));
                if ~isempty(layout_version)
                    merged.artifact_layout_version = layout_version;
                end
            end

            if isfield(merged, 'workflow_manifest') && isstruct(merged.workflow_manifest)
                if nargin >= 3 && isstruct(paths) && ~isempty(fieldnames(paths))
                    merged.workflow_manifest.paths = paths;
                    layout_version = char(string(ParallelSimulationExecutor.pick_struct_field(paths, {'artifact_layout_version'}, '')));
                    if ~isempty(layout_version)
                        merged.workflow_manifest.artifact_layout_version = layout_version;
                    end
                    raw_hwinfo_csv_path = char(string(ParallelSimulationExecutor.pick_struct_field(paths, {'raw_hwinfo_csv_path'}, '')));
                    if ~isempty(raw_hwinfo_csv_path)
                        merged.workflow_manifest.raw_hwinfo_csv_path = raw_hwinfo_csv_path;
                    end
                end
                if isfield(runtime_results, 'collector_status') && ~isempty(runtime_results.collector_status)
                    merged.workflow_manifest.collector_status = runtime_results.collector_status;
                end
                if isfield(runtime_results, 'collector_metric_catalog') && ~isempty(runtime_results.collector_metric_catalog)
                    merged.workflow_manifest.collector_metric_catalog = runtime_results.collector_metric_catalog;
                end
            end
        end

        function persist_initial_artifact_manifest(paths, results)
            if nargin < 1 || ~isstruct(paths)
                return;
            end
            manifest_path = char(string(ParallelSimulationExecutor.pick_struct_field(paths, {'manifest_path'}, '')));
            if isempty(manifest_path)
                matlab_data_root = char(string(ParallelSimulationExecutor.pick_struct_field(paths, {'matlab_data_root', 'data'}, '')));
                if isempty(matlab_data_root)
                    return;
                end
                manifest_path = fullfile(matlab_data_root, 'artifact_manifest.json');
            end
            if exist(manifest_path, 'file') == 2
                return;
            end

            manifest_payload = struct( ...
                'artifact_layout_version', char(string(ParallelSimulationExecutor.pick_struct_field(paths, {'artifact_layout_version'}, ''))), ...
                'run_root', char(string(ParallelSimulationExecutor.pick_struct_field(paths, {'base'}, ''))), ...
                'run_settings_path', char(string(ParallelSimulationExecutor.pick_struct_field(paths, {'run_settings_path'}, ''))), ...
                'matlab_data_root', char(string(ParallelSimulationExecutor.pick_struct_field(paths, {'matlab_data_root', 'data'}, ''))), ...
                'metrics_root', char(string(ParallelSimulationExecutor.pick_struct_field(paths, {'metrics_root', 'reports'}, ''))), ...
                'visuals_root', char(string(ParallelSimulationExecutor.pick_struct_field(paths, {'visuals_root', 'figures_root'}, ''))), ...
                'paths', paths, ...
                'raw_hwinfo_csv_path', char(string(ParallelSimulationExecutor.pick_struct_field(paths, {'raw_hwinfo_csv_path'}, ''))), ...
                'workflow_kind', char(string(ParallelSimulationExecutor.pick_struct_field(results, {'workflow_kind'}, ''))), ...
                'result_layout_kind', char(string(ParallelSimulationExecutor.pick_struct_field(results, {'result_layout_kind'}, ''))), ...
                'phase_label', char(string(ParallelSimulationExecutor.pick_struct_field(results, {'phase_label'}, ''))), ...
                'collector_status', ParallelSimulationExecutor.pick_struct_field(results, {'collector_status'}, struct()), ...
                'publication_status', 'pending_ui_publication');
            try
                ParallelSimulationExecutor.write_json_file(manifest_path, filter_graphics_objects(manifest_payload));
            catch ME
                ParallelSimulationExecutor.warn_once('ParallelSimulationExecutor:InitialArtifactManifestFailed', ...
                    'Could not create the initial artifact manifest before UI publication: %s', ME.message);
            end
        end

        function write_json_file(target_path, payload)
            if nargin < 1
                return;
            end
            target_path = char(string(target_path));
            if isempty(target_path)
                return;
            end
            target_dir = fileparts(target_path);
            if ~isempty(target_dir) && exist(target_dir, 'dir') ~= 7
                mkdir(target_dir);
            end
            fid = fopen(target_path, 'w');
            if fid < 0
                error('ParallelSimulationExecutor:JsonWriteOpenFailed', ...
                    'Could not open %s for writing.', target_path);
            end
            cleaner = onCleanup(@() fclose(fid)); %#ok<NASGU>
            fprintf(fid, '%s', jsonencode(payload));
            clear cleaner
        end

        function value = pick_collector_metric(sample, metric_key, fallback)
            value = fallback;
            if nargin < 3
                fallback = NaN;
            end
            if ~isstruct(sample) || ~isfield(sample, 'metrics') || ~isstruct(sample.metrics)
                return;
            end
            if ~isfield(sample.metrics, metric_key)
                return;
            end
            candidate = sample.metrics.(metric_key);
            if isnumeric(candidate) && isscalar(candidate) && isfinite(candidate)
                value = candidate;
            end
        end

        function value = pick_collector_metric_or_series(sample, metric_key, fallback)
            value = ParallelSimulationExecutor.pick_collector_metric(sample, metric_key, fallback);
            if isnumeric(value) && isscalar(value) && isfinite(value)
                return;
            end
            if ~(isstruct(sample) && isfield(sample, 'collector_series') && isstruct(sample.collector_series))
                value = fallback;
                return;
            end
            source_names = {'hwinfo', 'icue', 'matlab'};
            for si = 1:numel(source_names)
                source = source_names{si};
                if ~isfield(sample.collector_series, source) || ~isstruct(sample.collector_series.(source)) || ...
                        ~isfield(sample.collector_series.(source), metric_key)
                    continue;
                end
                candidate = sample.collector_series.(source).(metric_key);
                if ~(isnumeric(candidate) && ~isempty(candidate))
                    continue;
                end
                candidate = double(candidate(:));
                candidate = candidate(isfinite(candidate));
                if isempty(candidate)
                    continue;
                end
                value = candidate(end);
                return;
            end
            value = fallback;
        end

        function settings = sanitize_settings_for_publication(settings)
            if nargin < 1 || ~isstruct(settings)
                settings = struct();
                return;
            end
            strip_fields = {'ui_progress_callback', 'progress_data_queue'};
            for i = 1:numel(strip_fields)
                if isfield(settings, strip_fields{i})
                    settings = rmfield(settings, strip_fields{i});
                end
            end
        end

        function monitor_series = build_summary_monitor_series(results, paths, run_config)
            monitor_series = struct();
            if nargin < 1 || ~isstruct(results)
                return;
            end
            if nargin < 2 || ~isstruct(paths)
                paths = struct();
            end
            if nargin < 3 || ~isstruct(run_config)
                run_config = struct();
            end

            if isfield(results, 'collector_last_sample') && isstruct(results.collector_last_sample)
                monitor_series = ExternalCollectorDispatcher.normalize_collector_payload(results.collector_last_sample);
            end
            if isfield(results, 'collector_session') && isstruct(results.collector_session)
                session_series = ExternalCollectorDispatcher.normalize_collector_payload(results.collector_session);
                if isempty(fieldnames(monitor_series))
                    monitor_series = session_series;
                else
                    last_series_complete = ParallelSimulationExecutor.collector_payload_complete(monitor_series);
                    session_complete = ParallelSimulationExecutor.collector_payload_complete(session_series);
                    if session_complete && ~last_series_complete
                        monitor_series = session_series;
                    elseif ~last_series_complete && ~isempty(fieldnames(session_series))
                        monitor_series = ParallelSimulationExecutor.overlay_monitor_series(monitor_series, session_series);
                    end
                end
            end

            summary_context = struct( ...
                'results', results, ...
                'paths', paths, ...
                'run_config', run_config, ...
                'workflow_kind', ParallelSimulationExecutor.pick_struct_field(run_config, {'workflow_kind'}, ...
                    ParallelSimulationExecutor.pick_struct_field(results, {'workflow_kind'}, '')), ...
                'phase_id', ParallelSimulationExecutor.pick_struct_field(run_config, {'phase_id'}, ...
                    ParallelSimulationExecutor.pick_struct_field(results, {'phase_id'}, '')));
            monitor_series = ExternalCollectorDispatcher.recover_monitor_series(summary_context, monitor_series, paths);
        end

        function merged = overlay_monitor_series(base_series, overlay_series)
            merged = base_series;
            if ~isstruct(overlay_series) || isempty(fieldnames(overlay_series))
                return;
            end
            overlay_fields = {'collector_series', 'collector_status', 'coverage_domains', 'preferred_source', ...
                'raw_log_paths', 'overlay_metrics', 'collector_metric_catalog', 'hwinfo_transport', ...
                'hwinfo_status_reason', 'collector_probe_details', 'metrics'};
            for i = 1:numel(overlay_fields)
                field_name = overlay_fields{i};
                if isfield(overlay_series, field_name) && ~isempty(overlay_series.(field_name))
                    merged.(field_name) = overlay_series.(field_name);
                end
            end
            direct_metric_fields = {'cpu_proxy', 'gpu_series', 'memory_series', 'cpu_temp_c', 'power_w', ...
                'cpu_voltage_v', 'gpu_voltage_v', 'memory_voltage_v', 'cpu_power_w_hwinfo', ...
                'gpu_power_w_hwinfo', 'memory_power_w_or_proxy', 'system_power_w', ...
                'environmental_energy_wh_cum', 'environmental_co2_g_cum', 'fan_rpm', ...
                'pump_rpm', 'coolant_temp_c', 'device_battery_level'};
            for i = 1:numel(direct_metric_fields)
                field_name = direct_metric_fields{i};
                if isfield(overlay_series, field_name) && ~isempty(overlay_series.(field_name))
                    merged.(field_name) = overlay_series.(field_name);
                end
            end
            if isfield(overlay_series, 't') && ~isempty(overlay_series.t)
                merged.t = overlay_series.t;
            end
            if isfield(overlay_series, 'elapsed_wall_time') && ~isempty(overlay_series.elapsed_wall_time)
                merged.elapsed_wall_time = overlay_series.elapsed_wall_time;
            end
            if isfield(overlay_series, 'wall_clock_time') && ~isempty(overlay_series.wall_clock_time)
                merged.wall_clock_time = overlay_series.wall_clock_time;
            end
            merged = ExternalCollectorDispatcher.normalize_collector_payload(merged);
        end

        function tf = collector_payload_complete(sample)
            tf = false;
            if ~(isstruct(sample) && ~isempty(fieldnames(sample)))
                return;
            end
            collector_status = ParallelSimulationExecutor.pick_struct_field(sample, {'collector_status'}, struct());
            hwinfo_status = char(string(ParallelSimulationExecutor.pick_struct_field(collector_status, {'hwinfo'}, '')));
            if strcmpi(hwinfo_status, 'disabled')
                return;
            end
            hwinfo_transport = char(string(ParallelSimulationExecutor.pick_struct_field(sample, {'hwinfo_transport'}, 'none')));
            if ~(strcmpi(hwinfo_transport, 'shared_memory') || strcmpi(hwinfo_transport, 'csv'))
                return;
            end
            tf = ParallelSimulationExecutor.sample_has_hwinfo_series(sample) && ...
                ParallelSimulationExecutor.sample_has_hwinfo_catalog(sample);
        end

        function tf = sample_has_hwinfo_series(sample)
            tf = false;
            if ~(isstruct(sample) && isfield(sample, 'collector_series') && isstruct(sample.collector_series) && ...
                    isfield(sample.collector_series, 'hwinfo') && isstruct(sample.collector_series.hwinfo))
                return;
            end
            metric_fields = fieldnames(sample.collector_series.hwinfo);
            for i = 1:numel(metric_fields)
                value = sample.collector_series.hwinfo.(metric_fields{i});
                if isnumeric(value) && ~isempty(value) && any(isfinite(double(value(:))))
                    tf = true;
                    return;
                end
            end
        end

        function tf = sample_has_hwinfo_catalog(sample)
            tf = false;
            if ~(isstruct(sample) && isfield(sample, 'collector_metric_catalog') && ~isempty(sample.collector_metric_catalog))
                return;
            end
            catalog = sample.collector_metric_catalog;
            if isstruct(catalog)
                tf = ~isempty(fieldnames(catalog)) || ~isempty(catalog);
            elseif iscell(catalog) || isstring(catalog)
                tf = ~isempty(catalog);
            end
        end

        function sample = prefer_richer_collector_sample(primary, candidate)
            primary_empty = ~(isstruct(primary) && ~isempty(fieldnames(primary)));
            candidate_empty = ~(isstruct(candidate) && ~isempty(fieldnames(candidate)));
            sample = ExternalCollectorDispatcher.normalize_collector_payload(primary);
            candidate = ExternalCollectorDispatcher.normalize_collector_payload(candidate);
            if primary_empty
                sample = candidate;
                return;
            end
            if candidate_empty
                return;
            end

            sample_score = ParallelSimulationExecutor.collector_payload_score(sample);
            candidate_score = ParallelSimulationExecutor.collector_payload_score(candidate);
            sample_complete = ParallelSimulationExecutor.collector_payload_complete(sample);
            candidate_complete = ParallelSimulationExecutor.collector_payload_complete(candidate);
            if candidate_complete && ~sample_complete
                sample = candidate;
                return;
            end
            if sample_complete && ~candidate_complete
                return;
            end

            if candidate_score > sample_score && ~sample_complete
                sample = candidate;
                return;
            end
            if sample_score > candidate_score && ~candidate_complete
                return;
            end

            sample = ParallelSimulationExecutor.overlay_monitor_series(sample, candidate);
            merged_score = ParallelSimulationExecutor.collector_payload_score(sample);
            if merged_score < max(sample_score, candidate_score)
                if candidate_score > sample_score
                    sample = candidate;
                else
                    sample = primary;
                end
            end
        end

        function score = collector_payload_score(sample)
            score = 0;
            if ~(isstruct(sample) && ~isempty(fieldnames(sample)))
                return;
            end

            collector_status = ParallelSimulationExecutor.pick_struct_field(sample, {'collector_status'}, struct());
            hwinfo_status = char(string(ParallelSimulationExecutor.pick_struct_field(collector_status, {'hwinfo'}, '')));
            score = score + ParallelSimulationExecutor.collector_status_rank(hwinfo_status);

            hwinfo_transport = char(string(ParallelSimulationExecutor.pick_struct_field(sample, {'hwinfo_transport'}, 'none')));
            if strlength(string(strtrim(hwinfo_transport))) > 0 && ~strcmpi(hwinfo_transport, 'none')
                score = score + 40;
            end
            if ParallelSimulationExecutor.sample_has_hwinfo_series(sample)
                score = score + 45;
            end
            if ParallelSimulationExecutor.sample_has_hwinfo_catalog(sample)
                score = score + 35;
            end
            if isfield(sample, 'metrics') && isstruct(sample.metrics)
                metric_fields = fieldnames(sample.metrics);
                for i = 1:numel(metric_fields)
                    value = sample.metrics.(metric_fields{i});
                    if isnumeric(value) && ~isempty(value) && any(isfinite(double(value(:))))
                        score = score + 20;
                        break;
                    end
                end
            end
            raw_paths = ParallelSimulationExecutor.pick_struct_field(sample, {'raw_log_paths'}, struct());
            if isstruct(raw_paths) && isfield(raw_paths, 'hwinfo') && ...
                    strlength(string(strtrim(char(string(raw_paths.hwinfo))))) > 0
                score = score + 10;
            end
        end

        function rank = collector_status_rank(status_value)
            status_token = lower(strtrim(char(string(status_value))));
            switch status_token
                case {'shared_memory_connected', 'connected'}
                    rank = 100;
                case 'csv_fallback'
                    rank = 80;
                case {'shared_memory_incomplete', 'csv_target_mismatch', 'shared_memory_disabled', ...
                        'shared_memory_expired', 'csv_missing', 'parse_error', 'not_found'}
                    rank = 60;
                case {'disabled', 'off', 'none', ''}
                    rank = 0;
                otherwise
                    rank = 40;
            end
        end

        function tf = hwinfo_status_is_incomplete(sample)
            tf = false;
            if ~isstruct(sample) || isempty(fieldnames(sample))
                return;
            end
            status_struct = ParallelSimulationExecutor.pick_struct_field(sample, {'collector_status'}, struct());
            status_token = lower(strtrim(char(string(ParallelSimulationExecutor.pick_struct_field(status_struct, {'hwinfo'}, '')))));
            tf = strcmp(status_token, 'shared_memory_incomplete');
        end

        function power_w = estimate_power_from_cpu(cpu_usage)
            if ~(isnumeric(cpu_usage) && isscalar(cpu_usage) && isfinite(cpu_usage))
                power_w = NaN;
                return;
            end
            load_pct = min(max(double(cpu_usage), 0), 100);
            power_w = 45 + (load_pct / 100) * (220 - 45);
        end

        function wrapped = wrap_worker_failure(worker_error)
            % Wrap raw future error with a detailed diagnostic summary.
            if nargin < 1 || isempty(worker_error)
                wrapped = MException('ParallelSimulationExecutor:WorkerFailureUnknown', ...
                    'Background worker failed with unknown error.');
                return;
            end

            err_id = char(string(worker_error.identifier));
            err_msg = char(string(worker_error.message));
            if isempty(strtrim(err_id))
                err_id = 'unknown_identifier';
            end
            try
                report_txt = getReport(worker_error, 'extended', 'hyperlinks', 'off');
            catch
                report_txt = err_msg;
            end

            wrapped = MException('ParallelSimulationExecutor:WorkerFailure', ...
                'Background worker failed (%s): %s\n\nWorker report:\n%s', ...
                err_id, err_msg, report_txt);
            wrapped = addCause(wrapped, worker_error);
        end

        function policy = default_resource_policy()
            policy = struct( ...
                'cpu_target_pct', 80, ...
                'gpu_target_pct', 80, ...
                'memory_target_pct', 70, ...
                'monitor_hz', 2, ...
                'progress_hz', 6, ...
                'monitor_period_seconds', 0.5, ...
                'cpu_probe_interval_seconds', 1.0, ...
                'pool_workers_requested', 1, ...
                'pool_workers_effective', 1, ...
                'target_pool_workers', 1, ...
                'max_pool_workers', 1, ...
                'thread_cap', 1, ...
                'max_threads', 1, ...
                'gpu_enabled_effective', false, ...
                'host_profile_id', 'none', ...
                'planner_notes', {{}}, ...
                'worker_topology', 'single_worker_background');
        end

        function policy = resolve_resource_policy(parameters, settings, run_config)
            policy = ParallelSimulationExecutor.default_resource_policy();

            if nargin < 2 || ~isstruct(settings)
                settings = struct();
            end
            if nargin < 1 || ~isstruct(parameters)
                parameters = struct();
            end
            if nargin < 3 || ~isstruct(run_config)
                run_config = struct();
            end

            mode_token = '';
            method_token = '';
            if isfield(run_config, 'mode')
                mode_token = char(string(run_config.mode));
            end
            if isfield(run_config, 'method')
                method_token = char(string(run_config.method));
            end

            if ~(exist('ExecutionResourcePlanner', 'class') == 8 || exist('ExecutionResourcePlanner', 'file') == 2)
                error('ParallelSimulationExecutor:MissingExecutionResourcePlanner', ...
                    'ExecutionResourcePlanner is required for runtime resource policy resolution.');
            end
            planned = ExecutionResourcePlanner.plan(parameters, settings, ...
                'ModeToken', mode_token, ...
                'MethodToken', method_token);

            fields = fieldnames(planned);
            for i = 1:numel(fields)
                policy.(fields{i}) = planned.(fields{i});
            end

            sample_interval = ParallelSimulationExecutor.resolve_sample_interval(settings);
            if isfinite(sample_interval) && sample_interval > 0
                sample_interval = max(0.1, double(sample_interval));
                policy.monitor_period_seconds = sample_interval;
                policy.monitor_hz = 1 / sample_interval;
                policy.cpu_probe_interval_seconds = max(0.5, sample_interval);
            end

            if ~isfield(policy, 'monitor_period_seconds') || policy.monitor_period_seconds <= 0
                policy.monitor_hz = min(max(double(policy.monitor_hz), 0.5), 20);
                policy.monitor_period_seconds = 1 / policy.monitor_hz;
            end
            if ~isfield(policy, 'cpu_probe_interval_seconds') || policy.cpu_probe_interval_seconds <= 0
                policy.cpu_probe_interval_seconds = max(0.5, policy.monitor_period_seconds);
            end
            if ~isfield(policy, 'pool_workers_effective')
                if isfield(policy, 'target_pool_workers')
                    policy.pool_workers_effective = max(1, round(double(policy.target_pool_workers)));
                else
                    policy.pool_workers_effective = 1;
                end
            end
            if ~isfield(policy, 'target_pool_workers')
                policy.target_pool_workers = max(1, round(double(policy.pool_workers_effective)));
            end
            if ~isfield(policy, 'max_pool_workers')
                policy.max_pool_workers = max(1, round(double(policy.pool_workers_effective)));
            end
            if ~isfield(policy, 'thread_cap')
                policy.thread_cap = 1;
            end
            policy.max_threads = max(1, round(double(policy.thread_cap)));
            if ~isfield(policy, 'worker_topology') || isempty(policy.worker_topology)
                policy.worker_topology = ParallelSimulationExecutor.resolve_worker_topology(policy.target_pool_workers);
            end
        end

        function sample_interval = resolve_sample_interval(settings)
            sample_interval = NaN;
            if nargin < 1 || ~isstruct(settings)
                return;
            end
            candidates = {};
            if isfield(settings, 'sample_interval')
                candidates{end + 1} = settings.sample_interval; %#ok<AGROW>
            end
            if isfield(settings, 'sustainability') && isstruct(settings.sustainability) && ...
                    isfield(settings.sustainability, 'sample_interval')
                candidates{end + 1} = settings.sustainability.sample_interval; %#ok<AGROW>
            end
            for i = 1:numel(candidates)
                candidate = candidates{i};
                if isnumeric(candidate) && isscalar(candidate) && isfinite(candidate) && candidate > 0
                    sample_interval = double(candidate);
                    return;
                end
            end
        end

        function apply_memory_guard(parameters, policy)
            if nargin < 2 || ~isstruct(policy)
                return;
            end
            if ~isstruct(parameters)
                return;
            end
            if ~isfield(parameters, 'Nx') || ~isfield(parameters, 'Ny')
                return;
            end

            Nx = max(1, round(double(parameters.Nx)));
            Ny = max(1, round(double(parameters.Ny)));
            n_snap = 0;
            if isfield(parameters, 'num_snapshots')
                n_snap = max(n_snap, round(double(parameters.num_snapshots)));
            end
            if isfield(parameters, 'num_plot_snapshots')
                n_snap = max(n_snap, round(double(parameters.num_plot_snapshots)));
            end
            n_snap = max(1, n_snap);

            bytes_per_double = 8;
            core_field_count = 18; % omega/psi/u/v/stages/work arrays and diagnostics buffers
            snapshot_field_count = 2; % omega + psi snapshot cubes
            est_bytes = Nx * Ny * bytes_per_double * ...
                (core_field_count + snapshot_field_count * n_snap);
            est_mb = 1.25 * est_bytes / 1024^2; % include overhead margin

            if ispc
                try
                    [~, sys] = memory;
                    available_mb = double(sys.PhysicalMemory.Available) / 1024^2;
                catch
                    available_mb = NaN;
                end
            else
                available_mb = NaN;
            end

            if ~isfinite(available_mb)
                return;
            end

            mem_budget_mb = available_mb * policy.memory_target_pct / 100;
            if est_mb > mem_budget_mb
                error('ParallelSimulationExecutor:MemoryBudgetExceeded', ...
                    ['Estimated runtime memory %.1f MB exceeds budget %.1f MB ' ...
                     '(allocation %.0f%% of available %.1f MB). Reduce Nx/Ny/snapshots or raise memory allocation.'], ...
                    est_mb, mem_budget_mb, policy.memory_target_pct, available_mb);
            end
        end

        function n = resolve_available_threads()
            try
                n = max(1, feature('numcores'));
            catch
                n = 1;
            end
        end

        function tf = should_skip_host_monitor_timer()
            tf = false;
            try
                tf = ~usejava('desktop');
            catch
                tf = false;
            end
            if tf
                return;
            end
            try
                tf = ~feature('ShowFigureWindows');
            catch
                tf = false;
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
                n = ParallelSimulationExecutor.resolve_available_threads();
            end
            n = max(1, n);
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

        function out = pick_percent(source, key, fallback)
            out = fallback;
            if isfield(source, key)
                val = double(source.(key));
                if isfinite(val)
                    out = min(max(val, 5), 100);
                end
            end
        end

        function out = pick_positive(source, key, fallback)
            out = fallback;
            if isfield(source, key)
                val = double(source.(key));
                if isfinite(val) && val > 0
                    out = val;
                end
            end
        end

        function value = pick_struct_field(data, field_names, fallback)
            value = fallback;
            if nargin < 3
                fallback = [];
                value = fallback;
            end
            if ~isstruct(data)
                return;
            end
            for idx = 1:numel(field_names)
                field_name = field_names{idx};
                if isfield(data, field_name)
                    value = data.(field_name);
                    return;
                end
            end
        end

        function gpu_usage = gpu_usage_proxy()
            % GPU utilization proxy based on VRAM pressure.
            gpu_usage = NaN;
            try
                g = gpuDevice();
                if isprop(g, 'TotalMemory') && isprop(g, 'AvailableMemory')
                    total = double(g.TotalMemory);
                    used = double(g.TotalMemory - g.AvailableMemory);
                    if total > 0
                        gpu_usage = 100 * used / total;
                    end
                end
            catch
                gpu_usage = NaN;
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

function set_cleanup_flag(obj, value)
    try
        if ~isempty(obj) && isvalid(obj)
            obj.cleanup_in_progress = logical(value);
        end
    catch
    end
end

function parallel_executor_timer_monitor_callback(~, ~, obj)
    if isempty(obj)
        return;
    end
    try
        if isvalid(obj)
            obj.monitor_callback();
        end
    catch
    end
end

function parallel_executor_timer_error_callback(~, ~, obj)
    if isempty(obj)
        return;
    end
    try
        if isvalid(obj)
            obj.handle_timer_error();
        end
    catch
    end
end
