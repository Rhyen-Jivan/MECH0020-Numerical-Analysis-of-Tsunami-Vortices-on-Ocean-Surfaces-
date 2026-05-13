function invoke_runtime_progress_callback(progress_callback, payload)
%INVOKE_RUNTIME_PROGRESS_CALLBACK Invoke a runtime progress transport safely.
%   Supports direct function handles and structured callback specifications.

    if nargin < 2 || isempty(progress_callback) || ~isstruct(payload)
        return;
    end

    if isa(progress_callback, 'function_handle')
        progress_callback(payload);
        return;
    end

    if ~isstruct(progress_callback)
        return;
    end

    callback_kind = '';
    if isfield(progress_callback, 'callback_kind') && ~isempty(progress_callback.callback_kind)
        callback_kind = char(string(progress_callback.callback_kind));
    elseif isfield(progress_callback, 'kind') && ~isempty(progress_callback.kind)
        callback_kind = char(string(progress_callback.kind));
    end

    switch lower(strtrim(callback_kind))
        case 'ui_controller_live_monitor'
            app = [];
            cfg = struct();
            if isfield(progress_callback, 'app')
                app = progress_callback.app;
            end
            if isfield(progress_callback, 'cfg') && isstruct(progress_callback.cfg)
                cfg = progress_callback.cfg;
            end
            if isempty(app)
                return;
            end
            try
                if ~isvalid(app)
                    return;
                end
            catch
                return;
            end
            app.deliver_live_monitor_payload(payload, cfg);

        case 'parallel_executor_relay'
            executor = [];
            if isfield(progress_callback, 'executor')
                executor = progress_callback.executor;
            end
            if isempty(executor)
                return;
            end
            try
                if ~isvalid(executor)
                    return;
                end
            catch
                return;
            end
            executor.relay_progress_payload(payload);
    end
end
