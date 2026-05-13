function progress_callback = resolve_runtime_progress_callback(Settings)
%RESOLVE_RUNTIME_PROGRESS_CALLBACK Resolve runtime progress transport.
%   Prefer a direct function handle in synchronous paths. Otherwise accept
%   both DataQueue and PollableDataQueue worker channels.

    progress_callback = [];
    if nargin < 1 || ~isstruct(Settings)
        return;
    end

    if isfield(Settings, 'ui_progress_callback')
        callback_candidate = Settings.ui_progress_callback;
        if isa(callback_candidate, 'function_handle') || isstruct(callback_candidate)
            progress_callback = callback_candidate;
            return;
        end
    end

    if ~isfield(Settings, 'progress_data_queue')
        return;
    end

    q = Settings.progress_data_queue;
    if isa(q, 'parallel.pool.DataQueue') || isa(q, 'parallel.pool.PollableDataQueue')
        progress_callback = @(payload) send(q, payload);
    end
end
