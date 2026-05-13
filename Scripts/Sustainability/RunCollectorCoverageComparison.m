function artifact_summary = RunCollectorCoverageComparison(varargin)
% RunCollectorCoverageComparison  Emit collector coverage artifacts.
%
% Usage:
%   artifact_summary = RunCollectorCoverageComparison(monitor_series, paths, run_id)
%   artifact_summary = RunCollectorCoverageComparison(summary_struct)

    monitor_series = struct();
    paths = struct();
    run_id = '';

    first_arg_is_context = false;
    if nargin >= 1 && isstruct(varargin{1})
        first_fields = fieldnames(varargin{1});
        context_markers = {'monitor_series', 'results', 'run_config', 'paths', 'workflow_kind', ...
            'phase_id', 'metadata', 'run_id'};
        first_arg_is_context = any(ismember(context_markers, first_fields));
    end

    if first_arg_is_context
        summary = varargin{1};
        artifact_summary = ExternalCollectorDispatcher.write_run_artifacts(summary);
        return;
    else
        if nargin >= 1 && isstruct(varargin{1})
            monitor_series = varargin{1};
        end
        if nargin >= 2 && isstruct(varargin{2})
            paths = varargin{2};
        end
        if nargin >= 3
            run_id = char(string(varargin{3}));
        end
    end

    artifact_summary = ExternalCollectorDispatcher.write_run_artifacts(monitor_series, paths, run_id);
end
