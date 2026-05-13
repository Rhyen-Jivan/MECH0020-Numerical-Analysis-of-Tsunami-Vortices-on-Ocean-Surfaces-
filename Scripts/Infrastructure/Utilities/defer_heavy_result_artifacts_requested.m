function tf = defer_heavy_result_artifacts_requested(settings)
%defer_heavy_result_artifacts_requested True when heavy post-run artifacts should defer.
%
% UI-launched runs that rely on host-owned publication should persist only
% the minimal saved package on the worker, then leave heavier figures,
% reports, and enrichments to the host-side background queue.

    tf = false;
    if nargin < 1 || ~isstruct(settings)
        return;
    end

    field_candidates = { ...
        'defer_heavy_result_artifacts', ...
        'defer_heavy_artifacts', ...
        'defer_result_exports'};
    for i = 1:numel(field_candidates)
        key = field_candidates{i};
        if isfield(settings, key)
            tf = logical(settings.(key));
            return;
        end
    end
end
