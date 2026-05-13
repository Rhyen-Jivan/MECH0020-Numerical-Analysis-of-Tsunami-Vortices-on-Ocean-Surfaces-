% ========================================================================
% HelperUtils - Common Utility Functions (Static Class)
% ========================================================================
% Small reusable helper functions extracted from Analysis.m
% Used by: Analysis.m, MetricsExtractor.m, ResultsPersistence.m
%
% Usage: HelperUtils.safe_get(struct, field, default)
%
% Methods:
%   safe_get(S, field, default) - Safe struct field access with default
%   take_scalar_metric(val) - Coerce metric to scalar (extract final value)
%   sanitize_token(s) - Make string filesystem-safe
%
% Created: 2026-02-05
% Part of: Tsunami Vortex Analysis Framework
% ========================================================================

classdef HelperUtils
    methods(Static)
        function v = safe_get(S, field, default)
            % Safe struct field accessor: returns default if field is missing.
            if isstruct(S) && isfield(S, field)
                v = S.(field);
            else
                v = default;
            end
        end
        
        function val = take_scalar_metric(val)
            % Coerce metric value to scalar for logical operations
            % If array/vector, extract final value; if empty, return NaN
            if isempty(val)
                val = NaN;
                return;
            end
            if ~isscalar(val)
                val = val(end);
            end
        end
        
        function s = sanitize_token(s)
            % Make string filesystem-safe: replace spaces with underscores, remove special chars
            s = string(s);
            s = regexprep(s, "\s+", "_");
            s = regexprep(s, "[^a-zA-Z0-9_\-\.]", "");
        end
    end
end