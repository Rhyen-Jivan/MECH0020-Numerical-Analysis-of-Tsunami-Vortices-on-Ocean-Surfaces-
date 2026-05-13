classdef SafeConsoleIO
    % SafeConsoleIO - Guard command-window writes against UI launch failures.
    %
    % Purpose:
    %   Centralize stdout/warning writes so UI-backed launches can suppress
    %   duplicate command-window output and gracefully absorb stream failures
    %   such as iolib:badbit without crashing the run.

    methods (Static)
        function reset_stream_failure()
            if isappdata(0, 'tsunami_console_stream_failed')
                rmappdata(0, 'tsunami_console_stream_failed');
            end
            if isappdata(0, 'tsunami_console_stream_failure_note')
                rmappdata(0, 'tsunami_console_stream_failure_note');
            end
        end

        function push_suppression(~)
            depth = SafeConsoleIO.suppression_depth();
            setappdata(0, 'tsunami_console_suppression_depth', depth + 1);
        end

        function pop_suppression()
            depth = SafeConsoleIO.suppression_depth();
            if depth <= 1
                if isappdata(0, 'tsunami_console_suppression_depth')
                    rmappdata(0, 'tsunami_console_suppression_depth');
                end
                return;
            end
            setappdata(0, 'tsunami_console_suppression_depth', depth - 1);
        end

        function tf = is_enabled()
            tf = ~SafeConsoleIO.is_suppressed() && ~SafeConsoleIO.is_stream_failed();
        end

        function tf = is_suppressed()
            tf = SafeConsoleIO.suppression_depth() > 0;
        end

        function tf = is_stream_failed()
            tf = isappdata(0, 'tsunami_console_stream_failed') && ...
                logical(getappdata(0, 'tsunami_console_stream_failed'));
        end

        function count = fprintf(varargin)
            count = 0;
            if ~SafeConsoleIO.is_enabled()
                return;
            end
            try
                count = builtin('fprintf', varargin{:});
            catch ME
                if SafeConsoleIO.is_stream_write_failure(ME)
                    SafeConsoleIO.mark_stream_failed(ME);
                    return;
                end
                rethrow(ME);
            end
        end

        function warning(identifier, message, varargin)
            if ~SafeConsoleIO.is_enabled()
                return;
            end
            try
                builtin('warning', identifier, message, varargin{:});
            catch ME
                if SafeConsoleIO.is_stream_write_failure(ME)
                    SafeConsoleIO.mark_stream_failed(ME);
                    return;
                end
                rethrow(ME);
            end
        end
    end

    methods (Static, Access = private)
        function depth = suppression_depth()
            depth = 0;
            if isappdata(0, 'tsunami_console_suppression_depth')
                depth = double(getappdata(0, 'tsunami_console_suppression_depth'));
                if ~isscalar(depth) || ~isfinite(depth) || depth < 0
                    depth = 0;
                end
            end
        end

        function tf = is_stream_write_failure(ME)
            identifier = lower(char(string(ME.identifier)));
            message = lower(char(string(ME.message)));
            tf = contains(identifier, 'iolib:badbit') || ...
                contains(message, 'error writing to output stream') || ...
                contains(message, 'iostream stream error');
        end

        function mark_stream_failed(ME)
            setappdata(0, 'tsunami_console_stream_failed', true);
            setappdata(0, 'tsunami_console_stream_failure_note', struct( ...
                'identifier', char(string(ME.identifier)), ...
                'message', char(string(ME.message)), ...
                'timestamp', char(datetime('now', 'Format', 'yyyy-MM-dd HH:mm:ss'))));
        end
    end
end
