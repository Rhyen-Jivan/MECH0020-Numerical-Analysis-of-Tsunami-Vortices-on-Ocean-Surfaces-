classdef ErrorHandler
    % ErrorHandler - Utilities for structured error handling
    %
    % Purpose:
    %   Provides functions to build, throw, and log structured errors
    %   Integrates with ErrorRegistry for consistent error reporting
    %   Supports color-coded console output and UI panel logging
    %
    % Usage:
    %   % Build and throw structured error:
    %   ErrorHandler.throw('CFG-VAL-0001', ...
    %       'file', mfilename, ...
    %       'line', 42, ...
    %       'context', struct('Nx', Nx, 'Ny', Ny));
    %
    %   % Log error without throwing:
    %   ErrorHandler.log('WARN', 'MON-SUS-0001', ...
    %       'message', 'Hardware monitor unavailable', ...
    %       'file', mfilename);
    %
    %   % Build structured error object:
    %   err_struct = ErrorHandler.build('UI-CB-0001', ...
    %       'file', 'UIController.m', ...
    %       'line', 123, ...
    %       'context', struct('callback', 'launch_simulation'));
    %
    % Terminal Colors (Console Output):
    %   CRITICAL/ERROR:  Red
    %   WARN:            Yellow
    %   INFO:            Cyan
    %   SUCCESS:         Green

    % No ANSI constants needed - ColorPrintf handles all coloring

    methods (Static)
        function err_struct = build(error_code, varargin)
            % Build structured error object
            %
            % Inputs:
            %   error_code - Error code (e.g., 'CFG-VAL-0001')
            %   varargin - Name-value pairs:
            %       'file' - Source file where error occurred (default: '')
            %       'line' - Line number (default: 0)
            %       'message' - Custom message (default: from registry)
            %       'context' - Context struct with additional data (default: struct())
            %       'cause' - Underlying exception (MException object, default: [])
            %
            % Output:
            %   err_struct - Structured error with all metadata

            % Parse inputs
            p = inputParser;
            addRequired(p, 'error_code', @ischar);
            addParameter(p, 'file', '', @ischar);
            addParameter(p, 'line', 0, @isnumeric);
            addParameter(p, 'message', '', @ischar);
            addParameter(p, 'context', struct(), @isstruct);
            addParameter(p, 'cause', [], @(x) isempty(x) || isa(x, 'MException'));
            parse(p, error_code, varargin{:});

            % Lookup error code in registry
            reg_info = ErrorRegistry.lookup(error_code);

            % Build structured error
            err_struct = struct();
            err_struct.code = reg_info.code;
            err_struct.severity = reg_info.severity;
            err_struct.category = reg_info.category;
            err_struct.timestamp = char(datetime('now', 'Format', 'yyyy-MM-dd HH:mm:ss'));

            % Message: custom or from registry
            if ~isempty(p.Results.message)
                err_struct.message = p.Results.message;
            else
                err_struct.message = reg_info.description;
            end

            % Location
            err_struct.file = p.Results.file;
            err_struct.line = p.Results.line;

            % Remediation and causes from registry
            err_struct.remediation = reg_info.remediation;
            err_struct.causes = reg_info.causes;

            % Context data
            err_struct.context = p.Results.context;

            % Underlying cause (if any)
            if ~isempty(p.Results.cause)
                err_struct.underlying_cause = struct(...
                    'message', p.Results.cause.message, ...
                    'identifier', p.Results.cause.identifier, ...
                    'stack', p.Results.cause.stack);
            else
                err_struct.underlying_cause = [];
            end
        end

        function throw(error_code, varargin)
            % Build and throw structured error as MException
            %
            % Inputs:
            %   error_code - Error code (e.g., 'CFG-VAL-0001')
            %   varargin - Same as build()
            %
            % Throws:
            %   MException with identifier = error_code

            err_struct = ErrorHandler.build(error_code, varargin{:});

            % Format complete error message
            msg = ErrorHandler.format_error_message(err_struct);

            % Create and throw MException
            % Convert error_code 'CFG-VAL-0001' to MATLAB identifier 'CFG:VAL_0001'
            % (component:mnemonic format, replace only first hyphen with colon)
            parts = strsplit(error_code, '-');
            if length(parts) >= 2
                identifier = sprintf('%s:%s', parts{1}, strjoin(parts(2:end), '_'));
            else
                identifier = sprintf('REPO:%s', error_code);  % Fallback for malformed codes
            end
            ME = MException(identifier, '%s', msg);

            % Attach cause if present
            if ~isempty(err_struct.underlying_cause)
                % Reconstruct MException from underlying cause
                cause_ME = MException(err_struct.underlying_cause.identifier, ...
                    '%s', err_struct.underlying_cause.message);
                ME = addCause(ME, cause_ME);
            end

            throw(ME);
        end

        function log(severity, error_code, varargin)
            % Log error without throwing (non-fatal errors)
            %
            % Inputs:
            %   severity - 'CRITICAL' | 'ERROR' | 'WARN' | 'INFO'
            %   error_code - Error code
            %   varargin - Same as build()
            %
            % Prints color-coded message to console

            err_struct = ErrorHandler.build(error_code, varargin{:});

            % Override severity from call
            err_struct.severity = upper(severity);

            % Format message
            msg = ErrorHandler.format_error_message(err_struct);

            % Print with color coding
            ErrorHandler.print_colored(err_struct.severity, msg);
        end

        function log_success(message)
            % Log success message in green
            ErrorHandler.print_colored('SUCCESS', sprintf('[SUCCESS] %s', message));
        end

        function log_info(message)
            % Log info message in cyan
            ErrorHandler.print_colored('INFO', sprintf('[INFO] %s', message));
        end

        function msg = format_error_message(err_struct)
            % Format structured error into human-readable message
            %
            % Input:
            %   err_struct - Structured error from build()
            %
            % Output:
            %   msg - Formatted multi-line message

            % Pre-allocate lines cell with estimated capacity
            max_lines = 20;
            lines = cell(1, max_lines);
            n = 0;

            % Header
            n = n + 1; lines{n} = sprintf('[%s] %s', err_struct.severity, err_struct.code);
            n = n + 1; lines{n} = sprintf('Category: %s', err_struct.category);

            % Location
            if ~isempty(err_struct.file)
                if err_struct.line > 0
                    n = n + 1; lines{n} = sprintf('Location: %s:%d', err_struct.file, err_struct.line);
                else
                    n = n + 1; lines{n} = sprintf('Location: %s', err_struct.file);
                end
            end

            % Message
            n = n + 1; lines{n} = sprintf('Message: %s', err_struct.message);

            % Remediation
            n = n + 1; lines{n} = sprintf('Fix: %s', err_struct.remediation);

            % Likely causes
            if ~isempty(err_struct.causes)
                n = n + 1; lines{n} = 'Likely causes:';
                for i = 1:length(err_struct.causes)
                    n = n + 1; lines{n} = sprintf('  - %s', err_struct.causes{i});
                end
            end

            % Context (if present)
            if ~isempty(fieldnames(err_struct.context))
                n = n + 1; lines{n} = 'Context:';
                fields = fieldnames(err_struct.context);
                for i = 1:length(fields)
                    val = err_struct.context.(fields{i});
                    if isnumeric(val) || islogical(val)
                        n = n + 1; lines{n} = sprintf('  %s = %s', fields{i}, mat2str(val));
                    elseif ischar(val)
                        n = n + 1; lines{n} = sprintf('  %s = ''%s''', fields{i}, val);
                    else
                        n = n + 1; lines{n} = sprintf('  %s = [%s]', fields{i}, class(val));
                    end
                end
            end

            % Underlying cause (if present)
            if ~isempty(err_struct.underlying_cause)
                n = n + 1; lines{n} = 'Underlying cause:';
                n = n + 1; lines{n} = sprintf('  %s: %s', ...
                    err_struct.underlying_cause.identifier, ...
                    err_struct.underlying_cause.message);
            end

            % Trim and join
            lines = lines(1:n);

            % Join lines
            msg = strjoin(lines, '\n');
        end

        function print_colored(severity, message)
            % Print message with color coding based on severity
            % Uses ColorPrintf for cross-platform colored output
            ColorPrintf.print_severity(severity, '%s', message);
        end

        function pretty_print_error(err_struct)
            % Pretty-print structured error to console
            %
            % Input:
            %   err_struct - Structured error from build()

            fprintf('\n');
            fprintf('═══════════════════════════════════════════════════════════════\n');
            fprintf('  ERROR DETAILS\n');
            fprintf('═══════════════════════════════════════════════════════════════\n\n');

            fprintf('Code:        %s\n', err_struct.code);
            fprintf('Severity:    %s\n', err_struct.severity);
            fprintf('Category:    %s\n', err_struct.category);
            fprintf('Timestamp:   %s\n', err_struct.timestamp);

            if ~isempty(err_struct.file)
                if err_struct.line > 0
                    fprintf('Location:    %s:%d\n', err_struct.file, err_struct.line);
                else
                    fprintf('Location:    %s\n', err_struct.file);
                end
            end

            fprintf('\n');
            fprintf('Message:\n  %s\n\n', err_struct.message);
            fprintf('Remediation:\n  %s\n\n', err_struct.remediation);

            if ~isempty(err_struct.causes)
                fprintf('Likely Causes:\n');
                for i = 1:length(err_struct.causes)
                    fprintf('  %d. %s\n', i, err_struct.causes{i});
                end
                fprintf('\n');
            end

            if ~isempty(fieldnames(err_struct.context))
                fprintf('Context:\n');
                fields = fieldnames(err_struct.context);
                for i = 1:length(fields)
                    val = err_struct.context.(fields{i});
                    if isnumeric(val) || islogical(val)
                        fprintf('  %s = %s\n', fields{i}, mat2str(val));
                    elseif ischar(val)
                        fprintf('  %s = ''%s''\n', fields{i}, val);
                    else
                        fprintf('  %s = [%s]\n', fields{i}, class(val));
                    end
                end
                fprintf('\n');
            end

            fprintf('═══════════════════════════════════════════════════════════════\n\n');
        end
    end
end
