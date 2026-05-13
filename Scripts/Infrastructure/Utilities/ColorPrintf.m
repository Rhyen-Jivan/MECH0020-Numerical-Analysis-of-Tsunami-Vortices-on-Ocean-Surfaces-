classdef ColorPrintf
    % ColorPrintf - Safe colored output for MATLAB Command Window
    %
    % Purpose:
    %   Provides colored console output using cprintf (Yair Altman)
    %   when available, with graceful fallback to plain fprintf.
    %   Replaces ANSI escape codes that don't render in MATLAB desktop.
    %
    % Usage:
    %   ColorPrintf.success('Operation completed');
    %   ColorPrintf.error('Something failed: %s', msg);
    %   ColorPrintf.warn('CFL number is high: %.4f', cfl);
    %   ColorPrintf.info('Starting simulation...');
    %   ColorPrintf.header('SIMULATION RESULTS');
    %   ColorPrintf.section('Configuration');
    %   ColorPrintf.colored('Blue', 'Custom blue text\n');
    %
    % Supported styles (via cprintf):
    %   Named: 'Red', 'Green', 'Blue', 'Cyan', 'Magenta', 'Yellow', 'White'
    %   Bold:  '*Red', '*Green', '*Blue' (prefix * for bold)
    %   Under: '-Red', '-Blue' (prefix - for underline)
    %   RGB:   [0.1, 0.7, 0.3] or '#1ab34d'
    %
    % Notes:
    %   - cprintf only works in MATLAB desktop Command Window
    %   - In batch/terminal/deployed mode, falls back to plain fprintf
    %   - Never throws on color failure; always prints the text
    %
    % See also: cprintf, fprintf

    methods (Static)
        %% ===== HIGH-LEVEL API =====

        function success(fmt, varargin)
            % Print success message in green with check mark
            msg = sprintf(fmt, varargin{:});
            ColorPrintf.colored('*[0.0, 0.7, 0.0]', '[SUCCESS] %s\n', msg);
        end

        function error_msg(fmt, varargin)
            % Print error message in red with X mark
            msg = sprintf(fmt, varargin{:});
            ColorPrintf.colored('*[0.8, 0.0, 0.0]', '[ERROR] %s\n', msg);
        end

        function warn(fmt, varargin)
            % Print warning message in yellow/orange
            msg = sprintf(fmt, varargin{:});
            ColorPrintf.colored('[0.85, 0.55, 0.0]', '[WARN] %s\n', msg);
        end

        function info(fmt, varargin)
            % Print info message in cyan
            msg = sprintf(fmt, varargin{:});
            ColorPrintf.colored('[0.0, 0.6, 0.8]', '[INFO] %s\n', msg);
        end

        function header(title)
            % Print section header with double-line border
            border = repmat('=', 1, 63);
            SafeConsoleIO.fprintf('\n');
            ColorPrintf.colored('*[0.2, 0.4, 0.8]', '%s\n', border);
            ColorPrintf.colored('*[0.2, 0.4, 0.8]', '  %s\n', title);
            ColorPrintf.colored('*[0.2, 0.4, 0.8]', '%s\n', border);
            SafeConsoleIO.fprintf('\n');
        end

        function section(title)
            % Print subsection header with single-line border
            border = repmat('-', 1, 63);
            ColorPrintf.colored('[0.4, 0.4, 0.4]', '%s\n', border);
            ColorPrintf.colored('*[0.3, 0.3, 0.6]', '  %s\n', title);
            ColorPrintf.colored('[0.4, 0.4, 0.4]', '%s\n', border);
            SafeConsoleIO.fprintf('\n');
        end

        function success_inline(fmt, varargin)
            % Print inline success text (no newline, no prefix)
            msg = sprintf(fmt, varargin{:});
            ColorPrintf.colored('[0.0, 0.7, 0.0]', '%s', msg);
        end

        function warn_inline(fmt, varargin)
            % Print inline warning text (no newline, no prefix)
            msg = sprintf(fmt, varargin{:});
            ColorPrintf.colored('[0.85, 0.55, 0.0]', '%s', msg);
        end

        function error_inline(fmt, varargin)
            % Print inline error text (no newline, no prefix)
            msg = sprintf(fmt, varargin{:});
            ColorPrintf.colored('[0.8, 0.0, 0.0]', '%s', msg);
        end

        %% ===== SEVERITY-BASED API (for ErrorHandler integration) =====

        function print_severity(severity, fmt, varargin)
            % Print message color-coded by severity
            msg = sprintf(fmt, varargin{:});
            switch upper(severity)
                case {'CRITICAL', 'ERROR'}
                    ColorPrintf.colored('*[0.8, 0.0, 0.0]', '%s\n', msg);
                case 'WARN'
                    ColorPrintf.colored('[0.85, 0.55, 0.0]', '%s\n', msg);
                case 'INFO'
                    ColorPrintf.colored('[0.0, 0.6, 0.8]', '%s\n', msg);
                case 'SUCCESS'
                    ColorPrintf.colored('*[0.0, 0.7, 0.0]', '%s\n', msg);
                otherwise
                    SafeConsoleIO.fprintf('%s\n', msg);
            end
        end

        %% ===== MONITOR API (for dark-theme live monitor) =====

        function monitor_header(method, mode, ic_type)
            % Print simulation monitor header (dark theme style)
            border = repmat('=', 1, 55);
            SafeConsoleIO.fprintf('\n');
            ColorPrintf.colored('*[0.0, 0.8, 0.9]', '%s\n', border);
            ColorPrintf.colored('*[0.0, 0.8, 0.9]', '         TSUNAMI VORTEX SIMULATION MONITOR\n');
            ColorPrintf.colored('*[0.0, 0.8, 0.9]', '%s\n', border);
            ColorPrintf.colored('[0.3, 0.9, 0.3]', 'Method: ');
            SafeConsoleIO.fprintf('%s  ', method);
            ColorPrintf.colored('[0.5, 0.5, 0.5]', '|  ');
            ColorPrintf.colored('[0.3, 0.9, 0.3]', 'Mode: ');
            SafeConsoleIO.fprintf('%s  ', mode);
            ColorPrintf.colored('[0.5, 0.5, 0.5]', '|  ');
            ColorPrintf.colored('[0.3, 0.9, 0.3]', 'IC: ');
            SafeConsoleIO.fprintf('%s\n', ic_type);
            ColorPrintf.colored('[0.5, 0.5, 0.5]', '%s\n', repmat('-', 1, 55));
        end

        function monitor_update(step, t, dt_val, cfl, max_omega, elapsed)
            % Print monitor update line (overwrites previous line)
            ColorPrintf.colored('[0.3, 0.9, 0.3]', '[Step %d]', step);
            SafeConsoleIO.fprintf('  t=%.4f  dt=%.2e  ', t, dt_val);
            ColorPrintf.colored('[0.9, 0.8, 0.0]', 'CFL=%.3f', cfl);
            SafeConsoleIO.fprintf('  ');
            ColorPrintf.colored('[0.0, 0.8, 0.9]', '|w|max=%.2e', max_omega);
            SafeConsoleIO.fprintf('  ');
            ColorPrintf.colored('[0.5, 0.5, 0.5]', 'Elapsed: %.1fs', elapsed);
            SafeConsoleIO.fprintf('\n');
        end

        function monitor_footer(total_time)
            % Print monitor completion footer
            ColorPrintf.colored('[0.5, 0.5, 0.5]', '%s\n', repmat('-', 1, 55));
            ColorPrintf.colored('*[0.0, 0.8, 0.0]', 'Simulation completed');
            SafeConsoleIO.fprintf('  |  Total time: %.2fs\n\n', total_time);
        end

        %% ===== LOW-LEVEL API =====

        function colored(style, fmt, varargin)
            % Print colored text using cprintf if available, else fprintf
            %
            % Inputs:
            %   style - cprintf style string or RGB triplet
            %   fmt   - Format string (like fprintf)
            %   varargin - Format arguments

            msg = sprintf(fmt, varargin{:});
            if ~SafeConsoleIO.is_enabled()
                return;
            end

            % Only attempt color in MATLAB desktop mode
            if ~usejava('desktop')
                SafeConsoleIO.fprintf('%s', msg);
                return;
            end

            try
                % Try cprintf first
                cprintf(style, '%s', msg);
            catch ME
                ColorPrintf.warn_once('ColorPrintf:CprintfFallback', ...
                    'cprintf unavailable/failed; falling back to fprintf (first warning only): %s', ME.message);
                % cprintf not available or failed - fall back to fprintf
                SafeConsoleIO.fprintf('%s', msg);
            end
        end

        function available = is_color_available()
            % Check if colored output is available
            available = usejava('desktop') && (exist('cprintf', 'file') == 2);
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
