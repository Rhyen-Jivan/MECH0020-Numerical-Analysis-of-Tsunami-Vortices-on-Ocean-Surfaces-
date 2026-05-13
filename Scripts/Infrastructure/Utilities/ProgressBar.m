classdef ProgressBar < handle
    % PROGRESSBAR - Terminal progress bar for simulations
    %
    % Purpose:
    %   Displays a visual progress bar in the MATLAB terminal
    %   Updates in-place without cluttering output
    %
    % Usage:
    %   pb = ProgressBar(total_iterations);
    %   for i = 1:total_iterations
    %       pb.update(i);
    %       % ... do work ...
    %   end
    %   pb.finish();

    properties
        total              % Total number of iterations
        current            % Current iteration
        start_time         % Start timestamp
        bar_width          % Width of progress bar in characters
        last_update_time   % Last time bar was updated
        update_interval    % Minimum time between updates (seconds)
        show_eta           % Show estimated time remaining
        show_rate          % Show iteration rate
        prefix             % Text before progress bar
    end

    methods
        function obj = ProgressBar(total, varargin)
            % Constructor
            %
            % Parameters:
            %   total - Total number of iterations
            %   'BarWidth' - Width of bar (default: 40)
            %   'UpdateInterval' - Min seconds between updates (default: 0.1)
            %   'Prefix' - Text before bar (default: 'Progress')

            obj.total = total;
            obj.current = 0;
            obj.start_time = tic;
            obj.last_update_time = 0;

            % Parse options
            p = inputParser;
            addParameter(p, 'BarWidth', 40);
            addParameter(p, 'UpdateInterval', 0.1);
            addParameter(p, 'ShowETA', true);
            addParameter(p, 'ShowRate', true);
            addParameter(p, 'Prefix', 'Progress');
            parse(p, varargin{:});

            obj.bar_width = p.Results.BarWidth;
            obj.update_interval = p.Results.UpdateInterval;
            obj.show_eta = p.Results.ShowETA;
            obj.show_rate = p.Results.ShowRate;
            obj.prefix = p.Results.Prefix;

            % Print initial bar
            obj.render();
        end

        function update(obj, current, varargin)
            % Update progress bar
            %
            % Parameters:
            %   current - Current iteration number
            %   'Message' - Optional message to display

            obj.current = current;

            % Throttle updates
            elapsed = toc(obj.start_time);
            if elapsed - obj.last_update_time < obj.update_interval && current < obj.total
                return;
            end

            obj.last_update_time = elapsed;

            % Parse optional message
            p = inputParser;
            addParameter(p, 'Message', '');
            parse(p, varargin{:});
            message = p.Results.Message;

            obj.render(message);
        end

        function finish(obj, varargin)
            % Finish progress bar
            %
            % Parameters:
            %   'Message' - Optional completion message

            obj.current = obj.total;

            p = inputParser;
            addParameter(p, 'Message', 'Complete!');
            parse(p, varargin{:});
            message = p.Results.Message;

            obj.render(message);
            SafeConsoleIO.fprintf('\n');  % Move to next line
        end
    end

    methods (Access = private)
        function render(obj, message)
            % Render progress bar to terminal

            if nargin < 2
                message = '';
            end

            % Calculate progress
            if obj.total > 0
                percent = obj.current / obj.total;
            else
                percent = 0;
            end

            % Calculate timing info
            elapsed = toc(obj.start_time);
            if obj.current > 0
                rate = obj.current / elapsed;  % iterations/second
                eta = (obj.total - obj.current) / max(rate, eps);  % seconds
            else
                rate = 0;
                eta = 0;
            end

            % Build progress bar using ASCII-only characters. MATLAB's UI
            % command stream can fail on mojibake/unicode progress glyphs.
            filled = round(obj.bar_width * percent);
            bar_str = [repmat('#', 1, filled), repmat('-', 1, obj.bar_width - filled)];

            % Build stats string
            stats = '';
            if obj.show_rate && rate > 0
                if rate < 1
                    stats = sprintf('%s | %.2f s/it', stats, 1/rate);
                else
                    stats = sprintf('%s | %.2f it/s', stats, rate);
                end
            end
            if obj.show_eta && eta > 0 && obj.current < obj.total
                stats = sprintf('%s | ETA: %s', stats, obj.format_time(eta));
            end

            % Build full string
            out_str = sprintf('\r%s: [%s] %6.2f%% (%d/%d)%s', ...
                obj.prefix, bar_str, percent*100, obj.current, obj.total, stats);

            if ~isempty(message)
                out_str = sprintf('%s - %s', out_str, message);
            end

            % Print (use \r to overwrite previous line)
            SafeConsoleIO.fprintf('%s', out_str);
        end

        function str = format_time(~, seconds)
            % Format seconds as human-readable time

            if seconds < 60
                str = sprintf('%.0fs', seconds);
            elseif seconds < 3600
                mins = floor(seconds / 60);
                secs = mod(seconds, 60);
                str = sprintf('%dm%02ds', mins, secs);
            else
                hours = floor(seconds / 3600);
                mins = floor(mod(seconds, 3600) / 60);
                str = sprintf('%dh%02dm', hours, mins);
            end
        end
    end
end
