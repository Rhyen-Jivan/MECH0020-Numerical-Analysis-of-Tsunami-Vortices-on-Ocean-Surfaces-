classdef MonitorInterface
    % MonitorInterface - Single entry point for all monitoring
    %
    % Purpose:
    %   Unified interface for live execution monitoring
    %   Called by any solver/mode to show progress
    %   Supports both Standard mode (dark theme terminal) and UI mode

    methods (Static)
        function start(Run_Config, Settings)
            % Initialize monitor for a new run.
            if ~Settings.monitor_enabled
                return;
            end

            persistent monitor_state;
            monitor_state = struct();
            monitor_state.run_config = Run_Config;
            monitor_state.settings = Settings;
            monitor_state.start_time = datetime('now');
            monitor_state.iteration = 0;

            if strcmp(Settings.monitor_theme, 'dark')
                MonitorInterface.print_dark_header(Run_Config);
            else
                MonitorInterface.print_light_header(Run_Config);
            end
        end

        function update(Run_Status)
            % Update monitor with current simulation state.
            persistent monitor_state;
            if isempty(monitor_state) || ~monitor_state.settings.monitor_enabled
                return;
            end

            monitor_state.iteration = monitor_state.iteration + 1;
            if mod(monitor_state.iteration, 10) ~= 0
                return;
            end

            if strcmp(monitor_state.settings.monitor_theme, 'dark')
                MonitorInterface.print_dark_update(Run_Status, monitor_state);
            else
                MonitorInterface.print_light_update(Run_Status, monitor_state);
            end
        end

        function stop(Run_Summary)
            % Finalize monitor and display summary.
            persistent monitor_state;
            if isempty(monitor_state) || ~monitor_state.settings.monitor_enabled
                return;
            end

            if strcmp(monitor_state.settings.monitor_theme, 'dark')
                MonitorInterface.print_dark_footer(Run_Summary, monitor_state);
            else
                MonitorInterface.print_light_footer(Run_Summary, monitor_state);
            end

            monitor_state = [];
        end
    end

    methods (Static, Access = private)
        function print_dark_header(Run_Config)
            ColorPrintf.monitor_header(Run_Config.method, Run_Config.mode, Run_Config.ic_type);
        end

        function print_light_header(Run_Config)
            border = repmat('=', 1, 55);
            divider = repmat('-', 1, 55);
            SafeConsoleIO.fprintf('\n');
            SafeConsoleIO.fprintf('%s\n', border);
            SafeConsoleIO.fprintf('         TSUNAMI VORTEX SIMULATION MONITOR            \n');
            SafeConsoleIO.fprintf('%s\n', border);
            SafeConsoleIO.fprintf('Method: %s  |  Mode: %s  |  IC: %s\n', ...
                Run_Config.method, Run_Config.mode, Run_Config.ic_type);
            SafeConsoleIO.fprintf('%s\n', divider);
        end

        function print_dark_update(Run_Status, ~)
            ColorPrintf.monitor_update(Run_Status.step, Run_Status.time, ...
                Run_Status.dt, Run_Status.CFL, Run_Status.max_omega, 0);
        end

        function print_light_update(Run_Status, ~)
            SafeConsoleIO.fprintf('[Step %d]  t=%.4f  dt=%.2e  CFL=%.3f  |w|max=%.2e\n', ...
                Run_Status.step, Run_Status.time, Run_Status.dt, ...
                Run_Status.CFL, Run_Status.max_omega);
        end

        function print_dark_footer(Run_Summary, ~)
            ColorPrintf.monitor_footer(Run_Summary.total_time);
        end

        function print_light_footer(Run_Summary, ~)
            SafeConsoleIO.fprintf('\n---------------------------------------------------\n');
            SafeConsoleIO.fprintf('Simulation completed  |  Total time: %.2fs\n\n', Run_Summary.total_time);
        end
    end
end
