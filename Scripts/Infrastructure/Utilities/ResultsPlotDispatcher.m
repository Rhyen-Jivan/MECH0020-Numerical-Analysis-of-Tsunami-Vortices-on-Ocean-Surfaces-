classdef ResultsPlotDispatcher
% ResultsPlotDispatcher - Canonical results plotting dispatcher and style policy.
%
% This class is the single source of truth for:
%   1) Results payload dispatching (snapshot/diagnostics metadata)
%   2) Dark-theme plotting defaults and axis formatting
%   3) Legend formatting policy used across UI result/monitor tiles

    methods (Static)

        function packet = dispatch(analysis, params, run_cfg, meta)
            if nargin < 1 || ~isstruct(analysis)
                analysis = struct();
            end
            if nargin < 2 || ~isstruct(params)
                params = struct();
            end
            if nargin < 3 || ~isstruct(run_cfg)
                run_cfg = struct();
            end
            if nargin < 4 || ~isstruct(meta)
                meta = struct();
            end

            packet = struct();
            packet.format = ResultsPlotDispatcher.defaults();
            packet.plots = ResultsPlotDispatcher.build_plot_specs(analysis, params);
            packet.metadata = struct( ...
                'lines', {ResultsPlotDispatcher.build_metadata_lines(meta, params, run_cfg, analysis)});
            packet.dashboard = ResultsPlotDispatcher.build_dashboard(meta, params, run_cfg, analysis);
        end

        function spec = get_plot(packet, plot_id)
            spec = struct();
            if nargin < 2 || ~isstruct(packet) || ~isfield(packet, 'plots') || isempty(packet.plots)
                return;
            end
            idx = find(strcmp({packet.plots.id}, char(string(plot_id))), 1);
            if ~isempty(idx)
                spec = packet.plots(idx);
            end
        end

        function opts = defaults()
            opts = struct();
            opts.font_sizes = {12, 12, 14};
            opts.axis_line_width = 1.1;
            opts.grid_alpha = 0.30;
            opts.line_width = 1.6;
            opts.secondary_line_width = 1.4;
            opts.legend_font_size = 10;
            opts.legend_orientation = 'vertical';
            opts.legend_num_columns = 1;
            opts.legend_num_rows = [];
            opts.legend_auto_location = true;
            opts.legend_location_override = 'best';
            opts.legend_box = 'off';
            opts.snapshot_colormap = 'turbo';
        end

        function colors = default_colors()
            colors = struct( ...
                'bg', [0.11, 0.12, 0.15], ...
                'fg', [0.90, 0.92, 0.95], ...
                'grid', [0.45, 0.48, 0.52], ...
                'primary', [0.30, 0.75, 0.95], ...
                'secondary', [0.95, 0.70, 0.25], ...
                'tertiary', [0.55, 0.70, 1.00]);
        end

        function colors = default_light_colors()
            colors = struct( ...
                'bg', [1.00, 1.00, 1.00], ...
                'fg', [0.08, 0.10, 0.13], ...
                'grid', [0.72, 0.75, 0.80], ...
                'primary', [0.10, 0.34, 0.78], ...
                'secondary', [0.87, 0.43, 0.10], ...
                'tertiary', [0.22, 0.60, 0.33]);
        end

        function [x, y] = resolve_snapshot_axes(analysis, params)
            [x, y] = ResultsPlotDispatcher.resolve_snapshot_axes_impl(analysis, params);
        end

        function diag_data = resolve_diagnostics_payload(analysis)
            if nargin < 1 || ~isstruct(analysis)
                analysis = struct();
            end

            primary = ResultsPlotDispatcher.pick_numeric_series(analysis, 'kinetic_energy');
            secondary = ResultsPlotDispatcher.pick_numeric_series(analysis, 'enstrophy');

            primary_time = ResultsPlotDispatcher.resolve_series_time_vector(analysis, numel(primary));
            secondary_time = ResultsPlotDispatcher.resolve_series_time_vector(analysis, numel(secondary));

            shared_time = primary_time;
            if isempty(shared_time)
                shared_time = secondary_time;
            end

            diag_data = struct( ...
                'time', shared_time, ...
                'primary', primary, ...
                'secondary', secondary, ...
                'primary_time', primary_time, ...
                'secondary_time', secondary_time);
        end

        function apply_axes(ax, labels, opts, colors)
            if nargin < 1 || isempty(ax) || ~isgraphics(ax)
                error('ResultsPlotDispatcher:InvalidAxes', ...
                    'apply_axes requires a valid axes handle.');
            end
            if nargin < 2 || ~isstruct(labels)
                labels = struct();
            end
            if nargin < 3 || ~isstruct(opts)
                opts = ResultsPlotDispatcher.defaults();
            end
            if nargin < 4 || ~isstruct(colors)
                colors = ResultsPlotDispatcher.default_colors();
            end

            x_label = ResultsPlotDispatcher.get_label(labels, 'xlabel', '');
            y_label = ResultsPlotDispatcher.get_label(labels, 'ylabel', '');
            title_label = ResultsPlotDispatcher.get_label(labels, 'title', '');
            x_interp = ResultsPlotDispatcher.get_label(labels, 'x_interpreter', 'latex');
            y_interp = ResultsPlotDispatcher.get_label(labels, 'y_interpreter', 'latex');
            title_interp = ResultsPlotDispatcher.get_label(labels, 'title_interpreter', 'latex');

            font_sizes = ResultsPlotDispatcher.normalize_font_sizes(opts.font_sizes);

            if isprop(ax, 'LineWidth')
                ax.LineWidth = opts.axis_line_width;
            end
            if isprop(ax, 'GridLineWidth')
                ax.GridLineWidth = 1.0;
            end
            if isprop(ax, 'Color')
                ax.Color = colors.bg;
            end
            if isprop(ax, 'XColor')
                ax.XColor = colors.fg;
            end
            if isprop(ax, 'YColor')
                ax.YColor = colors.fg;
            end
            if isprop(ax, 'ZColor')
                ax.ZColor = colors.fg;
            end
            if isprop(ax, 'GridColor')
                ax.GridColor = colors.grid;
            end
            if isprop(ax, 'GridAlpha')
                ax.GridAlpha = opts.grid_alpha;
            end
            if isprop(ax, 'TickLabelInterpreter')
                ax.TickLabelInterpreter = 'latex';
            end

            if isprop(ax, 'XAxis')
                try
                    ax.XAxis.FontSize = font_sizes{1};
                catch
                end
            end
            if isprop(ax, 'YAxis')
                try
                    ax.YAxis.FontSize = font_sizes{2};
                catch
                end
            end
            if isprop(ax, 'Title') && isprop(ax.Title, 'FontSize')
                ax.Title.FontSize = font_sizes{3};
            end

            grid(ax, 'on');
            if isprop(ax, 'XMinorGrid')
                ax.XMinorGrid = 'off';
            end
            if isprop(ax, 'YMinorGrid')
                ax.YMinorGrid = 'off';
            end
            if isprop(ax, 'ZMinorGrid')
                ax.ZMinorGrid = 'off';
            end
            box(ax, 'on');

            title(ax, title_label, 'Color', colors.fg, 'Interpreter', title_interp);
            xlabel(ax, x_label, 'Color', colors.fg, 'Interpreter', x_interp);
            ylabel(ax, y_label, 'Color', colors.fg, 'Interpreter', y_interp);
        end

        function legend_handle = apply_legend(ax, entries, opts, colors)
            if nargin < 1 || isempty(ax) || ~isgraphics(ax)
                error('ResultsPlotDispatcher:InvalidAxes', ...
                    'apply_legend requires a valid axes handle.');
            end
            if nargin < 2 || isempty(entries)
                legend_handle = gobjects(0);
                return;
            end
            if nargin < 3 || ~isstruct(opts)
                opts = ResultsPlotDispatcher.defaults();
            end
            if nargin < 4 || ~isstruct(colors)
                colors = ResultsPlotDispatcher.default_colors();
            end

            if ischar(entries) || isstring(entries)
                entries = cellstr(string(entries));
            end

            legend_handle = legend(ax, entries, 'Interpreter', 'latex');
            legend_handle.FontSize = opts.legend_font_size;
            legend_handle.Box = opts.legend_box;
            if isprop(legend_handle, 'Color')
                legend_handle.Color = colors.bg;
            end
            if isprop(legend_handle, 'TextColor')
                legend_handle.TextColor = colors.fg;
            end
            if isprop(legend_handle, 'EdgeColor')
                if strcmpi(opts.legend_box, 'off')
                    legend_handle.EdgeColor = colors.bg;
                else
                    legend_handle.EdgeColor = colors.grid;
                end
            end

            if strcmpi(opts.legend_orientation, 'horizontal')
                legend_handle.NumColumns = max(1, opts.legend_num_columns);
            else
                if isempty(opts.legend_num_rows)
                    legend_handle.NumColumns = 1;
                else
                    legend_handle.NumColumns = ceil(numel(entries) / max(1, opts.legend_num_rows));
                end
            end

            if opts.legend_auto_location
                legend_handle.Location = 'best';
            else
                legend_handle.Location = char(string(opts.legend_location_override));
            end
        end

        function apply_dark_theme(target, colors)
            if nargin < 1 || isempty(target)
                return;
            end
            if nargin < 2 || ~isstruct(colors)
                colors = ResultsPlotDispatcher.default_colors();
            end

            if isgraphics(target, 'figure') && isprop(target, 'Color')
                try
                    target.Color = colors.bg;
                catch
                end
            end

            if isgraphics(target, 'axes')
                ResultsPlotDispatcher.apply_axes(target, ResultsPlotDispatcher.extract_existing_labels(target), ResultsPlotDispatcher.defaults(), colors);
                return;
            end

            ax_list = findall(target, 'Type', 'axes');
            for i = 1:numel(ax_list)
                try
                    ResultsPlotDispatcher.apply_axes(ax_list(i), ResultsPlotDispatcher.extract_existing_labels(ax_list(i)), ResultsPlotDispatcher.defaults(), colors);
                catch
                end
            end

            cb_list = findall(target, 'Type', 'ColorBar');
            for i = 1:numel(cb_list)
                try
                    if isprop(cb_list(i), 'Color')
                        cb_list(i).Color = colors.fg;
                    end
                catch
                end
            end

            legend_list = findall(target, 'Type', 'Legend');
            for i = 1:numel(legend_list)
                try
                    if isprop(legend_list(i), 'TextColor')
                        legend_list(i).TextColor = colors.fg;
                    end
                    if isprop(legend_list(i), 'Color')
                        legend_list(i).Color = colors.bg;
                    end
                    if isprop(legend_list(i), 'EdgeColor')
                        legend_list(i).EdgeColor = colors.grid;
                    end
                catch
                end
            end
        end

        function apply_light_theme(target, colors)
            if nargin < 1 || isempty(target)
                return;
            end
            if nargin < 2 || ~isstruct(colors)
                colors = ResultsPlotDispatcher.default_light_colors();
            end
            ResultsPlotDispatcher.apply_dark_theme(target, colors);
        end

        function rendered = render_deterministic_streamlines(ax, x_vec, y_vec, u, v, line_color, options)
            if nargin < 6 || isempty(line_color)
                line_color = [0.08, 0.10, 0.13];
            end
            if nargin < 7 || ~isstruct(options)
                options = struct();
            end
            rendered = false;
            if isempty(ax) || ~isgraphics(ax, 'axes') || isempty(u) || isempty(v)
                return;
            end

            opts = struct( ...
                'seed_rows', 14, ...
                'seed_cols', 14, ...
                'line_width', 1.0, ...
                'speed_floor_fraction', 0.02, ...
                'min_stream_vertices', 6, ...
                'fallback_skip', NaN, ...
                'fallback_scale', 1.4);
            opt_fields = fieldnames(options);
            for i = 1:numel(opt_fields)
                opts.(opt_fields{i}) = options.(opt_fields{i});
            end

            x_vec = double(x_vec(:)).';
            y_vec = double(y_vec(:));
            u = double(u);
            v = double(v);
            [Xg, Yg] = meshgrid(x_vec, y_vec);
            if ~isequal(size(Xg), size(u)) || ~isequal(size(Yg), size(v))
                return;
            end

            finite_mask = isfinite(u) & isfinite(v);
            u(~finite_mask) = 0;
            v(~finite_mask) = 0;
            speed = hypot(u, v);
            finite_speed = speed(isfinite(speed));
            if isempty(finite_speed)
                finite_speed = 0;
            end
            max_speed = max(finite_speed, [], 'omitnan');

            x_margin = 0.08 * max(abs(x_vec(end) - x_vec(1)), eps);
            y_margin = 0.08 * max(abs(y_vec(end) - y_vec(1)), eps);
            x_seed = linspace(x_vec(1) + x_margin, x_vec(end) - x_margin, ...
                max(4, min(numel(x_vec), round(double(opts.seed_cols)))));
            y_seed = linspace(y_vec(1) + y_margin, y_vec(end) - y_margin, ...
                max(4, min(numel(y_vec), round(double(opts.seed_rows)))));
            [seed_x_grid, seed_y_grid] = meshgrid(x_seed, y_seed);
            seed_valid = interp2(Xg, Yg, double(finite_mask), seed_x_grid, seed_y_grid, 'linear', 0) > 0.5;
            if isfinite(max_speed) && max_speed > 0
                speed_seed = interp2(Xg, Yg, speed, seed_x_grid, seed_y_grid, 'linear', 0);
                seed_valid = seed_valid & speed_seed > max(opts.speed_floor_fraction * max_speed, 1.0e-12);
            end
            seed_x = seed_x_grid(seed_valid);
            seed_y = seed_y_grid(seed_valid);

            hold_state = ishold(ax);
            hold(ax, 'on');
            cleanup_hold = onCleanup(@() local_restore_hold(ax, hold_state)); %#ok<NASGU>
            if ~isempty(seed_x)
                streams = ResultsPlotDispatcher.generate_streamline_segments(Xg, Yg, u, v, seed_x, seed_y, opts.min_stream_vertices);
                if ~isempty(streams)
                    try
                        h_stream = streamline(ax, streams);
                        if ~isempty(h_stream)
                            set(h_stream, 'Color', line_color, 'LineWidth', opts.line_width);
                            rendered = true;
                        end
                    catch
                        rendered = false;
                    end
                end
            end

            if ~rendered
                fallback_skip = round(double(opts.fallback_skip));
                if ~isfinite(fallback_skip) || fallback_skip < 1
                    fallback_skip = max(1, round(min(numel(x_vec), numel(y_vec)) / 24));
                end
                quiver(ax, Xg(1:fallback_skip:end, 1:fallback_skip:end), ...
                    Yg(1:fallback_skip:end, 1:fallback_skip:end), ...
                    u(1:fallback_skip:end, 1:fallback_skip:end), ...
                    v(1:fallback_skip:end, 1:fallback_skip:end), ...
                    opts.fallback_scale, 'Color', line_color, 'LineWidth', 0.9);
            end
        end

        function outputs = save_figure_bundle(fig_handle, output_path, settings)
            if nargin < 1 || isempty(fig_handle) || ~isgraphics(fig_handle, 'figure')
                error('ResultsPlotDispatcher:InvalidFigure', ...
                    'save_figure_bundle requires a valid figure handle.');
            end
            if nargin < 2 || ~(ischar(output_path) || isstring(output_path))
                error('ResultsPlotDispatcher:InvalidOutputPath', ...
                    'save_figure_bundle requires a target output path.');
            end
            if nargin < 3 || ~isstruct(settings)
                settings = struct();
            end

            [folder_path, file_stem, ~] = fileparts(char(string(output_path)));
            if isempty(folder_path)
                folder_path = pwd;
            end
            if ~exist(folder_path, 'dir')
                mkdir(folder_path);
            end

            dpi = ResultsPlotDispatcher.pick_numeric(settings, 'figure_dpi', 300);
            if ~isfinite(dpi) || dpi <= 0
                dpi = 300;
            end

            save_png = ResultsPlotDispatcher.resolve_save_flag(settings, ...
                {'figure_save_png', 'save_png'}, ...
                ResultsPlotDispatcher.infer_png_from_format(settings));
            save_pdf = ResultsPlotDispatcher.resolve_save_flag(settings, ...
                {'figure_save_pdf', 'save_pdf'}, ...
                ResultsPlotDispatcher.infer_pdf_from_format(settings));
            save_fig = ResultsPlotDispatcher.resolve_save_flag(settings, ...
                {'figure_save_fig', 'save_fig'}, false);
            if ~save_png && ~save_pdf && ~save_fig
                save_png = true;
            end

            export_fig = ResultsPlotDispatcher.clone_figure(fig_handle);
            using_clone = ~isempty(export_fig) && isgraphics(export_fig, 'figure');
            if ~using_clone
                export_fig = fig_handle;
            end
            cleanup_fig = []; %#ok<NASGU>
            if using_clone
                cleanup_fig = onCleanup(@() ResultsPlotDispatcher.safe_close(export_fig));
            end

            try
                export_fig.Visible = 'off';
            catch
            end
            try
                export_fig.Color = [1 1 1];
            catch
            end
            try
                export_fig.InvertHardcopy = 'off';
            catch
            end
            ResultsPlotDispatcher.apply_light_theme(export_fig, ResultsPlotDispatcher.default_light_colors());
            drawnow;

            outputs = struct('png_path', '', 'pdf_path', '', 'fig_path', '');
            base_path = fullfile(folder_path, file_stem);
            if save_png
                outputs.png_path = [base_path '.png'];
                outputs.png_path = ResultsPlotDispatcher.save_png_with_fallback(export_fig, outputs.png_path, dpi);
            end
            if save_pdf
                outputs.pdf_path = [base_path '.pdf'];
                outputs.pdf_path = ResultsPlotDispatcher.save_pdf_with_fallback(export_fig, outputs.pdf_path, dpi);
            end
            if save_fig
                outputs.fig_path = [base_path '.fig'];
                try
                    restore_visible = '';
                    restore_units = '';
                    restore_position = [];
                    restore_window_state = '';
                    restore_handle_visibility = '';
                    restore_menu_bar = '';
                    restore_tool_bar = '';
                    restore_dock_controls = '';
                    try
                        restore_visible = char(string(export_fig.Visible));
                    catch
                    end
                    try
                        restore_handle_visibility = char(string(export_fig.HandleVisibility));
                        export_fig.HandleVisibility = 'on';
                    catch
                    end
                    try
                        restore_units = char(string(export_fig.Units));
                        export_fig.Units = 'pixels';
                    catch
                    end
                    try
                        restore_position = export_fig.Position;
                    catch
                    end
                    try
                        restore_window_state = char(string(export_fig.WindowState));
                    catch
                    end
                    try
                        restore_menu_bar = char(string(export_fig.MenuBar));
                    catch
                    end
                    try
                        restore_tool_bar = char(string(export_fig.ToolBar));
                    catch
                    end
                    try
                        restore_dock_controls = char(string(export_fig.DockControls));
                    catch
                    end
                    try
                        export_fig.Visible = 'on';
                    catch
                    end
                    % Persist user-facing FIGs with standard MATLAB chrome so
                    % they reopen with toolbar/menu access instead of inheriting
                    % stripped export-shell settings.
                    try
                        export_fig.MenuBar = 'figure';
                    catch
                    end
                    try
                        export_fig.ToolBar = 'figure';
                    catch
                    end
                    try
                        export_fig.DockControls = 'on';
                    catch
                    end
                    try
                        export_fig.WindowState = 'minimized';
                    catch
                    end
                    try
                        export_fig.Position = ResultsPlotDispatcher.sanitize_saved_fig_position(restore_position);
                    catch
                    end
                    drawnow limitrate;
                    savefig(export_fig, outputs.fig_path);
                    try
                        if ~isempty(restore_dock_controls)
                            export_fig.DockControls = restore_dock_controls;
                        end
                    catch
                    end
                    try
                        if ~isempty(restore_tool_bar)
                            export_fig.ToolBar = restore_tool_bar;
                        end
                    catch
                    end
                    try
                        if ~isempty(restore_menu_bar)
                            export_fig.MenuBar = restore_menu_bar;
                        end
                    catch
                    end
                    try
                        if ~isempty(restore_window_state)
                            export_fig.WindowState = restore_window_state;
                        end
                    catch
                    end
                    try
                        if ~isempty(restore_position)
                            export_fig.Position = restore_position;
                        end
                    catch
                    end
                    try
                        if ~isempty(restore_units)
                            export_fig.Units = restore_units;
                        end
                    catch
                    end
                    try
                        if ~isempty(restore_visible)
                            export_fig.Visible = restore_visible;
                        end
                    catch
                    end
                    try
                        if ~isempty(restore_handle_visibility)
                            export_fig.HandleVisibility = restore_handle_visibility;
                        end
                    catch
                    end
                catch ME
                    try
                        if exist('restore_dock_controls', 'var') && ~isempty(restore_dock_controls)
                            export_fig.DockControls = restore_dock_controls;
                        end
                    catch
                    end
                    try
                        if exist('restore_tool_bar', 'var') && ~isempty(restore_tool_bar)
                            export_fig.ToolBar = restore_tool_bar;
                        end
                    catch
                    end
                    try
                        if exist('restore_menu_bar', 'var') && ~isempty(restore_menu_bar)
                            export_fig.MenuBar = restore_menu_bar;
                        end
                    catch
                    end
                    try
                        if exist('restore_window_state', 'var') && ~isempty(restore_window_state)
                            export_fig.WindowState = restore_window_state;
                        end
                    catch
                    end
                    try
                        if exist('restore_position', 'var') && ~isempty(restore_position)
                            export_fig.Position = restore_position;
                        end
                    catch
                    end
                    try
                        if exist('restore_units', 'var') && ~isempty(restore_units)
                            export_fig.Units = restore_units;
                        end
                    catch
                    end
                    try
                        if exist('restore_visible', 'var') && ~isempty(restore_visible)
                            export_fig.Visible = restore_visible;
                        end
                    catch
                    end
                    try
                        if exist('restore_handle_visibility', 'var') && ~isempty(restore_handle_visibility)
                            export_fig.HandleVisibility = restore_handle_visibility;
                        end
                    catch
                    end
                    warning('ResultsPlotDispatcher:SaveFigFailed', ...
                        'FIG export failed for %s: %s', outputs.fig_path, ME.message);
                    outputs.fig_path = '';
                end
            end
        end

        function output_path = save_png_with_fallback(export_fig, output_path, dpi)
            try
                drawnow nocallbacks;
            catch
            end
            try
                exportgraphics(export_fig, output_path, ...
                    'Resolution', round(dpi), ...
                    'BackgroundColor', 'white');
            catch ME1
                try
                    drawnow limitrate;
                    print(export_fig, output_path, '-dpng', sprintf('-r%d', round(dpi)));
                catch ME2
                    warning('ResultsPlotDispatcher:SavePNGFailed', ...
                        'PNG export failed for %s: %s | fallback: %s', output_path, ME1.message, ME2.message);
                    output_path = '';
                end
            end
        end

        function output_path = save_pdf_with_fallback(export_fig, output_path, dpi)
            try
                drawnow nocallbacks;
            catch
            end
            try
                exportgraphics(export_fig, output_path, ...
                    'ContentType', 'vector', ...
                    'BackgroundColor', 'white');
            catch ME1
                try
                    drawnow limitrate;
                    print(export_fig, output_path, '-dpdf', sprintf('-r%d', round(dpi)));
                catch ME2
                    warning('ResultsPlotDispatcher:SavePDFFailed', ...
                        'PDF export failed for %s: %s | fallback: %s', output_path, ME1.message, ME2.message);
                    output_path = '';
                end
            end
        end

        function output_path = primary_output_path(outputs, fallback_path)
            output_path = fallback_path;
            if ~isstruct(outputs)
                return;
            end
            if isfield(outputs, 'png_path') && ~isempty(outputs.png_path)
                output_path = outputs.png_path;
                return;
            end
            if isfield(outputs, 'pdf_path') && ~isempty(outputs.pdf_path)
                output_path = outputs.pdf_path;
                return;
            end
            if isfield(outputs, 'fig_path') && ~isempty(outputs.fig_path)
                output_path = outputs.fig_path;
            end
        end

        function title_text = compose_export_title(prefix, params, run_cfg)
            if nargin < 1 || isempty(prefix)
                prefix = 'Results';
            end
            if nargin < 2 || ~isstruct(params)
                params = struct();
            end
            if nargin < 3 || ~isstruct(run_cfg)
                run_cfg = struct();
            end

            nx = ResultsPlotDispatcher.pick_numeric(params, 'Nx', NaN);
            ny = ResultsPlotDispatcher.pick_numeric(params, 'Ny', NaN);
            nu = ResultsPlotDispatcher.pick_numeric(params, 'nu', NaN);

            ic_token = '';
            if isfield(run_cfg, 'ic_type') && ~isempty(run_cfg.ic_type)
                ic_token = ResultsPlotDispatcher.coerce_text(run_cfg.ic_type, '');
            elseif isfield(params, 'ic_type') && ~isempty(params.ic_type)
                ic_token = ResultsPlotDispatcher.coerce_text(params.ic_type, '');
            end

            bc_token = '';
            if isfield(params, 'boundary_condition_case') && ~isempty(params.boundary_condition_case)
                bc_token = ResultsPlotDispatcher.coerce_text(params.boundary_condition_case, '');
            elseif isfield(params, 'bc_case') && ~isempty(params.bc_case)
                bc_token = ResultsPlotDispatcher.coerce_text(params.bc_case, '');
            elseif isfield(run_cfg, 'bc_case') && ~isempty(run_cfg.bc_case)
                bc_token = ResultsPlotDispatcher.coerce_text(run_cfg.bc_case, '');
            end

            prefix_txt = ResultsPlotDispatcher.coerce_text(prefix, 'Results');
            ic_txt = ResultsPlotDispatcher.humanize_token(ic_token);
            bc_txt = ResultsPlotDispatcher.humanize_token(bc_token);
            if isempty(ic_txt)
                ic_txt = 'Unspecified';
            end
            if isempty(bc_txt)
                bc_txt = 'Unspecified';
            end

            if isfinite(nx) && isfinite(ny) && isfinite(nu)
                title_text = sprintf('%s | N_x \\times N_y = %d \\times %d, \\nu = %.3g, IC: %s, BC: %s', ...
                    prefix_txt, round(nx), round(ny), nu, ic_txt, bc_txt);
            else
                title_text = sprintf('%s | IC: %s, BC: %s', prefix_txt, ic_txt, bc_txt);
            end
        end

        function apply_tiled_annotations(tile_handle, title_text, xlabel_text, ylabel_text, colors)
            if nargin < 1 || isempty(tile_handle)
                return;
            end
            if nargin < 5 || ~isstruct(colors)
                colors = ResultsPlotDispatcher.default_light_colors();
            end
            if nargin >= 2 && ~isempty(title_text)
                title(tile_handle, ResultsPlotDispatcher.coerce_text(title_text, ''), ...
                    'Color', colors.fg, 'Interpreter', 'tex', ...
                    'FontWeight', 'bold');
            end
            if nargin >= 3 && ~isempty(xlabel_text)
                xlabel(tile_handle, ResultsPlotDispatcher.coerce_text(xlabel_text, ''), ...
                    'Color', colors.fg, 'Interpreter', 'none');
            end
            if nargin >= 4 && ~isempty(ylabel_text)
                ylabel(tile_handle, ResultsPlotDispatcher.coerce_text(ylabel_text, ''), ...
                    'Color', colors.fg, 'Interpreter', 'none');
            end
        end
    end

    methods (Static, Access = private)

        function labels = extract_existing_labels(ax)
            labels = struct('xlabel', '', 'ylabel', '', 'title', '', ...
                'x_interpreter', 'tex', 'y_interpreter', 'tex', 'title_interpreter', 'tex');
            if nargin < 1 || isempty(ax) || ~isgraphics(ax, 'axes')
                return;
            end
            try
                labels.xlabel = char(string(ax.XLabel.String));
            catch
            end
            try
                labels.ylabel = char(string(ax.YLabel.String));
            catch
            end
            try
                labels.title = char(string(ax.Title.String));
            catch
            end
            try
                labels.x_interpreter = char(string(ax.XLabel.Interpreter));
            catch
            end
            try
                labels.y_interpreter = char(string(ax.YLabel.Interpreter));
            catch
            end
            try
                labels.title_interpreter = char(string(ax.Title.Interpreter));
            catch
            end
        end

        function token = humanize_token(raw_token)
            token = strtrim(ResultsPlotDispatcher.coerce_text(raw_token, ''));
            if isempty(token)
                return;
            end
            token = regexprep(token, '[_-]+', ' ');
            token = regexprep(token, '\s+', ' ');
            parts = strsplit(token, ' ');
            for k = 1:numel(parts)
                part = ResultsPlotDispatcher.coerce_text(parts{k}, '');
                if ~isempty(part)
                    parts{k} = [upper(part(1)), lower(part(2:end))];
                end
            end
            token = strjoin(parts, ' ');
        end

        function plots = build_plot_specs(analysis, params)
            plots = struct('id', {}, 'type', {}, 'labels', {}, 'legend', {}, 'data', {}, 'slider', {});
            plot_labels = resolve_analysis_plot_labels(analysis);

            [x, y] = ResultsPlotDispatcher.resolve_snapshot_axes_impl(analysis, params);
            snapshot_times = [];
            if isfield(analysis, 'snapshot_times_requested') && ~isempty(analysis.snapshot_times_requested)
                snapshot_times = analysis.snapshot_times_requested(:)';
            elseif isfield(analysis, 'snapshot_times') && ~isempty(analysis.snapshot_times)
                snapshot_times = analysis.snapshot_times(:)';
            end
            omega_snaps = [];
            if isfield(analysis, 'omega_snaps') && ~isempty(analysis.omega_snaps)
                omega_snaps = analysis.omega_snaps;
            end

            plots(end + 1) = struct( ... %#ok<AGROW>
                'id', 'snapshot', ...
                'type', 'heatmap', ...
                'labels', struct( ...
                    'title', plot_labels.primary_title, ...
                    'xlabel', 'x', ...
                    'ylabel', 'y', ...
                    'primary_name', plot_labels.primary_name, ...
                    'secondary_name', plot_labels.secondary_name, ...
                    'snapshot_title_base', plot_labels.snapshot_title_base, ...
                    'contour_title_base', plot_labels.contour_title_base, ...
                    'vector_title_base', plot_labels.vector_title_base, ...
                    'streamline_title_base', plot_labels.streamline_title_base, ...
                    'streamfunction_title_base', plot_labels.streamfunction_title_base, ...
                    'speed_title_base', plot_labels.speed_title_base, ...
                    'title_interpreter', 'tex', ...
                    'x_interpreter', 'none', ...
                    'y_interpreter', 'none'), ...
                'legend', {{}}, ...
                'data', struct('x', x, 'y', y, 'z', omega_snaps, 'times', snapshot_times), ...
                'slider', true);

            diag_data = ResultsPlotDispatcher.resolve_diagnostics_payload(analysis);
            time_vec = diag_data.time;
            kinetic = diag_data.primary;
            enstrophy = diag_data.secondary;

            plots(end + 1) = struct( ... %#ok<AGROW>
                'id', 'diagnostics', ...
                'type', 'dual_series', ...
                'labels', struct( ...
                    'title', plot_labels.diagnostics_title, ...
                    'xlabel', 'Time (s)', ...
                    'ylabel', plot_labels.diagnostics_primary, ...
                    'ylabel_right', plot_labels.diagnostics_secondary), ...
                'legend', {{plot_labels.diagnostics_primary, plot_labels.diagnostics_secondary}}, ...
                'data', diag_data, ...
                'slider', false);

            max_omega_hist = [];
            if isfield(analysis, 'max_omega_history') && ~isempty(analysis.max_omega_history)
                max_omega_hist = analysis.max_omega_history(:)';
            elseif isfield(analysis, 'peak_vorticity') && isscalar(analysis.peak_vorticity) && ~isempty(time_vec)
                max_omega_hist = analysis.peak_vorticity * ones(size(time_vec));
            end

            plots(end + 1) = struct( ... %#ok<AGROW>
                'id', 'peak_vorticity', ...
                'type', 'series', ...
                'labels', struct( ...
                    'title', plot_labels.peak_title, ...
                    'xlabel', 'Time (s)', ...
                    'ylabel', plot_labels.peak_ylabel, ...
                    'y_interpreter', 'tex'), ...
                'legend', {{}}, ...
                'data', struct('time', time_vec, 'primary', max_omega_hist), ...
                'slider', false);
        end

        function [x, y] = resolve_snapshot_axes_impl(analysis, params)
            if isfield(analysis, 'omega_snaps') && ~isempty(analysis.omega_snaps)
                ny = size(analysis.omega_snaps, 1);
                nx = size(analysis.omega_snaps, 2);
            else
                nx = 64;
                ny = 64;
            end
            if isfield(params, 'Nx') && isnumeric(params.Nx) && params.Nx > 1
                nx = round(params.Nx);
            end
            if isfield(params, 'Ny') && isnumeric(params.Ny) && params.Ny > 1
                ny = round(params.Ny);
            end

            x = ResultsPlotDispatcher.resolve_axis_vector(analysis, params, ...
                {'x', 'x_vec', 'x_coords', 'snapshot_x', 'x_nodes'}, nx, 'Lx');
            y = ResultsPlotDispatcher.resolve_axis_vector(analysis, params, ...
                {'y', 'y_vec', 'y_coords', 'snapshot_y', 'y_nodes'}, ny, 'Ly');
        end

        function axis_vec = resolve_axis_vector(analysis, params, field_candidates, count, extent_field)
            axis_vec = ResultsPlotDispatcher.pick_axis_vector_from_struct(analysis, field_candidates, count);
            if isempty(axis_vec)
                axis_vec = ResultsPlotDispatcher.pick_axis_vector_from_struct(params, field_candidates, count);
            end
            if isempty(axis_vec)
                extent_value = ResultsPlotDispatcher.pick_numeric_extent(analysis, params, extent_field);
                if isfinite(extent_value) && extent_value > 0
                    axis_vec = linspace(-extent_value / 2, extent_value / 2, count);
                else
                    axis_vec = 1:count;
                end
            end
            axis_vec = double(axis_vec(:)).';
        end

        function axis_vec = pick_axis_vector_from_struct(source, field_candidates, count)
            axis_vec = [];
            if ~isstruct(source)
                return;
            end
            for i = 1:numel(field_candidates)
                field_name = field_candidates{i};
                if ~isfield(source, field_name) || isempty(source.(field_name))
                    continue;
                end
                candidate = source.(field_name);
                if isnumeric(candidate) && isvector(candidate) && numel(candidate) == count
                    axis_vec = candidate;
                    return;
                end
                if isnumeric(candidate) && ismatrix(candidate)
                    if size(candidate, 1) == 1 && size(candidate, 2) == count
                        axis_vec = candidate;
                        return;
                    end
                    if size(candidate, 2) == 1 && size(candidate, 1) == count
                        axis_vec = candidate(:).';
                        return;
                    end
                end
            end
        end

        function series = pick_numeric_series(source, field_name)
            series = [];
            if ~isstruct(source) || ~isfield(source, field_name) || isempty(source.(field_name))
                return;
            end
            candidate = source.(field_name);
            if ~isnumeric(candidate)
                return;
            end
            series = double(candidate(:)).';
        end

        function time_vec = resolve_series_time_vector(analysis, count)
            time_vec = [];
            if count <= 0
                return;
            end

            candidates = { ...
                'time_vec', ...
                'snapshot_times_requested', ...
                'snapshot_times', ...
                'snapshot_times_actual'};

            for i = 1:numel(candidates)
                field_name = candidates{i};
                if ~isstruct(analysis) || ~isfield(analysis, field_name) || isempty(analysis.(field_name))
                    continue;
                end
                candidate = analysis.(field_name);
                if ~isnumeric(candidate)
                    continue;
                end
                candidate = double(candidate(:)).';
                if numel(candidate) == count
                    time_vec = candidate;
                    return;
                end
            end

            time_vec = double(1:count);
        end

        function extent_value = pick_numeric_extent(analysis, params, field_name)
            extent_value = NaN;
            if isstruct(analysis) && isfield(analysis, field_name) && isnumeric(analysis.(field_name)) && isscalar(analysis.(field_name))
                extent_value = double(analysis.(field_name));
                return;
            end
            if isstruct(params) && isfield(params, field_name) && isnumeric(params.(field_name)) && isscalar(params.(field_name))
                extent_value = double(params.(field_name));
            end
        end

        function lines = build_metadata_lines(meta, params, run_cfg, analysis)
            combined = struct();
            if isstruct(params)
                fn = fieldnames(params);
                for i = 1:numel(fn)
                    v = params.(fn{i});
                    if isscalar(v) || ischar(v) || isstring(v)
                        combined.(fn{i}) = v;
                    end
                end
            end
            if isstruct(run_cfg)
                fn = fieldnames(run_cfg);
                for i = 1:numel(fn)
                    v = run_cfg.(fn{i});
                    if isscalar(v) || ischar(v) || isstring(v)
                        combined.(fn{i}) = v;
                    end
                end
            end
            if isstruct(meta)
                fn = fieldnames(meta);
                for i = 1:numel(fn)
                    combined.(fn{i}) = meta.(fn{i});
                end
            end

            if isfield(analysis, 'omega_snaps') && ~isempty(analysis.omega_snaps)
                combined.snapshot_count = size(analysis.omega_snaps, 3);
            end

            ordered = {'method', 'mode', 'ic_type', 'boundary_condition_case', 'bc_case', ...
                'Nx', 'Ny', 'Lx', 'Ly', 'nu', 'dt', 'Tfinal', 'snapshot_count', ...
                'wall_time', 'max_omega', 'final_energy', 'final_enstrophy', 'total_steps', ...
                'timestamp'};

            lines = {};
            for i = 1:numel(ordered)
                key = ordered{i};
                if ~isfield(combined, key)
                    continue;
                end
                value = combined.(key);
                value_text = ResultsPlotDispatcher.to_scalar_text(value);
                if isempty(value_text)
                    continue;
                end
                lines{end + 1} = sprintf('%-18s %s', strrep(key, '_', ' '), value_text); %#ok<AGROW>
            end
            if isempty(lines)
                lines = {'No metadata available'};
            end
        end

        function dashboard = build_dashboard(meta, params, run_cfg, analysis)
            dashboard = struct();
            dashboard.method = ResultsPlotDispatcher.pick_text(meta, run_cfg, 'method', 'unknown');
            dashboard.mode = ResultsPlotDispatcher.pick_text(meta, run_cfg, 'mode', 'unknown');
            dashboard.ic_type = ResultsPlotDispatcher.pick_text(meta, run_cfg, 'ic_type', 'unknown');
            dashboard.grid = sprintf('%dx%d', ...
                ResultsPlotDispatcher.pick_numeric(params, 'Nx', 0), ...
                ResultsPlotDispatcher.pick_numeric(params, 'Ny', 0));
            dashboard.max_omega = NaN;
            if isfield(meta, 'max_omega') && isnumeric(meta.max_omega)
                dashboard.max_omega = double(meta.max_omega);
            elseif isfield(analysis, 'peak_vorticity') && isnumeric(analysis.peak_vorticity)
                dashboard.max_omega = double(analysis.peak_vorticity);
            end
        end

        function value = pick_text(primary, secondary, field_name, fallback)
            value = fallback;
            if isstruct(primary) && isfield(primary, field_name) && ~isempty(primary.(field_name))
                value = ResultsPlotDispatcher.coerce_text(primary.(field_name), fallback);
                return;
            end
            if isstruct(secondary) && isfield(secondary, field_name) && ~isempty(secondary.(field_name))
                value = ResultsPlotDispatcher.coerce_text(secondary.(field_name), fallback);
            end
        end

        function text = coerce_text(value, fallback)
            if nargin < 2
                fallback = '';
            end

            text = '';
            try
                text = char(string(fallback));
            catch
                text = '';
            end

            if nargin < 1 || isempty(value)
                return;
            end

            candidate = value;
            if iscell(candidate)
                if isempty(candidate) || ~isscalar(candidate)
                    return;
                end
                text = ResultsPlotDispatcher.coerce_text(candidate{1}, text);
                return;
            end

            if ischar(candidate)
                text = char(candidate);
            elseif isstring(candidate)
                if ~isempty(candidate)
                    text = char(candidate(1));
                end
            elseif isnumeric(candidate) || islogical(candidate)
                if isscalar(candidate)
                    text = char(string(candidate));
                end
            elseif isstruct(candidate)
                if isscalar(candidate)
                    preview_keys = {'label', 'name', 'text', 'value', 'status_text', 'status', ...
                        'mode', 'phase', 'stage_name', 'job_label', 'job_key', 'workflow_kind', ...
                        'id', 'token', 'kind', 'run_id', 'study_id'};
                    for i = 1:numel(preview_keys)
                        key = preview_keys{i};
                        if isfield(candidate, key) && ~isempty(candidate.(key))
                            text = ResultsPlotDispatcher.coerce_text(candidate.(key), text);
                            return;
                        end
                    end

                    field_names = fieldnames(candidate);
                    if ~isempty(field_names)
                        parts = cell(1, 0);
                        for i = 1:min(numel(field_names), 4)
                            key = field_names{i};
                            value_text = ResultsPlotDispatcher.coerce_text(candidate.(key), '');
                            if ~isempty(strtrim(value_text))
                                parts{end + 1} = sprintf('%s=%s', key, value_text); %#ok<AGROW>
                            else
                                parts{end + 1} = sprintf('%s=[%s]', key, class(candidate.(key))); %#ok<AGROW>
                            end
                        end
                        if ~isempty(parts)
                            text = strjoin(parts, ', ');
                        end
                    end
                end
            end
        end

        function value = pick_numeric(s, field_name, fallback)
            value = fallback;
            if isstruct(s) && isfield(s, field_name) && isnumeric(s.(field_name)) && isscalar(s.(field_name))
                value = double(s.(field_name));
            end
        end

        function tf = resolve_save_flag(settings, field_names, fallback)
            tf = logical(fallback);
            if ~isstruct(settings)
                return;
            end
            for i = 1:numel(field_names)
                key = field_names{i};
                if isfield(settings, key) && ~isempty(settings.(key))
                    tf = logical(settings.(key));
                    return;
                end
            end
        end

        function tf = infer_png_from_format(settings)
            tf = true;
            if ~isstruct(settings) || ~isfield(settings, 'figure_format') || isempty(settings.figure_format)
                return;
            end
            token = char(string(settings.figure_format));
            tf = ~strcmpi(token, 'fig');
        end

        function tf = infer_pdf_from_format(settings)
            tf = false;
            if ~isstruct(settings) || ~isfield(settings, 'figure_format') || isempty(settings.figure_format)
                return;
            end
            tf = strcmpi(char(string(settings.figure_format)), 'pdf');
        end

        function new_fig = clone_figure(fig_handle)
            new_fig = [];
            if isempty(fig_handle) || ~isgraphics(fig_handle, 'figure')
                return;
            end
            if ~ResultsPlotDispatcher.should_clone_figure(fig_handle)
                return;
            end
            try
                new_fig = copyobj(fig_handle, groot);
            catch
                new_fig = [];
            end
        end

        function tf = should_clone_figure(fig_handle)
            tf = true;
            if isempty(fig_handle) || ~isgraphics(fig_handle, 'figure')
                tf = false;
                return;
            end
            try
                if isa(fig_handle, 'matlab.ui.Figure')
                    tf = false;
                    return;
                end
            catch
            end
        end

        function safe_close(fig_handle)
            if isempty(fig_handle) || ~isgraphics(fig_handle, 'figure')
                return;
            end
            try
                close(fig_handle);
            catch
            end
        end

        function streams = generate_streamline_segments(Xg, Yg, u, v, seed_x, seed_y, min_stream_vertices)
            streams = {};
            if nargin < 7 || ~isfinite(double(min_stream_vertices))
                min_stream_vertices = 6;
            end
            try
                streams = stream2(Xg, Yg, u, v, seed_x(:), seed_y(:));
            catch
                streams = {};
            end
            streams = ResultsPlotDispatcher.valid_streamline_segments(streams, min_stream_vertices);
        end

        function streams = valid_streamline_segments(streams, min_stream_vertices)
            if ~iscell(streams) || isempty(streams)
                streams = {};
                return;
            end
            keep = false(size(streams));
            for i = 1:numel(streams)
                segment = streams{i};
                keep(i) = isnumeric(segment) && size(segment, 2) >= 2 && size(segment, 1) >= min_stream_vertices;
            end
            streams = streams(keep);
        end

        function position = sanitize_saved_fig_position(raw_position)
            position = [48, 48, 880, 520];
            if isnumeric(raw_position) && numel(raw_position) >= 4
                candidate = double(raw_position(1:4));
                if all(isfinite(candidate(3:4))) && all(candidate(3:4) > 0)
                    position(3:4) = candidate(3:4);
                end
                if all(isfinite(candidate(1:2))) && all(candidate(1:2) >= 0)
                    position(1:2) = candidate(1:2);
                end
            end
        end

        function text = to_scalar_text(value)
            text = '';
            if isnumeric(value) && isscalar(value)
                if value == round(value) && abs(value) < 1.0e6
                    text = sprintf('%d', value);
                else
                    text = sprintf('%.5g', value);
                end
            elseif ischar(value) || isstring(value)
                text = char(string(value));
            end
        end

        function value = get_label(labels, field_name, default_value)
            value = default_value;
            if isstruct(labels) && isfield(labels, field_name) && ~isempty(labels.(field_name))
                value = char(string(labels.(field_name)));
            end
        end

        function font_sizes = normalize_font_sizes(raw_sizes)
            defaults = {12, 12, 14};
            font_sizes = raw_sizes;
            if isempty(font_sizes)
                font_sizes = defaults;
                return;
            end
            if isnumeric(font_sizes)
                if isscalar(font_sizes)
                    font_sizes = {font_sizes, font_sizes, font_sizes};
                elseif numel(font_sizes) >= 3
                    font_sizes = num2cell(font_sizes(1:3));
                else
                    font_sizes = defaults;
                end
                return;
            end
            if ischar(font_sizes) || isstring(font_sizes)
                font_sizes = defaults;
                return;
            end
            if ~iscell(font_sizes) || numel(font_sizes) < 3
                font_sizes = defaults;
                return;
            end
            font_sizes = {double(font_sizes{1}), double(font_sizes{2}), double(font_sizes{3})};
        end
    end
end

function local_restore_hold(ax, hold_state)
if isempty(ax) || ~isgraphics(ax, 'axes')
    return;
end
if hold_state
    hold(ax, 'on');
else
    hold(ax, 'off');
end
end
