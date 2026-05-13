classdef ResultsAnimationExporter
    % ResultsAnimationExporter - Shared MP4/GIF exporter for snapshot-capable runs.

    methods (Static)
        function artifacts = export_from_analysis(analysis, params, run_cfg, paths, settings)
            if nargin < 1 || ~isstruct(analysis)
                analysis = struct();
            end
            if nargin < 2 || ~isstruct(params)
                params = struct();
            end
            if nargin < 3 || ~isstruct(run_cfg)
                run_cfg = struct();
            end
            if nargin < 4 || ~isstruct(paths)
                paths = struct();
            end
            if nargin < 5 || ~isstruct(settings)
                settings = struct();
            end

            artifacts = struct('combined_mp4', '', 'combined_gif', '', ...
                'pane_mp4s', struct(), 'pane_gifs', struct(), 'active_panes', {{}}, ...
                'resolved_media', struct(), 'media_status', struct());
            if ~ResultsAnimationExporter.animations_enabled(params, settings)
                return;
            end
            if ~isfield(analysis, 'omega_snaps') || isempty(analysis.omega_snaps) || size(analysis.omega_snaps, 3) < 2
                return;
            end

            media = ResultsAnimationExporter.resolve_media_settings(params, settings);
            if ResultsAnimationExporter.safe_field(paths, 'disable_combined_animation_dir', false)
                media.export_combined_mp4 = false;
                media.export_combined_gif = false;
            end
            artifacts.resolved_media = media;
            [combined_dir, panes_dir] = ResultsAnimationExporter.resolve_output_dirs(paths);
            if (ResultsAnimationExporter.media_has_format(media, 'mp4') && ...
                    ResultsAnimationExporter.media_flag(media, 'export_combined_mp4', true)) || ...
                    (ResultsAnimationExporter.media_has_format(media, 'gif') && ...
                    ResultsAnimationExporter.media_flag(media, 'export_combined_gif', false))
                if ~exist(combined_dir, 'dir')
                    mkdir(combined_dir);
                end
            end
            if (ResultsAnimationExporter.media_has_format(media, 'mp4') && ...
                    ResultsAnimationExporter.media_flag(media, 'export_pane_mp4s', ...
                    ResultsAnimationExporter.media_flag(media, 'export_panes', true))) || ...
                    (ResultsAnimationExporter.media_has_format(media, 'gif') && ...
                    ResultsAnimationExporter.media_flag(media, 'export_pane_gifs', ...
                    ResultsAnimationExporter.media_flag(media, 'export_panes', true)))
                if ~exist(panes_dir, 'dir')
                    mkdir(panes_dir);
                end
            end

            [x_vec, y_vec, plot_times, domain_resolved] = ResultsAnimationExporter.resolve_axes(analysis, params);
            if ~domain_resolved
                artifacts.media_status = struct( ...
                    'requested_media', media, ...
                    'retry_media', ResultsAnimationExporter.safe_mp4_retry_media(media), ...
                    'combined', struct(), ...
                    'panes', struct(), ...
                    'any_failure', true, ...
                    'failure_messages', {{'Animation export skipped because snapshot axes could not be resolved from the saved run metadata.'}});
                return;
            end
            plot_context = resolve_bathymetry_plot_context(analysis, size(analysis.omega_snaps, 1), size(analysis.omega_snaps, 2));
            plot_mask = ResultsAnimationExporter.safe_field(plot_context, 'plot_mask', []);
            wall_mask = ResultsAnimationExporter.safe_field(plot_context, 'wall_mask', []);
            bathy_profile_x = ResultsAnimationExporter.safe_field(plot_context, 'profile_x', []);
            bathy_profile_y = ResultsAnimationExporter.safe_field(plot_context, 'profile_y', []);
            plot_labels = resolve_analysis_plot_labels(analysis);

            omega_cube = double(analysis.omega_snaps);
            psi_cube = ResultsAnimationExporter.extract_matching_cube(analysis, 'psi_snaps', size(omega_cube));
            wall_cube = ResultsAnimationExporter.extract_matching_cube(analysis, 'fd_wall_omega_snaps', size(omega_cube));
            [u_cube, v_cube] = ResultsAnimationExporter.extract_velocity_cubes(analysis, size(omega_cube), plot_context);

            time_span = ResultsAnimationExporter.resolve_time_span(params, analysis, plot_times, size(omega_cube, 3));
            [omega_cube, display_times] = ResultsAnimationExporter.resample_snapshot_cube(omega_cube, plot_times, media.frame_count, time_span);
            if ~isempty(psi_cube)
                [psi_cube, ~] = ResultsAnimationExporter.resample_snapshot_cube(psi_cube, plot_times, media.frame_count, time_span);
            end
            if ~isempty(u_cube)
                [u_cube, ~] = ResultsAnimationExporter.resample_snapshot_cube(u_cube, plot_times, media.frame_count, time_span);
            end
            if ~isempty(v_cube)
                [v_cube, ~] = ResultsAnimationExporter.resample_snapshot_cube(v_cube, plot_times, media.frame_count, time_span);
            end
            if ~isempty(wall_cube)
                [wall_cube, ~] = ResultsAnimationExporter.resample_snapshot_cube(wall_cube, plot_times, media.frame_count, time_span);
            end

            pane_specs = ResultsAnimationExporter.active_pane_specs(plot_labels, wall_cube, plot_context);
            pane_specs = ResultsAnimationExporter.filter_pane_specs(pane_specs, media);
            artifacts.active_panes = {pane_specs.token};
            if isempty(pane_specs)
                return;
            end

            main_title = ResultsPlotDispatcher.compose_export_title('Results Animation', params, run_cfg);
            run_token = ResultsAnimationExporter.run_token(run_cfg, params, paths);
            combined_paths = struct('mp4', '', 'gif', '');
            if ResultsAnimationExporter.media_has_format(media, 'mp4') && ...
                    ResultsAnimationExporter.media_flag(media, 'export_combined_mp4', true)
                combined_paths.mp4 = fullfile(combined_dir, sprintf('%s__animation_grid.mp4', run_token));
                artifacts.combined_mp4 = combined_paths.mp4;
            end
            legacy_combined_gif_path = fullfile(combined_dir, sprintf('%s__animation_grid.gif', run_token));
            if ResultsAnimationExporter.media_has_format(media, 'gif') && ...
                    ResultsAnimationExporter.media_flag(media, 'export_combined_gif', false)
                combined_paths.gif = fullfile(combined_dir, sprintf('%s__animation_grid.gif', run_token));
                artifacts.combined_gif = combined_paths.gif;
            else
                ResultsAnimationExporter.safe_delete(legacy_combined_gif_path);
            end

            pane_mp4_paths = struct();
            pane_gif_paths = struct();
            flatten_pane_dirs = logical(ResultsAnimationExporter.safe_field(paths, 'media_flatten_pane_dirs', false));
            pane_media_stem_map = ResultsAnimationExporter.safe_field(paths, 'pane_media_stem_map', struct());
            export_pane_mp4s = ResultsAnimationExporter.media_has_format(media, 'mp4') && ...
                ResultsAnimationExporter.media_flag(media, 'export_pane_mp4s', ...
                ResultsAnimationExporter.media_flag(media, 'export_panes', true));
            export_pane_gifs = ResultsAnimationExporter.media_has_format(media, 'gif') && ...
                ResultsAnimationExporter.media_flag(media, 'export_pane_gifs', ...
                ResultsAnimationExporter.media_flag(media, 'export_panes', true));
            for k = 1:numel(pane_specs)
                if flatten_pane_dirs
                    pane_dir = panes_dir;
                else
                    pane_dir = fullfile(panes_dir, pane_specs(k).folder_name);
                end
                if ~exist(pane_dir, 'dir')
                    mkdir(pane_dir);
                end
                pane_stem = ResultsAnimationExporter.safe_field(pane_media_stem_map, pane_specs(k).token, '');
                if isempty(pane_stem)
                    pane_stem = sprintf('%s__%s', run_token, pane_specs(k).token);
                end
                if export_pane_mp4s
                    pane_mp4_paths.(pane_specs(k).token) = fullfile(pane_dir, sprintf('%s.mp4', pane_stem));
                end
                if export_pane_gifs
                    pane_gif_paths.(pane_specs(k).token) = fullfile(pane_dir, sprintf('%s.gif', pane_stem));
                end
            end
            artifacts.pane_mp4s = pane_mp4_paths;
            artifacts.pane_gifs = pane_gif_paths;

            [combined_outputs, combined_status] = ResultsAnimationExporter.write_combined_media( ...
                combined_paths, pane_specs, omega_cube, psi_cube, u_cube, v_cube, ...
                wall_cube, x_vec, y_vec, display_times, plot_mask, wall_mask, ...
                bathy_profile_x, bathy_profile_y, plot_labels, main_title, media);
            pane_outputs = struct('mp4s', struct(), 'gifs', struct());
            pane_status = struct();
            if ~isempty(fieldnames(pane_mp4_paths)) || ~isempty(fieldnames(pane_gif_paths))
                [pane_outputs, pane_status] = ResultsAnimationExporter.write_pane_media( ...
                    pane_mp4_paths, pane_gif_paths, pane_specs, omega_cube, psi_cube, u_cube, v_cube, ...
                    wall_cube, x_vec, y_vec, display_times, plot_mask, wall_mask, ...
                    bathy_profile_x, bathy_profile_y, plot_labels, main_title, media);
            end
            artifacts.combined_mp4 = ResultsAnimationExporter.safe_field(combined_outputs, 'mp4', '');
            artifacts.combined_gif = ResultsAnimationExporter.safe_field(combined_outputs, 'gif', '');
            artifacts.pane_mp4s = ResultsAnimationExporter.safe_field(pane_outputs, 'mp4s', struct());
            artifacts.pane_gifs = ResultsAnimationExporter.safe_field(pane_outputs, 'gifs', struct());
            artifacts.media_status = struct( ...
                'requested_media', media, ...
                'retry_media', ResultsAnimationExporter.safe_mp4_retry_media(media), ...
                'combined', combined_status, ...
                'panes', pane_status, ...
                'any_failure', ResultsAnimationExporter.media_status_has_failure(combined_status) || ...
                    ResultsAnimationExporter.media_status_has_failure(pane_status), ...
                'failure_messages', {ResultsAnimationExporter.collect_failure_messages(combined_status, pane_status)});
        end

        function artifacts = export_quad_from_analysis(analysis, params, run_cfg, output_dir, stem, settings)
            if nargin < 1 || ~isstruct(analysis)
                analysis = struct();
            end
            if nargin < 2 || ~isstruct(params)
                params = struct();
            end
            if nargin < 3 || ~isstruct(run_cfg)
                run_cfg = struct();
            end
            if nargin < 4
                output_dir = '';
            end
            if nargin < 5 || isempty(stem)
                stem = 'Quad_Anim';
            end
            if nargin < 6 || ~isstruct(settings)
                settings = struct();
            end

            artifacts = struct('mp4', '', 'gif', '', 'resolved_media', struct(), 'media_status', struct());
            if ~ResultsAnimationExporter.animations_enabled(params, settings)
                return;
            end
            if ~isfield(analysis, 'omega_snaps') || isempty(analysis.omega_snaps) || size(analysis.omega_snaps, 3) < 2
                return;
            end

            output_dir = char(string(output_dir));
            stem = char(string(stem));
            if isempty(output_dir)
                return;
            end
            if exist(output_dir, 'dir') ~= 7
                mkdir(output_dir);
            end

            media = ResultsAnimationExporter.resolve_media_settings(params, settings);
            artifacts.resolved_media = media;
            if ResultsAnimationExporter.media_has_format(media, 'mp4')
                artifacts.mp4 = fullfile(output_dir, sprintf('%s.mp4', stem));
            end
            if ResultsAnimationExporter.media_has_format(media, 'gif')
                artifacts.gif = fullfile(output_dir, sprintf('%s.gif', stem));
            end

            [x_vec, y_vec, plot_times, domain_resolved] = ResultsAnimationExporter.resolve_axes(analysis, params);
            if ~domain_resolved
                artifacts.media_status = struct( ...
                    'mp4', ResultsAnimationExporter.empty_media_status_record(artifacts.mp4, media, 'mp4'), ...
                    'gif', ResultsAnimationExporter.empty_media_status_record(artifacts.gif, media, 'gif'), ...
                    'any_failure', true, ...
                    'failure_messages', {{'Quad animation export skipped because snapshot axes could not be resolved from the saved run metadata.'}});
                return;
            end
            plot_context = resolve_bathymetry_plot_context(analysis, size(analysis.omega_snaps, 1), size(analysis.omega_snaps, 2));
            plot_mask = ResultsAnimationExporter.safe_field(plot_context, 'plot_mask', []);
            wall_mask = ResultsAnimationExporter.safe_field(plot_context, 'wall_mask', []);
            bathy_profile_x = ResultsAnimationExporter.safe_field(plot_context, 'profile_x', []);
            bathy_profile_y = ResultsAnimationExporter.safe_field(plot_context, 'profile_y', []);
            plot_labels = resolve_analysis_plot_labels(analysis);

            omega_cube = double(analysis.omega_snaps);
            psi_cube = ResultsAnimationExporter.extract_matching_cube(analysis, 'psi_snaps', size(omega_cube));
            wall_cube = ResultsAnimationExporter.extract_matching_cube(analysis, 'fd_wall_omega_snaps', size(omega_cube));
            [u_cube, v_cube] = ResultsAnimationExporter.extract_velocity_cubes(analysis, size(omega_cube), plot_context);

            time_span = ResultsAnimationExporter.resolve_time_span(params, analysis, plot_times, size(omega_cube, 3));
            [omega_cube, display_times] = ResultsAnimationExporter.resample_snapshot_cube(omega_cube, plot_times, media.frame_count, time_span);
            if ~isempty(psi_cube)
                [psi_cube, ~] = ResultsAnimationExporter.resample_snapshot_cube(psi_cube, plot_times, media.frame_count, time_span);
            end
            if ~isempty(u_cube)
                [u_cube, ~] = ResultsAnimationExporter.resample_snapshot_cube(u_cube, plot_times, media.frame_count, time_span);
            end
            if ~isempty(v_cube)
                [v_cube, ~] = ResultsAnimationExporter.resample_snapshot_cube(v_cube, plot_times, media.frame_count, time_span);
            end
            if ~isempty(wall_cube)
                [wall_cube, ~] = ResultsAnimationExporter.resample_snapshot_cube(wall_cube, plot_times, media.frame_count, time_span);
            end

            status = struct( ...
                'mp4', ResultsAnimationExporter.empty_media_status_record(artifacts.mp4, media, 'mp4'), ...
                'gif', ResultsAnimationExporter.empty_media_status_record(artifacts.gif, media, 'gif'));
            [fig, tl, axes_list] = ResultsAnimationExporter.create_quad_figure(media);
            cleanup_fig = onCleanup(@() ResultsAnimationExporter.safe_close(fig)); %#ok<NASGU>
            title(tl, char(string(ResultsPlotDispatcher.compose_export_title('IC Study Quad Animation', params, run_cfg))), ...
                'Interpreter', 'none');

            writer = [];
            staged_mp4_path = '';
            if ~isempty(artifacts.mp4)
                try
                    [writer, staged_mp4_path] = ResultsAnimationExporter.open_mp4_writer(artifacts.mp4, media);
                    status.mp4.status = 'running';
                    status.mp4.staged_output_path = staged_mp4_path;
                catch ME
                    status.mp4 = ResultsAnimationExporter.record_media_failure(status.mp4, ME);
                end
            end
            cleanup_writer = onCleanup(@() ResultsAnimationExporter.safe_close_writer(writer)); %#ok<NASGU>
            cleanup_staged_mp4 = onCleanup(@() ResultsAnimationExporter.safe_delete(staged_mp4_path)); %#ok<NASGU>

            [gif_writer_path, staged_gif_path] = ResultsAnimationExporter.stage_gif_writer_path(artifacts.gif);
            status.gif.staged_output_path = staged_gif_path;
            cleanup_staged_gif = onCleanup(@() ResultsAnimationExporter.safe_delete(staged_gif_path)); %#ok<NASGU>

            mp4_frames_written = 0;
            gif_frames_written = 0;
            render_error = [];
            for idx = 1:size(omega_cube, 3)
                try
                    ResultsAnimationExporter.render_quad_frame(axes_list, ...
                        omega_cube(:, :, idx), ResultsAnimationExporter.extract_cube_snapshot(psi_cube, idx), ...
                        ResultsAnimationExporter.extract_cube_snapshot(u_cube, idx), ResultsAnimationExporter.extract_cube_snapshot(v_cube, idx), ...
                        ResultsAnimationExporter.extract_cube_snapshot(wall_cube, idx), ...
                        x_vec, y_vec, ResultsAnimationExporter.time_text(display_times, idx), ...
                        plot_mask, wall_mask, bathy_profile_x, bathy_profile_y, plot_labels);
                    rgb_frame = ResultsAnimationExporter.capture_rgb_frame(fig, media);
                catch ME
                    render_error = ME;
                    if ~isempty(writer)
                        ResultsAnimationExporter.safe_close_writer(writer);
                        writer = [];
                    end
                    status.mp4 = ResultsAnimationExporter.record_media_failure(status.mp4, ME);
                    status.gif = ResultsAnimationExporter.record_media_failure(status.gif, ME);
                    break;
                end
                if ~isempty(writer) && ~strcmp(status.mp4.status, 'failed')
                    try
                        writeVideo(writer, rgb_frame);
                        mp4_frames_written = mp4_frames_written + 1;
                    catch ME
                        status.mp4 = ResultsAnimationExporter.record_media_failure(status.mp4, ME);
                        ResultsAnimationExporter.safe_close_writer(writer);
                        writer = [];
                    end
                end
                if ~isempty(gif_writer_path) && ~strcmp(status.gif.status, 'failed')
                    try
                        ResultsAnimationExporter.write_gif_frame(gif_writer_path, rgb_frame, idx, media);
                        gif_frames_written = gif_frames_written + 1;
                    catch ME
                        status.gif = ResultsAnimationExporter.record_media_failure(status.gif, ME);
                        ResultsAnimationExporter.safe_delete(gif_writer_path);
                    end
                end
            end
            [status.mp4, writer] = ResultsAnimationExporter.close_mp4_writer_with_status(status.mp4, writer);
            if isempty(render_error)
                status.mp4 = ResultsAnimationExporter.finalize_media_status( ...
                    status.mp4, mp4_frames_written, staged_mp4_path, artifacts.mp4, media, 'mp4');
                status.gif = ResultsAnimationExporter.finalize_media_status( ...
                    status.gif, gif_frames_written, staged_gif_path, artifacts.gif, media, 'gif');
            end
            if ResultsAnimationExporter.should_retry_mp4(status.mp4, media)
                retry_media = ResultsAnimationExporter.safe_mp4_retry_media(media);
                status.mp4 = ResultsAnimationExporter.retry_quad_mp4( ...
                    status.mp4, artifacts.mp4, omega_cube, psi_cube, u_cube, v_cube, wall_cube, ...
                    x_vec, y_vec, display_times, plot_mask, wall_mask, bathy_profile_x, ...
                    bathy_profile_y, plot_labels, retry_media);
            end

            artifacts.mp4 = status.mp4.validated_output_path;
            artifacts.gif = status.gif.validated_output_path;
            artifacts.media_status = struct( ...
                'requested_media', media, ...
                'retry_media', ResultsAnimationExporter.safe_mp4_retry_media(media), ...
                'quad', status, ...
                'any_failure', ResultsAnimationExporter.media_status_has_failure(status), ...
                'failure_messages', {ResultsAnimationExporter.collect_failure_messages(status)});
        end
    end

    methods (Static, Access = private)
        function tf = animations_enabled(params, settings)
            tf = false;
            if isfield(settings, 'media') && isstruct(settings.media) && isfield(settings.media, 'enabled')
                tf = logical(settings.media.enabled);
            end
            if isfield(settings, 'animation_enabled')
                tf = tf || logical(settings.animation_enabled);
            end
            if isfield(params, 'create_animations')
                tf = tf || logical(params.create_animations);
            end
        end

        function media = resolve_media_settings(params, settings)
            media = struct('format', 'mp4+gif', 'formats', {{}}, 'fps', 30, 'frame_count', NaN, ...
                'duration_s', 10, 'dpi', 600, 'quality', 90, 'width_in', 7.16, ...
                'height_in', 5.37, 'resolution_px', [4296, 3222], 'gif_min_frame_count', 2, ...
                'pane_tokens', {{}}, 'export_panes', true);
            has_explicit_resolution = false;
            if isfield(params, 'animation_format'), media.format = char(string(params.animation_format)); end
            if isfield(params, 'animation_export_format'), media.format = char(string(params.animation_export_format)); end
            if isfield(params, 'animation_export_formats'), media.formats = params.animation_export_formats; end
            if isfield(params, 'animation_fps'), media.fps = double(params.animation_fps); end
            if isfield(params, 'animation_duration_s'), media.duration_s = double(params.animation_duration_s); end
            if isfield(params, 'animation_num_frames'), media.frame_count = double(params.animation_num_frames); end
            if isfield(params, 'num_animation_frames'), media.frame_count = double(params.num_animation_frames); end
            if isfield(params, 'animation_gif_min_frames'), media.gif_min_frame_count = double(params.animation_gif_min_frames); end
            if isfield(params, 'animation_export_dpi'), media.dpi = double(params.animation_export_dpi); end
            if isfield(params, 'animation_export_width_in'), media.width_in = double(params.animation_export_width_in); end
            if isfield(params, 'animation_export_height_in'), media.height_in = double(params.animation_export_height_in); end
            if isfield(params, 'animation_export_resolution_px')
                media.resolution_px = double(params.animation_export_resolution_px);
                has_explicit_resolution = true;
            end
            if isfield(settings, 'media') && isstruct(settings.media)
                media = ResultsAnimationExporter.overlay(media, settings.media);
                if isfield(settings.media, 'format') && ~isfield(settings.media, 'formats')
                    media.formats = {};
                end
                has_explicit_resolution = has_explicit_resolution || ...
                    (isfield(settings.media, 'resolution_px') && ~isempty(settings.media.resolution_px));
            end
            if isfield(settings, 'animation_format')
                media.format = char(string(settings.animation_format));
                if ~isfield(settings, 'animation_export_formats'), media.formats = {}; end
            end
            if isfield(settings, 'animation_export_format')
                media.format = char(string(settings.animation_export_format));
                if ~isfield(settings, 'animation_export_formats'), media.formats = {}; end
            end
            if isfield(settings, 'animation_export_formats'), media.formats = settings.animation_export_formats; end
            if isfield(settings, 'animation_fps'), media.fps = double(settings.animation_fps); end
            if isfield(settings, 'animation_duration_s'), media.duration_s = double(settings.animation_duration_s); end
            if isfield(settings, 'animation_frame_count'), media.frame_count = double(settings.animation_frame_count); end
            if isfield(settings, 'animation_num_frames'), media.frame_count = double(settings.animation_num_frames); end
            if isfield(settings, 'animation_gif_min_frames'), media.gif_min_frame_count = double(settings.animation_gif_min_frames); end
            if isfield(settings, 'animation_export_dpi'), media.dpi = double(settings.animation_export_dpi); end
            if isfield(settings, 'animation_export_width_in'), media.width_in = double(settings.animation_export_width_in); end
            if isfield(settings, 'animation_export_height_in'), media.height_in = double(settings.animation_export_height_in); end
            if isfield(settings, 'animation_export_resolution_px')
                media.resolution_px = double(settings.animation_export_resolution_px);
                has_explicit_resolution = true;
            end
            if isfield(settings, 'animation_quality'), media.quality = double(settings.animation_quality); end
            media.formats = ResultsAnimationExporter.normalize_media_formats(media);
            media.fps = max(1, double(media.fps));
            media.duration_s = max(0.1, double(media.duration_s));
            if ~isfinite(media.frame_count) || media.frame_count < 2
                media.frame_count = round(media.duration_s * media.fps);
            end
            media.frame_count = max(2, round(double(media.frame_count)));
            media.gif_min_frame_count = max(2, round(double(media.gif_min_frame_count)));
            if ResultsAnimationExporter.media_has_format(media, 'gif')
                media.frame_count = max(media.frame_count, media.gif_min_frame_count);
            else
                media.gif_min_frame_count = min(media.gif_min_frame_count, media.frame_count);
            end
            media.fps = max(1, double(media.frame_count) / media.duration_s);
            media.dpi = max(72, double(media.dpi));
            media.width_in = max(1, double(media.width_in));
            media.height_in = max(1, double(media.height_in));
            if has_explicit_resolution && isnumeric(media.resolution_px) && numel(media.resolution_px) >= 2
                media.resolution_px = double(media.resolution_px(1:2));
            else
                media.resolution_px = max(2, round([media.width_in, media.height_in] * media.dpi));
            end
            media.resolution_px = max(2, round(double(media.resolution_px(1:2))));
            media.resolution_px = 2 * ceil(media.resolution_px / 2);
            media.requested_resolution_px = media.resolution_px;
            media.encoder_safe_resize_applied = false;
            if ResultsAnimationExporter.media_has_format(media, 'mp4')
                safe_resolution = ResultsAnimationExporter.encoder_safe_resolution(media.resolution_px, [1920, 1440]);
                if any(safe_resolution ~= media.resolution_px)
                    media.encoder_safe_resize_applied = true;
                    media.resolution_px = safe_resolution;
                    media.width_in = media.resolution_px(1) / media.dpi;
                    media.height_in = media.resolution_px(2) / media.dpi;
                end
            end
            media.quality = min(max(round(double(media.quality)), 0), 100);
        end

        function [combined_dir, panes_dir] = resolve_output_dirs(paths)
            evolution_root = ResultsAnimationExporter.safe_field(paths, 'figures_evolution_root', '');
            if isempty(evolution_root)
                evolution_root = ResultsAnimationExporter.safe_field(paths, 'figures_animation', '');
            end
            if isempty(evolution_root)
                figures_root = ResultsAnimationExporter.safe_field(paths, 'figures_root', '');
                if ~isempty(figures_root)
                    evolution_root = fullfile(figures_root, 'Evolution');
                else
                    evolution_root = fullfile(char(string(ResultsAnimationExporter.safe_field(paths, 'base', pwd))), ...
                        'Figures', 'Evolution');
                end
            end
            combined_dir = ResultsAnimationExporter.safe_field(paths, 'figures_evolution_combined', ...
                fullfile(evolution_root, 'Combined'));
            panes_dir = evolution_root;
        end

        function [x_vec, y_vec, plot_times, domain_resolved] = resolve_axes(analysis, params)
            nx = size(analysis.omega_snaps, 2);
            ny = size(analysis.omega_snaps, 1);
            [x_vec, domain_x] = ResultsAnimationExporter.resolve_axis_vector(analysis, params, ...
                {'x', 'x_vec', 'x_coords', 'snapshot_x', 'x_nodes'}, nx, 'Lx');
            [y_vec, domain_y] = ResultsAnimationExporter.resolve_axis_vector(analysis, params, ...
                {'y', 'y_vec', 'y_coords', 'snapshot_y', 'y_nodes'}, ny, 'Ly');
            domain_resolved = domain_x && domain_y;
            plot_times = [];
            if isfield(analysis, 'snapshot_times_requested') && ~isempty(analysis.snapshot_times_requested)
                plot_times = double(analysis.snapshot_times_requested(:)).';
            elseif isfield(analysis, 'snapshot_times') && ~isempty(analysis.snapshot_times)
                plot_times = double(analysis.snapshot_times(:)).';
            end
        end

        function [axis_vec, resolved] = resolve_axis_vector(analysis, params, field_candidates, count, extent_field)
            axis_vec = [];
            resolved = false;
            axis_vec = ResultsAnimationExporter.pick_axis_vector(analysis, field_candidates, count);
            if ~isempty(axis_vec)
                resolved = true;
            end
            if isempty(axis_vec)
                axis_vec = ResultsAnimationExporter.pick_axis_vector(params, field_candidates, count);
                if ~isempty(axis_vec)
                    resolved = true;
                end
            end
            if isempty(axis_vec)
                extent_value = ResultsAnimationExporter.pick_numeric(analysis, extent_field, NaN);
                if ~(isfinite(extent_value) && extent_value > 0)
                    extent_value = ResultsAnimationExporter.pick_numeric(params, extent_field, NaN);
                end
                if isfinite(extent_value) && extent_value > 0
                    axis_vec = linspace(-extent_value / 2, extent_value / 2, count);
                    resolved = true;
                else
                    axis_vec = 1:count;
                    resolved = false;
                end
            end
            axis_vec = double(axis_vec(:)).';
        end

        function axis_vec = pick_axis_vector(source, field_candidates, count)
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

        function pane_specs = active_pane_specs(plot_labels, wall_cube, plot_context)
            pane_specs = struct( ...
                'token', {'evolution', 'contour', 'vector', 'streamlines', 'streamfunction', 'speed'}, ...
                'mode', {'evolution', 'contour', 'vector', 'streamline', 'streamfunction', 'speed'}, ...
                'title', {plot_labels.snapshot_title_base, plot_labels.contour_title_base, plot_labels.vector_title_base, ...
                    plot_labels.streamline_title_base, plot_labels.streamfunction_title_base, plot_labels.speed_title_base}, ...
                'folder_name', {'Evolution', 'Contour', 'Vector', 'Streamlines', 'Streamfunction', 'Velocity'});
            if ~isempty(wall_cube) && isfield(plot_context, 'wall_mask') && ~isempty(plot_context.wall_mask) && any(plot_context.wall_mask(:))
                pane_specs(end + 1) = struct('token', 'wall_vorticity', 'mode', 'wall_omega', ...
                    'title', plot_labels.wall_omega_title_base, 'folder_name', 'WallVorticity');
            end
        end

        function pane_specs = filter_pane_specs(pane_specs, media)
            if isempty(pane_specs) || ~isstruct(media) || ~isfield(media, 'pane_tokens') || isempty(media.pane_tokens)
                return;
            end
            requested = cellstr(string(media.pane_tokens));
            requested = lower(strtrim(requested(:).'));
            requested = requested(~cellfun(@isempty, requested));
            if isempty(requested)
                return;
            end
            keep = ismember(lower({pane_specs.token}), requested);
            pane_specs = pane_specs(keep);
        end

        function [validated_outputs, status] = write_combined_media(output_paths, pane_specs, omega_cube, psi_cube, u_cube, v_cube, wall_cube, ...
                x_vec, y_vec, display_times, plot_mask, wall_mask, bathy_profile_x, bathy_profile_y, plot_labels, main_title, media)
            validated_outputs = struct('mp4', '', 'gif', '');
            status = struct('mp4', ResultsAnimationExporter.empty_media_status_record('', media, 'mp4'), ...
                'gif', ResultsAnimationExporter.empty_media_status_record('', media, 'gif'));
            mp4_path = ResultsAnimationExporter.safe_field(output_paths, 'mp4', '');
            gif_path = ResultsAnimationExporter.safe_field(output_paths, 'gif', '');
            status.mp4 = ResultsAnimationExporter.empty_media_status_record(mp4_path, media, 'mp4');
            status.gif = ResultsAnimationExporter.empty_media_status_record(gif_path, media, 'gif');
            if isempty(mp4_path) && isempty(gif_path)
                return;
            end
            [fig, tl, axes_list] = ResultsAnimationExporter.create_combined_figure(numel(pane_specs), media);
            cleanup_fig = onCleanup(@() ResultsAnimationExporter.safe_close(fig));
            ResultsPlotDispatcher.apply_tiled_annotations(tl, main_title, 'x', 'y', ResultsPlotDispatcher.default_light_colors());
            writer = [];
            staged_mp4_path = '';
            if ~isempty(mp4_path)
                try
                    [writer, staged_mp4_path] = ResultsAnimationExporter.open_mp4_writer(mp4_path, media);
                    status.mp4.status = 'running';
                    status.mp4.staged_output_path = staged_mp4_path;
                catch ME
                    status.mp4 = ResultsAnimationExporter.record_media_failure(status.mp4, ME);
                end
            end
            cleanup_writer = onCleanup(@() ResultsAnimationExporter.safe_close_writer(writer));
            cleanup_staged_mp4 = onCleanup(@() ResultsAnimationExporter.safe_delete(staged_mp4_path)); %#ok<NASGU>
            [gif_writer_path, staged_gif_path] = ResultsAnimationExporter.stage_gif_writer_path(gif_path);
            status.gif.staged_output_path = staged_gif_path;
            cleanup_staged_gif = onCleanup(@() ResultsAnimationExporter.safe_delete(staged_gif_path)); %#ok<NASGU>
            mp4_frames_written = 0;
            gif_frames_written = 0;
            render_error = [];
            for idx = 1:size(omega_cube, 3)
                try
                    ResultsAnimationExporter.render_combined_frame(axes_list, pane_specs, ...
                        omega_cube(:, :, idx), ResultsAnimationExporter.extract_cube_snapshot(psi_cube, idx), ...
                        ResultsAnimationExporter.extract_cube_snapshot(u_cube, idx), ResultsAnimationExporter.extract_cube_snapshot(v_cube, idx), ...
                        ResultsAnimationExporter.extract_cube_snapshot(wall_cube, idx), x_vec, y_vec, ...
                        ResultsAnimationExporter.time_text(display_times, idx), plot_mask, wall_mask, bathy_profile_x, bathy_profile_y, plot_labels);
                    rgb_frame = ResultsAnimationExporter.capture_rgb_frame(fig, media);
                catch ME
                    render_error = ME;
                    if ~isempty(writer)
                        ResultsAnimationExporter.safe_close_writer(writer);
                        writer = [];
                    end
                    status.mp4 = ResultsAnimationExporter.record_media_failure(status.mp4, ME);
                    status.gif = ResultsAnimationExporter.record_media_failure(status.gif, ME);
                    break;
                end
                if ~isempty(writer) && ~strcmp(status.mp4.status, 'failed')
                    try
                        writeVideo(writer, rgb_frame);
                        mp4_frames_written = mp4_frames_written + 1;
                    catch ME
                        status.mp4 = ResultsAnimationExporter.record_media_failure(status.mp4, ME);
                        ResultsAnimationExporter.safe_close_writer(writer);
                        writer = [];
                    end
                end
                if ~isempty(gif_writer_path) && ~strcmp(status.gif.status, 'failed')
                    try
                        ResultsAnimationExporter.write_gif_frame(gif_writer_path, rgb_frame, idx, media);
                        gif_frames_written = gif_frames_written + 1;
                    catch ME
                        status.gif = ResultsAnimationExporter.record_media_failure(status.gif, ME);
                        ResultsAnimationExporter.safe_delete(gif_writer_path);
                    end
                end
            end
            [status.mp4, writer] = ResultsAnimationExporter.close_mp4_writer_with_status(status.mp4, writer);
            if isempty(render_error)
                status.mp4 = ResultsAnimationExporter.finalize_media_status( ...
                    status.mp4, mp4_frames_written, staged_mp4_path, mp4_path, media, 'mp4');
                status.gif = ResultsAnimationExporter.finalize_media_status( ...
                    status.gif, gif_frames_written, staged_gif_path, gif_path, media, 'gif');
            end
            if ResultsAnimationExporter.should_retry_mp4(status.mp4, media)
                retry_media = ResultsAnimationExporter.safe_mp4_retry_media(media);
                retry_status = ResultsAnimationExporter.retry_combined_mp4( ...
                    status.mp4, mp4_path, pane_specs, omega_cube, psi_cube, u_cube, v_cube, wall_cube, ...
                    x_vec, y_vec, display_times, plot_mask, wall_mask, bathy_profile_x, ...
                    bathy_profile_y, plot_labels, main_title, retry_media);
                status.mp4 = retry_status;
            end
            validated_outputs.mp4 = status.mp4.validated_output_path;
            validated_outputs.gif = status.gif.validated_output_path;
        end

        function [validated_outputs, status_by_pane] = write_pane_media(pane_mp4_paths, pane_gif_paths, pane_specs, omega_cube, psi_cube, u_cube, v_cube, wall_cube, ...
                x_vec, y_vec, display_times, plot_mask, wall_mask, bathy_profile_x, bathy_profile_y, plot_labels, main_title, media)
            validated_outputs = struct('mp4s', struct(), 'gifs', struct());
            status_by_pane = struct();
            pane_names = unique([fieldnames(pane_mp4_paths); fieldnames(pane_gif_paths)], 'stable');
            for i = 1:numel(pane_names)
                token = pane_names{i};
                spec_idx = find(strcmp({pane_specs.token}, token), 1, 'first');
                if isempty(spec_idx)
                    continue;
                end
                spec = pane_specs(spec_idx);
                mp4_path = ResultsAnimationExporter.safe_field(pane_mp4_paths, token, '');
                gif_path = ResultsAnimationExporter.safe_field(pane_gif_paths, token, '');
                pane_status = struct('mp4', ResultsAnimationExporter.empty_media_status_record(mp4_path, media, 'mp4'), ...
                    'gif', ResultsAnimationExporter.empty_media_status_record(gif_path, media, 'gif'));
                if isempty(mp4_path) && isempty(gif_path)
                    status_by_pane.(token) = pane_status;
                    continue;
                end
                [fig, ax] = ResultsAnimationExporter.create_single_pane_figure(media);
                cleanup_fig = onCleanup(@() ResultsAnimationExporter.safe_close(fig));
                writer = [];
                staged_mp4_path = '';
                if ~isempty(mp4_path)
                    try
                        [writer, staged_mp4_path] = ResultsAnimationExporter.open_mp4_writer(mp4_path, media);
                        pane_status.mp4.status = 'running';
                        pane_status.mp4.staged_output_path = staged_mp4_path;
                    catch ME
                        pane_status.mp4 = ResultsAnimationExporter.record_media_failure(pane_status.mp4, ME);
                    end
                end
                cleanup_writer = onCleanup(@() ResultsAnimationExporter.safe_close_writer(writer));
                cleanup_staged_mp4 = onCleanup(@() ResultsAnimationExporter.safe_delete(staged_mp4_path)); %#ok<NASGU>
                [gif_writer_path, staged_gif_path] = ResultsAnimationExporter.stage_gif_writer_path(gif_path);
                pane_status.gif.staged_output_path = staged_gif_path;
                cleanup_staged_gif = onCleanup(@() ResultsAnimationExporter.safe_delete(staged_gif_path)); %#ok<NASGU>
                mp4_frames_written = 0;
                gif_frames_written = 0;
                render_error = [];
                for idx = 1:size(omega_cube, 3)
                    try
                        ResultsAnimationExporter.render_single_pane(ax, spec.mode, omega_cube(:, :, idx), ...
                            ResultsAnimationExporter.extract_cube_snapshot(psi_cube, idx), ...
                            ResultsAnimationExporter.extract_cube_snapshot(u_cube, idx), ResultsAnimationExporter.extract_cube_snapshot(v_cube, idx), ...
                            ResultsAnimationExporter.extract_cube_snapshot(wall_cube, idx), ...
                            x_vec, y_vec, ResultsAnimationExporter.time_text(display_times, idx), plot_mask, wall_mask, ...
                            bathy_profile_x, bathy_profile_y, plot_labels);
                        rgb_frame = ResultsAnimationExporter.capture_rgb_frame(fig, media);
                    catch ME
                        render_error = ME;
                        if ~isempty(writer)
                            ResultsAnimationExporter.safe_close_writer(writer);
                            writer = [];
                        end
                        pane_status.mp4 = ResultsAnimationExporter.record_media_failure(pane_status.mp4, ME);
                        pane_status.gif = ResultsAnimationExporter.record_media_failure(pane_status.gif, ME);
                        break;
                    end
                    if ~isempty(writer) && ~strcmp(pane_status.mp4.status, 'failed')
                        try
                            writeVideo(writer, rgb_frame);
                            mp4_frames_written = mp4_frames_written + 1;
                        catch ME
                            pane_status.mp4 = ResultsAnimationExporter.record_media_failure(pane_status.mp4, ME);
                            ResultsAnimationExporter.safe_close_writer(writer);
                            writer = [];
                        end
                    end
                    if ~isempty(gif_writer_path) && ~strcmp(pane_status.gif.status, 'failed')
                        try
                            ResultsAnimationExporter.write_gif_frame(gif_writer_path, rgb_frame, idx, media);
                            gif_frames_written = gif_frames_written + 1;
                        catch ME
                            pane_status.gif = ResultsAnimationExporter.record_media_failure(pane_status.gif, ME);
                            ResultsAnimationExporter.safe_delete(gif_writer_path);
                        end
                    end
                end
                [pane_status.mp4, writer] = ResultsAnimationExporter.close_mp4_writer_with_status(pane_status.mp4, writer);
                if isempty(render_error)
                    pane_status.mp4 = ResultsAnimationExporter.finalize_media_status( ...
                        pane_status.mp4, mp4_frames_written, staged_mp4_path, mp4_path, media, 'mp4');
                    pane_status.gif = ResultsAnimationExporter.finalize_media_status( ...
                        pane_status.gif, gif_frames_written, staged_gif_path, gif_path, media, 'gif');
                end
                if ResultsAnimationExporter.should_retry_mp4(pane_status.mp4, media)
                    retry_media = ResultsAnimationExporter.safe_mp4_retry_media(media);
                    pane_status.mp4 = ResultsAnimationExporter.retry_pane_mp4( ...
                        pane_status.mp4, mp4_path, spec, omega_cube, psi_cube, u_cube, v_cube, wall_cube, ...
                        x_vec, y_vec, display_times, plot_mask, wall_mask, bathy_profile_x, ...
                        bathy_profile_y, plot_labels, main_title, retry_media);
                end
                validated_outputs.mp4s.(token) = pane_status.mp4.validated_output_path;
                validated_outputs.gifs.(token) = pane_status.gif.validated_output_path;
                status_by_pane.(token) = pane_status;
            end
        end

        function [fig, tl, axes_list] = create_combined_figure(n_active, media)
            fig = figure('Visible', 'off', 'HandleVisibility', 'off', 'Color', [1 1 1], ...
                'MenuBar', 'none', 'ToolBar', 'none', 'Units', 'inches', ...
                'Position', [0.5 0.5 media.width_in media.height_in], 'PaperPositionMode', 'auto');
            tl = tiledlayout(fig, 3, 3, 'Padding', 'compact', 'TileSpacing', 'compact');
            axes_list = gobjects(1, 9);
            for i = 1:9
                axes_list(i) = nexttile(tl, i);
                if i > n_active
                    axis(axes_list(i), 'off');
                end
            end
        end

        function [fig, tl, axes_list] = create_quad_figure(media)
            fig = figure('Visible', 'off', 'HandleVisibility', 'off', 'Color', [1 1 1], ...
                'MenuBar', 'none', 'ToolBar', 'none', 'Units', 'inches', ...
                'Position', [0.5 0.5 media.width_in media.height_in], 'PaperPositionMode', 'auto');
            tl = tiledlayout(fig, 2, 2, 'Padding', 'compact', 'TileSpacing', 'compact');
            axes_list = gobjects(1, 4);
            for i = 1:4
                axes_list(i) = nexttile(tl, i);
            end
        end

        function [fig, ax] = create_single_pane_figure(media)
            fig = figure('Visible', 'off', 'HandleVisibility', 'off', 'Color', [1 1 1], ...
                'MenuBar', 'none', 'ToolBar', 'none', 'Units', 'inches', ...
                'Position', [0.5 0.5 media.width_in media.height_in], 'PaperPositionMode', 'auto');
            ax = axes(fig, 'Units', 'normalized', ...
                'Position', ResultsAnimationExporter.single_pane_axes_position()); %#ok<LAXES>
        end

        function [writer, staged_output_path] = open_mp4_writer(output_path, media)
            staged_output_path = ResultsAnimationExporter.stage_mp4_output_path(output_path);
            writer_path = output_path;
            if ~isempty(staged_output_path)
                writer_path = staged_output_path;
            end
            try
                writer = VideoWriter(writer_path, 'MPEG-4');
            catch ME
                error('ResultsAnimationExporter:MP4Unavailable', ...
                    'MP4 export is required but VideoWriter(MPEG-4) failed for %s: %s', output_path, ME.message);
            end
            try
                writer.FrameRate = media.fps;
                writer.Quality = media.quality;
                open(writer);
            catch ME
                error('ResultsAnimationExporter:MP4WriterOpenFailed', ...
                    'MP4 writer failed for %s with fps=%g, quality=%g, resolution=%dx%d: %s', ...
                    output_path, double(media.fps), double(media.quality), ...
                    round(double(media.resolution_px(1))), round(double(media.resolution_px(2))), ME.message);
            end
        end

        function [writer_path, staged_output_path] = stage_gif_writer_path(output_path)
            writer_path = '';
            staged_output_path = '';
            if isempty(output_path)
                return;
            end
            output_path = char(string(output_path));
            staged_output_path = ResultsAnimationExporter.stage_gif_output_path(output_path);
            writer_path = output_path;
            if ~isempty(staged_output_path)
                writer_path = staged_output_path;
            end
        end

        function write_gif_frame(output_path, rgb_frame, frame_index, media)
            if isempty(output_path)
                return;
            end
            persistent gif_palette_cache;
            if isempty(gif_palette_cache)
                gif_palette_cache = containers.Map('KeyType', 'char', 'ValueType', 'any');
            end
            delay_time = 1 / max(1, double(media.fps));
            if ~isa(rgb_frame, 'uint8')
                rgb_frame = im2uint8(rgb_frame);
            end
            if size(rgb_frame, 3) == 1
                rgb_frame = repmat(rgb_frame, 1, 1, 3);
            end
            cache_key = char(string(output_path));
            if frame_index == 1
                [indexed, cmap] = rgb2ind(rgb_frame, 256);
                gif_palette_cache(cache_key) = cmap;
                imwrite(indexed, cmap, output_path, 'gif', 'LoopCount', inf, 'DelayTime', delay_time);
            else
                if isKey(gif_palette_cache, cache_key)
                    cmap = gif_palette_cache(cache_key);
                    try
                        indexed = rgb2ind(rgb_frame, cmap, 'nodither');
                    catch
                        [indexed, cmap] = rgb2ind(rgb_frame, 256);
                        gif_palette_cache(cache_key) = cmap;
                    end
                else
                    [indexed, cmap] = rgb2ind(rgb_frame, 256);
                    gif_palette_cache(cache_key) = cmap;
                end
                imwrite(indexed, cmap, output_path, 'gif', 'WriteMode', 'append', 'DelayTime', delay_time);
            end
        end

        function status = write_frame_sequence_gif(output_path, frames, media)
            if nargin < 2 || isempty(frames)
                frames = {};
            end
            if nargin < 3 || ~isstruct(media)
                media = struct();
            end
            if ~iscell(frames)
                frames = {frames};
            end
            status = ResultsAnimationExporter.empty_media_status_record(output_path, media, 'gif');
            frame_count = numel(frames);
            if isempty(strtrim(char(string(output_path)))) || frame_count < 1
                status.status = 'not_requested';
                status.frames_requested = double(frame_count);
                return;
            end
            status.attempt_count = 1;
            [gif_writer_path, staged_gif_path] = ResultsAnimationExporter.stage_gif_writer_path(output_path);
            try
                for idx = 1:frame_count
                    frame_rgb = frames{idx};
                    if isempty(frame_rgb)
                        error('ResultsAnimationExporter:EmptyFrameSequenceEntry', ...
                            'Frame %d for %s is empty.', idx, output_path);
                    end
                    ResultsAnimationExporter.write_gif_frame(gif_writer_path, frame_rgb, idx, media);
                end
                status = ResultsAnimationExporter.finalize_media_status( ...
                    status, frame_count, staged_gif_path, output_path, media, 'gif');
            catch ME
                status = ResultsAnimationExporter.record_media_failure(status, ME);
                ResultsAnimationExporter.safe_delete(staged_gif_path);
                ResultsAnimationExporter.safe_delete(output_path);
            end
        end

        function render_combined_frame(axes_list, pane_specs, omega_slice, psi_slice, u_slice, v_slice, wall_slice, ...
                x_vec, y_vec, t_str, plot_mask, wall_mask, bathy_profile_x, bathy_profile_y, plot_labels)
            for i = 1:numel(axes_list)
                if i > numel(pane_specs)
                    cla(axes_list(i));
                    axis(axes_list(i), 'off');
                    continue;
                end
                ResultsAnimationExporter.render_single_pane(axes_list(i), pane_specs(i).mode, omega_slice, psi_slice, u_slice, v_slice, ...
                    wall_slice, x_vec, y_vec, t_str, plot_mask, wall_mask, bathy_profile_x, bathy_profile_y, plot_labels);
            end
        end

        function render_quad_frame(axes_list, omega_slice, psi_slice, u_slice, v_slice, wall_slice, ...
                x_vec, y_vec, t_str, plot_mask, wall_mask, bathy_profile_x, bathy_profile_y, plot_labels)
            quad_modes = {'evolution', 'contour', 'vector', 'streamline'};
            for i = 1:numel(quad_modes)
                ResultsAnimationExporter.render_single_pane(axes_list(i), quad_modes{i}, omega_slice, psi_slice, u_slice, v_slice, ...
                    wall_slice, x_vec, y_vec, t_str, plot_mask, wall_mask, bathy_profile_x, bathy_profile_y, plot_labels);
            end
        end

        function render_single_pane(ax, mode_token, omega_slice, psi_slice, u_slice, v_slice, wall_slice, ...
                x_vec, y_vec, t_str, plot_mask, wall_mask, bathy_profile_x, bathy_profile_y, plot_labels)
            colors = ResultsPlotDispatcher.default_light_colors();
            active_mask = plot_mask;
            if strcmp(mode_token, 'wall_omega') && ~isempty(wall_slice)
                omega_input = wall_slice;
                active_mask = wall_mask;
            else
                omega_input = omega_slice;
            end
            [cmin, cmax] = ResultsAnimationExporter.compute_limits(omega_input, active_mask);
            ResultsAnimationExporter.render_view_mode(ax, mode_token, omega_slice, psi_slice, u_slice, v_slice, wall_slice, ...
                x_vec, y_vec, t_str, colors, active_mask, bathy_profile_x, bathy_profile_y, cmin, cmax, plot_labels);
        end

        function render_view_mode(ax, mode_token, omega_slice, psi_slice, u_slice, v_slice, wall_slice, ...
                x_vec, y_vec, t_str, plot_colors, plot_mask, bathy_profile_x, bathy_profile_y, cmin, cmax, plot_labels)
            cla(ax);
            omega_plot = ResultsAnimationExporter.apply_plot_mask(omega_slice, plot_mask);
            [Xg, Yg] = meshgrid(x_vec, y_vec);
            skip = max(1, round(min(numel(x_vec), numel(y_vec)) / 24));
            cb = [];

            switch lower(char(string(mode_token)))
                case 'evolution'
                    h = imagesc(ax, x_vec, y_vec, omega_plot);
                    set(h, 'AlphaData', double(isfinite(omega_plot)));
                    colormap(ax, turbo);
                    clim(ax, [cmin cmax]);
                    cb = colorbar(ax);
                    ResultsAnimationExporter.style_animation_colorbar(cb);
                    title(ax, ResultsAnimationExporter.animation_time_title(t_str), ...
                        'Color', ResultsAnimationExporter.animation_text_color(), 'Interpreter', 'none', 'FontWeight', 'bold', ...
                        'FontSize', ResultsAnimationExporter.animation_title_font_size());
                case 'contour'
                    ResultsAnimationExporter.render_contour_field(ax, x_vec, y_vec, omega_plot, [cmin cmax]);
                    colormap(ax, turbo);
                    clim(ax, [cmin cmax]);
                    cb = colorbar(ax);
                    ResultsAnimationExporter.style_animation_colorbar(cb);
                    title(ax, ResultsAnimationExporter.animation_time_title(t_str), ...
                        'Color', ResultsAnimationExporter.animation_text_color(), 'Interpreter', 'none', ...
                        'FontSize', ResultsAnimationExporter.animation_title_font_size(), 'FontWeight', 'bold');
                case 'vector'
                    [u, v] = ResultsAnimationExporter.resolve_velocity_slice(omega_slice, u_slice, v_slice, x_vec, y_vec);
                    if ~isempty(plot_mask) && isequal(size(plot_mask), size(u))
                        u(~plot_mask) = 0;
                        v(~plot_mask) = 0;
                    end
                    quiver(ax, Xg(1:skip:end,1:skip:end), Yg(1:skip:end,1:skip:end), ...
                        u(1:skip:end,1:skip:end), v(1:skip:end,1:skip:end), 1.4, ...
                        'Color', plot_colors.primary, 'LineWidth', 1.0);
                    title(ax, ResultsAnimationExporter.animation_time_title(t_str), ...
                        'Color', ResultsAnimationExporter.animation_text_color(), 'Interpreter', 'none', ...
                        'FontSize', ResultsAnimationExporter.animation_title_font_size(), 'FontWeight', 'bold');
                case 'streamline'
                    [u, v] = ResultsAnimationExporter.resolve_velocity_slice(omega_slice, u_slice, v_slice, x_vec, y_vec);
                    if ~isempty(plot_mask) && isequal(size(plot_mask), size(u))
                        u(~plot_mask) = 0;
                        v(~plot_mask) = 0;
                    end
                    ax.Color = plot_colors.bg;
                    ResultsPlotDispatcher.render_deterministic_streamlines(ax, x_vec, y_vec, u, v, plot_colors.fg, ...
                        struct('fallback_skip', skip, 'line_width', 1.0));
                    title(ax, ResultsAnimationExporter.animation_time_title(t_str), ...
                        'Color', ResultsAnimationExporter.animation_text_color(), 'Interpreter', 'none', ...
                        'FontSize', ResultsAnimationExporter.animation_title_font_size(), 'FontWeight', 'bold');
                case 'streamfunction'
                    psi = ResultsAnimationExporter.resolve_streamfunction_slice(omega_slice, psi_slice, x_vec, y_vec);
                    psi_plot = ResultsAnimationExporter.apply_plot_mask(psi, plot_mask);
                    psi_finite = psi_plot(isfinite(psi_plot));
                    pmin = -1;
                    pmax = 1;
                    if ~isempty(psi_finite)
                        pmin = min(psi_finite);
                        pmax = max(psi_finite);
                        if ~isfinite(pmin) || ~isfinite(pmax) || pmax <= pmin
                            pmin = -1;
                            pmax = 1;
                        end
                    end
                    ResultsAnimationExporter.render_scalar_field(ax, x_vec, y_vec, psi_plot, [pmin pmax]);
                    colormap(ax, turbo);
                    clim(ax, [pmin pmax]);
                    cb = colorbar(ax);
                    ResultsAnimationExporter.style_animation_colorbar(cb);
                    title(ax, ResultsAnimationExporter.animation_time_title(t_str), ...
                        'Color', ResultsAnimationExporter.animation_text_color(), 'Interpreter', 'none', ...
                        'FontSize', ResultsAnimationExporter.animation_title_font_size(), 'FontWeight', 'bold');
                case 'speed'
                    [u, v] = ResultsAnimationExporter.resolve_velocity_slice(omega_slice, u_slice, v_slice, x_vec, y_vec);
                    if ~isempty(plot_mask) && isequal(size(plot_mask), size(u))
                        u(~plot_mask) = 0;
                        v(~plot_mask) = 0;
                    end
                    speed_slice = hypot(u, v);
                    speed_plot = ResultsAnimationExporter.apply_plot_mask(speed_slice, plot_mask);
                    speed_finite = speed_plot(isfinite(speed_plot));
                    smax = 1;
                    if ~isempty(speed_finite)
                        smax = max(speed_finite);
                        if ~isfinite(smax) || smax <= 0
                            smax = 1;
                        end
                    end
                    h = imagesc(ax, x_vec, y_vec, speed_plot);
                    set(h, 'AlphaData', double(isfinite(speed_plot)));
                    colormap(ax, turbo);
                    clim(ax, [0 smax]);
                    cb = colorbar(ax);
                    ResultsAnimationExporter.style_animation_colorbar(cb);
                    title(ax, ResultsAnimationExporter.animation_time_title(t_str), ...
                        'Color', ResultsAnimationExporter.animation_text_color(), 'Interpreter', 'none', ...
                        'FontSize', ResultsAnimationExporter.animation_title_font_size(), 'FontWeight', 'bold');
                case 'wall_omega'
                    wall_plot = ResultsAnimationExporter.apply_plot_mask(wall_slice, plot_mask);
                    h = imagesc(ax, x_vec, y_vec, wall_plot);
                    set(h, 'AlphaData', double(isfinite(wall_plot)));
                    colormap(ax, turbo);
                    clim(ax, [cmin cmax]);
                    cb = colorbar(ax);
                    ResultsAnimationExporter.style_animation_colorbar(cb);
                    title(ax, ResultsAnimationExporter.animation_time_title(t_str), ...
                        'Color', ResultsAnimationExporter.animation_text_color(), 'Interpreter', 'none', ...
                        'FontSize', ResultsAnimationExporter.animation_title_font_size(), 'FontWeight', 'bold');
            end

            axis(ax, 'equal');
            axis(ax, 'tight');
            set(ax, 'YDir', 'normal');
            xlabel(ax, '');
            ylabel(ax, '');
            ax.XTick = [];
            ax.YTick = [];
            ax.TickLength = [0 0];
            grid(ax, 'off');
            ax.Color = plot_colors.bg;
            ax.XColor = ResultsAnimationExporter.animation_text_color();
            ax.YColor = ResultsAnimationExporter.animation_text_color();
            ax.GridColor = plot_colors.grid;
            ax.GridAlpha = 0.25;
            box(ax, 'on');
            if isprop(ax, 'LooseInset')
                ax.LooseInset = [0 0 0 0];
            end
            try
                ax.Position = ResultsAnimationExporter.single_pane_axes_position();
            catch
            end
            ResultsAnimationExporter.style_colorbar_to_axes(cb, ax);
            ResultsAnimationExporter.draw_bathymetry_outline(ax, bathy_profile_x, bathy_profile_y, plot_colors.fg);
        end

        function rgb_frame = capture_rgb_frame(fig, media)
            rgb_frame = [];
            try
                drawnow limitrate;
                captured = getframe(fig);
                if isstruct(captured) && isfield(captured, 'cdata')
                    rgb_frame = captured.cdata;
                end
            catch
                rgb_frame = [];
            end
            if ~ResultsAnimationExporter.is_valid_rgb_frame(rgb_frame)
                frame_path = [tempname, '.png'];
                cleanup_file = onCleanup(@() ResultsAnimationExporter.safe_delete(frame_path)); %#ok<NASGU>
                exportgraphics(fig, frame_path, 'Resolution', round(media.dpi), 'BackgroundColor', 'white');
                rgb_frame = imread(frame_path);
            end
            if ~ResultsAnimationExporter.is_valid_rgb_frame(rgb_frame)
                error('ResultsAnimationExporter:FrameCaptureFailed', ...
                    'Animation export did not capture a valid RGB frame.');
            end
            target_size = [round(media.resolution_px(2)), round(media.resolution_px(1))];
            if size(rgb_frame, 1) ~= target_size(1) || size(rgb_frame, 2) ~= target_size(2)
                rgb_frame = imresize(rgb_frame, target_size, 'bilinear');
            end
        end

        function [omega_out, times_out] = resample_snapshot_cube(omega_cube, snap_times, target_count, time_span)
            omega_out = omega_cube;
            times_out = snap_times;
            if isempty(omega_cube) || ismatrix(omega_cube)
                return;
            end
            target_count = max(1, round(double(target_count)));
            n_src = size(omega_cube, 3);
            if target_count <= n_src
                if isempty(times_out) || numel(times_out) ~= n_src
                    times_out = linspace(0, time_span, n_src);
                else
                    times_out = double(times_out(:)).';
                end
                return;
            end
            if n_src == 1
                omega_out = repmat(omega_cube, 1, 1, target_count);
                times_out = linspace(0, time_span, target_count);
                return;
            end
            src_t = linspace(0, 1, n_src);
            tgt_t = linspace(0, 1, target_count);
            omega_flat = reshape(double(omega_cube), [], n_src).';
            omega_interp = interp1(src_t, omega_flat, tgt_t, 'linear');
            omega_out = reshape(omega_interp.', size(omega_cube, 1), size(omega_cube, 2), target_count);
            if isempty(snap_times) || numel(snap_times) ~= n_src
                times_out = linspace(0, time_span, target_count);
            else
                times_out = interp1(src_t, double(snap_times(:)).', tgt_t, 'linear');
            end
        end

        function span = resolve_time_span(params, analysis, raw_snap_times, n_snaps)
            span = NaN;
            if ~isempty(raw_snap_times)
                vals = double(raw_snap_times(:));
                vals = vals(isfinite(vals));
                if ~isempty(vals)
                    span = max(vals) - min(vals);
                    if span <= 0
                        span = max(vals);
                    end
                end
            end
            if (~isfinite(span) || span <= 0) && isfield(analysis, 'time_vec') && ~isempty(analysis.time_vec)
                vals = double(analysis.time_vec(:));
                vals = vals(isfinite(vals));
                if ~isempty(vals)
                    span = max(vals) - min(vals);
                    if span <= 0
                        span = max(vals);
                    end
                end
            end
            if (~isfinite(span) || span <= 0) && isfield(params, 'Tfinal')
                span = double(params.Tfinal);
            end
            if ~isfinite(span) || span <= 0
                span = max(double(n_snaps - 1), 1.0);
            end
        end

        function cube = extract_matching_cube(analysis, field_name, target_size)
            cube = [];
            if ~isfield(analysis, field_name) || isempty(analysis.(field_name))
                return;
            end
            if isequal(size(analysis.(field_name)), target_size)
                cube = double(analysis.(field_name));
            end
        end

        function [u_cube, v_cube] = extract_velocity_cubes(analysis, omega_size, plot_context)
            u_cube = [];
            v_cube = [];
            if ~isfield(analysis, 'u_snaps') || ~isfield(analysis, 'v_snaps') || isempty(analysis.u_snaps) || isempty(analysis.v_snaps)
                if isfield(plot_context, 'requires_velocity_snapshots') && logical(plot_context.requires_velocity_snapshots)
                    error('ResultsAnimationExporter:MissingVelocitySnapshotsForWallDomain', ...
                        ['FD wall-domain animation export requires solver velocity snapshots; ' ...
                        'periodic FFT reconstruction is not allowed for this path.']);
                end
                return;
            end
            if isequal(size(analysis.u_snaps), omega_size) && isequal(size(analysis.v_snaps), omega_size)
                u_cube = double(analysis.u_snaps);
                v_cube = double(analysis.v_snaps);
            end
        end

        function [u, v] = resolve_velocity_slice(omega_slice, u_slice, v_slice, x_vec, y_vec)
            if ~isempty(u_slice) && ~isempty(v_slice) && isequal(size(u_slice), size(omega_slice)) && isequal(size(v_slice), size(omega_slice))
                u = double(u_slice);
                v = double(v_slice);
                u(~isfinite(u)) = 0;
                v(~isfinite(v)) = 0;
                return;
            end
            [~, u, v] = ResultsAnimationExporter.velocity_from_omega_slice(omega_slice, x_vec, y_vec);
        end

        function psi = resolve_streamfunction_slice(omega_slice, psi_slice, x_vec, y_vec)
            if ~isempty(psi_slice) && isequal(size(psi_slice), size(omega_slice))
                psi = double(psi_slice);
                psi(~isfinite(psi)) = 0;
                return;
            end
            [psi, ~, ~] = ResultsAnimationExporter.velocity_from_omega_slice(omega_slice, x_vec, y_vec);
            psi(~isfinite(psi)) = 0;
        end

        function [psi, u, v] = velocity_from_omega_slice(omega_slice, x_vec, y_vec)
            omega_slice = double(omega_slice);
            omega_slice(~isfinite(omega_slice)) = 0;
            ny = size(omega_slice, 1);
            nx = size(omega_slice, 2);
            dx = max(mean(diff(double(x_vec))), eps);
            dy = max(mean(diff(double(y_vec))), eps);
            omega0 = omega_slice - mean(omega_slice(:));
            omega_hat = fft2(omega0);
            kx = (2*pi/(nx*dx)) * [0:floor(nx/2), -floor((nx-1)/2):-1];
            ky = (2*pi/(ny*dy)) * [0:floor(ny/2), -floor((ny-1)/2):-1];
            [KX, KY] = meshgrid(kx, ky);
            k2 = KX.^2 + KY.^2;
            psi_hat = zeros(size(omega_hat));
            mask = k2 > 0;
            psi_hat(mask) = -omega_hat(mask) ./ k2(mask);
            psi = real(ifft2(psi_hat));
            % Repo streamfunction convention: u=-dpsi/dy, v=dpsi/dx.
            u = -real(ifft2(1i * KY .* psi_hat));
            v = real(ifft2(1i * KX .* psi_hat));
        end

        function tf = overlay_streamfunction_contours(ax, x_vec, y_vec, psi_slice, line_color)
            tf = false;
            psi_finite = psi_slice(isfinite(psi_slice));
            if isempty(psi_finite)
                return;
            end
            psi_span = max(psi_finite) - min(psi_finite);
            psi_scale = max(1.0, max(abs(psi_finite)));
            if ~isfinite(psi_span) || psi_span <= 1.0e-8 * psi_scale
                return;
            end
            try
                contour(ax, x_vec, y_vec, psi_slice, 12, 'LineColor', line_color, 'LineWidth', 0.9);
                tf = true;
            catch
                tf = false;
            end
        end

        function render_scalar_field(ax, x_vec, y_vec, z_slice, color_limits)
            if ResultsAnimationExporter.field_has_variation(z_slice)
                contourf(ax, x_vec, y_vec, z_slice, ResultsAnimationExporter.default_contour_levels(), 'LineStyle', 'none');
            else
                h = imagesc(ax, x_vec, y_vec, z_slice);
                set(h, 'AlphaData', double(isfinite(z_slice)));
                set(ax, 'YDir', 'normal');
            end
            if nargin >= 5 && isnumeric(color_limits) && numel(color_limits) >= 2
                limits = double(color_limits(1:2));
                if all(isfinite(limits)) && limits(2) > limits(1)
                    clim(ax, limits);
                end
            end
        end

        function render_contour_field(ax, x_vec, y_vec, z_slice, color_limits)
            if ResultsAnimationExporter.field_has_variation(z_slice)
                warn1 = warning('query', 'MATLAB:contour:ConstantZData');
                warn2 = warning('query', 'MATLAB:contourf:ConstantZData');
                warn3 = warning('query', 'MATLAB:contour:ConstantData');
                warn4 = warning('query', 'MATLAB:contourf:ConstantData');
                cleanup_obj = onCleanup(@() ResultsAnimationExporter.restore_contour_warning_state(warn1, warn2, warn3, warn4)); %#ok<NASGU>
                warning('off', 'MATLAB:contour:ConstantZData');
                warning('off', 'MATLAB:contourf:ConstantZData');
                warning('off', 'MATLAB:contour:ConstantData');
                warning('off', 'MATLAB:contourf:ConstantData');
                contour_levels = ResultsAnimationExporter.default_contour_levels();
                contourf(ax, x_vec, y_vec, z_slice, contour_levels, 'LineStyle', 'none');
                hold(ax, 'on');
                contour(ax, x_vec, y_vec, z_slice, max(10, round(contour_levels / 3)), ...
                    'LineWidth', 0.35, 'LineColor', [0.08 0.10 0.13]);
                hold(ax, 'off');
            else
                h = imagesc(ax, x_vec, y_vec, z_slice);
                set(h, 'AlphaData', double(isfinite(z_slice)));
                set(ax, 'YDir', 'normal');
            end
            if nargin >= 5 && isnumeric(color_limits) && numel(color_limits) >= 2
                limits = double(color_limits(1:2));
                if all(isfinite(limits)) && limits(2) > limits(1)
                    clim(ax, limits);
                end
            end
        end

        function restore_contour_warning_state(warn1, warn2, warn3, warn4)
            warning(warn1.state, 'MATLAB:contour:ConstantZData');
            warning(warn2.state, 'MATLAB:contourf:ConstantZData');
            warning(warn3.state, 'MATLAB:contour:ConstantData');
            warning(warn4.state, 'MATLAB:contourf:ConstantData');
        end

        function tf = field_has_variation(z_slice)
            tf = false;
            if isempty(z_slice)
                return;
            end
            finite_values = z_slice(isfinite(z_slice));
            if isempty(finite_values)
                return;
            end
            value_span = max(finite_values) - min(finite_values);
            value_scale = max(1.0, max(abs(finite_values)));
            tf = isfinite(value_span) && value_span > 1.0e-8 * value_scale;
        end

        function [cmin, cmax] = compute_limits(data_slice, plot_mask)
            scaling = resolve_result_plot_scaling(data_slice, plot_mask, struct());
            cmin = scaling.cmin;
            cmax = scaling.cmax;
        end

        function masked = apply_plot_mask(data, plot_mask)
            masked = double(data);
            if ~isempty(plot_mask) && isequal(size(masked), size(plot_mask))
                masked(~plot_mask) = NaN;
            end
        end

        function font_size = animation_title_font_size()
            font_size = 12;
        end

        function color_value = animation_text_color()
            color_value = [0 0 0];
        end

        function font_size = animation_colorbar_font_size()
            font_size = 11;
        end

        function text_value = animation_time_title(t_str)
            text_value = sprintf('t = %s s', char(string(t_str)));
        end

        function levels = default_contour_levels()
            persistent cached_levels;
            if isempty(cached_levels)
                cached_levels = 36;
                try
                    defaults = create_default_parameters();
                    if isfield(defaults, 'phase2') && isstruct(defaults.phase2) && ...
                            isfield(defaults.phase2, 'contour_levels') && isnumeric(defaults.phase2.contour_levels)
                        cached_levels = max(8, round(double(defaults.phase2.contour_levels)));
                    end
                catch
                    cached_levels = 36;
                end
            end
            levels = cached_levels;
        end

        function style_colorbar_to_axes(cb, ax)
            try
                drawnow limitrate;
                if isempty(cb) || ~isgraphics(cb) || isempty(ax) || ~isgraphics(ax)
                    return;
                end
                if isprop(cb, 'Location')
                    cb.Location = 'manual';
                end
                plot_pos = ResultsAnimationExporter.visible_plot_position(ax);
                if isempty(plot_pos) || numel(plot_pos) < 4 || any(~isfinite(plot_pos))
                    plot_pos = ax.Position;
                end
                cb_pos = cb.Position;
                gap = 0.006;
                cb_width = 0.018;
                cb_pos(1) = min(0.972 - cb_width, plot_pos(1) + plot_pos(3) + gap);
                cb_pos(2) = plot_pos(2);
                cb_pos(3) = cb_width;
                cb_pos(4) = plot_pos(4);
                cb.Position = cb_pos;
            catch
            end
        end

        function pos = single_pane_axes_position()
            pos = [0.060 0.075 0.685 0.84];
        end

        function plot_pos = visible_plot_position(ax)
            plot_pos = [];
            try
                if isempty(ax) || ~isgraphics(ax)
                    return;
                end
                fig = ancestor(ax, 'figure');
                if isempty(fig) || ~isgraphics(fig)
                    return;
                end

                original_ax_units = ax.Units;
                original_fig_units = fig.Units;
                cleanup_units = onCleanup(@() ResultsAnimationExporter.restore_units(ax, fig, original_ax_units, original_fig_units)); %#ok<NASGU>
                ax.Units = 'pixels';
                fig.Units = 'pixels';
                ax_pos_px = ax.Position;
                fig_pos_px = fig.Position;
                if numel(ax_pos_px) < 4 || numel(fig_pos_px) < 4 || fig_pos_px(3) <= 0 || fig_pos_px(4) <= 0
                    return;
                end

                x_limits = xlim(ax);
                y_limits = ylim(ax);
                data_w = abs(diff(double(x_limits)));
                data_h = abs(diff(double(y_limits)));
                if ~(isfinite(data_w) && isfinite(data_h) && data_w > 0 && data_h > 0)
                    return;
                end

                axes_ratio = ax_pos_px(3) / max(ax_pos_px(4), eps);
                data_ratio = data_w / max(data_h, eps);
                if axes_ratio > data_ratio
                    plot_h = ax_pos_px(4);
                    plot_w = plot_h * data_ratio;
                    inset_x = 0.5 * (ax_pos_px(3) - plot_w);
                    inset_y = 0;
                else
                    plot_w = ax_pos_px(3);
                    plot_h = plot_w / max(data_ratio, eps);
                    inset_x = 0;
                    inset_y = 0.5 * (ax_pos_px(4) - plot_h);
                end

                plot_pos_px = [ax_pos_px(1) + inset_x, ax_pos_px(2) + inset_y, plot_w, plot_h];
                plot_pos = [ ...
                    plot_pos_px(1) / fig_pos_px(3), ...
                    plot_pos_px(2) / fig_pos_px(4), ...
                    plot_pos_px(3) / fig_pos_px(3), ...
                    plot_pos_px(4) / fig_pos_px(4)];
            catch
                plot_pos = [];
            end
        end

        function restore_units(ax, fig, ax_units, fig_units)
            try
                if ~isempty(ax) && isgraphics(ax)
                    ax.Units = ax_units;
                end
            catch
            end
            try
                if ~isempty(fig) && isgraphics(fig)
                    fig.Units = fig_units;
                end
            catch
            end
        end

        function style_animation_colorbar(cb)
            if isempty(cb) || ~isgraphics(cb)
                return;
            end
            if isprop(cb, 'Color')
                cb.Color = ResultsAnimationExporter.animation_text_color();
            end
            if isprop(cb, 'FontSize')
                cb.FontSize = max(ResultsAnimationExporter.animation_colorbar_font_size(), cb.FontSize);
            end
        end

        function draw_bathymetry_outline(ax, profile_x, profile_y, line_color)
            if isempty(profile_x) || isempty(profile_y) || numel(profile_x) ~= numel(profile_y)
                return;
            end
            hold(ax, 'on');
            plot(ax, double(profile_x(:)).', double(profile_y(:)).', 'LineWidth', 1.4, 'LineStyle', '-', 'Color', line_color);
            hold(ax, 'off');
        end

        function out = safe_field(s, field_name, default)
            out = default;
            if isstruct(s) && isfield(s, field_name)
                out = s.(field_name);
            end
        end

        function value = pick_numeric(s, field_name, default)
            value = default;
            if isstruct(s) && isfield(s, field_name)
                raw = double(s.(field_name));
                if isscalar(raw) && isfinite(raw)
                    value = raw;
                end
            end
        end

        function merged = overlay(base, incoming)
            merged = base;
            if ~isstruct(incoming)
                return;
            end
            fields = fieldnames(incoming);
            for k = 1:numel(fields)
                merged.(fields{k}) = incoming.(fields{k});
            end
        end

        function slice = extract_cube_snapshot(cube, idx)
            slice = [];
            if isempty(cube) || ismatrix(cube)
                return;
            end
            idx = max(1, min(size(cube, 3), round(double(idx))));
            slice = double(cube(:, :, idx));
        end

        function txt = time_text(display_times, idx)
            if ~isempty(display_times) && numel(display_times) >= idx
                txt = sprintf('%.3g', display_times(idx));
            else
                txt = sprintf('%d', idx);
            end
        end

        function token = run_token(run_cfg, params, paths)
            if nargin >= 3 && isstruct(paths) && isfield(paths, 'animation_base_stem') && ~isempty(paths.animation_base_stem)
                token = char(string(paths.animation_base_stem));
            elseif nargin >= 3 && isstruct(paths) && isfield(paths, 'export_file_stem') && ~isempty(paths.export_file_stem)
                token = char(string(paths.export_file_stem));
            elseif nargin >= 3 && isstruct(paths) && isfield(paths, 'storage_id') && ~isempty(paths.storage_id)
                token = char(string(paths.storage_id));
            elseif isfield(run_cfg, 'storage_id') && ~isempty(run_cfg.storage_id)
                token = char(string(run_cfg.storage_id));
            elseif isfield(params, 'storage_id') && ~isempty(params.storage_id)
                token = char(string(params.storage_id));
            elseif isfield(run_cfg, 'run_id') && ~isempty(run_cfg.run_id)
                token = char(string(run_cfg.run_id));
            elseif isfield(params, 'run_id') && ~isempty(params.run_id)
                token = char(string(params.run_id));
            elseif isfield(run_cfg, 'mode') && ~isempty(run_cfg.mode)
                token = sprintf('%s_%s', char(string(run_cfg.mode)), char(datetime('now', 'Format', 'yyyyMMdd_HHmmss')));
            else
                token = char(datetime('now', 'Format', 'yyyyMMdd_HHmmss'));
            end
            token = regexprep(token, '[^a-zA-Z0-9_-]', '_');
        end

        function safe_close(fig_handle)
            if ~isempty(fig_handle) && isgraphics(fig_handle, 'figure')
                close(fig_handle);
            end
        end

        function safe_close_writer(writer)
            try
                evalc('close(writer);');
            catch
            end
        end

        function safe_delete(file_path)
            if isempty(file_path)
                return;
            end
            if exist(file_path, 'file') == 2
                delete(file_path);
            end
        end

        function assert_media_frames_written(frame_count, output_path, media_label)
            if isempty(output_path)
                return;
            end
            if ~isfinite(frame_count) || frame_count < 1
                error('ResultsAnimationExporter:NoFramesWritten', ...
                    'No valid %s frames were written for %s.', media_label, output_path);
            end
        end

        function tf = is_valid_rgb_frame(rgb_frame)
            tf = isnumeric(rgb_frame) && ndims(rgb_frame) >= 2 && ...
                ~isempty(rgb_frame) && size(rgb_frame, 1) >= 1 && size(rgb_frame, 2) >= 1;
        end

        function staged_path = stage_mp4_output_path(output_path)
            staged_path = ResultsAnimationExporter.stage_media_output_path(output_path, '.mp4', 'tsunami_video_writer_stage');
        end

        function staged_path = stage_gif_output_path(output_path)
            staged_path = ResultsAnimationExporter.stage_media_output_path(output_path, '.gif', 'tsunami_gif_writer_stage');
        end

        function staged_path = stage_media_output_path(output_path, extension, stage_folder)
            staged_path = '';
            output_path = char(string(output_path));
            if numel(output_path) < 240
                return;
            end

            stage_dir = fullfile(tempdir, stage_folder);
            if ~exist(stage_dir, 'dir')
                mkdir(stage_dir);
            end
            staged_path = [tempname(stage_dir), extension];
        end

        function finalize_staged_mp4(staged_path, output_path)
            ResultsAnimationExporter.finalize_staged_media(staged_path, output_path, 'MP4');
        end

        function finalize_staged_gif(staged_path, output_path)
            ResultsAnimationExporter.finalize_staged_media(staged_path, output_path, 'GIF');
        end

        function finalize_staged_media(staged_path, output_path, media_label)
            if isempty(staged_path)
                return;
            end
            staged_path = char(string(staged_path));
            output_path = char(string(output_path));
            if exist(staged_path, 'file') ~= 2
                error('ResultsAnimationExporter:MissingStagedMedia', ...
                    'Staged %s was not created for target %s.', media_label, output_path);
            end
            output_dir = fileparts(output_path);
            if ~isempty(output_dir) && ~exist(output_dir, 'dir')
                mkdir(output_dir);
            end
            [ok, msg] = movefile(staged_path, output_path, 'f');
            if ~ok
                error('ResultsAnimationExporter:StageMoveFailed', ...
                    'Could not move staged %s into %s: %s', media_label, output_path, msg);
            end
        end

        function status = empty_media_status_record(output_path, media, format_token)
            if nargin < 1 || isempty(output_path)
                output_path = '';
            end
            if nargin < 2 || ~isstruct(media)
                media = struct();
            end
            if nargin < 3
                format_token = '';
            end
            status = struct( ...
                'format', char(string(format_token)), ...
                'status', 'pending', ...
                'output_path', char(string(output_path)), ...
                'staged_output_path', '', ...
                'validated_output_path', '', ...
                'requested_media', media, ...
                'resolved_media', media, ...
                'retry_media', struct(), ...
                'retry_used', false, ...
                'frames_requested', ResultsAnimationExporter.media_frame_count(media), ...
                'frames_written', 0, ...
                'file_bytes', 0, ...
                'failure_identifier', '', ...
                'failure_message', '', ...
                'failure_history', repmat(struct( ...
                    'identifier', '', ...
                    'message', '', ...
                    'timestamp', '', ...
                    'attempt', 0), 1, 0), ...
                'profile_text', ResultsAnimationExporter.media_profile_text(media), ...
                'retry_profile_text', '', ...
                'attempt_count', 0);
            if isempty(strtrim(status.output_path))
                status.status = 'not_requested';
            end
        end

        function status = record_media_failure(status, ME)
            if nargin < 1 || ~isstruct(status)
                status = ResultsAnimationExporter.empty_media_status_record('', struct(), '');
            end
            if nargin < 2 || ~isa(ME, 'MException')
                ME = MException('ResultsAnimationExporter:UnknownMediaFailure', ...
                    'Unknown media export failure.');
            end
            status.status = 'failed';
            status.validated_output_path = '';
            status.file_bytes = 0;
            status.attempt_count = max(1, round(double(ResultsAnimationExporter.safe_field(status, 'attempt_count', 0))));
            status.failure_identifier = char(string(ME.identifier));
            status.failure_message = char(string(ME.message));
            failure_entry = struct( ...
                'identifier', status.failure_identifier, ...
                'message', status.failure_message, ...
                'timestamp', char(datetime('now', 'Format', 'yyyy-MM-dd HH:mm:ss')), ...
                'attempt', double(status.attempt_count));
            if ~isfield(status, 'failure_history') || isempty(status.failure_history)
                status.failure_history = failure_entry;
            else
                status.failure_history(end + 1) = failure_entry; %#ok<AGROW>
            end
        end

        function status = finalize_media_status(status, frame_count, staged_output_path, output_path, media, media_label)
            if nargin < 1 || ~isstruct(status)
                status = ResultsAnimationExporter.empty_media_status_record(output_path, media, media_label);
            end
            if nargin < 6
                media_label = '';
            end
            if nargin < 5 || ~isstruct(media)
                media = struct();
            end
            status.resolved_media = media;
            status.profile_text = ResultsAnimationExporter.media_profile_text(media);
            status.frames_written = max(0, round(double(frame_count)));
            if isempty(strtrim(char(string(output_path))))
                status.status = 'not_requested';
                return;
            end
            if strcmpi(char(string(ResultsAnimationExporter.safe_field(status, 'status', ''))), 'failed')
                ResultsAnimationExporter.safe_delete(staged_output_path);
                ResultsAnimationExporter.safe_delete(output_path);
                return;
            end
            try
                ResultsAnimationExporter.assert_media_frames_written(frame_count, output_path, media_label);
                candidate_path = output_path;
                if ~isempty(strtrim(char(string(staged_output_path))))
                    candidate_path = staged_output_path;
                end
                ResultsAnimationExporter.require_nonzero_file(candidate_path, media_label, output_path);
                if ~isempty(strtrim(char(string(staged_output_path))))
                    switch lower(char(string(media_label)))
                        case 'gif'
                            ResultsAnimationExporter.finalize_staged_gif(staged_output_path, output_path);
                        otherwise
                            ResultsAnimationExporter.finalize_staged_mp4(staged_output_path, output_path);
                    end
                end
                status.file_bytes = ResultsAnimationExporter.require_nonzero_file(output_path, media_label, output_path);
                status.validated_output_path = char(string(output_path));
                status.status = 'saved';
            catch ME
                ResultsAnimationExporter.safe_delete(staged_output_path);
                ResultsAnimationExporter.safe_delete(output_path);
                status = ResultsAnimationExporter.record_media_failure(status, ME);
            end
        end

        function [status, writer] = close_mp4_writer_with_status(status, writer)
            if isempty(writer)
                return;
            end
            if strcmpi(char(string(ResultsAnimationExporter.safe_field(status, 'status', ''))), 'failed')
                ResultsAnimationExporter.safe_close_writer(writer);
                writer = [];
                return;
            end
            try
                close(writer);
            catch ME
                status = ResultsAnimationExporter.record_media_failure(status, ME);
            end
            writer = [];
        end

        function tf = should_retry_mp4(status, media)
            tf = false;
            if ~(isstruct(status) && isstruct(media) && ResultsAnimationExporter.media_has_format(media, 'mp4'))
                return;
            end
            if isempty(strtrim(char(string(ResultsAnimationExporter.safe_field(status, 'output_path', '')))))
                return;
            end
            if logical(ResultsAnimationExporter.safe_field(status, 'retry_used', false))
                return;
            end
            if ~strcmpi(char(string(ResultsAnimationExporter.safe_field(status, 'status', ''))), 'failed')
                return;
            end
            failure_identifier = lower(char(string(ResultsAnimationExporter.safe_field(status, 'failure_identifier', ''))));
            if contains(failure_identifier, 'mp4unavailable')
                return;
            end
            tf = true;
        end

        function tf = media_flag(media, field_name, default_value)
            tf = logical(default_value);
            if ~isstruct(media) || ~isfield(media, field_name) || isempty(media.(field_name))
                return;
            end
            tf = logical(media.(field_name));
        end

        function retry_media = safe_mp4_retry_media(media)
            retry_media = media;
            if nargin < 1 || ~isstruct(retry_media)
                retry_media = struct();
            end
            retry_media.format = 'mp4';
            retry_media.formats = {'mp4'};
            retry_media.frame_count = max(2, round(double(ResultsAnimationExporter.media_frame_count(retry_media))));
            retry_media.duration_s = max(0.1, double(ResultsAnimationExporter.safe_field(retry_media, 'duration_s', 10)));
            retry_media.fps = min(max(double(retry_media.frame_count) / retry_media.duration_s, 1), 60);
            retry_media.quality = 75;
            retry_media.dpi = max(72, double(ResultsAnimationExporter.safe_field(retry_media, 'dpi', 144)));
            retry_media.resolution_px = ResultsAnimationExporter.encoder_safe_resolution( ...
                ResultsAnimationExporter.safe_field(retry_media, 'resolution_px', [640, 480]), [1920, 1440]);
            retry_media.width_in = retry_media.resolution_px(1) / retry_media.dpi;
            retry_media.height_in = retry_media.resolution_px(2) / retry_media.dpi;
        end

        function status = retry_combined_mp4(prior_status, output_path, pane_specs, omega_cube, psi_cube, u_cube, v_cube, wall_cube, ...
                x_vec, y_vec, display_times, plot_mask, wall_mask, bathy_profile_x, bathy_profile_y, plot_labels, main_title, retry_media)
            status = prior_status;
            if ~isstruct(status) || isempty(fieldnames(status))
                status = ResultsAnimationExporter.empty_media_status_record(output_path, retry_media, 'mp4');
            end
            status.retry_used = true;
            status.retry_media = retry_media;
            status.retry_profile_text = ResultsAnimationExporter.media_profile_text(retry_media);
            status.resolved_media = retry_media;
            status.profile_text = ResultsAnimationExporter.media_profile_text(retry_media);
            status.attempt_count = max(1, round(double(ResultsAnimationExporter.safe_field(status, 'attempt_count', 0)))) + 1;
            status.status = 'retrying';
            status.validated_output_path = '';
            status.file_bytes = 0;
            ResultsAnimationExporter.safe_delete(output_path);

            [fig, tl, axes_list] = ResultsAnimationExporter.create_combined_figure(numel(pane_specs), retry_media);
            cleanup_fig = onCleanup(@() ResultsAnimationExporter.safe_close(fig)); %#ok<NASGU>
            ResultsPlotDispatcher.apply_tiled_annotations(tl, main_title, 'x', 'y', ResultsPlotDispatcher.default_light_colors());
            writer = [];
            staged_mp4_path = '';
            try
                [writer, staged_mp4_path] = ResultsAnimationExporter.open_mp4_writer(output_path, retry_media);
                status.staged_output_path = staged_mp4_path;
                status.status = 'running';
                cleanup_writer = onCleanup(@() ResultsAnimationExporter.safe_close_writer(writer)); %#ok<NASGU>
                cleanup_staged = onCleanup(@() ResultsAnimationExporter.safe_delete(staged_mp4_path)); %#ok<NASGU>
                mp4_frames_written = 0;
                for idx = 1:size(omega_cube, 3)
                    ResultsAnimationExporter.render_combined_frame(axes_list, pane_specs, ...
                        omega_cube(:, :, idx), ResultsAnimationExporter.extract_cube_snapshot(psi_cube, idx), ...
                        ResultsAnimationExporter.extract_cube_snapshot(u_cube, idx), ResultsAnimationExporter.extract_cube_snapshot(v_cube, idx), ...
                        ResultsAnimationExporter.extract_cube_snapshot(wall_cube, idx), x_vec, y_vec, ...
                        ResultsAnimationExporter.time_text(display_times, idx), plot_mask, wall_mask, bathy_profile_x, bathy_profile_y, plot_labels);
                    rgb_frame = ResultsAnimationExporter.capture_rgb_frame(fig, retry_media);
                    writeVideo(writer, rgb_frame);
                    mp4_frames_written = mp4_frames_written + 1;
                end
                close(writer);
                writer = [];
                status = ResultsAnimationExporter.finalize_media_status( ...
                    status, mp4_frames_written, staged_mp4_path, output_path, retry_media, 'mp4');
            catch ME
                ResultsAnimationExporter.safe_close_writer(writer);
                ResultsAnimationExporter.safe_delete(staged_mp4_path);
                ResultsAnimationExporter.safe_delete(output_path);
                status = ResultsAnimationExporter.record_media_failure(status, ME);
            end
        end

        function status = retry_pane_mp4(prior_status, output_path, spec, omega_cube, psi_cube, u_cube, v_cube, wall_cube, ...
                x_vec, y_vec, display_times, plot_mask, wall_mask, bathy_profile_x, bathy_profile_y, plot_labels, main_title, retry_media)
            status = prior_status;
            if ~isstruct(status) || isempty(fieldnames(status))
                status = ResultsAnimationExporter.empty_media_status_record(output_path, retry_media, 'mp4');
            end
            status.retry_used = true;
            status.retry_media = retry_media;
            status.retry_profile_text = ResultsAnimationExporter.media_profile_text(retry_media);
            status.resolved_media = retry_media;
            status.profile_text = ResultsAnimationExporter.media_profile_text(retry_media);
            status.attempt_count = max(1, round(double(ResultsAnimationExporter.safe_field(status, 'attempt_count', 0)))) + 1;
            status.status = 'retrying';
            status.validated_output_path = '';
            status.file_bytes = 0;
            ResultsAnimationExporter.safe_delete(output_path);

            [fig, ax] = ResultsAnimationExporter.create_single_pane_figure(retry_media);
            cleanup_fig = onCleanup(@() ResultsAnimationExporter.safe_close(fig)); %#ok<NASGU>
            writer = [];
            staged_mp4_path = '';
            try
                [writer, staged_mp4_path] = ResultsAnimationExporter.open_mp4_writer(output_path, retry_media);
                status.staged_output_path = staged_mp4_path;
                status.status = 'running';
                cleanup_writer = onCleanup(@() ResultsAnimationExporter.safe_close_writer(writer)); %#ok<NASGU>
                cleanup_staged = onCleanup(@() ResultsAnimationExporter.safe_delete(staged_mp4_path)); %#ok<NASGU>
                mp4_frames_written = 0;
                for idx = 1:size(omega_cube, 3)
                    ResultsAnimationExporter.render_single_pane(ax, spec.mode, omega_cube(:, :, idx), ...
                        ResultsAnimationExporter.extract_cube_snapshot(psi_cube, idx), ...
                        ResultsAnimationExporter.extract_cube_snapshot(u_cube, idx), ResultsAnimationExporter.extract_cube_snapshot(v_cube, idx), ...
                        ResultsAnimationExporter.extract_cube_snapshot(wall_cube, idx), ...
                        x_vec, y_vec, ResultsAnimationExporter.time_text(display_times, idx), plot_mask, wall_mask, ...
                        bathy_profile_x, bathy_profile_y, plot_labels);
                    rgb_frame = ResultsAnimationExporter.capture_rgb_frame(fig, retry_media);
                    writeVideo(writer, rgb_frame);
                    mp4_frames_written = mp4_frames_written + 1;
                end
                close(writer);
                writer = [];
                status = ResultsAnimationExporter.finalize_media_status( ...
                    status, mp4_frames_written, staged_mp4_path, output_path, retry_media, 'mp4');
            catch ME
                ResultsAnimationExporter.safe_close_writer(writer);
                ResultsAnimationExporter.safe_delete(staged_mp4_path);
                ResultsAnimationExporter.safe_delete(output_path);
                status = ResultsAnimationExporter.record_media_failure(status, ME);
            end
        end

        function status = retry_quad_mp4(prior_status, output_path, omega_cube, psi_cube, u_cube, v_cube, wall_cube, ...
                x_vec, y_vec, display_times, plot_mask, wall_mask, bathy_profile_x, bathy_profile_y, plot_labels, retry_media)
            status = prior_status;
            if ~isstruct(status) || isempty(fieldnames(status))
                status = ResultsAnimationExporter.empty_media_status_record(output_path, retry_media, 'mp4');
            end
            status.retry_used = true;
            status.retry_media = retry_media;
            status.retry_profile_text = ResultsAnimationExporter.media_profile_text(retry_media);
            status.resolved_media = retry_media;
            status.profile_text = ResultsAnimationExporter.media_profile_text(retry_media);
            status.attempt_count = max(1, round(double(ResultsAnimationExporter.safe_field(status, 'attempt_count', 0)))) + 1;
            status.status = 'retrying';
            status.validated_output_path = '';
            status.file_bytes = 0;
            ResultsAnimationExporter.safe_delete(output_path);

            fig = figure('Visible', 'off', 'HandleVisibility', 'off', 'Color', [1 1 1], ...
                'MenuBar', 'none', 'ToolBar', 'none', 'Units', 'inches', ...
                'Position', [0.5 0.5 retry_media.width_in retry_media.height_in], 'PaperPositionMode', 'auto');
            cleanup_fig = onCleanup(@() ResultsAnimationExporter.safe_close(fig)); %#ok<NASGU>
            tl = tiledlayout(fig, 2, 2, 'Padding', 'compact', 'TileSpacing', 'compact');
            axes_list = gobjects(1, 4);
            for i = 1:4
                axes_list(i) = nexttile(tl, i);
            end
            title(tl, 'IC Study Quad Animation', 'Interpreter', 'none');
            writer = [];
            staged_mp4_path = '';
            try
                [writer, staged_mp4_path] = ResultsAnimationExporter.open_mp4_writer(output_path, retry_media);
                status.staged_output_path = staged_mp4_path;
                status.status = 'running';
                cleanup_writer = onCleanup(@() ResultsAnimationExporter.safe_close_writer(writer)); %#ok<NASGU>
                cleanup_staged = onCleanup(@() ResultsAnimationExporter.safe_delete(staged_mp4_path)); %#ok<NASGU>
                mp4_frames_written = 0;
                for idx = 1:size(omega_cube, 3)
                    ResultsAnimationExporter.render_quad_frame(axes_list, ...
                        omega_cube(:, :, idx), ResultsAnimationExporter.extract_cube_snapshot(psi_cube, idx), ...
                        ResultsAnimationExporter.extract_cube_snapshot(u_cube, idx), ResultsAnimationExporter.extract_cube_snapshot(v_cube, idx), ...
                        ResultsAnimationExporter.extract_cube_snapshot(wall_cube, idx), ...
                        x_vec, y_vec, ResultsAnimationExporter.time_text(display_times, idx), ...
                        plot_mask, wall_mask, bathy_profile_x, bathy_profile_y, plot_labels);
                    rgb_frame = ResultsAnimationExporter.capture_rgb_frame(fig, retry_media);
                    writeVideo(writer, rgb_frame);
                    mp4_frames_written = mp4_frames_written + 1;
                end
                close(writer);
                writer = [];
                status = ResultsAnimationExporter.finalize_media_status( ...
                    status, mp4_frames_written, staged_mp4_path, output_path, retry_media, 'mp4');
            catch ME
                ResultsAnimationExporter.safe_close_writer(writer);
                ResultsAnimationExporter.safe_delete(staged_mp4_path);
                ResultsAnimationExporter.safe_delete(output_path);
                status = ResultsAnimationExporter.record_media_failure(status, ME);
            end
        end

        function tf = media_status_has_failure(status_value)
            tf = false;
            if isempty(status_value)
                return;
            end
            if isstruct(status_value) && isfield(status_value, 'status')
                for i_status = 1:numel(status_value)
                    tf = tf || strcmpi(char(string(status_value(i_status).status)), 'failed');
                    if tf
                        return;
                    end
                end
                return;
            end
            if isstruct(status_value)
                fields = fieldnames(status_value);
                for i = 1:numel(fields)
                    tf = tf || ResultsAnimationExporter.media_status_has_failure(status_value.(fields{i}));
                    if tf
                        return;
                    end
                end
            end
        end

        function messages = collect_failure_messages(varargin)
            messages = {};
            for i = 1:nargin
                messages = [messages, ResultsAnimationExporter.collect_failure_messages_from_value(varargin{i}, '')]; %#ok<AGROW>
            end
            if isempty(messages)
                return;
            end
            messages = unique(messages, 'stable');
        end

        function messages = collect_failure_messages_from_value(value, prefix)
            messages = {};
            if nargin < 2
                prefix = '';
            end
            if isempty(value)
                return;
            end
            if isstruct(value) && isfield(value, 'status')
                for i_value = 1:numel(value)
                    if strcmpi(char(string(value(i_value).status)), 'failed')
                        label = strtrim(prefix);
                        if isempty(label)
                            label = upper(char(string(ResultsAnimationExporter.safe_field(value(i_value), 'format', 'media'))));
                        end
                        messages{end + 1} = sprintf('%s: %s', label, ... %#ok<AGROW>
                            char(string(ResultsAnimationExporter.safe_field(value(i_value), 'failure_message', 'Unknown media export failure.'))));
                    end
                end
                return;
            end
            if isstruct(value)
                fields = fieldnames(value);
                for i = 1:numel(fields)
                    if isempty(prefix)
                        child_prefix = fields{i};
                    else
                        child_prefix = sprintf('%s.%s', prefix, fields{i});
                    end
                    messages = [messages, ResultsAnimationExporter.collect_failure_messages_from_value(value.(fields{i}), child_prefix)]; %#ok<AGROW>
                end
            end
        end

        function frame_count = media_frame_count(media)
            frame_count = NaN;
            if ~(isstruct(media) && isfield(media, 'frame_count'))
                return;
            end
            raw = double(media.frame_count);
            if isscalar(raw) && isfinite(raw)
                frame_count = raw;
            end
        end

        function text = media_profile_text(media)
            if nargin < 1 || ~isstruct(media)
                text = '--';
                return;
            end
            resolution = ResultsAnimationExporter.safe_field(media, 'resolution_px', [NaN, NaN]);
            if ~isnumeric(resolution) || numel(resolution) < 2
                resolution = [NaN, NaN];
            end
            resolution = round(double(resolution(1:2)));
            fps = double(ResultsAnimationExporter.safe_field(media, 'fps', NaN));
            frame_count = double(ResultsAnimationExporter.safe_field(media, 'frame_count', NaN));
            duration_s = double(ResultsAnimationExporter.safe_field(media, 'duration_s', NaN));
            quality = double(ResultsAnimationExporter.safe_field(media, 'quality', NaN));
            text = sprintf('%dx%d | fps=%s | frames=%s | duration=%ss | quality=%s', ...
                resolution(1), resolution(2), ...
                ResultsAnimationExporter.numeric_or_dash(fps, '%.4g'), ...
                ResultsAnimationExporter.numeric_or_dash(frame_count, '%.0f'), ...
                ResultsAnimationExporter.numeric_or_dash(duration_s, '%.4g'), ...
                ResultsAnimationExporter.numeric_or_dash(quality, '%.0f'));
        end

        function value = require_nonzero_file(file_path, media_label, output_path)
            value = 0;
            file_path = char(string(file_path));
            if exist(file_path, 'file') ~= 2
                error('ResultsAnimationExporter:MissingMediaFile', ...
                    '%s export did not create an output file for %s.', upper(char(string(media_label))), ...
                    char(string(output_path)));
            end
            file_info = dir(file_path);
            value = double(file_info.bytes);
            if ~(isfinite(value) && value > 0)
                error('ResultsAnimationExporter:ZeroByteMediaFile', ...
                    '%s export produced a zero-byte file for %s.', upper(char(string(media_label))), ...
                    char(string(output_path)));
            end
        end

        function resolution_px = encoder_safe_resolution(resolution_px, ceiling_px)
            if nargin < 1 || ~isnumeric(resolution_px) || numel(resolution_px) < 2
                resolution_px = [640, 480];
            end
            if nargin < 2 || ~isnumeric(ceiling_px) || numel(ceiling_px) < 2
                ceiling_px = [1920, 1440];
            end
            resolution_px = max(2, round(double(resolution_px(1:2))));
            ceiling_px = max(2, round(double(ceiling_px(1:2))));
            scale = min([1, ceiling_px(1) / max(resolution_px(1), 1), ceiling_px(2) / max(resolution_px(2), 1)]);
            resolution_px = max(2, floor(resolution_px * scale));
            resolution_px = 2 * floor(resolution_px / 2);
            resolution_px(resolution_px < 2) = 2;
        end

        function text = numeric_or_dash(value, fmt)
            if nargin < 2 || isempty(fmt)
                fmt = '%.4g';
            end
            if isnumeric(value) && isscalar(value) && isfinite(value)
                text = sprintf(fmt, double(value));
            else
                text = '--';
            end
        end

        function tf = media_has_format(media, fmt)
            fmt = lower(char(string(fmt)));
            tf = isstruct(media) && isfield(media, 'formats') && any(strcmpi(cellstr(media.formats), fmt));
        end

        function formats = normalize_media_formats(media)
            formats = {};
            if isstruct(media) && isfield(media, 'formats') && ~isempty(media.formats)
                formats = ResultsAnimationExporter.flatten_format_tokens(media.formats);
            end
            if isempty(formats) && isstruct(media) && isfield(media, 'format')
                formats = ResultsAnimationExporter.flatten_format_tokens(media.format);
            end
            if isempty(formats)
                formats = {'mp4'};
            end
            formats = lower(formats(:).');
            if any(ismember(formats, {'both', 'mp4_gif', 'mp4+gif', 'gif+mp4', 'all'}))
                formats = [formats, {'mp4', 'gif'}];
            end
            formats = regexprep(formats, '^mpeg-?4$', 'mp4');
            formats = formats(ismember(formats, {'mp4', 'gif'}));
            formats = unique(formats, 'stable');
            if isempty(formats)
                error('ResultsAnimationExporter:UnsupportedAnimationFormat', ...
                    'Animation export format must include mp4 and/or gif.');
            end
        end

        function tokens = flatten_format_tokens(raw)
            if iscell(raw)
                tokens = {};
                for i = 1:numel(raw)
                    tokens = [tokens, ResultsAnimationExporter.flatten_format_tokens(raw{i})]; %#ok<AGROW>
                end
                return;
            end
            text = lower(char(join(string(raw(:).'), '+')));
            tokens = cellstr(strsplit(text, {'+', ',', ';', '/', '|', ' '}));
            tokens = tokens(~cellfun(@isempty, tokens));
            if contains(text, 'mp4+gif') || contains(text, 'gif+mp4')
                tokens = [tokens, {'mp4+gif'}];
            end
        end
    end
end
