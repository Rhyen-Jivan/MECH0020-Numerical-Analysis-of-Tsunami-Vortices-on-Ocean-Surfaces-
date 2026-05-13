classdef ReferenceEvolutionCalibration
% ReferenceEvolutionCalibration
% Shared helper for loading the GIF reference evolution, sampling
% simulation galleries to a 3x3 contract, and computing lightweight
% image-space comparison metrics without additional toolboxes.

    methods (Static)
        function ref = load_reference(ref_input)
            ref_cfg = ReferenceEvolutionCalibration.normalize_reference_config(ref_input);
            ref = struct( ...
                'asset_name', ReferenceEvolutionCalibration.pick_text(ref_cfg, {'asset_name'}, 'Reference GIF'), ...
                'asset_path', ReferenceEvolutionCalibration.pick_text(ref_cfg, {'asset_path'}, ''), ...
                'rows', max(1, round(ReferenceEvolutionCalibration.pick_numeric(ref_cfg, {'comparison_grid_rows'}, 3))), ...
                'cols', max(1, round(ReferenceEvolutionCalibration.pick_numeric(ref_cfg, {'comparison_grid_cols'}, 3))), ...
                'expected_frame_count', max(1, round(ReferenceEvolutionCalibration.pick_numeric(ref_cfg, {'expected_frame_count'}, 9))), ...
                'actual_frame_count', 0, ...
                'selected_indices', zeros(1, 0), ...
                'plot_box', zeros(1, 4), ...
                'frames_rgb', {{}}, ...
                'cropped_rgb', {{}}, ...
                'gray_frames', {{}});

            asset_path = ref.asset_path;
            if isempty(asset_path) || exist(asset_path, 'file') ~= 2
                return;
            end

            info = imfinfo(asset_path);
            actual_frames = numel(info);
            if actual_frames < 1
                return;
            end
            ref.actual_frame_count = actual_frames;
            tile_count = ref.rows * ref.cols;
            frame_cap = min(ref.expected_frame_count, actual_frames);
            ref.selected_indices = ReferenceEvolutionCalibration.select_snapshot_indices(frame_cap, tile_count);

            frames_rgb = cell(1, numel(ref.selected_indices));
            cropped_rgb = cell(1, numel(ref.selected_indices));
            gray_frames = cell(1, numel(ref.selected_indices));
            plot_box = [];
            for i = 1:numel(ref.selected_indices)
                [frame, cmap] = imread(asset_path, 'Frames', ref.selected_indices(i));
                if ~isempty(cmap)
                    frame = ind2rgb(frame, cmap);
                end
                frame = ReferenceEvolutionCalibration.ensure_rgb_double(frame);
                if isempty(plot_box)
                    plot_box = ReferenceEvolutionCalibration.detect_plot_box(frame);
                end
                cropped = ReferenceEvolutionCalibration.crop_rgb(frame, plot_box);
                frames_rgb{i} = frame;
                cropped_rgb{i} = cropped;
                gray_frames{i} = ReferenceEvolutionCalibration.rgb_to_gray(cropped);
            end

            ref.plot_box = plot_box;
            ref.frames_rgb = frames_rgb;
            ref.cropped_rgb = cropped_rgb;
            ref.gray_frames = gray_frames;
        end

        function fig = build_reference_grid_figure(ref_input, title_text)
            reference = ReferenceEvolutionCalibration.load_reference(ref_input);
            fig = [];
            if isempty(reference.selected_indices)
                return;
            end
            if nargin < 2 || strlength(string(title_text)) == 0
                title_text = sprintf('Reference Evolution Grid | %s | %d GIF frames', ...
                    reference.asset_name, reference.actual_frame_count);
            end

            fig = figure('Visible', 'off', 'Color', 'w', 'Units', 'pixels', 'Position', [120 120 1240 900]);
            layout = tiledlayout(fig, reference.rows, reference.cols, 'TileSpacing', 'compact', 'Padding', 'compact');
            title(layout, char(string(title_text)));
            tile_count = reference.rows * reference.cols;
            for i = 1:tile_count
                ax = nexttile(layout, i);
                if i <= numel(reference.selected_indices)
                    image(ax, reference.frames_rgb{i});
                    axis(ax, 'image');
                    axis(ax, 'off');
                    title(ax, sprintf('Frame %d / %d', reference.selected_indices(i), reference.actual_frame_count), ...
                        'Interpreter', 'none');
                else
                    axis(ax, 'off');
                end
            end
        end

        function sim = build_simulation_gallery(analysis, ref_input, varargin)
            ref_cfg = ReferenceEvolutionCalibration.normalize_reference_config(ref_input);
            label_text = 'Simulation Evolution Grid';
            method_label = '';
            if nargin >= 3
                label_text = char(string(varargin{1}));
            end
            if nargin >= 4
                method_label = char(string(varargin{2}));
            end

            sim = struct( ...
                'label_text', label_text, ...
                'method_label', method_label, ...
                'rows', max(1, round(ReferenceEvolutionCalibration.pick_numeric(ref_cfg, {'comparison_grid_rows'}, 3))), ...
                'cols', max(1, round(ReferenceEvolutionCalibration.pick_numeric(ref_cfg, {'comparison_grid_cols'}, 3))), ...
                'selected_indices', zeros(1, 0), ...
                'snapshot_times', zeros(1, 0), ...
                'x', zeros(1, 0), ...
                'y', zeros(1, 0), ...
                'omega_selected', zeros(0, 0, 0), ...
                'rgb_frames', {{}}, ...
                'gray_frames', {{}}, ...
                'color_limits', [-1 1], ...
                'crop_boundary_cells', 0, ...
                'crop_applied', false, ...
                'grid_size', [0 0], ...
                'snapshot_count', 0);

            omega_cube = ReferenceEvolutionCalibration.extract_omega_cube(analysis);
            if isempty(omega_cube)
                return;
            end

            [x_vec, y_vec] = ReferenceEvolutionCalibration.resolve_analysis_axes(analysis, size(omega_cube(:, :, 1)));
            snapshot_count = size(omega_cube, 3);
            tile_count = sim.rows * sim.cols;
            selected_indices = ReferenceEvolutionCalibration.select_snapshot_indices(snapshot_count, tile_count);
            snapshot_times = ReferenceEvolutionCalibration.resolve_snapshot_times(analysis, snapshot_count);
            selected_cube = omega_cube(:, :, selected_indices);
            crop_cells = max(0, round(ReferenceEvolutionCalibration.pick_numeric(ref_cfg, ...
                {'simulation_crop_boundary_cells', 'crop_boundary_cells'}, 0)));
            if crop_cells > 0
                [selected_cube, x_vec, y_vec, crop_applied] = ...
                    ReferenceEvolutionCalibration.crop_simulation_cube(selected_cube, x_vec, y_vec, crop_cells);
            else
                crop_applied = false;
            end
            color_limits = ReferenceEvolutionCalibration.resolve_color_limits(selected_cube);

            rgb_frames = cell(1, numel(selected_indices));
            gray_frames = cell(1, numel(selected_indices));
            for i = 1:numel(selected_indices)
                rgb_frames{i} = ReferenceEvolutionCalibration.omega_to_rgb(selected_cube(:, :, i), color_limits);
                gray_frames{i} = ReferenceEvolutionCalibration.rgb_to_gray(rgb_frames{i});
            end

            sim.selected_indices = selected_indices;
            sim.snapshot_times = snapshot_times(selected_indices);
            sim.x = x_vec;
            sim.y = y_vec;
            sim.omega_selected = selected_cube;
            sim.rgb_frames = rgb_frames;
            sim.gray_frames = gray_frames;
            sim.color_limits = color_limits;
            sim.crop_boundary_cells = crop_cells;
            sim.crop_applied = crop_applied;
            sim.grid_size = size(omega_cube(:, :, 1));
            sim.snapshot_count = snapshot_count;
        end

        function fig = build_simulation_grid_figure(sim, title_text)
            fig = [];
            if ~isstruct(sim) || isempty(sim.selected_indices) || isempty(sim.omega_selected)
                return;
            end
            if nargin < 2 || strlength(string(title_text)) == 0
                title_text = sim.label_text;
            end

            fig = figure('Visible', 'off', 'Color', 'w', 'Units', 'pixels', 'Position', [120 120 1240 900]);
            layout = tiledlayout(fig, sim.rows, sim.cols, 'TileSpacing', 'compact', 'Padding', 'compact');
            title(layout, char(string(title_text)));
            for i = 1:(sim.rows * sim.cols)
                ax = nexttile(layout, i);
                if i <= numel(sim.selected_indices)
                    imagesc(ax, sim.x, sim.y, double(sim.omega_selected(:, :, i)));
                    set(ax, 'YDir', 'normal');
                    axis(ax, 'equal');
                    axis(ax, 'tight');
                    colormap(ax, turbo);
                    clim(ax, sim.color_limits);
                    title(ax, sprintf('t = %.3g s', sim.snapshot_times(i)), 'Interpreter', 'none');
                    xlabel(ax, 'x');
                    ylabel(ax, 'y');
                    grid(ax, 'on');
                    box(ax, 'on');
                else
                    axis(ax, 'off');
                end
            end
        end

        function metrics = compute_metrics(reference, sim, metadata)
            if nargin < 3 || ~isstruct(metadata)
                metadata = struct();
            end

            frame_metric_template = struct( ...
                'tile_index', NaN, ...
                'reference_frame_index', NaN, ...
                'simulation_snapshot_index', NaN, ...
                'time_s', NaN, ...
                'grayscale_rmse', NaN, ...
                'contour_overlap_loss', NaN, ...
                'core_principal_axis_angle_error_rad', NaN, ...
                'spiral_arm_angle_error_rad', NaN, ...
                'composite_gif_match_score', NaN);

            metrics = struct( ...
                'preset_id', ReferenceEvolutionCalibration.pick_text(metadata, {'preset_id'}, ''), ...
                'method_label', ReferenceEvolutionCalibration.pick_text(metadata, {'method_label'}, sim.method_label), ...
                'reference_asset_path', ReferenceEvolutionCalibration.pick_text(reference, {'asset_path'}, ''), ...
                'comparison_grid_rows', ReferenceEvolutionCalibration.pick_numeric(reference, {'rows'}, 3), ...
                'comparison_grid_cols', ReferenceEvolutionCalibration.pick_numeric(reference, {'cols'}, 3), ...
                'calibration_grid_scope', ReferenceEvolutionCalibration.pick_numeric(metadata, {'grid_n'}, NaN), ...
                'frame_metrics', repmat(frame_metric_template, 1, 0), ...
                'summary', struct());

            frame_count = min(numel(reference.gray_frames), numel(sim.gray_frames));
            if frame_count < 1
                return;
            end

            frame_metrics = repmat(frame_metric_template, 1, frame_count);
            rmse_values = nan(1, frame_count);
            overlap_values = nan(1, frame_count);
            axis_values = nan(1, frame_count);
            arm_values = nan(1, frame_count);
            score_values = nan(1, frame_count);
            for i = 1:frame_count
                ref_gray = ReferenceEvolutionCalibration.normalize_image(reference.gray_frames{i});
                sim_gray = ReferenceEvolutionCalibration.normalize_image(sim.gray_frames{i});
                sim_gray = ReferenceEvolutionCalibration.resize_image(sim_gray, size(ref_gray, 1), size(ref_gray, 2));

                grayscale_rmse = sqrt(mean((sim_gray(:) - ref_gray(:)) .^ 2, 'omitnan'));
                contour_loss = ReferenceEvolutionCalibration.contour_overlap_loss(ref_gray, sim_gray);
                ref_axis = ReferenceEvolutionCalibration.principal_axis_angle(ref_gray);
                sim_axis = ReferenceEvolutionCalibration.principal_axis_angle(sim_gray);
                axis_error = ReferenceEvolutionCalibration.wrap_angle_delta(ref_axis, sim_axis);
                ref_arm = ReferenceEvolutionCalibration.spiral_arm_angle(ref_gray);
                sim_arm = ReferenceEvolutionCalibration.spiral_arm_angle(sim_gray);
                arm_error = ReferenceEvolutionCalibration.wrap_angle_delta(ref_arm, sim_arm);
                composite_score = max(0.0, 1.0 - mean([ ...
                    grayscale_rmse, ...
                    contour_loss, ...
                    axis_error / pi, ...
                    arm_error / pi], 'omitnan'));

                rmse_values(i) = grayscale_rmse;
                overlap_values(i) = contour_loss;
                axis_values(i) = axis_error;
                arm_values(i) = arm_error;
                score_values(i) = composite_score;

                frame_metrics(i) = struct( ...
                    'tile_index', i, ...
                    'reference_frame_index', reference.selected_indices(i), ...
                    'simulation_snapshot_index', sim.selected_indices(i), ...
                    'time_s', sim.snapshot_times(i), ...
                    'grayscale_rmse', grayscale_rmse, ...
                    'contour_overlap_loss', contour_loss, ...
                    'core_principal_axis_angle_error_rad', axis_error, ...
                    'spiral_arm_angle_error_rad', arm_error, ...
                    'composite_gif_match_score', composite_score);
            end

            metrics.frame_metrics = frame_metrics;
            metrics.summary = struct( ...
                'frame_count', frame_count, ...
                'mean_grayscale_rmse', mean(rmse_values, 'omitnan'), ...
                'mean_contour_overlap_loss', mean(overlap_values, 'omitnan'), ...
                'mean_core_principal_axis_angle_error_rad', mean(axis_values, 'omitnan'), ...
                'mean_spiral_arm_angle_error_rad', mean(arm_values, 'omitnan'), ...
                'mean_composite_gif_match_score', mean(score_values, 'omitnan'), ...
                'min_composite_gif_match_score', min(score_values, [], 'omitnan'), ...
                'max_composite_gif_match_score', max(score_values, [], 'omitnan'));
        end

        function fig = build_reference_vs_simulation_figure(reference, sim, metrics, title_text)
            fig = [];
            frame_count = min([numel(reference.cropped_rgb), numel(sim.rgb_frames), numel(metrics.frame_metrics)]);
            if frame_count < 1
                return;
            end
            if nargin < 4 || strlength(string(title_text)) == 0
                title_text = sprintf('Reference vs Simulation | %s', sim.label_text);
            end

            fig = figure('Visible', 'off', 'Color', 'w', 'Units', 'pixels', 'Position', [120 120 1440 980]);
            layout = tiledlayout(fig, sim.rows, sim.cols, 'TileSpacing', 'compact', 'Padding', 'compact');
            title(layout, char(string(title_text)));
            for i = 1:(sim.rows * sim.cols)
                ax = nexttile(layout, i);
                if i <= frame_count
                    ref_rgb = reference.cropped_rgb{i};
                    sim_rgb = ReferenceEvolutionCalibration.resize_rgb(sim.rgb_frames{i}, size(ref_rgb, 1), size(ref_rgb, 2));
                    panel_rgb = cat(2, ref_rgb, sim_rgb);
                    image(ax, panel_rgb);
                    axis(ax, 'image');
                    axis(ax, 'off');
                    fm = metrics.frame_metrics(i);
                    title(ax, sprintf('t = %.3g s | score %.3f', fm.time_s, fm.composite_gif_match_score), ...
                        'Interpreter', 'none');
                else
                    axis(ax, 'off');
                end
            end
        end

        function indices = select_snapshot_indices(snapshot_count, target_count)
            snapshot_count = max(0, round(double(snapshot_count)));
            target_count = max(0, round(double(target_count)));
            if snapshot_count < 1 || target_count < 1
                indices = zeros(1, 0);
                return;
            end
            indices = unique(round(linspace(1, snapshot_count, min(snapshot_count, target_count))), 'stable');
            indices = indices(indices >= 1 & indices <= snapshot_count);
            if isempty(indices)
                indices = 1;
            end
        end

        function ref_cfg = normalize_reference_config(ref_input)
            if isstruct(ref_input) && isfield(ref_input, 'asset_path') && ~isfield(ref_input, 'frames_rgb')
                ref_cfg = ref_input;
                return;
            end
            ref_cfg = struct();
            if isstruct(ref_input)
                ref_cfg = ref_input;
            end
        end

        function omega_cube = extract_omega_cube(analysis)
            omega_cube = [];
            if ~isstruct(analysis) || ~isfield(analysis, 'omega_snaps') || isempty(analysis.omega_snaps)
                return;
            end
            omega_cube = double(analysis.omega_snaps);
            if ndims(omega_cube) == 2
                omega_cube = reshape(omega_cube, size(omega_cube, 1), size(omega_cube, 2), 1);
            end
        end

        function [x_vec, y_vec] = resolve_analysis_axes(analysis, field_size)
            ny = field_size(1);
            nx = field_size(2);
            if isfield(analysis, 'x') && numel(analysis.x) == nx
                x_vec = double(analysis.x(:)).';
            else
                Lx = ReferenceEvolutionCalibration.pick_numeric(analysis, {'Lx'}, nx);
                x_vec = linspace(-Lx / 2, Lx / 2, nx);
            end
            if isfield(analysis, 'y') && numel(analysis.y) == ny
                y_vec = double(analysis.y(:));
            else
                Ly = ReferenceEvolutionCalibration.pick_numeric(analysis, {'Ly'}, ny);
                y_vec = linspace(-Ly / 2, Ly / 2, ny).';
            end
        end

        function snapshot_times = resolve_snapshot_times(analysis, snapshot_count)
            snapshot_times = [];
            if isstruct(analysis)
                if isfield(analysis, 'snapshot_times_requested') && numel(analysis.snapshot_times_requested) >= snapshot_count
                    snapshot_times = double(analysis.snapshot_times_requested(1:snapshot_count));
                elseif isfield(analysis, 'snapshot_times') && numel(analysis.snapshot_times) >= snapshot_count
                    snapshot_times = double(analysis.snapshot_times(1:snapshot_count));
                elseif isfield(analysis, 'time_vec') && numel(analysis.time_vec) >= snapshot_count
                    snapshot_times = double(analysis.time_vec(1:snapshot_count));
                end
            end
            if isempty(snapshot_times)
                snapshot_times = linspace(0, max(snapshot_count - 1, 1), snapshot_count);
            end
            snapshot_times = reshape(snapshot_times, 1, []);
        end

        function color_limits = resolve_color_limits(omega_cube)
            finite_values = double(omega_cube(isfinite(omega_cube)));
            if isempty(finite_values)
                color_limits = [-1 1];
                return;
            end
            cmin = min(finite_values);
            cmax = max(finite_values);
            if ~(isfinite(cmin) && isfinite(cmax) && cmax > cmin)
                color_limits = [-1 1];
                return;
            end
            color_limits = [cmin cmax];
        end

        function [omega_cube, x_vec, y_vec, crop_applied] = crop_simulation_cube(omega_cube, x_vec, y_vec, crop_cells)
            crop_applied = false;
            if isempty(omega_cube) || crop_cells < 1
                return;
            end
            ny = size(omega_cube, 1);
            nx = size(omega_cube, 2);
            max_crop = floor((min(ny, nx) - 2) / 2);
            crop_cells = min(max(0, round(double(crop_cells))), max_crop);
            if crop_cells < 1
                return;
            end
            row_idx = (1 + crop_cells):(ny - crop_cells);
            col_idx = (1 + crop_cells):(nx - crop_cells);
            if numel(row_idx) < 2 || numel(col_idx) < 2
                return;
            end
            omega_cube = omega_cube(row_idx, col_idx, :);
            if numel(x_vec) == nx
                x_vec = x_vec(col_idx);
            end
            if numel(y_vec) == ny
                y_vec = y_vec(row_idx);
            end
            crop_applied = true;
        end

        function rgb = omega_to_rgb(field, color_limits)
            field = double(field);
            field(~isfinite(field)) = color_limits(1);
            cmin = color_limits(1);
            cmax = color_limits(2);
            if ~(isfinite(cmin) && isfinite(cmax) && cmax > cmin)
                cmin = min(field(:));
                cmax = max(field(:));
                if ~(isfinite(cmin) && isfinite(cmax) && cmax > cmin)
                    cmin = -1;
                    cmax = 1;
                end
            end
            alpha = (field - cmin) ./ max(cmax - cmin, eps);
            alpha = max(0.0, min(1.0, alpha));
            cmap = turbo(256);
            idx = 1 + floor(alpha * 255);
            idx = max(1, min(256, idx));
            rgb = ind2rgb(idx, cmap);
        end

        function rgb = crop_rgb(rgb, plot_box)
            if isempty(rgb)
                return;
            end
            if isempty(plot_box) || numel(plot_box) < 4
                return;
            end
            x0 = max(1, round(plot_box(1)));
            y0 = max(1, round(plot_box(2)));
            x1 = min(size(rgb, 2), round(plot_box(1) + plot_box(3) - 1));
            y1 = min(size(rgb, 1), round(plot_box(2) + plot_box(4) - 1));
            if x1 <= x0 || y1 <= y0
                return;
            end
            rgb = rgb(y0:y1, x0:x1, :);
        end

        function plot_box = detect_plot_box(frame_rgb)
            frame_rgb = ReferenceEvolutionCalibration.ensure_rgb_double(frame_rgb);
            intensity = mean(frame_rgb, 3);
            dark_mask = intensity < 0.78;
            row_frac = mean(dark_mask, 2);
            col_frac = mean(dark_mask, 1);
            rows = find(row_frac > 0.20);
            cols = find(col_frac > 0.20);
            if isempty(rows) || isempty(cols)
                content_mask = intensity < 0.95;
                [row_idx, col_idx] = find(content_mask);
                if isempty(row_idx) || isempty(col_idx)
                    plot_box = [1, 1, size(frame_rgb, 2), size(frame_rgb, 1)];
                    return;
                end
                rows = row_idx(:);
                cols = col_idx(:);
            end
            plot_box = [ ...
                min(cols), ...
                min(rows), ...
                max(cols) - min(cols) + 1, ...
                max(rows) - min(rows) + 1];
        end

        function gray = rgb_to_gray(rgb)
            rgb = ReferenceEvolutionCalibration.ensure_rgb_double(rgb);
            gray = 0.2989 .* rgb(:, :, 1) + 0.5870 .* rgb(:, :, 2) + 0.1140 .* rgb(:, :, 3);
        end

        function gray = normalize_image(gray)
            gray = double(gray);
            gray(~isfinite(gray)) = 0;
            gmin = min(gray(:));
            gmax = max(gray(:));
            if ~(isfinite(gmin) && isfinite(gmax) && gmax > gmin)
                gray = zeros(size(gray));
                return;
            end
            gray = (gray - gmin) ./ (gmax - gmin);
        end

        function loss = contour_overlap_loss(ref_gray, sim_gray)
            ref_thr = max(0.25, quantile(ref_gray(:), 0.70));
            sim_thr = max(0.25, quantile(sim_gray(:), 0.70));
            ref_mask = ref_gray >= ref_thr;
            sim_mask = sim_gray >= sim_thr;
            union_count = nnz(ref_mask | sim_mask);
            if union_count < 1
                loss = 1.0;
                return;
            end
            loss = 1.0 - (nnz(ref_mask & sim_mask) / union_count);
        end

        function theta = principal_axis_angle(gray)
            theta = NaN;
            gray = ReferenceEvolutionCalibration.normalize_image(gray);
            [ny, nx] = size(gray);
            [X, Y] = meshgrid(linspace(-1, 1, nx), linspace(-1, 1, ny));
            weight = gray .^ 2;
            total = sum(weight(:));
            if ~(isfinite(total) && total > eps)
                return;
            end
            cx = sum(X(:) .* weight(:)) / total;
            cy = sum(Y(:) .* weight(:)) / total;
            dx = X(:) - cx;
            dy = Y(:) - cy;
            cxx = sum((dx .^ 2) .* weight(:)) / total;
            cyy = sum((dy .^ 2) .* weight(:)) / total;
            cxy = sum((dx .* dy) .* weight(:)) / total;
            theta = 0.5 * atan2(2 * cxy, cxx - cyy);
        end

        function theta = spiral_arm_angle(gray)
            theta = NaN;
            gray = ReferenceEvolutionCalibration.normalize_image(gray);
            [ny, nx] = size(gray);
            [X, Y] = meshgrid(linspace(-1, 1, nx), linspace(-1, 1, ny));
            weight = gray .^ 2;
            total = sum(weight(:));
            if ~(isfinite(total) && total > eps)
                return;
            end
            cx = sum(X(:) .* weight(:)) / total;
            cy = sum(Y(:) .* weight(:)) / total;
            dX = X - cx;
            dY = Y - cy;
            radius = hypot(dX, dY);
            radius_max = max(radius(:));
            if ~(isfinite(radius_max) && radius_max > eps)
                return;
            end
            ring_mask = radius >= 0.18 * radius_max & radius <= 0.82 * radius_max;
            bright_mask = gray >= max(0.20, quantile(gray(:), 0.60));
            use_mask = ring_mask & bright_mask;
            if nnz(use_mask) < 8
                return;
            end
            theta_values = atan2(dY(use_mask), dX(use_mask));
            w = gray(use_mask) .* max(radius(use_mask), eps);
            moment = sum(w .* exp(1i * theta_values));
            if abs(moment) <= eps
                return;
            end
            theta = angle(moment);
        end

        function delta = wrap_angle_delta(a, b)
            if ~(isfinite(a) && isfinite(b))
                delta = NaN;
                return;
            end
            delta = abs(angle(exp(1i * (a - b))));
        end

        function rgb = resize_rgb(rgb, target_rows, target_cols)
            rgb = ReferenceEvolutionCalibration.ensure_rgb_double(rgb);
            resized = zeros(target_rows, target_cols, size(rgb, 3));
            for c = 1:size(rgb, 3)
                resized(:, :, c) = ReferenceEvolutionCalibration.resize_image(rgb(:, :, c), target_rows, target_cols);
            end
            rgb = resized;
        end

        function resized = resize_image(image_in, target_rows, target_cols)
            image_in = double(image_in);
            [src_rows, src_cols] = size(image_in);
            if src_rows == target_rows && src_cols == target_cols
                resized = image_in;
                return;
            end
            x_src = linspace(1, src_cols, src_cols);
            y_src = linspace(1, src_rows, src_rows);
            xq = linspace(1, src_cols, target_cols);
            yq = linspace(1, src_rows, target_rows);
            [Xq, Yq] = meshgrid(xq, yq);
            resized = interp2(x_src, y_src, image_in, Xq, Yq, 'linear', 0);
        end

        function rgb = ensure_rgb_double(rgb)
            if isa(rgb, 'uint8') || isa(rgb, 'uint16') || isa(rgb, 'uint32')
                rgb = im2double(rgb);
            else
                rgb = double(rgb);
                if max(rgb(:), [], 'omitnan') > 1.0 || min(rgb(:), [], 'omitnan') < 0.0
                    rgb = max(0.0, min(1.0, rgb ./ 255.0));
                end
            end
            if ndims(rgb) == 2
                rgb = repmat(rgb, 1, 1, 3);
            end
        end

        function value = pick_numeric(s, keys, fallback)
            value = fallback;
            if ~isstruct(s)
                return;
            end
            for i = 1:numel(keys)
                key = keys{i};
                if isfield(s, key) && ~isempty(s.(key))
                    candidate = double(s.(key));
                    if isscalar(candidate) && isfinite(candidate)
                        value = candidate;
                        return;
                    end
                end
            end
        end

        function value = pick_text(s, keys, fallback)
            value = char(string(fallback));
            if ~isstruct(s)
                return;
            end
            for i = 1:numel(keys)
                key = keys{i};
                if isfield(s, key) && ~isempty(s.(key))
                    value = char(string(s.(key)));
                    return;
                end
            end
        end
    end
end
