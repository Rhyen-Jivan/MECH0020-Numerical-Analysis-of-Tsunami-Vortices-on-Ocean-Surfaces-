function [Results, paths] = mode_plotting(Run_Config, ~, Settings)
    % mode_plotting - METHOD-AGNOSTIC Plotting Mode
    %
    % Purpose:
    %   Loads existing simulation data and generates visualizations
    %   Method-agnostic (works with data from any method)
    %
    % Inputs:
    %   Run_Config - .source_run_id (required), .plot_types
    %   Parameters - plot settings
    %   Settings - IO settings
    %
    % Outputs:
    %   Results - plotting summary
    %   paths - directory structure

    % ===== VALIDATION =====
    if ~isfield(Run_Config, 'source_run_id') || isempty(Run_Config.source_run_id)
        error('Plotting mode requires Run_Config.source_run_id');
    end

    % ===== LOAD SOURCE DATA =====
    source_run_id = Run_Config.source_run_id;
    fprintf('[Plotting] Loading data from run: %s\n', source_run_id);

    output_root = 'Results';
    if nargin >= 3 && isstruct(Settings) && isfield(Settings, 'output_root') && ~isempty(Settings.output_root)
        output_root = char(string(Settings.output_root));
    end

    % Attempt to load results (method-agnostic path search)
    data_path = find_run_data(source_run_id, output_root);
    if isempty(data_path)
        error('Could not find data for run_id: %s', source_run_id);
    end

    load(data_path, 'analysis');

    % ===== SETUP OUTPUT =====
    if ~isfield(Run_Config, 'run_id') || isempty(Run_Config.run_id)
        Run_Config.run_id = sprintf('plot_%s', source_run_id);
    end

    paths = PathBuilder.get_run_paths('Plotting', 'Plotting', Run_Config.run_id, output_root);
    PathBuilder.ensure_directories(paths);

    % ===== GENERATE PLOTS =====
    fprintf('[Plotting] Generating visualizations...\n');

    % Plot types
    if ~isfield(Run_Config, 'plot_types')
        plot_types = {'contours', 'streamlines'};
    else
        plot_types = Run_Config.plot_types;
    end

    for k = 1:length(plot_types)
        switch lower(plot_types{k})
            case 'contours'
                generate_contour_plots(analysis, paths, Settings);
            case 'evolution'
                generate_evolution_plots(analysis, paths, Settings);
            case 'streamlines'
                generate_streamline_plots(analysis, paths, Settings);
            otherwise
                warning('Unknown plot type: %s', plot_types{k});
        end
    end

    % ===== RESULTS =====
    Results = struct();
    Results.source_run_id = source_run_id;
    Results.plot_types = plot_types;
    Results.status = 'completed';

    fprintf('[Plotting] Completed for run: %s\n', source_run_id);
end

%% ===== LOCAL FUNCTIONS =====

function data_path = find_run_data(run_id, output_root)
    % Search for run data in canonical Results root.
    % This replaces legacy Data/Output lookup and keeps plotting aligned
    % with PathBuilder-produced output trees.
    if exist('PathBuilder', 'class') == 8 || exist('PathBuilder', 'file') == 2
        repo_root = PathBuilder.get_repo_root();
    else
        this_file = mfilename('fullpath');
        repo_root = fullfile(fileparts(this_file), '..', '..');
    end
    if nargin < 2 || isempty(output_root)
        output_root = 'Results';
    end
    if exist('PathBuilder', 'class') == 8 || exist('PathBuilder', 'file') == 2
        [results_root, ~] = PathBuilder.resolve_output_root(repo_root, output_root);
    else
        results_root = fullfile(repo_root, char(string(output_root)));
    end

    methods = {'FD', 'Spectral', 'FV', 'SWE', 'Bathymetry', 'Plotting'};
    modes = {'Evolution', 'Convergence', 'ParameterSweep', 'Plotting'};
    folder_tokens = {char(string(run_id))};
    if exist('RunIDGenerator', 'class') == 8 || exist('RunIDGenerator', 'file') == 2
        compact_token = RunIDGenerator.make_storage_id(run_id);
        if ~isempty(compact_token)
            folder_tokens{end + 1} = char(string(compact_token)); %#ok<AGROW>
        end
    end
    folder_tokens = unique(folder_tokens, 'stable');

    data_path = '';
    newest_datenum = -inf;
    for i = 1:numel(methods)
        method_token = methods{i};
        for token_idx = 1:numel(folder_tokens)
            compact_root = fullfile(results_root, method_token, folder_tokens{token_idx});
            compact_candidates = { ...
                fullfile(compact_root, 'Data', 'results.mat'), ...
                fullfile(compact_root, 'Data', sprintf('%s_Run_Data.mat', method_token)), ...
                fullfile(compact_root, 'Data', 'convergence_results.mat'), ...
                fullfile(compact_root, 'Data', 'sweep_results.mat'), ...
                fullfile(compact_root, 'MATLAB_Data', method_token, 'results.mat'), ...
                fullfile(compact_root, 'MATLAB_Data', method_token, sprintf('%s_Run_Data.mat', method_token)), ...
                fullfile(compact_root, 'MATLAB_Data', method_token, 'convergence_results.mat'), ...
                fullfile(compact_root, 'MATLAB_Data', method_token, 'sweep_results.mat')};
            for ci = 1:numel(compact_candidates)
                candidate = compact_candidates{ci};
                if exist(candidate, 'file')
                    file_info = dir(candidate);
                    if ~isempty(file_info) && file_info.datenum > newest_datenum
                        newest_datenum = file_info.datenum;
                        data_path = candidate;
                    end
                end
            end
        end

        for j = 1:numel(modes)
            for token_idx = 1:numel(folder_tokens)
                candidate = fullfile(results_root, method_token, modes{j}, folder_tokens{token_idx}, 'Data', 'results.mat');
                if exist(candidate, 'file')
                    file_info = dir(candidate);
                    if ~isempty(file_info) && file_info.datenum > newest_datenum
                        newest_datenum = file_info.datenum;
                        data_path = candidate;
                    end
                end
            end

            % Evolution mode now stores run files under shared mode Data/.
            if strcmp(modes{j}, 'Evolution')
                shared_candidate = fullfile(results_root, method_token, modes{j}, 'Data', ...
                    sprintf('results_%s.mat', run_id));
                if exist(shared_candidate, 'file')
                    file_info = dir(shared_candidate);
                    if ~isempty(file_info) && file_info.datenum > newest_datenum
                        newest_datenum = file_info.datenum;
                        data_path = shared_candidate;
                    end
                end
            end
        end
    end

    if isempty(data_path)
        run_root = find_run_root_by_settings_token(results_root, run_id);
        if ~isempty(run_root)
            candidate_paths = { ...
                fullfile(run_root, 'Data', 'results.mat'), ...
                fullfile(run_root, 'Data', 'convergence_results.mat'), ...
                fullfile(run_root, 'Data', 'sweep_results.mat'), ...
                fullfile(run_root, 'MATLAB_Data', 'FD', 'results.mat'), ...
                fullfile(run_root, 'MATLAB_Data', 'FD', 'convergence_results.mat'), ...
                fullfile(run_root, 'MATLAB_Data', 'Spectral', 'results.mat'), ...
                fullfile(run_root, 'MATLAB_Data', 'Spectral', 'convergence_results.mat'), ...
                fullfile(run_root, 'MATLAB_Data', 'FV', 'results.mat'), ...
                fullfile(run_root, 'MATLAB_Data', 'SWE', 'results.mat'), ...
                fullfile(run_root, 'MATLAB_Data', 'Bathymetry', 'results.mat')};
            for ci = 1:numel(candidate_paths)
                candidate = candidate_paths{ci};
                if exist(candidate, 'file')
                    file_info = dir(candidate);
                    if ~isempty(file_info) && file_info.datenum > newest_datenum
                        newest_datenum = file_info.datenum;
                        data_path = candidate;
                    end
                end
            end
        end
    end
end

function run_root = find_run_root_by_settings_token(results_root, run_id)
    run_root = '';
    if exist(results_root, 'dir') ~= 7
        return;
    end

    settings_files = dir(fullfile(results_root, '**', 'Run_Settings.txt'));
    for i = 1:numel(settings_files)
        settings_path = fullfile(settings_files(i).folder, settings_files(i).name);
        try
            settings_text = fileread(settings_path);
        catch
            settings_text = '';
        end
        if contains(settings_text, char(string(run_id)))
            run_root = settings_files(i).folder;
            return;
        end
    end
end

function generate_contour_plots(analysis, paths, Settings)
    % Generate contour plots from snapshots
    if ~isfield(analysis, 'omega_snaps')
        return;
    end

    plot_labels = resolve_analysis_plot_labels(analysis);
    omega_snaps = double(analysis.omega_snaps);
    Nsnap = size(omega_snaps, 3);
    [x_vec, y_vec] = resolve_plot_axes(analysis, omega_snaps);
    snap_times = resolve_plot_times(analysis, Nsnap);
    plot_context = resolve_bathymetry_plot_context(analysis, size(omega_snaps, 1), size(omega_snaps, 2));
    scaling = resolve_result_plot_scaling(omega_snaps, plot_context, resolve_plot_scaling_params(Settings));
    cmin = scaling.cmin;
    cmax = scaling.cmax;

    fig = figure('Position', [100, 100, 1200, 800]);
    apply_dark_theme_for_figure(fig);
    ncols = min(4, Nsnap);
    nrows = ceil(Nsnap / ncols);

    for k = 1:Nsnap
        ax = subplot(nrows, ncols, k);
        omega_slice = omega_snaps(:, :, k);
        omega_plot = apply_plot_mask(omega_slice, scaling.plot_mask);
        contourf(ax, x_vec, y_vec, omega_plot, resolve_contour_level_count(Settings), 'LineStyle', 'none');
        hold(ax, 'on');
        overlay_streamfunction_contours(ax, x_vec, y_vec, ...
            apply_plot_mask(resolve_streamfunction_slice(analysis, k, omega_slice, x_vec, y_vec), scaling.plot_mask), [0.92 0.92 0.92]);
        draw_bathymetry_outline(ax, plot_context.profile_x, plot_context.profile_y, [0.92 0.92 0.95]);
        hold(ax, 'off');
        axis(ax, 'equal', 'tight');
        set(ax, 'YDir', 'normal');
        colormap(ax, turbo);
        clim(ax, [cmin cmax]);
        cb = colorbar(ax);
        if isprop(cb, 'Color')
            cb.Color = [0.90 0.92 0.95];
        end
        title(ax, sprintf('t = %.3g s', snap_times(k)));
    end

    sgtitle(plot_labels.gallery_contour_title);
    ResultsPlotDispatcher.save_figure_bundle(fig, fullfile(resolve_plot_output_dir(paths, 'figures_contours', 'figures_evolution'), 'contours.png'), Settings);
    close(fig);

    if scaling.include_boundary_diagnostics && any(scaling.boundary_strip_mask(:))
        generate_boundary_strip_plots(analysis, paths, Settings, x_vec, y_vec, snap_times, plot_context, scaling.boundary_strip_mask, cmin, cmax);
    end
end

function generate_evolution_plots(analysis, paths, Settings)
    % Generate time evolution plots
    if ~isfield(analysis, 'time_vec')
        return;
    end

    plot_labels = resolve_analysis_plot_labels(analysis);
    fig = figure('Position', [100, 100, 1000, 600]);
    apply_dark_theme_for_figure(fig);

    if isfield(analysis, 'kinetic_energy')
        subplot(2, 1, 1);
        plot(analysis.time_vec, analysis.kinetic_energy, 'LineWidth', 2);
        grid on;
        xlabel('Time');
        ylabel(plot_labels.diagnostics_primary);
        title(sprintf('%s evolution', plot_labels.diagnostics_primary));
    end

    if isfield(analysis, 'enstrophy')
        subplot(2, 1, 2);
        plot(analysis.time_vec, analysis.enstrophy, 'LineWidth', 2);
        grid on;
        xlabel('Time');
        ylabel(plot_labels.diagnostics_secondary);
        title(sprintf('%s evolution', plot_labels.diagnostics_secondary));
    end

    sgtitle(plot_labels.diagnostics_title);
    ResultsPlotDispatcher.save_figure_bundle(fig, fullfile(paths.figures_evolution, 'evolution.png'), Settings);
    close(fig);
end

function generate_streamline_plots(analysis, paths, Settings)
    % Generate streamline-only plots from snapshots.
    if ~isfield(analysis, 'omega_snaps')
        return;
    end

    plot_labels = resolve_analysis_plot_labels(analysis);
    omega_snaps = double(analysis.omega_snaps);
    Nsnap = size(omega_snaps, 3);
    [x_vec, y_vec] = resolve_plot_axes(analysis, omega_snaps);
    snap_times = resolve_plot_times(analysis, Nsnap);
    plot_context = resolve_bathymetry_plot_context(analysis, size(omega_snaps, 1), size(omega_snaps, 2));

    fig = figure('Position', [100, 100, 1200, 800]);
    apply_dark_theme_for_figure(fig);
    ncols = min(4, Nsnap);
    nrows = ceil(Nsnap / ncols);

    for k = 1:Nsnap
        ax = subplot(nrows, ncols, k);
        omega_slice = omega_snaps(:, :, k);
        [u, v] = resolve_velocity_slice(analysis, k, omega_slice, x_vec, y_vec, plot_context);
        if ~isempty(plot_context.plot_mask) && isequal(size(plot_context.plot_mask), size(u))
            u(~plot_context.plot_mask) = NaN;
            v(~plot_context.plot_mask) = NaN;
        end
        render_streamlines_only(ax, x_vec, y_vec, u, v, [0.90 0.92 0.95]);
        draw_bathymetry_outline(ax, plot_context.profile_x, plot_context.profile_y, [0.92 0.92 0.95]);
        axis(ax, 'equal', 'tight');
        set(ax, 'YDir', 'normal');
        title(ax, sprintf('t = %.3g s', snap_times(k)));
        xlabel(ax, 'x');
        ylabel(ax, 'y');
        grid(ax, 'on');
    end

    sgtitle(plot_labels.gallery_streamline_title);
    ResultsPlotDispatcher.save_figure_bundle(fig, fullfile(resolve_plot_output_dir(paths, 'figures_streamlines', 'figures_evolution'), 'streamlines.png'), Settings);
    close(fig);
end

function apply_dark_theme_for_figure(fig_handle)
    if isempty(fig_handle) || ~isvalid(fig_handle)
        return;
    end
    try
        ResultsPlotDispatcher.apply_dark_theme(fig_handle, ResultsPlotDispatcher.default_colors());
    catch
        % Plot styling failure should not abort plotting mode.
    end
end

function scaling_params = resolve_plot_scaling_params(Settings)
    scaling_params = struct();
    if nargin < 1 || ~isstruct(Settings)
        return;
    end
    if isfield(Settings, 'results_plot_scaling') && isstruct(Settings.results_plot_scaling)
        scaling_params = Settings.results_plot_scaling;
        return;
    end
    fields = {'plot_trim_layers', 'plot_limit_mode', 'plot_percentile_band', 'plot_include_boundary_diagnostics'};
    for idx = 1:numel(fields)
        key = fields{idx};
        if isfield(Settings, key)
            scaling_params.(key) = Settings.(key);
        end
    end
end

function generate_boundary_strip_plots(analysis, paths, Settings, x_vec, y_vec, snap_times, plot_context, boundary_strip_mask, cmin, cmax)
    omega_snaps = double(analysis.omega_snaps);
    Nsnap = size(omega_snaps, 3);
    fig = figure('Position', [140, 140, 1200, 800]);
    apply_dark_theme_for_figure(fig);
    ncols = min(4, Nsnap);
    nrows = ceil(Nsnap / ncols);

    for k = 1:Nsnap
        ax = subplot(nrows, ncols, k);
        omega_slice = omega_snaps(:, :, k);
        omega_plot = apply_plot_mask(omega_slice, boundary_strip_mask);
        imagesc(ax, x_vec, y_vec, omega_plot);
        set(findobj(ax, 'Type', 'image'), 'AlphaData', double(isfinite(omega_plot)));
        axis(ax, 'equal', 'tight');
        set(ax, 'YDir', 'normal');
        colormap(ax, turbo);
        clim(ax, [cmin cmax]);
        cb = colorbar(ax);
        if isprop(cb, 'Color')
            cb.Color = [0.90 0.92 0.95];
        end
        draw_bathymetry_outline(ax, plot_context.profile_x, plot_context.profile_y, [0.92 0.92 0.95]);
        title(ax, sprintf('Boundary strip, t = %.3g s', snap_times(k)));
    end

    sgtitle('Boundary-strip vorticity diagnostics');
    ResultsPlotDispatcher.save_figure_bundle(fig, fullfile(resolve_plot_output_dir(paths, 'figures_contours', 'figures_evolution'), 'contours_boundary_strips.png'), Settings);
    close(fig);
end

function [x_vec, y_vec] = resolve_plot_axes(analysis, omega_snaps)
    ny = size(omega_snaps, 1);
    nx = size(omega_snaps, 2);
    x_vec = 1:nx;
    y_vec = 1:ny;
    if isfield(analysis, 'x') && numel(analysis.x) == nx
        x_vec = double(analysis.x(:)).';
    end
    if isfield(analysis, 'y') && numel(analysis.y) == ny
        y_vec = double(analysis.y(:)).';
    end
end

function snap_times = resolve_plot_times(analysis, n_snapshots)
    snap_times = 0:(n_snapshots - 1);
    if isfield(analysis, 'snapshot_times_requested') && numel(analysis.snapshot_times_requested) == n_snapshots
        snap_times = double(analysis.snapshot_times_requested(:)).';
    elseif isfield(analysis, 'snapshot_times') && numel(analysis.snapshot_times) == n_snapshots
        snap_times = double(analysis.snapshot_times(:)).';
    end
end

function [cmin, cmax] = resolve_color_limits(omega_snaps, plot_mask)
    if nargin >= 2 && ~isempty(plot_mask) && ismatrix(plot_mask) && ...
            size(plot_mask, 1) == size(omega_snaps, 1) && size(plot_mask, 2) == size(omega_snaps, 2)
        mask3 = repmat(logical(plot_mask), 1, 1, size(omega_snaps, 3));
        finite_vals = omega_snaps(mask3 & isfinite(omega_snaps));
    else
        finite_vals = omega_snaps(isfinite(omega_snaps));
    end
    if isempty(finite_vals)
        cmin = -1;
        cmax = 1;
        return;
    end

    cmin = min(finite_vals);
    cmax = max(finite_vals);
    if ~isfinite(cmin) || ~isfinite(cmax) || cmax <= cmin
        cmin = -1;
        cmax = 1;
    end
end

function psi_slice = resolve_streamfunction_slice(analysis, idx, omega_slice, x_vec, y_vec)
    if isfield(analysis, 'psi_snaps') && ndims(analysis.psi_snaps) >= 3 && ...
            size(analysis.psi_snaps, 3) >= idx && ...
            isequal(size(analysis.psi_snaps(:, :, idx)), size(omega_slice))
        psi_slice = double(analysis.psi_snaps(:, :, idx));
        psi_slice(~isfinite(psi_slice)) = 0;
        return;
    end

    [psi_slice, ~, ~] = velocity_from_omega_slice(omega_slice, x_vec, y_vec);
    psi_slice(~isfinite(psi_slice)) = 0;
end

function [u, v] = resolve_velocity_slice(analysis, idx, omega_slice, x_vec, y_vec, plot_context)
    if nargin < 6 || ~isstruct(plot_context)
        plot_context = struct('requires_velocity_snapshots', false);
    end
    has_snapshots = isfield(analysis, 'u_snaps') && isfield(analysis, 'v_snaps') && ...
        ndims(analysis.u_snaps) >= 3 && ndims(analysis.v_snaps) >= 3 && ...
        size(analysis.u_snaps, 3) >= idx && size(analysis.v_snaps, 3) >= idx && ...
        isequal(size(analysis.u_snaps(:, :, idx)), size(omega_slice)) && ...
        isequal(size(analysis.v_snaps(:, :, idx)), size(omega_slice));
    if has_snapshots
        u = double(analysis.u_snaps(:, :, idx));
        v = double(analysis.v_snaps(:, :, idx));
        u(~isfinite(u)) = 0;
        v(~isfinite(v)) = 0;
        return;
    end
    if isfield(plot_context, 'requires_velocity_snapshots') && plot_context.requires_velocity_snapshots
        error('mode_plotting:MissingVelocitySnapshotsForWallDomain', ...
            ['FD wall-domain plotting requires solver velocity snapshots; ' ...
             'periodic FFT reconstruction is not allowed.']);
    end

    [~, u, v] = velocity_from_omega_slice(omega_slice, x_vec, y_vec);
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
    % Repository convention: u = -dpsi/dy, v = dpsi/dx.
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

function render_streamlines_only(ax, x_vec, y_vec, u, v, line_color)
    ResultsPlotDispatcher.render_deterministic_streamlines(ax, x_vec, y_vec, u, v, line_color, struct());
end

function levels = resolve_contour_level_count(Settings)
    levels = 36;
    candidates = {Settings};
    try
        defaults = create_default_parameters();
        candidates{end + 1} = defaults; %#ok<AGROW>
        if isfield(defaults, 'phase2') && isstruct(defaults.phase2)
            candidates{end + 1} = defaults.phase2; %#ok<AGROW>
        end
    catch
    end
    for idx = 1:numel(candidates)
        source = candidates{idx};
        if isstruct(source) && isfield(source, 'contour_levels') && isnumeric(source.contour_levels) && ...
                isscalar(source.contour_levels) && isfinite(source.contour_levels)
            levels = max(8, round(double(source.contour_levels)));
            return;
        end
    end
end

function masked = apply_plot_mask(field, plot_mask)
    masked = double(field);
    if ~isempty(plot_mask) && isequal(size(masked), size(plot_mask))
        masked(~plot_mask) = NaN;
    end
end

function draw_bathymetry_outline(ax, profile_x, profile_y, line_color)
    if isempty(profile_x) || isempty(profile_y) || numel(profile_x) ~= numel(profile_y)
        return;
    end
    hold(ax, 'on');
    plot(ax, double(profile_x(:)).', double(profile_y(:)).', ...
        'LineWidth', 1.4, 'LineStyle', '-', 'Color', line_color);
    hold(ax, 'off');
end

function output_dir = resolve_plot_output_dir(paths, preferred_field, fallback_field)
    output_dir = pwd;
    if isstruct(paths) && isfield(paths, preferred_field) && ~isempty(paths.(preferred_field))
        output_dir = paths.(preferred_field);
        return;
    end
    if isstruct(paths) && isfield(paths, fallback_field) && ~isempty(paths.(fallback_field))
        output_dir = paths.(fallback_field);
    end
end
