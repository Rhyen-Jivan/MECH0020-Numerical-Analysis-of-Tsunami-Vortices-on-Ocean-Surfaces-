%% FINITE DIFFERENCE SOLVER FOR 2D VORTICITY-STREAMFUNCTION FORMULATION
%
% Purpose:
%   Solves the 2D incompressible Navier-Stokes equations in vorticity-
%   streamfunction form using finite difference spatial discretization and
%   RK3-SSP time integration (from ODE solver specified in Parameters).
%
% Main Function:
%   [fig_handle, analysis] = Finite_Difference_Analysis(Parameters)
%
% Physics Solved:
%   Vorticity equation:    ∂ω/∂t + u·∇ω = ν∇²ω
%   Poisson equation:      ∇²ψ = -ω
%   Velocity recovery:     u = -∂ψ/∂y, v = ∂ψ/∂x
%
% Spatial Discretization:
%   - Method: 2nd-order finite differences on regular Cartesian grid
%   - Advection: Arakawa 3-point scheme (energy-conserving)
%   - Diffusion: Standard 5-point stencil
%   - Boundary: Periodic (via circshift-based shifts)
%   - Poisson solver: Sparse matrix A = (1/dx²)⊗Tx + (1/dy²)Ty⊗I
%
% Time Integration:
%   - Method: RK3-SSP (3rd-order Strong Stability Preserving)
%   - ODE solver: ode45 (MATLAB's adaptive Runge-Kutta)
%   - CFL condition: dt must satisfy standard explicit stability criterion
%
% Output Structure 'analysis':
%   - omega_snaps     : Vorticity at snap_times (Ny × Nx × Nsnap)
%   - psi_snaps       : Streamfunction snapshots
%   - kinetic_energy  : Time history of kinetic energy
%   - enstrophy       : Time history of enstrophy (ω² integrated)
%   - time_vec        : Time points of simulation
%   - peak_vorticity  : Maximum vorticity during run

function [fig_handle, analysis] = Finite_Difference_Analysis(Parameters)

    required_fields = {'nu','Lx','Ly','Nx','Ny','dt','Tfinal','snap_times','ic_type'};
    for k = 1:numel(required_fields)
        if ~isfield(Parameters, required_fields{k})
            error('Missing required field: %s', required_fields{k});
        end
    end

    if ~isfield(Parameters,'mode')
        Parameters.mode = "solve";
    end
    if ~isfield(Parameters,'progress_stride')
        Parameters.progress_stride = 0;
    end
    if ~isfield(Parameters,'live_preview')
        Parameters.live_preview = false;
    end
    if ~isfield(Parameters,'live_stride')
        Parameters.live_stride = 0;
    end
    if isfield(Parameters, 'ic_coeff')
        ic_coeff = Parameters.ic_coeff;
    else
        ic_coeff = [];  % Default empty if not specified
    end
    nu = Parameters.nu;
    Lx = Parameters.Lx;
    Ly = Parameters.Ly;
    Nx = Parameters.Nx;
    Ny = Parameters.Ny;
    dt = Parameters.dt;
    Tfinal = Parameters.Tfinal;
    snap_times = Parameters.snap_times;
    ic_type = Parameters.ic_type;
    cpu0_setup = cputime;
    t0_setup = tic;
    setup = fd_setup(Parameters);
    setup_wall_time_s = toc(t0_setup);
    setup_cpu_time_s = cputime - cpu0_setup;

    A = setup.A;  % Define A immediately after setup
    dx = setup.dx;
    dy = setup.dy;
    delta = setup.delta;
    X = setup.X;
    Y = setup.Y;
    x = X(1,:);
    y = Y(:,1).';

    % Handle initial condition: use pre-computed omega from Parameters if available
    if isfield(Parameters, 'omega') && ~isempty(Parameters.omega)
        omega0 = Parameters.omega;
        fprintf('[FD SOLVER] Using pre-computed omega from Parameters: size %d x %d, range [%.4f, %.4f]\n', ...
            size(omega0, 1), size(omega0, 2), min(omega0(:)), max(omega0(:)));
    else
        % Fallback: compute omega from IC type (should not reach here if prepare_simulation_params was called)
        fprintf('[FD SOLVER] WARNING: Parameters.omega not found, computing from IC type\n');
        if isfield(Parameters, 'ic_coeff')
            ic_coeff = Parameters.ic_coeff;
        else
            ic_coeff = [];
        end
        omega0 = initialise_omega(X, Y, ic_type, ic_coeff);
        fprintf('[FD SOLVER] Computed omega from IC: range [%.4f, %.4f]\n', ...
            min(omega0(:)), max(omega0(:)));
    end
    omega = reshape(omega0, Ny, Nx);

    Nt = round(Tfinal / dt);
    Nsnap = numel(snap_times);

    omega_snaps = zeros(Ny, Nx, Nsnap);
    omega_snaps(:,:,1) = omega;

    psi_snaps = zeros(Ny, Nx, Nsnap);  % Initialize psi snapshots
    psi_vec = delta^2 * (A \ omega(:));  % Now A is defined
    psi_snaps(:,:,1) = reshape(psi_vec, Ny, Nx);

    snap_index  = 2;
    if Nsnap >= 2
        next_snap_t = snap_times(snap_index);
    else
        next_snap_t = inf;
    end

    shift_xp = @(A) circshift(A, [0, +1]);
    shift_xm = @(A) circshift(A, [0, -1]);
    shift_yp = @(A) circshift(A, [+1, 0]);
    shift_ym = @(A) circshift(A, [-1, 0]);

    if Parameters.progress_stride <= 0
        progress_stride = max(1, round(Nt/20));
    else
        progress_stride = Parameters.progress_stride;
    end

    if Parameters.live_stride <= 0
        live_stride = max(1, round(Nt/40));
    else
        live_stride = Parameters.live_stride;
    end

    live_fig = [];
    live_im = [];
    if logical(Parameters.live_preview)
        live_fig = figure;
        live_im = imagesc(x, y, omega);
        axis equal tight
        set(gca,'YDir','normal')
        colormap(turbo)
        colorbar
        title(sprintf('Live vorticity preview: t = %.3f', 0))
        drawnow
    end

    t = 0;
    cpu0_solve = cputime;
    t0_solve = tic;
    
    % Initialize live monitoring for this simulation
    global monitor_data monitor_figure; %#ok<GVMIS>
    if isempty(monitor_data)
        monitor_data = struct();
        monitor_data.performance = struct();
        monitor_data.performance.iteration_times = [];
        monitor_data.performance.memory_usage = [];
        monitor_data.performance.monitor_overhead = 0;
    end
    if (isempty(monitor_figure) || ~isvalid(monitor_figure)) && exist('create_live_monitor_dashboard', 'file') == 2
        monitor_figure = create_live_monitor_dashboard();
    end
    if ~isfield(monitor_data, 'ui') || isempty(monitor_data.ui)
        if exist('create_live_monitor_dashboard', 'file') == 2
            monitor_figure = create_live_monitor_dashboard();
        end
    end
    use_live_monitor = ~isempty(monitor_figure) && isvalid(monitor_figure) && isfield(monitor_data, 'ui') && ~isempty(monitor_data.ui);
    if use_live_monitor
        monitor_data.total_iterations = Nt;
        monitor_data.current_phase = sprintf('Time Integration (Nx=%d, Ny=%d)', Nx, Ny);
    end
    
    % Monitoring update frequency (update every N iterations to reduce overhead)
    monitor_update_stride = max(1, round(Nt/100));  % Update ~100 times during simulation

    for n = 1:Nt
        k1 = rhs_fd_arakawa(omega(:), A, dx, dy, nu, shift_xp, shift_xm, shift_yp, shift_ym, Nx, Ny, delta);
        k2 = rhs_fd_arakawa(omega(:) + 0.5*dt*k1, A, dx, dy, nu, shift_xp, shift_xm, shift_yp, shift_ym, Nx, Ny, delta);
        k3 = rhs_fd_arakawa(omega(:) + 0.5*dt*k2, A, dx, dy, nu, shift_xp, shift_xm, shift_yp, shift_ym, Nx, Ny, delta);
        k4 = rhs_fd_arakawa(omega(:) + dt*k3, A, dx, dy, nu, shift_xp, shift_xm, shift_yp, shift_ym, Nx, Ny, delta);
        % Update omega using RK4
        omega(:) = omega(:) + (dt/6) * (k1 + 2*k2 + 2*k3 + k4);
        
        t = t + dt;
        if snap_index <= Nsnap && t >= next_snap_t - 1e-12
            omega_snaps(:,:,snap_index) = omega;
            psi_vec = delta^2 * (A \ omega(:));
            psi_snaps(:,:,snap_index) = reshape(psi_vec, Ny, Nx);
            snap_index = snap_index + 1;
            if snap_index <= Nsnap
                next_snap_t = snap_times(snap_index);
            end
        end
        if mod(n, progress_stride) == 0 || n == 1 || n == Nt
            if ic_type == "stretched_gaussian"
                ic_coeff_str = mat2str(ic_coeff);
            else
                ic_coeff_str = "N/A";
            end
            % Always use \n for proper newline formatting
            fprintf("FD | %6.2f%% | t = %.3f / %.3f s | Nx = %d Ny = %d | nu = %.3e m^2/s | dt = %.3e s | delta = %.3e m | IC = %s | ic_coeff = %s\n", ...
                100*n/Nt, t, Tfinal, Nx, Ny, nu, dt, delta, string(ic_type), ic_coeff_str);
        end
        
        % Update live monitor dashboard
        if use_live_monitor && (mod(n, monitor_update_stride) == 0 || n == 1 || n == Nt)
            % Calculate current simulation metrics
            max_vorticity = max(abs(omega(:)));
            enstrophy = sum(omega(:).^2) * dx * dy;
            
            % Pack metrics for dashboard
            metrics = struct();
            metrics.grid_size = [Nx, Ny];
            metrics.time_steps = Nt;
            metrics.max_vorticity = max_vorticity;
            metrics.total_energy = enstrophy;  % Using enstrophy as energy proxy
            metrics.convergence_metric = NaN;  % Not applicable during time integration
            
            % Update dashboard (only if function is available)
            if exist('update_live_monitor', 'file') == 2
                update_live_monitor(n, Nt, monitor_data.current_phase, metrics);
            else
                use_live_monitor = false;
            end
        end
        
        % Update live preview if enabled
        if logical(Parameters.live_preview) && mod(n, live_stride) == 0 && ~isempty(live_fig) && isvalid(live_fig)
            set(live_im, 'CData', omega);
            title(live_fig.CurrentAxes, sprintf('Live vorticity preview: t = %.3f', t));
            drawnow limitrate;
        end
    end
    
    % Close live preview figure if it exists
    if logical(Parameters.live_preview) && ~isempty(live_fig) && isvalid(live_fig)
        close(live_fig);
    end

    solve_wall_time_s = toc(t0_solve);
    solve_cpu_time_s = cputime - cpu0_solve;

    analysis = struct;  % Move analysis struct creation before figures

    analysis.method = "FD + Arakawa + RK4";
    analysis.nu = nu;

    analysis.Lx = Lx;
    analysis.Ly = Ly;
    analysis.Nx = Nx;
    analysis.Ny = Ny;
    analysis.dx = dx;
    analysis.dy = dy;
    analysis.delta = delta;

    analysis.dt = dt;
    analysis.Tfinal = Tfinal;
    analysis.Nt = Nt;

    analysis.grid_points = Nx * Ny;
    analysis.unknowns = analysis.grid_points;

    analysis.rhs_calls = 4 * Nt;
    analysis.poisson_solves = analysis.rhs_calls;

    analysis.setup_wall_time_s = setup_wall_time_s;
    analysis.setup_cpu_time_s = setup_cpu_time_s;

    analysis.solve_wall_time_s = solve_wall_time_s;
    analysis.solve_cpu_time_s = solve_cpu_time_s;

    analysis.wall_time_s = setup_wall_time_s + solve_wall_time_s;
    analysis.cpu_time_s  = setup_cpu_time_s + solve_cpu_time_s;

    analysis.snapshot_times = snap_times(:);
    analysis.snapshots_stored = Nsnap;  % Ensure all 9 snapshots are accounted for
    analysis.omega_snaps = omega_snaps;
    analysis.psi_snaps = psi_snaps;
    analysis.time_vec = snap_times(:);

    % === UNIFIED METRICS EXTRACTION ===
    % Use comprehensive metrics framework for consistency across all methods
    if exist('extract_unified_metrics', 'file') == 2
        unified_metrics = extract_unified_metrics(omega_snaps, psi_snaps, snap_times, dx, dy, Parameters);
        
        % Merge unified metrics into analysis struct
        analysis = mergestruct(analysis, unified_metrics);
        
        fprintf('[FD] Unified metrics extraction complete: %d metrics loaded\n', length(fieldnames(unified_metrics)));
    else
        % Fallback: compute basic metrics if helper function not available
        % Peak absolute vorticity
        omega_final = omega_snaps(:,:,end);
        psi_final = psi_snaps(:,:,end);
        
        analysis.peak_abs_omega = max(abs(omega_final(:)));
        
        % Enstrophy = (1/2) * integral of omega^2
        analysis.enstrophy = 0.5 * sum(omega_final(:).^2) * (dx * dy);
        
        % Velocity components at final time (using shift operators defined earlier)
        u_final = -(shift_yp(psi_final) - shift_ym(psi_final)) / (2 * dy);
        v_final = (shift_xp(psi_final) - shift_xm(psi_final)) / (2 * dx);
        speed_final = sqrt(u_final.^2 + v_final.^2);
        
        analysis.peak_u = max(abs(u_final(:)));
        analysis.peak_v = max(abs(v_final(:)));
        analysis.peak_speed = max(speed_final(:));
        
        % Store u,v snapshots for streamline plotting
        analysis.u_snaps = NaN(Ny, Nx, Nsnap);
        analysis.v_snaps = NaN(Ny, Nx, Nsnap);
        
        for k = 1:Nsnap
            psi_snap = psi_snaps(:,:,k);
            analysis.u_snaps(:,:,k) = -(shift_yp(psi_snap) - shift_ym(psi_snap)) / (2 * dy);
            analysis.v_snaps(:,:,k) = (shift_xp(psi_snap) - shift_xm(psi_snap)) / (2 * dx);
        end
        
        fprintf('[DIAGNOSTICS] peak_abs_omega=%.6e s^-1, enstrophy=%.6e s^-2, peak_u=%.6e m/s, peak_v=%.6e m/s, peak_speed=%.6e m/s\n', ...
            analysis.peak_abs_omega, analysis.enstrophy, analysis.peak_u, analysis.peak_v, analysis.peak_speed);
        
        % Verification that values are finite
        if ~isfinite(analysis.peak_abs_omega)
            warning('[DIAGNOSTICS] peak_abs_omega is NaN or Inf!');
        end
        if ~isfinite(analysis.enstrophy)
            warning('[DIAGNOSTICS] enstrophy is NaN or Inf!');
        end
        if ~isfinite(analysis.peak_u) || ~isfinite(analysis.peak_v) || ~isfinite(analysis.peak_speed)
            warning('[DIAGNOSTICS] Velocity diagnostics contain NaN or Inf!');
        end
    end

    if ~isfield(analysis, 'peak_abs_omega') || isempty(analysis.peak_abs_omega)
        analysis.peak_abs_omega = max(abs(omega_snaps(:)));
    end
    analysis.peak_vorticity = analysis.peak_abs_omega;
    
    % Poisson matrix properties
    analysis.poisson_matrix_n = analysis.grid_points;
    analysis.poisson_matrix_nnz = nnz(A);

    analysis.memory_fields = {
        'omega', [Ny, Nx];
        'psi',   [Ny, Nx];
        'omega_snaps', [Ny, Nx, Nsnap];
        'psi_snaps', [Ny, Nx, Nsnap]  % Add psi_snaps to memory fields
    };

    % Plot formatting settings (from Parameters or defaults)
    plot_settings = get_plot_settings(Parameters);

    show_figs = usejava('desktop') && ~strcmpi(get(0, 'DefaultFigureVisible'), 'off');

    if ~show_figs
        fig_handle = figure('Visible', 'off');
        return;
    end

    fig_handle = figure;  % First figure
    tiledlayout(3,3,'TileSpacing','compact');

    for k = 1:analysis.snapshots_stored  % Now analysis is defined
        nexttile;
        contourf(x, y, omega_snaps(:,:,k), 25, 'LineColor', 'none');
        axis equal tight;
        set(gca,'YDir','normal');
        title(sprintf('t = %.3g s', snap_times(k)));
        apply_plot_format_to_axes(gca, plot_settings, '$x$ (m)', '$y$ (m)', '');
    end

    colormap(plot_settings.Colormap);
    cb = colorbar;
    cb.Layout.Tile = 'east';
    ylabel(cb, 'Vorticity (s^{-1})', 'Interpreter', 'tex');
    sgtitle('Finite-difference vorticity evolution \omega(x,y,t)');
    set(gcf, 'Name', 'Evolution');  % Set figure name for categorization

    figure  % Second figure - Contour plot method can be customized
    tiledlayout(3,3,'TileSpacing','compact');
    
    % Get visualization method from Parameters (default: contourf)
    contour_method = 'contourf';  % Default: filled contours
    contour_levels = 25;           % Default: 25 levels
    if isfield(Parameters, 'visualization')
        if isfield(Parameters.visualization, 'contour_method')
            contour_method = Parameters.visualization.contour_method;
        end
        if isfield(Parameters.visualization, 'contour_levels')
            contour_levels = Parameters.visualization.contour_levels;
        end
    end

    for k = 1:analysis.snapshots_stored
        nexttile;
        contourf(x, y, omega_snaps(:,:,k), 25, 'LineColor', 'none');
        hold on;
        if all(isfinite(psi_snaps(:,:,k)), 'all')
            % Use selected contour method
            if strcmpi(contour_method, 'contourf')
                % Filled contours (more visually appealing)
                contourf(x, y, psi_snaps(:,:,k), contour_levels, 'k', 'LineWidth', plot_settings.LineWidth);
            else
                % Line contours (traditional style)
                contour(x, y, psi_snaps(:,:,k), contour_levels, 'k', 'LineWidth', plot_settings.LineWidth);
            end
        end
        hold off;
        axis equal tight;
        set(gca,'YDir','normal');
        title(sprintf('t = %.3g s', snap_times(k)));
        apply_plot_format_to_axes(gca, plot_settings, '$x$ (m)', '$y$ (m)', '');
    end

    colormap(plot_settings.Colormap);
    cb = colorbar;
    cb.Layout.Tile = 'east';
    ylabel(cb, 'Vorticity (s^{-1})', 'Interpreter', 'tex');
    sgtitle('Vortex evolution: vorticity (colour) and streamfunction (contours)');
    set(gcf, 'Name', 'Contour');  % Set figure name for categorization

    figure  % Third figure - Vectorised (with multiple methods)
    tiledlayout(3,3,'TileSpacing','compact');

    % Get vector visualization method and settings from Parameters
    vector_method = 'quiver';      % Default: quiver arrows
    vector_stride = 4;              % Default: subsampling stride
    vector_scale = 1.0;             % Default: auto-scaling
    if isfield(Parameters, 'visualization')
        if isfield(Parameters.visualization, 'vector_method')
            vector_method = Parameters.visualization.vector_method;
        end
        if isfield(Parameters.visualization, 'vector_subsampling')
            vector_stride = Parameters.visualization.vector_subsampling;
        end
        if isfield(Parameters.visualization, 'vector_scale')
            vector_scale = Parameters.visualization.vector_scale;
        end
    end

    for k = 1:analysis.snapshots_stored
        nexttile;
        psi = psi_snaps(:,:,k);
        % Compute velocity components using finite differences
        u = -(shift_yp(psi) - shift_ym(psi)) / (2 * dy);  % -dpsi/dy
        v = (shift_xp(psi) - shift_xm(psi)) / (2 * dx);   % dpsi/dx
        speed = sqrt(u.^2 + v.^2);
        imagesc(x, y, speed);  % Use x, y vectors
        hold on;
        
        % Choose visualization method
        if strcmpi(vector_method, 'streamlines')
            % Streamlines (flow visualization)
            [X_stream, Y_stream] = meshgrid(x(1:vector_stride:end), y(1:vector_stride:end));
            u_stream = u(1:vector_stride:end, 1:vector_stride:end);
            v_stream = v(1:vector_stride:end, 1:vector_stride:end);
            streamline(X_stream, Y_stream, u_stream, v_stream, X_stream, Y_stream, 'k', 'LineWidth', 0.5);
        else
            % Default: Quiver arrows
            x_sub = x(1:vector_stride:end);
            y_sub = y(1:vector_stride:end);
            [Xq, Yq] = meshgrid(x_sub, y_sub);
            u_sub = u(1:vector_stride:end, 1:vector_stride:end);
            v_sub = v(1:vector_stride:end, 1:vector_stride:end);
            if vector_scale ~= 1.0
                quiver(Xq, Yq, u_sub, v_sub, vector_scale, 'k', 'LineWidth', plot_settings.LineWidth);  % Plot vectors with scale
            else
                quiver(Xq, Yq, u_sub, v_sub, 'k', 'LineWidth', plot_settings.LineWidth);  % Auto-scale
            end
        end
        hold off;
        axis equal tight;
        set(gca,'YDir','normal');
        title(sprintf('t = %.2f s', snap_times(k)));
        apply_plot_format_to_axes(gca, plot_settings, '$x$ (m)', '$y$ (m)', '');
    end

    colormap(plot_settings.Colormap);
    cb = colorbar;
    cb.Layout.Tile = 'east';
    ylabel(cb, 'Speed (m/s)', 'Interpreter', 'tex');
    sgtitle('Velocity field: direction (vectors) and speed (colour)');
    set(gcf, 'Name', 'Vectorised');  % Set figure name for categorization

    % Fourth figure - Streamlines overlaid on vorticity
    figure
    tiledlayout(3,3,'TileSpacing','compact');
    % Get streamline settings from Parameters
    streamline_density = 4;     % Default: moderate streamline density
    streamline_color_mode = "vorticity";  % Default: color by vorticity influence
    streamline_color = 'k';     % Default: black streamlines
    streamline_width = 1.0;     % Default: line width
    if isfield(Parameters, 'visualization')
        if isfield(Parameters.visualization, 'streamline_density')
            streamline_density = Parameters.visualization.streamline_density;
        end
        if isfield(Parameters.visualization, 'streamline_color_mode')
            streamline_color_mode = Parameters.visualization.streamline_color_mode;
        end
        if isfield(Parameters.visualization, 'streamline_color')
            streamline_color = Parameters.visualization.streamline_color;
        end
        if isfield(Parameters.visualization, 'streamline_width')
            streamline_width = Parameters.visualization.streamline_width;
        end
    end
    for k = 1:analysis.snapshots_stored
        nexttile;
        psi = psi_snaps(:,:,k);
        omega = omega_snaps(:,:,k);
        % Compute velocity components
        u = -(shift_yp(psi) - shift_ym(psi)) / (2 * dy);
        v = (shift_xp(psi) - shift_xm(psi)) / (2 * dx);
        % Plot vorticity as background
        contourf(x, y, omega, 25, 'LineColor', 'none');
        hold on;
        % Create streamlines using stream2 for better control
        stream_x = linspace(x(1), x(end), streamline_density * 3);
        stream_y = linspace(y(1), y(end), streamline_density * 3);
        [start_x, start_y] = meshgrid(stream_x, stream_y);
        [X, Y] = meshgrid(x, y);
        streamlines = stream2(X, Y, v, u, start_x, start_y);
        if strcmpi(string(streamline_color_mode), "vorticity")
            draw_vorticity_colored_streamlines(streamlines, X, Y, omega, streamline_width);
        else
            h = streamline(streamlines);
            set(h, 'Color', streamline_color, 'LineWidth', streamline_width);
        end
        hold off;
        axis equal tight;
        set(gca,'YDir','normal');
        title(sprintf('t = %.3g s', snap_times(k)));
        apply_plot_format_to_axes(gca, plot_settings, '$x$ (m)', '$y$ (m)', '');
    end
    colormap(plot_settings.Colormap);
    cb = colorbar;
    cb.Layout.Tile = 'east';
    ylabel(cb, 'Vorticity (s^{-1})', 'Interpreter', 'tex');
    sgtitle('Flow streamlines overlaid on vorticity field');
    set(gcf, 'Name', 'Streamlines');

    % Create vorticity animation if requested
    % Enable animations for all modes except convergence non-converged studies
    should_animate = true;  % Default: enable animations
    
    % Disable for convergence mode with non-converged meshes
    if isfield(Parameters, 'mode') && strcmpi(Parameters.mode, 'convergence')
        if isfield(Parameters, 'converged') && ~Parameters.converged
            should_animate = false;  % Skip animation for non-converged convergence studies
        end
    end
    
    % Allow explicit override if animation_fps is set
    if isfield(Parameters, 'animation_fps')
        if Parameters.animation_fps <= 0
            should_animate = false;
        else
            should_animate = true;
        end
    end
    
    % Allow create_animations field to override
    if isfield(Parameters, 'create_animations')
        should_animate = should_animate && Parameters.create_animations;
    end
    
    % Debug animation status
    if isfield(Parameters, 'animation_fps')
        fps_val = Parameters.animation_fps;
    else
        fps_val = 0;
    end
    if isfield(Parameters, 'create_animations')
        create_val = Parameters.create_animations;
    else
        create_val = 1;
    end
    fprintf('[ANIMATION] should_animate=%d, mode=%s, fps=%d, create_animations=%d\n', ...
        should_animate, string(Parameters.mode), fps_val, create_val);
    
    if should_animate
        % Get animation format (default to GIF if not specified)
        if isfield(Parameters, 'animation_format')
            anim_format = lower(Parameters.animation_format);
        else
            anim_format = 'gif';
        end
        
        fps = Parameters.animation_fps;
        
        % DEBUG: Print snapshot info
        fprintf('[ANIMATION] Total snapshots stored: %d, snap_times: [%.3f to %.3f]\n', ...
            analysis.snapshots_stored, snap_times(1), snap_times(min(end, analysis.snapshots_stored)));
        fprintf('[ANIMATION] omega_snaps size: [%d x %d x %d]\n', size(omega_snaps, 1), size(omega_snaps, 2), size(omega_snaps, 3));
        
        % Construct filename with timestamp and parameters
        timestamp = char(datetime('now', 'Format', 'yyyyMMdd_HHmmss'));
        filename_base = sprintf('vorticity_evolution_Nx%d_Ny%d_nu%.4f_dt%.4f_Tfinal%.1f_ic_%s_mode_%s_%s', ...
            Nx, Ny, nu, dt, Tfinal, ic_type, Parameters.mode, timestamp);
        
        if isfield(Parameters, 'animation_dir') && ~isempty(Parameters.animation_dir)
            % Ensure base animation directory exists
            if ~exist(Parameters.animation_dir, 'dir')
                mkdir(Parameters.animation_dir);
            end
            % Create mode-specific subdirectory
            mode_dir = "unknown";
            if isfield(Parameters, 'mode') && strlength(string(Parameters.mode)) > 0
                mode_dir = string(Parameters.mode);
            end
            mode_dir = regexprep(mode_dir, "\s+", "_");
            mode_dir = regexprep(mode_dir, "[^a-zA-Z0-9_\-]", "");
            mode_path = fullfile(Parameters.animation_dir, mode_dir);
            if ~exist(mode_path, 'dir')
                mkdir(mode_path);
            end
            base_path = fullfile(mode_path, filename_base);
        else
            % Use current directory if animation_dir not specified
            base_path = filename_base;
        end
        
        fprintf('[ANIMATION] base_path = "%s"\n', base_path);
        
        % Create animation figure
        fig_anim = figure('Name', 'Animation', 'NumberTitle', 'off');
        
        % Format-specific animation creation
        switch anim_format
            case 'gif'
                % GIF animation (existing method)
                base_path_char = char(base_path);  % Convert string to char first
                filename = [base_path_char '.gif'];  % Now concatenation works correctly
                delay_time = 1 / fps;
                
                fprintf('[ANIMATION] Creating GIF: %s\n', filename);
                fprintf('[ANIMATION] fps = %.2f, delay_time = %.4f s\n', fps, delay_time);
                fprintf('[ANIMATION] Generating %d frames...\n', analysis.snapshots_stored);
                
                frame_count = 0;
                for k = 1:analysis.snapshots_stored
                    imagesc(x, y, omega_snaps(:,:,k));
                    axis equal tight;
                    set(gca, 'YDir', 'normal');
                    colormap(turbo);
                    cb = colorbar;
                    ylabel(cb, 'Vorticity (s^{-1})', 'Interpreter', 'tex');
                    title(sprintf('Vorticity evolution: t = %.3f s', snap_times(k)));
                    xlabel('$x$ (m)', 'Interpreter', 'latex');
                    ylabel('$y$ (m)', 'Interpreter', 'latex');
                    drawnow;
                    
                    frame = getframe(fig_anim);
                    im = frame2im(frame);
                    [A, map] = rgb2ind(im, 256);
                    
                    if k == 1
                        imwrite(A, map, filename, 'gif', 'LoopCount', inf, 'DelayTime', delay_time);
                    else
                        imwrite(A, map, filename, 'gif', 'WriteMode', 'append', 'DelayTime', delay_time);
                    end
                    frame_count = frame_count + 1;
                    if mod(k, max(1, round(analysis.snapshots_stored/5))) == 0 || k == 1 || k == analysis.snapshots_stored
                        fprintf('[ANIMATION] Frame %d/%d written\n', k, analysis.snapshots_stored);
                    end
                end
                fprintf('[ANIMATION] Vorticity animation saved as GIF: %s (%d frames)\n', filename, frame_count);
                
            case {'mp4', 'avi'}
                % Video animation using VideoWriter (with GIF fallback)
                try
                    base_path_char = char(base_path);  % Convert string to char first
                    if strcmp(anim_format, 'mp4')
                        filename = [base_path_char '.mp4'];
                        if isfield(Parameters, 'animation_codec') && ~isempty(Parameters.animation_codec)
                            profile = string(Parameters.animation_codec);
                        else
                            profile = "MPEG-4";
                        end
                    else  % avi
                        filename = [base_path_char '.avi'];
                        if isfield(Parameters, 'animation_codec') && ~isempty(Parameters.animation_codec)
                            profile = string(Parameters.animation_codec);
                        else
                            profile = "Uncompressed AVI";
                        end
                    end
                    
                    % Ensure profile is a valid string scalar
                    profile = string(profile);
                    if ~isscalar(profile) || strlength(profile) == 0
                        warning('Invalid animation codec. Using default.');
                        profile = "MPEG-4";
                    end
                    
                    % Validate filename before creating VideoWriter
                    if isempty(filename) || ~ischar(filename) && ~isstring(filename)
                        error('Animation filename is empty or invalid. Check animation_dir setting.');
                    end
                    
                    % Ensure profile is char for VideoWriter BEFORE any operations
                    profile_char = char(profile);
                    if isempty(profile_char)
                        profile_char = 'MPEG-4';
                    end
                    
                    fprintf('[ANIMATION] Creating video: %s with profile: %s\n', filename, profile_char);
                    
                    % CRITICAL: profile_char must be char, not string, for VideoWriter
                    v = VideoWriter(filename, profile_char);
                    v.FrameRate = fps;
                    
                    % Set quality for MPEG-4
                    if strcmp(profile, 'MPEG-4') && isfield(Parameters, 'animation_quality')
                        v.Quality = Parameters.animation_quality;
                    end
                    
                    open(v);
                    
                    % Generate frames
                    frame_count = 0;
                    fprintf('[ANIMATION] Generating %d video frames...\n', analysis.snapshots_stored);
                    for k = 1:analysis.snapshots_stored
                        imagesc(x, y, omega_snaps(:,:,k));
                        axis equal tight;
                        set(gca, 'YDir', 'normal');
                        colormap(turbo);
                        cb = colorbar;
                        ylabel(cb, 'Vorticity (s^{-1})', 'Interpreter', 'tex', 'FontSize', 11);
                        title(sprintf('Vorticity evolution: t = %.3f s', snap_times(k)), 'FontSize', 14);
                        xlabel('$x$ (m)', 'Interpreter', 'latex', 'FontSize', 12);
                        ylabel('$y$ (m)', 'Interpreter', 'latex', 'FontSize', 12);
                        
                        frame = getframe(fig_anim);
                        writeVideo(v, frame);
                        frame_count = frame_count + 1;
                        if mod(k, max(1, round(analysis.snapshots_stored/5))) == 0 || k == 1 || k == analysis.snapshots_stored
                            fprintf('[ANIMATION] Frame %d/%d written\n', k, analysis.snapshots_stored);
                        end
                    end
                    close(v);
                    fprintf('[ANIMATION] Vorticity animation saved as %s: %s (%d frames)\n', upper(anim_format), filename, frame_count);
                    
                catch ME
                    % Fallback to GIF if VideoWriter fails
                    fprintf('[ANIMATION] VideoWriter error (%s). Falling back to GIF format.\n', ME.message);
                    base_path_char = char(base_path);  % Convert string to char first
                    filename = [base_path_char '.gif'];
                    delay_time = 1/fps;
                    frame_count = 0;
                    fprintf('[ANIMATION] Generating %d GIF frames (fallback)...\n', analysis.snapshots_stored);
                    
                    for k = 1:analysis.snapshots_stored
                        imagesc(x, y, omega_snaps(:,:,k));
                        axis equal tight;
                        set(gca, 'YDir', 'normal');
                        colormap(turbo);
                        cb = colorbar;
                        ylabel(cb, 'Vorticity (s^{-1})', 'Interpreter', 'tex', 'FontSize', 11);
                        title(sprintf('Vorticity evolution: t = %.3f s', snap_times(k)), 'FontSize', 14);
                        xlabel('$x$ (m)', 'Interpreter', 'latex', 'FontSize', 12);
                        ylabel('$y$ (m)', 'Interpreter', 'latex', 'FontSize', 12);
                        drawnow;
                        
                        frame = getframe(fig_anim);
                        im = frame2im(frame);
                        [A, map] = rgb2ind(im, 256);
                        
                        if k == 1
                            imwrite(A, map, filename, 'gif', 'LoopCount', inf, 'DelayTime', delay_time);
                        else
                            imwrite(A, map, filename, 'gif', 'WriteMode', 'append', 'DelayTime', delay_time);
                        end
                        frame_count = frame_count + 1;
                        if mod(k, max(1, round(analysis.snapshots_stored/5))) == 0 || k == 1 || k == analysis.snapshots_stored
                            fprintf('[ANIMATION] Frame %d/%d written\n', k, analysis.snapshots_stored);
                        end
                    end
                    fprintf('[ANIMATION] Vorticity animation saved as GIF (fallback): %s (%d frames)\n', filename, frame_count);
                end
                
            otherwise
                warning('Unsupported animation format: %s. Using GIF.', anim_format);
                % Fallback to GIF
                base_path_char = char(base_path);  % Convert string to char first
                filename = [base_path_char '.gif'];
                delay_time = 1 / fps;
                
                for k = 1:analysis.snapshots_stored
                    imagesc(x, y, omega_snaps(:,:,k));
                    axis equal tight;
                    set(gca, 'YDir', 'normal');
                    colormap(turbo);
                    colorbar;
                    title(sprintf('Vorticity evolution: t = %.3f', snap_times(k)));
                    drawnow;
                    
                    frame = getframe(fig_anim);
                    im = frame2im(frame);
                    [A, map] = rgb2ind(im, 256);
                    
                    if k == 1
                        imwrite(A, map, filename, 'gif', 'LoopCount', inf, 'DelayTime', delay_time);
                    else
                        imwrite(A, map, filename, 'gif', 'WriteMode', 'append', 'DelayTime', delay_time);
                    end
                end
                fprintf('Vorticity animation saved as GIF: %s\n', filename);
        end
        
        % Keep animation figure open for viewing
    end

end

%% HELPER FUNCTIONS ORGANIZATION
% This section organizes helper functions by purpose:
%
%% SECTION A: PLOT SETTINGS & FORMATTING
%   - get_plot_settings()        : Retrieves plot configuration
%   - apply_plot_format_to_axes() : Applies OWL formatting to axes
%
%% SECTION B: NUMERICAL SOLVER (Arakawa Scheme)
%   - rhs_fd_arakawa()           : Computes RHS using Arakawa 3-point scheme
%                                  Advection: J_Arakawa = (J1 + J2 + J3) / 3
%                                  Diffusion: ν∇²ω using finite differences
%
%% SECTION C: FINITE DIFFERENCE SETUP
%   - fd_setup()                 : Creates Laplacian matrix, meshgrid, spacing
%                                  Sparse matrix A via Kronecker products
%                                  Periodic boundary conditions (circshift-ready)

function plot_settings = get_plot_settings(Parameters)
    % Returns plot settings struct from Parameters or defaults
    plot_settings = struct();
    plot_settings.LineWidth = 1.5;
    plot_settings.FontSize = 12;
    plot_settings.MarkerSize = 8;
    plot_settings.AxisLineWidth = 1.0;
    plot_settings.ColorOrder = lines(7);
    plot_settings.Grid = 'on';
    plot_settings.Box = 'on';
    plot_settings.Interpreter = 'latex';
    plot_settings.Colormap = 'turbo';

    if isfield(Parameters, 'plot_settings') && isstruct(Parameters.plot_settings)
        user = Parameters.plot_settings;
        fields = fieldnames(plot_settings);
        for i = 1:numel(fields)
            f = fields{i};
            if isfield(user, f)
                plot_settings.(f) = user.(f);
            end
        end
    end
end

function apply_plot_format_to_axes(ax, plot_settings, xlab, ylab, tstr)
    % Apply OWL utilities if available; otherwise fall back to basic styling
    if nargin < 5
        tstr = '';
    end
    if nargin < 4
        ylab = '';
    end
    if nargin < 3
        xlab = '';
    end

    if exist('Plot_Format', 'file') == 2
        Plot_Format(xlab, ylab, tstr, 'Default', plot_settings.AxisLineWidth);
    else
        if ~isempty(xlab)
            xlabel(ax, xlab, 'Interpreter', plot_settings.Interpreter);
        end
        if ~isempty(ylab)
            ylabel(ax, ylab, 'Interpreter', plot_settings.Interpreter);
        end
        if ~isempty(tstr)
            title(ax, tstr, 'Interpreter', plot_settings.Interpreter);
        end
    end

    set(ax, 'FontSize', plot_settings.FontSize, ...
        'LineWidth', plot_settings.AxisLineWidth, ...
        'Box', plot_settings.Box, ...
        'XGrid', plot_settings.Grid, ...
        'YGrid', plot_settings.Grid, ...
        'ColorOrder', plot_settings.ColorOrder, ...
        'TickLabelInterpreter', plot_settings.Interpreter);
end

%% HELPER FUNCTIONS: NUMERICAL SOLVER (Arakawa Scheme)
% Computes right-hand-side of vorticity equation with energy conservation

function dwdt = rhs_fd_arakawa(omega_in, A, dx, dy, nu, shift_xp, shift_xm, shift_yp, shift_ym, Nx, Ny, delta)  % Renamed L to A

    psi_vec = delta^2 * (A \ omega_in(:));  % Renamed L to A
    psi = reshape(psi_vec, Ny, Nx);

    omega = reshape(omega_in, Ny, Nx);  % Reshape omega_in to matrix for shifts

    psi_ip = shift_xp(psi);
    psi_im = shift_xm(psi);
    psi_jp = shift_yp(psi);
    psi_jm = shift_ym(psi);

    psi_ipjp = shift_yp(psi_ip);
    psi_ipjm = shift_ym(psi_ip);
    psi_imjp = shift_yp(psi_im);
    psi_imjm = shift_ym(psi_im);

    om = omega;  % Use reshaped omega
    om_ip = shift_xp(om);
    om_im = shift_xm(om);
    om_jp = shift_yp(om);
    om_jm = shift_ym(om);

    om_ipjp = shift_yp(om_ip);
    om_ipjm = shift_ym(om_ip);
    om_imjp = shift_yp(om_im);
    om_imjm = shift_ym(om_im);

    J1 = ( (psi_ip - psi_im).*(om_jp - om_jm) ...
        - (psi_jp - psi_jm).*(om_ip - om_im) ) / (4*dx*dy);

    J2 = ( psi_ip.*(om_ipjp - om_ipjm) ...
        - psi_im.*(om_imjp - om_imjm) ...
        - psi_jp.*(om_ipjp - om_imjp) ...
        + psi_jm.*(om_ipjm - om_imjm) ) / (4*dx*dy);

    J3 = ( psi_ipjp.*(om_jp - om_ip) ...
        - psi_imjm.*(om_im - om_jm) ...
        - psi_imjp.*(om_jp - om_im) ...
        + psi_ipjm.*(om_ip - om_jm) ) / (4*dx*dy);

    J = (J1 + J2 + J3) / 3;

    lap_omega = (shift_xp(om) - 2*om + shift_xm(om)) / dx^2 ...
              + (shift_yp(om) - 2*om + shift_ym(om)) / dy^2;

    dwdt = -J + nu * lap_omega;
    dwdt = dwdt(:);  % Ensure output is vector

end

%% HELPER FUNCTIONS: FINITE DIFFERENCE SETUP
% Creates sparse Laplacian matrix and mesh infrastructure

function setup = fd_setup(Parameters)
    Lx = Parameters.Lx; Ly = Parameters.Ly; Nx = Parameters.Nx; Ny = Parameters.Ny;
    dx = Lx / (Nx - 1); dy = Ly / (Ny - 1);  % Example grid spacing
    x = linspace(-Lx/2, Lx/2, Nx); y = linspace(-Ly/2, Ly/2, Ny);
    [X, Y] = meshgrid(x, y);

    Ex = ones(Nx,1);
    Tx = spdiags([Ex -2*Ex Ex], [-1 0 1], Nx, Nx);
    Tx(1,end) = 1;
    Tx(end,1) = 1;

    Ey = ones(Ny,1);
    Ty = spdiags([Ey -2*Ey Ey], [-1 0 1], Ny, Ny);
    Ty(1,end) = 1;
    Ty(end,1) = 1;

    Ix = speye(Nx);
    Iy = speye(Ny);

    setup.A = kron(Iy, Tx)/dx^2 + kron(Ty, Ix)/dy^2;  

    setup.dx = dx;
    setup.dy = dy;
    setup.delta = Parameters.delta;  % Grid spacing parameter
    setup.X = X;
    setup.Y = Y;

end

function draw_vorticity_colored_streamlines(streamlines, X, Y, omega, line_width)
    if isempty(streamlines)
        return;
    end
    if nargin < 5 || isempty(line_width)
        line_width = 1.0;
    end
    hold_state = ishold;
    if ~hold_state
        hold on;
    end
    for i = 1:numel(streamlines)
        s = streamlines{i};
        if isempty(s) || size(s, 1) < 2
            continue;
        end
        xs = s(:,1);
        ys = s(:,2);
        omega_s = interp2(X, Y, omega, xs, ys, 'linear', NaN);
        zs = zeros(size(xs));
        surface([xs xs], [ys ys], [zs zs], [omega_s omega_s], ...
            'EdgeColor', 'interp', 'FaceColor', 'none', 'LineWidth', line_width);
    end
    if ~hold_state
        hold off;
    end
end

function s_merged = mergestruct(s1, s2)
    % MERGESTRUCT Merge two structs, with s2 values taking precedence for overlapping fields
    s_merged = s1;
    if isempty(s2)
        return;
    end
    fields = fieldnames(s2);
    for i = 1:numel(fields)
        s_merged.(fields{i}) = s2.(fields{i});
    end
end
