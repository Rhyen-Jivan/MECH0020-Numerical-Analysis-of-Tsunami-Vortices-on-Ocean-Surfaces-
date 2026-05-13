function [fig_handle, analysis] = Variable_Bathymetry_Analysis(Parameters)
% VARIABLE_BATHYMETRY_ANALYSIS FD solver with topography-induced vorticity
%
% Extends Finite_Difference_Analysis with bathymetric forcing term
%
% Physics:
%   ω/t + uω = νω + f*(u/x) + β_topo*F_bathymetry
%   where F_bathymetry captures vorticity generation from topography

    required_fields = {'nu','Lx','Ly','Nx','Ny','dt','Tfinal','snap_times','ic_type'};
    for k = 1:numel(required_fields)
        if ~isfield(Parameters, required_fields{k})
            error('Missing required field: %s', required_fields{k});
        end
    end

    Nx = Parameters.Nx;
    Ny = Parameters.Ny;
    Lx = Parameters.Lx;
    Ly = Parameters.Ly;
    dx = Lx / Nx;
    dy = Ly / Ny;
    
    x = linspace(0, Lx - dx, Nx);
    y = linspace(0, Ly - dy, Ny);
    [X, Y] = meshgrid(x, y);
    
    % Load or generate bathymetry
    [bathymetry_field, bath_x, bath_y] = load_bathymetry(Parameters, Nx, Ny, Lx, Ly);
    
    % Interpolate to simulation grid if needed
    if ~(length(bath_x) == Nx && length(bath_y) == Ny)
        [Bath_X, Bath_Y] = meshgrid(bath_x, bath_y);
        bathymetry_field = interp2(Bath_X, Bath_Y, bathymetry_field, X, Y, 'linear', 0);
    end
    
    % Compute bathymetric slopes for forcing
    [dbathy_dx, dbathy_dy] = gradient(bathymetry_field, dx, dy);
    
    % Initial condition
    if exist('initialise_omega', 'file') == 2
        omega = initialise_omega(X, Y, Parameters.ic_type, Parameters.ic_coeff);
    elseif exist('ic_factory', 'file') == 2
        omega = ic_factory(X, Y, Parameters.ic_type, Parameters.ic_coeff);
    else
        omega = exp(-2*(X.^2 + Y.^2));
    end
    
    dt = Parameters.dt;
    Tfinal = Parameters.Tfinal;
    t = 0;
    n = 0;
    
    snap_times = Parameters.snap_times;
    omega_snaps = zeros(Ny, Nx, length(snap_times));
    psi_snaps = zeros(Ny, Nx, length(snap_times));
    time_vec = [];
    
    omega_snaps(:,:,1) = omega;
    psi_snaps(:,:,1) = solve_poisson_bathy(omega, dx, dy);
    time_vec = [time_vec, t];
    snap_idx = 2;
    
    fprintf('[Bathymetry] Grid: %dx%d, Topography forcing enabled\n', Nx, Ny);
    fprintf('[Bathymetry] Bathy range: [%.3f, %.3f]\n', min(bathymetry_field(:)), max(bathymetry_field(:)));
    
    while t < Tfinal && n < 10000
        % Compute stream function
        psi = solve_poisson_bathy(omega, dx, dy);
        
        % Compute velocity
        [u, v] = get_velocity_bathy(psi, dx, dy);
        
        % Arakawa scheme for advection (conservative)
        dudt = arakawa_advect(omega, psi, dx, dy);
        
        % Viscous dissipation
        dudt = dudt + Parameters.nu * laplacian_periodic(omega, dx, dy);
        
        % Bathymetric forcing term
        % ω_bathy = -u/x * b/x - v/y * b/y (simplified)
        [du_dx, ~] = gradient(u, dx, dy);
        [~, dv_dy] = gradient(v, dx, dy);
        bathy_forcing = -(du_dx .* dbathy_dx + dv_dy .* dbathy_dy);
        
        dudt = dudt + 0.1 * bathy_forcing;  % 0.1 = coupling strength
        
        % RK3-SSP time integration
        if n == 0
            omega_rk1 = omega + dt * dudt;
        else
            omega_rk1 = 0.75 * omega + 0.25 * (omega_rk1 + dt * dudt);
        end
        omega = omega_rk1;
        
        t = t + dt;
        n = n + 1;
        
        % Snapshots
        while snap_idx <= length(snap_times) && t >= snap_times(snap_idx)
            omega_snaps(:,:,snap_idx) = omega;
            psi_snaps(:,:,snap_idx) = solve_poisson_bathy(omega, dx, dy);
            time_vec = [time_vec, t]; %#ok<AGROW>
            snap_idx = snap_idx + 1;
        end
        
        if mod(n, max(1, round(Tfinal/dt/20))) == 0
            fprintf('  t=%.3f: ||ω||_=%.4e (with bathy forcing)\n', t, max(abs(omega(:))));
        end
    end
    
    omega_snaps = omega_snaps(:,:,1:snap_idx-1);
    psi_snaps = psi_snaps(:,:,1:snap_idx-1);
    
    analysis = struct();
    analysis.method = 'bathymetry';
    analysis.omega_snaps = omega_snaps;
    analysis.psi_snaps = psi_snaps;
    analysis.snapshot_times = time_vec;
    analysis.snap_times = time_vec;  % Ensure both naming conventions work
    analysis.time_vec = time_vec;
    analysis.snapshots_stored = numel(time_vec);
    analysis.bathymetry_field = bathymetry_field;
    analysis.dx = dx;
    analysis.dy = dy;
    analysis.Nx = Nx;
    analysis.Ny = Ny;
    analysis.grid_points = Nx * Ny;
    analysis.peak_abs_omega = max(abs(omega_snaps(:)));
    analysis.peak_vorticity = analysis.peak_abs_omega;
    
    % === UNIFIED METRICS EXTRACTION ===
    % Use comprehensive metrics framework for consistency across all methods
    if exist('extract_unified_metrics', 'file') == 2
        unified_metrics = extract_unified_metrics(omega_snaps, psi_snaps, time_vec, dx, dy, Parameters);
        
        % Merge unified metrics into analysis struct
        analysis = mergestruct(analysis, unified_metrics);
        
        % Add bathymetry-specific metrics
        analysis.bathymetry_max = max(bathymetry_field(:));
        analysis.bathymetry_min = min(bathymetry_field(:));
        analysis.bathymetry_rms = sqrt(mean(bathymetry_field(:).^2));
    else
        % Fallback: compute basic metrics if helper function not available
        analysis.kinetic_energy = zeros(1, length(time_vec));
        analysis.enstrophy = zeros(1, length(time_vec));
        for i = 1:length(time_vec)
            omega_t = omega_snaps(:,:,i);
            psi_t = psi_snaps(:,:,i);
            
            [dpsi_dx, dpsi_dy] = gradient(psi_t);
            dpsi_dx = dpsi_dx / dx;
            dpsi_dy = dpsi_dy / dy;
            analysis.kinetic_energy(i) = 0.5 * sum(sum(dpsi_dx.^2 + dpsi_dy.^2)) * dx * dy;
            analysis.enstrophy(i) = 0.5 * sum(sum(omega_t.^2)) * dx * dy;
        end
        analysis.peak_vorticity = max(abs(omega_snaps(:)));
    end
    
    show_figs = usejava('desktop') && ~strcmpi(get(0, 'DefaultFigureVisible'), 'off');
    fig_visibility = 'off';
    if show_figs
        fig_visibility = 'on';
    end

    fig_handle = figure('Name', 'Bathymetry Analysis', 'NumberTitle', 'off', 'Visible', fig_visibility);
    apply_dark_theme_for_figure(fig_handle);
    
    subplot(2, 2, 1);
    contourf(X, Y, analysis.omega_snaps(:,:,end), 20);
    colorbar; title('Vorticity (final)'); xlabel('x'); ylabel('y');
    
    subplot(2, 2, 2);
    contourf(X, Y, bathymetry_field, 15);
    colorbar; title('Bathymetry'); xlabel('x'); ylabel('y');
    
    subplot(2, 2, 3);
    semilogy(analysis.time_vec, analysis.enstrophy + 1e-10);
    hold on; semilogy(analysis.time_vec, analysis.kinetic_energy + 1e-10);
    legend('Enstrophy', 'KE'); xlabel('Time'); ylabel('Value');
    grid on;
    
    subplot(2, 2, 4);
    omega_abs = abs(analysis.omega_snaps);
    if isempty(omega_abs)
        omega_max_t = [];
    elseif ismatrix(omega_abs)
        omega_max_t = max(omega_abs(:));
    else
        omega_max_t = squeeze(max(max(omega_abs, [], 1), [], 2));
    end
    omega_max_t = omega_max_t(:);
    time_plot = analysis.time_vec(:);
    if isempty(omega_max_t)
        omega_max_t = nan(size(time_plot));
    elseif numel(time_plot) ~= numel(omega_max_t)
        min_len = min(numel(time_plot), numel(omega_max_t));
        time_plot = time_plot(1:min_len);
        omega_max_t = omega_max_t(1:min_len);
    end
    plot(time_plot, omega_max_t);
    xlabel('Time'); ylabel('Max |ω|'); grid on; title('Vorticity evolution');
end

function [bath, x_bath, y_bath] = load_bathymetry(Parameters, Nx, Ny, Lx, Ly)
    % Load bathymetry from file or generate from canonical scenario equations.
    
    if isfield(Parameters, 'bathymetry_file') && ~isempty(Parameters.bathymetry_file)
        file = Parameters.bathymetry_file;
        if isfile(file)
            if endsWith(file, '.mat')
                data = load(file);
                if isfield(data, 'bathymetry')
                    bath = data.bathymetry;
                else
                    fields = fieldnames(data);
                    if isempty(fields)
                        error('Bathymetry file %s contained no variables.', file);
                    end
                    bath = data.(fields{1});
                end
            else
                bath = readmatrix(file);
            end
            [Ny_b, Nx_b] = size(bath);
            x_bath = linspace(0, Lx, Nx_b);
            y_bath = linspace(0, Ly, Ny_b);
        else
            fprintf('[Bathymetry] File not found: %s. Using scenario generator.\n', file);
            [bath, x_bath, y_bath] = generate_scenario_bathymetry(Parameters, Nx, Ny, Lx, Ly);
        end
    else
        [bath, x_bath, y_bath] = generate_scenario_bathymetry(Parameters, Nx, Ny, Lx, Ly);
    end
end

function [bath, x, y] = generate_scenario_bathymetry(Parameters, Nx, Ny, Lx, Ly)
    x = linspace(0, Lx, Nx);
    y = linspace(0, Ly, Ny);
    [X, Y] = meshgrid(x, y);

    scenario_id = 'shore_runup_2d';
    if isfield(Parameters, 'bathymetry_scenario') && ~isempty(Parameters.bathymetry_scenario)
        scenario_id = char(string(Parameters.bathymetry_scenario));
    end
    bath_params = struct( ...
        'bed_slope', pick_numeric_field(Parameters, 'bathymetry_bed_slope', 0.03), ...
        'bathymetry_resolution', pick_numeric_field(Parameters, 'bathymetry_resolution', 96), ...
        'z0', pick_numeric_field(Parameters, 'bathymetry_depth_offset', 1000.0), ...
        'amplitude', pick_numeric_field_multi(Parameters, {'bathymetry_relief_amplitude', 'bathymetry_amplitude'}, 180.0));
    if isfield(Parameters, 'bathymetry_custom_points')
        bath_params.bathymetry_custom_points = Parameters.bathymetry_custom_points;
    end
    if isfield(Parameters, 'bathymetry_dynamic_params') && isstruct(Parameters.bathymetry_dynamic_params)
        bath_params.bathymetry_dynamic_params = Parameters.bathymetry_dynamic_params;
    end

    try
        [bath, ~] = generate_bathymetry_field(X, Y, scenario_id, bath_params);
    catch ME
        is_missing_symbol = strcmp(ME.identifier, 'MATLAB:UndefinedFunction') || ...
            strcmp(ME.identifier, 'MATLAB:UndefinedFunctionOrVariable') || ...
            contains(ME.message, 'Undefined function') || ...
            contains(ME.message, 'Unrecognized function or variable');
        if ~is_missing_symbol
            rethrow(ME);
        end
        PathSetup.attach_and_verify();
        [bath, ~] = generate_bathymetry_field(X, Y, scenario_id, bath_params);
    end
end

function value = pick_numeric_field(source, field_name, fallback)
    value = fallback;
    if isfield(source, field_name)
        candidate = double(source.(field_name));
        if isfinite(candidate)
            value = candidate;
        end
    end
end

function value = pick_numeric_field_multi(source, field_names, fallback)
    value = fallback;
    for idx = 1:numel(field_names)
        field_name = field_names{idx};
        if isfield(source, field_name)
            candidate = double(source.(field_name));
            if isfinite(candidate)
                value = candidate;
                return;
            end
        end
    end
end

function psi = solve_poisson_bathy(omega, dx, dy)
    [Ny, Nx] = size(omega);
    omega_hat = fft2(omega);
    Lx_eff = max(Nx * max(dx, eps), eps);
    Ly_eff = max(Ny * max(dy, eps), eps);
    kx = 2*pi/Lx_eff * [0:Nx/2-1, -Nx/2:-1];
    ky = 2*pi/Ly_eff * [0:Ny/2-1, -Ny/2:-1];
    [Kx, Ky] = meshgrid(kx, ky);
    K2 = Kx.^2 + Ky.^2;
    K2(1,1) = 1;
    psi_hat = -omega_hat ./ K2;
    psi_hat(1,1) = 0;
    psi = real(ifft2(psi_hat));
end

function apply_dark_theme_for_figure(fig_handle)
    if isempty(fig_handle) || ~isvalid(fig_handle)
        return;
    end
    try
        ResultsPlotDispatcher.apply_dark_theme(fig_handle, ResultsPlotDispatcher.default_colors());
    catch
        % Plot styling failure should not abort bathymetry analysis.
    end
end

function [u, v] = get_velocity_bathy(psi, dx, dy)
    [v, u] = gradient(psi);
    u = u / dx;
    v = v / dy;
end

function lapl = laplacian_periodic(f, dx, dy)
    lapl = (circshift(f,1,2) + circshift(f,-1,2) - 2*f) / (dx^2) + ...
           (circshift(f,1,1) + circshift(f,-1,1) - 2*f) / (dy^2);
end

function advection = arakawa_advect(omega, psi, dx, dy)
    % Arakawa Jacobian J(psi,omega) with periodic shifts.
    shift_xp = @(F) circshift(F, [0, +1]);
    shift_xm = @(F) circshift(F, [0, -1]);
    shift_yp = @(F) circshift(F, [+1, 0]);
    shift_ym = @(F) circshift(F, [-1, 0]);

    psi_ip = shift_xp(psi);
    psi_im = shift_xm(psi);
    psi_jp = shift_yp(psi);
    psi_jm = shift_ym(psi);
    psi_ipjp = shift_yp(psi_ip);
    psi_ipjm = shift_ym(psi_ip);
    psi_imjp = shift_yp(psi_im);
    psi_imjm = shift_ym(psi_im);

    om_ip = shift_xp(omega);
    om_im = shift_xm(omega);
    om_jp = shift_yp(omega);
    om_jm = shift_ym(omega);
    om_ipjp = shift_yp(om_ip);
    om_ipjm = shift_ym(om_ip);
    om_imjp = shift_yp(om_im);
    om_imjm = shift_ym(om_im);

    J1 = ((psi_ip - psi_im) .* (om_jp - om_jm) ...
        - (psi_jp - psi_jm) .* (om_ip - om_im)) / (4 * dx * dy);
    J2 = (psi_ip .* (om_ipjp - om_ipjm) ...
        - psi_im .* (om_imjp - om_imjm) ...
        - psi_jp .* (om_ipjp - om_imjp) ...
        + psi_jm .* (om_ipjm - om_imjm)) / (4 * dx * dy);
    J3 = (psi_ipjp .* (om_jp - om_ip) ...
        - psi_imjm .* (om_im - om_jm) ...
        - psi_imjp .* (om_jp - om_im) ...
        + psi_ipjm .* (om_ip - om_jm)) / (4 * dx * dy);

    advection = -(J1 + J2 + J3) / 3;
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
