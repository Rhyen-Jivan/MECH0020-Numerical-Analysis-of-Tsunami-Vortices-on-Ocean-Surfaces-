function varargout = ShallowWaterMethod(action, varargin)
% ShallowWaterMethod - Conservative 2D nonlinear SWE method module.
%
% Supported actions:
%   callbacks                     -> struct with init/step/diagnostics/run/finalize_analysis handles
%   init(cfg, ctx)                -> State
%   step(State, cfg, ctx)         -> State
%   diagnostics(State, cfg, ctx)  -> Metrics
%   finalize_analysis(...)        -> analysis
%   run(Parameters)               -> [fig_handle, analysis]
%
% Notes:
%   - Cell-centered conservative finite-volume update for h, hu, hv.
%   - Uses hydrostatic reconstruction plus Rusanov fluxes.
%   - Keeps eta/h as public SWE outputs while publishing omega/psi
%     compatibility aliases for the shared results pipeline.

    narginchk(1, inf);
    action_name = lower(string(action));

    switch action_name
        case "callbacks"
            callbacks = struct();
            callbacks.init = @(cfg, ctx) ShallowWaterMethod("init", cfg, ctx);
            callbacks.step = @(State, cfg, ctx) ShallowWaterMethod("step", State, cfg, ctx);
            callbacks.diagnostics = @(State, cfg, ctx) ShallowWaterMethod("diagnostics", State, cfg, ctx);
            callbacks.finalize_analysis = @(analysis, State, cfg, Parameters, ctx) ...
                ShallowWaterMethod("finalize_analysis", analysis, State, cfg, Parameters, ctx);
            callbacks.run = @(Parameters) ShallowWaterMethod("run", Parameters);
            varargout{1} = callbacks;

        case "init"
            cfg = varargin{1};
            varargout{1} = sw_init_internal(cfg);

        case "step"
            State = varargin{1};
            cfg = varargin{2};
            varargout{1} = sw_step_internal(State, cfg);

        case "diagnostics"
            State = varargin{1};
            varargout{1} = sw_diagnostics_internal(State);

        case "finalize_analysis"
            analysis = varargin{1};
            State = varargin{2};
            cfg = varargin{3};
            Parameters = varargin{4};
            varargout{1} = sw_finalize_analysis_internal(analysis, State, cfg, Parameters);

        case "run"
            Parameters = varargin{1};
            [fig_handle, analysis] = sw_run_internal(Parameters);
            varargout{1} = fig_handle;
            varargout{2} = analysis;

        otherwise
            error("SWE:InvalidAction", ...
                "Unsupported action '%s'. Valid actions: callbacks, init, step, diagnostics, finalize_analysis, run.", ...
                char(string(action)));
    end
end

function State = sw_init_internal(cfg)
    cfg = sw_normalize_cfg(cfg);
    bc = BCDispatcher.resolve(cfg, 'swe', struct());
    if ~bc.capability.supported
        error("SWE:UnsupportedBoundaryConfiguration", "%s", bc.capability.reason);
    end

    setup = sw_build_setup(cfg, bc);
    [h, hu, hv] = sw_initial_conserved_state(cfg, setup);
    State = sw_state_from_conserved(h, hu, hv, setup, 0.0, 0);
end

function State = sw_step_internal(State, cfg)
    cfg = sw_normalize_cfg(cfg);
    setup = State.setup;

    h = State.h;
    hu = State.hu;
    hv = State.hv;
    remaining = cfg.dt;
    substeps = 0;

    while remaining > max(1.0e-12 * cfg.dt, eps)
        dt_cfl = sw_stable_timestep(h, hu, hv, setup, cfg);
        if ~isfinite(dt_cfl) || dt_cfl <= 0
            dt_cfl = remaining;
        end
        dt_stage = min(remaining, dt_cfl);
        [h, hu, hv] = sw_ssprk3_step(h, hu, hv, setup, cfg, dt_stage);
        remaining = remaining - dt_stage;
        substeps = substeps + 1;
        if substeps > 4096
            error("SWE:SubstepOverflow", ...
                "Exceeded conservative SWE substep budget while advancing one UI/runtime step.");
        end
    end

    State = sw_state_from_conserved(h, hu, hv, setup, State.t + cfg.dt, State.step + 1);
    State.last_substeps = substeps;
end

function Metrics = sw_diagnostics_internal(State)
    setup = State.setup;
    wet_mask = State.h > setup.dry_tolerance;
    cell_area = setup.dx * setup.dy;
    speed = sqrt(State.u .^ 2 + State.v .^ 2);

    eta_vals = State.eta(isfinite(State.eta));
    if isempty(eta_vals)
        peak_eta = 0.0;
    else
        peak_eta = max(abs(eta_vals));
    end

    wet_h = State.h(wet_mask);
    wet_u = State.u(wet_mask);
    wet_v = State.v(wet_mask);
    wet_eta = State.eta(wet_mask);
    wet_speed = speed(wet_mask);

    kinetic = 0.0;
    potential = 0.0;
    if ~isempty(wet_h)
        kinetic = 0.5 * sum(wet_h .* (wet_u .^ 2 + wet_v .^ 2), 'all') * cell_area;
        potential = 0.5 * setup.g * sum(wet_eta .^ 2, 'all') * cell_area;
    end

    Metrics = struct();
    Metrics.max_vorticity = peak_eta;
    Metrics.peak_eta = peak_eta;
    Metrics.kinetic_energy = kinetic + potential;
    Metrics.enstrophy = 0.5 * sum(State.eta(:) .^ 2, 'all', 'omitnan') * cell_area;
    Metrics.mass_total = sum(State.h(:), 'all', 'omitnan') * cell_area;
    Metrics.min_depth = min(State.h(:), [], 'omitnan');
    if isempty(wet_speed)
        Metrics.peak_speed = 0.0;
    else
        Metrics.peak_speed = max(wet_speed);
    end
    Metrics.t = State.t;
    Metrics.step = State.step;
end

function [fig_handle, analysis] = sw_run_internal(Parameters)
    run_cfg = sw_cfg_from_parameters(Parameters);

    if ~isfield(run_cfg, "snap_times") || isempty(run_cfg.snap_times) || ~isnumeric(run_cfg.snap_times)
        error("SWE:MissingSnapTimes", ...
            "snap_times must be provided by the UI/runtime contract for shallow-water runs.");
    end

    State = sw_init_internal(run_cfg);
    snap_times = double(run_cfg.snap_times(:)).';
    n_snapshots = numel(snap_times);
    snapshots = sw_allocate_snapshots(State, n_snapshots);
    [snapshots, snap_idx] = sw_store_snapshot(State, snapshots, 1, true);

    Nt = max(0, ceil(run_cfg.Tfinal / run_cfg.dt));
    next_target = inf;
    if n_snapshots >= 2
        next_target = snap_times(2);
    end
    for n = 1:Nt
        State = sw_step_internal(State, run_cfg);
        while snap_idx < n_snapshots && State.t >= next_target - 1.0e-12
            snap_idx = snap_idx + 1;
            [snapshots, ~] = sw_store_snapshot(State, snapshots, snap_idx, false);
            if snap_idx < n_snapshots
                next_target = snap_times(snap_idx + 1);
            else
                next_target = inf;
            end
        end
    end
    while snap_idx < n_snapshots
        snap_idx = snap_idx + 1;
        [snapshots, ~] = sw_store_snapshot(State, snapshots, snap_idx, false);
    end

    analysis = struct();
    analysis.omega_snaps = snapshots.eta;
    analysis.psi_snaps = snapshots.h;
    analysis.eta_snaps = snapshots.eta;
    analysis.h_snaps = snapshots.h;
    analysis.hu_snaps = snapshots.hu;
    analysis.hv_snaps = snapshots.hv;
    analysis.u_snaps = snapshots.u;
    analysis.v_snaps = snapshots.v;
    analysis.snapshot_times = snap_times;
    analysis.time_vec = snap_times;

    max_eta_history = squeeze(max(max(abs(snapshots.eta), [], 1), [], 2)).';
    mass_total = zeros(1, n_snapshots);
    min_depth = zeros(1, n_snapshots);
    kinetic_energy = zeros(1, n_snapshots);
    enstrophy = zeros(1, n_snapshots);
    peak_speed = zeros(1, n_snapshots);
    cell_area = State.setup.dx * State.setup.dy;
    for idx = 1:n_snapshots
        h = snapshots.h(:, :, idx);
        u = snapshots.u(:, :, idx);
        v = snapshots.v(:, :, idx);
        eta = snapshots.eta(:, :, idx);
        wet = h > State.setup.dry_tolerance;
        mass_total(idx) = sum(h(:), 'omitnan') * cell_area;
        min_depth(idx) = min(h(:), [], 'omitnan');
        kinetic_energy(idx) = 0.5 * sum(h(wet) .* (u(wet) .^ 2 + v(wet) .^ 2), 'all') * cell_area + ...
            0.5 * State.setup.g * sum(eta(wet) .^ 2, 'all') * cell_area;
        enstrophy(idx) = 0.5 * sum(eta(:) .^ 2, 'omitnan') * cell_area;
        peak_speed(idx) = max(sqrt(u(wet) .^ 2 + v(wet) .^ 2), [], 'all', 'omitnan');
        if ~isfinite(peak_speed(idx))
            peak_speed(idx) = 0.0;
        end
    end
    analysis.max_omega_history = max_eta_history;
    analysis.peak_vorticity = max(max_eta_history);
    analysis.mass_total = mass_total;
    analysis.min_depth = min_depth;
    analysis.kinetic_energy = kinetic_energy;
    analysis.enstrophy = enstrophy;
    analysis.peak_speed_history = peak_speed;
    analysis = sw_finalize_analysis_internal(analysis, State, run_cfg, Parameters);

    fig_handle = sw_summary_figure(analysis);
end

function analysis = sw_finalize_analysis_internal(analysis, State, cfg, Parameters)
    analysis = MethodConfigBuilder.apply_analysis_contract(analysis, cfg, Parameters);
    analysis.method = "shallow_water_2d";
    analysis.method_family = "shallow_water";
    analysis.display_method = "Shallow Water";
    analysis.solver_form = "nonlinear_swe_conservative_fv_2d";
    analysis.time_integrator = "SSP_RK3";
    analysis.gravity = State.setup.g;
    analysis.base_depth = State.setup.base_depth;
    analysis.dry_tolerance = State.setup.dry_tolerance;
    analysis.bed_friction_coeff = State.setup.bed_friction_coeff;
    analysis.wind_forcing_enabled = logical(State.setup.enable_wind);
    analysis.eta_final = State.eta;
    analysis.h_final = State.h;
    analysis.hu_final = State.hu;
    analysis.hv_final = State.hv;
    analysis.u_final = State.u;
    analysis.v_final = State.v;
    analysis.bed_elevation_2d = State.setup.bed;
    analysis.bathymetry_field_2d = State.setup.bed;
    analysis.bathymetry_display_name = State.setup.bathymetry_meta.display_name;
    if isfield(analysis, 'omega_snaps') && ~isfield(analysis, 'eta_snaps')
        analysis.eta_snaps = analysis.omega_snaps;
    end
    if isfield(analysis, 'psi_snaps') && ~isfield(analysis, 'h_snaps')
        analysis.h_snaps = analysis.psi_snaps;
    end
    if ~isfield(analysis, 'peak_eta') || isempty(analysis.peak_eta)
        if isfield(analysis, 'eta_snaps') && ~isempty(analysis.eta_snaps)
            analysis.peak_eta = max(abs(analysis.eta_snaps(:)));
        else
            analysis.peak_eta = max(abs(State.eta(:)));
        end
    end
    analysis.peak_surface_elevation = analysis.peak_eta;
    analysis.peak_vorticity = analysis.peak_eta;
    analysis.primary_scalar_key = 'eta';
    analysis.primary_scalar_name = 'Free-surface elevation';
    analysis.primary_scalar_symbol = '\eta';
    analysis.secondary_scalar_key = 'h';
    analysis.secondary_scalar_name = 'Water depth';
    analysis.secondary_scalar_symbol = 'h';
    analysis.vector_field_name = 'Depth-averaged velocity';
    analysis.speed_field_name = 'Depth-averaged speed';
    analysis.diagnostic_primary_name = 'Total energy';
    analysis.diagnostic_secondary_name = 'Surface variance';
    analysis.compatibility_note = 'omega/psi snapshot aliases carry eta/h for shared plotting compatibility.';
end

function cfg = sw_cfg_from_parameters(Parameters)
    cfg = MethodConfigBuilder.build(Parameters, 'shallow_water', 'ShallowWaterMethod.run');
end

function cfg = sw_normalize_cfg(cfg)
    if ~isfield(cfg, 'swe2d') || ~isstruct(cfg.swe2d)
        error("SWE:MissingConfig", "cfg.swe2d is required for the Shallow Water solver.");
    end
    defaults = create_default_parameters();
    swe_defaults = defaults.method_config.swe2d;
    cfg.swe2d = merge_structs(swe_defaults, cfg.swe2d);
    cfg.swe2d.initial_condition = lower(char(string(cfg.swe2d.initial_condition)));
    cfg.swe2d.time_integrator = upper(char(string(cfg.swe2d.time_integrator)));
end

function setup = sw_build_setup(cfg, bc)
    x = linspace(-cfg.Lx / 2, cfg.Lx / 2, cfg.Nx);
    y = linspace(-cfg.Ly / 2, cfg.Ly / 2, cfg.Ny);
    [X, Y] = meshgrid(x, y);

    bathy_params = struct( ...
        'bed_slope', pick_field_or(cfg, {'bathymetry_bed_slope'}, 0.03), ...
        'bathymetry_resolution', pick_field_or(cfg, {'bathymetry_resolution'}, 96), ...
        'z0', pick_field_or(cfg, {'bathymetry_depth_offset'}, 1000.0), ...
        'amplitude', pick_field_or(cfg, {'bathymetry_relief_amplitude', 'bathymetry_amplitude'}, 180.0));
    if isfield(cfg, 'bathymetry_custom_points') && ~isempty(cfg.bathymetry_custom_points)
        bathy_params.bathymetry_custom_points = cfg.bathymetry_custom_points;
    end
    if isfield(cfg, 'bathymetry_dynamic_params') && isstruct(cfg.bathymetry_dynamic_params)
        bathy_params.bathymetry_dynamic_params = cfg.bathymetry_dynamic_params;
    end
    scenario = pick_field_or(cfg, {'bathymetry_scenario'}, 'flat_2d');
    [bath_field, bath_meta] = generate_bathymetry_field(X, Y, scenario, bathy_params);
    if ~strcmpi(char(string(bath_meta.dimension)), '2d')
        error("SWE:UnsupportedBathymetryDimension", ...
            "Shallow Water phase 1 requires 2D bathymetry scenarios. Requested: %s", scenario);
    end

    base_depth = double(cfg.swe2d.base_depth);
    relief_fraction = double(cfg.swe2d.bed_relief_fraction);
    bed = sw_normalize_bed_elevation(bath_field, scenario, base_depth, relief_fraction);

    setup = struct();
    setup.Nx = cfg.Nx;
    setup.Ny = cfg.Ny;
    setup.Lx = cfg.Lx;
    setup.Ly = cfg.Ly;
    setup.dx = cfg.dx;
    setup.dy = cfg.dy;
    setup.x = x;
    setup.y = y;
    setup.X = X;
    setup.Y = Y;
    setup.bc = bc;
    setup.g = double(cfg.swe2d.gravity);
    setup.base_depth = base_depth;
    setup.dry_tolerance = double(cfg.swe2d.dry_tolerance);
    setup.cfl = double(cfg.swe2d.cfl);
    setup.bed_friction_coeff = double(cfg.swe2d.bed_friction_coeff);
    setup.enable_wind = logical(cfg.swe2d.enable_wind);
    setup.wind_velocity_x = double(cfg.swe2d.wind_velocity_x);
    setup.wind_velocity_y = double(cfg.swe2d.wind_velocity_y);
    setup.wind_drag_coeff = double(cfg.swe2d.wind_drag_coeff);
    setup.air_density = double(cfg.swe2d.air_density);
    setup.water_density = double(cfg.swe2d.water_density);
    setup.bed = bed;
    setup.bathymetry_meta = bath_meta;
    setup.still_water_eta = zeros(size(bed));
    setup.apply_ghost_cells = bc.method.swe.apply_ghost_cells;
end

function bed = sw_normalize_bed_elevation(bath_field, scenario, base_depth, relief_fraction)
    if any(strcmpi(char(string(scenario)), {'flat_2d'}))
        bed = zeros(size(bath_field));
        return;
    end
    bath_field = double(bath_field);
    bath_min = min(bath_field(:));
    bath_max = max(bath_field(:));
    if ~isfinite(bath_min) || ~isfinite(bath_max) || abs(bath_max - bath_min) <= eps
        bed = zeros(size(bath_field));
        return;
    end
    normalized = (bath_field - bath_min) / max(bath_max - bath_min, eps);
    bed = max(0.0, relief_fraction * base_depth * normalized);
end

function [h, hu, hv] = sw_initial_conserved_state(cfg, setup)
    X = setup.X;
    Y = setup.Y;
    bath = setup.bed;

    eta0 = zeros(size(X));
    u0 = zeros(size(X));
    v0 = zeros(size(X));

    ic_name = lower(char(string(cfg.swe2d.initial_condition)));
    x0 = double(cfg.swe2d.surface_center_x);
    y0 = double(cfg.swe2d.surface_center_y);
    sx = max(double(cfg.swe2d.surface_sigma_x), eps);
    sy = max(double(cfg.swe2d.surface_sigma_y), eps);
    amp = double(cfg.swe2d.surface_amplitude);

    switch ic_name
        case {'lake_at_rest', 'still_water'}
            % leave eta0 and velocity zero

        case {'solitary_eta', 'surface_gaussian', 'eta_gaussian'}
            eta0 = amp * exp(-0.5 * ((X - x0) ./ sx) .^ 2 - 0.5 * ((Y - y0) ./ sy) .^ 2);

        case {'momentum_gaussian', 'momentum_pulse'}
            kernel = exp(-0.5 * ((X - x0) ./ sx) .^ 2 - 0.5 * ((Y - y0) ./ sy) .^ 2);
            u0 = double(cfg.swe2d.momentum_amplitude_x) * kernel;
            v0 = double(cfg.swe2d.momentum_amplitude_y) * kernel;

        otherwise
            error("SWE:UnknownInitialCondition", ...
                "Unknown swe2d.initial_condition token '%s'.", ic_name);
    end

    h = max(0.0, setup.base_depth + eta0 - bath);
    wet = h > setup.dry_tolerance;
    hu = zeros(size(h));
    hv = zeros(size(h));
    hu(wet) = h(wet) .* u0(wet);
    hv(wet) = h(wet) .* v0(wet);
    [h, hu, hv] = sw_enforce_physical_bounds(h, hu, hv, setup);
end

function State = sw_state_from_conserved(h, hu, hv, setup, t, step)
    [h, hu, hv] = sw_enforce_physical_bounds(h, hu, hv, setup);
    h_eff = max(h, setup.dry_tolerance);
    u = zeros(size(h));
    v = zeros(size(h));
    wet = h > setup.dry_tolerance;
    u(wet) = hu(wet) ./ h_eff(wet);
    v(wet) = hv(wet) ./ h_eff(wet);
    eta = h + setup.bed - setup.base_depth;

    State = struct();
    State.h = h;
    State.hu = hu;
    State.hv = hv;
    State.eta = eta;
    State.u = u;
    State.v = v;
    State.omega = eta;
    State.psi = h;
    State.t = t;
    State.step = step;
    State.setup = setup;
end

function [h, hu, hv] = sw_ssprk3_step(h, hu, hv, setup, cfg, dt)
    [rh1, rhu1, rhv1] = sw_rhs(h, hu, hv, setup, cfg);
    h1 = h + dt * rh1;
    hu1 = hu + dt * rhu1;
    hv1 = hv + dt * rhv1;
    [h1, hu1, hv1] = sw_enforce_physical_bounds(h1, hu1, hv1, setup);

    [rh2, rhu2, rhv2] = sw_rhs(h1, hu1, hv1, setup, cfg);
    h2 = 0.75 * h + 0.25 * (h1 + dt * rh2);
    hu2 = 0.75 * hu + 0.25 * (hu1 + dt * rhu2);
    hv2 = 0.75 * hv + 0.25 * (hv1 + dt * rhv2);
    [h2, hu2, hv2] = sw_enforce_physical_bounds(h2, hu2, hv2, setup);

    [rh3, rhu3, rhv3] = sw_rhs(h2, hu2, hv2, setup, cfg);
    h = (1 / 3) * h + (2 / 3) * (h2 + dt * rh3);
    hu = (1 / 3) * hu + (2 / 3) * (hu2 + dt * rhu3);
    hv = (1 / 3) * hv + (2 / 3) * (hv2 + dt * rhv3);
    [h, hu, hv] = sw_enforce_physical_bounds(h, hu, hv, setup);
end

function [dh, dhu, dhv] = sw_rhs(h, hu, hv, setup, cfg)
    [hg, hug, hvg, bg] = sw_apply_ghost_cells(h, hu, hv, setup);

    [Fx_h, Fx_hu, Fx_hv, Sx_minus, Sx_plus] = sw_flux_x( ...
        hg(2:end-1, 1:end-1), hug(2:end-1, 1:end-1), hvg(2:end-1, 1:end-1), bg(2:end-1, 1:end-1), ...
        hg(2:end-1, 2:end), hug(2:end-1, 2:end), hvg(2:end-1, 2:end), bg(2:end-1, 2:end), ...
        setup.g, setup.dry_tolerance);

    [Fy_h, Fy_hu, Fy_hv, Sy_minus, Sy_plus] = sw_flux_y( ...
        hg(1:end-1, 2:end-1), hug(1:end-1, 2:end-1), hvg(1:end-1, 2:end-1), bg(1:end-1, 2:end-1), ...
        hg(2:end, 2:end-1), hug(2:end, 2:end-1), hvg(2:end, 2:end-1), bg(2:end, 2:end-1), ...
        setup.g, setup.dry_tolerance);

    dh = -(Fx_h(:, 2:end) - Fx_h(:, 1:end-1)) / setup.dx ...
         -(Fy_h(2:end, :) - Fy_h(1:end-1, :)) / setup.dy;

    dhu = -(Fx_hu(:, 2:end) - Fx_hu(:, 1:end-1)) / setup.dx ...
          -(Fy_hu(2:end, :) - Fy_hu(1:end-1, :)) / setup.dy ...
          + (Sx_minus(:, 2:end) + Sx_plus(:, 1:end-1)) / setup.dx;

    dhv = -(Fx_hv(:, 2:end) - Fx_hv(:, 1:end-1)) / setup.dx ...
          -(Fy_hv(2:end, :) - Fy_hv(1:end-1, :)) / setup.dy ...
          + (Sy_minus(2:end, :) + Sy_plus(1:end-1, :)) / setup.dy;

    [fric_hu, fric_hv] = sw_friction_source(h, hu, hv, setup);
    dhu = dhu + fric_hu;
    dhv = dhv + fric_hv;

    if logical(setup.enable_wind)
        [tau_hu, tau_hv] = sw_wind_source(h, hu, hv, setup);
        dhu = dhu + tau_hu;
        dhv = dhv + tau_hv;
    end

    if cfg.nu > 0
        dhu = dhu + cfg.nu * sw_laplacian(hu, setup.dx, setup.dy);
        dhv = dhv + cfg.nu * sw_laplacian(hv, setup.dx, setup.dy);
    end
end

function [hg, hug, hvg, bg] = sw_apply_ghost_cells(h, hu, hv, setup)
    [hg, hug, hvg, bg] = setup.apply_ghost_cells(h, hu, hv, setup);
end

function [Fh, Fhu, Fhv, Sminus, Splus] = sw_flux_x(hL, huL, hvL, bL, hR, huR, hvR, bR, g, dry_tol)
    zstar = max(bL, bR);
    hLstar = max(0.0, hL + bL - zstar);
    hRstar = max(0.0, hR + bR - zstar);

    uL = zeros(size(hL));
    vL = zeros(size(hL));
    uR = zeros(size(hR));
    vR = zeros(size(hR));
    wetL = hL > dry_tol;
    wetR = hR > dry_tol;
    uL(wetL) = huL(wetL) ./ hL(wetL);
    vL(wetL) = hvL(wetL) ./ hL(wetL);
    uR(wetR) = huR(wetR) ./ hR(wetR);
    vR(wetR) = hvR(wetR) ./ hR(wetR);

    UL_h = hLstar;
    UL_hu = hLstar .* uL;
    UL_hv = hLstar .* vL;
    UR_h = hRstar;
    UR_hu = hRstar .* uR;
    UR_hv = hRstar .* vR;

    [FL_h, FL_hu, FL_hv] = sw_physical_flux_x(UL_h, UL_hu, UL_hv, g, dry_tol);
    [FR_h, FR_hu, FR_hv] = sw_physical_flux_x(UR_h, UR_hu, UR_hv, g, dry_tol);

    cL = sqrt(g * hLstar);
    cR = sqrt(g * hRstar);
    smax = max(abs(uL) + cL, abs(uR) + cR);

    Fh = 0.5 * (FL_h + FR_h) - 0.5 * smax .* (UR_h - UL_h);
    Fhu = 0.5 * (FL_hu + FR_hu) - 0.5 * smax .* (UR_hu - UL_hu);
    Fhv = 0.5 * (FL_hv + FR_hv) - 0.5 * smax .* (UR_hv - UL_hv);

    Sminus = 0.5 * g * (hL .^ 2 - hLstar .^ 2);
    Splus = 0.5 * g * (hRstar .^ 2 - hR .^ 2);
end

function [Gh, Ghu, Ghv, Sminus, Splus] = sw_flux_y(hB, huB, hvB, bB, hT, huT, hvT, bT, g, dry_tol)
    zstar = max(bB, bT);
    hBstar = max(0.0, hB + bB - zstar);
    hTstar = max(0.0, hT + bT - zstar);

    uB = zeros(size(hB));
    vB = zeros(size(hB));
    uT = zeros(size(hT));
    vT = zeros(size(hT));
    wetB = hB > dry_tol;
    wetT = hT > dry_tol;
    uB(wetB) = huB(wetB) ./ hB(wetB);
    vB(wetB) = hvB(wetB) ./ hB(wetB);
    uT(wetT) = huT(wetT) ./ hT(wetT);
    vT(wetT) = hvT(wetT) ./ hT(wetT);

    UB_h = hBstar;
    UB_hu = hBstar .* uB;
    UB_hv = hBstar .* vB;
    UT_h = hTstar;
    UT_hu = hTstar .* uT;
    UT_hv = hTstar .* vT;

    [GB_h, GB_hu, GB_hv] = sw_physical_flux_y(UB_h, UB_hu, UB_hv, g, dry_tol);
    [GT_h, GT_hu, GT_hv] = sw_physical_flux_y(UT_h, UT_hu, UT_hv, g, dry_tol);

    cB = sqrt(g * hBstar);
    cT = sqrt(g * hTstar);
    smax = max(abs(vB) + cB, abs(vT) + cT);

    Gh = 0.5 * (GB_h + GT_h) - 0.5 * smax .* (UT_h - UB_h);
    Ghu = 0.5 * (GB_hu + GT_hu) - 0.5 * smax .* (UT_hu - UB_hu);
    Ghv = 0.5 * (GB_hv + GT_hv) - 0.5 * smax .* (UT_hv - UB_hv);

    Sminus = 0.5 * g * (hB .^ 2 - hBstar .^ 2);
    Splus = 0.5 * g * (hTstar .^ 2 - hT .^ 2);
end

function [Fh, Fhu, Fhv] = sw_physical_flux_x(h, hu, hv, g, dry_tol)
    u = zeros(size(h));
    v = zeros(size(h));
    wet = h > dry_tol;
    u(wet) = hu(wet) ./ h(wet);
    v(wet) = hv(wet) ./ h(wet);
    Fh = hu;
    Fhu = hu .* u + 0.5 * g * h .^ 2;
    Fhv = hu .* v;
end

function [Gh, Ghu, Ghv] = sw_physical_flux_y(h, hu, hv, g, dry_tol)
    u = zeros(size(h));
    v = zeros(size(h));
    wet = h > dry_tol;
    u(wet) = hu(wet) ./ h(wet);
    v(wet) = hv(wet) ./ h(wet);
    Gh = hv;
    Ghu = hv .* u;
    Ghv = hv .* v + 0.5 * g * h .^ 2;
end

function [src_hu, src_hv] = sw_friction_source(h, hu, hv, setup)
    src_hu = zeros(size(h));
    src_hv = zeros(size(h));
    wet = h > setup.dry_tolerance;
    if ~any(wet, 'all') || setup.bed_friction_coeff <= 0
        return;
    end
    u = zeros(size(h));
    v = zeros(size(h));
    u(wet) = hu(wet) ./ h(wet);
    v(wet) = hv(wet) ./ h(wet);
    speed = sqrt(u .^ 2 + v .^ 2);
    src_hu(wet) = -setup.bed_friction_coeff * u(wet) .* speed(wet);
    src_hv(wet) = -setup.bed_friction_coeff * v(wet) .* speed(wet);
end

function [src_hu, src_hv] = sw_wind_source(h, hu, hv, setup)
    src_hu = zeros(size(h));
    src_hv = zeros(size(h));
    wet = h > setup.dry_tolerance;
    if ~any(wet, 'all')
        return;
    end
    u = zeros(size(h));
    v = zeros(size(h));
    u(wet) = hu(wet) ./ h(wet);
    v(wet) = hv(wet) ./ h(wet);
    du = setup.wind_velocity_x - u;
    dv = setup.wind_velocity_y - v;
    rel = sqrt(du .^ 2 + dv .^ 2);
    tau_scale = (setup.air_density / max(setup.water_density, eps)) * setup.wind_drag_coeff;
    src_hu(wet) = tau_scale * rel(wet) .* du(wet);
    src_hv(wet) = tau_scale * rel(wet) .* dv(wet);
end

function lap = sw_laplacian(field, dx, dy)
    lap = zeros(size(field));
    if size(field, 1) < 3 || size(field, 2) < 3
        return;
    end
    lap(2:end-1, 2:end-1) = ...
        (field(2:end-1, 3:end) - 2 * field(2:end-1, 2:end-1) + field(2:end-1, 1:end-2)) / (dx ^ 2) + ...
        (field(3:end, 2:end-1) - 2 * field(2:end-1, 2:end-1) + field(1:end-2, 2:end-1)) / (dy ^ 2);
end

function dt_cfl = sw_stable_timestep(h, hu, hv, setup, cfg)
    wet = h > setup.dry_tolerance;
    if ~any(wet, 'all')
        dt_cfl = cfg.dt;
        return;
    end
    u = zeros(size(h));
    v = zeros(size(h));
    u(wet) = hu(wet) ./ h(wet);
    v(wet) = hv(wet) ./ h(wet);
    c = sqrt(setup.g * max(h, 0.0));
    denom_x = max(abs(u(wet)) + c(wet), [], 'all');
    denom_y = max(abs(v(wet)) + c(wet), [], 'all');
    dt_x = inf;
    dt_y = inf;
    if isfinite(denom_x) && denom_x > 0
        dt_x = setup.dx / denom_x;
    end
    if isfinite(denom_y) && denom_y > 0
        dt_y = setup.dy / denom_y;
    end
    dt_cfl = setup.cfl * min([dt_x, dt_y, cfg.dt]);
    if ~isfinite(dt_cfl) || dt_cfl <= 0
        dt_cfl = cfg.dt;
    end
end

function [h, hu, hv] = sw_enforce_physical_bounds(h, hu, hv, setup)
    h(~isfinite(h)) = 0.0;
    hu(~isfinite(hu)) = 0.0;
    hv(~isfinite(hv)) = 0.0;
    h = max(h, 0.0);
    dry = h <= setup.dry_tolerance;
    hu(dry) = 0.0;
    hv(dry) = 0.0;
end

function snapshots = sw_allocate_snapshots(State, n_snapshots)
    sz = [size(State.h, 1), size(State.h, 2), n_snapshots];
    snapshots = struct();
    snapshots.eta = zeros(sz);
    snapshots.h = zeros(sz);
    snapshots.hu = zeros(sz);
    snapshots.hv = zeros(sz);
    snapshots.u = zeros(sz);
    snapshots.v = zeros(sz);
end

function [snapshots, idx] = sw_store_snapshot(State, snapshots, idx, store_initial) %#ok<INUSD>
    snapshots.eta(:, :, idx) = State.eta;
    snapshots.h(:, :, idx) = State.h;
    snapshots.hu(:, :, idx) = State.hu;
    snapshots.hv(:, :, idx) = State.hv;
    snapshots.u(:, :, idx) = State.u;
    snapshots.v(:, :, idx) = State.v;
end

function fig_handle = sw_summary_figure(analysis)
    fig_handle = figure('Name', 'Shallow Water Analysis', 'Visible', 'off', ...
        'Color', [0.11 0.12 0.15], 'Position', [100, 100, 1000, 420]);
    tiledlayout(fig_handle, 1, 2, 'Padding', 'compact', 'TileSpacing', 'compact');

    nexttile;
    if isfield(analysis, 'eta_snaps') && ~isempty(analysis.eta_snaps)
        imagesc(analysis.eta_snaps(:, :, end));
        title('Final \eta', 'Interpreter', 'tex');
        colorbar;
        axis equal tight;
        set(gca, 'YDir', 'normal');
    else
        title('No \eta snapshots', 'Interpreter', 'tex');
    end

    nexttile;
    if isfield(analysis, 'time_vec') && isfield(analysis, 'kinetic_energy') && ~isempty(analysis.time_vec)
        plot(analysis.time_vec, analysis.kinetic_energy, 'LineWidth', 1.5);
        title('Energy history');
        xlabel('t (s)');
        ylabel('E');
        grid on;
    else
        title('No diagnostic history');
    end
end

function value = pick_field_or(s, keys, fallback)
    value = fallback;
    for idx = 1:numel(keys)
        key = keys{idx};
        if isstruct(s) && isfield(s, key) && ~isempty(s.(key))
            value = s.(key);
            return;
        end
    end
end

function merged = merge_structs(a, b)
    merged = a;
    if ~isstruct(merged)
        merged = struct();
    end
    if ~isstruct(b)
        return;
    end
    fn = fieldnames(b);
    for idx = 1:numel(fn)
        key = fn{idx};
        if isstruct(b.(key)) && isfield(merged, key) && isstruct(merged.(key))
            merged.(key) = merge_structs(merged.(key), b.(key));
        else
            merged.(key) = b.(key);
        end
    end
end
