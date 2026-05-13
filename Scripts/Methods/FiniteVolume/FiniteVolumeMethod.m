function varargout = FiniteVolumeMethod(action, varargin)
% FiniteVolumeMethod - Self-contained conservative finite-volume 3D module.
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
%   - Single-file module following the FD action-contract architecture.
%   - Uses a structured Cartesian Nx x Ny x Nz control-volume mesh.
%   - Evolves active transported scalars through conservative face-flux
%     divergence with SSP-RK3 time stepping.
%   - Keeps projected 2D state as compatibility output for the shared
%     plotting/reporting pipeline.

    narginchk(1, inf);
    action_name = lower(string(action));

    switch action_name
        case "callbacks"
            callbacks = struct();
            callbacks.init = @(cfg, ctx) FiniteVolumeMethod("init", cfg, ctx);
            callbacks.step = @(State, cfg, ctx) FiniteVolumeMethod("step", State, cfg, ctx);
            callbacks.diagnostics = @(State, cfg, ctx) FiniteVolumeMethod("diagnostics", State, cfg, ctx);
            callbacks.finalize_analysis = @(analysis, State, cfg, Parameters, ctx) ...
                FiniteVolumeMethod("finalize_analysis", analysis, State, cfg, Parameters, ctx);
            callbacks.run = @(Parameters) FiniteVolumeMethod("run", Parameters);
            varargout{1} = callbacks;

        case "init"
            cfg = varargin{1};
            varargout{1} = fv_init_internal(cfg);

        case "step"
            State = varargin{1};
            cfg = varargin{2};
            varargout{1} = fv_step_internal(State, cfg);

        case "diagnostics"
            State = varargin{1};
            varargout{1} = fv_diagnostics_internal(State);

        case "finalize_analysis"
            analysis = varargin{1};
            State = varargin{2};
            cfg = varargin{3};
            Parameters = varargin{4};
            varargout{1} = fv_finalize_analysis_internal(analysis, State, cfg, Parameters);

        case "run"
            Parameters = varargin{1};
            [fig_handle, analysis] = fv_run_internal(Parameters);
            varargout{1} = fig_handle;
            varargout{2} = analysis;

        otherwise
            error("FV:InvalidAction", ...
                "Unsupported action '%s'. Valid actions: callbacks, init, step, diagnostics, finalize_analysis, run.", ...
                char(string(action)));
    end
end

function State = fv_init_internal(cfg)
    cfg = fv_normalize_cfg(cfg);
    bc = fv_resolve_bc(cfg);
    if ~bc.capability.supported
        error("FV:UnsupportedBoundaryConfiguration", "%s", bc.capability.reason);
    end
    setup = fv_build_setup(cfg);
    setup.bc = bc;
    setup.apply_bc = bc.method.fv.apply_wall_omega;
    setup.enforce_velocity_bc = bc.method.fv.enforce_velocity_bc;
    setup.wall_model = bc.method.fv.wall_model;
    setup.periodic_x = bc.method.fv.periodic_x;
    setup.periodic_y = bc.method.fv.periodic_y;
    setup.bathymetry_geometry = resolve_fv_bathymetry_geometry(cfg, setup);
    setup = fv_finalize_setup_masks(setup, bc);
    setup.layer_poisson = fv_build_layer_poisson_solvers(setup, bc);
    setup.transport_channel_registry = fv_transport_channel_registry();

    omega2d = fv_initial_vorticity_2d(cfg, setup.X, setup.Y);
    omega3d = fv_lift_to_3d(omega2d, setup);
    transport_state = fv_initialize_transport_state(setup, struct('omega3d', omega3d));
    [transport_state, closure_state] = fv_prepare_transport_rhs_inputs(transport_state, setup, cfg, true);

    State = fv_state_from_transport(transport_state, closure_state, setup, 0.0, 0);
end

function State = fv_step_internal(State, cfg)
    cfg = fv_normalize_cfg(cfg);
    setup = State.setup;
    transport_state = State.transport_state;

    [transport_next, closure_next] = fv_advance_transport_ssprk3(transport_state, setup, cfg);
    State = fv_state_from_transport(transport_next, closure_next, setup, State.t + cfg.dt, State.step + 1);
end

function Metrics = fv_diagnostics_internal(State)
    setup = State.setup;
    closure_state = fv_get_closure_state(State, setup);
    wet_mask = fv_wet_mask3d(setup);
    omega3d = fv_get_primary_omega3d(State);

    speed = sqrt(closure_state.u3d.^2 + closure_state.v3d.^2);

    Metrics = struct();
    Metrics.max_vorticity = max(abs(omega3d(wet_mask)));
    Metrics.enstrophy = 0.5 * sum(omega3d(wet_mask).^2) * setup.cell_volume;
    Metrics.kinetic_energy = 0.5 * sum((closure_state.u3d(wet_mask).^2 + closure_state.v3d(wet_mask).^2)) * setup.cell_volume;
    Metrics.peak_speed = max(speed(wet_mask));
    Metrics.t = State.t;
    Metrics.step = State.step;
end

function [fig_handle, analysis] = fv_run_internal(Parameters)
    run_cfg = fv_cfg_from_parameters(Parameters);

    if ~isfield(run_cfg, "snap_times") || isempty(run_cfg.snap_times) || ~isnumeric(run_cfg.snap_times)
        error("FV:MissingSnapTimes", ...
            "snap_times must be provided by the UI/runtime contract for FV runs.");
    end

    State = fv_init_internal(run_cfg);

    snap_times = run_cfg.snap_times(:).';
    n_snapshots = numel(snap_times);

    omega_snaps = zeros(run_cfg.Ny, run_cfg.Nx, n_snapshots);
    psi_snaps = zeros(run_cfg.Ny, run_cfg.Nx, n_snapshots);
    kinetic_energy = zeros(n_snapshots, 1);
    enstrophy = zeros(n_snapshots, 1);
    peak_speed = zeros(n_snapshots, 1);
    max_omega = zeros(n_snapshots, 1);
    sampled_times = zeros(n_snapshots, 1);

    snap_idx = 1;
    while snap_idx <= n_snapshots && State.t >= snap_times(snap_idx) - 1e-12
        [omega_snaps, psi_snaps, kinetic_energy, enstrophy, peak_speed, max_omega, sampled_times, snap_idx] = ...
            fv_store_snapshot(State, omega_snaps, psi_snaps, kinetic_energy, enstrophy, peak_speed, max_omega, sampled_times, snap_idx);
    end

    Nt = max(0, ceil(run_cfg.Tfinal / run_cfg.dt));
    for n = 1:Nt
        State = fv_step_internal(State, run_cfg);

        while snap_idx <= n_snapshots && State.t >= snap_times(snap_idx) - 1e-12
            [omega_snaps, psi_snaps, kinetic_energy, enstrophy, peak_speed, max_omega, sampled_times, snap_idx] = ...
                fv_store_snapshot(State, omega_snaps, psi_snaps, kinetic_energy, enstrophy, peak_speed, max_omega, sampled_times, snap_idx);
        end
    end

    if snap_idx <= n_snapshots
        for idx = snap_idx:n_snapshots
            [omega_snaps, psi_snaps, kinetic_energy, enstrophy, peak_speed, max_omega, sampled_times, ~] = ...
                fv_store_snapshot(State, omega_snaps, psi_snaps, kinetic_energy, enstrophy, peak_speed, max_omega, sampled_times, idx);
        end
    end

    analysis = struct();
    analysis.method = "finite_volume_3d_conservative";
    analysis.omega_snaps = omega_snaps;
    analysis.psi_snaps = psi_snaps;
    analysis.snapshot_times_requested = snap_times(:);
    analysis.snapshot_times_actual = sampled_times;
    analysis.snapshot_times = snap_times(:);
    analysis.time_vec = sampled_times;
    analysis.snapshots_stored = n_snapshots;
    analysis.Nx = run_cfg.Nx;
    analysis.Ny = run_cfg.Ny;
    analysis.Nz = run_cfg.Nz;
    analysis.Lx = run_cfg.Lx;
    analysis.Ly = run_cfg.Ly;
    analysis.Lz = run_cfg.Lz;
    analysis.grid_mode = run_cfg.grid_mode;
    analysis.is_anisotropic = run_cfg.is_anisotropic;
    analysis.dx = State.setup.dx;
    analysis.dy = State.setup.dy;
    analysis.dz = State.setup.dz;
    analysis.grid_points = nnz(fv_wet_mask3d(State.setup));
    analysis.kinetic_energy = kinetic_energy;
    analysis.enstrophy = enstrophy;
    analysis.peak_speed_history = peak_speed;
    analysis.max_omega_history = max_omega;
    analysis.peak_abs_omega = max(max_omega);
    analysis.peak_vorticity = analysis.peak_abs_omega;
    analysis.vertical_bc = State.setup.z_bc;
    analysis.time_integrator = "SSP_RK3";
    analysis.solver_form = "conservative_control_volume";
    analysis.projection = "compatibility_depth_average";
    analysis.compatibility_projection = "depth_average";
    analysis.wall_model = char(string(State.setup.wall_model));
    analysis.omega3d_final = State.omega3d;
    analysis.psi3d_final = State.psi3d;
    analysis = fv_finalize_analysis_internal(analysis, State, run_cfg, Parameters);

    fig_handle = fv_summary_figure(analysis);
end

function analysis = fv_finalize_analysis_internal(analysis, State, cfg, Parameters)
% fv_finalize_analysis_internal Normalize FV analysis for run/evolution paths.
    analysis.method = "finite_volume_3d_conservative";
    analysis.Nx = cfg.Nx;
    analysis.Ny = cfg.Ny;
    analysis.Nz = cfg.Nz;
    analysis.Lx = cfg.Lx;
    analysis.Ly = cfg.Ly;
    analysis.Lz = cfg.Lz;
    analysis.grid_mode = cfg.grid_mode;
    analysis.is_anisotropic = cfg.is_anisotropic;
    analysis.dx = State.setup.dx;
    analysis.dy = State.setup.dy;
    analysis.dz = State.setup.dz;
    if ~isfield(analysis, 'grid_points') || isempty(analysis.grid_points)
        analysis.grid_points = nnz(fv_wet_mask3d(State.setup));
    end
    analysis.vertical_bc = State.setup.z_bc;
    analysis.time_integrator = "SSP_RK3";
    analysis.solver_form = "conservative_control_volume";
    analysis.projection = "compatibility_depth_average";
    analysis.compatibility_projection = "depth_average";
    analysis.wall_model = char(string(State.setup.wall_model));
    analysis.omega3d_final = State.omega3d;
    analysis.psi3d_final = State.psi3d;
    analysis = fv_append_bathymetry_analysis_metadata(analysis, State.setup);
    analysis = fv_maybe_merge_unified_metrics(analysis, Parameters);
    analysis = MethodConfigBuilder.apply_analysis_contract(analysis, cfg, Parameters);
end

function cfg = fv_cfg_from_parameters(Parameters)
    cfg = MethodConfigBuilder.build(Parameters, "fv", "fv.run");
    if ~isfield(cfg, "snap_times") || isempty(cfg.snap_times)
        error("FV:MissingSnapTimes", ...
            "fv.run requires snap_times in the method config.");
    end
end

function cfg = fv_normalize_cfg(cfg)
    needed = {"nu", "Lx", "Ly", "Nx", "Ny", "dt", "Tfinal"};
    for i = 1:numel(needed)
        if ~isfield(cfg, needed{i})
            error("FV:MissingField", "Missing required field: %s", needed{i});
        end
    end

    if cfg.Nx <= 0 || cfg.Ny <= 0 || cfg.Nz <= 0 || cfg.dt <= 0 || cfg.Tfinal <= 0
        error("FV:InvalidConfig", "Nx, Ny, Nz, dt, and Tfinal must be positive.");
    end

    if ~isfield(cfg, "fv3d") || ~isstruct(cfg.fv3d)
        error("FV:MissingFV3DConfig", ...
            "fv3d config must be provided by MethodConfigBuilder.");
    end
    if ~isfield(cfg.fv3d, "vertical_diffusivity_scale") || isempty(cfg.fv3d.vertical_diffusivity_scale)
        error("FV:MissingVerticalDiffusivityScale", ...
            "fv3d.vertical_diffusivity_scale is required.");
    end
    if ~isfield(cfg.fv3d, "z_boundary") || isempty(cfg.fv3d.z_boundary)
        error("FV:MissingZBoundary", ...
            "fv3d.z_boundary is required.");
    end
    if isfield(cfg, "time_integrator") && ~isempty(cfg.time_integrator) && ~strcmpi(cfg.time_integrator, "SSP_RK3")
        error("FV:InvalidTimeIntegrator", ...
            "Finite Volume uses SSP_RK3 in the active conservative 3D runtime.");
    end
end

function setup = fv_build_setup(cfg)
    dx = cfg.dx;
    dy = cfg.dy;
    dz = cfg.Lz / cfg.Nz;

    x = linspace(0, cfg.Lx - dx, cfg.Nx);
    y = linspace(0, cfg.Ly - dy, cfg.Ny);
    z = linspace(0, cfg.Lz - dz, cfg.Nz);

    [X, Y] = meshgrid(x, y);

    setup = struct();
    setup.Nx = cfg.Nx;
    setup.Ny = cfg.Ny;
    setup.Nz = cfg.Nz;
    setup.dx = dx;
    setup.dy = dy;
    setup.dz = dz;
    setup.Lx = cfg.Lx;
    setup.Ly = cfg.Ly;
    setup.Lz = cfg.Lz;
    setup.cell_volume = dx * dy * dz;
    setup.x = x;
    setup.y = y;
    setup.z = z;
    setup.X = X;
    setup.Y = Y;
    setup.nu_z = cfg.nu * cfg.fv3d.vertical_diffusivity_scale;
    setup.z_bc = char(string(cfg.fv3d.z_boundary));
    setup.bottom_psi_value = 0.0;
end

function omega2d = fv_initial_vorticity_2d(cfg, X, Y)
    if isfield(cfg, "omega") && ~isempty(cfg.omega)
        omega2d = cfg.omega;
        return;
    end

    omega2d = ICDispatcher.resolve(X, Y, cfg, 'fv');
end

function omega3d = fv_lift_to_3d(omega2d, setup)
    omega3d = zeros(setup.Ny, setup.Nx, setup.Nz);

    z_mid = 0.5 * setup.Lz;
    sigma = max(setup.Lz / 6, eps);

    profile = exp(-((setup.z - z_mid).^2) / (2 * sigma^2));
    profile = profile / max(mean(profile), eps);

    for k = 1:setup.Nz
        omega3d(:, :, k) = omega2d * profile(k);
    end
end

function psi3d = fv_solve_poisson_layers(omega3d, setup)
    psi3d = zeros(size(omega3d));
    for k = 1:setup.Nz
        if setup.layer_poisson.use_fft(k)
            psi3d(:, :, k) = fv_poisson_2d_periodic(omega3d(:, :, k), setup);
        else
            psi3d(:, :, k) = fv_solve_poisson_masked_layer(omega3d(:, :, k), setup.layer_poisson.layers{k});
        end
    end
    psi3d = fv_apply_domain_mask(psi3d, setup);
end

function psi = fv_poisson_2d_periodic(omega, setup)
    [Ny, Nx] = size(omega);
    omega_hat = fft2(omega);

    kx = 2 * pi / setup.Lx * [0:(Nx/2 - 1), (-Nx/2):-1];
    ky = 2 * pi / setup.Ly * [0:(Ny/2 - 1), (-Ny/2):-1];
    [Kx, Ky] = meshgrid(kx, ky);

    K2 = Kx.^2 + Ky.^2;
    K2(1, 1) = 1;

    psi_hat = -omega_hat ./ K2;
    psi_hat(1, 1) = 0;

    psi = real(ifft2(psi_hat));
end

function [u3d, v3d] = fv_velocity_from_psi3d(psi3d, setup)
    [Ny, Nx, Nz] = size(psi3d);
    u3d = zeros(Ny, Nx, Nz);
    v3d = zeros(Ny, Nx, Nz);

    for k = 1:Nz
        psi = psi3d(:, :, k);
        u3d(:, :, k) = -(fv_shift_yp(psi, setup) - fv_shift_ym(psi, setup)) / (2 * setup.dy);
        v3d(:, :, k) = (fv_shift_xp(psi, setup) - fv_shift_xm(psi, setup)) / (2 * setup.dx);
    end
    if isfield(setup, 'enforce_velocity_bc') && isa(setup.enforce_velocity_bc, 'function_handle')
        [u3d, v3d] = setup.enforce_velocity_bc(u3d, v3d, setup);
    end
end

function registry = fv_transport_channel_registry()
    registry = struct( ...
        'name', {'omega3d', 'w3d'}, ...
        'enabled', {true, false}, ...
        'role', {'primary_vorticity', 'reserved_vortex_state'});
end

function transport_state = fv_initialize_transport_state(setup, initial_channels)
    registry = setup.transport_channel_registry;
    transport_state = struct();
    transport_state.channel_registry = registry;
    transport_state.channels = struct();
    transport_state.active_channel_names = {};
    transport_state.reserved_channels = struct();

    for i = 1:numel(registry)
        name = registry(i).name;
        if registry(i).enabled
            if ~isfield(initial_channels, name)
                error("FV:MissingInitialChannel", ...
                    "Initial payload for active transported channel '%s' is required.", name);
            end
            transport_state.channels.(name) = double(initial_channels.(name));
            transport_state.active_channel_names{end + 1} = name;
        else
            transport_state.reserved_channels.(name) = [];
        end
    end
end

function State = fv_state_from_transport(transport_state, closure_state, setup, t, step)
    omega3d = fv_get_transport_channel(transport_state, 'omega3d');
    psi3d = closure_state.psi3d;

    State = struct();
    State.transport_state = transport_state;
    State.closure_state = closure_state;
    State.omega3d = omega3d;
    State.psi3d = psi3d;
    State.omega = fv_reduce_to_2d(omega3d, setup);
    State.psi = fv_reduce_to_2d(psi3d, setup);
    State.t = t;
    State.step = step;
    State.setup = setup;
end

function closure_state = fv_get_closure_state(State, setup)
    if isfield(State, 'closure_state') && isstruct(State.closure_state) && ...
            isfield(State.closure_state, 'psi3d') && isfield(State.closure_state, 'u3d') && isfield(State.closure_state, 'v3d')
        closure_state = State.closure_state;
    else
        [~, closure_state] = fv_prepare_transport_rhs_inputs(State.transport_state, setup, struct('dt', 0, 'nu', 0), false);
    end
end

function omega3d = fv_get_primary_omega3d(State)
    if isfield(State, 'transport_state') && isstruct(State.transport_state)
        omega3d = fv_get_transport_channel(State.transport_state, 'omega3d');
    else
        omega3d = State.omega3d;
    end
end

function [transport_next, closure_next] = fv_advance_transport_ssprk3(transport_state, setup, cfg)
    [qn, closure_n] = fv_prepare_transport_rhs_inputs(transport_state, setup, cfg, true);
    rhs1 = fv_transport_rhs_bundle(qn, closure_n, setup, cfg);
    q1 = fv_transport_add_scaled(qn, rhs1, cfg.dt);

    [q1, closure_1] = fv_prepare_transport_rhs_inputs(q1, setup, cfg, true);
    rhs2 = fv_transport_rhs_bundle(q1, closure_1, setup, cfg);
    q2_tmp = fv_transport_add_scaled(q1, rhs2, cfg.dt);
    q2 = fv_transport_axpby(qn, 0.75, q2_tmp, 0.25);

    [q2, closure_2] = fv_prepare_transport_rhs_inputs(q2, setup, cfg, true);
    rhs3 = fv_transport_rhs_bundle(q2, closure_2, setup, cfg);
    q3_tmp = fv_transport_add_scaled(q2, rhs3, cfg.dt);
    transport_next = fv_transport_axpby(qn, 1 / 3, q3_tmp, 2 / 3);

    [transport_next, closure_next] = fv_prepare_transport_rhs_inputs(transport_next, setup, cfg, true);
end

function [transport_state, closure_state] = fv_prepare_transport_rhs_inputs(transport_state, setup, cfg, validate_cfl)
    transport_state = fv_apply_transport_constraints(transport_state, setup);
    closure_state = fv_resolve_closure_state(transport_state, setup);
    transport_state = fv_apply_transport_boundary_updates(transport_state, closure_state, setup);
    transport_state = fv_apply_transport_constraints(transport_state, setup);
    closure_state = fv_resolve_closure_state(transport_state, setup);

    if nargin >= 4 && validate_cfl
        fv_validate_transport_cfl(closure_state, setup, cfg);
    end
end

function closure_state = fv_resolve_closure_state(transport_state, setup)
    omega3d = fv_get_transport_channel(transport_state, 'omega3d');
    psi3d = fv_solve_poisson_layers(omega3d, setup);
    [u3d, v3d] = fv_velocity_from_psi3d(psi3d, setup);

    closure_state = struct();
    closure_state.psi3d = psi3d;
    closure_state.u3d = u3d;
    closure_state.v3d = v3d;
end

function transport_state = fv_apply_transport_boundary_updates(transport_state, closure_state, setup)
    if isfield(transport_state.channels, 'omega3d')
        omega3d = transport_state.channels.omega3d;
        omega3d = setup.apply_bc(omega3d, closure_state.psi3d, setup);
        transport_state.channels.omega3d = omega3d;
    end
end

function transport_state = fv_apply_transport_constraints(transport_state, setup)
    names = transport_state.active_channel_names;
    for i = 1:numel(names)
        name = names{i};
        transport_state.channels.(name) = fv_apply_domain_mask(transport_state.channels.(name), setup);
    end
end

function rhs_bundle = fv_transport_rhs_bundle(transport_state, closure_state, setup, cfg)
    rhs_bundle = fv_empty_transport_like(transport_state);
    names = transport_state.active_channel_names;
    for i = 1:numel(names)
        name = names{i};
        rhs = fv_conservative_rhs_for_channel(transport_state.channels.(name), closure_state, setup, cfg.nu, setup.nu_z);
        rhs_bundle.channels.(name) = rhs;
    end

    source_bundle = fv_transport_source_bundle(transport_state, closure_state, setup);
    rhs_bundle = fv_transport_axpby(rhs_bundle, 1.0, source_bundle, 1.0);
end

function rhs_bundle = fv_transport_source_bundle(transport_state, ~, ~)
    rhs_bundle = fv_empty_transport_like(transport_state);
end

function rhs = fv_conservative_rhs_for_channel(field3d, closure_state, setup, nu_xy, nu_z)
    adv_div = fv_advective_flux_divergence(field3d, closure_state.u3d, closure_state.v3d, setup);
    diff_xy = fv_diffusive_flux_divergence_xy(field3d, setup);
    diff_z = fv_diffusive_flux_divergence_z(field3d, setup);
    rhs = -adv_div + nu_xy * diff_xy + nu_z * diff_z;
    rhs = fv_zero_fixed_rhs(rhs, setup);
end

function div = fv_advective_flux_divergence(field3d, u3d, v3d, setup)
    [Ny, Nx, Nz] = size(field3d);
    face_masks = setup.face_masks;

    flux_x = zeros(Ny, Nx + 1, Nz);
    flux_y = zeros(Ny + 1, Nx, Nz);

    flux_x(:, 2:Nx, :) = 0.25 * (u3d(:, 1:Nx-1, :) + u3d(:, 2:Nx, :)) .* ...
        (field3d(:, 1:Nx-1, :) + field3d(:, 2:Nx, :));
    flux_y(2:Ny, :, :) = 0.25 * (v3d(1:Ny-1, :, :) + v3d(2:Ny, :, :)) .* ...
        (field3d(1:Ny-1, :, :) + field3d(2:Ny, :, :));

    if setup.periodic_x
        wrap_flux_x = 0.25 * (u3d(:, end, :) + u3d(:, 1, :)) .* (field3d(:, end, :) + field3d(:, 1, :));
        flux_x(:, 1, :) = wrap_flux_x;
        flux_x(:, end, :) = wrap_flux_x;
    end
    if setup.periodic_y
        wrap_flux_y = 0.25 * (v3d(end, :, :) + v3d(1, :, :)) .* (field3d(end, :, :) + field3d(1, :, :));
        flux_y(1, :, :) = wrap_flux_y;
        flux_y(end, :, :) = wrap_flux_y;
    end

    flux_x(~face_masks.x) = 0;
    flux_y(~face_masks.y) = 0;

    div = (flux_x(:, 2:end, :) - flux_x(:, 1:end-1, :)) / setup.dx + ...
        (flux_y(2:end, :, :) - flux_y(1:end-1, :, :)) / setup.dy;
end

function div = fv_diffusive_flux_divergence_xy(field3d, setup)
    [Ny, Nx, Nz] = size(field3d);
    face_masks = setup.face_masks;

    grad_x = zeros(Ny, Nx + 1, Nz);
    grad_y = zeros(Ny + 1, Nx, Nz);

    grad_x(:, 2:Nx, :) = (field3d(:, 2:Nx, :) - field3d(:, 1:Nx-1, :)) / setup.dx;
    grad_y(2:Ny, :, :) = (field3d(2:Ny, :, :) - field3d(1:Ny-1, :, :)) / setup.dy;

    if setup.periodic_x
        wrap_grad_x = (field3d(:, 1, :) - field3d(:, end, :)) / setup.dx;
        grad_x(:, 1, :) = wrap_grad_x;
        grad_x(:, end, :) = wrap_grad_x;
    end
    if setup.periodic_y
        wrap_grad_y = (field3d(1, :, :) - field3d(end, :, :)) / setup.dy;
        grad_y(1, :, :) = wrap_grad_y;
        grad_y(end, :, :) = wrap_grad_y;
    end

    grad_x(~face_masks.x) = 0;
    grad_y(~face_masks.y) = 0;

    div = (grad_x(:, 2:end, :) - grad_x(:, 1:end-1, :)) / setup.dx + ...
        (grad_y(2:end, :, :) - grad_y(1:end-1, :, :)) / setup.dy;
end

function div = fv_diffusive_flux_divergence_z(field3d, setup)
    [Ny, Nx, Nz] = size(field3d);
    face_masks = setup.face_masks;
    grad_z = zeros(Ny, Nx, Nz + 1);

    grad_z(:, :, 2:Nz) = (field3d(:, :, 2:Nz) - field3d(:, :, 1:Nz-1)) / setup.dz;

    if strcmpi(setup.z_bc, "periodic")
        wrap_grad_z = (field3d(:, :, 1) - field3d(:, :, end)) / setup.dz;
        grad_z(:, :, 1) = wrap_grad_z;
        grad_z(:, :, end) = wrap_grad_z;
    end

    grad_z(~face_masks.z) = 0;
    div = (grad_z(:, :, 2:end) - grad_z(:, :, 1:end-1)) / setup.dz;
end

function transport_out = fv_transport_add_scaled(transport_in, rhs_in, scale)
    transport_out = transport_in;
    names = transport_in.active_channel_names;
    for i = 1:numel(names)
        name = names{i};
        transport_out.channels.(name) = transport_in.channels.(name) + scale * rhs_in.channels.(name);
    end
end

function transport_out = fv_transport_axpby(transport_a, alpha, transport_b, beta)
    transport_out = transport_a;
    names = transport_a.active_channel_names;
    for i = 1:numel(names)
        name = names{i};
        transport_out.channels.(name) = alpha * transport_a.channels.(name) + beta * transport_b.channels.(name);
    end
end

function transport_state = fv_empty_transport_like(reference_transport)
    transport_state = reference_transport;
    names = transport_state.active_channel_names;
    for i = 1:numel(names)
        name = names{i};
        transport_state.channels.(name) = zeros(size(reference_transport.channels.(name)));
    end
end

function field3d = fv_get_transport_channel(transport_state, channel_name)
    if ~isfield(transport_state.channels, channel_name)
        error("FV:MissingTransportChannel", ...
            "Transport channel '%s' is not active in the current state bundle.", channel_name);
    end
    field3d = transport_state.channels.(channel_name);
end

function fv_validate_transport_cfl(closure_state, setup, cfg)
    if ~isfield(cfg, 'dt') || cfg.dt <= 0
        return;
    end

    wet_mask = fv_wet_mask3d(setup);
    if ~any(wet_mask(:))
        return;
    end

    advective_density = abs(closure_state.u3d) / setup.dx + abs(closure_state.v3d) / setup.dy;
    advective_cfl = cfg.dt * max(advective_density(wet_mask));
    diffusive_cfl = cfg.dt * (2 * cfg.nu / setup.dx^2 + 2 * cfg.nu / setup.dy^2 + 2 * setup.nu_z / setup.dz^2);

    if advective_cfl > 1.0
        error("FV:CFLViolation", ...
            "FV advective CFL %.4g exceeds the SSP_RK3 stability envelope.", advective_cfl);
    end
    if diffusive_cfl > 1.0
        error("FV:CFLViolation", ...
            "FV diffusive CFL %.4g exceeds the active explicit stability envelope.", diffusive_cfl);
    end
end

function geometry = resolve_fv_bathymetry_geometry(cfg, setup)
% resolve_fv_bathymetry_geometry Build active 3D bathymetry/seabed geometry.
    extra = struct('Nz', setup.Nz, 'Lz', setup.Lz, 'dz', setup.dz, 'z', setup.z);
    geometry = build_bathymetry_geometry(cfg, setup.X, setup.Y, 'fv', extra);
    if ~geometry.enabled
        scenario = 'flat_3d';
        if isfield(cfg, 'bathymetry_scenario') && ~isempty(cfg.bathymetry_scenario)
            scenario = char(string(cfg.bathymetry_scenario));
        end
        geometry = struct('enabled', false, 'dimension', '3d', ...
            'wet_mask', true(setup.Ny, setup.Nx, setup.Nz), ...
            'solid_mask', false(setup.Ny, setup.Nx, setup.Nz), ...
            'boundary_mask', false(setup.Ny, setup.Nx, setup.Nz), ...
            'floor_height', zeros(setup.Ny, setup.Nx), ...
            'bottom_drive_scale', ones(setup.Ny, setup.Nx), ...
            'bottom_drive_u', ones(setup.Ny, setup.Nx), ...
            'bottom_drive_v', zeros(setup.Ny, setup.Nx), ...
            'scenario', scenario);
    end
end

function setup = fv_finalize_setup_masks(setup, bc)
% fv_finalize_setup_masks Combine seabed and outer-wall masks into active fixed-node masks.
    wet_mask = fv_wet_mask3d(setup);
    fixed_mask = false(size(wet_mask));
    outer_wall_mask = false(size(wet_mask));
    wall_sides = bc.method.fv.wall_sides;

    if wall_sides.top
        outer_wall_mask(end, :, :) = wet_mask(end, :, :);
    end
    if wall_sides.bottom
        outer_wall_mask(1, :, :) = wet_mask(1, :, :);
    end
    if wall_sides.left
        outer_wall_mask(:, 1, :) = wet_mask(:, 1, :);
    end
    if wall_sides.right
        outer_wall_mask(:, end, :) = wet_mask(:, end, :);
    end

    if isfield(setup.bathymetry_geometry, 'enabled') && setup.bathymetry_geometry.enabled && ...
            isfield(setup.bathymetry_geometry, 'boundary_mask')
        fixed_mask = fixed_mask | logical(setup.bathymetry_geometry.boundary_mask);
    end

    fixed_mask = fixed_mask | outer_wall_mask;

    setup.outer_wall_mask = outer_wall_mask;
    setup.fixed_mask = fixed_mask;
    setup.use_fft_poisson = logical(setup.periodic_x && setup.periodic_y && ...
        ~(isfield(setup.bathymetry_geometry, 'enabled') && setup.bathymetry_geometry.enabled));
    setup.face_masks = fv_build_face_masks(setup, wet_mask);
end

function face_masks = fv_build_face_masks(setup, wet_mask)
    [Ny, Nx, Nz] = size(wet_mask);

    face_masks = struct();
    face_masks.x = false(Ny, Nx + 1, Nz);
    face_masks.y = false(Ny + 1, Nx, Nz);
    face_masks.z = false(Ny, Nx, Nz + 1);

    face_masks.x(:, 2:Nx, :) = wet_mask(:, 1:Nx-1, :) & wet_mask(:, 2:Nx, :);
    face_masks.y(2:Ny, :, :) = wet_mask(1:Ny-1, :, :) & wet_mask(2:Ny, :, :);
    face_masks.z(:, :, 2:Nz) = wet_mask(:, :, 1:Nz-1) & wet_mask(:, :, 2:Nz);

    if setup.periodic_x
        wrap_x = wet_mask(:, end, :) & wet_mask(:, 1, :);
        face_masks.x(:, 1, :) = wrap_x;
        face_masks.x(:, end, :) = wrap_x;
    end
    if setup.periodic_y
        wrap_y = wet_mask(end, :, :) & wet_mask(1, :, :);
        face_masks.y(1, :, :) = wrap_y;
        face_masks.y(end, :, :) = wrap_y;
    end
    if strcmpi(setup.z_bc, "periodic")
        wrap_z = wet_mask(:, :, end) & wet_mask(:, :, 1);
        face_masks.z(:, :, 1) = wrap_z;
        face_masks.z(:, :, end) = wrap_z;
    end
end

function layer_poisson = fv_build_layer_poisson_solvers(setup, bc)
% fv_build_layer_poisson_solvers Precompute sparse Poisson solves for masked/non-periodic layers.
    layer_poisson = struct();
    layer_poisson.use_fft = false(setup.Nz, 1);
    layer_poisson.layers = cell(setup.Nz, 1);
    wall_sides = bc.method.fv.wall_sides;
    psi_boundary = bc.method.fv.psi_boundary;

    wet_mask = fv_wet_mask3d(setup);
    for k = 1:setup.Nz
        if setup.use_fft_poisson
            layer_poisson.use_fft(k) = true;
            layer_poisson.layers{k} = [];
            continue;
        end

        wet2d = wet_mask(:, :, k);
        fixed2d = logical(setup.fixed_mask(:, :, k)) | ~wet2d;
        boundary_values = zeros(setup.Ny, setup.Nx);
        if wall_sides.top
            boundary_values(end, :) = psi_boundary.top;
        end
        if wall_sides.bottom
            boundary_values(1, :) = psi_boundary.bottom;
        end
        if wall_sides.left
            boundary_values(:, 1) = psi_boundary.left;
        end
        if wall_sides.right
            boundary_values(:, end) = psi_boundary.right;
        end
        if isfield(setup.bathymetry_geometry, 'enabled') && setup.bathymetry_geometry.enabled && ...
                isfield(setup.bathymetry_geometry, 'boundary_mask')
            seabed_mask = logical(setup.bathymetry_geometry.boundary_mask(:, :, k));
            boundary_values(seabed_mask) = psi_boundary.bottom;
        end

        unknown_mask = wet2d & ~fixed2d;
        layer_poisson.layers{k} = fv_build_layer_poisson_solver( ...
            setup, wet2d, unknown_mask, fixed2d, boundary_values);
    end
end

function layer_meta = fv_build_layer_poisson_solver(setup, wet2d, unknown_mask, fixed2d, boundary_values)
% fv_build_layer_poisson_solver Build one masked 2D Poisson solve.
    layer_meta = struct();
    layer_meta.wet_mask = wet2d;
    layer_meta.unknown_mask = unknown_mask;
    layer_meta.fixed_mask = fixed2d;
    layer_meta.boundary_values = boundary_values;

    n_unknown = nnz(unknown_mask);
    if n_unknown == 0
        layer_meta.A = sparse(0, 0);
        layer_meta.solve = [];
        layer_meta.index_map = zeros(size(unknown_mask));
        layer_meta.rhs_offset = zeros(0, 1);
        return;
    end

    index_map = zeros(size(unknown_mask));
    index_map(unknown_mask) = 1:n_unknown;
    rows = [];
    cols = [];
    vals = [];
    rhs_offset = zeros(n_unknown, 1);

    [rr, cc] = find(unknown_mask);
    for n = 1:numel(rr)
        row = rr(n);
        col = cc(n);
        idx = index_map(row, col);
        diag_val = -2 / setup.dx^2 - 2 / setup.dy^2;

        [rows, cols, vals, rhs_offset] = fv_add_poisson_neighbor( ...
            rows, cols, vals, rhs_offset, idx, row, col - 1, 'x', setup, unknown_mask, fixed2d, boundary_values, index_map);
        [rows, cols, vals, rhs_offset] = fv_add_poisson_neighbor( ...
            rows, cols, vals, rhs_offset, idx, row, col + 1, 'x', setup, unknown_mask, fixed2d, boundary_values, index_map);
        [rows, cols, vals, rhs_offset] = fv_add_poisson_neighbor( ...
            rows, cols, vals, rhs_offset, idx, row - 1, col, 'y', setup, unknown_mask, fixed2d, boundary_values, index_map);
        [rows, cols, vals, rhs_offset] = fv_add_poisson_neighbor( ...
            rows, cols, vals, rhs_offset, idx, row + 1, col, 'y', setup, unknown_mask, fixed2d, boundary_values, index_map);

        rows(end + 1, 1) = idx; %#ok<AGROW>
        cols(end + 1, 1) = idx; %#ok<AGROW>
        vals(end + 1, 1) = diag_val; %#ok<AGROW>
    end

    A = sparse(rows, cols, vals, n_unknown, n_unknown);
    layer_meta.A = A;
    layer_meta.solve = decomposition(A, "lu");
    layer_meta.index_map = index_map;
    layer_meta.rhs_offset = rhs_offset;
end

function [rows, cols, vals, rhs_offset] = fv_add_poisson_neighbor(rows, cols, vals, rhs_offset, idx, row, col, axis_id, setup, unknown_mask, fixed2d, boundary_values, index_map)
    if axis_id == 'x'
        coeff = 1 / setup.dx^2;
        periodic_axis = setup.periodic_x;
    else
        coeff = 1 / setup.dy^2;
        periodic_axis = setup.periodic_y;
    end

    if periodic_axis
        if col < 1
            col = size(unknown_mask, 2);
        elseif col > size(unknown_mask, 2)
            col = 1;
        elseif row < 1
            row = size(unknown_mask, 1);
        elseif row > size(unknown_mask, 1)
            row = 1;
        end
    end

    if row < 1 || row > size(unknown_mask, 1) || col < 1 || col > size(unknown_mask, 2)
        return;
    end

    if unknown_mask(row, col)
        rows(end + 1, 1) = idx;
        cols(end + 1, 1) = index_map(row, col);
        vals(end + 1, 1) = coeff;
    elseif fixed2d(row, col)
        rhs_offset(idx) = rhs_offset(idx) - coeff * boundary_values(row, col);
    end
end

function psi = fv_solve_poisson_masked_layer(omega, layer_meta)
% fv_solve_poisson_masked_layer Solve one masked/non-periodic Poisson layer.
    psi = layer_meta.boundary_values;
    if isempty(layer_meta.solve)
        psi(~layer_meta.wet_mask) = 0;
        return;
    end
    rhs = omega(layer_meta.unknown_mask) + layer_meta.rhs_offset;
    psi(layer_meta.unknown_mask) = layer_meta.solve \ rhs;
    psi(~layer_meta.wet_mask) = 0;
end

function F = fv_apply_domain_mask(F, setup)
% fv_apply_domain_mask Zero out solid cells under the active seabed geometry.
    wet_mask = fv_wet_mask3d(setup);
    F(~wet_mask) = 0;
end

function rhs = fv_zero_fixed_rhs(rhs, setup)
% fv_zero_fixed_rhs Keep wall/seabed boundary nodes fixed during evolution.
    rhs(setup.fixed_mask) = 0;
    rhs(~fv_wet_mask3d(setup)) = 0;
end

function wet_mask = fv_wet_mask3d(setup)
% fv_wet_mask3d Convenience accessor for active wet mask.
    if isfield(setup, 'bathymetry_geometry') && isstruct(setup.bathymetry_geometry) && ...
            isfield(setup.bathymetry_geometry, 'wet_mask') && ~isempty(setup.bathymetry_geometry.wet_mask)
        wet_mask = logical(setup.bathymetry_geometry.wet_mask);
    else
        wet_mask = true(setup.Ny, setup.Nx, setup.Nz);
    end
end

function field2d = fv_reduce_to_2d(field3d, setup)
% fv_reduce_to_2d Wet-depth average for 2D reporting/plotting.
    wet_mask = fv_wet_mask3d(setup);
    wet_count = sum(wet_mask, 3);
    wet_count(wet_count == 0) = 1;
    field2d = sum(field3d .* wet_mask, 3) ./ wet_count;
end

function shifted = fv_shift_xp(F, setup)
    if ndims(F) == 2
        if setup.periodic_x
            shifted = circshift(F, [0, +1]);
        else
            shifted = [F(:, 1), F(:, 1:end-1)];
        end
    else
        if setup.periodic_x
            shifted = circshift(F, [0, +1, 0]);
        else
            shifted = cat(2, F(:, 1, :), F(:, 1:end-1, :));
        end
    end
end

function shifted = fv_shift_xm(F, setup)
    if ndims(F) == 2
        if setup.periodic_x
            shifted = circshift(F, [0, -1]);
        else
            shifted = [F(:, 2:end), F(:, end)];
        end
    else
        if setup.periodic_x
            shifted = circshift(F, [0, -1, 0]);
        else
            shifted = cat(2, F(:, 2:end, :), F(:, end, :));
        end
    end
end

function shifted = fv_shift_yp(F, setup)
    if ndims(F) == 2
        if setup.periodic_y
            shifted = circshift(F, [+1, 0]);
        else
            shifted = [F(1, :); F(1:end-1, :)];
        end
    else
        if setup.periodic_y
            shifted = circshift(F, [+1, 0, 0]);
        else
            shifted = cat(1, F(1, :, :), F(1:end-1, :, :));
        end
    end
end

function shifted = fv_shift_ym(F, setup)
    if ndims(F) == 2
        if setup.periodic_y
            shifted = circshift(F, [-1, 0]);
        else
            shifted = [F(2:end, :); F(end, :)];
        end
    else
        if setup.periodic_y
            shifted = circshift(F, [-1, 0, 0]);
        else
            shifted = cat(1, F(2:end, :, :), F(end, :, :));
        end
    end
end

function analysis = fv_append_bathymetry_analysis_metadata(analysis, setup)
% fv_append_bathymetry_analysis_metadata Attach normalized 3D seabed metadata to analysis.
    if ~isfield(setup, 'bathymetry_geometry') || ~isstruct(setup.bathymetry_geometry)
        return;
    end
    geometry = setup.bathymetry_geometry;
    if ~isfield(geometry, 'enabled') || ~geometry.enabled
        return;
    end

    analysis.bathymetry_geometry_dimension = '3d';
    analysis.bathymetry_floor_3d = double(geometry.floor_height);
    analysis.bathymetry_wet_mask_3d = logical(geometry.wet_mask);
    analysis.bathymetry_boundary_mask_3d = logical(geometry.boundary_mask);
    analysis.bathymetry_field = double(geometry.bathymetry_field);
    if isfield(geometry, 'cell_averaged_bathymetry')
        analysis.bathymetry_cell_average_3d = double(geometry.cell_averaged_bathymetry);
    end
    if isfield(geometry, 'bathymetry_slope_x')
        analysis.bathymetry_slope_x_3d = double(geometry.bathymetry_slope_x);
    end
    if isfield(geometry, 'bathymetry_slope_y')
        analysis.bathymetry_slope_y_3d = double(geometry.bathymetry_slope_y);
    end
    if isfield(geometry, 'interface_floor_height_x')
        analysis.bathymetry_interface_floor_x = double(geometry.interface_floor_height_x);
    end
    if isfield(geometry, 'interface_floor_height_y')
        analysis.bathymetry_interface_floor_y = double(geometry.interface_floor_height_y);
    end
    analysis.bathymetry_scenario = char(string(geometry.scenario));
end

function [omega_snaps, psi_snaps, kinetic_energy, enstrophy, peak_speed, max_omega, sampled_times, next_idx] = ...
        fv_store_snapshot(State, omega_snaps, psi_snaps, kinetic_energy, enstrophy, peak_speed, max_omega, sampled_times, idx)
    M = fv_diagnostics_internal(State);

    omega_snaps(:, :, idx) = State.omega;
    psi_snaps(:, :, idx) = State.psi;
    kinetic_energy(idx) = M.kinetic_energy;
    enstrophy(idx) = M.enstrophy;
    peak_speed(idx) = M.peak_speed;
    max_omega(idx) = M.max_vorticity;
    sampled_times(idx) = State.t;
    next_idx = idx + 1;
end

function analysis = fv_maybe_merge_unified_metrics(analysis, Parameters)
    metrics = extract_unified_metrics( ...
        analysis.omega_snaps, ...
        analysis.psi_snaps, ...
        analysis.snapshot_times, ...
        analysis.dx, ...
        analysis.dy, ...
        Parameters);

    analysis = merge_structs(analysis, metrics);
    if ~isfield(analysis, "peak_abs_omega") || isempty(analysis.peak_abs_omega)
        analysis.peak_abs_omega = max(abs(analysis.omega_snaps(:)));
    end
    analysis.peak_vorticity = analysis.peak_abs_omega;
end

function fig_handle = fv_summary_figure(analysis)
    show_figs = usejava("desktop") && ~strcmpi(get(0, "DefaultFigureVisible"), "off");
    fig_visibility = "off";
    if show_figs
        fig_visibility = "on";
    end

    fig_handle = figure("Name", "Finite Volume 3D Conservative Analysis", "NumberTitle", "off", "Visible", fig_visibility);
    apply_dark_theme_for_figure(fig_handle);

    subplot(1, 2, 1);
    contourf(analysis.omega_snaps(:, :, end), 20);
    colorbar;
    title("Compatibility-projected vorticity (final)");
    xlabel("x-index");
    ylabel("y-index");

    subplot(1, 2, 2);
    semilogy(analysis.time_vec, analysis.enstrophy + 1e-10, "LineWidth", 1.6);
    hold on;
    semilogy(analysis.time_vec, analysis.kinetic_energy + 1e-10, "LineWidth", 1.6);
    legend("Enstrophy", "Kinetic Energy", "Location", "best");
    xlabel("Time");
    ylabel("Value");
    grid on;
end

function merged = merge_structs(a, b)
    merged = a;
    if isempty(b)
        return;
    end
    fields = fieldnames(b);
    for i = 1:numel(fields)
        merged.(fields{i}) = b.(fields{i});
    end
end

function bc = fv_resolve_bc(cfg)
    grid_meta = struct('Lx', cfg.Lx, 'Ly', cfg.Ly, 'Nx', cfg.Nx, 'Ny', cfg.Ny);
    bc = BCDispatcher.resolve(cfg, 'fv', grid_meta);
end

function apply_dark_theme_for_figure(fig_handle)
    if isempty(fig_handle) || ~isvalid(fig_handle)
        return;
    end
    try
        ResultsPlotDispatcher.apply_dark_theme(fig_handle, ResultsPlotDispatcher.default_colors());
    catch
        % Styling helper is optional for solver correctness.
    end
end
