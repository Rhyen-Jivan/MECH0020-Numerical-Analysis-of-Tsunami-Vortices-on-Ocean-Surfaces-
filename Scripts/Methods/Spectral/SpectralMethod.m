function varargout = SpectralMethod(action, varargin)
% SpectralMethod - Self-contained spectral (FFT) method module.
%
% Supported actions:
%   callbacks                     -> struct with init/step/diagnostics/run/finalize_analysis handles
%   init(cfg, ctx)                -> State
%   step(State, cfg, ctx)         -> State
%   diagnostics(State, cfg, ctx)  -> Metrics
%   finalize_analysis(...)        -> analysis
%   run(Parameters)               -> [fig_handle, analysis]
%   debug_build_setup(cfg)        -> normalized setup for contract tests
%   debug_forward_transform(f,s)  -> transform physical field to modal coefficients
%   debug_inverse_transform(c,s)  -> transform modal coefficients to physical field
%   debug_streamfunction_hat(c,s) -> invert Poisson operator in modal space
%   debug_derivative_x(f,s)       -> basis-aware x-derivative on the physical grid
%   debug_derivative_y(f,s)       -> basis-aware y-derivative on the physical grid
%   debug_velocity_from_psi(f,s)  -> basis-aware velocity recovery on the physical grid
%
% Notes:
%   - Single-file method module following the FD action-contract pattern.
%   - Uses Fourier-space RK4 stepping with 2/3-rule dealiasing.
%   - Supports explicit k-space controls through cfg.kx/cfg.ky when provided.

    narginchk(1, inf);
    action_name = lower(string(action));

    switch action_name
        case "callbacks"
            callbacks = struct();
            callbacks.init = @(cfg, ctx) SpectralMethod("init", cfg, ctx);
            callbacks.step = @(State, cfg, ctx) SpectralMethod("step", State, cfg, ctx);
            callbacks.diagnostics = @(State, cfg, ctx) SpectralMethod("diagnostics", State, cfg, ctx);
            callbacks.finalize_analysis = @(analysis, State, cfg, Parameters, ctx) ...
                SpectralMethod("finalize_analysis", analysis, State, cfg, Parameters, ctx);
            callbacks.run = @(Parameters) SpectralMethod("run", Parameters);
            varargout{1} = callbacks;

        case "init"
            cfg = varargin{1};
            varargout{1} = spectral_init_internal(cfg);

        case "step"
            State = varargin{1};
            cfg = varargin{2};
            varargout{1} = spectral_step_internal(State, cfg);

        case "diagnostics"
            State = varargin{1};
            varargout{1} = spectral_diagnostics_internal(State);

        case "finalize_analysis"
            analysis = varargin{1};
            State = varargin{2};
            cfg = varargin{3};
            Parameters = varargin{4};
            varargout{1} = spectral_finalize_analysis_internal(analysis, State, cfg, Parameters);

        case "run"
            Parameters = varargin{1};
            [fig_handle, analysis] = spectral_run_internal(Parameters);
            varargout{1} = fig_handle;
            varargout{2} = analysis;

        case "debug_build_setup"
            cfg = varargin{1};
            cfg = spectral_normalize_cfg(cfg);
            bc = spectral_resolve_bc(cfg);
            if ~bc.capability.supported
                error("Spectral:UnsupportedBoundaryConfiguration", "%s", bc.capability.reason);
            end
            setup = spectral_build_setup(cfg, bc.method.spectral);
            setup.bc = bc;
            spectral_validate_transform_dependencies(setup);
            varargout{1} = setup;

        case "debug_forward_transform"
            varargout{1} = spectral_forward_transform2(varargin{1}, varargin{2});

        case "debug_inverse_transform"
            varargout{1} = spectral_inverse_transform2(varargin{1}, varargin{2});

        case "debug_streamfunction_hat"
            varargout{1} = spectral_streamfunction_hat(varargin{1}, varargin{2});

        case "debug_derivative_x"
            varargout{1} = spectral_derivative_x(varargin{1}, varargin{2});

        case "debug_derivative_y"
            varargout{1} = spectral_derivative_y(varargin{1}, varargin{2});

        case "debug_velocity_from_psi"
            [varargout{1}, varargout{2}] = spectral_velocity_from_psi(varargin{1}, varargin{2});

        otherwise
            error("Spectral:InvalidAction", ...
                "Unsupported action '%s'. Valid actions: callbacks, init, step, diagnostics, finalize_analysis, run, debug_build_setup, debug_forward_transform, debug_inverse_transform, debug_streamfunction_hat, debug_derivative_x, debug_derivative_y, debug_velocity_from_psi.", ...
                char(string(action)));
    end
end

function State = spectral_init_internal(cfg)
    cfg = spectral_normalize_cfg(cfg);
    bc = spectral_resolve_bc(cfg);
    if ~bc.capability.supported
        error("Spectral:UnsupportedBoundaryConfiguration", "%s", bc.capability.reason);
    end

    setup = spectral_build_setup(cfg, bc.method.spectral);
    setup.bc = bc;
    spectral_validate_transform_dependencies(setup);

    omega0 = spectral_initial_vorticity(cfg, setup.X_physical, setup.Y_physical);
    omega0 = reshape(omega0, setup.Ny, setup.Nx) - setup.lifting.omega;

    omega_hat = spectral_forward_transform2(omega0, setup);
    omega_hat = spectral_apply_state_contract(omega_hat, setup);
    psi_hat = spectral_streamfunction_hat(omega_hat, setup);

    State = spectral_state_from_coefficients(omega_hat, psi_hat, setup, 0.0, 0);
end

function State = spectral_step_internal(State, cfg)
    cfg = spectral_normalize_cfg(cfg);
    setup = State.setup;

    dt = cfg.dt;
    nu = cfg.nu;

    k1 = spectral_rhs_hat(State.omega_hat, setup, nu);
    w2 = spectral_apply_state_contract(State.omega_hat + 0.5 * dt * k1, setup);

    k2 = spectral_rhs_hat(w2, setup, nu);
    w3 = spectral_apply_state_contract(State.omega_hat + 0.5 * dt * k2, setup);

    k3 = spectral_rhs_hat(w3, setup, nu);
    w4 = spectral_apply_state_contract(State.omega_hat + dt * k3, setup);

    k4 = spectral_rhs_hat(w4, setup, nu);

    omega_hat = spectral_apply_state_contract( ...
        State.omega_hat + (dt / 6) * (k1 + 2 * k2 + 2 * k3 + k4), setup);
    psi_hat = spectral_streamfunction_hat(omega_hat, setup);

    State = spectral_state_from_coefficients(omega_hat, psi_hat, setup, State.t + dt, State.step + 1);
end

function Metrics = spectral_diagnostics_internal(State)
    setup = State.setup;
    omega = State.omega;
    [u, v] = spectral_total_velocity_from_psi(State.psi, setup);
    integration_mask = spectral_integration_mask(setup);

    Metrics = struct();
    Metrics.max_vorticity = max(abs(omega(:)));
    Metrics.enstrophy = 0.5 * sum((omega(:).^2) .* integration_mask(:)) * setup.dx * setup.dy;
    Metrics.kinetic_energy = 0.5 * sum((u(:).^2 + v(:).^2) .* integration_mask(:)) * setup.dx * setup.dy;
    Metrics.peak_speed = max(sqrt(u(:).^2 + v(:).^2));
    Metrics.max_abs_u_plus_v = max(abs(u(:)) + abs(v(:)));
    Metrics.t = State.t;
    Metrics.step = State.step;
end

function [fig_handle, analysis] = spectral_run_internal(Parameters)
    run_cfg = spectral_cfg_from_parameters(Parameters);
    snapshot_precision = spectral_snapshot_storage_precision(Parameters);

    if ~isfield(run_cfg, "snap_times") || isempty(run_cfg.snap_times) || ~isnumeric(run_cfg.snap_times)
        error("Spectral:MissingSnapTimes", ...
            "snap_times must be provided by the UI/runtime contract for spectral runs.");
    end

    State = spectral_init_internal(run_cfg);

    snap_times = run_cfg.snap_times(:).';
    n_snapshots = numel(snap_times);

    omega_snaps = zeros(run_cfg.Ny, run_cfg.Nx, n_snapshots, snapshot_precision);
    psi_snaps = zeros(run_cfg.Ny, run_cfg.Nx, n_snapshots, snapshot_precision);
    kinetic_energy = zeros(n_snapshots, 1);
    enstrophy = zeros(n_snapshots, 1);
    peak_speed = zeros(n_snapshots, 1);
    max_omega = zeros(n_snapshots, 1);
    sampled_times = zeros(n_snapshots, 1);

    snap_idx = 1;
    while snap_idx <= n_snapshots && State.t >= snap_times(snap_idx) - 1e-12
        [omega_snaps, psi_snaps, kinetic_energy, enstrophy, peak_speed, max_omega, sampled_times, snap_idx] = ...
            spectral_store_snapshot(State, omega_snaps, psi_snaps, kinetic_energy, enstrophy, peak_speed, max_omega, sampled_times, snap_idx);
    end

    Nt = max(0, ceil(run_cfg.Tfinal / run_cfg.dt));
    for n = 1:Nt
        State = spectral_step_internal(State, run_cfg);

        while snap_idx <= n_snapshots && State.t >= snap_times(snap_idx) - 1e-12
            [omega_snaps, psi_snaps, kinetic_energy, enstrophy, peak_speed, max_omega, sampled_times, snap_idx] = ...
                spectral_store_snapshot(State, omega_snaps, psi_snaps, kinetic_energy, enstrophy, peak_speed, max_omega, sampled_times, snap_idx);
        end
    end

    if snap_idx <= n_snapshots
        for idx = snap_idx:n_snapshots
            [omega_snaps, psi_snaps, kinetic_energy, enstrophy, peak_speed, max_omega, sampled_times, ~] = ...
                spectral_store_snapshot(State, omega_snaps, psi_snaps, kinetic_energy, enstrophy, peak_speed, max_omega, sampled_times, idx);
        end
    end

    analysis = struct();
    analysis.method = "spectral_transform_family_rk4";
    analysis.omega_snaps = omega_snaps;
    analysis.psi_snaps = psi_snaps;
    analysis.snapshot_times_requested = snap_times(:);
    analysis.snapshot_times_actual = sampled_times;
    analysis.snapshot_times = snap_times(:);
    analysis.time_vec = sampled_times;
    analysis.snapshots_stored = n_snapshots;
    analysis.grid_points = run_cfg.Nx * run_cfg.Ny;
    analysis.Nx = run_cfg.Nx;
    analysis.Ny = run_cfg.Ny;
    analysis.Lx = run_cfg.Lx;
    analysis.Ly = run_cfg.Ly;
    analysis.dx = State.setup.dx;
    analysis.dy = State.setup.dy;
    analysis.x = State.setup.x_physical;
    analysis.y = State.setup.y_physical;
    analysis.grid_mode = run_cfg.grid_mode;
    analysis.is_anisotropic = run_cfg.is_anisotropic;
    analysis.kinetic_energy = kinetic_energy;
    analysis.enstrophy = enstrophy;
    analysis.peak_speed_history = peak_speed;
    analysis.max_omega_history = max_omega;
    analysis.peak_abs_omega = max(max_omega);
    analysis.peak_vorticity = analysis.peak_abs_omega;
    analysis.kx = State.setup.axis_x.modal_values;
    analysis.ky = State.setup.axis_y.modal_values;
    analysis.frequency_metadata = spectral_frequency_metadata(State.setup);
    analysis.snapshot_storage_precision = snapshot_precision;

    analysis = spectral_finalize_analysis_internal(analysis, State, run_cfg, Parameters);
    analysis = spectral_cast_snapshot_cubes(analysis, snapshot_precision);

    fig_handle = spectral_summary_figure(analysis);
end

function analysis = spectral_finalize_analysis_internal(analysis, State, cfg, Parameters)
% spectral_finalize_analysis_internal Normalize spectral analysis for run/evolution paths.
    analysis.method = "spectral_transform_family_rk4";
    analysis.Nx = cfg.Nx;
    analysis.Ny = cfg.Ny;
    analysis.Lx = cfg.Lx;
    analysis.Ly = cfg.Ly;
    analysis.dx = State.setup.dx;
    analysis.dy = State.setup.dy;
    analysis.x = State.setup.x_physical;
    analysis.y = State.setup.y_physical;
    analysis.grid_mode = cfg.grid_mode;
    analysis.is_anisotropic = cfg.is_anisotropic;
    if ~isfield(analysis, 'grid_points') || isempty(analysis.grid_points)
        analysis.grid_points = cfg.Nx * cfg.Ny;
    end
    analysis.kx = State.setup.axis_x.modal_values;
    analysis.ky = State.setup.axis_y.modal_values;
    analysis.frequency_metadata = spectral_frequency_metadata(State.setup);
    analysis.wall_model = char(string(State.setup.wall_model));
    analysis.lifting_model = char(string(State.setup.lifting_model));
    analysis.boundary_profile = char(string(State.setup.lifting.boundary_profile));
    analysis.bathymetry_model = char(string(State.setup.bathymetry_model));
    if isstruct(Parameters) && isfield(Parameters, 'bathymetry_scenario') && ~isempty(Parameters.bathymetry_scenario)
        analysis.bathymetry_scenario = char(string(Parameters.bathymetry_scenario));
    else
        analysis.bathymetry_scenario = 'flat_2d';
    end
    analysis = spectral_maybe_merge_unified_metrics(analysis, Parameters);
    analysis = spectral_attach_native_velocity_snapshots(analysis, State.setup, Parameters);
    analysis = MethodConfigBuilder.apply_analysis_contract(analysis, cfg, Parameters);
end

function cfg = spectral_cfg_from_parameters(Parameters)
    cfg = MethodConfigBuilder.build(Parameters, "spectral", "spectral.run");
    if ~isfield(cfg, "snap_times") || isempty(cfg.snap_times)
        error("Spectral:MissingSnapTimes", ...
            "spectral.run requires snap_times in the method config.");
    end
end

function cfg = spectral_normalize_cfg(cfg)
    needed = {"nu", "Lx", "Ly", "Nx", "Ny", "dt", "Tfinal"};
    for i = 1:numel(needed)
        if ~isfield(cfg, needed{i})
            error("Spectral:MissingField", "Missing required field: %s", needed{i});
        end
    end

    cfg.user_supplied_kx = isfield(cfg, "kx") && ~isempty(cfg.kx);
    cfg.user_supplied_ky = isfield(cfg, "ky") && ~isempty(cfg.ky);
    if cfg.user_supplied_kx
        cfg.kx = double(cfg.kx(:).');
        if numel(cfg.kx) ~= cfg.Nx
            error("Spectral:InvalidKxLength", ...
                "Length(kx) must match Nx from normalized config contract.");
        end
    else
        cfg.kx = [];
    end

    if cfg.user_supplied_ky
        cfg.ky = double(cfg.ky(:).');
        if numel(cfg.ky) ~= cfg.Ny
            error("Spectral:InvalidKyLength", ...
                "Length(ky) must match Ny from normalized config contract.");
        end
    else
        cfg.ky = [];
    end

    if cfg.Nx <= 0 || cfg.Ny <= 0 || cfg.dt <= 0 || cfg.Tfinal <= 0 || cfg.Lx <= 0 || cfg.Ly <= 0
        error("Spectral:InvalidConfig", "Nx, Ny, dt, Tfinal, Lx, Ly must all be positive.");
    end
end

function setup = spectral_build_setup(cfg, spectral_bc)
    axis_x = spectral_build_axis_setup(cfg, spectral_bc.axis_x, 'x');
    axis_y = spectral_build_axis_setup(cfg, spectral_bc.axis_y, 'y');

    [X, Y] = meshgrid(axis_x.nodes, axis_y.nodes);
    [X_physical, Y_physical] = meshgrid(axis_x.physical_nodes, axis_y.physical_nodes);
    lambda2 = axis_y.lambda(:) + axis_x.lambda(:).';
    lambda2_safe = lambda2;
    lambda2_safe(lambda2 == 0) = 1;
    dealias = axis_y.dealias_mask(:) * axis_x.dealias_mask(:).';

    setup = struct();
    setup.Nx = cfg.Nx;
    setup.Ny = cfg.Ny;
    setup.dx = axis_x.spacing;
    setup.dy = axis_y.spacing;
    setup.X = X;
    setup.Y = Y;
    setup.x = axis_x.nodes;
    setup.y = axis_y.nodes;
    setup.X_physical = X_physical;
    setup.Y_physical = Y_physical;
    setup.x_physical = axis_x.physical_nodes;
    setup.y_physical = axis_y.physical_nodes;
    setup.axis_x = axis_x;
    setup.axis_y = axis_y;
    setup.kx = axis_x.modal_values;
    setup.ky = axis_y.modal_values;
    setup.lambda2 = lambda2;
    setup.lambda2_safe = lambda2_safe;
    setup.null_mode_mask = lambda2 == 0;
    setup.project_zero_mean = strcmp(axis_x.family, 'dct') && strcmp(axis_y.family, 'dct');
    setup.dealias = dealias;
    setup.dkx = axis_x.modal_spacing;
    setup.dky = axis_y.modal_spacing;
    setup.required_transform_functions = unique([{axis_x.required_functions{:}}, {axis_y.required_functions{:}}], 'stable');
    setup.spectral_bc = spectral_bc;
    setup.requires_lifting = logical(spectral_bc.requires_lifting);
    setup.lifting_model = char(string(spectral_bc.lifting_model));
    setup.requires_wall_closure = logical(spectral_bc.requires_wall_closure);
    setup.wall_model = char(string(spectral_bc.wall_model));
    setup.sides = spectral_bc.sides;
    setup.supports_shaped_bathymetry = logical(spectral_bc.supports_shaped_bathymetry);
    setup.requires_bathymetry_penalization = logical(spectral_bc.requires_bathymetry_penalization);
    setup.bathymetry_model = char(string(spectral_bc.bathymetry_model));
    setup.bathymetry_payload = spectral_bc.bathymetry_payload;
    setup.lifting = spectral_build_lifting(setup, spectral_bc);
    setup.bathymetry = spectral_build_bathymetry_penalization(cfg, setup, spectral_bc);
end

function omega0 = spectral_initial_vorticity(cfg, X, Y)
    if isfield(cfg, "omega") && ~isempty(cfg.omega)
        omega0 = cfg.omega;
        return;
    end
    omega0 = ICDispatcher.resolve(X, Y, cfg, 'spectral');
end

function rhs_hat = spectral_rhs_hat(omega_hat, setup, nu)
    % Transform-family pseudo-spectral vorticity RHS:
    %   d(omega_hat)/dt = -T(J(psi,omega)) + nu*T(laplacian(omega))
    % where T is the active separable transform family (FFT/DST/DCT).
    omega_hat = spectral_apply_state_contract(omega_hat, setup);
    omega_fluct = spectral_inverse_transform2(omega_hat, setup);
    psi_hat = spectral_streamfunction_hat(omega_hat, setup);
    psi_fluct = spectral_inverse_transform2(psi_hat, setup);

    omega = omega_fluct + setup.lifting.omega;
    psi = psi_fluct + setup.lifting.psi;

    [u, v] = spectral_total_velocity_from_psi(psi, setup);
    dwdx = spectral_derivative_x(omega, setup);
    dwdy = spectral_derivative_y(omega, setup);

    advection = u .* dwdx + v .* dwdy;
    advection = spectral_apply_bathymetry_field_mask(advection, setup);
    adv_hat = spectral_apply_dealias_mask(spectral_forward_transform2(advection, setup), setup);
    diff_hat = -nu * setup.lambda2 .* omega_hat;
    penalty_hat = spectral_bathymetry_penalty_hat(omega, setup);

    rhs_hat = -adv_hat + diff_hat + nu * setup.lifting.delta_omega_hat + penalty_hat;
end

function psi_hat = spectral_streamfunction_hat(omega_hat, setup)
    % Solve Poisson in transform space under the repo's sign convention:
    %   nabla^2 psi = omega, while the separable transform Laplacian is
    %   represented as -lambda2, so psi_hat = -omega_hat / lambda2.
    psi_hat = -omega_hat ./ setup.lambda2_safe;
    psi_hat(setup.null_mode_mask) = 0;
end

function [u, v] = spectral_velocity_from_psi(psi, setup)
    u = -spectral_derivative_y(psi, setup);
    v = spectral_derivative_x(psi, setup);
end

function [u, v] = spectral_total_velocity_from_psi(psi, setup)
    [u, v] = spectral_velocity_from_psi(psi, setup);
    [u, v] = spectral_apply_velocity_wall_policy(u, v, setup);
end

function coeff = spectral_forward_transform2(field, setup)
    coeff = spectral_apply_axis_transform(field, setup.axis_y, 1, 'forward');
    coeff = spectral_apply_axis_transform(coeff, setup.axis_x, 2, 'forward');
end

function field = spectral_inverse_transform2(coeff, setup)
    field = spectral_apply_axis_transform(coeff, setup.axis_x, 2, 'inverse');
    field = spectral_apply_axis_transform(field, setup.axis_y, 1, 'inverse');
    field = real(field);
end

function coeff = spectral_apply_dealias_mask(coeff, setup)
    coeff = coeff .* setup.dealias;
end

function coeff = spectral_project_coefficients(coeff, setup)
    coeff = spectral_apply_dealias_mask(coeff, setup);
    coeff = spectral_enforce_null_mode_constraints(coeff, setup);
end

function coeff = spectral_enforce_null_mode_constraints(coeff, setup)
    if setup.project_zero_mean
        coeff(setup.null_mode_mask) = 0;
    end
end

function coeff = spectral_apply_state_contract(coeff, setup)
    coeff = spectral_project_coefficients(coeff, setup);
    if ~setup.requires_wall_closure
        omega_fluct = spectral_inverse_transform2(coeff, setup);
        omega_fluct = spectral_apply_bathymetry_fluctuation_contract(omega_fluct, setup);
        coeff = spectral_project_coefficients(spectral_forward_transform2(omega_fluct, setup), setup);
        return;
    end

    omega_fluct = spectral_inverse_transform2(coeff, setup);
    psi_fluct = spectral_inverse_transform2(spectral_streamfunction_hat(coeff, setup), setup);
    omega_fluct = spectral_apply_fluctuation_wall_closure(omega_fluct, psi_fluct, setup);
    omega_fluct = spectral_apply_bathymetry_fluctuation_contract(omega_fluct, setup);
    coeff = spectral_project_coefficients(spectral_forward_transform2(omega_fluct, setup), setup);
end

function omega_fluct = spectral_apply_fluctuation_wall_closure(omega_fluct, psi_fluct, setup)
    if ~setup.requires_wall_closure
        return;
    end

    [ny, nx] = size(omega_fluct);
    if ny < 2 || nx < 2
        return;
    end

    dx = setup.dx;
    dy = setup.dy;
    sides = setup.sides;

    if strcmp(sides.bottom.kind, 'wall') && strcmp(sides.bottom.math_type, 'dirichlet')
        omega_fluct(1, 2:max(nx - 1, 2)) = -2.0 * psi_fluct(1, 2:max(nx - 1, 2)) / (dy^2);
    end
    if strcmp(sides.top.kind, 'wall') && strcmp(sides.top.math_type, 'dirichlet')
        omega_fluct(end, 2:max(nx - 1, 2)) = -2.0 * psi_fluct(end, 2:max(nx - 1, 2)) / (dy^2);
    end
    if strcmp(sides.left.kind, 'wall') && strcmp(sides.left.math_type, 'dirichlet')
        omega_fluct(2:max(ny - 1, 2), 1) = -2.0 * psi_fluct(2:max(ny - 1, 2), 1) / (dx^2);
    end
    if strcmp(sides.right.kind, 'wall') && strcmp(sides.right.math_type, 'dirichlet')
        omega_fluct(2:max(ny - 1, 2), end) = -2.0 * psi_fluct(2:max(ny - 1, 2), end) / (dx^2);
    end

    omega_fluct(1, 1) = mean([omega_fluct(1, min(2, nx)), omega_fluct(min(2, ny), 1)]);
    omega_fluct(1, end) = mean([omega_fluct(1, max(nx - 1, 1)), omega_fluct(min(2, ny), end)]);
    omega_fluct(end, 1) = mean([omega_fluct(end, min(2, nx)), omega_fluct(max(ny - 1, 1), 1)]);
    omega_fluct(end, end) = mean([omega_fluct(end, max(nx - 1, 1)), omega_fluct(max(ny - 1, 1), end)]);
end

function [u, v] = spectral_apply_velocity_wall_policy(u, v, setup)
    if ~setup.requires_wall_closure
        return;
    end

    sides = setup.sides;
    lift_u = setup.lifting.u;
    lift_v = setup.lifting.v;

    if strcmp(sides.bottom.kind, 'wall')
        u(1, :) = lift_u(1, :);
        v(1, :) = lift_v(1, :);
    end
    if strcmp(sides.top.kind, 'wall')
        u(end, :) = lift_u(end, :);
        v(end, :) = lift_v(end, :);
    end
    if strcmp(sides.left.kind, 'wall')
        u(:, 1) = lift_u(:, 1);
        v(:, 1) = lift_v(:, 1);
    end
    if strcmp(sides.right.kind, 'wall')
        u(:, end) = lift_u(:, end);
        v(:, end) = lift_v(:, end);
    end

    [u, v] = spectral_apply_bathymetry_velocity_policy(u, v, setup);
end

function bathymetry = spectral_build_bathymetry_penalization(cfg, setup, spectral_bc)
    bathymetry = struct( ...
        'enabled', false, ...
        'model', 'none', ...
        'payload', struct(), ...
        'geometry', struct(), ...
        'solid_mask', false(setup.Ny, setup.Nx), ...
        'wet_mask', true(setup.Ny, setup.Nx), ...
        'integration_mask', ones(setup.Ny, setup.Nx), ...
        'penalty_mask', zeros(setup.Ny, setup.Nx), ...
        'penalization_strength', 0.0);

    if ~isfield(spectral_bc, 'requires_bathymetry_penalization') || ~logical(spectral_bc.requires_bathymetry_penalization)
        return;
    end

    geometry = build_bathymetry_geometry(cfg, setup.X, setup.Y, 'fd');
    bathymetry.enabled = logical(isfield(geometry, 'enabled') && geometry.enabled);
    bathymetry.model = char(string(spectral_bc.bathymetry_model));
    bathymetry.payload = spectral_bc.bathymetry_payload;
    bathymetry.geometry = geometry;
    if bathymetry.enabled
        bathymetry.solid_mask = logical(geometry.solid_mask);
        bathymetry.wet_mask = logical(geometry.wet_mask);
        bathymetry.integration_mask = double(geometry.wet_mask);
        bathymetry.penalty_mask = double(geometry.solid_mask);
        bathymetry.penalization_strength = double(spectral_bc.bathymetry_payload.penalization_strength);
    end
end

function field = spectral_apply_bathymetry_field_mask(field_in, setup)
    field = field_in;
    if ~isfield(setup, 'bathymetry') || ~isstruct(setup.bathymetry) || ~setup.bathymetry.enabled
        return;
    end
    field(~setup.bathymetry.wet_mask) = 0;
end

function omega_fluct = spectral_apply_bathymetry_fluctuation_contract(omega_fluct, setup)
    if ~isfield(setup, 'bathymetry') || ~isstruct(setup.bathymetry) || ~setup.bathymetry.enabled
        return;
    end
    omega_fluct(setup.bathymetry.solid_mask) = 0;
end

function omega_total = spectral_apply_bathymetry_total_contract(omega_total, setup)
    if ~isfield(setup, 'bathymetry') || ~isstruct(setup.bathymetry) || ~setup.bathymetry.enabled
        return;
    end
    omega_total(setup.bathymetry.solid_mask) = 0;
end

function [u, v] = spectral_apply_bathymetry_velocity_policy(u, v, setup)
    if ~isfield(setup, 'bathymetry') || ~isstruct(setup.bathymetry) || ~setup.bathymetry.enabled
        return;
    end
    u(setup.bathymetry.solid_mask) = 0;
    v(setup.bathymetry.solid_mask) = 0;
end

function penalty_hat = spectral_bathymetry_penalty_hat(omega_total, setup)
    penalty_hat = zeros(size(setup.lambda2));
    if ~isfield(setup, 'bathymetry') || ~isstruct(setup.bathymetry) || ~setup.bathymetry.enabled
        return;
    end
    penalty_field = -setup.bathymetry.penalization_strength .* setup.bathymetry.penalty_mask .* omega_total;
    penalty_hat = spectral_apply_dealias_mask(spectral_forward_transform2(penalty_field, setup), setup);
end

function mask = spectral_integration_mask(setup)
    if isfield(setup, 'bathymetry') && isstruct(setup.bathymetry) && setup.bathymetry.enabled
        mask = setup.bathymetry.integration_mask;
    else
        mask = ones(setup.Ny, setup.Nx);
    end
end

function lifting = spectral_build_lifting(setup, spectral_bc)
    lifting = spectral_zero_lifting(setup);
    lifting.model = char(string(spectral_bc.lifting_model));
    lifting.payload = spectral_bc.lifting_payload;
    lifting.boundary_profile = char(string(spectral_bc.lifting_payload.boundary_profile));

    switch lower(char(string(spectral_bc.lifting_model)))
        case {'', 'none'}
            % Zero lifting already initialized.
        case 'couette_y'
            lifting.psi = spectral_build_couette_y_lifted_psi(setup, spectral_bc.lifting_payload);
        case {'cavity_2d', 'enclosed_shear_2d', 'wall_box_2d'}
            lifting.psi = spectral_build_box_wall_lifted_psi(setup, spectral_bc.lifting_payload);
        otherwise
            error('Spectral:UnknownLiftingModel', ...
                'Unknown spectral lifting model "%s".', char(string(spectral_bc.lifting_model)));
    end

    lifting.u = -spectral_derivative_y(lifting.psi, setup);
    lifting.v = spectral_derivative_x(lifting.psi, setup);
    lifting.omega = spectral_laplacian_field(lifting.psi, setup);
    lifting.delta_omega = spectral_laplacian_field(lifting.omega, setup);
    lifting.delta_omega_hat = spectral_project_coefficients(spectral_forward_transform2(lifting.delta_omega, setup), setup);
end

function lifting = spectral_zero_lifting(setup)
    zeros_field = zeros(setup.Ny, setup.Nx);
    lifting = struct( ...
        'model', 'none', ...
        'payload', struct(), ...
        'boundary_profile', 'transform_native', ...
        'psi', zeros_field, ...
        'u', zeros_field, ...
        'v', zeros_field, ...
        'omega', zeros_field, ...
        'delta_omega', zeros_field, ...
        'delta_omega_hat', zeros_field);
end

function psi = spectral_build_couette_y_lifted_psi(setup, payload)
    y = setup.Y;
    Ly = max(eps, double(payload.Ly));
    u_bottom = double(payload.bottom_speed);
    u_top = double(payload.top_speed);
    shear_rate = (u_top - u_bottom) / Ly;
    psi = -(u_bottom .* y + 0.5 .* shear_rate .* (y .^ 2));
end

function psi = spectral_build_box_wall_lifted_psi(setup, payload)
    r = setup.X / max(eps, double(payload.Lx));
    s = setup.Y / max(eps, double(payload.Ly));
    wx = spectral_zero_value_zero_slope_window(r);
    wy = spectral_zero_value_zero_slope_window(s);
    hb = spectral_hermite_left_slope(s);
    ht = spectral_hermite_right_slope(s);
    hl = spectral_hermite_left_slope(r);
    hr = spectral_hermite_right_slope(r);

    u_top = double(payload.top_speed);
    u_bottom = double(payload.bottom_speed);
    v_left = double(payload.left_speed);
    v_right = double(payload.right_speed);

    psi = -double(payload.Ly) .* wx .* (u_bottom .* hb + u_top .* ht) ...
        + double(payload.Lx) .* wy .* (v_left .* hl + v_right .* hr);
end

function field = spectral_laplacian_field(field_in, setup)
    field = spectral_derivative_x(spectral_derivative_x(field_in, setup), setup) + ...
        spectral_derivative_y(spectral_derivative_y(field_in, setup), setup);
    field = real(field);
end

function window = spectral_zero_value_zero_slope_window(q)
    window = 16 .* (q .^ 2) .* ((1 - q) .^ 2);
end

function h = spectral_hermite_left_slope(q)
    h = q .^ 3 - 2 .* q .^ 2 + q;
end

function h = spectral_hermite_right_slope(q)
    h = q .^ 3 - q .^ 2;
end

function State = spectral_state_from_coefficients(omega_hat, psi_hat, setup, t_value, step_value)
    omega_fluct = spectral_inverse_transform2(omega_hat, setup);
    psi_fluct = spectral_inverse_transform2(psi_hat, setup);
    omega_fluct = spectral_apply_bathymetry_fluctuation_contract(omega_fluct, setup);
    omega_total = spectral_apply_bathymetry_total_contract(omega_fluct + setup.lifting.omega, setup);

    State = struct();
    State.omega_hat = omega_hat;
    State.psi_hat = psi_hat;
    State.omega_fluct = omega_fluct;
    State.psi_fluct = psi_fluct;
    State.omega = omega_total;
    State.psi = psi_fluct + setup.lifting.psi;
    State.t = t_value;
    State.step = step_value;
    State.setup = setup;
end

function metadata = spectral_frequency_metadata(setup)
    metadata = struct( ...
        "axis_x_family", setup.axis_x.family, ...
        "axis_y_family", setup.axis_y.family, ...
        "axis_x_label", setup.axis_x.label, ...
        "axis_y_label", setup.axis_y.label, ...
        "axis_x_modes", setup.axis_x.modal_values, ...
        "axis_y_modes", setup.axis_y.modal_values, ...
        "dealiasing_rule", "2/3_transform_family", ...
        "nonlinear_term", "pseudo_spectral_physical_product", ...
        "poisson_solver", "separable_modal_diagonal", ...
        "dkx", setup.dkx, ...
        "dky", setup.dky);
end

function axis = spectral_build_axis_setup(cfg, axis_payload, axis_name)
    axis = axis_payload;
    if strcmp(axis_name, 'x')
        N = cfg.Nx;
        L = cfg.Lx;
        user_supplied_modes = cfg.user_supplied_kx;
        explicit_modes = cfg.kx;
    else
        N = cfg.Ny;
        L = cfg.Ly;
        user_supplied_modes = cfg.user_supplied_ky;
        explicit_modes = cfg.ky;
    end

    axis.N = N;
    axis.L = L;
    axis.d1_matrix = [];

    switch axis.family
        case 'fft'
            if user_supplied_modes
                axis.modal_values = explicit_modes(:).';
            else
                axis.modal_values = spectral_make_wavenumbers(N, L);
            end
            axis.lambda = axis.modal_values.^2;
            axis.nodes = linspace(0, L - (L / max(N, 1)), N);
            axis.spacing = L / max(N, 1);
            kmax = max(abs(axis.modal_values));
            if kmax <= 0
                axis.dealias_mask = true(1, N);
            else
                axis.dealias_mask = abs(axis.modal_values) <= (2 / 3) * kmax;
            end
            axis.physical_nodes = axis.nodes - 0.5 * L;

        case 'dst'
            if user_supplied_modes
                error('Spectral:ExplicitModesUnsupportedForNonPeriodicAxis', ...
                    'Explicit k-vectors are only supported on periodic spectral axes.');
            end
            axis.mode_indices = 1:N;
            axis.modal_values = (pi / L) * axis.mode_indices;
            axis.lambda = axis.modal_values.^2;
            axis.nodes = (1:N) * (L / (N + 1));
            axis.spacing = L / (N + 1);
            basis = sin(axis.nodes(:) * axis.modal_values);
            dbasis = cos(axis.nodes(:) * axis.modal_values) .* axis.modal_values;
            axis.d1_matrix = dbasis / basis;
            axis.dealias_mask = spectral_low_mode_mask(N, false);
            axis.physical_nodes = axis.nodes;

        case 'dct'
            if user_supplied_modes
                error('Spectral:ExplicitModesUnsupportedForNonPeriodicAxis', ...
                    'Explicit k-vectors are only supported on periodic spectral axes.');
            end
            axis.mode_indices = 0:(N - 1);
            axis.modal_values = (pi / L) * axis.mode_indices;
            axis.lambda = axis.modal_values.^2;
            axis.nodes = ((0:(N - 1)) + 0.5) * (L / N);
            axis.spacing = L / max(N, 1);
            basis = cos(axis.nodes(:) * axis.modal_values);
            dbasis = -sin(axis.nodes(:) * axis.modal_values) .* axis.modal_values;
            axis.d1_matrix = dbasis / basis;
            axis.dealias_mask = spectral_low_mode_mask(N, true);
            axis.physical_nodes = axis.nodes;

        otherwise
            error('Spectral:UnknownAxisFamily', ...
                'Unsupported spectral axis family "%s".', axis.family);
    end

    axis.modal_spacing = spectral_min_positive_spacing(axis.modal_values);
end

function mask = spectral_low_mode_mask(N, has_constant_mode)
    mask = false(1, N);
    if has_constant_mode
        keep_count = min(N, 1 + floor(2 * max(N - 1, 0) / 3));
    else
        keep_count = min(N, max(1, floor(2 * N / 3)));
    end
    mask(1:keep_count) = true;
end

function spectral_validate_transform_dependencies(setup)
    required = setup.required_transform_functions;
    if isempty(required)
        return;
    end

    probe = ones(4, 1);
    for i = 1:numel(required)
        fn = required{i};
        if exist(fn, 'file') ~= 2
            error('Spectral:MissingTransformDependency', ...
                'Spectral transform-family BCs require MATLAB function "%s".', fn);
        end
        try
            feval(fn, probe);
        catch ME
            error('Spectral:MissingTransformDependency', ...
                'Spectral transform-family BCs require callable MATLAB function "%s" (%s).', ...
                fn, ME.message);
        end
    end
end

function out = spectral_apply_axis_transform(data, axis, dim, direction)
    family = axis.family;
    switch family
        case 'fft'
            if strcmp(direction, 'forward')
                out = fft(data, [], dim);
            else
                out = ifft(data, [], dim);
            end
        case 'dct'
            out = spectral_apply_real_transform_dim(data, dim, direction, 'dct');
        case 'dst'
            out = spectral_apply_real_transform_dim(data, dim, direction, 'dst');
        otherwise
            error('Spectral:UnknownAxisFamily', ...
                'Unsupported transform family "%s".', family);
    end
end

function out = spectral_apply_real_transform_dim(data, dim, direction, family)
    if dim == 1
        out = spectral_apply_real_transform_columns(data, direction, family);
    elseif dim == 2
        out = spectral_apply_real_transform_columns(data.', direction, family).';
    else
        error('Spectral:InvalidTransformDimension', ...
            'Spectral transform engine supports dimensions 1 and 2 only.');
    end
end

function out = spectral_apply_real_transform_columns(data, direction, family)
    switch family
        case 'dct'
            if strcmp(direction, 'forward')
                out = dct(data, [], 1);
            else
                out = idct(data, [], 1);
            end
        case 'dst'
            if strcmp(direction, 'forward')
                out = dst(data);
            else
                out = idst(data);
            end
        otherwise
            error('Spectral:UnknownRealTransformFamily', ...
                'Unknown real transform family "%s".', family);
    end
end

function dfdx = spectral_derivative_x(field, setup)
    dfdx = spectral_derivative_axis(field, setup.axis_x, 2);
end

function dfdy = spectral_derivative_y(field, setup)
    dfdy = spectral_derivative_axis(field, setup.axis_y, 1);
end

function out = spectral_derivative_axis(field, axis, dim)
    switch axis.family
        case 'fft'
            coeff = spectral_apply_axis_transform(field, axis, dim, 'forward');
            if dim == 1
                coeff = coeff .* (1i * axis.modal_values(:));
            else
                coeff = coeff .* (1i * axis.modal_values(:).');
            end
            out = spectral_apply_axis_transform(coeff, axis, dim, 'inverse');
        otherwise
            out = spectral_apply_operator_dim(field, axis.d1_matrix, dim);
    end
    out = real(out);
end

function out = spectral_apply_operator_dim(field, operator_matrix, dim)
    if dim == 1
        out = operator_matrix * field;
    elseif dim == 2
        out = field * operator_matrix.';
    else
        error('Spectral:InvalidOperatorDimension', ...
            'Spectral derivative operators support dimensions 1 and 2 only.');
    end
end

function [omega_snaps, psi_snaps, kinetic_energy, enstrophy, peak_speed, max_omega, sampled_times, next_idx] = ...
        spectral_store_snapshot(State, omega_snaps, psi_snaps, kinetic_energy, enstrophy, peak_speed, max_omega, sampled_times, idx)
    M = spectral_diagnostics_internal(State);

    omega_snaps(:, :, idx) = State.omega;
    psi_snaps(:, :, idx) = State.psi;
    kinetic_energy(idx) = M.kinetic_energy;
    enstrophy(idx) = M.enstrophy;
    peak_speed(idx) = M.peak_speed;
    max_omega(idx) = M.max_vorticity;
    sampled_times(idx) = State.t;
    next_idx = idx + 1;
end

function analysis = spectral_maybe_merge_unified_metrics(analysis, Parameters)
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

function analysis = spectral_attach_native_velocity_snapshots(analysis, setup, Parameters)
    if ~spectral_should_store_native_velocity_snapshots(Parameters)
        analysis.native_velocity_snapshots_stored = false;
        return;
    end
    if ~isstruct(analysis) || ~isfield(analysis, 'psi_snaps') || isempty(analysis.psi_snaps)
        return;
    end

    psi_cube = double(analysis.psi_snaps);
    if ndims(psi_cube) == 2
        psi_cube = reshape(psi_cube, size(psi_cube, 1), size(psi_cube, 2), 1);
    end
    u_snaps = zeros(size(psi_cube));
    v_snaps = zeros(size(psi_cube));
    peak_speed_history = nan(1, size(psi_cube, 3));
    for idx = 1:size(psi_cube, 3)
        [u_snap, v_snap] = spectral_total_velocity_from_psi(psi_cube(:, :, idx), setup);
        u_snaps(:, :, idx) = double(u_snap);
        v_snaps(:, :, idx) = double(v_snap);
        peak_speed_history(idx) = max(hypot(double(u_snap), double(v_snap)), [], 'all');
    end

    analysis.u_snaps = u_snaps;
    analysis.v_snaps = v_snaps;
    analysis.peak_u = max(abs(u_snaps(:, :, end)), [], 'all');
    analysis.peak_v = max(abs(v_snaps(:, :, end)), [], 'all');
    analysis.peak_speed = max(hypot(u_snaps(:, :, end), v_snaps(:, :, end)), [], 'all');
    analysis.peak_speed_history = peak_speed_history;
    analysis.native_velocity_snapshots_stored = true;
end

function precision = spectral_snapshot_storage_precision(Parameters)
    precision = 'double';
    if ~isstruct(Parameters)
        return;
    end
    if isfield(Parameters, 'snapshot_storage_precision') && ~isempty(Parameters.snapshot_storage_precision)
        requested = lower(strtrim(char(string(Parameters.snapshot_storage_precision))));
        if any(strcmp(requested, {'single', 'double'}))
            precision = requested;
        end
    end
end

function tf = spectral_should_store_native_velocity_snapshots(Parameters)
    tf = true;
    if ~isstruct(Parameters)
        return;
    end
    if isfield(Parameters, 'store_native_velocity_snapshots') && ~isempty(Parameters.store_native_velocity_snapshots)
        tf = logical(Parameters.store_native_velocity_snapshots);
        return;
    end
    if isfield(Parameters, 'store_velocity_snapshot_cubes') && ~isempty(Parameters.store_velocity_snapshot_cubes)
        tf = logical(Parameters.store_velocity_snapshot_cubes);
    end
end

function analysis = spectral_cast_snapshot_cubes(analysis, precision)
    if ~isstruct(analysis) || ~(strcmp(precision, 'single') || strcmp(precision, 'double'))
        return;
    end
    cube_fields = {'omega_snaps', 'psi_snaps', 'u_snaps', 'v_snaps'};
    for i = 1:numel(cube_fields)
        key = cube_fields{i};
        if isfield(analysis, key) && ~isempty(analysis.(key)) && isnumeric(analysis.(key))
            analysis.(key) = cast(analysis.(key), precision);
        end
    end
end

function fig_handle = spectral_summary_figure(analysis)
    show_figs = usejava("desktop") && ~strcmpi(get(0, "DefaultFigureVisible"), "off");
    fig_visibility = "off";
    if show_figs
        fig_visibility = "on";
    end

    fig_handle = figure("Name", "Spectral Analysis Results", "NumberTitle", "off", "Visible", fig_visibility);
    apply_dark_theme_for_figure(fig_handle);

    subplot(1, 2, 1);
    contourf(analysis.omega_snaps(:, :, end), 20);
    colorbar;
    title("Vorticity (final)");
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

function k = spectral_make_wavenumbers(N, L)
    k = (2 * pi / L) * [0:floor(N / 2), -floor((N - 1) / 2):-1];
end

function dk = spectral_min_positive_spacing(k)
    vals = unique(sort(k(:)));
    dv = diff(vals);
    dv = dv(dv > 0);
    if isempty(dv)
        dk = NaN;
    else
        dk = min(dv);
    end
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

function bc = spectral_resolve_bc(cfg)
    grid_meta = struct('Lx', cfg.Lx, 'Ly', cfg.Ly, 'Nx', cfg.Nx, 'Ny', cfg.Ny);
    bc = BCDispatcher.resolve(cfg, 'spectral', grid_meta);
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
