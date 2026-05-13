function varargout = FiniteDifferenceMethod(action, varargin)
% FiniteDifferenceMethod - Finite-difference vorticity-streamfunction runtime.
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
%   - The action-based API is the contract used by mode dispatchers.
%   - The numerical core uses the full Arakawa Jacobian for conservative
%     advection and RK4 for time integration.
%   - This implementation intentionally keeps the module self-contained so
%     direct calls from tests and scripts behave like dispatcher runs.

    narginchk(1, inf);
    action_name = lower(string(action));

    switch action_name
        case "callbacks"
            callbacks = struct();
            callbacks.init = @(cfg, ctx) FiniteDifferenceMethod("init", cfg, ctx);
            callbacks.step = @(State, cfg, ctx) FiniteDifferenceMethod("step", State, cfg, ctx);
            callbacks.diagnostics = @(State, cfg, ctx) FiniteDifferenceMethod("diagnostics", State, cfg, ctx);
            callbacks.finalize_analysis = @(analysis, State, cfg, Parameters, ctx) ...
                FiniteDifferenceMethod("finalize_analysis", analysis, State, cfg, Parameters, ctx);
            callbacks.run = @(Parameters) FiniteDifferenceMethod("run", Parameters);
            varargout{1} = callbacks;

        case "init"
            cfg = varargin{1};
            varargout{1} = fd_init_internal(cfg);

        case "step"
            State = varargin{1};
            cfg = varargin{2};
            varargout{1} = fd_step_internal(State, cfg);

        case "diagnostics"
            State = varargin{1};
            varargout{1} = fd_diagnostics_internal(State);

        case "finalize_analysis"
            analysis = varargin{1};
            State = varargin{2};
            cfg = varargin{3};
            Parameters = varargin{4};
            ctx = struct();
            if numel(varargin) >= 5 && isstruct(varargin{5})
                ctx = varargin{5};
            end
            varargout{1} = fd_finalize_analysis_internal(analysis, State, cfg, Parameters, ctx);

        case "run"
            Parameters = varargin{1};
            [fig_handle, analysis] = fd_run_internal(Parameters);
            varargout{1} = fig_handle;
            varargout{2} = analysis;

        otherwise
            error("FD:InvalidAction", ...
                "Unsupported action '%s'. Valid actions: callbacks, init, step, diagnostics, finalize_analysis, run.", ...
                char(string(action)));
    end
end

function State = fd_init_internal(cfg)
% fd_init_internal - Build initial FD state for dispatcher-driven runs.

    validate_fd_cfg(cfg, "init");
    setup = fd_setup_internal(cfg);

    omega_initial = fd_build_initial_vorticity(cfg, setup.X, setup.Y);
    omega_initial = reshape(omega_initial, setup.Ny, setup.Nx);
    if setup.use_gpu
        omega_initial = gpuArray(omega_initial);
    end
    omega_initial = apply_fd_domain_mask(omega_initial, setup);
    psi_initial = reshape(setup.solve_poisson(omega_initial(:)), setup.Ny, setup.Nx);

    % Project the t=0 state onto the dispatcher wall model so no-slip
    % does not activate abruptly only after the first RK stage.
    if isfield(setup, 'apply_bc') && ~isempty(setup.apply_bc)
        omega_initial = setup.apply_bc(omega_initial, psi_initial, setup, 0.0);
        omega_initial = apply_fd_post_closure_edge_omega_clamp(omega_initial, setup);
        omega_initial = apply_fd_domain_mask(omega_initial, setup);
        psi_initial = reshape(setup.solve_poisson(omega_initial(:)), setup.Ny, setup.Nx);
    end

    State = struct();
    State.omega = omega_initial;
    State.psi = psi_initial;
    State.t = 0.0;
    State.step = 0;
    State.setup = setup;
end

function State = fd_step_internal(State, cfg)
% fd_step_internal - Advance one explicit step using configured integrator.

    validate_fd_cfg(cfg, "step");
    dt = cfg.dt;
    nu = cfg.nu;
    setup = State.setup;
    if ~isfield(cfg, 'time_integrator')
        cfg.time_integrator = 'RK4';
    end
    integrator = normalize_fd_integrator(cfg.time_integrator);

    omega_vector = State.omega(:);

    % Select RHS function based on Arakawa toggle
    if setup.use_arakawa
        rhs_fn = @(w) rhs_fd_arakawa(w, setup, nu);
    else
        rhs_fn = @(w) rhs_fd_simple(w, setup, nu);
    end

    switch integrator
        case "FORWARD_EULER"
            omega_stage = apply_fd_stage_bc(omega_vector, setup, State.t);
            stage1 = rhs_fn(omega_stage);
            omega_vector = omega_vector + dt * stage1;

        case "RK4"
            omega_stage = apply_fd_stage_bc(omega_vector, setup, State.t);
            stage1 = rhs_fn(omega_stage);

            omega_stage = apply_fd_stage_bc(omega_vector + 0.5 * dt * stage1, setup, State.t + 0.5 * dt);
            stage2 = rhs_fn(omega_stage);

            omega_stage = apply_fd_stage_bc(omega_vector + 0.5 * dt * stage2, setup, State.t + 0.5 * dt);
            stage3 = rhs_fn(omega_stage);

            omega_stage = apply_fd_stage_bc(omega_vector + dt * stage3, setup, State.t + dt);
            stage4 = rhs_fn(omega_stage);

            omega_vector = omega_vector + (dt / 6) * (stage1 + 2 * stage2 + 2 * stage3 + stage4);

        otherwise
            error('FD:InvalidIntegrator', ...
                'Unsupported FD integrator "%s".', char(string(cfg.time_integrator)));
    end

    omega_vector = apply_fd_stage_bc(omega_vector, setup, State.t + dt);
    omega_matrix = reshape(omega_vector, setup.Ny, setup.Nx);
    omega_matrix = apply_fd_domain_mask(omega_matrix, setup);

    State.omega = omega_matrix;
    State.psi = reshape(setup.solve_poisson(omega_matrix(:)), setup.Ny, setup.Nx);
    State.t = State.t + dt;
    State.step = State.step + 1;
end

function Metrics = fd_diagnostics_internal(State)
% fd_diagnostics_internal - Compute common diagnostics from current state.

    omega = State.omega;
    psi = State.psi;
    setup = State.setup;

    [velocity_u, velocity_v] = velocity_from_streamfunction(psi, setup);
    fluid_mask = resolve_fd_metric_mask(setup);
    kinetic_energy = 0.5 * sum((velocity_u(:).^2 + velocity_v(:).^2)) * setup.dx * setup.dy;
    enstrophy = 0.5 * sum(double(omega(fluid_mask)).^2) * setup.dx * setup.dy;
    max_vorticity = 0;
    if any(fluid_mask(:))
        max_vorticity = max(abs(double(omega(fluid_mask))));
    end

    Metrics = struct();
    Metrics.max_vorticity = gather_if_gpu(max_vorticity);
    Metrics.enstrophy = gather_if_gpu(enstrophy);
    Metrics.kinetic_energy = gather_if_gpu(kinetic_energy);
    Metrics.peak_speed = gather_if_gpu(max(sqrt(velocity_u(:).^2 + velocity_v(:).^2)));
    Metrics.max_abs_u_plus_v = gather_if_gpu(max(abs(velocity_u(:)) + abs(velocity_v(:))));
    Metrics.t = State.t;
    Metrics.step = State.step;
end

function [fig_handle, analysis] = fd_run_internal(Parameters)
% fd_run_internal - Self-contained batch run path used by tests/scripts.

    run_options = normalize_fd_run_options(Parameters);
    validate_fd_cfg(run_options, "run");

    cfg = fd_cfg_from_parameters(run_options);
    integrator = normalize_fd_integrator(cfg.time_integrator);
    snapshot_times = run_options.snap_times(:).';
    n_snapshots = numel(snapshot_times);
    n_steps = max(0, ceil(cfg.Tfinal / cfg.dt));

    setup_start_cpu = cputime;
    setup_start_wall = tic;
    State = fd_init_internal(cfg);
    setup_cpu_time_s = cputime - setup_start_cpu;
    setup_wall_time_s = toc(setup_start_wall);

    omega_snapshots = zeros(cfg.Ny, cfg.Nx, n_snapshots);
    psi_snapshots = zeros(cfg.Ny, cfg.Nx, n_snapshots);

    snapshot_index = 1;
    while snapshot_index <= n_snapshots && State.t >= snapshot_times(snapshot_index) - 1e-12
        omega_snapshots(:, :, snapshot_index) = State.omega;
        psi_snapshots(:, :, snapshot_index) = State.psi;
        snapshot_index = snapshot_index + 1;
    end

    progress_stride = resolve_progress_stride(run_options, n_steps);
    live_stride = resolve_live_stride(run_options, n_steps);
    live_preview = open_live_preview_if_requested(run_options, cfg, State.omega);

    if n_steps > 0
        scheme_label = 'Arakawa';
        if ~cfg.use_arakawa, scheme_label = 'SparseMatrix'; end
        if ~strcmpi(State.setup.advection_scheme_effective, State.setup.advection_scheme_selected)
            scheme_label = sprintf('%s->%s', State.setup.advection_scheme_selected, State.setup.advection_scheme_effective);
        end
        integrator_label = char(strrep(integrator, '_', ' '));
        gpu_label = '';
        if cfg.use_gpu, gpu_label = ' [GPU]'; end
        bc_label = char(string(State.setup.bc_type));
        fprintf('[FD] Running %d steps (dt=%.3e, Tfinal=%.3f, scheme=%s, integrator=%s, BC=%s%s)\n', ...
            n_steps, cfg.dt, cfg.Tfinal, scheme_label, integrator_label, bc_label, gpu_label);
    end

    solve_start_cpu = cputime;
    solve_start_wall = tic;

    for step_index = 1:n_steps
        State = fd_step_internal(State, cfg);

        while snapshot_index <= n_snapshots && State.t >= snapshot_times(snapshot_index) - 1e-12
            omega_snapshots(:, :, snapshot_index) = State.omega;
            psi_snapshots(:, :, snapshot_index) = State.psi;
            snapshot_index = snapshot_index + 1;
        end

        if mod(step_index, progress_stride) == 0 || step_index == 1 || step_index == n_steps
            Metrics_live = fd_diagnostics_internal(State);
            cfl_estimate = compute_cfl_estimate(Metrics_live.peak_speed, cfg.dt, State.setup);
            fprintf(['[FD] %6.2f%% | step %d/%d | t = %.3f / %.3f | ', ...
                     'max|omega| = %.3e | CFL(est)=%.3f\n'], ...
                100 * step_index / max(1, n_steps), step_index, n_steps, State.t, cfg.Tfinal, ...
                Metrics_live.max_vorticity, cfl_estimate);
        end

        if mod(step_index, live_stride) == 0 || step_index == n_steps
            update_live_preview(live_preview, State.omega, State.t);
        end
    end

    solve_cpu_time_s = cputime - solve_start_cpu;
    solve_wall_time_s = toc(solve_start_wall);
    close_live_preview(live_preview);

    if snapshot_index <= n_snapshots
        % Ensure all requested snapshot slots are populated for downstream tools.
        omega_snapshots(:, :, snapshot_index:end) = repmat(State.omega, 1, 1, n_snapshots - snapshot_index + 1);
        psi_snapshots(:, :, snapshot_index:end) = repmat(State.psi, 1, 1, n_snapshots - snapshot_index + 1);
    end

    % Gather GPU arrays back to CPU for downstream plotting/saving
    omega_snapshots = gather_if_gpu(omega_snapshots);
    psi_snapshots = gather_if_gpu(psi_snapshots);

    analysis = struct();
    stage_count = 4;
    if strcmp(integrator, "FORWARD_EULER")
        stage_count = 1;
    end
    analysis.rhs_calls = stage_count * n_steps;
    analysis.poisson_solves = 1 + (stage_count + 1) * n_steps;
    analysis.poisson_matrix_n = size(State.setup.A, 1);
    analysis.poisson_matrix_nnz = nnz(State.setup.A);
    analysis.setup_wall_time_s = setup_wall_time_s;
    analysis.setup_cpu_time_s = setup_cpu_time_s;
    analysis.solve_wall_time_s = solve_wall_time_s;
    analysis.solve_cpu_time_s = solve_cpu_time_s;
    analysis.wall_time_s = setup_wall_time_s + solve_wall_time_s;
    analysis.cpu_time_s = setup_cpu_time_s + solve_cpu_time_s;
    analysis.snapshot_times_requested = snapshot_times(:);
    analysis.snapshot_times_actual = snapshot_times(:);
    analysis.snapshot_times = snapshot_times(:);
    analysis.time_vec = snapshot_times(:);
    analysis.snapshots_stored = n_snapshots;
    analysis.omega_snaps = omega_snapshots;
    analysis.psi_snaps = psi_snapshots;
    analysis = fd_finalize_analysis_internal(analysis, State, cfg, run_options, struct('mode', 'run'));

    fig_handle = create_fd_summary_figure(analysis, run_options);
    maybe_write_vorticity_animation(analysis, cfg, run_options);
end

function analysis = fd_finalize_analysis_internal(analysis, State, cfg, Parameters, ~)
% fd_finalize_analysis_internal Normalize FD analysis output for run/evolution paths.
    if cfg.use_arakawa
        analysis.method = "finite_difference_arakawa";
    else
        analysis.method = "finite_difference_sparse_matrix";
    end
    analysis.time_integrator = char(strrep(normalize_fd_integrator(cfg.time_integrator), '_', ' '));
    analysis.use_arakawa = cfg.use_arakawa;
    analysis.use_gpu = cfg.use_gpu;
    analysis.nu = cfg.nu;
    analysis.Lx = cfg.Lx;
    analysis.Ly = cfg.Ly;
    analysis.Nx = cfg.Nx;
    analysis.Ny = cfg.Ny;
    analysis.dx = State.setup.dx;
    analysis.dy = State.setup.dy;
    analysis.delta = State.setup.delta;
    analysis.dt = cfg.dt;
    analysis.Tfinal = cfg.Tfinal;
    if ~isfield(analysis, 'Nt') || isempty(analysis.Nt)
        analysis.Nt = max(0, round(cfg.Tfinal / cfg.dt));
    end
    if ~isfield(analysis, 'grid_points') || isempty(analysis.grid_points)
        analysis.grid_points = cfg.Nx * cfg.Ny;
    end
    if ~isfield(analysis, 'unknowns') || isempty(analysis.unknowns)
        analysis.unknowns = analysis.grid_points;
    end
    if ~isfield(analysis, 'poisson_matrix_n') || isempty(analysis.poisson_matrix_n)
        analysis.poisson_matrix_n = size(State.setup.A, 1);
    end
    if ~isfield(analysis, 'poisson_matrix_nnz') || isempty(analysis.poisson_matrix_nnz)
        analysis.poisson_matrix_nnz = nnz(State.setup.A);
    end

    analysis = maybe_merge_unified_metrics(analysis, Parameters);
    analysis.wall_model = char(string(State.setup.wall_model));
    analysis.poisson_wall_model = char(string(State.setup.wall_model));
    analysis.fd_operator_mode = char(string(State.setup.fd_operator_mode));
    analysis.fd_periodic_x = logical(State.setup.periodic_x);
    analysis.fd_periodic_y = logical(State.setup.periodic_y);
    analysis.poisson_bc_mode = char(string(State.setup.poisson_bc_mode));
    analysis.advection_scheme_selected = char(string(State.setup.advection_scheme_selected));
    analysis.advection_scheme_effective = char(string(State.setup.advection_scheme_effective));
    if isfield(State.setup, 'poisson_meta') && isstruct(State.setup.poisson_meta)
        analysis.poisson_nullspace_handling = char(string(local_pick_struct_text(State.setup.poisson_meta, 'nullspace_handling', 'none')));
        analysis.poisson_rhs_projection = char(string(local_pick_struct_text(State.setup.poisson_meta, 'rhs_projection', 'none')));
    else
        analysis.poisson_nullspace_handling = 'none';
        analysis.poisson_rhs_projection = 'none';
    end
    if strcmpi(analysis.poisson_rhs_projection, 'subtract_mean')
        analysis.poisson_rhs_mean_removed_final = mean(double(State.omega(:)));
    end
    analysis = append_bathymetry_analysis_metadata(analysis, State.setup);
    analysis = append_fd_wall_analysis_metadata(analysis, State.setup);
    analysis = append_snapshot_metrics(analysis, State.setup);
    analysis = MethodConfigBuilder.apply_analysis_contract(analysis, cfg, Parameters);
    analysis.peak_vorticity = analysis.peak_abs_omega;
end

function run_options = normalize_fd_run_options(Parameters)
% normalize_fd_run_options - Validate and normalize run flags from UI inputs.

    run_options = Parameters;
    required_fields = { ...
        'snap_times', 'mode', 'progress_stride', 'live_preview', 'live_stride', ...
        'create_animations', 'animation_fps', 'animation_format', ...
        'animation_dir', 'animation_quality'};
    require_struct_fields(run_options, required_fields, 'run');

    if ~isnumeric(run_options.snap_times) || isempty(run_options.snap_times)
        error('FD:InvalidSnapTimes', ...
            'run.snap_times must be a non-empty numeric vector from the UI.');
    end
    if any(~isfinite(run_options.snap_times(:)))
        error('FD:InvalidSnapTimes', ...
            'run.snap_times contains non-finite values.');
    end
    if isempty(string(run_options.mode))
        error('FD:InvalidMode', ...
            'run.mode must be provided by the UI and cannot be empty.');
    end
    if ~isnumeric(run_options.progress_stride) || ~isscalar(run_options.progress_stride) || ~isfinite(run_options.progress_stride)
        error('FD:InvalidProgressStride', ...
            'run.progress_stride must be a finite numeric scalar.');
    end
    if ~isnumeric(run_options.live_stride) || ~isscalar(run_options.live_stride) || ~isfinite(run_options.live_stride)
        error('FD:InvalidLiveStride', ...
            'run.live_stride must be a finite numeric scalar.');
    end
    if ~isnumeric(run_options.animation_fps) || ~isscalar(run_options.animation_fps) || ~isfinite(run_options.animation_fps)
        error('FD:InvalidAnimationFPS', ...
            'run.animation_fps must be a finite numeric scalar.');
    end
    if isempty(char(string(run_options.animation_format)))
        error('FD:InvalidAnimationFormat', ...
            'run.animation_format must be provided by the UI.');
    end
    if isempty(char(string(run_options.animation_dir)))
        error('FD:InvalidAnimationDir', ...
            'run.animation_dir must be provided by the UI.');
    end
    if ~isnumeric(run_options.animation_quality) || ~isscalar(run_options.animation_quality) || ~isfinite(run_options.animation_quality)
        error('FD:InvalidAnimationQuality', ...
            'run.animation_quality must be a finite numeric scalar.');
    end

    run_options.live_preview = logical(run_options.live_preview);
    run_options.create_animations = logical(run_options.create_animations);
end

function validate_fd_cfg(cfg, caller_name)
% validate_fd_cfg - Guard required fields for init/step/run paths.

    core_required = {'nu', 'Lx', 'Ly', 'Nx', 'Ny', 'dt', 'Tfinal', ...
        'ic_type', 'delta', 'use_arakawa', 'use_gpu', ...
        'grid_mode', 'dx', 'dy', 'is_anisotropic'};
    bc_required = {'bc_case', 'bc_top', 'bc_bottom', 'bc_left', 'bc_right', ...
        'bc_top_physical', 'bc_bottom_physical', 'bc_left_physical', 'bc_right_physical', ...
        'bc_top_math', 'bc_bottom_math', 'bc_left_math', 'bc_right_math', ...
        'U_top', 'U_bottom', 'U_left', 'U_right'};
    require_struct_fields(cfg, [core_required, bc_required], caller_name);

    has_omega = isfield(cfg, 'omega') && ~isempty(cfg.omega);
    has_ic_coeff = isfield(cfg, 'ic_coeff') && ~isempty(cfg.ic_coeff);
    if ~(has_omega || has_ic_coeff)
        if ~fd_ic_type_allows_empty_coeff(cfg.ic_type)
            error('FD:MissingInitialCondition', ...
                'Missing initial-condition payload for %s: provide omega or ic_coeff.', caller_name);
        end
    end

    if cfg.Nx <= 0 || cfg.Ny <= 0 || cfg.dt <= 0 || cfg.Tfinal <= 0 || cfg.Lx <= 0 || cfg.Ly <= 0
        error('FD:InvalidConfig', ...
            'Nx, Ny, dt, Tfinal, Lx, and Ly must all be positive in %s path.', caller_name);
    end
    if ~isfinite(cfg.delta) || cfg.delta <= 0
        error('FD:InvalidDelta', ...
            'delta must be a finite positive scalar in %s path.', caller_name);
    end
    if isfield(cfg, 'time_integrator')
        normalize_fd_integrator(cfg.time_integrator);
    end
end

function integrator = normalize_fd_integrator(raw_value)
    integrator = upper(char(string(raw_value)));
    integrator = strrep(integrator, '-', '_');
    integrator = strrep(integrator, ' ', '_');
    if strcmp(integrator, 'EULER')
        integrator = 'FORWARD_EULER';
    end
    if ~any(strcmp(integrator, {'RK4', 'FORWARD_EULER'}))
        error('FD:InvalidIntegrator', ...
            'FD supports RK4 or Forward Euler (received "%s").', char(string(raw_value)));
    end
end

function tf = fd_ic_type_allows_empty_coeff(ic_type_raw)
    ic_type = lower(strtrim(char(string(ic_type_raw))));
    ic_type = strrep(ic_type, '-', '_');
    ic_type = strrep(ic_type, ' ', '_');
    tf = any(strcmp(ic_type, {'placeholder2', 'kutz', 'no_initial_condition'}));
end

function cfg = fd_cfg_from_parameters(Parameters)
% fd_cfg_from_parameters - Build canonical cfg used by init/step functions.
    cfg = MethodConfigBuilder.build(Parameters, "fd", "fd.run");
end

function setup = fd_setup_internal(cfg)
% fd_setup_internal - Precompute finite-difference operators and solvers.

    Nx = cfg.Nx;
    Ny = cfg.Ny;
    Lx = cfg.Lx;
    Ly = cfg.Ly;

    dx = cfg.dx;
    dy = cfg.dy;
    delta = cfg.delta;
    if ~isfinite(delta) || delta <= 0
        error('FD:InvalidDelta', ...
            'delta must be a finite positive scalar.');
    end

    x = linspace(-Lx/2, Lx/2 - dx, Nx);
    y = linspace(-Ly/2, Ly/2 - dy, Ny);
    [X, Y] = meshgrid(x, y);

    use_arakawa = logical(cfg.use_arakawa);
    use_gpu = logical(cfg.use_gpu);

    bc = resolve_bc_dispatch(cfg, X, Y, dx, dy);
    if ~bc.capability.supported
        error('FD:UnsupportedBoundaryConfiguration', ...
            'FD received unsupported BC configuration: %s', bc.capability.reason);
    end

    bathymetry_geometry = resolve_fd_bathymetry_geometry(cfg, X, Y);
    poisson_mode = bc.method.fd.poisson_bc_mode;
    if bathymetry_geometry.enabled
        poisson_mode = 'bathymetry_dirichlet_psi';
    end
    if use_gpu && ~strcmp(poisson_mode, 'periodic')
        error('FD:GPUWallBCUnsupported', ...
            'GPU + wall Dirichlet streamfunction BC mode is not enabled in this checkpoint. Set use_gpu=false.');
    end

    [A, solve_poisson, poisson_meta] = fd_build_poisson_solver( ...
        Nx, Ny, dx, dy, poisson_mode, bc.method.fd.psi_boundary, use_gpu, bathymetry_geometry);
    [Bx, Cy, L_op, operator_meta] = fd_build_rectangular_operators(Nx, Ny, dx, dy, bc.method.fd.operator_mode);

    % GPU acceleration: convert grid arrays after BC dispatch so spacing
    % metadata remains CPU-side for dispatcher hooks.
    if use_gpu
        X = gpuArray(X);
        Y = gpuArray(Y);
    end

    setup = struct();
    setup.Nx = Nx;
    setup.Ny = Ny;
    setup.Lx = Lx;
    setup.Ly = Ly;
    setup.dx = dx;
    setup.dy = dy;
    setup.delta = delta;
    setup.X = X;
    setup.Y = Y;
    setup.A = A;
    setup.Bx = Bx;
    setup.Cy = Cy;
    setup.L = L_op;
    setup.solve_poisson = solve_poisson;
    if bc.method.fd.periodic_x
        setup.shift_xp = @(F) circshift(F, [0, +1]);
        setup.shift_xm = @(F) circshift(F, [0, -1]);
    else
        setup.shift_xp = @shift_xp_nonperiodic;
        setup.shift_xm = @shift_xm_nonperiodic;
    end
    if bc.method.fd.periodic_y
        setup.shift_yp = @(F) circshift(F, [+1, 0]);
        setup.shift_ym = @(F) circshift(F, [-1, 0]);
    else
        setup.shift_yp = @shift_yp_nonperiodic;
        setup.shift_ym = @shift_ym_nonperiodic;
    end
    setup.use_arakawa = use_arakawa;
    setup.use_gpu = use_gpu;
    setup.bc = bc;
    setup.apply_bc = bc.method.fd.apply_wall_omega;
    setup.enforce_velocity_bc = bc.method.fd.enforce_velocity_bc;
    setup.bc_type  = bc.common.case_name;
    setup.wall_model = bc.method.fd.wall_model;
    setup.fd_post_closure_edge_omega_zero = isfield(cfg, 'fd_post_closure_edge_omega_zero') && ...
        logical(cfg.fd_post_closure_edge_omega_zero);
    setup.poisson_bc_mode = poisson_mode;
    setup.poisson_meta = poisson_meta;
    setup.is_periodic_bc = logical(bc.method.fd.is_periodic);
    setup.periodic_x = logical(bc.method.fd.periodic_x);
    setup.periodic_y = logical(bc.method.fd.periodic_y);
    setup.fd_operator_mode = char(string(operator_meta.operator_mode));
    setup.advection_scheme_selected = 'SparseMatrix';
    if setup.use_arakawa
        setup.advection_scheme_selected = 'Arakawa';
    end
    setup.advection_scheme_effective = setup.advection_scheme_selected;
    if bathymetry_geometry.enabled && ~setup.use_arakawa
        setup.advection_scheme_effective = 'MaskedCentralLegacy';
        warning('FD:BathymetrySparseMatrixLegacyPath', ...
            ['FD bathymetry runs retain the legacy masked pointwise non-Arakawa RHS. ' ...
             'Rectangular sparse-matrix Bx/Cy advection is only active on flat rectangular domains.']);
    end
    setup.bathymetry_geometry = bathymetry_geometry;
    [setup.fd_fluid_mask, setup.fd_wall_mask, setup.fd_solid_mask] = ...
        resolve_fd_domain_role_masks(bc.common, bathymetry_geometry, Ny, Nx);
    if bathymetry_geometry.enabled
        setup.grid_points = nnz(bathymetry_geometry.fluid_mask);
        setup.fixed_omega_mask = bathymetry_geometry.boundary_mask;
    else
        setup.grid_points = Nx * Ny;
        setup.fixed_omega_mask = setup.fd_wall_mask;
    end
end

function [A, solve_poisson, poisson_meta] = fd_build_poisson_solver(Nx, Ny, dx, dy, poisson_mode, psi_boundary, use_gpu, bathymetry_geometry)
% fd_build_poisson_solver - Build periodic or wall-compatible Poisson solver.
    if nargin < 8 || ~isstruct(bathymetry_geometry)
        bathymetry_geometry = struct('enabled', false);
    end
    switch lower(string(poisson_mode))
        case "periodic"
            ex = ones(Nx, 1);
            ey = ones(Ny, 1);
            Tx = spdiags([ex, -2 * ex, ex], [-1, 0, 1], Nx, Nx);
            Ty = spdiags([ey, -2 * ey, ey], [-1, 0, 1], Ny, Ny);
            Tx(1, end) = 1;
            Tx(end, 1) = 1;
            Ty(1, end) = 1;
            Ty(end, 1) = 1;

            Ix = speye(Nx);
            Iy = speye(Ny);
            A = (1 / dx^2) * kron(Tx, Iy) + (1 / dy^2) * kron(Ix, Ty);
            gauge_fixed_A = A;
            gauge_fixed_A(1, :) = 0;
            gauge_fixed_A(1, 1) = 1;

            if use_gpu
                if ~(exist('gpuDevice', 'file') == 2 || exist('gpuDevice', 'builtin') > 0)
                    error('FD:NoGPU', ...
                        'GPU requested but Parallel Computing Toolbox is not installed. Install PCT or set use_gpu=false.');
                end
                gpu_info = gpuDevice;
                if ~gpu_info.DeviceAvailable
                    error('FD:GPUUnavailable', ...
                        'GPU device not available (driver issue?). Check GPU status or set use_gpu=false.');
                end
            end

            poisson_solver = decomposition(gauge_fixed_A, "lu");
            if use_gpu
                solve_poisson = @(omega_vector) fd_solve_poisson_periodic_gpu_bridge( ...
                    poisson_solver, omega_vector);
            else
                solve_poisson = @(omega_vector) fd_solve_poisson_periodic(poisson_solver, omega_vector);
            end
            poisson_meta = struct( ...
                'operator_mode', 'periodic_periodic', ...
                'nullspace_handling', 'zero_mean_rhs_plus_pinned_gauge', ...
                'gauge_index', 1, ...
                'rhs_projection', 'subtract_mean');

        case "periodic_x_dirichlet_y"
            if Ny < 3
                error('FD:GridTooSmallForWallBC', ...
                    'Periodic-x / Dirichlet-y streamfunction BC mode requires Ny >= 3.');
            end
            Nyi = Ny - 2;
            ex = ones(Nx, 1);
            ey = ones(Nyi, 1);
            Tx = spdiags([ex, -2 * ex, ex], [-1, 0, 1], Nx, Nx);
            Tx(1, end) = 1;
            Tx(end, 1) = 1;
            Ty = spdiags([ey, -2 * ey, ey], [-1, 0, 1], Nyi, Nyi);
            Ix = speye(Nx);
            Iy = speye(Nyi);
            A = (1 / dx^2) * kron(Tx, Iy) + (1 / dy^2) * kron(Ix, Ty);
            poisson_solver = decomposition(A, "lu");
            solve_poisson = @(omega_vector) fd_solve_poisson_periodic_x_dirichlet_y( ...
                omega_vector, Nx, Ny, dy, poisson_solver, psi_boundary);
            poisson_meta = struct( ...
                'operator_mode', 'periodic_x_dirichlet_y', ...
                'nullspace_handling', 'none', ...
                'gauge_index', NaN, ...
                'rhs_projection', 'none');

        case "dirichlet_x_periodic_y"
            if Nx < 3
                error('FD:GridTooSmallForWallBC', ...
                    'Dirichlet-x / periodic-y streamfunction BC mode requires Nx >= 3.');
            end
            Nxi = Nx - 2;
            ex = ones(Nxi, 1);
            ey = ones(Ny, 1);
            Tx = spdiags([ex, -2 * ex, ex], [-1, 0, 1], Nxi, Nxi);
            Ty = spdiags([ey, -2 * ey, ey], [-1, 0, 1], Ny, Ny);
            Ty(1, end) = 1;
            Ty(end, 1) = 1;
            Ix = speye(Nxi);
            Iy = speye(Ny);
            A = (1 / dx^2) * kron(Tx, Iy) + (1 / dy^2) * kron(Ix, Ty);
            poisson_solver = decomposition(A, "lu");
            solve_poisson = @(omega_vector) fd_solve_poisson_dirichlet_x_periodic_y( ...
                omega_vector, Nx, Ny, dx, poisson_solver, psi_boundary);
            poisson_meta = struct( ...
                'operator_mode', 'dirichlet_x_periodic_y', ...
                'nullspace_handling', 'none', ...
                'gauge_index', NaN, ...
                'rhs_projection', 'none');

        case "wall_dirichlet_psi"
            if Nx < 3 || Ny < 3
                error('FD:GridTooSmallForWallBC', ...
                    'Wall streamfunction BC mode requires Nx, Ny >= 3.');
            end
            Nxi = Nx - 2;
            Nyi = Ny - 2;

            ex = ones(Nxi, 1);
            ey = ones(Nyi, 1);
            Tx = spdiags([ex, -2 * ex, ex], [-1, 0, 1], Nxi, Nxi);
            Ty = spdiags([ey, -2 * ey, ey], [-1, 0, 1], Nyi, Nyi);
            Ix = speye(Nxi);
            Iy = speye(Nyi);
            A = (1 / dx^2) * kron(Tx, Iy) + (1 / dy^2) * kron(Ix, Ty);

            poisson_solver = decomposition(A, "lu");
            solve_poisson = @(omega_vector) fd_solve_poisson_dirichlet( ...
                omega_vector, Nx, Ny, dx, dy, poisson_solver, psi_boundary);
            poisson_meta = struct( ...
                'operator_mode', 'dirichlet_dirichlet', ...
                'nullspace_handling', 'none', ...
                'gauge_index', NaN, ...
                'rhs_projection', 'none');

        case "bathymetry_dirichlet_psi"
            [A, poisson_solver, masked_meta] = fd_build_masked_poisson_solver( ...
                Nx, Ny, dx, dy, psi_boundary, bathymetry_geometry);
            solve_poisson = @(omega_vector) fd_solve_poisson_masked( ...
                omega_vector, Nx, Ny, poisson_solver, masked_meta);
            poisson_meta = struct( ...
                'operator_mode', 'bathymetry_dirichlet_psi', ...
                'nullspace_handling', 'none', ...
                'gauge_index', NaN, ...
                'rhs_projection', 'none');

        otherwise
            error('FD:UnknownPoissonBCMode', ...
                'Unsupported Poisson BC mode: %s', char(string(poisson_mode)));
    end
end

function psi_vector = fd_solve_poisson_dirichlet(omega_vector, Nx, Ny, dx, dy, poisson_solver, psi_boundary)
% fd_solve_poisson_dirichlet - Solve Poisson with Dirichlet streamfunction walls.
    omega = reshape(omega_vector, Ny, Nx);

    psi = zeros(Ny, Nx, 'like', omega);
    psi(1, :) = psi_boundary.bottom;
    psi(end, :) = psi_boundary.top;
    psi(:, 1) = psi_boundary.left;
    psi(:, end) = psi_boundary.right;

    rhs = omega(2:end-1, 2:end-1);
    rhs(1, :) = rhs(1, :) - psi(1, 2:end-1) / dy^2;
    rhs(end, :) = rhs(end, :) - psi(end, 2:end-1) / dy^2;
    rhs(:, 1) = rhs(:, 1) - psi(2:end-1, 1) / dx^2;
    rhs(:, end) = rhs(:, end) - psi(2:end-1, end) / dx^2;

    psi_inner = poisson_solver \ rhs(:);
    psi(2:end-1, 2:end-1) = reshape(psi_inner, Ny - 2, Nx - 2);
    psi_vector = psi(:);
end

function psi_vector = fd_solve_poisson_periodic(poisson_solver, omega_vector)
% fd_solve_poisson_periodic Solve periodic Poisson with explicit gauge handling.
    rhs = omega_vector(:);
    rhs = rhs - mean(rhs);
    rhs(1) = 0;
    psi_vector = poisson_solver \ rhs;
    psi_vector(1) = 0;
end

function psi_vector = fd_solve_poisson_periodic_x_dirichlet_y(omega_vector, Nx, Ny, dy, poisson_solver, psi_boundary)
% fd_solve_poisson_periodic_x_dirichlet_y Solve mixed periodic-x / Dirichlet-y Poisson system.
    omega = reshape(omega_vector, Ny, Nx);

    psi = zeros(Ny, Nx, 'like', omega);
    psi(1, :) = psi_boundary.bottom;
    psi(end, :) = psi_boundary.top;

    rhs = omega(2:end-1, :);
    rhs(1, :) = rhs(1, :) - psi_boundary.bottom / dy^2;
    rhs(end, :) = rhs(end, :) - psi_boundary.top / dy^2;

    psi_inner = poisson_solver \ rhs(:);
    psi(2:end-1, :) = reshape(psi_inner, Ny - 2, Nx);
    psi_vector = psi(:);
end

function psi_vector = fd_solve_poisson_dirichlet_x_periodic_y(omega_vector, Nx, Ny, dx, poisson_solver, psi_boundary)
% fd_solve_poisson_dirichlet_x_periodic_y Solve mixed Dirichlet-x / periodic-y Poisson system.
    omega = reshape(omega_vector, Ny, Nx);

    psi = zeros(Ny, Nx, 'like', omega);
    psi(:, 1) = psi_boundary.left;
    psi(:, end) = psi_boundary.right;

    rhs = omega(:, 2:end-1);
    rhs(:, 1) = rhs(:, 1) - psi_boundary.left / dx^2;
    rhs(:, end) = rhs(:, end) - psi_boundary.right / dx^2;

    psi_inner = poisson_solver \ rhs(:);
    psi(:, 2:end-1) = reshape(psi_inner, Ny, Nx - 2);
    psi_vector = psi(:);
end

function psi_vector = fd_solve_poisson_periodic_gpu_bridge(poisson_solver, omega_vector)
% fd_solve_poisson_periodic_gpu_bridge Solve periodic Poisson with CPU LU for GPU state vectors.
    omega_cpu = gather_if_gpu(omega_vector);
    psi_cpu = fd_solve_poisson_periodic(poisson_solver, omega_cpu);
    if isa(omega_vector, 'gpuArray')
        psi_vector = gpuArray(psi_cpu);
    else
        psi_vector = psi_cpu;
    end
end

function [Bx, Cy, L_op, meta] = fd_build_rectangular_operators(Nx, Ny, dx, dy, operator_mode)
% fd_build_rectangular_operators Build full-grid sparse derivative operators.
    operator_mode = lower(char(string(operator_mode)));
    switch operator_mode
        case 'periodic_periodic'
            periodic_x = true;
            periodic_y = true;
        case 'periodic_x_dirichlet_y'
            periodic_x = true;
            periodic_y = false;
        case 'dirichlet_x_periodic_y'
            periodic_x = false;
            periodic_y = true;
        otherwise
            periodic_x = false;
            periodic_y = false;
            operator_mode = 'dirichlet_dirichlet';
    end

    D1x = fd_build_first_derivative_1d(Nx, dx, periodic_x);
    D1y = fd_build_first_derivative_1d(Ny, dy, periodic_y);
    D2x = fd_build_second_derivative_1d(Nx, dx, periodic_x);
    D2y = fd_build_second_derivative_1d(Ny, dy, periodic_y);

    Ix = speye(Nx);
    Iy = speye(Ny);
    Bx = kron(D1x, Iy);
    Cy = kron(Ix, D1y);
    L_op = kron(D2x, Iy) + kron(Ix, D2y);
    meta = struct('operator_mode', operator_mode, 'periodic_x', periodic_x, 'periodic_y', periodic_y);
end

function D1 = fd_build_first_derivative_1d(n, spacing, periodic_axis)
% fd_build_first_derivative_1d Build sparse first-derivative matrix on the full grid.
    e = ones(n, 1);
    D1 = spdiags([-e, e], [-1, 1], n, n) / (2 * spacing);
    if periodic_axis
        D1(1, end) = -1 / (2 * spacing);
        D1(end, 1) = 1 / (2 * spacing);
    else
        D1(1, :) = 0;
        D1(end, :) = 0;
    end
end

function D2 = fd_build_second_derivative_1d(n, spacing, periodic_axis)
% fd_build_second_derivative_1d Build sparse second-derivative matrix on the full grid.
    e = ones(n, 1);
    D2 = spdiags([e, -2 * e, e], [-1, 0, 1], n, n) / (spacing^2);
    if periodic_axis
        D2(1, end) = 1 / (spacing^2);
        D2(end, 1) = 1 / (spacing^2);
    else
        D2(1, :) = 0;
        D2(end, :) = 0;
    end
end

function [A, poisson_solver, meta] = fd_build_masked_poisson_solver(Nx, Ny, dx, dy, psi_boundary, geometry)
% fd_build_masked_poisson_solver Build sparse Poisson system over wet interior cells.
    if ~isfield(geometry, 'enabled') || ~geometry.enabled
        error('FD:MissingBathymetryGeometry', ...
            'Bathymetry Poisson mode requires an enabled bathymetry geometry contract.');
    end

    boundary_values = zeros(Ny, Nx);
    boundary_values(end, :) = psi_boundary.top;
    boundary_values(:, 1) = psi_boundary.left;
    boundary_values(:, end) = psi_boundary.right;
    boundary_values(geometry.bottom_boundary_mask) = psi_boundary.bottom;

    unknown_mask = logical(geometry.interior_mask);
    if ~any(unknown_mask(:))
        error('FD:EmptyWetInterior', ...
            'Bathymetry geometry produced no active wet interior cells.');
    end

    index_map = zeros(Ny, Nx);
    index_map(unknown_mask) = 1:nnz(unknown_mask);
    rows = [];
    cols = [];
    vals = [];
    rhs_offset = zeros(nnz(unknown_mask), 1);

    [rr, cc] = find(unknown_mask);
    for n = 1:numel(rr)
        row = rr(n);
        col = cc(n);
        idx = index_map(row, col);
        diag_val = -2 / dx^2 - 2 / dy^2;

        [rows, cols, vals, rhs_offset] = add_masked_neighbor( ...
            rows, cols, vals, rhs_offset, idx, row, col - 1, dx, dy, index_map, unknown_mask, boundary_values, 'x');
        [rows, cols, vals, rhs_offset] = add_masked_neighbor( ...
            rows, cols, vals, rhs_offset, idx, row, col + 1, dx, dy, index_map, unknown_mask, boundary_values, 'x');
        [rows, cols, vals, rhs_offset] = add_masked_neighbor( ...
            rows, cols, vals, rhs_offset, idx, row - 1, col, dx, dy, index_map, unknown_mask, boundary_values, 'y');
        [rows, cols, vals, rhs_offset] = add_masked_neighbor( ...
            rows, cols, vals, rhs_offset, idx, row + 1, col, dx, dy, index_map, unknown_mask, boundary_values, 'y');

        rows(end + 1, 1) = idx; %#ok<AGROW>
        cols(end + 1, 1) = idx; %#ok<AGROW>
        vals(end + 1, 1) = diag_val; %#ok<AGROW>
    end

    A = sparse(rows, cols, vals, nnz(unknown_mask), nnz(unknown_mask));
    poisson_solver = decomposition(A, "lu");

    meta = struct();
    meta.unknown_mask = unknown_mask;
    meta.boundary_values = boundary_values;
    meta.rhs_offset = rhs_offset;
    meta.solid_mask = logical(geometry.solid_mask);
end

function [rows, cols, vals, rhs_offset] = add_masked_neighbor(rows, cols, vals, rhs_offset, idx, row, col, dx, dy, index_map, unknown_mask, boundary_values, axis_id)
    if strcmp(axis_id, 'x')
        coeff = 1 / dx^2;
    else
        coeff = 1 / dy^2;
    end

    if row >= 1 && row <= size(index_map, 1) && col >= 1 && col <= size(index_map, 2)
        if unknown_mask(row, col)
            rows(end + 1, 1) = idx; %#ok<AGROW>
            cols(end + 1, 1) = index_map(row, col); %#ok<AGROW>
            vals(end + 1, 1) = coeff; %#ok<AGROW>
        else
            rhs_offset(idx) = rhs_offset(idx) - coeff * boundary_values(row, col);
        end
    end
end

function psi_vector = fd_solve_poisson_masked(omega_vector, Nx, Ny, poisson_solver, meta)
% fd_solve_poisson_masked Solve Poisson on a bathymetry-shaped wet interior.
    omega = reshape(omega_vector, Ny, Nx);
    psi = meta.boundary_values;
    rhs = omega(meta.unknown_mask) + meta.rhs_offset;
    psi(meta.unknown_mask) = poisson_solver \ rhs;
    psi(meta.solid_mask) = 0;
    psi_vector = psi(:);
end

function omega_vector = apply_fd_stage_bc(omega_vector, setup, time_value)
% apply_fd_stage_bc - Enforce dispatcher-provided BC at RK stages.
    omega_matrix = reshape(omega_vector, setup.Ny, setup.Nx);
    omega_matrix = apply_fd_domain_mask(omega_matrix, setup);
    if ~isfield(setup, 'apply_bc') || isempty(setup.apply_bc)
        error('FD:MissingBoundaryHook', ...
            'setup.apply_bc is missing; boundary conditions must be provided by BCDispatcher.');
    end
    psi_matrix = reshape(setup.solve_poisson(omega_matrix(:)), setup.Ny, setup.Nx);
    omega_matrix = setup.apply_bc(omega_matrix, psi_matrix, setup, time_value);
    omega_matrix = apply_fd_post_closure_edge_omega_clamp(omega_matrix, setup);
    omega_matrix = apply_fd_domain_mask(omega_matrix, setup);
    omega_vector = omega_matrix(:);
end

function omega_initial = fd_build_initial_vorticity(cfg, X, Y)
% fd_build_initial_vorticity - Resolve initial vorticity via ICDispatcher.
%
% Priority:
%   1. cfg.omega     - caller-supplied field (e.g., hot-start / continuation)
%   2. ICDispatcher  - translates cfg.ic_type / cfg.ic_coeff -> omega
%   3. Hard failure  - dispatcher must be on the path

    if isfield(cfg, "omega") && ~isempty(cfg.omega)
        omega_initial = cfg.omega;
        return;
    end

    omega_initial = resolve_ic_dispatch(X, Y, cfg);
end

function rhs_vector = rhs_fd_arakawa(omega_vector, setup, nu)
% rhs_fd_arakawa - Conservative Arakawa Jacobian + diffusion RHS.

    Nx = setup.Nx;
    Ny = setup.Ny;
    dx = setup.dx;
    dy = setup.dy;

    omega_matrix = reshape(omega_vector, Ny, Nx);
    psi_matrix = reshape(setup.solve_poisson(omega_vector), Ny, Nx);

    shift_xp = setup.shift_xp;
    shift_xm = setup.shift_xm;
    shift_yp = setup.shift_yp;
    shift_ym = setup.shift_ym;

    psi_ip = shift_xp(psi_matrix);
    psi_im = shift_xm(psi_matrix);
    psi_jp = shift_yp(psi_matrix);
    psi_jm = shift_ym(psi_matrix);

    psi_ipjp = shift_yp(psi_ip);
    psi_ipjm = shift_ym(psi_ip);
    psi_imjp = shift_yp(psi_im);
    psi_imjm = shift_ym(psi_im);

    omega_ip = shift_xp(omega_matrix);
    omega_im = shift_xm(omega_matrix);
    omega_jp = shift_yp(omega_matrix);
    omega_jm = shift_ym(omega_matrix);

    omega_ipjp = shift_yp(omega_ip);
    omega_ipjm = shift_ym(omega_ip);
    omega_imjp = shift_yp(omega_im);
    omega_imjm = shift_ym(omega_im);

    jacobian_1 = ((psi_ip - psi_im) .* (omega_jp - omega_jm) ...
                - (psi_jp - psi_jm) .* (omega_ip - omega_im)) / (4 * dx * dy);

    jacobian_2 = (psi_ip .* (omega_ipjp - omega_ipjm) ...
                - psi_im .* (omega_imjp - omega_imjm) ...
                - psi_jp .* (omega_ipjp - omega_imjp) ...
                + psi_jm .* (omega_ipjm - omega_imjm)) / (4 * dx * dy);

    jacobian_3 = (psi_ipjp .* (omega_jp - omega_ip) ...
                - psi_imjm .* (omega_im - omega_jm) ...
                - psi_imjp .* (omega_jp - omega_im) ...
                + psi_ipjm .* (omega_ip - omega_jm)) / (4 * dx * dy);

    arakawa_jacobian = (jacobian_1 + jacobian_2 + jacobian_3) / 3;

    laplacian_omega = (omega_ip - 2 * omega_matrix + omega_im) / dx^2 ...
                    + (omega_jp - 2 * omega_matrix + omega_jm) / dy^2;

    rhs_matrix = -arakawa_jacobian + nu * laplacian_omega;
    rhs_matrix = apply_fd_domain_mask(rhs_matrix, setup);
    rhs_matrix = zero_wall_rhs_boundaries(rhs_matrix, setup);
    rhs_vector = rhs_matrix(:);
end

function rhs_vector = rhs_fd_simple(omega_vector, setup, nu)
% rhs_fd_simple - Sparse-matrix FD advection + diffusion RHS.
%
%   Production non-Arakawa path:
%     J(psi,omega) = (Bx*psi).*(Cy*omega) - (Cy*psi).*(Bx*omega)
%   on the rectangular FD operator family. Bathymetry-shaped domains keep
%   the legacy masked pointwise path until a geometry-aware sparse-Jacobian
%   operator family is implemented for irregular walls.

    Nx = setup.Nx;
    Ny = setup.Ny;

    omega_matrix = reshape(omega_vector, Ny, Nx);

    if strcmpi(char(string(setup.advection_scheme_effective)), 'MaskedCentralLegacy')
        rhs_vector = rhs_fd_masked_legacy(omega_vector, setup, nu);
        return;
    end

    psi_vector = setup.solve_poisson(omega_vector);

    dpsi_dx = reshape(setup.Bx * psi_vector, Ny, Nx);
    dpsi_dy = reshape(setup.Cy * psi_vector, Ny, Nx);
    domega_dx = reshape(setup.Bx * omega_vector, Ny, Nx);
    domega_dy = reshape(setup.Cy * omega_vector, Ny, Nx);
    advection = dpsi_dx .* domega_dy - dpsi_dy .* domega_dx;

    laplacian_omega = reshape(setup.L * omega_vector, Ny, Nx);

    rhs_matrix = -advection + nu * laplacian_omega;
    rhs_matrix = apply_fd_domain_mask(rhs_matrix, setup);
    rhs_matrix = zero_wall_rhs_boundaries(rhs_matrix, setup);
    rhs_vector = rhs_matrix(:);
end

function rhs_vector = rhs_fd_masked_legacy(omega_vector, setup, nu)
% rhs_fd_masked_legacy - Legacy masked pointwise RHS for shaped bathymetry.

    Nx = setup.Nx;
    Ny = setup.Ny;
    dx = setup.dx;
    dy = setup.dy;

    omega_matrix = reshape(omega_vector, Ny, Nx);
    psi_matrix = reshape(setup.solve_poisson(omega_vector), Ny, Nx);

    shift_xp = setup.shift_xp;
    shift_xm = setup.shift_xm;
    shift_yp = setup.shift_yp;
    shift_ym = setup.shift_ym;

    omega_ip = shift_xp(omega_matrix);
    omega_im = shift_xm(omega_matrix);
    omega_jp = shift_yp(omega_matrix);
    omega_jm = shift_ym(omega_matrix);

    psi_ip = shift_xp(psi_matrix);
    psi_im = shift_xm(psi_matrix);
    psi_jp = shift_yp(psi_matrix);
    psi_jm = shift_ym(psi_matrix);

    u = -(psi_jp - psi_jm) / (2 * dy);
    v =  (psi_ip - psi_im) / (2 * dx);
    advection = u .* (omega_ip - omega_im) / (2 * dx) ...
              + v .* (omega_jp - omega_jm) / (2 * dy);

    laplacian_omega = (omega_ip - 2 * omega_matrix + omega_im) / dx^2 ...
                    + (omega_jp - 2 * omega_matrix + omega_jm) / dy^2;

    rhs_matrix = -advection + nu * laplacian_omega;
    rhs_matrix = apply_fd_domain_mask(rhs_matrix, setup);
    rhs_matrix = zero_wall_rhs_boundaries(rhs_matrix, setup);
    rhs_vector = rhs_matrix(:);
end

function val = gather_if_gpu(val)
% gather_if_gpu - Transfer gpuArray to CPU; pass-through for regular arrays.

    if isa(val, 'gpuArray')
        val = gather(val);
    end
end

function analysis = append_snapshot_metrics(analysis, setup)
% append_snapshot_metrics - Derive per-snapshot quantities used by plots/reports.

    n_snapshots = size(analysis.omega_snaps, 3);
    kinetic_energy = zeros(n_snapshots, 1);
    enstrophy = zeros(n_snapshots, 1);
    max_omega_history = zeros(n_snapshots, 1);
    peak_speed_history = zeros(n_snapshots, 1);
    wall_enstrophy = zeros(n_snapshots, 1);
    wall_max_omega_history = zeros(n_snapshots, 1);
    u_snapshots = zeros(size(analysis.omega_snaps));
    v_snapshots = zeros(size(analysis.omega_snaps));
    fluid_mask = resolve_fd_metric_mask(setup);
    wall_mask = resolve_fd_wall_mask(setup);
    wall_cube = [];
    if any(wall_mask(:))
        wall_cube = NaN(size(analysis.omega_snaps));
    end

    for idx = 1:n_snapshots
        omega_snapshot = analysis.omega_snaps(:, :, idx);
        psi_snapshot = analysis.psi_snaps(:, :, idx);
        [velocity_u, velocity_v] = velocity_from_streamfunction(psi_snapshot, setup);

        u_snapshots(:, :, idx) = velocity_u;
        v_snapshots(:, :, idx) = velocity_v;

        kinetic_energy(idx) = 0.5 * sum((velocity_u(:).^2 + velocity_v(:).^2)) * setup.dx * setup.dy;
        fluid_omega = double(omega_snapshot(fluid_mask));
        if isempty(fluid_omega)
            fluid_omega = 0;
        end
        enstrophy(idx) = 0.5 * sum(fluid_omega(:).^2) * setup.dx * setup.dy;
        max_omega_history(idx) = max(abs(fluid_omega(:)));
        peak_speed_history(idx) = max(sqrt(velocity_u(:).^2 + velocity_v(:).^2));

        if any(wall_mask(:))
            snapshot_time = 0.0;
            if isfield(analysis, 'snapshot_times') && numel(analysis.snapshot_times) >= idx
                snapshot_time = double(analysis.snapshot_times(idx));
            end
            raw_wall_snapshot = reconstruct_fd_raw_wall_snapshot(omega_snapshot, psi_snapshot, setup, snapshot_time);
            wall_omega = double(raw_wall_snapshot(wall_mask));
            if ~isempty(wall_omega)
                wall_enstrophy(idx) = 0.5 * sum(wall_omega(:).^2) * setup.dx * setup.dy;
                wall_max_omega_history(idx) = max(abs(wall_omega(:)));
                wall_slice = NaN(size(omega_snapshot));
                wall_slice(wall_mask) = raw_wall_snapshot(wall_mask);
                wall_cube(:, :, idx) = wall_slice;
            end
        end
    end

    analysis.kinetic_energy = kinetic_energy;
    analysis.enstrophy = enstrophy;
    analysis.max_omega_history = max_omega_history;
    analysis.peak_speed_history = peak_speed_history;
    analysis.u_snaps = u_snapshots;
    analysis.v_snaps = v_snapshots;
    analysis.peak_abs_omega = max(max_omega_history);
    analysis.peak_speed = max(peak_speed_history);
    if any(wall_mask(:))
        analysis.fd_wall_enstrophy = wall_enstrophy;
        analysis.fd_wall_max_omega_history = wall_max_omega_history;
        analysis.fd_wall_omega_snaps = wall_cube;
    end
end

function analysis = maybe_merge_unified_metrics(analysis, Parameters)
% maybe_merge_unified_metrics - Optional harmonization with shared metrics extractor.
    unified_metrics = extract_unified_metrics( ...
        analysis.omega_snaps, ...
        analysis.psi_snaps, ...
        analysis.snapshot_times, ...
        analysis.dx, ...
        analysis.dy, ...
        Parameters);

    analysis = merge_structs(analysis, unified_metrics);
end

function bc = resolve_bc_dispatch(cfg, X, Y, dx, dy)
% resolve_bc_dispatch - Resolve BCs through dispatcher (no local fallback).
    grid_meta = struct('X', X, 'Y', Y, 'dx', dx, 'dy', dy);
    bc = BCDispatcher.resolve(cfg, 'fd', grid_meta);
end

function omega_initial = resolve_ic_dispatch(X, Y, cfg)
% resolve_ic_dispatch - Resolve ICs through dispatcher (no local fallback).
    omega_initial = ICDispatcher.resolve(X, Y, cfg, 'fd');
end

function geometry = resolve_fd_bathymetry_geometry(cfg, X, Y)
% resolve_fd_bathymetry_geometry Build the active 2D bathymetry-domain contract.
    geometry = build_bathymetry_geometry(cfg, X, Y, 'fd');
    if ~geometry.enabled
        geometry = struct('enabled', false, 'dimension', '2d');
    end
end

function field = apply_fd_domain_mask(field, setup)
% apply_fd_domain_mask Zero out solid cells beneath active bathymetry.
    if isfield(setup, 'bathymetry_geometry') && isstruct(setup.bathymetry_geometry) && ...
            isfield(setup.bathymetry_geometry, 'enabled') && logical(setup.bathymetry_geometry.enabled) && ...
            isfield(setup.bathymetry_geometry, 'solid_mask') && ~isempty(setup.bathymetry_geometry.solid_mask)
        field(setup.bathymetry_geometry.solid_mask) = 0;
    end
end

function analysis = append_bathymetry_analysis_metadata(analysis, setup)
% append_bathymetry_analysis_metadata Attach active geometry metadata to analysis outputs.
    if ~isfield(setup, 'bathymetry_geometry') || ~isstruct(setup.bathymetry_geometry)
        return;
    end
    geometry = setup.bathymetry_geometry;
    if ~isfield(geometry, 'enabled') || ~geometry.enabled
        return;
    end

    analysis.grid_points = nnz(geometry.fluid_mask);
    analysis.unknowns = nnz(geometry.interior_mask);
    analysis.poisson_matrix_n = size(setup.A, 1);
    analysis.poisson_matrix_nnz = nnz(setup.A);
    analysis.bathymetry_geometry_dimension = '2d';
    analysis.bathymetry_wet_mask_2d = logical(geometry.wet_mask);
    analysis.bathymetry_fluid_mask_2d = logical(geometry.fluid_mask);
    analysis.bathymetry_wall_mask_2d = logical(geometry.wall_mask);
    analysis.bathymetry_solid_mask_2d = logical(geometry.solid_mask);
    analysis.bathymetry_boundary_mask_2d = logical(geometry.boundary_mask);
    analysis.bathymetry_profile_x = double(geometry.profile_x(:));
    analysis.bathymetry_profile_2d = double(geometry.profile_y(:));
    analysis.bathymetry_field = double(geometry.bathymetry_field);
    if isfield(geometry, 'cell_averaged_bathymetry')
        analysis.bathymetry_cell_average_2d = double(geometry.cell_averaged_bathymetry);
    end
    if isfield(geometry, 'bathymetry_slope_x')
        analysis.bathymetry_slope_x_2d = double(geometry.bathymetry_slope_x);
    end
    if isfield(geometry, 'bathymetry_slope_y')
        analysis.bathymetry_slope_y_2d = double(geometry.bathymetry_slope_y);
    end
    analysis.bathymetry_scenario = char(string(geometry.scenario));
end

function analysis = append_fd_wall_analysis_metadata(analysis, setup)
% append_fd_wall_analysis_metadata Attach generic FD fluid/wall metadata.
    if ~isfield(setup, 'fd_fluid_mask') || ~isfield(setup, 'fd_wall_mask')
        return;
    end

    fluid_mask = logical(setup.fd_fluid_mask);
    wall_mask = logical(setup.fd_wall_mask);
    if ~isequal(size(fluid_mask), size(wall_mask))
        return;
    end

    analysis.fd_fluid_mask_2d = fluid_mask;
    analysis.fd_wall_mask_2d = wall_mask;
end

function field = apply_fd_post_closure_edge_omega_clamp(field, setup)
% apply_fd_post_closure_edge_omega_clamp Optionally zero working wall omega after closure.
    if ~isfield(setup, 'fd_post_closure_edge_omega_zero') || ~logical(setup.fd_post_closure_edge_omega_zero)
        return;
    end

    wall_mask = [];
    if isfield(setup, 'fixed_omega_mask') && ~isempty(setup.fixed_omega_mask)
        wall_mask = logical(setup.fixed_omega_mask);
    elseif isfield(setup, 'fd_wall_mask') && ~isempty(setup.fd_wall_mask)
        wall_mask = logical(setup.fd_wall_mask);
    end
    if isempty(wall_mask)
        return;
    end

    field(wall_mask) = 0;
end

function raw_wall_snapshot = reconstruct_fd_raw_wall_snapshot(omega_snapshot, psi_snapshot, setup, time_value)
% reconstruct_fd_raw_wall_snapshot Recover the unclamped wall-closure field for diagnostics.
    raw_wall_snapshot = omega_snapshot;
    if ~isfield(setup, 'apply_bc') || isempty(setup.apply_bc)
        return;
    end
    raw_wall_snapshot = setup.apply_bc(raw_wall_snapshot, psi_snapshot, setup, time_value);
    raw_wall_snapshot = apply_fd_domain_mask(raw_wall_snapshot, setup);
end

function [fluid_mask, wall_mask, solid_mask] = resolve_fd_domain_role_masks(common, bathymetry_geometry, Ny, Nx)
% resolve_fd_domain_role_masks Resolve fluid/wall/solid roles for FD diagnostics.
    if nargin >= 2 && isstruct(bathymetry_geometry) && ...
            isfield(bathymetry_geometry, 'enabled') && logical(bathymetry_geometry.enabled)
        fluid_mask = logical(bathymetry_geometry.fluid_mask);
        wall_mask = logical(bathymetry_geometry.wall_mask);
        solid_mask = logical(bathymetry_geometry.solid_mask);
        return;
    end

    wall_mask = false(Ny, Nx);
    if nargin >= 1 && isstruct(common) && isfield(common, 'sides')
        if isfield(common.sides, 'top') && strcmp(common.sides.top.kind, 'wall')
            wall_mask(end, :) = true;
        end
        if isfield(common.sides, 'bottom') && strcmp(common.sides.bottom.kind, 'wall')
            wall_mask(1, :) = true;
        end
        if isfield(common.sides, 'left') && strcmp(common.sides.left.kind, 'wall')
            wall_mask(:, 1) = true;
        end
        if isfield(common.sides, 'right') && strcmp(common.sides.right.kind, 'wall')
            wall_mask(:, end) = true;
        end
    end

    fluid_mask = true(Ny, Nx);
    fluid_mask(wall_mask) = false;
    solid_mask = false(Ny, Nx);
end

function fluid_mask = resolve_fd_metric_mask(setup)
% resolve_fd_metric_mask Resolve fluid-facing mask for FD omega diagnostics.
    fluid_mask = [];
    if isfield(setup, 'fd_fluid_mask') && ~isempty(setup.fd_fluid_mask)
        fluid_mask = logical(setup.fd_fluid_mask);
    end
    if isempty(fluid_mask)
        fluid_mask = true(setup.Ny, setup.Nx);
    end
end

function wall_mask = resolve_fd_wall_mask(setup)
% resolve_fd_wall_mask Resolve auxiliary wall mask for FD diagnostics.
    wall_mask = false(setup.Ny, setup.Nx);
    if isfield(setup, 'fd_wall_mask') && ~isempty(setup.fd_wall_mask)
        wall_mask = logical(setup.fd_wall_mask);
    end
end

function fig_handle = create_fd_summary_figure(analysis, Parameters)
% create_fd_summary_figure - Build a compact diagnostic figure set.

    show_figures = usejava("desktop") && ~strcmpi(get(0, "DefaultFigureVisible"), "off");
    figure_visibility = "off";
    if show_figures
        figure_visibility = "on";
    end

    fig_handle = figure("Name", "Finite Difference Analysis", ...
        "NumberTitle", "off", ...
        "Visible", figure_visibility, ...
        "Position", [100, 100, 1100, 700]);
    apply_dark_theme_for_figure(fig_handle);

    snapshot_times = analysis.snapshot_times(:);
    n_snapshots = max(1, size(analysis.omega_snaps, 3));
    snapshot_indices = unique(round(linspace(1, n_snapshots, min(4, n_snapshots))));

    tiledlayout(2, 2, "TileSpacing", "compact");

    nexttile;
    imagesc(analysis.omega_snaps(:, :, 1));
    axis equal tight;
    set(gca, "YDir", "normal");
    colorbar;
    title(sprintf("Initial vorticity (t=%.3f)", snapshot_times(1)));
    xlabel("x-index");
    ylabel("y-index");

    nexttile;
    imagesc(analysis.omega_snaps(:, :, end));
    axis equal tight;
    set(gca, "YDir", "normal");
    colorbar;
    title(sprintf("Final vorticity (t=%.3f)", snapshot_times(end)));
    xlabel("x-index");
    ylabel("y-index");

    nexttile;
    plot(snapshot_times, analysis.kinetic_energy, "LineWidth", 1.8);
    hold on;
    plot(snapshot_times, analysis.enstrophy, "LineWidth", 1.8);
    hold off;
    grid on;
    xlabel("time (s)");
    ylabel("integral quantity");
    legend("Kinetic energy", "Enstrophy", "Location", "best");
    title("Integral diagnostics");

    nexttile;
    plot(snapshot_times, analysis.max_omega_history, "LineWidth", 1.8);
    hold on;
    plot(snapshot_times, analysis.peak_speed_history, "LineWidth", 1.5);
    hold off;
    grid on;
    xlabel("time (s)");
    ylabel("peak value");
    legend("max|omega|", "peak speed", "Location", "best");
    title("Peak metrics");

    ic_name = char(string(Parameters.ic_type));
    integrator_label = 'RK4';
    if isfield(analysis, 'time_integrator') && ~isempty(analysis.time_integrator)
        integrator_label = char(string(analysis.time_integrator));
    end
    scheme_label = 'SparseMatrix';
    if isfield(analysis, 'advection_scheme_effective') && ~isempty(analysis.advection_scheme_effective)
        scheme_label = char(string(analysis.advection_scheme_effective));
    elseif analysis.use_arakawa
        scheme_label = 'Arakawa';
    end
    if analysis.use_arakawa
        method_label = "FD Arakawa-" + string(integrator_label);
    else
        method_label = "FD " + string(scheme_label) + "-" + string(integrator_label);
    end
    if analysis.use_gpu
        method_label = method_label + " [GPU]";
    end
    sgtitle(sprintf("%s | IC=%s | Grid=%dx%d", method_label, ic_name, analysis.Nx, analysis.Ny));

    % Add tiny overlay markers for representative snapshots to aid quick visual checks.
    for idx = 1:numel(snapshot_indices)
        marker_index = snapshot_indices(idx);
        annotation_text = sprintf("t=%.3f", snapshot_times(marker_index));
        x_pos = 0.02 + 0.10 * (idx - 1);
        annotation("textbox", [x_pos, 0.01, 0.09, 0.03], ...
            "String", annotation_text, ...
            "EdgeColor", "none", ...
            "HorizontalAlignment", "left", ...
            "FontSize", 8);
    end
end

function maybe_write_vorticity_animation(analysis, cfg, Parameters)
% maybe_write_vorticity_animation - Delegate direct-run media export to the shared MP4 exporter.

    if ~logical(Parameters.create_animations)
        return;
    end
    if size(analysis.omega_snaps, 3) < 2
        return;
    end
    if Parameters.animation_fps <= 0
        error('FD:InvalidAnimationFPS', ...
            'animation_fps must be positive when create_animations=true.');
    end

    media_root = '';
    if isfield(Parameters, 'animation_dir') && ~isempty(Parameters.animation_dir)
        media_root = char(string(Parameters.animation_dir));
    elseif isfield(Parameters, 'paths') && isstruct(Parameters.paths) && isfield(Parameters.paths, 'media_animation')
        media_root = char(string(Parameters.paths.media_animation));
    end
    if isempty(media_root)
        error('FD:InvalidAnimationDir', ...
            'A canonical media_animation root is required when create_animations=true.');
    end

    run_cfg = struct();
    if isfield(Parameters, 'mode')
        run_cfg.mode = char(string(Parameters.mode));
    else
        run_cfg.mode = 'Evolution';
    end
    if isfield(Parameters, 'run_id')
        run_cfg.run_id = char(string(Parameters.run_id));
    end
    if isfield(Parameters, 'ic_type')
        run_cfg.ic_type = char(string(Parameters.ic_type));
    end

    paths = struct( ...
        'media_animation', media_root, ...
        'media_animation_combined', fullfile(media_root, 'Combined'), ...
        'media_animation_panes', fullfile(media_root, 'Panes'));
    animation_format = 'mp4+gif';
    animation_formats = {'mp4', 'gif'};
    animation_gif_min_frames = 100;
    if isfield(Parameters, 'animation_export_format') && ~isempty(Parameters.animation_export_format)
        animation_format = char(string(Parameters.animation_export_format));
    elseif isfield(Parameters, 'animation_format') && ~isempty(Parameters.animation_format)
        animation_format = char(string(Parameters.animation_format));
    end
    if isfield(Parameters, 'animation_export_formats') && ~isempty(Parameters.animation_export_formats)
        animation_formats = Parameters.animation_export_formats;
    end
    if isfield(Parameters, 'animation_gif_min_frames') && isfinite(Parameters.animation_gif_min_frames)
        animation_gif_min_frames = max(100, round(double(Parameters.animation_gif_min_frames)));
    end
    settings = struct( ...
        'animation_enabled', true, ...
        'animation_format', animation_format, ...
        'animation_export_format', animation_format, ...
        'animation_export_formats', {animation_formats}, ...
        'animation_fps', double(Parameters.animation_fps), ...
        'animation_duration_s', max(double(Parameters.animation_num_frames) / max(double(Parameters.animation_fps), 1), 0.1), ...
        'animation_gif_min_frames', animation_gif_min_frames, ...
        'animation_export_dpi', 600, ...
        'animation_export_preset', 'double_column_report', ...
        'animation_export_width_in', 7.16, ...
        'animation_export_height_in', 5.37, ...
        'animation_export_resolution_px', [4296, 3222], ...
        'media', struct( ...
            'enabled', true, ...
            'format', animation_format, ...
            'formats', {animation_formats}, ...
            'fps', double(Parameters.animation_fps), ...
            'frame_count', max(2, round(double(Parameters.animation_num_frames))), ...
            'duration_s', max(double(Parameters.animation_num_frames) / max(double(Parameters.animation_fps), 1), 0.1), ...
            'gif_min_frame_count', animation_gif_min_frames, ...
            'dpi', 600, ...
            'preset', 'double_column_report', ...
            'width_in', 7.16, ...
            'height_in', 5.37, ...
            'resolution_px', [4296, 3222]));
    ResultsAnimationExporter.export_from_analysis(analysis, Parameters, run_cfg, paths, settings);
end

function cfl_estimate = compute_cfl_estimate(peak_speed, dt, setup)
% compute_cfl_estimate - Cheap CFL estimate for console progress output.

    cfl_estimate = peak_speed * dt / min(setup.dx, setup.dy);
end

function [velocity_u, velocity_v] = velocity_from_streamfunction(psi, setup)
% velocity_from_streamfunction - Recover velocity from streamfunction.

    velocity_u = -(setup.shift_yp(psi) - setup.shift_ym(psi)) / (2 * setup.dy);
    velocity_v = (setup.shift_xp(psi) - setup.shift_xm(psi)) / (2 * setup.dx);
    if isfield(setup, 'bathymetry_geometry') && isstruct(setup.bathymetry_geometry) && ...
            isfield(setup.bathymetry_geometry, 'enabled') && logical(setup.bathymetry_geometry.enabled) && ...
            isfield(setup.bathymetry_geometry, 'solid_mask')
        [velocity_u, velocity_v] = apply_fd_bathymetry_velocity_mask(psi, velocity_u, velocity_v, setup);
    elseif isfield(setup, 'bathymetry_geometry') && isstruct(setup.bathymetry_geometry) && ...
            isfield(setup.bathymetry_geometry, 'solid_mask')
        solid_mask = setup.bathymetry_geometry.solid_mask;
        velocity_u(solid_mask) = 0;
        velocity_v(solid_mask) = 0;
    end
    if isfield(setup, 'enforce_velocity_bc') && isa(setup.enforce_velocity_bc, 'function_handle')
        [velocity_u, velocity_v] = setup.enforce_velocity_bc(velocity_u, velocity_v, setup);
    end
end

function [velocity_u, velocity_v] = apply_fd_bathymetry_velocity_mask(psi, velocity_u, velocity_v, setup)
% apply_fd_bathymetry_velocity_mask Rebuild reported velocity without sampling through the wall.
    geometry = setup.bathymetry_geometry;
    if ~isfield(geometry, 'fluid_mask') || isempty(geometry.fluid_mask)
        geometry.fluid_mask = geometry.wet_mask;
    end
    fluid_mask = logical(geometry.fluid_mask);
    wall_mask = false(size(fluid_mask));
    if isfield(geometry, 'wall_mask') && ~isempty(geometry.wall_mask)
        wall_mask = logical(geometry.wall_mask);
    end
    solid_mask = false(size(fluid_mask));
    if isfield(geometry, 'solid_mask') && ~isempty(geometry.solid_mask)
        solid_mask = logical(geometry.solid_mask);
    end

    velocity_u(:) = 0;
    velocity_v(:) = 0;

    [rows, cols] = find(fluid_mask);
    for idx = 1:numel(rows)
        row = rows(idx);
        col = cols(idx);
        dpsi_dy = masked_first_derivative(psi, fluid_mask, row, col, setup.dy, 'y');
        dpsi_dx = masked_first_derivative(psi, fluid_mask, row, col, setup.dx, 'x');
        velocity_u(row, col) = -dpsi_dy;
        velocity_v(row, col) = dpsi_dx;
    end

    velocity_u(wall_mask | solid_mask) = 0;
    velocity_v(wall_mask | solid_mask) = 0;
end

function deriv = masked_first_derivative(field, fluid_mask, row, col, spacing, axis_id)
% masked_first_derivative One-sided/central derivative that never crosses into masked cells.
    if strcmp(axis_id, 'x')
        forward1 = col + 1;
        forward2 = col + 2;
        backward1 = col - 1;
        backward2 = col - 2;
        sample = @(r, c) field(r, c);
        valid = @(r, c) c >= 1 && c <= size(field, 2) && fluid_mask(r, c);
    else
        forward1 = row + 1;
        forward2 = row + 2;
        backward1 = row - 1;
        backward2 = row - 2;
        sample = @(r, c) field(r, c);
        valid = @(r, c) r >= 1 && r <= size(field, 1) && fluid_mask(r, c);
    end

    if strcmp(axis_id, 'x')
        if valid(row, backward1) && valid(row, forward1)
            deriv = (sample(row, forward1) - sample(row, backward1)) / (2 * spacing);
            return;
        end
        if valid(row, forward1) && valid(row, forward2)
            deriv = (-3 * sample(row, col) + 4 * sample(row, forward1) - sample(row, forward2)) / (2 * spacing);
            return;
        end
        if valid(row, backward1) && valid(row, backward2)
            deriv = (3 * sample(row, col) - 4 * sample(row, backward1) + sample(row, backward2)) / (2 * spacing);
            return;
        end
        if valid(row, forward1)
            deriv = (sample(row, forward1) - sample(row, col)) / spacing;
            return;
        end
        if valid(row, backward1)
            deriv = (sample(row, col) - sample(row, backward1)) / spacing;
            return;
        end
    else
        if valid(backward1, col) && valid(forward1, col)
            deriv = (sample(forward1, col) - sample(backward1, col)) / (2 * spacing);
            return;
        end
        if valid(forward1, col) && valid(forward2, col)
            deriv = (-3 * sample(row, col) + 4 * sample(forward1, col) - sample(forward2, col)) / (2 * spacing);
            return;
        end
        if valid(backward1, col) && valid(backward2, col)
            deriv = (3 * sample(row, col) - 4 * sample(backward1, col) + sample(backward2, col)) / (2 * spacing);
            return;
        end
        if valid(forward1, col)
            deriv = (sample(forward1, col) - sample(row, col)) / spacing;
            return;
        end
        if valid(backward1, col)
            deriv = (sample(row, col) - sample(backward1, col)) / spacing;
            return;
        end
    end

    deriv = 0;
end

function shifted = shift_xp_nonperiodic(F)
% shift_xp_nonperiodic - Mimic circshift(F,[0,+1]) without periodic wrap.
    shifted = [F(:, 1), F(:, 1:end-1)];
end

function shifted = shift_xm_nonperiodic(F)
% shift_xm_nonperiodic - Mimic circshift(F,[0,-1]) without periodic wrap.
    shifted = [F(:, 2:end), F(:, end)];
end

function shifted = shift_yp_nonperiodic(F)
% shift_yp_nonperiodic - Mimic circshift(F,[+1,0]) without periodic wrap.
    shifted = [F(1, :); F(1:end-1, :)];
end

function shifted = shift_ym_nonperiodic(F)
% shift_ym_nonperiodic - Mimic circshift(F,[-1,0]) without periodic wrap.
    shifted = [F(2:end, :); F(end, :)];
end

function rhs_matrix = zero_wall_rhs_boundaries(rhs_matrix, setup)
% zero_wall_rhs_boundaries - Keep wall vorticity fixed by dispatcher updates.
    if ~isfield(setup, 'is_periodic_bc') || logical(setup.is_periodic_bc)
        return;
    end
    if isfield(setup, 'fixed_omega_mask') && ~isempty(setup.fixed_omega_mask)
        rhs_matrix(logical(setup.fixed_omega_mask)) = 0;
    else
        rhs_matrix(1, :) = 0;
        rhs_matrix(end, :) = 0;
        rhs_matrix(:, 1) = 0;
        rhs_matrix(:, end) = 0;
    end
    if isfield(setup, 'bathymetry_geometry') && isstruct(setup.bathymetry_geometry) && ...
            isfield(setup.bathymetry_geometry, 'solid_mask') && ~isempty(setup.bathymetry_geometry.solid_mask)
        rhs_matrix(setup.bathymetry_geometry.solid_mask) = 0;
    end
end

function progress_stride = resolve_progress_stride(Parameters, n_steps)
% resolve_progress_stride - Auto/explicit stride for textual progress logs.

    if ~isnumeric(Parameters.progress_stride) || ~isscalar(Parameters.progress_stride) || ~isfinite(Parameters.progress_stride)
        error('FD:InvalidProgressStride', ...
            'progress_stride must be a finite numeric scalar.');
    end
    progress_stride = round(Parameters.progress_stride);

    if progress_stride <= 0
        progress_stride = max(1, round(n_steps / 20));
    else
        progress_stride = max(1, progress_stride);
    end
end

function live_stride = resolve_live_stride(Parameters, n_steps)
% resolve_live_stride - Auto/explicit stride for live figure updates.

    if ~isnumeric(Parameters.live_stride) || ~isscalar(Parameters.live_stride) || ~isfinite(Parameters.live_stride)
        error('FD:InvalidLiveStride', ...
            'live_stride must be a finite numeric scalar.');
    end
    live_stride = round(Parameters.live_stride);

    if live_stride <= 0
        live_stride = max(1, round(n_steps / 40));
    else
        live_stride = max(1, live_stride);
    end
end

function live_preview = open_live_preview_if_requested(Parameters, cfg, omega_initial)
% open_live_preview_if_requested - Create optional live-preview figure.

    live_preview = struct();
    live_preview.enabled = false;
    live_preview.figure = [];
    live_preview.image = [];
    live_preview.axes = [];

    if ~logical(Parameters.live_preview)
        return;
    end
    if ~usejava("desktop")
        return;
    end

    dx = cfg.Lx / cfg.Nx;
    dy = cfg.Ly / cfg.Ny;
    x = linspace(-cfg.Lx/2, cfg.Lx/2 - dx, cfg.Nx);
    y = linspace(-cfg.Ly/2, cfg.Ly/2 - dy, cfg.Ny);

    live_preview.figure = figure("Name", "FD Live Preview", "NumberTitle", "off");
    apply_dark_theme_for_figure(live_preview.figure);
    live_preview.axes = axes("Parent", live_preview.figure);
    live_preview.image = imagesc(live_preview.axes, x, y, omega_initial);
    axis(live_preview.axes, "equal", "tight");
    set(live_preview.axes, "YDir", "normal");
    colormap(live_preview.axes, turbo);
    colorbar(live_preview.axes);
    title(live_preview.axes, "Live vorticity preview: t = 0.000");
    drawnow;

    live_preview.enabled = true;
end

function update_live_preview(live_preview, omega_matrix, time_value)
% update_live_preview - Refresh optional live-preview frame.

    if ~live_preview.enabled
        return;
    end
    if isempty(live_preview.figure) || ~isvalid(live_preview.figure)
        return;
    end

    set(live_preview.image, "CData", omega_matrix);
    title(live_preview.axes, sprintf("Live vorticity preview: t = %.3f", time_value));
    drawnow limitrate;
end

function close_live_preview(live_preview)
% close_live_preview - Close optional live-preview window cleanly.

    if ~live_preview.enabled
        return;
    end
    safe_close(live_preview.figure);
end

function safe_close(fig_handle)
% safe_close - Close figure if valid (used by cleanup code paths).

    if ~isempty(fig_handle) && isvalid(fig_handle)
        close(fig_handle);
    end
end

function apply_dark_theme_for_figure(fig_handle)
% apply_dark_theme_for_figure - Apply canonical dark plotting style.
    if isempty(fig_handle) || ~isvalid(fig_handle)
        return;
    end
    try
        ResultsPlotDispatcher.apply_dark_theme(fig_handle, ResultsPlotDispatcher.default_colors());
    catch
        % Keep simulation robust even if styling utility is unavailable.
    end
end

function merged = merge_structs(a, b)
% merge_structs - Merge structs with right-hand side precedence.

    merged = a;
    if isempty(b)
        return;
    end

    fields = fieldnames(b);
    for idx = 1:numel(fields)
        merged.(fields{idx}) = b.(fields{idx});
    end
end

function value = local_pick_struct_text(source_struct, field_name, default_value)
% local_pick_struct_text - Resolve optional string-like struct field.

    value = default_value;
    if nargin < 3
        default_value = "";
        value = default_value;
    end
    if isstruct(source_struct) && isfield(source_struct, field_name) && ~isempty(source_struct.(field_name))
        value = source_struct.(field_name);
    end
end

function require_struct_fields(source_struct, required_fields, context_label)
% require_struct_fields - Fail fast when required fields are absent.
    for idx = 1:numel(required_fields)
        field_name = required_fields{idx};
        if ~isfield(source_struct, field_name)
            error('FD:MissingField', ...
                'Missing required field for %s: %s', context_label, field_name);
        end
    end
end
