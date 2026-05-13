classdef MethodConfigBuilder
% MethodConfigBuilder - Canonical method configuration normalizer for FD/FV/Spectral/Shallow Water.
%
% Contract produced by build(...):
%   grid_mode, dx, dy, Lx, Ly, Nx, Ny, is_anisotropic
% plus method-specific required fields.

    methods (Static)

        function cfg = build(Parameters, method_name, caller_label)
            if nargin < 3 || isempty(caller_label)
                caller_label = 'runtime';
            end

            method = lower(strtrim(char(string(method_name))));
            method = strrep(method, ' ', '_');

            cfg = struct();
            cfg.method = method;

            MethodConfigBuilder.require_fields(Parameters, ...
                {'nu', 'dt', 'Tfinal', 'ic_type', 'Nx', 'Ny', 'grid_mode'}, caller_label);

            cfg.nu = double(Parameters.nu);
            cfg.dt = double(Parameters.dt);
            cfg.Tfinal = double(Parameters.Tfinal);
            cfg.ic_type = char(string(Parameters.ic_type));
            cfg.Nx = round(double(Parameters.Nx));
            cfg.Ny = round(double(Parameters.Ny));

            if cfg.Nx <= 0 || cfg.Ny <= 0
                error('CFG:InvalidGridSize', ...
                    'Nx and Ny must be positive integers (%s).', caller_label);
            end
            if ~(isfinite(cfg.dt) && cfg.dt > 0 && isfinite(cfg.Tfinal) && cfg.Tfinal > 0)
                error('CFG:InvalidTimeConfig', ...
                    'dt and Tfinal must be finite positive values (%s).', caller_label);
            end
            if ~(isfinite(cfg.nu) && cfg.nu >= 0)
                error('CFG:InvalidViscosity', ...
                    'nu must be finite and >= 0 (%s).', caller_label);
            end

            cfg = MethodConfigBuilder.normalize_grid(cfg, Parameters, caller_label);
            cfg = MethodConfigBuilder.copy_initial_condition(cfg, Parameters, caller_label);
            cfg = MethodConfigBuilder.copy_bc_fields(cfg, Parameters, caller_label, method);

            switch method
                case {'fd', 'finite_difference', 'finite_difference_method'}
                    MethodConfigBuilder.require_fields(Parameters, {'delta', 'use_gpu'}, caller_label);
                    cfg.delta = double(Parameters.delta);
                    cfg.use_gpu = logical(Parameters.use_gpu);
                    cfg.fd_advection_scheme = MethodConfigBuilder.resolve_fd_advection_scheme(Parameters);
                    cfg.use_arakawa = strcmp(cfg.fd_advection_scheme, 'ARAKAWA');
                    if isfield(Parameters, 'fd_post_closure_edge_omega_zero')
                        cfg.fd_post_closure_edge_omega_zero = logical(Parameters.fd_post_closure_edge_omega_zero);
                    else
                        cfg.fd_post_closure_edge_omega_zero = false;
                    end
                    cfg.time_integrator = MethodConfigBuilder.resolve_time_integrator(Parameters, method, 'RK4');
                    if ~(isfinite(cfg.delta) && cfg.delta > 0)
                        error('CFG:InvalidDelta', ...
                            'delta must be finite and positive (%s).', caller_label);
                    end
                    if ~any(strcmp(cfg.time_integrator, {'RK4', 'FORWARD_EULER'}))
                        error('CFG:InvalidIntegrator', ...
                            'FD integrator must be RK4 or Forward Euler (%s).', caller_label);
                    end

                case {'spectral', 'fft', 'pseudo_spectral'}
                    cfg.time_integrator = MethodConfigBuilder.resolve_time_integrator(Parameters, method, 'RK4');
                    if ~strcmp(cfg.time_integrator, 'RK4')
                        error('CFG:InvalidIntegrator', ...
                            'Spectral integrator support is RK4 only (%s).', caller_label);
                    end
                    if isfield(Parameters, 'kx') && ~isempty(Parameters.kx)
                        cfg.kx = double(Parameters.kx(:).');
                    end
                    if isfield(Parameters, 'ky') && ~isempty(Parameters.ky)
                        cfg.ky = double(Parameters.ky(:).');
                    end

                case {'fv', 'finite_volume', 'finitevolume'}
                    MethodConfigBuilder.require_fields(Parameters, {'Nz', 'Lz', 'method_config'}, caller_label);
                    cfg.Nz = round(double(Parameters.Nz));
                    cfg.Lz = double(Parameters.Lz);
                    cfg.time_integrator = MethodConfigBuilder.resolve_time_integrator(Parameters, method, 'SSP_RK3');
                    if strcmp(cfg.time_integrator, 'RK3')
                        cfg.time_integrator = 'SSP_RK3';
                    end
                    if ~strcmp(cfg.time_integrator, 'SSP_RK3')
                        error('CFG:InvalidIntegrator', ...
                            'Finite Volume integrator support is SSP_RK3 only (%s).', caller_label);
                    end

                    if cfg.Nz <= 0 || ~(isfinite(cfg.Lz) && cfg.Lz > 0)
                        error('CFG:InvalidFVVerticalGrid', ...
                            'Nz must be positive and Lz must be finite positive (%s).', caller_label);
                    end

                    if ~isstruct(Parameters.method_config) || ~isfield(Parameters.method_config, 'fv3d')
                        error('CFG:MissingFV3DConfig', ...
                            'method_config.fv3d is required for FV (%s).', caller_label);
                    end
                    fv3d = Parameters.method_config.fv3d;
                    MethodConfigBuilder.require_fields(fv3d, ...
                        {'vertical_diffusivity_scale', 'z_boundary'}, ...
                        [caller_label '.method_config.fv3d']);
                    cfg.fv3d = fv3d;

                case {'swe', 'shallow_water', 'shallowwater', 'shallow_water_method'}
                    MethodConfigBuilder.require_fields(Parameters, {'method_config'}, caller_label);
                    if strcmpi(cfg.ic_type, 'no_initial_condition')
                        error('CFG:UnsupportedICForShallowWater', ...
                            'Shallow Water does not accept the no_initial_condition vorticity initializer (%s).', caller_label);
                    end
                    cfg.time_integrator = MethodConfigBuilder.resolve_time_integrator(Parameters, method, 'SSP_RK3');
                    if strcmp(cfg.time_integrator, 'RK3')
                        cfg.time_integrator = 'SSP_RK3';
                    end
                    if ~strcmp(cfg.time_integrator, 'SSP_RK3')
                        error('CFG:InvalidIntegrator', ...
                            'Shallow Water integrator support is SSP_RK3 only (%s).', caller_label);
                    end
                    if ~isstruct(Parameters.method_config) || ~isfield(Parameters.method_config, 'swe2d')
                        error('CFG:MissingSWEConfig', ...
                            'method_config.swe2d is required for Shallow Water (%s).', caller_label);
                    end
                    swe2d = Parameters.method_config.swe2d;
                    MethodConfigBuilder.require_fields(swe2d, ...
                        {'gravity', 'base_depth', 'dry_tolerance', 'cfl', ...
                         'bed_relief_fraction', 'bed_friction_coeff', 'initial_condition', ...
                         'surface_amplitude', 'surface_sigma_x', 'surface_sigma_y', ...
                         'surface_center_x', 'surface_center_y', ...
                         'momentum_amplitude_x', 'momentum_amplitude_y', ...
                         'enable_wind', 'wind_velocity_x', 'wind_velocity_y', ...
                         'wind_drag_coeff', 'air_density', 'water_density'}, ...
                        [caller_label '.method_config.swe2d']);
                    cfg.swe2d = swe2d;

                otherwise
                    error('CFG:UnknownMethod', 'Unknown method token in MethodConfigBuilder: %s', method);
            end

            if isfield(Parameters, 'snap_times')
                cfg.snap_times = Parameters.snap_times;
            end
            if isfield(Parameters, 'run_id')
                cfg.run_id = Parameters.run_id;
            end
            cfg = MethodConfigBuilder.copy_bathymetry_fields(cfg, Parameters);
        end

        function integrator = resolve_time_integrator(Parameters, method, fallback)
            integrator = upper(char(string(fallback)));

            if isfield(Parameters, 'time_integrator') && ~isempty(Parameters.time_integrator)
                integrator = upper(char(string(Parameters.time_integrator)));
            end

            if isfield(Parameters, 'method_config') && isstruct(Parameters.method_config)
                switch method
                    case {'fd', 'finite_difference', 'finite_difference_method'}
                        subkey = 'fd';
                    case {'spectral', 'fft', 'pseudo_spectral'}
                        subkey = 'spectral';
                    case {'fv', 'finite_volume', 'finitevolume'}
                        subkey = 'fv';
                    case {'swe', 'shallow_water', 'shallowwater', 'shallow_water_method'}
                        subkey = 'swe2d';
                    otherwise
                        subkey = '';
                end
                if ~isempty(subkey) && isfield(Parameters.method_config, subkey)
                    sub_cfg = Parameters.method_config.(subkey);
                    if isstruct(sub_cfg) && isfield(sub_cfg, 'time_integrator') && ...
                            ~isempty(sub_cfg.time_integrator)
                        integrator = upper(char(string(sub_cfg.time_integrator)));
                    end
                end
            end

            integrator = strrep(integrator, '-', '_');
            integrator = strrep(integrator, ' ', '_');
            if strcmp(integrator, 'EULER')
                integrator = 'FORWARD_EULER';
            elseif strcmp(integrator, 'SSPRK3')
                integrator = 'SSP_RK3';
            end
        end

        function analysis = apply_analysis_contract(analysis, cfg, Parameters)
            % Ensure common analysis metadata across FD/FV/Spectral/Shallow Water outputs.
            analysis.grid_mode = cfg.grid_mode;
            analysis.Nx = cfg.Nx;
            analysis.Ny = cfg.Ny;
            analysis.Lx = cfg.Lx;
            analysis.Ly = cfg.Ly;
            analysis.dx = cfg.dx;
            analysis.dy = cfg.dy;
            analysis.is_anisotropic = cfg.is_anisotropic;

            if ~isfield(analysis, 'grid_points') || isempty(analysis.grid_points)
                analysis.grid_points = cfg.Nx * cfg.Ny;
            end

            if isfield(Parameters, 'run_id') && ~isempty(Parameters.run_id)
                analysis.run_id = Parameters.run_id;
            elseif isfield(cfg, 'run_id') && ~isempty(cfg.run_id)
                analysis.run_id = cfg.run_id;
            end

            if ~isfield(analysis, 'snapshot_times_requested') || isempty(analysis.snapshot_times_requested)
                if isfield(analysis, 'snapshot_times') && ~isempty(analysis.snapshot_times)
                    analysis.snapshot_times_requested = analysis.snapshot_times;
                elseif isfield(Parameters, 'plot_snap_times') && ~isempty(Parameters.plot_snap_times)
                    analysis.snapshot_times_requested = Parameters.plot_snap_times;
                elseif isfield(Parameters, 'snap_times') && ~isempty(Parameters.snap_times)
                    analysis.snapshot_times_requested = Parameters.snap_times;
                end
            end
            if ~isfield(analysis, 'snapshot_times_actual') || isempty(analysis.snapshot_times_actual)
                if isfield(analysis, 'time_vec') && ~isempty(analysis.time_vec)
                    analysis.snapshot_times_actual = analysis.time_vec;
                elseif isfield(analysis, 'snapshot_times') && ~isempty(analysis.snapshot_times)
                    analysis.snapshot_times_actual = analysis.snapshot_times;
                elseif isfield(analysis, 'snapshot_times_requested') && ~isempty(analysis.snapshot_times_requested)
                    analysis.snapshot_times_actual = analysis.snapshot_times_requested;
                end
            end
            if isfield(analysis, 'snapshot_times_requested') && ~isempty(analysis.snapshot_times_requested)
                analysis.snapshot_times = analysis.snapshot_times_requested;
            elseif ~isfield(analysis, 'snapshot_times') && isfield(analysis, 'time_vec')
                analysis.snapshot_times = analysis.time_vec;
            end
            if ~isfield(analysis, 'time_vec') || isempty(analysis.time_vec)
                if isfield(analysis, 'snapshot_times_actual') && ~isempty(analysis.snapshot_times_actual)
                    analysis.time_vec = analysis.snapshot_times_actual;
                elseif isfield(analysis, 'snapshot_times') && ~isempty(analysis.snapshot_times)
                    analysis.time_vec = analysis.snapshot_times;
                end
            end
        end

        function require_fields(source_struct, required_fields, context_label)
            for i = 1:numel(required_fields)
                key = required_fields{i};
                if ~isfield(source_struct, key)
                    error('CFG:MissingField', ...
                        'Missing required field for %s: %s', context_label, key);
                end
            end
        end

    end

    methods (Static, Access = private)

        function cfg = normalize_grid(cfg, Parameters, caller_label)
            grid_mode = lower(strtrim(char(string(Parameters.grid_mode))));
            grid_mode = strrep(grid_mode, '-', '_');
            grid_mode = strrep(grid_mode, ' ', '_');

            switch grid_mode
                case 'domain_driven'
                    MethodConfigBuilder.require_fields(Parameters, {'Lx', 'Ly'}, caller_label);
                    cfg.Lx = double(Parameters.Lx);
                    cfg.Ly = double(Parameters.Ly);
                    if ~(isfinite(cfg.Lx) && cfg.Lx > 0 && isfinite(cfg.Ly) && cfg.Ly > 0)
                        error('CFG:InvalidDomain', ...
                            'Lx and Ly must be finite positive values (%s).', caller_label);
                    end
                    cfg.dx = cfg.Lx / cfg.Nx;
                    cfg.dy = cfg.Ly / cfg.Ny;

                case 'spacing_driven'
                    MethodConfigBuilder.require_fields(Parameters, {'dx', 'dy'}, caller_label);
                    cfg.dx = double(Parameters.dx);
                    cfg.dy = double(Parameters.dy);
                    if ~(isfinite(cfg.dx) && cfg.dx > 0 && isfinite(cfg.dy) && cfg.dy > 0)
                        error('CFG:InvalidSpacing', ...
                            'dx and dy must be finite positive values (%s).', caller_label);
                    end
                    cfg.Lx = cfg.dx * cfg.Nx;
                    cfg.Ly = cfg.dy * cfg.Ny;

                    if isfield(Parameters, 'Lx')
                        Lx_in = double(Parameters.Lx);
                        if isfinite(Lx_in) && Lx_in > 0 && abs(Lx_in - cfg.Lx) > 1.0e-10 * max(1, cfg.Lx)
                            error('CFG:InconsistentLx', ...
                                'spacing_driven requires Lx=dx*Nx consistency (%s).', caller_label);
                        end
                    end
                    if isfield(Parameters, 'Ly')
                        Ly_in = double(Parameters.Ly);
                        if isfinite(Ly_in) && Ly_in > 0 && abs(Ly_in - cfg.Ly) > 1.0e-10 * max(1, cfg.Ly)
                            error('CFG:InconsistentLy', ...
                                'spacing_driven requires Ly=dy*Ny consistency (%s).', caller_label);
                        end
                    end

                otherwise
                    error('CFG:InvalidGridMode', ...
                        'grid_mode must be domain_driven or spacing_driven (%s).', caller_label);
            end

            cfg.grid_mode = grid_mode;
            cfg.is_anisotropic = abs(cfg.dx - cfg.dy) > 1.0e-12 * max(cfg.dx, cfg.dy);
        end

        function cfg = copy_initial_condition(cfg, Parameters, caller_label)
            has_omega = isfield(Parameters, 'omega') && ~isempty(Parameters.omega);
            has_ic_coeff = isfield(Parameters, 'ic_coeff') && ~isempty(Parameters.ic_coeff);

            if has_omega
                cfg.omega = Parameters.omega;
            end
            if has_ic_coeff
                cfg.ic_coeff = Parameters.ic_coeff;
            end

            % Preserve the richer IC context so ICDISPATCHER can honor
            % scenario-driven and structured multi-vortex initializations.
            ic_context_fields = { ...
                'ic_scenario', 'ic_pattern', 'ic_arrangement', ...
                'ic_count', 'ic_dynamic_values', ...
                'ic_center_x', 'ic_center_y', 'ic_scale', 'ic_amplitude', ...
                'ic_multi_vortex_experimental', 'ic_multi_vortex_rows'};
            for i = 1:numel(ic_context_fields)
                key = ic_context_fields{i};
                if isfield(Parameters, key)
                    cfg.(key) = Parameters.(key);
                end
            end

            if ~(has_omega || has_ic_coeff)
                if MethodConfigBuilder.ic_type_allows_empty_coeff(cfg.ic_type)
                    % Coefficient-free catalog entries are explicitly allowed.
                    cfg.ic_coeff = [];
                    return;
                end
                error('CFG:MissingInitialConditionPayload', ...
                    'Provide omega or ic_coeff (%s).', caller_label);
            end
        end

        function tf = ic_type_allows_empty_coeff(ic_type_raw)
            ic_type = lower(strtrim(char(string(ic_type_raw))));
            ic_type = strrep(ic_type, '-', '_');
            ic_type = strrep(ic_type, ' ', '_');
            tf = any(strcmp(ic_type, {'placeholder2', 'kutz', 'no_initial_condition'}));
        end

        function cfg = copy_bc_fields(cfg, Parameters, caller_label, method_name)
            bc_fields = {'bc_case', 'bc_top', 'bc_bottom', 'bc_left', 'bc_right', ...
                         'bc_top_physical', 'bc_bottom_physical', 'bc_left_physical', 'bc_right_physical', ...
                         'bc_top_math', 'bc_bottom_math', 'bc_left_math', 'bc_right_math', ...
                         'U_top', 'U_bottom', 'U_left', 'U_right'};
            MethodConfigBuilder.require_fields(Parameters, bc_fields, caller_label);

            for i = 1:numel(bc_fields)
                key = bc_fields{i};
                cfg.(key) = Parameters.(key);
            end
            if isfield(Parameters, 'allow_preset_speed_overrides')
                cfg.allow_preset_speed_overrides = logical(Parameters.allow_preset_speed_overrides);
            end

            % Compatibility key accepted by BCDispatcher.extract_bc_case
            if isfield(Parameters, 'boundary_condition_case')
                cfg.boundary_condition_case = Parameters.boundary_condition_case;
            end

            cfg = MethodConfigBuilder.canonicalize_bc_fields(cfg, Parameters, method_name);
        end

        function cfg = canonicalize_bc_fields(cfg, Parameters, method_name)
            bc = BCDispatcher.resolve(Parameters, method_name, ...
                struct('dx', cfg.dx, 'dy', cfg.dy));
            cfg.bc_case = bc.common.case_name;
            cfg.boundary_condition_case = bc.common.case_name;

            if strcmp(bc.common.case_name, 'user_defined')
                return;
            end

            sides = {'top', 'bottom', 'left', 'right'};
            for idx = 1:numel(sides)
                sid = sides{idx};
                side = bc.common.sides.(sid);
                cfg.(['bc_' sid]) = side.display_label;
                cfg.(['bc_' sid '_math']) = side.math_type;
                cfg.(['bc_' sid '_physical']) = side.physical_type;
                cfg.(['U_' sid]) = side.U_tangent;
            end
        end

        function cfg = copy_bathymetry_fields(cfg, Parameters)
            bathy_fields = {'bathymetry_scenario', 'bathymetry_bed_slope', ...
                'bathymetry_resolution', 'bathymetry_custom_points', ...
                'bathymetry_use_dry_mask', 'bathymetry_dimension_policy'};
            for i = 1:numel(bathy_fields)
                key = bathy_fields{i};
                if isfield(Parameters, key)
                    cfg.(key) = Parameters.(key);
                end
            end
        end

        function scheme = resolve_fd_advection_scheme(Parameters)
            scheme = 'SPARSEMATRIX';

            if isfield(Parameters, 'method_config') && isstruct(Parameters.method_config) && ...
                    isfield(Parameters.method_config, 'fd') && isstruct(Parameters.method_config.fd) && ...
                    isfield(Parameters.method_config.fd, 'advection_scheme') && ...
                    ~isempty(Parameters.method_config.fd.advection_scheme)
                scheme = MethodConfigBuilder.normalize_fd_advection_scheme_token( ...
                    Parameters.method_config.fd.advection_scheme);
            elseif isfield(Parameters, 'use_arakawa') && ~isempty(Parameters.use_arakawa)
                if logical(Parameters.use_arakawa)
                    scheme = 'ARAKAWA';
                else
                    scheme = 'SPARSEMATRIX';
                end
            end
        end

        function scheme = normalize_fd_advection_scheme_token(raw_value)
            scheme = upper(char(string(raw_value)));
            scheme = strrep(scheme, '-', '_');
            scheme = strrep(scheme, ' ', '_');

            switch scheme
                case {'ARAKAWA', 'ARAKAWA_JACOBIAN'}
                    scheme = 'ARAKAWA';
                case {'SPARSEMATRIX', 'SPARSE_MATRIX', 'MATRIX', 'MATRIX_JACOBIAN'}
                    scheme = 'SPARSEMATRIX';
                otherwise
                    error('CFG:InvalidFDAdvectionScheme', ...
                        'FD advection scheme must be Arakawa or SparseMatrix (received "%s").', ...
                        char(string(raw_value)));
            end
        end

    end
end
