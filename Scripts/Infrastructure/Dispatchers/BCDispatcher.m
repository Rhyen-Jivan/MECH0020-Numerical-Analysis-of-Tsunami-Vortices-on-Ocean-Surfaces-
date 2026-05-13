classdef BCDispatcher
% BCDISPATCHER  Canonical boundary-condition source for all methods.
%
% Dispatcher contract:
%   bc = BCDispatcher.resolve(Parameters, method, grid_meta)
%
% Returns:
%   bc.common      - normalized, validated side schema
%   bc.method.fd   - FD hooks (Dirichlet-psi + Thom wall updater)
%   bc.method.spectral - transform-family capability metadata (FFT/DST/DCT)
%   bc.method.fv   - FV hooks + metadata for dispatcher-managed wall/periodic faces
%   bc.method.swe  - SWE ghost-cell hooks + metadata for wall/open/periodic faces
%   bc.capability  - support status for the requested method
%
% Side schema (bc.common.sides.<side>):
%   .id, .side_name, .kind, .math_type, .physical_type,
%   .psi_value, .dpsi_dn, .prescribed_value, .prescribed_gradient,
%   .U_tangent, .wall_speed, .periodic_partner, .requires_base_flow,
%   .target_quantity, .direction_hint, .display_label

    methods(Static)

        function bc = resolve(Parameters, varargin)
            [method_name, grid_meta] = BCDispatcher.parse_resolve_args(varargin{:});

            case_name = BCDispatcher.extract_bc_case(Parameters);
            common = BCDispatcher.build_common_from_case(case_name);
            common = BCDispatcher.apply_overrides(common, Parameters);
            common.bathymetry_scenario = BCDispatcher.extract_bathymetry_scenario(Parameters);
            common = BCDispatcher.validate_common(common);
            BCDispatcher.validate_bathymetry_compatibility(common, Parameters, method_name, grid_meta);

            bc = struct();
            bc.common = common;
            bc.method = struct();
            bc.method.fd = BCDispatcher.build_fd_payload(common, grid_meta);
            bc.method.spectral = BCDispatcher.build_spectral_payload(common, grid_meta);
            bc.method.fv = BCDispatcher.build_fv_payload(common);
            bc.method.swe = BCDispatcher.build_swe_payload(common);
            bc.capability = BCDispatcher.compute_capability(common, bc.method, method_name, Parameters);

            % Compatibility fields used by older call sites.
            bc.type = common.case_name;
            bc.description = common.description;
            bc.bc_top = common.sides.top.display_label;
            bc.bc_bottom = common.sides.bottom.display_label;
            bc.bc_left = common.sides.left.display_label;
            bc.bc_right = common.sides.right.display_label;
            bc.U_top = common.sides.top.U_tangent;
            bc.U_bottom = common.sides.bottom.U_tangent;
            bc.U_left = common.sides.left.U_tangent;
            bc.U_right = common.sides.right.U_tangent;
            bc.omega_bc = @(omega, psi, setup) bc.method.fd.apply_wall_omega(omega, psi, setup, 0.0);
        end

        function bc_case = extract_bc_case(Parameters)
            if isfield(Parameters, 'bc_case') && ~isempty(Parameters.bc_case)
                bc_case = lower(strtrim(char(string(Parameters.bc_case))));
                return;
            end
            if isfield(Parameters, 'boundary_condition_case') && ~isempty(Parameters.boundary_condition_case)
                bc_case = lower(strtrim(char(string(Parameters.boundary_condition_case))));
                return;
            end
            bc_case = 'periodic';
        end

        function scenario = extract_bathymetry_scenario(Parameters)
            if isfield(Parameters, 'bathymetry_scenario') && ~isempty(Parameters.bathymetry_scenario)
                scenario = normalize_bathymetry_scenario_token(Parameters.bathymetry_scenario);
                return;
            end
            scenario = 'flat_2d';
        end

        function tf = is_periodic(bc)
            tf = BCDispatcher.is_periodic_common(bc.common);
        end

        function tf = bathymetry_axis_is_periodic(Parameters, axis_name)
            scenario = BCDispatcher.extract_bathymetry_scenario(Parameters);
            tf = BCDispatcher.is_bathymetry_axis_periodic(Parameters, axis_name, scenario, struct());
        end

    end

    methods(Static, Access = private)

        function [method_name, grid_meta] = parse_resolve_args(varargin)
            method_name = 'fd';
            grid_meta = struct();

            if isempty(varargin)
                return;
            end

            if numel(varargin) == 1
                arg = varargin{1};
                if ischar(arg) || isstring(arg)
                    method_name = lower(char(string(arg)));
                    return;
                end
                if isstruct(arg)
                    grid_meta = arg;
                    return;
                end
                error('BCDispatcher:InvalidResolveArgs', ...
                    'Unsupported resolve arg type for single optional argument.');
            end

            arg1 = varargin{1};
            arg2 = varargin{2};

            if ischar(arg1) || isstring(arg1)
                method_name = lower(char(string(arg1)));
                if isstruct(arg2)
                    grid_meta = arg2;
                else
                    error('BCDispatcher:InvalidResolveArgs', ...
                        'When method is supplied, grid_meta must be a struct.');
                end
                return;
            end

            % Legacy compatibility: resolve(Parameters, X, Y)
            grid_meta = struct('X', arg1, 'Y', arg2);
            method_name = 'fd';
        end

        function common = build_common_from_case(case_name)
            key = BCDispatcher.normalize_case_key(case_name);
            % Boundary Condition Scenarios + Presets
            switch key
                case 'periodic'
                    common = BCDispatcher.base_common(key, ...
                        'Pure periodic boundaries on all sides.');
                    common.sides.top = BCDispatcher.make_side('top', 'periodic', 'periodic', 'periodic', 0.0, 0.0, 0.0, 'wrap');
                    common.sides.bottom = BCDispatcher.make_side('bottom', 'periodic', 'periodic', 'periodic', 0.0, 0.0, 0.0, 'wrap');
                    common.sides.left = BCDispatcher.make_side('left', 'periodic', 'periodic', 'periodic', 0.0, 0.0, 0.0, 'wrap');
                    common.sides.right = BCDispatcher.make_side('right', 'periodic', 'periodic', 'periodic', 0.0, 0.0, 0.0, 'wrap');

                case 'lid_driven_cavity'
                    common = BCDispatcher.base_common(key, ...
                        'Enclosed cavity: top wall driven, remaining walls stationary walls.');
                    common.sides.top = BCDispatcher.make_side('top', 'wall', 'dirichlet', 'driven', 0.0, 0.0, +1.0, '+x');
                    common.sides.bottom = BCDispatcher.make_side('bottom', 'wall', 'dirichlet', 'no_slip', 0.0, 0.0, 0.0, '+x');
                    common.sides.left = BCDispatcher.make_side('left', 'wall', 'dirichlet', 'no_slip', 0.0, 0.0, 0.0, '+y');
                    common.sides.right = BCDispatcher.make_side('right', 'wall', 'dirichlet', 'no_slip', 0.0, 0.0, 0.0, '+y');

                case 'driven_channel_flow'
                    common = BCDispatcher.base_common(key, ...
                        'Top driven wall, bottom stationary wall, left/right periodic.');
                    common.sides.top = BCDispatcher.make_side('top', 'wall', 'dirichlet', 'driven', 0.0, 0.0, +1.0, '+x');
                    common.sides.bottom = BCDispatcher.make_side('bottom', 'wall', 'dirichlet', 'no_slip', 0.0, 0.0, 0.0, '+x');
                    common.sides.left = BCDispatcher.make_side('left', 'periodic', 'periodic', 'periodic', 0.0, 0.0, 0.0, 'wrap');
                    common.sides.right = BCDispatcher.make_side('right', 'periodic', 'periodic', 'periodic', 0.0, 0.0, 0.0, 'wrap');

                case {'lid_and_bottom_driven_cavity', 'enclosed_shear_layer'}
                    common = BCDispatcher.base_common(key, ...
                        'Top and bottom walls driven in opposite directions; left/right stationary walls.');
                    common.sides.top = BCDispatcher.make_side('top', 'wall', 'dirichlet', 'driven', 0.0, 0.0, +1.0, '+x');
                    common.sides.bottom = BCDispatcher.make_side('bottom', 'wall', 'dirichlet', 'driven', 0.0, 0.0, -1.0, '+x');
                    common.sides.left = BCDispatcher.make_side('left', 'wall', 'dirichlet', 'no_slip', 0.0, 0.0, 0.0, '+y');
                    common.sides.right = BCDispatcher.make_side('right', 'wall', 'dirichlet', 'no_slip', 0.0, 0.0, 0.0, '+y');
                    common.case_id = 'enclosed_shear_layer';
                    common.case_name = 'enclosed_shear_layer';

                case 'enclosed_cavity'
                    common = BCDispatcher.base_common(key, ...
                        'All walls stationary with constant streamfunction.');
                    common.sides.top = BCDispatcher.make_side('top', 'wall', 'dirichlet', 'no_slip', 0.0, 0.0, 0.0, '+x');
                    common.sides.bottom = BCDispatcher.make_side('bottom', 'wall', 'dirichlet', 'no_slip', 0.0, 0.0, 0.0, '+x');
                    common.sides.left = BCDispatcher.make_side('left', 'wall', 'dirichlet', 'no_slip', 0.0, 0.0, 0.0, '+y');
                    common.sides.right = BCDispatcher.make_side('right', 'wall', 'dirichlet', 'no_slip', 0.0, 0.0, 0.0, '+y');

                case 'pinned_box'
                    common = BCDispatcher.base_common(key, ...
                        'All walls pinned with homogeneous Dirichlet boundary values.');
                    common.sides.top = BCDispatcher.make_side('top', 'wall', 'dirichlet', 'pinned', 0.0, 0.0, 0.0, '+x');
                    common.sides.bottom = BCDispatcher.make_side('bottom', 'wall', 'dirichlet', 'pinned', 0.0, 0.0, 0.0, '+x');
                    common.sides.left = BCDispatcher.make_side('left', 'wall', 'dirichlet', 'pinned', 0.0, 0.0, 0.0, '+y');
                    common.sides.right = BCDispatcher.make_side('right', 'wall', 'dirichlet', 'pinned', 0.0, 0.0, 0.0, '+y');

                case 'no_flux_box'
                    common = BCDispatcher.base_common(key, ...
                        'All walls satisfy homogeneous Neumann no-flux conditions.');
                    common.sides.top = BCDispatcher.make_side('top', 'wall', 'neumann', 'no_flux', 0.0, 0.0, 0.0, '+x');
                    common.sides.bottom = BCDispatcher.make_side('bottom', 'wall', 'neumann', 'no_flux', 0.0, 0.0, 0.0, '+x');
                    common.sides.left = BCDispatcher.make_side('left', 'wall', 'neumann', 'no_flux', 0.0, 0.0, 0.0, '+y');
                    common.sides.right = BCDispatcher.make_side('right', 'wall', 'neumann', 'no_flux', 0.0, 0.0, 0.0, '+y');

                case 'user_defined'
                    common = BCDispatcher.base_common(key, ...
                        'User-defined per-side boundary settings.');
                    common.sides.top = BCDispatcher.make_side('top', 'periodic', 'periodic', 'periodic', 0.0, 0.0, 0.0, 'wrap');
                    common.sides.bottom = BCDispatcher.make_side('bottom', 'periodic', 'periodic', 'periodic', 0.0, 0.0, 0.0, 'wrap');
                    common.sides.left = BCDispatcher.make_side('left', 'periodic', 'periodic', 'periodic', 0.0, 0.0, 0.0, 'wrap');
                    common.sides.right = BCDispatcher.make_side('right', 'periodic', 'periodic', 'periodic', 0.0, 0.0, 0.0, 'wrap');

                otherwise
                    error('BCDispatcher:UnknownBoundaryCase', ...
                        'Unsupported boundary condition case "%s" (normalized key "%s").', ...
                        char(string(case_name)), key);
            end
        end

        function common = base_common(case_name, description)
            common = struct();
            common.case_id = case_name;
            common.case_name = case_name;
            common.description = description;
            common.requires_base_flow = false;
            common.target_quantity = 'streamfunction_vorticity';
            common.sides = struct();
        end

        function side = make_side(id, kind, math_type, physical_type, psi_value, dpsi_dn, U_tangent, direction_hint)
            side = struct();
            side.id = id;
            side.side_name = id;
            side.kind = kind;
            side.math_type = math_type;
            side.physical_type = physical_type;
            side.psi_value = psi_value;
            side.dpsi_dn = dpsi_dn;
            side.prescribed_value = psi_value;
            side.prescribed_gradient = dpsi_dn;
            side.U_tangent = U_tangent;
            side.wall_speed = U_tangent;
            side.periodic_partner = BCDispatcher.default_periodic_partner(id, kind);
            side.requires_base_flow = false;
            side.target_quantity = 'streamfunction_vorticity';
            side.direction_hint = direction_hint;
            side.display_label = BCDispatcher.side_display_label(side);
        end

        function key = normalize_case_key(raw)
            key = lower(strtrim(char(string(raw))));
            key = strrep(key, '-', '_');
            key = strrep(key, ' ', '_');
            key = strrep(key, '(', '');
            key = strrep(key, ')', '');
            key = regexprep(key, '^case[\s_]*', '');
            switch key
                case '1', key = 'lid_driven_cavity';
                case '2', key = 'driven_channel_flow';
                case '3', key = 'lid_and_bottom_driven_cavity';
                case '4', key = 'enclosed_cavity';
                case '5', key = 'periodic';
                case '6', key = 'user_defined';
                case 'periodic_all', key = 'periodic';
                case {'enclosed_shear', 'enclosed_shear_layer', 'shear_layer_enclosed', 'opposed_wall_shear'}
                    key = 'enclosed_shear_layer';
                case {'pinned_box', 'dirichlet_box', 'pinned_dirichlet_box'}
                    key = 'pinned_box';
                case {'no_flux_box', 'neumann_box', 'noflux_box'}
                    key = 'no_flux_box';
            end
        end

        function common = apply_overrides(common, Parameters)
            allow_side_overrides = strcmp(common.case_name, 'user_defined');
            allow_speed_overrides = allow_side_overrides;
            if isfield(Parameters, 'bc_case') && ~isempty(Parameters.bc_case)
                allow_side_overrides = strcmp( ...
                    BCDispatcher.normalize_case_key(Parameters.bc_case), 'user_defined');
            elseif isfield(Parameters, 'boundary_condition_case') && ~isempty(Parameters.boundary_condition_case)
                allow_side_overrides = strcmp( ...
                    BCDispatcher.normalize_case_key(Parameters.boundary_condition_case), 'user_defined');
            end
            if isfield(Parameters, 'allow_preset_speed_overrides') && logical(Parameters.allow_preset_speed_overrides)
                allow_speed_overrides = true;
            else
                allow_speed_overrides = allow_side_overrides;
            end

            sides = {'top', 'bottom', 'left', 'right'};
            for i = 1:numel(sides)
                sid = sides{i};
                field_bc = ['bc_' sid];
                field_phys = ['bc_' sid '_physical'];
                field_math = ['bc_' sid '_math'];
                field_u = ['U_' sid];

                side = common.sides.(sid);
                u_was_explicitly_set = false;

                if allow_side_overrides
                    if isfield(Parameters, field_phys) && ~isempty(Parameters.(field_phys))
                        side = BCDispatcher.apply_physical_token(side, Parameters.(field_phys));
                    end
                    if isfield(Parameters, field_math) && ~isempty(Parameters.(field_math))
                        side = BCDispatcher.apply_math_token(side, Parameters.(field_math));
                    end
                    if isfield(Parameters, field_bc) && ~isempty(Parameters.(field_bc))
                        % The user-facing side token is the canonical override and must win over
                        % any stale cached math/physical side metadata carried in saved configs.
                        side = BCDispatcher.apply_side_token(side, Parameters.(field_bc));
                    end
                end

                if allow_speed_overrides && isfield(Parameters, field_u) && ~isempty(Parameters.(field_u))
                    side.U_tangent = double(Parameters.(field_u));
                    u_was_explicitly_set = true;
                end

                if u_was_explicitly_set && strcmp(side.physical_type, 'no_slip') && abs(side.U_tangent) > 1.0e-12
                    error('BCDispatcher:NoSlipWithMotion', ...
                        'No-slip side ''%s'' cannot carry nonzero wall speed.', sid);
                end
                if u_was_explicitly_set && strcmp(side.kind, 'periodic') && abs(side.U_tangent) > 1.0e-12
                    error('BCDispatcher:PeriodicWithMotion', ...
                        'Side ''%s'' is periodic and cannot have wall motion.', sid);
                end

                if strcmp(side.physical_type, 'no_slip')
                    side.U_tangent = 0.0;
                end
                if strcmp(side.kind, 'periodic')
                    side.U_tangent = 0.0;
                    side.psi_value = 0.0;
                    side.dpsi_dn = 0.0;
                end

                side = BCDispatcher.sync_side_contract_fields(side);
                side.display_label = BCDispatcher.side_display_label(side);
                common.sides.(sid) = side;
            end

            if isfield(Parameters, 'bc_case') && ~isempty(Parameters.bc_case)
                common.case_name = BCDispatcher.normalize_case_key(Parameters.bc_case);
            elseif isfield(Parameters, 'boundary_condition_case') && ~isempty(Parameters.boundary_condition_case)
                common.case_name = BCDispatcher.normalize_case_key(Parameters.boundary_condition_case);
            end
        end

        function side = apply_side_token(side, token_raw)
            token = BCDispatcher.normalize_boundary_token_key(token_raw);

            switch token
                case {'periodic', 'periodic_all'}
                    side.kind = 'periodic';
                    side.math_type = 'periodic';
                    side.physical_type = 'periodic';
                case {'pinned', 'pinned_dirichlet'}
                    side.kind = 'wall';
                    side.math_type = 'dirichlet';
                    side.physical_type = 'pinned';
                case {'dirichlet', 'no_slip', 'noslip', 'stationary', 'no_slip_dirichlet', ...
                        'no_slip_wall', 'no_slip_wall_psi_const', 'stationary_wall', 'stationary_wall_psi_const'}
                    side.kind = 'wall';
                    side.math_type = 'dirichlet';
                    side.physical_type = 'no_slip';
                case {'no_flux', 'no_flux_neumann', 'noflux', 'neumann'}
                    side.kind = 'wall';
                    side.math_type = 'neumann';
                    side.physical_type = 'no_flux';
                case {'driven', 'driven_neumann', 'driven_wall', ...
                        'driven_wall_psi_const_wall_speed', 'moving_wall'}
                    side.kind = 'wall';
                    side.math_type = 'dirichlet';
                    side.physical_type = 'driven';
                case {'open_absorbing', 'open', 'absorbing'}
                    side.kind = 'open';
                    side.math_type = 'radiation';
                    side.physical_type = 'open_absorbing';
                otherwise
                    error('BCDispatcher:UnknownBoundaryToken', ...
                        'Unknown boundary token ''%s''.', char(string(token_raw)));
            end
        end

        function side = apply_physical_token(side, token_raw)
            token = BCDispatcher.normalize_boundary_token_key(token_raw);

            switch token
                case 'periodic'
                    side.kind = 'periodic';
                    side.physical_type = 'periodic';
                    side.math_type = 'periodic';
                case {'no_slip', 'noslip', 'stationary', 'no_slip_wall', 'stationary_wall'}
                    side.kind = 'wall';
                    side.physical_type = 'no_slip';
                    side.math_type = 'dirichlet';
                case 'pinned'
                    side.kind = 'wall';
                    side.physical_type = 'pinned';
                    side.math_type = 'dirichlet';
                case {'no_flux', 'noflux'}
                    side.kind = 'wall';
                    side.physical_type = 'no_flux';
                    side.math_type = 'neumann';
                case {'driven', 'driven_wall', 'moving_wall'}
                    side.kind = 'wall';
                    side.physical_type = 'driven';
                    side.math_type = 'dirichlet';
                case {'open_absorbing', 'open', 'absorbing'}
                    side.kind = 'open';
                    side.physical_type = 'open_absorbing';
                    side.math_type = 'radiation';
                otherwise
                    error('BCDispatcher:UnknownPhysicalToken', ...
                        'Unknown physical boundary token ''%s''.', char(string(token_raw)));
            end
        end

        function side = apply_math_token(side, token_raw)
            token = BCDispatcher.normalize_boundary_token_key(token_raw);

            switch token
                case 'periodic'
                    side.kind = 'periodic';
                    side.math_type = 'periodic';
                    side.physical_type = 'periodic';
                case 'dirichlet'
                    side.kind = 'wall';
                    side.math_type = 'dirichlet';
                    if strcmp(side.physical_type, 'periodic')
                        side.physical_type = 'no_slip';
                    end
                case 'neumann'
                    side.kind = 'wall';
                    side.math_type = 'neumann';
                    if strcmp(side.physical_type, 'periodic')
                        side.physical_type = 'no_flux';
                    elseif strcmp(side.physical_type, 'driven')
                        side.math_type = 'dirichlet';
                    end
                case {'radiation', 'open', 'open_absorbing', 'absorbing'}
                    side.kind = 'open';
                    side.math_type = 'radiation';
                    side.physical_type = 'open_absorbing';
                otherwise
                    error('BCDispatcher:UnknownMathToken', ...
                        'Unknown mathematical boundary token ''%s''.', char(string(token_raw)));
            end
        end

        function common = validate_common(common)
            sides = {'top', 'bottom', 'left', 'right'};
            for i = 1:numel(sides)
                sid = sides{i};
                side = common.sides.(sid);

                if strcmp(side.kind, 'periodic')
                    if ~strcmp(side.math_type, 'periodic') || ~strcmp(side.physical_type, 'periodic')
                        error('BCDispatcher:InvalidPeriodicMix', ...
                            'Side ''%s'' is periodic but has non-periodic semantics.', sid);
                    end
                    if abs(side.U_tangent) > 1.0e-12
                        error('BCDispatcher:PeriodicWithMotion', ...
                            'Side ''%s'' is periodic and cannot have wall motion.', sid);
                    end
                elseif strcmp(side.kind, 'open')
                    if ~strcmp(side.math_type, 'radiation') || ~strcmp(side.physical_type, 'open_absorbing')
                        error('BCDispatcher:InvalidOpenMix', ...
                            'Side ''%s'' is open but has inconsistent open-boundary semantics.', sid);
                    end
                    if abs(side.U_tangent) > 1.0e-12
                        error('BCDispatcher:OpenWithWallMotion', ...
                            'Open side ''%s'' cannot carry tangential wall motion.', sid);
                    end
                else
                    if strcmp(side.math_type, 'periodic')
                        error('BCDispatcher:InvalidWallMath', ...
                            'Wall side ''%s'' cannot use periodic math type.', sid);
                    end
                    if strcmp(side.physical_type, 'periodic')
                        error('BCDispatcher:InvalidWallPhysical', ...
                            'Wall side ''%s'' cannot use periodic physical type.', sid);
                    end
                    if any(strcmp(side.physical_type, {'no_slip', 'driven', 'pinned'})) && ...
                            ~strcmp(side.math_type, 'dirichlet')
                        error('BCDispatcher:InvalidDirichletWallMix', ...
                            'Wall side ''%s'' with physical type ''%s'' must use Dirichlet streamfunction semantics.', ...
                            sid, side.physical_type);
                    end
                    if strcmp(side.physical_type, 'no_flux') && ~strcmp(side.math_type, 'neumann')
                        error('BCDispatcher:InvalidNeumannWallMix', ...
                            'Wall side ''%s'' with physical type ''no_flux'' must use Neumann streamfunction semantics.', sid);
                    end
                end

                if strcmp(side.physical_type, 'no_slip') && abs(side.U_tangent) > 1.0e-12
                    error('BCDispatcher:NoSlipWithMotion', ...
                        'No-slip side ''%s'' cannot carry nonzero wall speed.', sid);
                end

                side = BCDispatcher.sync_side_contract_fields(side);
                common.sides.(sid) = side;
                common.sides.(sid).display_label = BCDispatcher.side_display_label(common.sides.(sid));
            end

            % Axis consistency for periodic boundaries
            if xor(strcmp(common.sides.left.kind, 'periodic'), strcmp(common.sides.right.kind, 'periodic'))
                error('BCDispatcher:InconsistentPeriodicX', ...
                    'Left/right periodicity must match.');
            end
            if xor(strcmp(common.sides.top.kind, 'periodic'), strcmp(common.sides.bottom.kind, 'periodic'))
                error('BCDispatcher:InconsistentPeriodicY', ...
                    'Top/bottom periodicity must match.');
            end

            if strcmp(common.sides.left.kind, 'periodic') && strcmp(common.sides.right.kind, 'periodic')
                common.sides.left.periodic_partner = 'right';
                common.sides.right.periodic_partner = 'left';
            end
            if strcmp(common.sides.bottom.kind, 'periodic') && strcmp(common.sides.top.kind, 'periodic')
                common.sides.bottom.periodic_partner = 'top';
                common.sides.top.periodic_partner = 'bottom';
            end
        end

        function validate_bathymetry_compatibility(common, Parameters, method_name, grid_meta)
            scenario = common.bathymetry_scenario;
            if ~bathymetry_blocks_periodic_bc(scenario)
                return;
            end

            method_name = lower(strtrim(char(string(method_name))));
            method_name = strrep(method_name, '-', '_');
            method_name = strrep(method_name, ' ', '_');
            if any(strcmp(method_name, {'spectral', 'fft', 'pseudo_spectral', 'pseudospectral'}))
                return;
            end

            periodic_x = strcmp(common.sides.left.kind, 'periodic') && strcmp(common.sides.right.kind, 'periodic');
            periodic_y = strcmp(common.sides.bottom.kind, 'periodic') && strcmp(common.sides.top.kind, 'periodic');
            if ~periodic_x && ~periodic_y
                return;
            end

            if periodic_y
                error('BCDispatcher:BathymetryPeriodicConflict', ...
                    ['Non-flat bathymetry in the active structured-grid bathymetry path supports wall-bounded y only. ' ...
                     'Periodic top/bottom boundaries are unsupported.']);
            end

            if periodic_x && ~BCDispatcher.is_bathymetry_axis_periodic(Parameters, 'x', scenario, grid_meta)
                error('BCDispatcher:BathymetryPeriodicConflict', ...
                    ['Bathymetry scenario "%s" is not periodic in x, so periodic left/right boundaries ' ...
                     'cannot be applied without a discontinuous wrap.'], scenario);
            end
        end

        function payload = build_fd_payload(common, grid_meta)
            payload = struct();
            payload.adapter_name = 'fd_vsf';
            payload.is_periodic = BCDispatcher.is_periodic_common(common);
            payload.periodic_x = strcmp(common.sides.left.kind, 'periodic') && strcmp(common.sides.right.kind, 'periodic');
            payload.periodic_y = strcmp(common.sides.bottom.kind, 'periodic') && strcmp(common.sides.top.kind, 'periodic');
            payload.operator_mode = BCDispatcher.resolve_fd_operator_mode(payload.periodic_x, payload.periodic_y);
            payload.wall_model = 'dirichlet_psi_plus_thom';
            payload.supports_no_slip_walls = true;
            payload.supports_moving_walls = true;
            payload.supports_streamfunction_neumann_walls = false;
            payload.psi_boundary = BCDispatcher.build_psi_boundary(common);

            switch payload.operator_mode
                case 'periodic_periodic'
                    payload.poisson_bc_mode = 'periodic';
                    payload.apply_wall_omega = @(omega, ~, ~, ~) omega;
                case 'periodic_x_dirichlet_y'
                    payload.poisson_bc_mode = 'periodic_x_dirichlet_y';
                    payload.apply_wall_omega = @(omega, psi, setup, t) BCDispatcher.apply_fd_wall_omega( ...
                        omega, psi, setup, t, common);
                case 'dirichlet_x_periodic_y'
                    payload.poisson_bc_mode = 'dirichlet_x_periodic_y';
                    payload.apply_wall_omega = @(omega, psi, setup, t) BCDispatcher.apply_fd_wall_omega( ...
                        omega, psi, setup, t, common);
                otherwise
                    payload.poisson_bc_mode = 'wall_dirichlet_psi';
                    payload.apply_wall_omega = @(omega, psi, setup, t) BCDispatcher.apply_fd_wall_omega( ...
                        omega, psi, setup, t, common);
            end
            payload.enforce_velocity_bc = @(u, v, setup) BCDispatcher.enforce_fd_velocity_bc(u, v, setup, common);

            payload.grid_meta = grid_meta;
        end

        function payload = build_spectral_payload(common, grid_meta)
            [supported_by_transform_family, transform_reason, axis_x, axis_y] = ...
                BCDispatcher.resolve_spectral_axis_payloads(common);
            [requires_lifting, lifting_model, lifting_payload] = ...
                BCDispatcher.resolve_spectral_lifting_model(common, grid_meta, axis_x, axis_y);
            requires_wall_closure = logical(axis_x.requires_wall_closure || axis_y.requires_wall_closure);
            [requires_bathymetry_penalization, bathymetry_model, bathymetry_payload] = ...
                BCDispatcher.resolve_spectral_bathymetry_payload(common, grid_meta);
            wall_model = 'none';
            if requires_wall_closure
                wall_model = 'lifted_zero_tangential_thom_like';
            end
            [supported_by_current_solver, solver_reason] = BCDispatcher.resolve_spectral_solver_support( ...
                common, axis_x, axis_y, requires_lifting, lifting_model, requires_wall_closure, ...
                requires_bathymetry_penalization, bathymetry_model);

            payload = struct();
            payload.supports_nonperiodic = true;
            payload.is_periodic = strcmp(axis_x.family, 'fft') && strcmp(axis_y.family, 'fft');
            payload.supported_by_transform_family = logical(supported_by_transform_family);
            payload.supported_by_current_solver = logical(supported_by_transform_family && supported_by_current_solver);
            payload.supported = logical(payload.supported_by_transform_family && payload.supported_by_current_solver);
            payload.reason = '';
            if ~payload.supported_by_transform_family
                payload.reason = transform_reason;
            elseif ~payload.supported_by_current_solver
                payload.reason = solver_reason;
            end
            payload.axis_x = axis_x;
            payload.axis_y = axis_y;
            payload.sides = common.sides;
            payload.transform_mode = sprintf('%s_x__%s_y', axis_x.family, axis_y.family);
            payload.modal_refinement_supported = payload.is_periodic;
            payload.requires_transforms = unique([{axis_x.required_functions{:}}, {axis_y.required_functions{:}}], 'stable');
            payload.requires_lifting = logical(requires_lifting);
            payload.lifting_model = char(string(lifting_model));
            payload.lifting_payload = lifting_payload;
            payload.requires_wall_closure = logical(requires_wall_closure);
            payload.wall_model = wall_model;
            payload.supports_shaped_bathymetry = logical(strcmpi(char(string(common.bathymetry_scenario)), 'flat_2d') || ...
                requires_bathymetry_penalization);
            payload.requires_bathymetry_penalization = logical(requires_bathymetry_penalization);
            payload.bathymetry_model = char(string(bathymetry_model));
            payload.bathymetry_payload = bathymetry_payload;
            payload.fluctuation_contract = struct( ...
                'field_basis', 'homogeneous_after_lifting', ...
                'psi_boundary', 'homogeneous', ...
                'wall_speed_mode', 'zero_tangential_after_lifting', ...
                'omega_closure', wall_model, ...
                'profile_mode', char(string(lifting_payload.boundary_profile)), ...
                'bathymetry_mode', char(string(bathymetry_model)));
        end

        function payload = build_fv_payload(common)
            payload = struct();
            payload.adapter_name = 'fv_vsf';
            payload.supports_nonperiodic_xy = true;
            payload.volume_face_policy = 'dispatcher_common_sides';
            payload.is_periodic = BCDispatcher.is_periodic_common(common);
            payload.periodic_x = strcmp(common.sides.left.kind, 'periodic') && strcmp(common.sides.right.kind, 'periodic');
            payload.periodic_y = strcmp(common.sides.bottom.kind, 'periodic') && strcmp(common.sides.top.kind, 'periodic');
            payload.wall_model = 'dirichlet_psi_plus_thom';
            payload.supports_no_slip_walls = true;
            payload.supports_moving_walls = true;
            payload.supports_streamfunction_neumann_walls = false;
            payload.psi_boundary = BCDispatcher.build_psi_boundary(common);
            payload.wall_sides = struct( ...
                'top', strcmp(common.sides.top.kind, 'wall'), ...
                'bottom', strcmp(common.sides.bottom.kind, 'wall'), ...
                'left', strcmp(common.sides.left.kind, 'wall'), ...
                'right', strcmp(common.sides.right.kind, 'wall'));
            payload.apply_wall_omega = @(omega3d, psi3d, setup) BCDispatcher.apply_fv_wall_omega( ...
                omega3d, psi3d, setup, common);
            payload.enforce_velocity_bc = @(u3d, v3d, setup) BCDispatcher.enforce_fv_velocity_bc( ...
                u3d, v3d, setup, common);
        end

        function payload = build_swe_payload(common)
            payload = struct();
            payload.supports_periodic = true;
            payload.supports_reflective_wall = true;
            payload.supports_open_absorbing = true;
            payload.is_periodic = BCDispatcher.is_periodic_common(common);
            payload.apply_ghost_cells = @(h, hu, hv, setup) BCDispatcher.apply_swe_ghost_cells( ...
                h, hu, hv, setup, common);
        end

        function capability = compute_capability(common, method_payloads, method_name, Parameters)
            method_name = lower(strtrim(char(string(method_name))));
            periodic = BCDispatcher.is_periodic_common(common);
            has_open = any(strcmp({common.sides.top.kind, common.sides.bottom.kind, ...
                common.sides.left.kind, common.sides.right.kind}, 'open'));
            has_driven = any(strcmp({common.sides.top.physical_type, common.sides.bottom.physical_type, ...
                common.sides.left.physical_type, common.sides.right.physical_type}, 'driven'));
            has_base_flow_request = BCDispatcher.has_base_flow_request(Parameters);

            capability = struct();
            capability.supported = true;
            capability.reason = '';
            capability.fallback_allowed = false;

            switch method_name
                case {'fd', 'finite_difference', 'finite difference'}
                    capability.supported = true;
                    if has_open
                        capability.supported = false;
                        capability.reason = 'FD does not support open/absorbing boundaries in the dispatcher BC model.';
                    elseif any(strcmp({common.sides.top.physical_type, common.sides.bottom.physical_type, ...
                            common.sides.left.physical_type, common.sides.right.physical_type}, 'no_flux'))
                        capability.supported = false;
                        capability.reason = ['FD wall boundaries use constant streamfunction plus Thom wall-vorticity ' ...
                            'closure; pure no-flux streamfunction walls are unsupported.'];
                    elseif has_base_flow_request
                        capability.supported = false;
                        capability.reason = ['FD channel-like runs that require imposed mean throughflow or a base-flow ' ...
                            'decomposition are unsupported in the active VSF wall model.'];
                    end
                case {'spectral', 'fft', 'pseudo_spectral', 'pseudospectral'}
                    capability.supported = method_payloads.spectral.supported;
                    capability.reason = method_payloads.spectral.reason;
                case {'fv', 'finite_volume', 'finite volume', 'finitevolume'}
                    capability.supported = true;
                    if has_open
                        capability.supported = false;
                        capability.reason = 'Finite Volume vorticity mode does not support open/absorbing boundaries.';
                    elseif any(strcmp({common.sides.top.physical_type, common.sides.bottom.physical_type, ...
                            common.sides.left.physical_type, common.sides.right.physical_type}, 'no_flux'))
                        capability.supported = false;
                        capability.reason = ['Finite Volume wall boundaries use constant streamfunction plus wall-vorticity ' ...
                            'closure; pure no-flux streamfunction walls are unsupported.'];
                    elseif has_base_flow_request
                        capability.supported = false;
                        capability.reason = ['Finite Volume channel-like runs that require imposed mean throughflow or ' ...
                            'a base-flow decomposition are unsupported in the active VSF solver family.'];
                    end
                case {'swe', 'shallow_water', 'shallow water', 'shallowwater'}
                    capability.supported = ~has_driven;
                    if has_driven
                        capability.reason = 'Shallow Water phase 1 supports periodic, reflective wall, and open-absorbing boundaries only.';
                    end
                otherwise
                    capability.supported = false;
                    capability.reason = sprintf('Unknown method token: %s', method_name);
            end
        end

        function tf = is_periodic_common(common)
            tf = strcmp(common.sides.top.kind, 'periodic') && ...
                 strcmp(common.sides.bottom.kind, 'periodic') && ...
                 strcmp(common.sides.left.kind, 'periodic') && ...
                 strcmp(common.sides.right.kind, 'periodic');
        end

        function operator_mode = resolve_fd_operator_mode(periodic_x, periodic_y)
            if periodic_x && periodic_y
                operator_mode = 'periodic_periodic';
            elseif periodic_x
                operator_mode = 'periodic_x_dirichlet_y';
            elseif periodic_y
                operator_mode = 'dirichlet_x_periodic_y';
            else
                operator_mode = 'dirichlet_dirichlet';
            end
        end

        function omega = apply_fd_wall_omega(omega, psi, setup, ~, common)
            % Second-order wall-vorticity closure for FD streamfunction walls.
            [dx, dy] = BCDispatcher.resolve_spacing(setup);
            geometry = BCDispatcher.resolve_bathymetry_geometry(setup);
            if geometry.enabled && strcmpi(char(string(geometry.dimension)), '2d')
                omega = BCDispatcher.apply_fd_bathymetry_wall_omega(omega, psi, setup, common, geometry, dx, dy);
                return;
            end

            Ny = size(omega, 1);
            Nx = size(omega, 2);
            if Ny < 2 || Nx < 2
                return;
            end

            % Top wall (row end)
            st = common.sides.top;
            if strcmp(st.kind, 'wall')
                omega(end, 2:Nx-1) = 3 * (psi(end-1, 2:Nx-1) - st.psi_value) / (dy^2) ...
                    - 0.5 * omega(end-1, 2:Nx-1) ...
                    - 3 * st.U_tangent / dy;
            end

            % Bottom wall (row 1)
            sb = common.sides.bottom;
            if strcmp(sb.kind, 'wall')
                omega(1, 2:Nx-1) = 3 * (psi(2, 2:Nx-1) - sb.psi_value) / (dy^2) ...
                    - 0.5 * omega(2, 2:Nx-1) ...
                    + 3 * sb.U_tangent / dy;
            end

            % Left wall (col 1)
            sl = common.sides.left;
            if strcmp(sl.kind, 'wall')
                omega(2:Ny-1, 1) = 3 * (psi(2:Ny-1, 2) - sl.psi_value) / (dx^2) ...
                    - 0.5 * omega(2:Ny-1, 2) ...
                    - 3 * sl.U_tangent / dx;
            end

            % Right wall (col end)
            sr = common.sides.right;
            if strcmp(sr.kind, 'wall')
                omega(2:Ny-1, end) = 3 * (psi(2:Ny-1, end-1) - sr.psi_value) / (dx^2) ...
                    - 0.5 * omega(2:Ny-1, end-1) ...
                    + 3 * sr.U_tangent / dx;
            end

            omega = BCDispatcher.update_corners(omega, common);
        end

        function omega = apply_fd_bathymetry_wall_omega(omega, psi, setup, common, geometry, dx, dy)
            Ny = size(omega, 1);
            Nx = size(omega, 2);
            if Ny < 2 || Nx < 2
                return;
            end

            omega(~geometry.wet_mask) = 0;

            st = common.sides.top;
            if strcmp(st.kind, 'wall')
                cols = 2:max(2, Nx - 1);
                cols = cols(geometry.wet_mask(end, cols));
                if ~isempty(cols)
                    omega(end, cols) = 3 * (psi(end - 1, cols) - st.psi_value) / (dy^2) ...
                        - 0.5 * omega(end - 1, cols) ...
                        - 3 * st.U_tangent / dy;
                end
            end

            sb = common.sides.bottom;
            if strcmp(sb.kind, 'wall')
                for col = 1:Nx
                    row = geometry.first_wet_row(col);
                    if row <= 0 || row > Ny || ~geometry.wet_mask(row, col)
                        continue;
                    end
                    if ~isfield(geometry, 'first_fluid_valid') || ~geometry.first_fluid_valid(col)
                        continue;
                    end
                    interior_row = geometry.first_fluid_row(col);
                    if interior_row <= row || interior_row > Ny || ~geometry.fluid_mask(interior_row, col)
                        continue;
                    end
                    h = geometry.first_fluid_distance(col);
                    u_eff = double(sb.U_tangent) * double(geometry.bottom_drive_scale(col));
                    omega(row, col) = 3 * (psi(interior_row, col) - sb.psi_value) / (h^2) ...
                        - 0.5 * omega(interior_row, col) ...
                        + 3 * u_eff / h;
                end
            end

            sl = common.sides.left;
            if strcmp(sl.kind, 'wall')
                rows = find(geometry.wet_mask(:, 1));
                rows = rows(rows > 1 & rows < Ny);
                if isfield(geometry, 'first_fluid_valid') && ~isempty(geometry.first_fluid_valid) && ...
                        ~geometry.first_fluid_valid(1)
                    rows = rows(rows ~= geometry.first_wet_row(1));
                end
                if ~isempty(rows)
                    omega(rows, 1) = 3 * (psi(rows, 2) - sl.psi_value) / (dx^2) ...
                        - 0.5 * omega(rows, 2) ...
                        - 3 * sl.U_tangent / dx;
                end
            end

            sr = common.sides.right;
            if strcmp(sr.kind, 'wall')
                rows = find(geometry.wet_mask(:, end));
                rows = rows(rows > 1 & rows < Ny);
                if isfield(geometry, 'first_fluid_valid') && ~isempty(geometry.first_fluid_valid) && ...
                        ~geometry.first_fluid_valid(end)
                    rows = rows(rows ~= geometry.first_wet_row(end));
                end
                if ~isempty(rows)
                    omega(rows, end) = 3 * (psi(rows, end - 1) - sr.psi_value) / (dx^2) ...
                        - 0.5 * omega(rows, end - 1) ...
                        + 3 * sr.U_tangent / dx;
                end
            end

            omega(~geometry.wet_mask) = 0;
        end

        function omega = update_corners(omega, common)
            Ny = size(omega, 1);
            Nx = size(omega, 2);
            if Ny < 2 || Nx < 2
                return;
            end

            omega(1, 1) = BCDispatcher.average_corner_closures(omega(1, min(2, Nx)), omega(min(2, Ny), 1), ...
                common.sides.bottom, common.sides.left, omega(1, 1));
            omega(1, end) = BCDispatcher.average_corner_closures(omega(1, max(1, Nx - 1)), omega(min(2, Ny), end), ...
                common.sides.bottom, common.sides.right, omega(1, end));
            omega(end, 1) = BCDispatcher.average_corner_closures(omega(end, min(2, Nx)), omega(max(1, Ny - 1), 1), ...
                common.sides.top, common.sides.left, omega(end, 1));
            omega(end, end) = BCDispatcher.average_corner_closures(omega(end, max(1, Nx - 1)), omega(max(1, Ny - 1), end), ...
                common.sides.top, common.sides.right, omega(end, end));
        end

        function psi_boundary = build_psi_boundary(common)
            psi_boundary = struct();
            psi_boundary.top = common.sides.top.prescribed_value;
            psi_boundary.bottom = common.sides.bottom.prescribed_value;
            psi_boundary.left = common.sides.left.prescribed_value;
            psi_boundary.right = common.sides.right.prescribed_value;
        end

        function [velocity_u, velocity_v] = enforce_fd_velocity_bc(velocity_u, velocity_v, setup, common)
            if ~isfield(setup, 'is_periodic_bc') || logical(setup.is_periodic_bc)
                return;
            end

            sides = common.sides;
            has_bathymetry_bottom = isfield(setup, 'bathymetry_geometry') && isstruct(setup.bathymetry_geometry) && ...
                isfield(setup.bathymetry_geometry, 'enabled') && logical(setup.bathymetry_geometry.enabled) && ...
                isfield(setup.bathymetry_geometry, 'dimension') && ...
                strcmpi(char(string(setup.bathymetry_geometry.dimension)), '2d');

            velocity_u(1, :) = 0;
            velocity_u(end, :) = 0;
            velocity_v(:, 1) = 0;
            velocity_v(:, end) = 0;

            if isfield(sides, 'top') && strcmp(sides.top.kind, 'wall')
                velocity_u(end, :) = BCDispatcher.resolve_wall_tangent_speed(sides.top);
                velocity_v(end, :) = 0;
            end
            if isfield(sides, 'bottom') && strcmp(sides.bottom.kind, 'wall')
                if has_bathymetry_bottom
                    geometry = setup.bathymetry_geometry;
                    boundary_rows = geometry.first_wet_row;
                    drive_u = BCDispatcher.resolve_wall_tangent_speed(sides.bottom) * geometry.bottom_drive_u;
                    drive_v = BCDispatcher.resolve_wall_tangent_speed(sides.bottom) * geometry.bottom_drive_v;
                    for col = 1:numel(boundary_rows)
                        row = boundary_rows(col);
                        if row >= 1 && row <= size(velocity_u, 1) && geometry.wet_mask(row, col)
                            velocity_u(row, col) = drive_u(col);
                            velocity_v(row, col) = drive_v(col);
                        end
                    end
                else
                    velocity_u(1, :) = BCDispatcher.resolve_wall_tangent_speed(sides.bottom);
                    velocity_v(1, :) = 0;
                end
            end
            if isfield(sides, 'left') && strcmp(sides.left.kind, 'wall')
                velocity_u(:, 1) = 0;
                velocity_v(:, 1) = BCDispatcher.resolve_wall_tangent_speed(sides.left);
            end
            if isfield(sides, 'right') && strcmp(sides.right.kind, 'wall')
                velocity_u(:, end) = 0;
                velocity_v(:, end) = BCDispatcher.resolve_wall_tangent_speed(sides.right);
            end

            if isfield(sides, 'top') && strcmp(sides.top.kind, 'wall') && strcmp(sides.top.physical_type, 'driven')
                velocity_u(end, :) = BCDispatcher.resolve_wall_tangent_speed(sides.top);
                velocity_v(end, :) = 0;
            end
            if isfield(sides, 'bottom') && strcmp(sides.bottom.kind, 'wall') && ...
                    strcmp(sides.bottom.physical_type, 'driven')
                if has_bathymetry_bottom
                    geometry = setup.bathymetry_geometry;
                    boundary_rows = geometry.first_wet_row;
                    drive_u = BCDispatcher.resolve_wall_tangent_speed(sides.bottom) * geometry.bottom_drive_u;
                    drive_v = BCDispatcher.resolve_wall_tangent_speed(sides.bottom) * geometry.bottom_drive_v;
                    for col = 1:numel(boundary_rows)
                        row = boundary_rows(col);
                        if row >= 1 && row <= size(velocity_u, 1) && geometry.wet_mask(row, col)
                            velocity_u(row, col) = drive_u(col);
                            velocity_v(row, col) = drive_v(col);
                        end
                    end
                else
                    velocity_u(1, :) = BCDispatcher.resolve_wall_tangent_speed(sides.bottom);
                    velocity_v(1, :) = 0;
                end
            end

            if has_bathymetry_bottom && isfield(setup.bathymetry_geometry, 'solid_mask')
                velocity_u(setup.bathymetry_geometry.solid_mask) = 0;
                velocity_v(setup.bathymetry_geometry.solid_mask) = 0;
            end
        end

        function omega3d = apply_fv_wall_omega(omega3d, psi3d, setup, common)
            wet_mask = BCDispatcher.resolve_fv_wet_mask3d(setup);
            sides = common.sides;
            omega3d(~wet_mask) = 0;

            if strcmp(sides.top.kind, 'wall')
                cols = 2:max(2, setup.Nx - 1);
                for k = 1:setup.Nz
                    active_cols = cols(squeeze(wet_mask(end, cols, k)));
                    if isempty(active_cols), continue; end
                    omega3d(end, active_cols, k) = -2 * (psi3d(end - 1, active_cols, k) - sides.top.psi_value) / (setup.dy^2) ...
                        - 2 * BCDispatcher.resolve_wall_tangent_speed(sides.top) / setup.dy;
                end
            end

            if strcmp(sides.bottom.kind, 'wall')
                cols = 2:max(2, setup.Nx - 1);
                for k = 1:setup.Nz
                    active_cols = cols(squeeze(wet_mask(1, cols, k)));
                    if isempty(active_cols), continue; end
                    omega3d(1, active_cols, k) = -2 * (psi3d(2, active_cols, k) - sides.bottom.psi_value) / (setup.dy^2) ...
                        + 2 * BCDispatcher.resolve_wall_tangent_speed(sides.bottom) / setup.dy;
                end
            end

            if strcmp(sides.left.kind, 'wall')
                rows = 2:max(2, setup.Ny - 1);
                for k = 1:setup.Nz
                    active_rows = rows(squeeze(wet_mask(rows, 1, k)));
                    if isempty(active_rows), continue; end
                    omega3d(active_rows, 1, k) = -2 * (psi3d(active_rows, 2, k) - sides.left.psi_value) / (setup.dx^2) ...
                        + 2 * BCDispatcher.resolve_wall_tangent_speed(sides.left) / setup.dx;
                end
            end

            if strcmp(sides.right.kind, 'wall')
                rows = 2:max(2, setup.Ny - 1);
                for k = 1:setup.Nz
                    active_rows = rows(squeeze(wet_mask(rows, end, k)));
                    if isempty(active_rows), continue; end
                    omega3d(active_rows, end, k) = -2 * (psi3d(active_rows, end - 1, k) - sides.right.psi_value) / (setup.dx^2) ...
                        - 2 * BCDispatcher.resolve_wall_tangent_speed(sides.right) / setup.dx;
                end
            end

            if isfield(setup, 'bathymetry_geometry') && isstruct(setup.bathymetry_geometry) && ...
                    isfield(setup.bathymetry_geometry, 'enabled') && setup.bathymetry_geometry.enabled
                geometry = setup.bathymetry_geometry;
                first_wet_k = geometry.first_wet_k;
                for row = 1:setup.Ny
                    for col = 1:setup.Nx
                        k = first_wet_k(row, col);
                        if k <= 0 || k >= setup.Nz || ~wet_mask(row, col, k)
                            continue;
                        end
                        h = max(setup.z(k + 1) - geometry.floor_height(row, col), 0.35 * setup.dz);
                        u_eff = BCDispatcher.resolve_wall_tangent_speed(sides.bottom) * geometry.bottom_drive_scale(row, col);
                        omega3d(row, col, k) = -2 * (psi3d(row, col, k + 1) - sides.bottom.psi_value) / (h^2) ...
                            + 2 * u_eff / h;
                    end
                end
            end

            omega3d(~wet_mask) = 0;
        end

        function [u3d, v3d] = enforce_fv_velocity_bc(u3d, v3d, setup, common)
            wet_mask = BCDispatcher.resolve_fv_wet_mask3d(setup);
            sides = common.sides;
            u3d(~wet_mask) = 0;
            v3d(~wet_mask) = 0;

            if strcmp(sides.top.kind, 'wall')
                u3d(end, :, :) = BCDispatcher.resolve_wall_tangent_speed(sides.top);
                v3d(end, :, :) = 0;
            end
            if strcmp(sides.bottom.kind, 'wall')
                u3d(1, :, :) = BCDispatcher.resolve_wall_tangent_speed(sides.bottom);
                v3d(1, :, :) = 0;
            end
            if strcmp(sides.left.kind, 'wall')
                u3d(:, 1, :) = 0;
                v3d(:, 1, :) = BCDispatcher.resolve_wall_tangent_speed(sides.left);
            end
            if strcmp(sides.right.kind, 'wall')
                u3d(:, end, :) = 0;
                v3d(:, end, :) = BCDispatcher.resolve_wall_tangent_speed(sides.right);
            end

            if isfield(setup, 'bathymetry_geometry') && isstruct(setup.bathymetry_geometry) && ...
                    isfield(setup.bathymetry_geometry, 'enabled') && setup.bathymetry_geometry.enabled
                geometry = setup.bathymetry_geometry;
                for row = 1:setup.Ny
                    for col = 1:setup.Nx
                        k = geometry.first_wet_k(row, col);
                        if k >= 1 && k <= setup.Nz && wet_mask(row, col, k)
                            u3d(row, col, k) = BCDispatcher.resolve_wall_tangent_speed(sides.bottom) * geometry.bottom_drive_u(row, col);
                            v3d(row, col, k) = BCDispatcher.resolve_wall_tangent_speed(sides.bottom) * geometry.bottom_drive_v(row, col);
                        end
                    end
                end
            end

            u3d(~wet_mask) = 0;
            v3d(~wet_mask) = 0;
        end

        function [hg, hug, hvg, bg] = apply_swe_ghost_cells(h, hu, hv, setup, common)
            [Ny, Nx] = size(h);
            hg = zeros(Ny + 2, Nx + 2);
            hug = zeros(Ny + 2, Nx + 2);
            hvg = zeros(Ny + 2, Nx + 2);
            bg = zeros(Ny + 2, Nx + 2);

            hg(2:end-1, 2:end-1) = h;
            hug(2:end-1, 2:end-1) = hu;
            hvg(2:end-1, 2:end-1) = hv;
            bg(2:end-1, 2:end-1) = setup.bed;

            sides = common.sides;
            [hg(2:end-1, 1), hug(2:end-1, 1), hvg(2:end-1, 1), bg(2:end-1, 1)] = ...
                BCDispatcher.fill_swe_ghost_side(h, hu, hv, setup.bed, sides.left, 'left');
            [hg(2:end-1, end), hug(2:end-1, end), hvg(2:end-1, end), bg(2:end-1, end)] = ...
                BCDispatcher.fill_swe_ghost_side(h, hu, hv, setup.bed, sides.right, 'right');
            [hg(1, 2:end-1), hug(1, 2:end-1), hvg(1, 2:end-1), bg(1, 2:end-1)] = ...
                BCDispatcher.fill_swe_ghost_side(h, hu, hv, setup.bed, sides.bottom, 'bottom');
            [hg(end, 2:end-1), hug(end, 2:end-1), hvg(end, 2:end-1), bg(end, 2:end-1)] = ...
                BCDispatcher.fill_swe_ghost_side(h, hu, hv, setup.bed, sides.top, 'top');

            hg(1, 1) = 0.5 * (hg(1, 2) + hg(2, 1));
            hg(1, end) = 0.5 * (hg(1, end-1) + hg(2, end));
            hg(end, 1) = 0.5 * (hg(end-1, 1) + hg(end, 2));
            hg(end, end) = 0.5 * (hg(end-1, end) + hg(end, end-1));

            hug(1, 1) = 0.5 * (hug(1, 2) + hug(2, 1));
            hug(1, end) = 0.5 * (hug(1, end-1) + hug(2, end));
            hug(end, 1) = 0.5 * (hug(end-1, 1) + hug(end, 2));
            hug(end, end) = 0.5 * (hug(end-1, end) + hug(end, end-1));

            hvg(1, 1) = 0.5 * (hvg(1, 2) + hvg(2, 1));
            hvg(1, end) = 0.5 * (hvg(1, end-1) + hvg(2, end));
            hvg(end, 1) = 0.5 * (hvg(end-1, 1) + hvg(end, 2));
            hvg(end, end) = 0.5 * (hvg(end-1, end) + hvg(end, end-1));

            bg(1, 1) = 0.5 * (bg(1, 2) + bg(2, 1));
            bg(1, end) = 0.5 * (bg(1, end-1) + bg(2, end));
            bg(end, 1) = 0.5 * (bg(end-1, 1) + bg(end, 2));
            bg(end, end) = 0.5 * (bg(end-1, end) + bg(end, end-1));
        end

        function [hghost, hughost, hvghost, bghost] = fill_swe_ghost_side(h, hu, hv, bed, side, location)
            switch location
                case 'left'
                    h_edge = h(:, 1);
                    hu_edge = hu(:, 1);
                    hv_edge = hv(:, 1);
                    bed_edge = bed(:, 1);
                    h_periodic = h(:, end);
                    hu_periodic = hu(:, end);
                    hv_periodic = hv(:, end);
                    bed_periodic = bed(:, end);
                case 'right'
                    h_edge = h(:, end);
                    hu_edge = hu(:, end);
                    hv_edge = hv(:, end);
                    bed_edge = bed(:, end);
                    h_periodic = h(:, 1);
                    hu_periodic = hu(:, 1);
                    hv_periodic = hv(:, 1);
                    bed_periodic = bed(:, 1);
                case 'bottom'
                    h_edge = h(1, :);
                    hu_edge = hu(1, :);
                    hv_edge = hv(1, :);
                    bed_edge = bed(1, :);
                    h_periodic = h(end, :);
                    hu_periodic = hu(end, :);
                    hv_periodic = hv(end, :);
                    bed_periodic = bed(end, :);
                case 'top'
                    h_edge = h(end, :);
                    hu_edge = hu(end, :);
                    hv_edge = hv(end, :);
                    bed_edge = bed(end, :);
                    h_periodic = h(1, :);
                    hu_periodic = hu(1, :);
                    hv_periodic = hv(1, :);
                    bed_periodic = bed(1, :);
                otherwise
                    error('BCDispatcher:UnknownSWELocation', ...
                        'Unknown SWE ghost-cell boundary location ''%s''.', location);
            end

            if strcmp(side.kind, 'periodic')
                hghost = h_periodic;
                hughost = hu_periodic;
                hvghost = hv_periodic;
                bghost = bed_periodic;
                return;
            end

            if strcmp(side.kind, 'open')
                hghost = h_edge;
                hughost = hu_edge;
                hvghost = hv_edge;
                bghost = bed_edge;
                return;
            end

            hghost = h_edge;
            hughost = hu_edge;
            hvghost = hv_edge;
            bghost = bed_edge;

            switch location
                case {'left', 'right'}
                    hughost = -hu_edge;
                case {'bottom', 'top'}
                    hvghost = -hv_edge;
            end
        end

        function wet_mask = resolve_fv_wet_mask3d(setup)
            if isfield(setup, 'bathymetry_geometry') && isstruct(setup.bathymetry_geometry) && ...
                    isfield(setup.bathymetry_geometry, 'wet_mask') && ~isempty(setup.bathymetry_geometry.wet_mask)
                wet_mask = logical(setup.bathymetry_geometry.wet_mask);
            else
                wet_mask = true(setup.Ny, setup.Nx, setup.Nz);
            end
        end

        function speed = resolve_wall_tangent_speed(side)
            speed = 0;
            if isstruct(side) && isfield(side, 'physical_type') && strcmp(side.physical_type, 'driven') && ...
                    isfield(side, 'U_tangent') && isnumeric(side.U_tangent) && isfinite(side.U_tangent)
                speed = double(side.U_tangent);
            end
        end

        function [dx, dy] = resolve_spacing(setup)
            dx = NaN;
            dy = NaN;

            if isfield(setup, 'dx') && isnumeric(setup.dx) && isfinite(setup.dx) && setup.dx > 0
                dx = setup.dx;
            end
            if isfield(setup, 'dy') && isnumeric(setup.dy) && isfinite(setup.dy) && setup.dy > 0
                dy = setup.dy;
            end

            if (isnan(dx) || isnan(dy)) && isfield(setup, 'X') && isfield(setup, 'Y')
                X = setup.X;
                Y = setup.Y;
                if size(X, 2) >= 2
                    dx = abs(X(1, 2) - X(1, 1));
                end
                if size(Y, 1) >= 2
                    dy = abs(Y(2, 1) - Y(1, 1));
                end
            end

            if isnan(dx) || isnan(dy) || dx <= 0 || dy <= 0
                error('BCDispatcher:MissingGridSpacing', ...
                    'FD wall BC update requires positive dx and dy in setup.');
            end
        end

        function geometry = resolve_bathymetry_geometry(setup)
            geometry = struct('enabled', false, 'dimension', '', ...
                'wet_mask', [], 'fluid_mask', [], 'wall_mask', [], ...
                'first_wet_row', [], 'first_fluid_row', [], 'first_fluid_valid', [], ...
                'first_fluid_distance', [], 'bottom_drive_scale', []);
            if isfield(setup, 'bathymetry_geometry') && isstruct(setup.bathymetry_geometry)
                geometry = setup.bathymetry_geometry;
            end
        end

        function omega = legacy_omega_bc(omega, common)
            % Compatibility-only hook for older call-sites expecting omega-only BC.
            % Non-periodic sides are clamped to zero as a legacy approximation.
            if strcmp(common.sides.bottom.kind, 'wall')
                omega(1, :) = 0;
            end
            if strcmp(common.sides.top.kind, 'wall')
                omega(end, :) = 0;
            end
            if strcmp(common.sides.left.kind, 'wall')
                omega(:, 1) = 0;
            end
            if strcmp(common.sides.right.kind, 'wall')
                omega(:, end) = 0;
            end
        end

        function out = side_display_label(side)
            if strcmp(side.kind, 'periodic')
                out = 'Periodic';
                return;
            end
            if strcmp(side.kind, 'open')
                out = 'Open (Absorbing)';
                return;
            end
            switch side.physical_type
                case 'pinned'
                    out = 'Pinned (Dirichlet)';
                case 'no_flux'
                    out = 'No-flux (Neumann)';
                case 'driven'
                    out = 'Driven wall (psi const + wall speed)';
                case 'no_slip'
                    out = 'No-slip wall (psi const)';
                otherwise
                    error('BCDispatcher:UnknownPhysicalBoundaryType', ...
                        'Unsupported physical boundary type "%s" for side "%s".', ...
                        char(string(side.physical_type)), char(string(side.id)));
            end
        end

        function [supported, reason, axis_x, axis_y] = resolve_spectral_axis_payloads(common)
            [supported_x, reason_x, axis_x] = BCDispatcher.resolve_spectral_axis_payload( ...
                common.sides.left, common.sides.right, 'x');
            [supported_y, reason_y, axis_y] = BCDispatcher.resolve_spectral_axis_payload( ...
                common.sides.bottom, common.sides.top, 'y');

            supported = supported_x && supported_y;
            reason = '';
            if ~supported_x
                reason = reason_x;
            elseif ~supported_y
                reason = reason_y;
            end
        end

        function [supported, reason, axis_payload] = resolve_spectral_axis_payload(side_a, side_b, axis_name)
            supported = true;
            reason = '';
            axis_payload = struct( ...
                'axis', axis_name, ...
                'family', 'fft', ...
                'math_type', 'periodic', ...
                'label', 'Periodic', ...
                'required_functions', {{}}, ...
                'supports_explicit_modes', true, ...
                'allows_constant_mode', true, ...
                'physical_pair', {{char(string(side_a.physical_type)), char(string(side_b.physical_type))}}, ...
                'transform_compatible', true, ...
                'homogeneous_after_lifting', true, ...
                'requires_lifting', false, ...
                'requires_wall_closure', false);

            if strcmp(side_a.kind, 'open') || strcmp(side_b.kind, 'open')
                supported = false;
                reason = sprintf('Spectral %s-axis does not support open/absorbing boundaries.', axis_name);
                return;
            end

            if ~strcmp(side_a.kind, side_b.kind)
                supported = false;
                reason = sprintf('Spectral %s-axis requires matching opposite-side boundary kinds.', axis_name);
                return;
            end

            if ~strcmp(side_a.math_type, side_b.math_type)
                supported = false;
                reason = sprintf('Spectral %s-axis requires matching opposite-side math types.', axis_name);
                return;
            end

            switch side_a.math_type
                case 'periodic'
                    axis_payload.family = 'fft';
                    axis_payload.math_type = 'periodic';
                    axis_payload.label = 'Periodic';
                    axis_payload.required_functions = {};
                    axis_payload.supports_explicit_modes = true;
                    axis_payload.allows_constant_mode = true;

                case 'dirichlet'
                    if ~(BCDispatcher.is_spectral_dirichlet_physical(side_a.physical_type) && ...
                            BCDispatcher.is_spectral_dirichlet_physical(side_b.physical_type))
                        supported = false;
                        reason = sprintf(['Spectral %s-axis Dirichlet support requires pinned/no-slip/driven wall semantics. ' ...
                            'Unsupported physical boundary pairing detected.'], axis_name);
                        return;
                    end
                    axis_payload.family = 'dst';
                    axis_payload.math_type = 'dirichlet';
                    axis_payload.label = 'Pinned (Dirichlet)';
                    axis_payload.required_functions = {'dst', 'idst'};
                    axis_payload.supports_explicit_modes = false;
                    axis_payload.allows_constant_mode = false;
                    axis_payload.requires_lifting = any(strcmp(axis_payload.physical_pair, 'driven'));
                    axis_payload.requires_wall_closure = any(ismember(axis_payload.physical_pair, {'no_slip', 'driven'}));
                    axis_payload.homogeneous_after_lifting = ~axis_payload.requires_lifting;

                case 'neumann'
                    if ~(BCDispatcher.is_spectral_neumann_physical(side_a.physical_type) && ...
                            BCDispatcher.is_spectral_neumann_physical(side_b.physical_type))
                        supported = false;
                        reason = sprintf('Spectral %s-axis Neumann support is limited to homogeneous no-flux walls.', axis_name);
                        return;
                    end
                    axis_payload.family = 'dct';
                    axis_payload.math_type = 'neumann';
                    axis_payload.label = 'No-flux (Neumann)';
                    axis_payload.required_functions = {'dct', 'idct'};
                    axis_payload.supports_explicit_modes = false;
                    axis_payload.allows_constant_mode = true;

                otherwise
                    error('BCDispatcher:SpectralUnsupportedMathType', ...
                        'Spectral %s-axis does not support boundary math type "%s".', ...
                        axis_name, side_a.math_type);
            end
            axis_payload.transform_compatible = logical(supported);
        end

        function tf = is_spectral_dirichlet_physical(physical_type)
            tf = any(strcmp(physical_type, {'pinned', 'no_slip', 'driven'}));
        end

        function tf = is_spectral_neumann_physical(physical_type)
            tf = strcmp(physical_type, 'no_flux');
        end

        function [requires_lifting, lifting_model, lifting_payload] = resolve_spectral_lifting_model(common, grid_meta, axis_x, axis_y)
            if nargin < 2 || ~isstruct(grid_meta)
                grid_meta = struct();
            end
            if nargin < 3 || ~isstruct(axis_x)
                axis_x = struct();
            end
            if nargin < 4 || ~isstruct(axis_y)
                axis_y = struct();
            end

            requires_lifting = false;
            lifting_model = 'none';
            lifting_payload = struct( ...
                'model', 'none', ...
                'case_name', char(string(common.case_name)), ...
                'Lx', BCDispatcher.pick_grid_meta_value(grid_meta, 'Lx', NaN), ...
                'Ly', BCDispatcher.pick_grid_meta_value(grid_meta, 'Ly', NaN), ...
                'Nx', BCDispatcher.pick_grid_meta_value(grid_meta, 'Nx', NaN), ...
                'Ny', BCDispatcher.pick_grid_meta_value(grid_meta, 'Ny', NaN), ...
                'top_speed', double(common.sides.top.U_tangent), ...
                'bottom_speed', double(common.sides.bottom.U_tangent), ...
                'left_speed', double(common.sides.left.U_tangent), ...
                'right_speed', double(common.sides.right.U_tangent), ...
                'periodic_axis', '', ...
                'wall_axis', '', ...
                'boundary_profile', 'transform_native');

            side_physical = {common.sides.top.physical_type, common.sides.bottom.physical_type, ...
                common.sides.left.physical_type, common.sides.right.physical_type};
            if ~any(strcmp(side_physical, 'driven'))
                return;
            end

            requires_lifting = true;
            lifting_payload.boundary_profile = 'corner_regularized';

            if strcmp(axis_x.family, 'fft') && strcmp(axis_y.family, 'dst')
                lifting_model = 'couette_y';
                lifting_payload.periodic_axis = 'x';
                lifting_payload.wall_axis = 'y';
                lifting_payload.boundary_profile = 'exact_channel';
                return;
            end

            switch lower(char(string(common.case_name)))
                case 'lid_driven_cavity'
                    lifting_model = 'cavity_2d';
                case {'enclosed_shear_layer', 'lid_and_bottom_driven_cavity'}
                    lifting_model = 'enclosed_shear_2d';
                otherwise
                    lifting_model = 'wall_box_2d';
            end
        end

        function [supported, reason] = resolve_spectral_solver_support(common, axis_x, axis_y, requires_lifting, lifting_model, requires_wall_closure, requires_bathymetry_penalization, bathymetry_model)
            supported = true;
            reason = '';

            if any(strcmp({axis_x.math_type, axis_y.math_type}, 'radiation'))
                supported = false;
                reason = 'Spectral transform-family runtime does not support open/absorbing boundaries.';
                return;
            end

            if requires_lifting && strcmp(lifting_model, 'none')
                supported = false;
                reason = 'Spectral wall-bounded moving-wall support requires a lifting model, but none was resolved for this case.';
                return;
            end

            if requires_wall_closure
                dirichlet_axes = strcmp(axis_x.math_type, 'dirichlet') || strcmp(axis_y.math_type, 'dirichlet');
                if ~dirichlet_axes
                    supported = false;
                    reason = 'Spectral wall closure is only implemented for Dirichlet wall axes.';
                    return;
                end
            end

            if requires_bathymetry_penalization && strcmpi(char(string(bathymetry_model)), 'none')
                supported = false;
                reason = 'Spectral shaped bathymetry requires an immersed-mask penalization model.';
                return;
            end

            side_kinds = {common.sides.top.kind, common.sides.bottom.kind, common.sides.left.kind, common.sides.right.kind};
            if any(strcmp(side_kinds, 'open'))
                supported = false;
                reason = 'Spectral wall-bounded support excludes open/absorbing sides.';
            end
        end

        function [requires_penalization, bathymetry_model, bathymetry_payload] = resolve_spectral_bathymetry_payload(common, grid_meta)
            scenario = 'flat_2d';
            if isfield(common, 'bathymetry_scenario') && ~isempty(common.bathymetry_scenario)
                scenario = normalize_bathymetry_scenario_token(common.bathymetry_scenario);
            end

            requires_penalization = false;
            bathymetry_model = 'none';
            bathymetry_payload = struct( ...
                'scenario', scenario, ...
                'model', 'none', ...
                'penalization_strength', 48.0, ...
                'supports_internal_mask', false, ...
                'Lx', BCDispatcher.pick_grid_meta_value(grid_meta, 'Lx', NaN), ...
                'Ly', BCDispatcher.pick_grid_meta_value(grid_meta, 'Ly', NaN), ...
                'Nx', BCDispatcher.pick_grid_meta_value(grid_meta, 'Nx', NaN), ...
                'Ny', BCDispatcher.pick_grid_meta_value(grid_meta, 'Ny', NaN));

            if strcmpi(scenario, 'flat_2d')
                return;
            end

            if endsWith(lower(char(string(scenario))), '_3d')
                bathymetry_payload.model = 'unsupported_3d_bathymetry';
                return;
            end

            requires_penalization = true;
            bathymetry_model = 'immersed_mask_2d';
            bathymetry_payload.model = bathymetry_model;
            bathymetry_payload.supports_internal_mask = true;
        end

        function value = pick_grid_meta_value(grid_meta, field_name, default_value)
            value = default_value;
            if nargin < 3
                default_value = [];
                value = default_value;
            end
            if ~isstruct(grid_meta) || ~isfield(grid_meta, field_name)
                return;
            end
            candidate = grid_meta.(field_name);
            if isnumeric(candidate) && isscalar(candidate) && isfinite(candidate)
                value = double(candidate);
            end
        end

        function side = sync_side_contract_fields(side)
            side.side_name = side.id;
            side.prescribed_value = side.psi_value;
            side.prescribed_gradient = side.dpsi_dn;
            side.wall_speed = side.U_tangent;
            if strcmp(side.kind, 'periodic')
                side.periodic_partner = BCDispatcher.default_periodic_partner(side.id, side.kind);
            elseif ~isfield(side, 'periodic_partner') || isempty(side.periodic_partner)
                side.periodic_partner = '';
            end
            if ~isfield(side, 'requires_base_flow') || isempty(side.requires_base_flow)
                side.requires_base_flow = false;
            end
            if ~isfield(side, 'target_quantity') || isempty(side.target_quantity)
                side.target_quantity = 'streamfunction_vorticity';
            end
        end

        function partner = default_periodic_partner(side_id, kind)
            partner = '';
            if ~strcmp(kind, 'periodic')
                return;
            end
            switch lower(char(string(side_id)))
                case 'left'
                    partner = 'right';
                case 'right'
                    partner = 'left';
                case 'top'
                    partner = 'bottom';
                case 'bottom'
                    partner = 'top';
            end
        end

        function value = average_corner_closures(edge_a, edge_b, side_a, side_b, fallback)
            values = [];
            if strcmp(side_a.kind, 'wall')
                values(end + 1) = edge_a; %#ok<AGROW>
            end
            if strcmp(side_b.kind, 'wall')
                values(end + 1) = edge_b; %#ok<AGROW>
            end
            if isempty(values)
                value = fallback;
            else
                value = mean(values);
            end
        end

        function tf = has_base_flow_request(Parameters)
            tf = false;
            if nargin < 1 || ~isstruct(Parameters)
                return;
            end
            if isfield(Parameters, 'requires_base_flow') && logical(Parameters.requires_base_flow)
                tf = true;
                return;
            end
            probe_fields = {'mean_throughflow', 'throughflow_velocity', 'bulk_velocity', ...
                'base_flow_velocity', 'pressure_gradient'};
            for idx = 1:numel(probe_fields)
                key = probe_fields{idx};
                if isfield(Parameters, key)
                    value = Parameters.(key);
                    if isnumeric(value) && isscalar(value) && isfinite(value) && abs(double(value)) > 1.0e-12
                        tf = true;
                        return;
                    end
                end
            end
        end

        function tf = is_bathymetry_axis_periodic(Parameters, axis_name, scenario, grid_meta)
            if nargin < 3 || isempty(scenario)
                scenario = BCDispatcher.extract_bathymetry_scenario(Parameters);
            end
            if ~bathymetry_blocks_periodic_bc(scenario)
                tf = true;
                return;
            end

            [X, Y] = BCDispatcher.build_bathymetry_probe_grid(Parameters, grid_meta);
            [bath_field, ~] = generate_bathymetry_field(X, Y, scenario, ...
                BCDispatcher.build_bathymetry_generator_params(Parameters));
            bath_field = double(bath_field);
            tol = 1.0e-8 * max(1.0, max(abs(bath_field(:))));

            switch lower(char(string(axis_name)))
                case 'x'
                    boundary_gap = max(abs(bath_field(:, 1) - bath_field(:, end)), [], 'all');
                    tf = boundary_gap <= tol;
                case 'y'
                    boundary_gap = max(abs(bath_field(1, :) - bath_field(end, :)), [], 'all');
                    tf = boundary_gap <= tol;
                otherwise
                    error('BCDispatcher:InvalidBathymetryAxis', ...
                        'Unsupported bathymetry periodicity axis "%s".', char(string(axis_name)));
            end
        end

        function params = build_bathymetry_generator_params(Parameters)
            params = struct();
            keys = {'bathymetry_bed_slope', 'bathymetry_resolution', 'bathymetry_depth_offset', ...
                'bathymetry_relief_amplitude', 'bathymetry_custom_points', 'bathymetry_dynamic_params'};
            for idx = 1:numel(keys)
                key = keys{idx};
                if isfield(Parameters, key)
                    params.(key) = Parameters.(key);
                end
            end
        end

        function [X, Y] = build_bathymetry_probe_grid(Parameters, grid_meta)
            if nargin >= 2 && isstruct(grid_meta) && isfield(grid_meta, 'X') && isfield(grid_meta, 'Y') && ...
                    isnumeric(grid_meta.X) && isnumeric(grid_meta.Y) && isequal(size(grid_meta.X), size(grid_meta.Y))
                X_candidate = double(grid_meta.X);
                Y_candidate = double(grid_meta.Y);
                if numel(unique(X_candidate(:))) > 1 && numel(unique(Y_candidate(:))) > 1
                    X = X_candidate;
                    Y = Y_candidate;
                    return;
                end
            end

            Nx = 64;
            Ny = 64;
            Lx = 1.0;
            Ly = 1.0;
            if isstruct(Parameters)
                if isfield(Parameters, 'Nx') && isnumeric(Parameters.Nx) && isfinite(Parameters.Nx)
                    Nx = max(8, round(double(Parameters.Nx)));
                end
                if isfield(Parameters, 'Ny') && isnumeric(Parameters.Ny) && isfinite(Parameters.Ny)
                    Ny = max(8, round(double(Parameters.Ny)));
                end
                if isfield(Parameters, 'Lx') && isnumeric(Parameters.Lx) && isfinite(Parameters.Lx) && Parameters.Lx > 0
                    Lx = double(Parameters.Lx);
                end
                if isfield(Parameters, 'Ly') && isnumeric(Parameters.Ly) && isfinite(Parameters.Ly) && Parameters.Ly > 0
                    Ly = double(Parameters.Ly);
                end
            end

            x = linspace(0.0, Lx, Nx);
            y = linspace(0.0, Ly, Ny);
            [X, Y] = meshgrid(x, y);
        end

        function out = title_case(token)
            out = char(string(token));
            if isempty(out)
                return;
            end
            out = [upper(out(1)), lower(out(2:end))];
        end

        function token = normalize_boundary_token_key(token_raw)
            token = lower(strtrim(char(string(token_raw))));
            token = regexprep(token, '[^a-z0-9]+', '_');
            token = regexprep(token, '_+', '_');
            token = regexprep(token, '^_|_$', '');
        end

    end
end
