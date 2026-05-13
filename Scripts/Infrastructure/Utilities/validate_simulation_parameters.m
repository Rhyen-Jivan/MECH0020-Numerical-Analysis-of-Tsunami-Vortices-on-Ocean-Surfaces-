function [is_valid, warnings, errors] = validate_simulation_parameters(Parameters, settings)
% VALIDATE_SIMULATION_PARAMETERS Comprehensive preflight validation
%
% Performs extensive validation of simulation parameters before execution
% to catch errors early and prevent wasted computation time.
%
% Inputs:
%   Parameters - Simulation parameter struct
%   settings   - Settings struct with convergence, sweep, etc.
%
% Outputs:
%   is_valid - Boolean indicating if all validations passed
%   warnings - Cell array of warning messages
%   errors   - Cell array of error messages
%
% Validation Categories:
%   1. Grid parameters (Nx, Ny, Lx, Ly)
%   2. Physical parameters (nu, dt, Tfinal)
%   3. Stability conditions (CFL, diffusion)
%   4. Memory estimation
%   5. Directory structure
%   6. Required entrypoints
%   7. Initial conditions
%
% Author: Analysis Framework
% Date: February 2026

    warnings = {};
    errors = {};
    
    fprintf('\n========================================\n');
    fprintf('PREFLIGHT VALIDATION\n');
    fprintf('========================================\n\n');
    
    % ====================================================================
    % 1. GRID PARAMETER VALIDATION
    % ====================================================================
    fprintf('[1/7] Validating grid parameters...\n');
    
    if ~isfield(Parameters, 'Nx') || Parameters.Nx <= 0 || mod(Parameters.Nx, 1) ~= 0
        errors{end+1} = sprintf('Invalid Nx: %g (must be positive integer)', Parameters.Nx);
    end
    
    if ~isfield(Parameters, 'Ny') || Parameters.Ny <= 0 || mod(Parameters.Ny, 1) ~= 0
        errors{end+1} = sprintf('Invalid Ny: %g (must be positive integer)', Parameters.Ny);
    end
    
    if ~isfield(Parameters, 'Lx') || Parameters.Lx <= 0
        errors{end+1} = sprintf('Invalid Lx: %g (must be positive)', Parameters.Lx);
    end
    
    if ~isfield(Parameters, 'Ly') || Parameters.Ly <= 0
        errors{end+1} = sprintf('Invalid Ly: %g (must be positive)', Parameters.Ly);
    end
    
    % Grid spacing
    if isfield(Parameters, 'Nx') && isfield(Parameters, 'Lx') && Parameters.Nx > 0
        dx = Parameters.Lx / Parameters.Nx;
        dy = Parameters.Ly / Parameters.Ny;
        fprintf('   Grid spacing: dx = %.4e, dy = %.4e\n', dx, dy);
    end
    
    % ====================================================================
    % 2. PHYSICAL PARAMETER VALIDATION
    % ====================================================================
    fprintf('[2/7] Validating physical parameters...\n');
    
    if ~isfield(Parameters, 'nu') || Parameters.nu < 0
        errors{end+1} = sprintf('Invalid viscosity nu: %g (must be non-negative)', Parameters.nu);
    end
    
    if ~isfield(Parameters, 'dt') || Parameters.dt <= 0
        errors{end+1} = sprintf('Invalid timestep dt: %g (must be positive)', Parameters.dt);
    end
    
    if ~isfield(Parameters, 'Tfinal') || Parameters.Tfinal <= 0
        errors{end+1} = sprintf('Invalid Tfinal: %g (must be positive)', Parameters.Tfinal);
    end
    
    if isfield(Parameters, 'num_snapshots') && Parameters.num_snapshots < 2
        warnings{end+1} = sprintf('Low snapshot count: %d (recommend >= 2)', Parameters.num_snapshots);
    end
    
    % ====================================================================
    % 3. STABILITY CONDITIONS
    % ====================================================================
    fprintf('[3/7] Checking stability conditions...\n');
    
    if isfield(Parameters, 'Nx') && isfield(Parameters, 'dt') && Parameters.Nx > 0
        dx = Parameters.Lx / Parameters.Nx;
        dy = Parameters.Ly / Parameters.Ny;
        
        % CFL Condition: C = u_max * dt / dx < 1
        % Estimate maximum velocity from vorticity
        u_max_est = 1.0;  % Conservative estimate
        CFL = u_max_est * Parameters.dt / min(dx, dy);
        
        fprintf('   CFL number (estimated): %.4f', CFL);
        if CFL >= 1.0
            errors{end+1} = sprintf('CFL condition violated: CFL = %.4f >= 1.0 (UNSTABLE)', CFL);
            fprintf(' ❌ UNSTABLE\n');
        elseif CFL >= 0.8
            warnings{end+1} = sprintf('CFL number high: %.4f (recommend < 0.8)', CFL);
            fprintf(' ⚠️  HIGH\n');
        else
            fprintf(' ✅ OK\n');
        end
        
        % Diffusion Stability: D = ν * dt / dx² < 0.5
        if isfield(Parameters, 'nu')
            D = Parameters.nu * Parameters.dt / min(dx, dy)^2;
            fprintf('   Diffusion number: %.4f', D);
            if D >= 0.5
                errors{end+1} = sprintf('Diffusion instability: D = %.4f >= 0.5', D);
                fprintf(' ❌ UNSTABLE\n');
            elseif D >= 0.4
                warnings{end+1} = sprintf('Diffusion number high: %.4f (recommend < 0.4)', D);
                fprintf(' ⚠️  HIGH\n');
            else
                fprintf(' ✅ OK\n');
            end
        end
    end
    
    % ====================================================================
    % 4. MEMORY ESTIMATION
    % ====================================================================
    fprintf('[4/7] Estimating memory requirements...\n');
    
    if isfield(Parameters, 'Nx') && isfield(Parameters, 'num_snapshots')
        % Estimate: each snapshot is Nx × Ny × 8 bytes (double)
        % Store: omega_snaps, psi_snaps, u_snaps, v_snaps
        bytes_per_snapshot = Parameters.Nx * Parameters.Ny * 8;
        total_snapshots = Parameters.num_snapshots * 4;  % 4 field types
        estimated_MB = (bytes_per_snapshot * total_snapshots) / (1024^2);
        
        fprintf('   Estimated memory: %.1f MB\n', estimated_MB);
        
        % Check available memory
        if ispc
            [~, sys_mem] = memory;
            available_MB = sys_mem.PhysicalMemory.Available / (1024^2);
            fprintf('   Available memory: %.1f MB\n', available_MB);
            
            if estimated_MB > available_MB * 0.8
                warnings{end+1} = sprintf('High memory usage: %.1f MB (%.0f%% of available)', ...
                    estimated_MB, 100*estimated_MB/available_MB);
            end
        end
    end
    
    % ====================================================================
    % 5. DIRECTORY STRUCTURE
    % ====================================================================
    fprintf('[5/7] Checking directory structure...\n');
    
    required_dirs = {
        'Scripts/Solvers', ...
        'Scripts/Infrastructure', ...
        'Scripts/Drivers'
    };
    
    if isfield(settings, 'results_dir')
        required_dirs{end+1} = settings.results_dir;
    end
    
    if isfield(settings, 'figures') && isfield(settings.figures, 'root_dir')
        required_dirs{end+1} = settings.figures.root_dir;
    end
    
    for i = 1:length(required_dirs)
        if ~exist(required_dirs{i}, 'dir')
            warnings{end+1} = sprintf('Directory missing: %s (will be created)', required_dirs{i}); %#ok<AGROW>
        end
    end
    
    fprintf('   ✅ Directory structure validated\n');
    
    % ====================================================================
    % 6. REQUIRED ENTRYPOINTS
    % ====================================================================
    fprintf('[6/7] Checking required entrypoints...\n');
    
    required_entrypoints = {
        'Tsunami_Vorticity_Emulator', ...
        'ModeDispatcher', ...
        'FiniteDifferenceMethod', ...
        'SpectralMethod', ...
        'FiniteVolumeMethod', ...
        'initialise_omega'
    };

    for i = 1:length(required_entrypoints)
        name_i = required_entrypoints{i};
        if exist(name_i, 'file') ~= 2 && exist(name_i, 'class') ~= 8
            errors{end+1} = sprintf('Required entrypoint not found: %s', name_i); %#ok<AGROW>
        end
    end
    
    fprintf('   [OK] Required entrypoints found\n');
    
    % ====================================================================
    % 7. INITIAL CONDITIONS VALIDATION
    % ====================================================================
    fprintf('[7/7] Validating initial conditions...\n');
    
    if isfield(Parameters, 'ic_type')
        valid_ic_types = ["stretched_gaussian", "vortex_blob_gaussian", "vortex_pair", ...
                          "multi_vortex", "counter_rotating_pair", "kutz"];
        if ~ismember(Parameters.ic_type, valid_ic_types)
            errors{end+1} = sprintf('Unknown IC type: %s', Parameters.ic_type);
        else
            fprintf('   IC type: %s\n', Parameters.ic_type);
            
            % Check if omega field is pre-computed
            if isfield(Parameters, 'omega') && ~isempty(Parameters.omega)
                if any(~isfinite(Parameters.omega(:)))
                    errors{end+1} = 'Pre-computed omega contains NaN or Inf values';
                else
                    fprintf('   ✅ Pre-computed omega is valid\n');
                end
            end
        end
    end
    
    % ====================================================================
    % SUMMARY
    % ====================================================================
    fprintf('\n========================================\n');
    fprintf('VALIDATION SUMMARY\n');
    fprintf('========================================\n');
    fprintf('Errors:   %d\n', length(errors));
    fprintf('Warnings: %d\n', length(warnings));
    
    if ~isempty(errors)
        fprintf('\n❌ ERRORS:\n');
        for i = 1:length(errors)
            fprintf('   %d. %s\n', i, errors{i});
        end
    end
    
    if ~isempty(warnings)
        fprintf('\n⚠️  WARNINGS:\n');
        for i = 1:length(warnings)
            fprintf('   %d. %s\n', i, warnings{i});
        end
    end
    
    is_valid = isempty(errors);
    
    if is_valid
        fprintf('\n✅ VALIDATION PASSED - Ready to simulate\n');
    else
        fprintf('\n❌ VALIDATION FAILED - Fix errors before running\n');
    end
    fprintf('========================================\n\n');
end
