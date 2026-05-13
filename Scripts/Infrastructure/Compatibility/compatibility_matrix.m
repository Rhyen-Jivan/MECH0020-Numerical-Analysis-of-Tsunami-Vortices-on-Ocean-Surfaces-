function [status, reason] = compatibility_matrix(method, mode)
    % compatibility_matrix - Method/Mode Compatibility Checker
    %
    % Purpose:
    %   Single source of truth for method/mode compatibility
    %   Used by mode scripts to validate configurations early
    %
    % Inputs:
    %   method - Method name ('FD', 'Spectral', 'FV', 'SWE')
    %   mode - Mode name ('Evolution', 'Convergence', 'ParameterSweep', 'Plotting')
    %
    % Outputs:
    %   status - 'supported' | 'experimental' | 'blocked'
    %   reason - Explanation string (for blocked/experimental)
    %
    % Usage:
    %   [status, reason] = compatibility_matrix('FD', 'Evolution');
    %   if strcmp(status, 'blocked')
    %       error('Incompatible: %s', reason);
    %   end

    % Normalize inputs
    method = lower(method);
    mode = lower(mode);

    % ===== FINITE DIFFERENCE COMPATIBILITY =====
    if strcmp(method, 'fd')
        switch mode
            case 'evolution'
                status = 'supported';
                reason = '';
            case 'convergence'
                status = 'supported';
                reason = '';
            case 'parametersweep'
                status = 'supported';
                reason = '';
            case 'plotting'
                status = 'supported';
                reason = '';
            case 'variablebathymetry'
                status = 'experimental';
                reason = 'Variable bathymetry is experimental for FD method';
            otherwise
                status = 'blocked';
                reason = sprintf('Unknown mode: %s', mode);
        end
        return;
    end

    % ===== SPECTRAL METHOD COMPATIBILITY =====
    if strcmp(method, 'spectral') || strcmp(method, 'fft')
        switch mode
            case 'evolution'
                status = 'experimental';
                reason = ['Spectral evolution supports transform-family homogeneous rectangular BCs and the active lifted flat-wall ' ...
                    'Phase 2 wall-bounded cases on flat bathymetry.'];
            case 'convergence'
                status = 'experimental';
                reason = ['Spectral convergence uses staged dt and modal refinement on the active transform-family solver, ' ...
                    'including supported non-periodic axis pairs on flat bathymetry.'];
            case 'parametersweep'
                status = 'blocked';
                reason = 'Spectral parameter sweep not enabled in this checkpoint.';
            case 'plotting'
                status = 'supported';
                reason = '';  % Plotting is method-agnostic
            case 'variablebathymetry'
                status = 'experimental';
                reason = 'Spectral variable bathymetry now uses an immersed-mask penalized transform-family runtime for supported 2D profile cases.';
            otherwise
                status = 'blocked';
                reason = sprintf('Unknown mode: %s', mode);
        end
        return;
    end

    % ===== FINITE VOLUME COMPATIBILITY =====
    if strcmp(method, 'fv') || strcmp(method, 'finitevolume')
        switch mode
            case 'evolution'
                status = 'experimental';
                reason = 'Finite Volume evolution runs as conservative 3D FV on a structured Cartesian mesh.';
            case 'convergence'
                status = 'experimental';
                reason = 'Finite Volume convergence runs staged dt and horizontal control-volume refinement with fixed Nz by default.';
            case 'parametersweep'
                status = 'blocked';
                reason = 'Finite Volume parameter sweep is currently unavailable.';
            case 'plotting'
                status = 'supported';
                reason = '';  % Plotting is method-agnostic
            case 'variablebathymetry'
                status = 'experimental';
                reason = 'FV + bathymetry is experimental in the active VSF-only seabed-geometry path.';
            otherwise
                status = 'blocked';
                reason = sprintf('Unknown mode: %s', mode);
        end
        return;
    end

    % ===== SHALLOW WATER COMPATIBILITY =====
    if any(strcmp(method, {'swe', 'shallowwater', 'shallow_water'}))
        switch mode
            case 'evolution'
                status = 'experimental';
                reason = 'Shallow Water evolution is enabled as conservative 2D nonlinear SWE.';
            case 'convergence'
                status = 'blocked';
                reason = 'Shallow Water convergence is currently unavailable.';
            case 'parametersweep'
                status = 'blocked';
                reason = 'Shallow Water parameter sweep is currently unavailable.';
            case 'plotting'
                status = 'supported';
                reason = '';
            case 'variablebathymetry'
                status = 'experimental';
                reason = 'Shallow Water uses active 2D bathymetry in its conservative free-surface update.';
            otherwise
                status = 'blocked';
                reason = sprintf('Unknown mode: %s', mode);
        end
        return;
    end

    % ===== UNKNOWN METHOD =====
    status = 'blocked';
    reason = sprintf('Unknown method: %s. Valid methods: FD, Spectral, FV, SWE', method);
end
