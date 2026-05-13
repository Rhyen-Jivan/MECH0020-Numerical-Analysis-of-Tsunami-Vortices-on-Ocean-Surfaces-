function analysis = extract_unified_metrics(omega_snaps, psi_snaps, snap_times, dx, dy, Parameters)
% EXTRACT_UNIFIED_METRICS Comprehensive data extraction for all numerical methods
%
% This function provides a unified interface for extracting key simulation metrics:
%   - Vorticity statistics (peak, mean, RMS)
%   - Energy metrics (kinetic energy, enstrophy)
%   - Velocity statistics (peak u, v, speed)
%   - Sustainability metrics (energy decay, dissipation)
%   - Grid and computational diagnostics
%
% Usage:
%   analysis = extract_unified_metrics(omega_snaps, psi_snaps, snap_times, dx, dy, Parameters)
%
% Inputs:
%   omega_snaps - Vorticity field snapshots (Ny  Nx  Nsnap)
%   psi_snaps   - Streamfunction snapshots (Ny  Nx  Nsnap)
%   snap_times  - Time values for snapshots (1  Nsnap)
%   dx          - Grid spacing x-direction
%   dy          - Grid spacing y-direction
%   Parameters  - Simulation parameters struct
%
% Outputs:
%   analysis - struct with all extracted metrics

    [Ny, Nx, Nsnap] = size(omega_snaps);
    
    analysis = struct();
    analysis.snapshot_times = snap_times(:);
    analysis.snapshots_stored = Nsnap;
    
    % ===== VORTICITY METRICS =====
    % Peak vorticity across all snapshots
    omega_all_snaps = reshape(omega_snaps, [], Nsnap);
    analysis.peak_abs_omega = max(abs(omega_all_snaps(:)));
    
    % Time-history of peak vorticity
    analysis.peak_omega_history = zeros(1, Nsnap);
    analysis.mean_omega_history = zeros(1, Nsnap);
    analysis.rms_omega_history = zeros(1, Nsnap);
    
    store_velocity_snapshot_cubes = local_pick_logical(Parameters, {'store_velocity_snapshot_cubes'}, true);
    velocity_snapshot_precision = local_pick_text(Parameters, {'velocity_snapshot_precision'}, ...
        local_pick_text(Parameters, {'snapshot_storage_precision'}, 'double'));

    % ===== VELOCITY METRICS =====
    if store_velocity_snapshot_cubes
        analysis.u_snaps = zeros(Ny, Nx, Nsnap, velocity_snapshot_precision);
        analysis.v_snaps = zeros(Ny, Nx, Nsnap, velocity_snapshot_precision);
    else
        analysis.u_snaps = [];
        analysis.v_snaps = [];
    end
    analysis.velocity_snapshot_cubes_stored = logical(store_velocity_snapshot_cubes);
    
    % ===== ENERGY METRICS =====
    analysis.kinetic_energy = zeros(1, Nsnap);
    analysis.enstrophy = zeros(1, Nsnap);
    
    % ===== VELOCITY STATISTICS =====
    analysis.peak_u_history = zeros(1, Nsnap);
    analysis.peak_v_history = zeros(1, Nsnap);
    analysis.peak_speed_history = zeros(1, Nsnap);
    analysis.mean_speed_history = zeros(1, Nsnap);
    
    % ===== DISSIPATION METRICS =====
    analysis.dissipation_rate = zeros(1, Nsnap);
    analysis.energy_decay = zeros(1, Nsnap);
    
    % Compute all metrics for each snapshot
    u_final = zeros(Ny, Nx);
    v_final = zeros(Ny, Nx);
    for k = 1:Nsnap
        omega_k = omega_snaps(:,:,k);
        psi_k = psi_snaps(:,:,k);
        
        % Vorticity metrics
        analysis.peak_omega_history(k) = max(abs(omega_k(:)));
        analysis.mean_omega_history(k) = mean(abs(omega_k(:)));
        analysis.rms_omega_history(k) = sqrt(mean(omega_k(:).^2));
        
        % Velocity recovery from streamfunction (finite differences)
        [dpsi_dy, dpsi_dx] = gradient(psi_k);
        dpsi_dx = dpsi_dx / dx;
        dpsi_dy = dpsi_dy / dy;
        
        u_k = -dpsi_dy;  % u = -ψ/y
        v_k = dpsi_dx;   % v = ψ/x
        
        if store_velocity_snapshot_cubes
            analysis.u_snaps(:,:,k) = u_k;
            analysis.v_snaps(:,:,k) = v_k;
        end
        u_final = u_k;
        v_final = v_k;
        
        % Velocity statistics
        speed_k = sqrt(u_k.^2 + v_k.^2);
        analysis.peak_u_history(k) = max(abs(u_k(:)));
        analysis.peak_v_history(k) = max(abs(v_k(:)));
        analysis.peak_speed_history(k) = max(speed_k(:));
        analysis.mean_speed_history(k) = mean(speed_k(:));
        
        % Energy metrics: KE = 0.5 *  (u + v) dA
        analysis.kinetic_energy(k) = 0.5 * sum((u_k(:).^2 + v_k(:).^2)) * dx * dy;
        
        % Enstrophy = 0.5 *  ω dA
        analysis.enstrophy(k) = 0.5 * sum(omega_k(:).^2) * dx * dy;
        
        % Dissipation rate (finite difference approximation of -ν ω)
        if Parameters.nu > 0
            laplacian_omega = (circshift(omega_k,1,1) + circshift(omega_k,-1,1) - 2*omega_k)/(dy^2) + ...
                             (circshift(omega_k,1,2) + circshift(omega_k,-1,2) - 2*omega_k)/(dx^2);
            analysis.dissipation_rate(k) = Parameters.nu * sum(laplacian_omega(:).^2) * dx * dy;
        end
    end
    
    % Peak velocity statistics
    analysis.peak_u = max(analysis.peak_u_history);
    analysis.peak_v = max(analysis.peak_v_history);
    analysis.peak_speed = max(analysis.peak_speed_history);
    
    % Energy decay (normalized by initial energy)
    analysis.energy_decay = zeros(1, Nsnap);
    if analysis.kinetic_energy(1) > 0
        analysis.energy_decay = 1 - analysis.kinetic_energy ./ analysis.kinetic_energy(1);
    end
    
    % ===== SUSTAINABILITY METRICS =====
    if Nsnap > 1
        % Energy dissipation rate (time derivative)
        dt_snap = diff(snap_times);
        analysis.energy_dissipation = zeros(1, Nsnap-1);
        for k = 1:(Nsnap-1)
            analysis.energy_dissipation(k) = -diff(analysis.kinetic_energy(k:k+1)) / dt_snap(k);
        end
        
        % Enstrophy decay (normalized decay rate)
        analysis.enstrophy_decay_rate = zeros(1, Nsnap-1);
        for k = 1:(Nsnap-1)
            if analysis.enstrophy(k) > 0
                analysis.enstrophy_decay_rate(k) = -diff(analysis.enstrophy(k:k+1)) / analysis.enstrophy(k) / dt_snap(k);
            end
        end
    end
    
    % ===== SUSTAINABILITY INDEX =====
    % Measure of how well the vortex structure is maintained
    if Nsnap > 1
        % Calculate circulation (integral of vorticity)
        analysis.circulation = zeros(1, Nsnap);
        for k = 1:Nsnap
            omega_k = omega_snaps(:,:,k);
            analysis.circulation(k) = sum(omega_k(:)) * dx * dy;
        end
        
        % Circulation decay over simulation
        if abs(analysis.circulation(1)) > 1e-10
            analysis.circulation_decay = 1 - abs(analysis.circulation) ./ abs(analysis.circulation(1));
        else
            analysis.circulation_decay = zeros(1, Nsnap);
        end
        
        % Sustainability index: measure of structure preservation (0=lost, 1=perfect)
        % Based on how well peak vorticity is maintained
        analysis.sustainability_index = analysis.peak_omega_history ./ analysis.peak_omega_history(1);
    else
        analysis.circulation = sum(omega_snaps(:)) * dx * dy;
        analysis.circulation_decay = 0;
        analysis.sustainability_index = 1;
    end
    
    % ===== FINAL STATE DIAGNOSTICS =====
    omega_final = omega_snaps(:,:,end);
    analysis.final_peak_omega = max(abs(omega_final(:)));
    analysis.final_peak_u = max(abs(u_final(:)));
    analysis.final_peak_v = max(abs(v_final(:)));
    analysis.final_kinetic_energy = analysis.kinetic_energy(end);
    analysis.final_enstrophy = analysis.enstrophy(end);
    
    % ===== GRID DIAGNOSTICS =====
    analysis.Nx = Nx;
    analysis.Ny = Ny;
    analysis.dx = dx;
    analysis.dy = dy;
    analysis.grid_points = Nx * Ny;
    analysis.domain_area = (Nx * dx) * (Ny * dy);
    
    % ===== VALIDATE METRICS =====
    if ~isfinite(analysis.peak_abs_omega)
        warning('[METRICS] peak_abs_omega is NaN or Inf!');
    end
    if ~isfinite(analysis.final_kinetic_energy)
        warning('[METRICS] final_kinetic_energy is NaN or Inf!');
    end
    
end

function value = local_pick_logical(s, keys, fallback)
    value = logical(fallback);
    if ~(isstruct(s) && ~isempty(keys))
        return;
    end
    for i = 1:numel(keys)
        key = keys{i};
        if isfield(s, key) && ~isempty(s.(key))
            value = logical(s.(key));
            return;
        end
    end
end

function value = local_pick_text(s, keys, fallback)
    value = char(string(fallback));
    if ~(isstruct(s) && ~isempty(keys))
        return;
    end
    for i = 1:numel(keys)
        key = keys{i};
        if isfield(s, key) && ~isempty(s.(key))
            value = char(string(s.(key)));
            return;
        end
    end
end
