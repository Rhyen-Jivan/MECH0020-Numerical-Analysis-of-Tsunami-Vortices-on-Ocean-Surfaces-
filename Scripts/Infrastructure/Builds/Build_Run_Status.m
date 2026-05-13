function Run_Status = Build_Run_Status(step, time, dt, metrics)
    % Build_Run_Status - Construct Run_Status structure for live updates
    %
    % Purpose:
    %   Centralized builder for Run_Status struct
    %   Used for monitor updates during simulation
    %
    % Inputs:
    %   step - Current timestep number
    %   time - Current physical time
    %   dt - Timestep size
    %   metrics - Struct with derived metrics (CFL, max_omega, etc.)
    %
    % Output:
    %   Run_Status - Live status structure
    %
    % Usage:
    %   metrics = struct('CFL', 0.5, 'max_omega', 1.2e-3);
    %   status = Build_Run_Status(100, 0.1, 0.001, metrics);
    
    Run_Status = struct();
    Run_Status.step = step;
    Run_Status.time = time;
    Run_Status.dt = dt;
    
    % Unpack metrics
    if isfield(metrics, 'CFL')
        Run_Status.CFL = metrics.CFL;
    else
        Run_Status.CFL = NaN;
    end
    
    if isfield(metrics, 'max_omega')
        Run_Status.max_omega = metrics.max_omega;
    else
        Run_Status.max_omega = NaN;
    end
    
    if isfield(metrics, 'mean_omega')
        Run_Status.mean_omega = metrics.mean_omega;
    end
    
    if isfield(metrics, 'enstrophy')
        Run_Status.enstrophy = metrics.enstrophy;
    end
    
    if isfield(metrics, 'energy')
        Run_Status.energy = metrics.energy;
    end
end
