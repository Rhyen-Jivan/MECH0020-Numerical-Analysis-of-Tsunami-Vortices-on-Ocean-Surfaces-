function Run_Config = Build_Run_Config(method, mode, ic_type, varargin)
    % Build_Run_Config - Construct Run_Config structure
    %
    % Purpose:
    %   Centralized builder for Run_Config struct
    %   Ensures consistent field naming and validation
    %
    % Required Inputs:
    %   method - 'FD', 'FFT', 'FV'
    %   mode - 'Evolution', 'Convergence', 'ParameterSweep', 'Plotting'
    %   ic_type - Initial condition type (e.g., 'Lamb-Oseen')
    %
    % Optional Name-Value Pairs:
    %   'run_id' - Specific run ID (auto-generated if not provided)
    %   'study_id' - Study ID for Convergence/ParameterSweep
    %   'source_run_id' - Source for Plotting mode
    %
    % Output:
    %   Run_Config - Structured configuration
    %
    % Usage:
    %   cfg = Build_Run_Config('FD', 'Evolution', 'Lamb-Oseen');
    %   cfg = Build_Run_Config('FD', 'Convergence', 'Gaussian', 'study_id', 'conv_001');
    
    % Parse optional arguments
    p = inputParser;
    addParameter(p, 'run_id', '', @ischar);
    addParameter(p, 'study_id', '', @ischar);
    addParameter(p, 'source_run_id', '', @ischar);
    parse(p, varargin{:});
    
    % Build Run_Config
    Run_Config = struct();
    Run_Config.method = method;
    Run_Config.mode = mode;
    Run_Config.ic_type = ic_type;
    
    % Add optional fields
    if ~isempty(p.Results.run_id)
        Run_Config.run_id = p.Results.run_id;
    end
    if ~isempty(p.Results.study_id)
        Run_Config.study_id = p.Results.study_id;
    end
    if ~isempty(p.Results.source_run_id)
        Run_Config.source_run_id = p.Results.source_run_id;
    end
end
