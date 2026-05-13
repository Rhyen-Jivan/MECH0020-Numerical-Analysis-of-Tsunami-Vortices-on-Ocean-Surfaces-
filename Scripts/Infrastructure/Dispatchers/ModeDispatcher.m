function [Results, paths] = ModeDispatcher(Run_Config, Parameters, Settings)
    % ModeDispatcher - Canonical dispatcher for standard solver-mode runs.
    %
    % Runtime role:
    %   - Owns method/mode routing for standard runs only
    %   - Normalizes user-facing aliases before they reach mode code
    %   - Finalizes run artifacts once the selected mode returns
    %
    % Inputs:
    %   Run_Config - method, mode, ic_type, identifiers
    %   Parameters - physics + numerics
    %   Settings - IO, monitoring, logging
    %
    % Outputs:
    %   Results - simulation/study results
    %   paths - directory structure
    %
    % %//NOTE
    % Phase workflows are owned by RunDispatcher, not by ModeDispatcher.

    [Parameters, Settings] = resolve_mode_resource_policy(Parameters, Settings, Run_Config);
    apply_mode_resource_policy(Settings);

    % Validate required fields
    if ~isfield(Run_Config, 'method')
        ErrorHandler.throw('RUN-EXEC-0001', ...
            'file', mfilename, ...
            'line', 23, ...
            'message', 'Run_Config.method is required but not provided', ...
            'context', struct('Run_Config_fields', fieldnames(Run_Config)));
    end
    if ~isfield(Run_Config, 'mode')
        ErrorHandler.throw('RUN-EXEC-0002', ...
            'file', mfilename, ...
            'line', 29, ...
            'message', 'Run_Config.mode is required but not provided', ...
            'context', struct('Run_Config_fields', fieldnames(Run_Config)));
    end

    method = normalize_method_token(Run_Config.method);
    mode = Run_Config.mode;

    % Normalize mode name
    mode_normalized = normalize_mode_name(mode);
    Parameters = sanitize_vorticity_only_legacy_fields( ...
        Parameters, sprintf('%s dispatch', mode_normalized), 'ModeDispatcher');

    % Accepted method aliases (cell-based matching)
    fd_aliases = {'FD', 'Finite Difference', 'Finite_Difference', 'FiniteDifference'};
    spectral_aliases = {'Spectral', 'FFT', 'PseudoSpectral', 'Spectral Method'};
    fv_aliases = {'FV', 'Finite Volume', 'Finite_Volume', 'FiniteVolume'};
    swe_aliases = {'SWE', 'Shallow Water', 'Shallow_Water', 'ShallowWater'};
    spectral3d_aliases = {'3D Spectral', '3D Spectral Method', '3D FFT', 'Spectral 3D', 'FFT 3D'};
    placeholder_aliases = {'Placeholder', 'Placeholder Method', 'TBD', 'To Be Implemented'};

    % Route to appropriate method/mode handler with structured error handling
    try
        if method_matches(method, fd_aliases)
            Run_Config.method = 'FD';
            [Results, paths] = dispatch_method_mode('FD', mode_normalized, Run_Config, Parameters, Settings);

        elseif method_matches(method, spectral_aliases)
            Run_Config.method = 'Spectral';
            [Results, paths] = dispatch_method_mode('Spectral', mode_normalized, Run_Config, Parameters, Settings);

        elseif method_matches(method, fv_aliases)
            Run_Config.method = 'FV';
            [Results, paths] = dispatch_method_mode('FV', mode_normalized, Run_Config, Parameters, Settings);

        elseif method_matches(method, swe_aliases)
            Run_Config.method = 'SWE';
            [Results, paths] = dispatch_method_mode('SWE', mode_normalized, Run_Config, Parameters, Settings);

        elseif method_matches(method, spectral3d_aliases)
            % 3D Spectral method placeholder - use structured error
            ErrorHandler.throw('SOL-SP-0002', ...
                'file', mfilename, ...
                'line', 71, ...
                'context', struct('requested_method', Run_Config.method));

        elseif method_matches(method, placeholder_aliases)
            % Explicit placeholder method path
            ErrorHandler.throw('SOL-PL-0001', ...
                'file', mfilename, ...
                'line', 78, ...
                'context', struct('requested_method', Run_Config.method));

        else
            % Unknown method - use structured error
            ErrorHandler.throw('RUN-EXEC-0001', ...
                'file', mfilename, ...
                'line', 85, ...
                'context', struct( ...
                    'requested_method', Run_Config.method, ...
                    'valid_methods', {{'FD', 'Spectral', 'FV', 'SWE', '3D Spectral', 'Placeholder'}}, ...
                    'fd_aliases', {fd_aliases}, ...
                    'spectral_aliases', {spectral_aliases}, ...
                    'fv_aliases', {fv_aliases}, ...
                    'swe_aliases', {swe_aliases}, ...
                    'spectral3d_aliases', {spectral3d_aliases}, ...
                    'placeholder_aliases', {placeholder_aliases}));
        end

        % Ensure downstream artifacts always have a stable run identifier.
        Results = attach_run_identifier(Results, Run_Config);

        % Finalize only the minimal saved package on host-owned async runs so
        % the UI can publish immediately, then let richer artifacts finish
        % in the host background queue.
        if should_defer_heavy_artifact_finalization(Settings)
            Results.artifacts = finalize_minimal_artifacts_with_path_fix(Run_Config, Parameters, Settings, Results, paths);
            maybe_emit_standard_completion_payload(Settings, Run_Config, Parameters, Results, paths);
        else
            Results.artifacts = finalize_artifacts_with_path_fix(Run_Config, Parameters, Settings, Results, paths);
        end

    catch ME
        % Wrap any errors from mode execution with context
        err_id = char(string(ME.identifier));
        if startsWith(err_id, 'RUN') || startsWith(err_id, 'SOL')
            % Already a structured error, just rethrow
            rethrow(ME);
        else
            % Unexpected error - wrap with structured error
            ErrorHandler.throw('RUN-EXEC-0003', ...
                'file', mfilename, ...
                'line', 101, ...
                'cause', ME, ...
                'context', struct('method', method, 'mode', mode));
        end
    end
end

function [Parameters, Settings] = resolve_mode_resource_policy(Parameters, Settings, Run_Config)
% resolve_mode_resource_policy - Apply planner output for direct ModeDispatcher launches.
    if nargin < 2 || ~isstruct(Settings)
        return;
    end
    if ~(exist('ExecutionResourcePlanner', 'class') == 8 || exist('ExecutionResourcePlanner', 'file') == 2)
        return;
    end

    mode_token = '';
    method_token = '';
    if nargin >= 3 && isstruct(Run_Config)
        if isfield(Run_Config, 'mode') && ~isempty(Run_Config.mode)
            mode_token = char(string(Run_Config.mode));
        end
        if isfield(Run_Config, 'method') && ~isempty(Run_Config.method)
            method_token = char(string(Run_Config.method));
        end
    end

    try
        planned = ExecutionResourcePlanner.plan(Parameters, Settings, ...
            'ModeToken', mode_token, 'MethodToken', method_token);
        Settings.resource_allocation = planned;
        if ~isstruct(Parameters)
            Parameters = struct();
        end
        Parameters.use_gpu_requested = isfield(Parameters, 'use_gpu') && logical(Parameters.use_gpu);
        Parameters.use_gpu = logical(local_pick_mode_field(planned, {'gpu_enabled_effective'}, false));
        Parameters.pool_workers_requested = local_pick_mode_field(planned, {'pool_workers_requested'}, ...
            local_pick_mode_field(Parameters, {'pool_workers'}, 1));
        Parameters.pool_workers = local_pick_mode_field(planned, {'pool_workers_effective'}, ...
            local_pick_mode_field(Parameters, {'pool_workers'}, 1));
        Parameters.thread_cap = local_pick_mode_field(planned, {'thread_cap'}, ...
            local_pick_mode_field(Parameters, {'thread_cap'}, 1));
    catch
        % Fall back to caller-provided settings when the planner cannot resolve.
    end
end

function value = local_pick_mode_field(s, keys, fallback)
    value = fallback;
    if ~(isstruct(s) && ~isempty(keys))
        return;
    end
    for i = 1:numel(keys)
        key = keys{i};
        if isfield(s, key)
            value = s.(key);
            return;
        end
    end
end

function apply_mode_resource_policy(Settings)
% apply_mode_resource_policy - Best-effort host-side thread/GPU activation.
    if nargin < 1 || ~isstruct(Settings) || ...
            ~isfield(Settings, 'resource_allocation') || ~isstruct(Settings.resource_allocation)
        return;
    end

    policy = Settings.resource_allocation;

    if isfield(policy, 'thread_cap')
        try
            maxNumCompThreads(max(1, round(double(policy.thread_cap))));
        catch
            % Thread caps are best-effort on the active host/worker.
        end
    elseif isfield(policy, 'max_threads')
        try
            maxNumCompThreads(max(1, round(double(policy.max_threads))));
        catch
            % Thread caps are best-effort on the active host/worker.
        end
    end

    if isfield(policy, 'gpu_enabled_effective') && logical(policy.gpu_enabled_effective)
        try
            gpuDevice();
        catch
            % Leave CPU-only execution active when no compatible GPU exists.
        end
    end
end

function Results = attach_run_identifier(Results, Run_Config)
    % Ensure each run has one canonical identifier for reporting/ledger rows.
    if isfield(Results, 'run_id') && ~isempty(Results.run_id)
        return;
    end

    if isfield(Run_Config, 'run_id') && ~isempty(Run_Config.run_id)
        Results.run_id = Run_Config.run_id;
    elseif isfield(Run_Config, 'study_id') && ~isempty(Run_Config.study_id)
        Results.run_id = Run_Config.study_id;
    else
        Results.run_id = RunIDGenerator.generate(Run_Config, struct());
    end
end

function mode_normalized = normalize_mode_name(mode)
    % Normalize mode name to standard format
    mode_lower = lower(mode);
    
    % Map common variations to standard names
    switch mode_lower
        case {'evolution', 'evolve', 'solve'}
            mode_normalized = 'Evolution';
        case {'convergence', 'converge', 'mesh'}
            mode_normalized = 'Convergence';
        case {'parametersweep', 'parameter_sweep', 'sweep', 'param_sweep'}
            mode_normalized = 'ParameterSweep';
        case {'plotting', 'plot', 'visualize', 'visualization'}
            mode_normalized = 'Plotting';
        otherwise
            error('ModeDispatcher:UnsupportedModeAlias', ...
                'Unsupported mode alias "%s". Expected Evolution, Convergence, ParameterSweep, or Plotting.', ...
                char(string(mode)));
    end
end

function tf = method_matches(method, aliases)
    % Return true when method matches any alias in the provided cell array
    tf = false;
    for i = 1:numel(aliases)
        if strcmp(method, normalize_method_token(aliases{i}))
            tf = true;
            return;
        end
    end
end

function method_token = normalize_method_token(method_raw)
    % Normalize user-facing method strings for robust alias matching
    if isstring(method_raw) || ischar(method_raw)
        method_token = char(string(method_raw));
    else
        method_token = '';
    end
    method_token = strtrim(method_token);
    method_token = regexprep(method_token, '[\s_-]+', ' ');
    method_token = upper(method_token);
end

function artifact_summary = finalize_artifacts_with_path_fix(Run_Config, Parameters, Settings, Results, paths)
    % Finalize run artifacts; if manager is missing due to path drift,
    % reroute through PathSetup once and retry.
    try
        artifact_summary = RunArtifactsManager.finalize(Run_Config, Parameters, Settings, Results, paths);
    catch ME
        if ~is_missing_symbol_error(ME)
            rethrow(ME);
        end
        attach_project_paths_from_here();
        artifact_summary = RunArtifactsManager.finalize(Run_Config, Parameters, Settings, Results, paths);
    end
end

function artifact_summary = finalize_minimal_artifacts_with_path_fix(Run_Config, Parameters, Settings, Results, paths)
    try
        artifact_summary = RunArtifactsManager.finalize_minimal(Run_Config, Parameters, Settings, Results, paths);
    catch ME
        if ~is_missing_symbol_error(ME)
            rethrow(ME);
        end
        attach_project_paths_from_here();
        artifact_summary = RunArtifactsManager.finalize_minimal(Run_Config, Parameters, Settings, Results, paths);
    end
end

function tf = should_defer_heavy_artifact_finalization(Settings)
    tf = false;
    if nargin < 1 || ~isstruct(Settings)
        return;
    end
    if exist('defer_heavy_result_artifacts_requested', 'file') ~= 2
        return;
    end
    tf = defer_heavy_result_artifacts_requested(Settings);
end

function maybe_emit_standard_completion_payload(Settings, Run_Config, Parameters, Results, paths)
    if nargin < 5 || ~isstruct(Settings)
        return;
    end
    if should_suppress_standard_completion_payload(Settings, Run_Config)
        return;
    end
    if exist('resolve_runtime_progress_callback', 'file') ~= 2 || ...
            exist('emit_completion_report_payload', 'file') ~= 2
        return;
    end
    progress_callback = resolve_runtime_progress_callback(Settings);
    if isempty(progress_callback)
        return;
    end
    options = struct( ...
        'phase_label', '', ...
        'workflow_kind', '', ...
        'result_layout_kind', '', ...
        'result_publication_mode', local_pick_mode_field(Run_Config, {'result_publication_mode'}, 'manual'), ...
        'completion_results_already_persisted', false);
    emit_completion_report_payload(progress_callback, Results, paths, Run_Config, Parameters, options);
end

function tf = should_suppress_standard_completion_payload(Settings, Run_Config)
    tf = false;
    if isstruct(Settings) && isfield(Settings, 'suppress_standard_completion_payload')
        tf = logical(Settings.suppress_standard_completion_payload);
        if tf
            return;
        end
    end
    if isstruct(Run_Config) && isfield(Run_Config, 'suppress_standard_completion_payload')
        tf = logical(Run_Config.suppress_standard_completion_payload);
    end
end

function attach_project_paths_from_here()
    if exist('PathSetup', 'class') ~= 8
        runner_dir = fileparts(mfilename('fullpath'));       % .../Scripts/Infrastructure/Dispatchers
        scripts_dir = fileparts(fileparts(runner_dir));      % .../Scripts
        infra_dir = fullfile(scripts_dir, 'Infrastructure');
        if exist(infra_dir, 'dir') == 7
            addpath(infra_dir);
        end
        utilities_dir = fullfile(fileparts(scripts_dir), 'utilities');
        if exist(utilities_dir, 'dir') == 7
            addpath(utilities_dir);
        end
    end
    PathSetup.attach_and_verify();
end

function tf = is_missing_symbol_error(ME)
    tf = strcmp(ME.identifier, 'MATLAB:UndefinedFunction') || ...
         strcmp(ME.identifier, 'MATLAB:UndefinedFunctionOrVariable') || ...
         contains(ME.message, 'Undefined function') || ...
         contains(ME.message, 'Unrecognized function or variable');
end

function [Results, paths] = dispatch_method_mode(method_name, mode, Run_Config, Parameters, Settings)
    % Dispatch to method-agnostic mode modules with method-specific gating.
    Run_Config.mode = mode;
    Run_Config.method = method_name;

    try
        switch mode
            case 'Evolution'
                [Results, paths] = mode_evolution(Run_Config, Parameters, Settings);

            case 'Convergence'
                if strcmp(method_name, 'SWE')
                    ErrorHandler.throw('SOL-SW-0001', ...
                        'file', mfilename, ...
                        'line', 28, ...
                        'message', 'Shallow Water convergence is not currently enabled', ...
                        'context', struct('requested_method', Run_Config.method, 'requested_mode', mode));
                end
                [Results, paths] = mode_convergence(Run_Config, Parameters, Settings);

            case 'ParameterSweep'
                if strcmp(method_name, 'Spectral')
                    ErrorHandler.throw('SOL-SP-0001', ...
                        'file', mfilename, ...
                        'line', 39, ...
                        'message', 'Spectral parameter sweep is not enabled in this checkpoint', ...
                        'context', struct('requested_method', Run_Config.method, 'requested_mode', mode));
                elseif strcmp(method_name, 'FV')
                    ErrorHandler.throw('SOL-FV-0001', ...
                        'file', mfilename, ...
                        'line', 45, ...
                        'message', 'Finite Volume parameter sweep is not currently enabled', ...
                        'context', struct('requested_method', Run_Config.method, 'requested_mode', mode));
                elseif strcmp(method_name, 'SWE')
                    ErrorHandler.throw('SOL-SW-0001', ...
                        'file', mfilename, ...
                        'line', 45, ...
                        'message', 'Shallow Water parameter sweep is not currently enabled', ...
                        'context', struct('requested_method', Run_Config.method, 'requested_mode', mode));
                end
                [Results, paths] = mode_parameter_sweep(Run_Config, Parameters, Settings);

            case 'Plotting'
                [Results, paths] = mode_plotting(Run_Config, Parameters, Settings);

            otherwise
                ErrorHandler.throw('RUN-EXEC-0002', ...
                    'file', mfilename, ...
                    'line', 25, ...
                    'context', struct(...
                        'requested_method', method_name, ...
                        'requested_mode', mode, ...
                        'valid_modes', {{'Evolution', 'Convergence', 'ParameterSweep', 'Plotting'}}));
        end

    catch ME
        if contains(ME.identifier, {'RUN', 'SOL', 'CFG', 'IO'})
            rethrow(ME);
        else
            ErrorHandler.throw('RUN-EXEC-0003', ...
                'file', mfilename, ...
                'line', 41, ...
                'cause', ME, ...
                'context', struct('method', method_name, 'mode', mode));
        end
    end
end
