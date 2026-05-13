function [Results, paths] = RunDispatcher(Run_Config, Parameters, Settings)
% RunDispatcher - Canonical top-level dispatcher for standard runs and workflows.
%
% Runtime role:
%   - Shared backend dispatcher used by the UI/executor path
%   - Routes phase workflows to their workflow owners
%   - Routes standard runs to ModeDispatcher
%
% Supported workflow kinds:
%   - ''                               -> standard ModeDispatcher path
%   - mesh_convergence_study           -> MeshConvergenceStudy
%   - phase1_periodic_comparison       -> Phase1PeriodicComparison
%   - phase2_boundary_condition_study  -> Phase2BoundaryConditionStudy
%   - phase3_bathymetry_study          -> Phase3BathymetryStudy
%
% %//NOTE
% New workflow launch surfaces should route through RunDispatcher instead of
% bypassing it. This keeps the canonical UI-driven path intact.

    [Parameters, Settings] = resolve_runtime_resource_policy(Parameters, Settings, Run_Config);
    apply_runtime_resource_policy(Settings);

    workflow_kind = '';
    if isstruct(Run_Config) && isfield(Run_Config, 'workflow_kind') && ~isempty(Run_Config.workflow_kind)
        workflow_kind = lower(char(string(Run_Config.workflow_kind)));
    end

    switch workflow_kind
        case ''
            [Results, paths] = ModeDispatcher(Run_Config, Parameters, Settings);
        case 'mesh_convergence_study'
            [Results, paths] = MeshConvergenceStudy(Run_Config, Parameters, Settings);
        case 'phase1_periodic_comparison'
            [Results, paths] = Phase1PeriodicComparison(Run_Config, Parameters, Settings);
        case 'phase2_boundary_condition_study'
            [Results, paths] = Phase2BoundaryConditionStudy(Run_Config, Parameters, Settings);
        case 'phase3_bathymetry_study'
            [Results, paths] = Phase3BathymetryStudy(Run_Config, Parameters, Settings);
        otherwise
            error('RunDispatcher:UnsupportedWorkflowKind', ...
                'Unsupported workflow_kind "%s" for RunDispatcher.', workflow_kind);
    end
end

function [Parameters, Settings] = resolve_runtime_resource_policy(Parameters, Settings, Run_Config)
% resolve_runtime_resource_policy - Apply planner output before dispatch.
    if nargin < 2 || ~isstruct(Settings)
        return;
    end
    if ~(exist('ExecutionResourcePlanner', 'class') == 8 || exist('ExecutionResourcePlanner', 'file') == 2)
        return;
    end

    mode_token = '';
    method_token = '';
    if nargin >= 3 && isstruct(Run_Config)
        if isfield(Run_Config, 'workflow_kind') && ~isempty(Run_Config.workflow_kind)
            mode_token = char(string(Run_Config.workflow_kind));
        elseif isfield(Run_Config, 'mode') && ~isempty(Run_Config.mode)
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
        Parameters.use_gpu = logical(local_pick_field(planned, {'gpu_enabled_effective'}, false));
        Parameters.pool_workers_requested = local_pick_field(planned, {'pool_workers_requested'}, ...
            local_pick_field(Parameters, {'pool_workers'}, 1));
        Parameters.pool_workers = local_pick_field(planned, {'pool_workers_effective'}, ...
            local_pick_field(Parameters, {'pool_workers'}, 1));
        Parameters.thread_cap = local_pick_field(planned, {'thread_cap'}, ...
            local_pick_field(Parameters, {'thread_cap'}, 1));
    catch
        % Fall back to the caller-provided settings when the planner cannot resolve.
    end
end

function value = local_pick_field(s, keys, fallback)
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

function apply_runtime_resource_policy(Settings)
% apply_runtime_resource_policy - Best-effort host-side thread/GPU activation.
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
