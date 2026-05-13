function [Results, paths] = MeshConvergenceStudy(Run_Config, Parameters, Settings)
% MeshConvergenceStudy - Dedicated periodic FD vs Spectral mesh workflow.
%
% This workflow reuses the Phase 1 local mesh-sweep engine while publishing
% a dedicated workflow kind and result layout.

    if nargin < 1 || ~isstruct(Run_Config)
        Run_Config = struct();
    end
    Run_Config.workflow_kind = 'mesh_convergence_study';
    Run_Config.result_layout_kind = 'mesh_convergence_workflow';
    Run_Config.phase_label = 'Mesh Convergence';
    Run_Config.launch_origin = 'phase_button';
    [Results, paths] = Phase1PeriodicComparison(Run_Config, Parameters, Settings);
end
