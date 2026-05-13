function Results = run_agent_guided_convergence(Run_Config, Parameters, Settings, paths, dispatch_info, dispatch_request)
% run_agent_guided_convergence Execute adaptive convergence agent in mode flow.
%
% This wrapper integrates AdaptiveConvergenceAgent into mode_convergence
% without invoking standalone scripts.

    if nargin < 6 || isempty(dispatch_request)
        dispatch_request = ConvergenceAgentDispatcher.build_dispatch_request( ...
            Run_Config, Parameters, Settings, paths, dispatch_info);
    end

    if exist('AdaptiveConvergenceAgent', 'class') ~= 8
        error('run_agent_guided_convergence:MissingAdaptiveAgent', ...
            'AdaptiveConvergenceAgent class is required for agent-guided convergence dispatch.');
    end

    agent_settings = build_agent_settings(Settings, Parameters, paths, dispatch_info, dispatch_request);
    agent = AdaptiveConvergenceAgent(Parameters, agent_settings);

    t_run = tic;
    agent.run_preflight();
    [N_star, trace_table, metadata] = agent.execute_convergence_study();
    total_time = toc(t_run);

    [n_values, metric_values, wall_times, phases] = unpack_trace(trace_table);
    if isempty(n_values) && isfinite(N_star)
        n_values = double(N_star);
        metric_values = NaN;
        wall_times = total_time;
        phases = "adaptive_search";
    end

    level_labels = arrayfun(@(n) sprintf('N%d', round(n)), n_values, 'UniformOutput', false);
    h_values = Parameters.Lx ./ max(n_values, 1);
    convergence_order = estimate_order(h_values, metric_values);

    Results = struct();
    Results.study_id = Run_Config.study_id;
    Results.method = Run_Config.method;
    Results.level_labels = level_labels(:);
    Results.Nx_values = n_values(:);
    Results.Ny_values = n_values(:);
    Results.QoI_values = metric_values(:);
    Results.wall_times = wall_times(:);
    Results.convergence_order = convergence_order;
    Results.total_time = total_time;
    Results.convergence_variable = 'richardson_metric';
    Results.refinement_axis = 'h';
    Results.h_values = h_values(:);
    Results.mesh_sizes = n_values(:);
    Results.convergence_dispatch_strategy = 'agent_guided';
    Results.convergence_objective_mode = dispatch_info.objective_mode;
    Results.convergence_dispatch_request = dispatch_request;
    Results.converged_N = N_star;
    Results.agent_trace_phases = cellstr(phases(:));
    Results.agent_metadata = metadata;
    Results.agent_trace_table = trace_table;

    persist_agent_outputs(Results, paths, Settings);
end

function settings = build_agent_settings(Settings, Parameters, paths, dispatch_info, dispatch_request)
    settings = struct();
    settings.convergence = struct();
    settings.figures = struct();

    settings.convergence.tolerance = pick_tolerance(Parameters);
    if isstruct(dispatch_request) && isfield(dispatch_request, 'target_tolerance')
        settings.convergence.tolerance = double(dispatch_request.target_tolerance);
    end
    settings.convergence.save_iteration_figures = logical(Settings.save_figures);
    settings.convergence.study_dir = paths.data;
    settings.convergence.preflight_figs_dir = fullfile(paths.figures_convergence, 'Preflight');
    settings.convergence.agent_objective_mode = dispatch_info.objective_mode;
    settings.convergence.dispatch_request = dispatch_request;

    settings.figures.close_after_save = true;

    if ~exist(settings.convergence.study_dir, 'dir')
        mkdir(settings.convergence.study_dir);
    end
    if ~exist(settings.convergence.preflight_figs_dir, 'dir')
        mkdir(settings.convergence.preflight_figs_dir);
    end
end

function tol = pick_tolerance(Parameters)
    tol = 5e-2;
    if isfield(Parameters, 'convergence') && isstruct(Parameters.convergence) && ...
            isfield(Parameters.convergence, 'study') && isstruct(Parameters.convergence.study) && ...
            isfield(Parameters.convergence.study, 'tolerance') && ...
            isnumeric(Parameters.convergence.study.tolerance) && ...
            isfinite(Parameters.convergence.study.tolerance) && ...
            Parameters.convergence.study.tolerance > 0
        tol = double(Parameters.convergence.study.tolerance);
        return;
    end
    if isfield(Parameters, 'convergence_tol') && isnumeric(Parameters.convergence_tol) && ...
            isfinite(Parameters.convergence_tol) && Parameters.convergence_tol > 0
        tol = double(Parameters.convergence_tol);
    end
end

function [n_values, metric_values, wall_times, phases] = unpack_trace(trace_table)
    n_values = [];
    metric_values = [];
    wall_times = [];
    phases = strings(0, 1);

    if ~istable(trace_table) || isempty(trace_table)
        return;
    end

    if ismember('N', trace_table.Properties.VariableNames)
        n_values = double(trace_table.N(:));
    end
    if ismember('metric', trace_table.Properties.VariableNames)
        metric_values = double(trace_table.metric(:));
    end
    if ismember('wall_time', trace_table.Properties.VariableNames)
        wall_times = double(trace_table.wall_time(:));
    end
    if ismember('phase', trace_table.Properties.VariableNames)
        phases = string(trace_table.phase(:));
    end

    n_rows = height(trace_table);
    if isempty(n_values), n_values = nan(n_rows, 1); end
    if isempty(metric_values), metric_values = nan(n_rows, 1); end
    if isempty(wall_times), wall_times = nan(n_rows, 1); end
    if isempty(phases), phases = repmat("adaptive_search", n_rows, 1); end
end

function order = estimate_order(h_values, metric_values)
    order = NaN;
    valid = isfinite(h_values) & h_values > 0 & isfinite(metric_values) & metric_values > 0;
    if nnz(valid) < 2
        return;
    end
    p = polyfit(log(h_values(valid)), log(metric_values(valid)), 1);
    order = p(1);
end

function persist_agent_outputs(Results, paths, Settings)
    if ~logical(Settings.save_data)
        return;
    end

    safe_save_mat(fullfile(paths.data, 'convergence_results_agent_guided.mat'), ...
        struct('Results', Results), '-v7.3');
    if isfield(Results, 'agent_trace_table') && istable(Results.agent_trace_table) && ~isempty(Results.agent_trace_table)
        writetable(Results.agent_trace_table, fullfile(paths.data, 'convergence_trace_agent_guided.csv'));
    end
end
