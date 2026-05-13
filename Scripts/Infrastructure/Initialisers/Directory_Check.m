function storage = Directory_Check(repo_root, varargin)
% ensure_results_storage_ready - Create/verify compact Results storage layout
%
% Purpose:
%   Ensure the shared Results root exists before runs/tests start.
%   The legacy top-level Figures tree is no longer created for new writes;
%   run-specific visuals are written lazily under each run root by
%   PathBuilder and the export pipeline.
%
% Inputs:
%   repo_root (optional) - repository root path
%
% Name-Value:
%   'Verbose' (default true) - print created/existing directories summary
%
% Outputs:
%   storage struct with fields:
%     .repo_root
%     .results_root
%     .figures_root
%     .master_table_path
%     .created_dirs
%     .existing_dirs

    p = inputParser;
    addOptional(p, 'repo_root', '', @(x) ischar(x) || isstring(x));
    addParameter(p, 'Verbose', true, @islogical);
    parse(p, repo_root, varargin{:});

    repo_root = char(string(p.Results.repo_root));
    verbose = p.Results.Verbose;

    if isempty(repo_root)
        if exist('PathBuilder', 'class') == 8 || exist('PathBuilder', 'file') == 2
            repo_root = PathBuilder.get_repo_root();
        else
            this_dir = fileparts(mfilename('fullpath'));
            repo_root = fileparts(fileparts(fileparts(this_dir)));
        end
    end

    results_root = fullfile(repo_root, 'Results');
    figures_root = fullfile(repo_root, 'Figures');

    result_dir_list = build_results_layout(results_root);
    [created_results_dirs, existing_results_dirs] = ensure_directory_list(result_dir_list);
    created_figure_dirs = {};
    existing_figure_dirs = {};
    if exist(figures_root, 'dir')
        existing_figure_dirs{end + 1} = figures_root; %#ok<AGROW>
    end

    master_table_path = fullfile(results_root, 'Runs_Table.csv');
    master_parent = fileparts(master_table_path);
    if ~exist(master_parent, 'dir')
        mkdir(master_parent);
        created_results_dirs{end + 1} = master_parent;
    end

    created_dirs = [created_results_dirs, created_figure_dirs];
    existing_dirs = [existing_results_dirs, existing_figure_dirs];

    if verbose
        fprintf('\n[Storage Preflight] Results storage check\n');
        fprintf('  Repo root:      %s\n', repo_root);
        fprintf('  Results root:   %s\n', results_root);
        fprintf('  Figures root:   %s (legacy root not auto-created)\n', figures_root);
        fprintf('  Master CSV dir: %s\n', master_parent);
        fprintf('  Created (Results/Figures): %d / %d\n', ...
            numel(created_results_dirs), numel(created_figure_dirs));
        fprintf('  Existing (Results/Figures): %d / %d\n\n', ...
            numel(existing_results_dirs), numel(existing_figure_dirs));
    end

    storage = struct();
    storage.repo_root = repo_root;
    storage.results_root = results_root;
    storage.figures_root = figures_root;
    storage.master_table_path = master_table_path;
    storage.created_results_dirs = created_results_dirs;
    storage.existing_results_dirs = existing_results_dirs;
    storage.created_figure_dirs = created_figure_dirs;
    storage.existing_figure_dirs = existing_figure_dirs;
    storage.created_dirs = created_dirs;
    storage.existing_dirs = existing_dirs;
end

function dir_list = build_results_layout(results_root)
    dir_list = { ...
        results_root, ...
        fullfile(results_root, 'Phases')};
end

function [created_dirs, existing_dirs] = ensure_directory_list(dir_list)
    created_dirs = {};
    existing_dirs = {};

    for k = 1:numel(dir_list)
        d = dir_list{k};
        if exist(d, 'dir')
            existing_dirs{end + 1} = d; %#ok<AGROW>
        else
            mkdir(d);
            created_dirs{end + 1} = d; %#ok<AGROW>
        end
    end
end
