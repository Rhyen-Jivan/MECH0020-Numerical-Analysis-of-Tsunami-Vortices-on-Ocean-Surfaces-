function worker_result = run_post_publication_worker_job(job)
%run_post_publication_worker_job Execute one post-publication worker task.
%
% This helper runs in a background MATLAB worker/process so the UI can
% continue accepting new solver launches while final package persistence and
% artifact finalization continue.

    worker_result = struct( ...
        'ok', false, ...
        'scope', '', ...
        'artifact_summary', struct(), ...
        'error_identifier', '', ...
        'error_message', '');

    if nargin < 1 || ~isstruct(job)
        worker_result.error_identifier = 'run_post_publication_worker_job:InvalidJob';
        worker_result.error_message = 'Worker job payload must be a struct.';
        return;
    end

    worker_result.scope = char(string(local_pick_field(job, 'scope', '')));
    try
        local_attach_project_paths(local_pick_field(job, 'worker_repo_root', ''));
        summary = local_pick_field(job, 'summary', struct());
        switch lower(worker_result.scope)
            case 'persist_completion_package'
                ParallelSimulationExecutor.persist_results_package( ...
                    local_pick_field(summary, 'results', struct()), ...
                    local_pick_field(summary, 'paths', struct()));

            case 'complete_run_artifacts'
                worker_result.artifact_summary = RunArtifactsManager.finalize( ...
                    local_pick_field(summary, 'run_config', struct()), ...
                    local_pick_field(summary, 'parameters', struct()), ...
                    local_pick_field(summary, 'settings', struct()), ...
                    local_pick_field(summary, 'results', struct()), ...
                    local_pick_field(summary, 'paths', struct()));

            otherwise
                error('run_post_publication_worker_job:UnsupportedScope', ...
                    'Unsupported background export scope: %s', worker_result.scope);
        end
        worker_result.ok = true;
    catch ME
        worker_result.ok = false;
        worker_result.error_identifier = char(string(ME.identifier));
        worker_result.error_message = char(string(ME.message));
    end
end

function local_attach_project_paths(repo_root)
    if nargin < 1 || isempty(repo_root)
        repo_root = pwd;
    end
    repo_root = char(string(repo_root));
    if exist('PathSetup', 'class') ~= 8
        scripts_dir = fullfile(repo_root, 'Scripts');
        if exist(scripts_dir, 'dir') == 7
            try
                addpath(genpath(scripts_dir));
            catch ME
                if ~local_is_thread_path_restriction(ME)
                    rethrow(ME);
                end
            end
        end
        utilities_dir = fullfile(repo_root, 'utilities');
        if exist(utilities_dir, 'dir') == 7
            try
                addpath(utilities_dir);
            catch ME
                if ~local_is_thread_path_restriction(ME)
                    rethrow(ME);
                end
            end
        end
    end
    if exist('PathSetup', 'class') == 8
        try
            PathSetup.attach_and_verify();
        catch ME
            if ~local_is_thread_path_restriction(ME)
                rethrow(ME);
            end
        end
    end
end

function tf = local_is_thread_path_restriction(ME)
    identifier = lower(char(string(ME.identifier)));
    message = lower(char(string(ME.message)));
    tf = contains(identifier, 'threadpool') || ...
        contains(message, 'thread-based worker') || ...
        contains(message, 'matlabpath');
end

function value = local_pick_field(s, field_name, fallback)
    value = fallback;
    if isstruct(s) && isfield(s, field_name) && ~isempty(s.(field_name))
        value = s.(field_name);
    end
end
