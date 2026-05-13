classdef PathSetup
    % PathSetup - Canonical runtime path gate for the repository.
    %
    % Attaches active runtime paths, prunes legacy/archive trees, and
    % verifies that the required bootstrap manifest is reachable. Call once
    % at each entry point before any project code runs.
    %
    % Usage:
    %   PathSetup.attach();            % Attach paths only
    %   PathSetup.verify();            % Check manifest (paths must already be set)
    %   PathSetup.attach_and_verify(); % Both at once (recommended)
    %
    % Bootstrap note:
    %   attach() resolves the project root from PathSetup.m's own location via
    %   mfilename('fullpath'), so it works even when called as the very first
    %   project function (no other paths need to be set first).
    %
    % %//NOTE
    % The pruning policy in pruned_genpath() is part of the active runtime
    % contract. Legacy/reference trees should be moved under paths that this
    % class already excludes, not reintroduced onto the default runtime path.

    methods (Static)

        function repo_root = attach()
            % Attach all Scripts/ subdirectories to the MATLAB path.
            % Returns repo_root so callers that need it do not repeat the logic.

            this_dir   = fileparts(mfilename('fullpath'));   % .../Scripts/Infrastructure
            scripts_dir = fileparts(this_dir);               % .../Scripts
            repo_root   = fileparts(scripts_dir);            % project root

            addpath(PathSetup.pruned_genpath(scripts_dir));

            % Also add top-level utilities/ folder if present.
            utilities_dir = fullfile(repo_root, 'utilities');
            if exist(utilities_dir, 'dir') == 7
                addpath(utilities_dir);
            end
        end

        function verify()
            % Verify that every required function/class is reachable on the path.
            % Errors loudly, listing every missing item and the fix to apply.

            manifest = PathSetup.required_manifest();
            missing  = {};

            for i = 1:numel(manifest)
                name = manifest{i};
                if exist(name, 'file') ~= 2 && exist(name, 'class') ~= 8
                    missing{end + 1} = name; %#ok<AGROW>
                end
            end

            if ~isempty(missing)
                lines = cell(numel(missing) + 3, 1);
                lines{1} = sprintf('PathSetup.verify: %d required item(s) missing from MATLAB path:', numel(missing));
                for i = 1:numel(missing)
                    lines{i + 1} = sprintf('  (%d) %s', i, missing{i});
                end
                lines{end - 1} = '';
                lines{end} = 'Fix: run PathSetup.attach() before calling any project code, or call PathSetup.attach_and_verify() at your entry point.';
                error('PathSetup:MissingFiles', '%s', strjoin(lines, newline));
            end
        end

        function repo_root = attach_and_verify()
            % Attach all paths then verify the manifest.  Recommended for all entry points.
            repo_root = PathSetup.attach();
            PathSetup.verify();
        end

    end

    methods (Static, Access = private)

        function path_str = pruned_genpath(root_dir)
            % Build a genpath string while excluding legacy/archive trees.
            raw = genpath(root_dir);
            parts = regexp(raw, pathsep, 'split');
            keep = {};
            for i = 1:numel(parts)
                p = strtrim(parts{i});
                if isempty(p)
                    continue;
                end
                p_norm = strrep(lower(p), '\', '/');
                if contains(p_norm, '/scripts/legacy') || contains(p_norm, '/archive/') || ...
                        contains(p_norm, '/legacy_') || contains(p_norm, '/legacy/')
                    continue;
                end
                keep{end + 1} = p; %#ok<AGROW>
            end
            path_str = strjoin(keep, pathsep);
        end

        function items = required_manifest()
            % Canonical list of functions/classes required by active runtime entrypoints.

            % Dispatchers (canonical source-of-truth; no legacy wrappers)
            dispatchers = {'ICDispatcher'; 'BCDispatcher'; 'ModeDispatcher'};

            % Infrastructure
            infra = {'PathBuilder'; 'RunIDGenerator'; 'MonitorInterface'; 'ProgressBar'; ...
                     'ColorPrintf'; 'filter_graphics_objects'; ...
                     'Directory_Check'; 'create_default_parameters'};

            % Utilities (pipeline path is canonical for runtime reporting)
            utils = {'RunReportPipeline'; 'RunArtifactsManager'; 'EnhancedReportGenerator'; ...
                     'MethodConfigBuilder'; 'ExecutionResourcePlanner'; ...
                     'merge_structs'};

            % Solvers / shared
            solvers = {'extract_unified_metrics'};

            % Methods
            methods_list = {'FiniteDifferenceMethod'; 'SpectralMethod'; 'FiniteVolumeMethod'; 'ShallowWaterMethod'};

            % Modes
            modes = {'mode_evolution'};

            % UI entry
            ui = {'UIController'};

            items = [dispatchers; infra; utils; solvers; methods_list; modes; ui]';
        end

    end
end
