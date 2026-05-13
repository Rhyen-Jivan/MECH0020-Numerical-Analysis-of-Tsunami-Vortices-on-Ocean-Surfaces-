classdef PathBuilder
    % PathBuilder - Canonical run-path contract for Results artifacts.
    %
    % Compact layout for all new writes:
    %   Standard runs:
    %       Results/<Method>/<StorageId>/
    %           Run_Settings.txt
    %           Data/
    %           Metrics/
    %           Visuals/
    %
    %   Phase runs:
    %       Results/Phases/<Phase>/<StorageId>/
    %           Run_Settings.txt
    %           Data/
    %           Metrics/
    %           Visuals/
    %
    % Compatibility aliases still expose legacy field names so existing
    % writers can keep using paths.config / paths.data / paths.figures_*.

    methods (Static)
        function paths = get_run_paths(method, mode, identifier, varargin)
            % get_run_paths - Build compact directory map for one run/study.
            %
            % Inputs:
            %   method     - user-facing token (FD/Spectral/FV/Bathymetry/...)
            %   mode       - user-facing token (Evolution/Convergence/...)
            %   identifier - run_id / study_id / plotting job id
            %
            % Optional:
            %   output_root (default "Results")

            if nargin >= 4 && ~isempty(varargin{1})
                output_root = char(string(varargin{1}));
            else
                output_root = 'Results';
            end

            repo_root = PathBuilder.get_repo_root();
            [results_root, output_root] = PathBuilder.resolve_output_root(repo_root, output_root);
            method_token = PathBuilder.normalize_method_token(method);
            mode_token = PathBuilder.normalize_mode_token(mode);

            run_parent_root = fullfile(results_root, method_token);
            storage_id = PathBuilder.resolve_storage_identifier(identifier, run_parent_root);
            base_path = fullfile(run_parent_root, storage_id);

            matlab_data_root = fullfile(base_path, 'Data');
            metrics_root = fullfile(base_path, 'Metrics');
            visuals_root = fullfile(base_path, 'Visuals');
            comparisons_root = fullfile(visuals_root, 'Comparisons');
            evolutions_root = fullfile(visuals_root, 'Evolutions');
            collectors_visuals_root = fullfile(visuals_root, 'Collectors');
            method_data_root = matlab_data_root;
            method_metrics_root = metrics_root;
            method_visuals_root = evolutions_root;

            figure_stem_map = PathBuilder.compact_figure_stem_map(method_token);
            pane_media_stem_map = PathBuilder.compact_pane_media_stem_map(method_token);

            paths = struct();
            paths.repo_root = repo_root;
            paths.output_root = output_root;
            paths.output_root_resolved = results_root;
            paths.results_root = results_root;
            paths.base = base_path;
            paths.method = method_token;
            paths.mode = mode_token;
            paths.identifier = identifier;
            paths.storage_id = storage_id;
            paths.artifact_layout_version = 'compact_v3';
            paths.run_settings_path = fullfile(base_path, 'Run_Settings.txt');
            paths.manifest_path = fullfile(matlab_data_root, 'artifact_manifest.json');
            paths.run_data_workbook_path = fullfile(metrics_root, 'Run_Data.xlsx');
            paths.raw_hwinfo_csv_path = fullfile(metrics_root, 'HWiNFO_Telemetry.csv');
            paths.matlab_data_root = matlab_data_root;
            paths.metrics_root = metrics_root;
            paths.visuals_root = visuals_root;
            paths.method_data_root = method_data_root;
            paths.method_metrics_root = method_metrics_root;
            paths.method_visuals_root = method_visuals_root;
            paths.export_file_stem = method_token;
            paths.animation_base_stem = method_token;
            paths.figure_stem_map = figure_stem_map;
            paths.pane_media_stem_map = pane_media_stem_map;
            paths.media_flatten_pane_dirs = true;
            paths.disable_combined_animation_dir = true;

            % Canonical shallow roots.
            paths.config = method_data_root;
            paths.data = method_data_root;
            paths.figures_root = visuals_root;
            paths.reports = method_metrics_root;
            paths.logs = fullfile(metrics_root, 'Logs');
            paths.sustainability = metrics_root;
            paths.sustainability_collectors = metrics_root;

            % Shallow public branches.
            paths.figures = method_visuals_root;
            paths.figures_comparisons = comparisons_root;
            paths.figures_main_plots = evolutions_root;
            paths.figures_diagnostics = evolutions_root;
            paths.figures_evolution_root = evolutions_root;
            paths.figures_evolution_combined = evolutions_root;
            paths.figures_evolution_evolution = evolutions_root;
            paths.figures_evolution_contour = evolutions_root;
            paths.figures_evolution_vector = evolutions_root;
            paths.figures_evolution_streamlines = evolutions_root;
            paths.figures_evolution_streamfunction = evolutions_root;
            paths.figures_evolution_velocity = evolutions_root;
            paths.figures_evolution_wall_vorticity = evolutions_root;
            paths.figures_collectors_root = collectors_visuals_root;
            paths.figures_collectors_matlab = collectors_visuals_root;
            paths.figures_collectors_hwinfo = collectors_visuals_root;
            paths.figures_collectors_icue = collectors_visuals_root;
            paths.figures_stages = visuals_root;
            paths.logs_runtime = fullfile(metrics_root, 'Logs');
            paths.logs_status = fullfile(metrics_root, 'Logs');

            % Backward-compat aliases.
            paths.media = '';
            paths.media_animation = paths.figures_evolution_root;
            paths.media_animation_combined = paths.figures_evolution_combined;
            paths.media_animation_panes = paths.figures_evolution_root;
            paths.media_frames = '';
            paths.figures_evolution = paths.figures_evolution_evolution;
            paths.figures_streamfunction = paths.figures_evolution_streamfunction;
            paths.figures_velocity = paths.figures_evolution_velocity;
            paths.figures_contours = paths.figures_evolution_contour;
            paths.figures_vector = paths.figures_evolution_vector;
            paths.figures_streamlines = paths.figures_evolution_streamlines;
            paths.figures_wall_vorticity = paths.figures_evolution_wall_vorticity;
            paths.figures_animation = paths.figures_evolution_root;

            switch upper(mode_token)
                case 'EVOLUTION'
                case 'CONVERGENCE'
                    paths.figures_convergence = comparisons_root;
                    paths.figures_iterations = comparisons_root;
                    paths.figures_refined_meshes = comparisons_root;
                    % Legacy aliases retained for old scripts/readers.
                    paths.evolution = paths.figures_evolution;
                    paths.mesh_contours = comparisons_root;
                    paths.mesh_grids = comparisons_root;
                    paths.mesh_plots = comparisons_root;
                    paths.convergence_metrics = comparisons_root;

                case 'PARAMETERSWEEP'
                    paths.figures_sweep = comparisons_root;

                case 'PLOTTING'
                otherwise
                    ErrorHandler.throw('RUN-EXEC-0002', ...
                        'file', 'PathBuilder', ...
                        'line', 92, ...
                        'context', struct( ...
                            'requested_mode', mode, ...
                            'valid_modes', {{'Evolution', 'Convergence', 'ParameterSweep', 'Plotting'}}));
            end
        end

        function paths = get_phase_paths(phase_name, identifier, varargin)
            % get_phase_paths - Build compact phase workflow artifact roots.
            %
            % Layout:
            %   Results/Phases/<Phase>/<StorageId>/
            %       Run_Settings.txt
            %       Data/
            %       Metrics/
            %       Visuals/

            if nargin >= 3 && ~isempty(varargin{1})
                output_root = char(string(varargin{1}));
            else
                output_root = 'Results';
            end

            repo_root = PathBuilder.get_repo_root();
            [results_root, output_root] = PathBuilder.resolve_output_root(repo_root, output_root);
            phase_token = PathBuilder.normalize_phase_token(phase_name);
            phase_parent_root = fullfile(results_root, 'Phases', phase_token);
            storage_id = PathBuilder.resolve_storage_identifier(identifier, phase_parent_root);
            base_path = fullfile(phase_parent_root, storage_id);

            matlab_data_root = fullfile(base_path, 'Data');
            metrics_root = fullfile(base_path, 'Metrics');
            sustainability_root = metrics_root;
            if strcmpi(phase_token, 'MeshConvergence')
                sustainability_root = fullfile(base_path, 'Sustainability');
            end
            visuals_root = fullfile(base_path, 'Visuals');
            comparisons_root = fullfile(visuals_root, 'Comparisons');
            evolutions_root = fullfile(visuals_root, 'Evolutions');
            collectors_visuals_root = fullfile(visuals_root, 'Collectors');

            paths = struct();
            paths.repo_root = repo_root;
            paths.output_root = output_root;
            paths.output_root_resolved = results_root;
            paths.results_root = results_root;
            paths.phase = phase_token;
            paths.phase_id = char(string(identifier));
            paths.storage_id = storage_id;
            paths.base = base_path;
            paths.artifact_layout_version = 'compact_v3';
            paths.run_settings_path = fullfile(base_path, 'Run_Settings.txt');
            paths.run_data_workbook_path = fullfile(sustainability_root, 'Run_Data.xlsx');
            paths.raw_hwinfo_csv_path = fullfile(sustainability_root, 'HWiNFO_Telemetry.csv');
            paths.stage_boundaries_csv_path = fullfile(sustainability_root, 'Telemetry_Stage_Boundaries.csv');
            paths.matlab_data_root = matlab_data_root;
            paths.metrics_root = metrics_root;
            paths.visuals_root = visuals_root;

            % Compatibility aliases for existing phase/runtime writers.
            paths.config = matlab_data_root;
            paths.data = matlab_data_root;
            paths.figures = evolutions_root;
            paths.figures_root = visuals_root;
            paths.figures_comparisons = comparisons_root;
            paths.figures_main_plots = evolutions_root;
            paths.figures_diagnostics = evolutions_root;
            paths.figures_evolution_root = evolutions_root;
            paths.figures_evolution_combined = evolutions_root;
            paths.figures_evolution_evolution = evolutions_root;
            paths.figures_evolution_contour = evolutions_root;
            paths.figures_evolution_vector = evolutions_root;
            paths.figures_evolution_streamlines = evolutions_root;
            paths.figures_evolution_streamfunction = evolutions_root;
            paths.figures_evolution_velocity = evolutions_root;
            paths.figures_evolution_wall_vorticity = evolutions_root;
            paths.figures_collectors_root = collectors_visuals_root;
            paths.figures_collectors_matlab = collectors_visuals_root;
            paths.figures_collectors_hwinfo = collectors_visuals_root;
            paths.figures_collectors_icue = collectors_visuals_root;
            paths.figures_stages = visuals_root;
            paths.reports = metrics_root;
            paths.logs = fullfile(metrics_root, 'Logs');
            paths.sustainability = sustainability_root;
            paths.sustainability_collectors = sustainability_root;
            % Phase child jobs now live directly under the phase root to
            % avoid the extra path depth that previously triggered Windows
            % long-path failures during mesh-level worker runs.
            paths.runs_root = base_path;
            paths.manifest_path = fullfile(matlab_data_root, 'artifact_manifest.json');
            paths.media_flatten_pane_dirs = true;
            paths.disable_combined_animation_dir = true;
            paths.export_file_stem = phase_token;
            paths.animation_base_stem = phase_token;

            paths.media = '';
            paths.media_animation = paths.figures_evolution_root;
            paths.media_animation_combined = paths.figures_evolution_combined;
            paths.media_animation_panes = paths.figures_evolution_root;
            paths.media_frames = '';
            paths.figures_evolution = paths.figures_evolution_evolution;
            paths.figures_streamfunction = paths.figures_evolution_streamfunction;
            paths.figures_velocity = paths.figures_evolution_velocity;
            paths.figures_contours = paths.figures_evolution_contour;
            paths.figures_vector = paths.figures_evolution_vector;
            paths.figures_streamlines = paths.figures_evolution_streamlines;
            paths.figures_wall_vorticity = paths.figures_evolution_wall_vorticity;
            paths.figures_animation = paths.figures_evolution_root;
        end

        function paths = get_existing_root_paths(base_root, method, mode)
            % get_existing_root_paths - Map a preinitialized artifact root
            % to the canonical shallow run-path contract without allocating
            % another dated storage folder beneath it.

            if nargin < 2 || isempty(method)
                method = '';
            end
            if nargin < 3 || isempty(mode)
                mode = 'Evolution';
            end

            repo_root = PathBuilder.get_repo_root();
            method_token = PathBuilder.normalize_method_token(method);
            mode_token = PathBuilder.normalize_mode_token(mode);
            base_root = char(string(base_root));
            matlab_data_root = fullfile(base_root, 'Data');
            metrics_root = fullfile(base_root, 'Metrics');
            visuals_root = fullfile(base_root, 'Visuals');
            comparisons_root = fullfile(visuals_root, 'Comparisons');
            evolutions_root = fullfile(visuals_root, 'Evolutions');
            collectors_visuals_root = fullfile(visuals_root, 'Collectors');
            storage_id = string(base_root);
            [~, leaf_name] = fileparts(base_root);
            if strlength(string(leaf_name)) ~= 0
                storage_id = string(leaf_name);
            end

            figure_stem_map = PathBuilder.compact_figure_stem_map(method_token);
            pane_media_stem_map = PathBuilder.compact_pane_media_stem_map(method_token);

            paths = struct();
            paths.repo_root = repo_root;
            paths.output_root = base_root;
            paths.output_root_resolved = base_root;
            paths.results_root = fileparts(base_root);
            paths.base = base_root;
            paths.method = method_token;
            paths.mode = mode_token;
            paths.identifier = base_root;
            paths.storage_id = char(storage_id);
            paths.artifact_layout_version = 'compact_v3';
            paths.run_settings_path = fullfile(base_root, 'Run_Settings.txt');
            paths.manifest_path = fullfile(matlab_data_root, 'artifact_manifest.json');
            paths.run_data_workbook_path = fullfile(metrics_root, 'Run_Data.xlsx');
            paths.raw_hwinfo_csv_path = fullfile(metrics_root, 'HWiNFO_Telemetry.csv');
            paths.matlab_data_root = matlab_data_root;
            paths.metrics_root = metrics_root;
            paths.visuals_root = visuals_root;
            paths.method_data_root = matlab_data_root;
            paths.method_metrics_root = metrics_root;
            paths.method_visuals_root = evolutions_root;
            paths.export_file_stem = method_token;
            paths.animation_base_stem = method_token;
            paths.figure_stem_map = figure_stem_map;
            paths.pane_media_stem_map = pane_media_stem_map;
            paths.media_flatten_pane_dirs = true;
            paths.disable_combined_animation_dir = true;
            paths.preinitialized_root = true;

            paths.config = matlab_data_root;
            paths.data = matlab_data_root;
            paths.figures_root = visuals_root;
            paths.reports = metrics_root;
            paths.logs = fullfile(metrics_root, 'Logs');
            paths.sustainability = metrics_root;
            paths.sustainability_collectors = metrics_root;

            paths.figures = evolutions_root;
            paths.figures_comparisons = comparisons_root;
            paths.figures_main_plots = evolutions_root;
            paths.figures_diagnostics = evolutions_root;
            paths.figures_evolution_root = evolutions_root;
            paths.figures_evolution_combined = evolutions_root;
            paths.figures_evolution_evolution = evolutions_root;
            paths.figures_evolution_contour = evolutions_root;
            paths.figures_evolution_vector = evolutions_root;
            paths.figures_evolution_streamlines = evolutions_root;
            paths.figures_evolution_streamfunction = evolutions_root;
            paths.figures_evolution_velocity = evolutions_root;
            paths.figures_evolution_wall_vorticity = evolutions_root;
            paths.figures_collectors_root = collectors_visuals_root;
            paths.figures_collectors_matlab = collectors_visuals_root;
            paths.figures_collectors_hwinfo = collectors_visuals_root;
            paths.figures_collectors_icue = collectors_visuals_root;
            paths.figures_stages = visuals_root;
            paths.logs_runtime = fullfile(metrics_root, 'Logs');
            paths.logs_status = fullfile(metrics_root, 'Logs');

            paths.media = '';
            paths.media_animation = paths.figures_evolution_root;
            paths.media_animation_combined = paths.figures_evolution_combined;
            paths.media_animation_panes = paths.figures_evolution_root;
            paths.media_frames = '';
            paths.figures_evolution = paths.figures_evolution_evolution;
            paths.figures_streamfunction = paths.figures_evolution_streamfunction;
            paths.figures_velocity = paths.figures_evolution_velocity;
            paths.figures_contours = paths.figures_evolution_contour;
            paths.figures_vector = paths.figures_evolution_vector;
            paths.figures_streamlines = paths.figures_evolution_streamlines;
            paths.figures_wall_vorticity = paths.figures_evolution_wall_vorticity;
            paths.figures_animation = paths.figures_evolution_root;

            switch upper(mode_token)
                case 'EVOLUTION'
                case 'CONVERGENCE'
                    paths.figures_convergence = comparisons_root;
                    paths.figures_iterations = comparisons_root;
                    paths.figures_refined_meshes = comparisons_root;
                    paths.evolution = paths.figures_evolution;
                    paths.mesh_contours = comparisons_root;
                    paths.mesh_grids = comparisons_root;
                    paths.mesh_plots = comparisons_root;
                    paths.convergence_metrics = comparisons_root;
                case 'PARAMETERSWEEP'
                    paths.figures_sweep = comparisons_root;
                case 'PLOTTING'
                otherwise
                    ErrorHandler.throw('RUN-EXEC-0002', ...
                        'file', 'PathBuilder', ...
                        'line', 269, ...
                        'context', struct( ...
                            'requested_mode', mode, ...
                            'valid_modes', {{'Evolution', 'Convergence', 'ParameterSweep', 'Plotting'}}));
            end
        end

        function ensure_directories(paths)
            % ensure_directories - Idempotently create directory fields.

            create_fields = { ...
                'base', 'matlab_data_root', 'metrics_root', 'visuals_root', ...
                'method_data_root', 'method_metrics_root', 'method_visuals_root', ...
                'config', 'data', 'reports', 'logs', 'sustainability', 'sustainability_collectors', ...
                'figures_root', 'figures', 'figures_comparisons', 'figures_main_plots', ...
                'figures_diagnostics', 'figures_evolution_root', 'figures_collectors_root', ...
                'figures_collectors_matlab', 'figures_collectors_hwinfo', 'figures_collectors_icue', ...
                'levels_root', ...
                'runs_root'};

            for i = 1:numel(create_fields)
                field_name = create_fields{i};
                if ~isfield(paths, field_name)
                    continue;
                end

                target = paths.(field_name);
                if ~(ischar(target) || isstring(target))
                    continue;
                end
                target = char(string(target));
                if isempty(target)
                    continue;
                end

                if ~exist(target, 'dir')
                    try
                        mkdir(target);
                    catch ME
                        ErrorHandler.throw('IO-FS-0001', ...
                            'file', 'PathBuilder', ...
                            'line', 128, ...
                            'cause', ME, ...
                            'context', struct('target_directory', target));
                    end
                end
            end

            PathBuilder.ensure_run_data_workbook_placeholder(paths);
        end

        function ensure_run_settings_placeholder(settings_path, logical_id)
            % ensure_run_settings_placeholder - Preseed a compact
            % Run_Settings.txt file for preinitialized queue roots.
            settings_path = char(string(settings_path));
            logical_id = char(string(logical_id));
            if isempty(settings_path) || exist(settings_path, 'file') == 2
                return;
            end
            write_run_settings_text(settings_path, 'Storage Reservation', struct( ...
                'logical_id', logical_id, ...
                'reserved_at', char(datetime('now', 'Format', 'yyyy-MM-dd HH:mm:ss'))));
        end

        function [paths, param_path] = add_parameter_dir(paths, param_name)
            % add_parameter_dir - Add dynamic per-parameter folders for sweeps.

            if ~strcmpi(paths.mode, 'ParameterSweep')
                ErrorHandler.throw('RUN-EXEC-0002', ...
                    'file', 'PathBuilder', ...
                    'line', 141, ...
                    'message', 'add_parameter_dir only valid for ParameterSweep mode', ...
                    'context', struct('current_mode', paths.mode));
            end

            param_token = matlab.lang.makeValidName(char(string(param_name)));
            param_path = fullfile(paths.base, 'Data', param_token);
            paths.(param_token) = param_path;

            if ~exist(param_path, 'dir')
                try
                    mkdir(param_path);
                    mkdir(fullfile(param_path, 'Figures'));
                catch ME
                    ErrorHandler.throw('IO-FS-0001', ...
                        'file', 'PathBuilder', ...
                        'line', 157, ...
                        'cause', ME, ...
                        'context', struct('target_directory', param_path));
                end
            end
        end

        function root = get_repo_root()
            % get_repo_root - Locate repository root from this class path.

            current = fileparts(mfilename('fullpath'));
            while ~isempty(current)
                if exist(fullfile(current, '.git'), 'dir') || ...
                        exist(fullfile(current, 'MECH0020_COPILOT_AGENT_SPEC.md'), 'file')
                    root = current;
                    return;
                end

                parent = fileparts(current);
                if strcmp(parent, current)
                    break;
                end
                current = parent;
            end

            % Conservative fallback from Scripts/Infrastructure/DataRelatedHelpers.
            root = fullfile(fileparts(mfilename('fullpath')), '..', '..', '..');
        end

        function master_table_path = get_master_table_path()
            % get_master_table_path - Path to consolidated runs CSV.
            repo_root = PathBuilder.get_repo_root();
            master_table_path = fullfile(repo_root, 'Results', 'Runs_Table.csv');
        end

        function [resolved_root, output_root] = resolve_output_root(repo_root, output_root_raw)
            % resolve_output_root - Canonicalize caller-provided output root.
            %
            % Supports:
            %   - relative roots (resolved against repo_root)
            %   - absolute roots (used as provided)
            %
            % Fails fast for malformed drive-relative roots (e.g. "C:tmp").
            if nargin < 1 || isempty(repo_root)
                repo_root = PathBuilder.get_repo_root();
            end
            if nargin < 2 || isempty(output_root_raw)
                output_root = 'Results';
            else
                output_root = strtrim(char(string(output_root_raw)));
                if isempty(output_root)
                    output_root = 'Results';
                end
            end

            if any(output_root == char(0))
                ErrorHandler.throw('IO-FS-0001', ...
                    'file', 'PathBuilder', ...
                    'line', 211, ...
                    'message', 'output_root contains null characters', ...
                    'context', struct('output_root', output_root));
            end

            if PathBuilder.is_drive_relative_path(output_root)
                ErrorHandler.throw('IO-FS-0001', ...
                    'file', 'PathBuilder', ...
                    'line', 220, ...
                    'message', 'output_root must be repo-relative or fully absolute (drive-relative roots are unsupported)', ...
                    'context', struct('output_root', output_root));
            end

            if PathBuilder.is_absolute_path(output_root)
                resolved_root = output_root;
            else
                resolved_root = fullfile(repo_root, output_root);
            end
            resolved_root = char(string(resolved_root));
        end
    end

    methods (Static, Access = private)
        function ensure_run_data_workbook_placeholder(paths)
            if ~isstruct(paths) || ~isfield(paths, 'run_data_workbook_path')
                return;
            end
            workbook_path = char(string(paths.run_data_workbook_path));
            if isempty(workbook_path) || exist(workbook_path, 'file') == 2
                return;
            end
            workbook_dir = fileparts(workbook_path);
            if ~isempty(workbook_dir) && exist(workbook_dir, 'dir') ~= 7
                mkdir(workbook_dir);
            end

            sheet_names = PathBuilder.placeholder_workbook_sheet_names(paths);
            placeholder_rows = { ...
                'Status', 'Workbook placeholder created at launch'; ...
                'CreatedAt', char(datetime('now', 'Format', 'yyyy-MM-dd HH:mm:ss')); ...
                'ArtifactRoot', PathBuilder.safe_path_field(paths, 'base'); ...
                'WorkbookPath', workbook_path};
            try
                for i = 1:numel(sheet_names)
                    writecell(placeholder_rows, workbook_path, 'Sheet', sheet_names{i});
                end
            catch ME
                ErrorHandler.throw('IO-FS-0001', ...
                    'file', 'PathBuilder', ...
                    'line', 134, ...
                    'cause', ME, ...
                    'message', 'Could not create Run_Data.xlsx placeholder at launch', ...
                    'context', struct('workbook_path', workbook_path));
            end
        end

        function sheet_names = placeholder_workbook_sheet_names(paths)
            phase_token = lower(strtrim(char(string(PathBuilder.safe_path_field(paths, 'phase')))));
            switch phase_token
                case 'meshconvergence'
                    sheet_names = { ...
                        'plotting_data', ...
                        'comparison data', ...
                        'convergence', ...
                        'runtime_vs_resolution', ...
                        'adaptive_timestep', ...
                        'FD summary', ...
                        'SM summary', ...
                        'sustainability_processed', ...
                        'Metric Guide', ...
                        'Telemetry Raw'};
                case 'phase1'
                    sheet_names = { ...
                        'plotting_data', ...
                        'comparison data', ...
                        'convergence', ...
                        'FD summary', ...
                        'SM summary', ...
                        'sustainability_raw', ...
                        'sustainability_processed'};
                otherwise
                    if strlength(string(phase_token)) ~= 0
                        sheet_names = {'Run Summary', 'Metric Guide', 'Telemetry Raw'};
                    else
                        sheet_names = {'Overview', 'Metric Guide', 'Telemetry Raw'};
                    end
            end
        end

        function value = safe_path_field(paths, field_name)
            value = '';
            if isstruct(paths) && isfield(paths, field_name) && ~isempty(paths.(field_name))
                value = char(string(paths.(field_name)));
            end
        end

        function storage_id = resolve_storage_identifier(identifier, parent_root)
            storage_id = char(string(identifier));
            token = strtrim(storage_id);
            if isempty(token)
                token = 'run';
            end

            if nargin >= 2 && ~isempty(parent_root)
                storage_id = PathBuilder.find_or_allocate_storage_identifier(char(string(parent_root)), token);
                return;
            end

            needs_compaction = strlength(string(token)) > 48;
            if exist('RunIDGenerator', 'class') == 8 || exist('RunIDGenerator', 'file') == 2
                maybe_canonical = ~isempty(regexp(token, ...
                    '^\d{4}-\d{2}-\d{2}__\d{2}-\d{2}-\d{2}__M-', 'once'));
                if maybe_canonical
                    info = RunIDGenerator.parse(token);
                    if isstruct(info) && isfield(info, 'format') && ...
                            strcmpi(char(string(info.format)), 'canonical_v2')
                        needs_compaction = true;
                    end
                end
                if needs_compaction
                    storage_id = RunIDGenerator.make_storage_id(token);
                    return;
                end
            end

            storage_id = token;
        end

        function storage_id = find_or_allocate_storage_identifier(parent_root, logical_id)
            persistent storage_cache
            if isempty(storage_cache)
                storage_cache = containers.Map('KeyType', 'char', 'ValueType', 'char');
            end

            parent_root = char(string(parent_root));
            logical_id = strtrim(char(string(logical_id)));
            if isempty(logical_id)
                logical_id = 'run';
            end
            cache_key = lower(sprintf('%s|%s', parent_root, logical_id));

            if isKey(storage_cache, cache_key)
                cached_id = char(string(storage_cache(cache_key)));
                if exist(fullfile(parent_root, cached_id), 'dir') == 7
                    storage_id = cached_id;
                    return;
                end
                remove(storage_cache, cache_key);
            end

            storage_id = PathBuilder.find_reserved_storage_identifier(parent_root, logical_id);
            if isempty(storage_id)
                storage_id = PathBuilder.allocate_daily_storage_identifier(parent_root, logical_id);
            end
            storage_cache(cache_key) = storage_id;
        end

        function storage_id = find_reserved_storage_identifier(parent_root, logical_id)
            storage_id = '';
            if exist(parent_root, 'dir') ~= 7
                return;
            end

            entries = dir(parent_root);
            for i = 1:numel(entries)
                entry = entries(i);
                if ~entry.isdir || any(strcmp(entry.name, {'.', '..'}))
                    continue;
                end
                if isempty(regexp(entry.name, '^\d{2}-\d{2}-\d{4}__run-\d{3}$', 'once'))
                    continue;
                end
                settings_path = fullfile(parent_root, entry.name, 'Run_Settings.txt');
                if exist(settings_path, 'file') ~= 2
                    continue;
                end
                try
                    settings_text = fileread(settings_path);
                catch
                    settings_text = '';
                end
                if contains(settings_text, logical_id)
                    storage_id = entry.name;
                    return;
                end
            end
        end

        function storage_id = allocate_daily_storage_identifier(parent_root, logical_id)
            if exist(parent_root, 'dir') ~= 7
                mkdir(parent_root);
            end

            date_prefix = char(datetime('now', 'Format', 'dd-MM-yyyy'));
            existing = dir(fullfile(parent_root, sprintf('%s__run-*', date_prefix)));
            next_index = 1;
            for i = 1:numel(existing)
                if ~existing(i).isdir
                    continue;
                end
                token = regexp(existing(i).name, '__run-(\d{3})$', 'tokens', 'once');
                if isempty(token)
                    continue;
                end
                next_index = max(next_index, str2double(token{1}) + 1);
            end

            while true
                storage_id = sprintf('%s__run-%03d', date_prefix, next_index);
                candidate_root = fullfile(parent_root, storage_id);
                if exist(candidate_root, 'dir') == 7
                    next_index = next_index + 1;
                    continue;
                end
                mkdir(candidate_root);
                PathBuilder.write_storage_reservation(candidate_root, logical_id);
                return;
            end
        end

        function write_storage_reservation(base_root, logical_id)
            settings_path = fullfile(base_root, 'Run_Settings.txt');
            if exist(settings_path, 'file') == 2
                return;
            end
            fid = fopen(settings_path, 'w');
            if fid < 0
                return;
            end
            cleaner = onCleanup(@() fclose(fid)); %#ok<NASGU>
            fprintf(fid, 'Storage Reservation\n');
            fprintf(fid, 'logical_id=%s\n', logical_id);
            fprintf(fid, 'reserved_at=%s\n', char(datetime('now', 'Format', 'yyyy-MM-dd HH:mm:ss')));
            clear cleaner
        end

        function token = normalize_method_token(raw_method)
            token = upper(strtrim(char(string(raw_method))));
            token = regexprep(token, '[\s_-]+', ' ');
            switch token
                case {'FD', 'FINITE DIFFERENCE', 'FINITE DIFFERENTIAL', 'FINITE_DIFFERENCE'}
                    token = 'FD';
                case {'SPECTRAL', 'FFT', 'PSEUDOSPECTRAL', 'PSEUDO SPECTRAL'}
                    token = 'Spectral';
                case {'FV', 'FINITE VOLUME', 'FINITE_VOLUME'}
                    token = 'FV';
                case {'SWE', 'SHALLOW WATER', 'SHALLOW_WATER', 'SHALLOWWATER'}
                    token = 'SWE';
                case {'BATHYMETRY', 'VARIABLE BATHYMETRY'}
                    token = 'Bathymetry';
                case {'PLOTTING'}
                    token = 'Plotting';
                otherwise
                    token = regexprep(token, '\s+', '');
            end
        end

        function token = normalize_mode_token(raw_mode)
            token = lower(strtrim(char(string(raw_mode))));
            token = regexprep(token, '[\s_-]+', '');
            switch token
                case {'evolution', 'evolve', 'solve'}
                    token = 'Evolution';
                case {'convergence', 'converge', 'mesh'}
                    token = 'Convergence';
                case {'parametersweep', 'sweep', 'paramsweep'}
                    token = 'ParameterSweep';
                case {'plotting', 'plot', 'visualization', 'visualise'}
                    token = 'Plotting';
                otherwise
                    token = char(string(raw_mode));
            end
        end

        function token = normalize_phase_token(raw_phase)
            token = lower(strtrim(char(string(raw_phase))));
            token = regexprep(token, '[\s_-]+', '');
            switch token
                case {'phase1', '1'}
                    token = 'Phase1';
                case {'phase2', '2'}
                    token = 'Phase2';
                case {'phase3', '3'}
                    token = 'Phase3';
                case {'meshconvergence', 'meshconvergencestudy', 'mesh'}
                    token = 'MeshConvergence';
                otherwise
                    ErrorHandler.throw('RUN-EXEC-0002', ...
                        'file', 'PathBuilder', ...
                        'line', 379, ...
                        'message', 'Unsupported phase token for compact phase paths', ...
                        'context', struct( ...
                            'requested_phase', raw_phase, ...
                            'valid_phases', {{'MeshConvergence', 'Phase1', 'Phase2', 'Phase3'}}));
            end
        end

        function tf = is_absolute_path(path_token)
            path_token = strtrim(char(string(path_token)));
            tf = ~isempty(regexp(path_token, '^[A-Za-z]:[\\/]', 'once')) || ... % Windows drive absolute
                startsWith(path_token, '\\') || ...                             % UNC
                startsWith(path_token, '/');                                    % POSIX absolute
        end

        function tf = is_drive_relative_path(path_token)
            % Windows drive-relative form "C:foo" is ambiguous and unsupported.
            path_token = strtrim(char(string(path_token)));
            tf = ~isempty(regexp(path_token, '^[A-Za-z]:(?![\\/])', 'once'));
        end

        function stem_map = compact_figure_stem_map(prefix)
            prefix = char(string(prefix));
            stem_map = struct( ...
                'main_plots', sprintf('%s_MainPlots', prefix), ...
                'diagnostics', sprintf('%s_Diagnostics', prefix), ...
                'evolution_3x3', sprintf('%s_Evolution_3x3', prefix), ...
                'streamfunction_3x3', sprintf('%s_Streamfunction_3x3', prefix), ...
                'velocity_3x3', sprintf('%s_Velocity_3x3', prefix), ...
                'vector_3x3', sprintf('%s_Vector_3x3', prefix), ...
                'contour_3x3', sprintf('%s_Contour_3x3', prefix), ...
                'streamlines_3x3', sprintf('%s_Streamlines_3x3', prefix), ...
                'wall_vorticity_3x3', sprintf('%s_WallVorticity_3x3', prefix));
        end

        function stem_map = compact_pane_media_stem_map(prefix)
            prefix = char(string(prefix));
            stem_map = struct( ...
                'evolution', sprintf('%s_Evolution_Anim', prefix), ...
                'contour', sprintf('%s_Contour_Anim', prefix), ...
                'vector', sprintf('%s_Vector_Anim', prefix), ...
                'streamlines', sprintf('%s_Streamlines_Anim', prefix), ...
                'streamfunction', sprintf('%s_Streamfunction_Anim', prefix), ...
                'speed', sprintf('%s_Velocity_Anim', prefix), ...
                'wall_vorticity', sprintf('%s_WallVorticity_Anim', prefix));
        end
    end
end
