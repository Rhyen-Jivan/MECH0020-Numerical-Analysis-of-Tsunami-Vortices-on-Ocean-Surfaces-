classdef ConvergedMeshRegistry
    % ConvergedMeshRegistry - Discover reusable mesh settings from convergence artifacts.
    %
    % The registry is derived from the canonical convergence output contract
    % rather than a separate database. UI loaders and phase workflows can use
    % these entries as seeds while still re-running convergence when required.

    methods (Static)
        function entries = discover(output_root)
            if nargin < 1 || isempty(output_root)
                output_root = 'Results';
            end
            repo_root = PathBuilder.get_repo_root();
            [results_root, ~] = PathBuilder.resolve_output_root(repo_root, output_root);
            listing = [ ...
                dir(fullfile(results_root, '*', '*', 'Data', 'convergence_results.mat')); ...
                dir(fullfile(results_root, '*', 'Convergence', '*', 'Data', 'convergence_results.mat'))];
            entries = repmat(ConvergedMeshRegistry.empty_entry(), 0, 1);
            for i = 1:numel(listing)
                source_path = fullfile(listing(i).folder, listing(i).name);
                try
                    payload = load(source_path, 'Results');
                    if ~isfield(payload, 'Results') || ~isstruct(payload.Results)
                        continue;
                    end
                    config_path = ConvergedMeshRegistry.config_path_for_result(source_path);
                    entry = ConvergedMeshRegistry.from_results(payload.Results, source_path, config_path, listing(i));
                    if ~isempty(entry.method)
                        entries(end + 1, 1) = entry; %#ok<AGROW>
                    end
                catch
                    % Malformed generated artifacts are ignored by discovery.
                    % Contract-critical loading happens through select_latest.
                end
            end
        end

        function entry = select_latest(output_root, criteria)
            if nargin < 1 || isempty(output_root)
                output_root = 'Results';
            end
            if nargin < 2 || ~isstruct(criteria)
                criteria = struct();
            end
            entries = ConvergedMeshRegistry.discover(output_root);
            entry = ConvergedMeshRegistry.empty_entry();
            if isempty(entries)
                return;
            end
            keep = true(size(entries));
            if isfield(criteria, 'method') && ~isempty(criteria.method)
                method_key = ConvergedMeshRegistry.normalize_method(criteria.method);
                keep = keep & strcmp({entries.method_key}, method_key);
            end
            if isfield(criteria, 'ic_type') && ~isempty(criteria.ic_type)
                ic_key = lower(strtrim(char(string(criteria.ic_type))));
                keep = keep & strcmpi({entries.ic_type}, ic_key);
            end
            if isfield(criteria, 'bc_case') && ~isempty(criteria.bc_case)
                bc_key = lower(strtrim(char(string(criteria.bc_case))));
                keep = keep & strcmpi({entries.bc_case}, bc_key);
            end
            if isfield(criteria, 'bathymetry_scenario') && ~isempty(criteria.bathymetry_scenario)
                bathy_key = lower(strtrim(char(string(criteria.bathymetry_scenario))));
                keep = keep & strcmpi({entries.bathymetry_scenario}, bathy_key);
            end
            candidates = entries(keep);
            if isempty(candidates)
                return;
            end
            [~, idx] = max([candidates.timestamp_datenum]);
            entry = candidates(idx);
        end

        function entry = from_results(Results, source_path, config_path, listing_entry)
            if nargin < 2, source_path = ''; end
            if nargin < 3, config_path = ''; end
            if nargin < 4 || ~isstruct(listing_entry)
                listing_entry = struct('datenum', now);
            end

            entry = ConvergedMeshRegistry.empty_entry();
            method = ConvergedMeshRegistry.pick_text(Results, {'method'}, '');
            entry.method = method;
            entry.method_key = ConvergedMeshRegistry.normalize_method(method);
            entry.source_path = char(string(source_path));
            entry.config_path = char(string(config_path));
            entry.timestamp_datenum = double(ConvergedMeshRegistry.pick_field(listing_entry, 'datenum', now));
            entry.timestamp = char(datetime(entry.timestamp_datenum, 'ConvertFrom', 'datenum', ...
                'Format', 'yyyy-MM-dd HH:mm:ss'));
            entry.verdict = ConvergedMeshRegistry.pick_verdict(Results);
            entry.is_converged = strcmpi(entry.verdict, 'converged');

            [run_config, parameters] = ConvergedMeshRegistry.load_config_metadata(config_path);
            entry.ic_type = ConvergedMeshRegistry.pick_text(run_config, {'ic_type'}, ...
                ConvergedMeshRegistry.pick_text(parameters, {'ic_type'}, ''));
            entry.bc_case = ConvergedMeshRegistry.pick_text(parameters, {'bc_case', 'boundary_condition_case'}, '');
            entry.bathymetry_scenario = ConvergedMeshRegistry.pick_text(parameters, {'bathymetry_scenario'}, '');

            record = ConvergedMeshRegistry.pick_primary_record(Results);
            if ~isempty(record)
                entry.Nx = ConvergedMeshRegistry.pick_numeric(record, {'Nx'}, NaN);
                entry.Ny = ConvergedMeshRegistry.pick_numeric(record, {'Ny'}, NaN);
                entry.Nz = ConvergedMeshRegistry.pick_numeric(record, {'Nz'}, ...
                    ConvergedMeshRegistry.pick_numeric(parameters, {'Nz'}, NaN));
                entry.dt = ConvergedMeshRegistry.pick_numeric(record, {'dt'}, ...
                    ConvergedMeshRegistry.pick_numeric(parameters, {'dt'}, NaN));
                entry.h = ConvergedMeshRegistry.pick_numeric(record, {'h'}, NaN);
                entry.mode_count = ConvergedMeshRegistry.pick_numeric(record, {'mode_count'}, NaN);
                entry.dof = ConvergedMeshRegistry.pick_numeric(record, {'dof'}, NaN);
                entry.refinement_axis = ConvergedMeshRegistry.pick_text(record, {'refinement_axis'}, '');
            end
        end

        function parameters = apply_to_parameters(parameters, entry)
            if nargin < 2 || ~isstruct(entry) || isempty(entry)
                return;
            end
            if isfield(entry, 'Nx') && isfinite(entry.Nx), parameters.Nx = round(entry.Nx); end
            if isfield(entry, 'Ny') && isfinite(entry.Ny), parameters.Ny = round(entry.Ny); end
            if isfield(entry, 'Nz') && isfinite(entry.Nz), parameters.Nz = round(entry.Nz); end
            if isfield(entry, 'dt') && isfinite(entry.dt), parameters.dt = double(entry.dt); end
            if isfield(parameters, 'Tfinal')
                parameters.t_final = parameters.Tfinal;
            end
        end

        function save_registry(entries, registry_path)
            registry_dir = fileparts(registry_path);
            if ~isempty(registry_dir) && exist(registry_dir, 'dir') ~= 7
                mkdir(registry_dir);
            end
            save(registry_path, 'entries');
            [root, name] = fileparts(registry_path);
            json_path = fullfile(root, [name, '.json']);
            fid = fopen(json_path, 'w');
            if fid < 0
                error('ConvergedMeshRegistry:WriteFailed', ...
                    'Could not write registry JSON: %s', json_path);
            end
            cleaner = onCleanup(@() fclose(fid));
            fprintf(fid, '%s', jsonencode(entries));
            clear cleaner
        end

        function entry = empty_entry()
            entry = struct( ...
                'method', '', ...
                'method_key', '', ...
                'ic_type', '', ...
                'bc_case', '', ...
                'bathymetry_scenario', '', ...
                'Nx', NaN, ...
                'Ny', NaN, ...
                'Nz', NaN, ...
                'dt', NaN, ...
                'h', NaN, ...
                'mode_count', NaN, ...
                'dof', NaN, ...
                'refinement_axis', '', ...
                'verdict', '', ...
                'is_converged', false, ...
                'timestamp', '', ...
                'timestamp_datenum', NaN, ...
                'source_path', '', ...
                'config_path', '');
        end
    end

    methods (Static, Access = private)
        function config_path = config_path_for_result(source_path)
            data_dir = fileparts(source_path);
            study_dir = fileparts(data_dir);
            config_candidates = { ...
                fullfile(data_dir, 'Config.mat'), ...
                fullfile(study_dir, 'Config', 'Config.mat')};
            config_path = config_candidates{1};
            for i = 1:numel(config_candidates)
                if exist(config_candidates{i}, 'file') == 2
                    config_path = config_candidates{i};
                    return;
                end
            end
        end

        function [run_config, parameters] = load_config_metadata(config_path)
            run_config = struct();
            parameters = struct();
            if isempty(config_path) || exist(config_path, 'file') ~= 2
                return;
            end
            payload = load(config_path);
            if isfield(payload, 'Run_Config_clean')
                run_config = payload.Run_Config_clean;
            elseif isfield(payload, 'Run_Config')
                run_config = payload.Run_Config;
            end
            if isfield(payload, 'Parameters_clean')
                parameters = payload.Parameters_clean;
            elseif isfield(payload, 'Parameters')
                parameters = payload.Parameters;
            end
        end

        function record = pick_primary_record(Results)
            record = [];
            if ~isfield(Results, 'run_records') || isempty(Results.run_records)
                return;
            end
            records = Results.run_records(:);
            primary_stage = '';
            if isfield(Results, 'stage_summaries') && ~isempty(Results.stage_summaries)
                names = {Results.stage_summaries.stage_name};
                idx = find(~strcmp(names, 'temporal'), 1, 'first');
                if ~isempty(idx)
                    primary_stage = names{idx};
                end
            end
            if isempty(primary_stage)
                primary_stage = ConvergedMeshRegistry.pick_text(Results, {'refinement_axis'}, '');
            end
            if ~isempty(primary_stage) && isfield(records, 'study_stage')
                subset = records(strcmp({records.study_stage}, primary_stage));
                if ~isempty(subset)
                    records = subset(:);
                end
            end
            converged_idx = [];
            if isfield(records, 'convergence_verdict')
                converged_idx = find(strcmpi({records.convergence_verdict}, 'converged'), 1, 'first');
            end
            if ~isempty(converged_idx)
                record = records(converged_idx);
            else
                record = records(end);
            end
        end

        function verdict = pick_verdict(Results)
            verdict = '';
            if isfield(Results, 'summary') && isstruct(Results.summary)
                verdict = ConvergedMeshRegistry.pick_text(Results.summary, {'overall_verdict'}, '');
            end
            if isempty(verdict)
                verdict = ConvergedMeshRegistry.pick_text(Results, {'verdict'}, '');
            end
        end

        function out = normalize_method(raw)
            token = upper(strtrim(char(string(raw))));
            token = regexprep(token, '[\s_-]+', ' ');
            switch token
                case {'FD', 'FINITE DIFFERENCE', 'FINITEDIFFERENCE'}
                    out = 'fd';
                case {'SPECTRAL', 'FFT', 'PSEUDOSPECTRAL', 'PSEUDO SPECTRAL'}
                    out = 'spectral';
                case {'FV', 'FINITE VOLUME', 'FINITEVOLUME'}
                    out = 'fv';
                otherwise
                    out = lower(regexprep(token, '\s+', '_'));
            end
        end

        function val = pick_field(s, field, default)
            if isstruct(s) && isfield(s, field)
                val = s.(field);
            else
                val = default;
            end
        end

        function txt = pick_text(s, fields, default)
            txt = default;
            if ~isstruct(s), return; end
            for i = 1:numel(fields)
                if isfield(s, fields{i}) && ~isempty(s.(fields{i}))
                    txt = lower(strtrim(char(string(s.(fields{i})))));
                    return;
                end
            end
        end

        function val = pick_numeric(s, fields, default)
            val = default;
            if ~isstruct(s), return; end
            for i = 1:numel(fields)
                if isfield(s, fields{i}) && ~isempty(s.(fields{i})) && isnumeric(s.(fields{i}))
                    candidate = double(s.(fields{i}));
                    if isscalar(candidate)
                        val = candidate;
                        return;
                    end
                end
            end
        end
    end
end
