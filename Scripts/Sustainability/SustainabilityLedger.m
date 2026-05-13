classdef SustainabilityLedger
    % SustainabilityLedger - Append-only per-run sustainability CSV ledger.
    %
    % Ledger path:
    %   Results/Sustainability/runs_sustainability.csv

    methods (Static)
        function [ledger_path, row] = append_run(Run_Config, Parameters, Settings, Results, paths)
            repo_root = PathBuilder.get_repo_root();
            ledger_dir = fullfile(repo_root, 'Results', 'Sustainability');
            if ~exist(ledger_dir, 'dir')
                mkdir(ledger_dir);
            end

            ledger_path = fullfile(ledger_dir, 'runs_sustainability.csv');
            row = SustainabilityLedger.build_row(Run_Config, Parameters, Settings, Results, paths);

            row_table = struct2table(row, 'AsArray', true);
            if exist(ledger_path, 'file')
                existing = readtable(ledger_path, 'TextType', 'string');
                if SustainabilityLedger.has_legacy_var_schema(existing, row_table.Properties.VariableNames)
                    ledger = SustainabilityLedger.repair_legacy_schema(ledger_path, existing, row_table);
                else
                    [existing, row_table] = SustainabilityLedger.align_schema(existing, row_table);
                    ledger = [existing; row_table];
                end
            else
                ledger = row_table;
            end

            writetable(ledger, ledger_path);
        end
    end

    methods (Static, Access = private)
        function row = build_row(Run_Config, Parameters, Settings, Results, paths)
            run_id = SustainabilityLedger.resolve_run_id(Run_Config, Results);
            profile = SystemProfileCollector.collect(Settings);
            timestamp_utc = char(datetime('now', 'TimeZone', 'UTC', 'Format', 'yyyy-MM-dd''T''HH:mm:ss''Z'''));

            [memory_mb, memory_source] = SustainabilityLedger.get_memory_snapshot();
            wall_time_s = SustainabilityLedger.safe_extract_number(Results, {'wall_time', 'total_time', 'wall_time_s'});
            cpu_time_s = SustainabilityLedger.safe_extract_number(Results, {'cpu_time_s'});
            energy_j = SustainabilityLedger.safe_extract_number(Results, {'energy_joules', 'energy_j', 'total_energy_joules'});

            row = struct();
            row.timestamp_utc = string(timestamp_utc);
            row.run_id = string(run_id);
            row.method = string(SustainabilityLedger.safe_extract_text(Run_Config, 'method', 'unknown'));
            row.mode = string(SustainabilityLedger.safe_extract_text(Run_Config, 'mode', 'unknown'));
            row.machine_id = string(profile.machine_id);
            row.machine_label = string(profile.machine_label);
            row.hostname = string(profile.hostname);
            row.os = string(profile.os);
            row.matlab_release = string(profile.matlab_release);
            row.cpu_arch = string(profile.cpu_arch);
            row.cpu_cores = profile.cpu_cores;
            row.ram_total_gb = profile.ram_total_gb;
            row.wall_time_s = wall_time_s;
            row.cpu_time_s = cpu_time_s;
            row.memory_mb = memory_mb;
            row.memory_source = string(memory_source);
            row.energy_joules = energy_j;
            row.collector_matlab = "__YES__";
            row.collector_hwinfo = SustainabilityLedger.bool_to_token(profile.collectors.hwinfo);
            row.collector_icue = SustainabilityLedger.bool_to_token(profile.collectors.icue);
            row.collector_hwinfo_source = string(profile.collectors.hwinfo_source);
            row.collector_icue_source = string(profile.collectors.icue_source);
            row.source_quality = string(profile.source_quality);
            row.results_path = string(SustainabilityLedger.safe_extract_text(paths, 'base', ''));
            row.grid_nx = SustainabilityLedger.safe_extract_number(Parameters, {'Nx'});
            row.grid_ny = SustainabilityLedger.safe_extract_number(Parameters, {'Ny'});
            row.dt = SustainabilityLedger.safe_extract_number(Parameters, {'dt'});
            row.tfinal = SustainabilityLedger.safe_extract_number(Parameters, {'Tfinal'});
            row.status = string(SustainabilityLedger.safe_extract_text(Results, 'status', 'completed'));
        end

        function [existing, incoming] = align_schema(existing, incoming)
            existing_cols = existing.Properties.VariableNames;
            incoming_cols = incoming.Properties.VariableNames;

            for i = 1:numel(incoming_cols)
                col = incoming_cols{i};
                if ~ismember(col, existing_cols)
                    existing.(col) = repmat("", height(existing), 1);
                end
            end

            existing_cols = existing.Properties.VariableNames;
            for i = 1:numel(existing_cols)
                col = existing_cols{i};
                if ~ismember(col, incoming_cols)
                    if isstring(existing.(col))
                        incoming.(col) = "";
                    elseif isnumeric(existing.(col))
                        incoming.(col) = NaN;
                    else
                        incoming.(col) = "";
                    end
                end
            end

            incoming = incoming(:, existing.Properties.VariableNames);
        end

        function tf = has_legacy_var_schema(existing, required_cols)
            tf = false;
            if ~istable(existing) || isempty(required_cols)
                return;
            end
            vars = string(existing.Properties.VariableNames);
            has_generic_prefix = any(startsWith(vars, "Var"));
            missing_required = ~all(ismember(string(required_cols), vars));
            tf = has_generic_prefix || missing_required;
        end

        function ledger = repair_legacy_schema(ledger_path, existing, incoming)
            required_cols = incoming.Properties.VariableNames;
            repaired = table();
            n_rows = height(existing);

            for i = 1:numel(required_cols)
                target = required_cols{i};
                fallback = sprintf('Var%d', i);
                values = strings(n_rows, 1);

                if ismember(target, existing.Properties.VariableNames)
                    values = SustainabilityLedger.column_to_string(existing.(target), n_rows);
                end

                if ismember(fallback, existing.Properties.VariableNames)
                    fallback_values = SustainabilityLedger.column_to_string(existing.(fallback), n_rows);
                    missing_mask = SustainabilityLedger.is_missing_string(values);
                    values(missing_mask) = fallback_values(missing_mask);
                end

                repaired.(target) = values;
            end

            keep_mask = false(height(repaired), 1);
            for i = 1:width(repaired)
                keep_mask = keep_mask | ~SustainabilityLedger.is_missing_string(repaired.(i));
            end
            repaired = repaired(keep_mask, :);

            incoming_text = SustainabilityLedger.table_to_string_table(incoming, required_cols);
            ledger = [repaired; incoming_text];

            backup_path = fullfile(fileparts(ledger_path), sprintf('runs_sustainability__legacy_backup__%s.csv', ...
                char(datetime('now', 'Format', 'yyyyMMdd_HHmmss'))));
            copyfile(ledger_path, backup_path);
            SustainabilityLedger.warn_once('SustainabilityLedger:LegacySchemaRepair', ...
                'Repaired legacy sustainability ledger schema and archived backup to %s', backup_path);
        end

        function text_table = table_to_string_table(tbl, cols)
            if nargin < 2 || isempty(cols)
                cols = tbl.Properties.VariableNames;
            end
            text_table = table();
            for i = 1:numel(cols)
                col = cols{i};
                text_table.(col) = SustainabilityLedger.column_to_string(tbl.(col), height(tbl));
            end
        end

        function out = column_to_string(values, n_rows)
            if nargin < 2
                n_rows = numel(values);
            end
            if isstring(values)
                out = values;
            elseif ischar(values)
                out = string(cellstr(values));
            elseif isnumeric(values)
                out = strings(n_rows, 1);
                valid = ~isnan(values);
                out(valid) = string(values(valid));
            elseif islogical(values)
                out = strings(n_rows, 1);
                out(values) = "true";
                out(~values) = "false";
            elseif iscell(values)
                out = strings(n_rows, 1);
                for k = 1:min(numel(values), n_rows)
                    if isempty(values{k})
                        out(k) = "";
                    else
                        out(k) = string(values{k});
                    end
                end
            else
                out = repmat("", n_rows, 1);
            end
            out = reshape(out, [], 1);
        end

        function mask = is_missing_string(values)
            values = string(values);
            mask = ismissing(values) | strlength(strtrim(values)) == 0 | values == "NaN";
        end

        function run_id = resolve_run_id(Run_Config, Results)
            if isfield(Results, 'run_id') && ~isempty(Results.run_id)
                run_id = char(string(Results.run_id));
                return;
            end
            if isfield(Run_Config, 'run_id') && ~isempty(Run_Config.run_id)
                run_id = char(string(Run_Config.run_id));
                return;
            end
            if isfield(Run_Config, 'study_id') && ~isempty(Run_Config.study_id)
                run_id = char(string(Run_Config.study_id));
                return;
            end
            run_id = RunIDGenerator.generate(Run_Config, struct());
        end

        function [memory_mb, source] = get_memory_snapshot()
            memory_mb = NaN;
            source = 'unavailable';
            if ispc
                try
                    mem = memory;
                    memory_mb = mem.MemUsedMATLAB / 1024 / 1024;
                    source = 'matlab_memory';
                catch ME
                    SustainabilityLedger.warn_once('SustainabilityLedger:MemoryProbeFailed', ...
                        'MATLAB memory probe failed for sustainability row; storing NaN: %s', ME.message);
                    memory_mb = NaN;
                end
            end
        end

        function value = safe_extract_number(s, candidate_fields)
            value = NaN;
            if ~isstruct(s)
                return;
            end
            for i = 1:numel(candidate_fields)
                field_name = candidate_fields{i};
                if isfield(s, field_name) && isnumeric(s.(field_name)) && isscalar(s.(field_name))
                    value = s.(field_name);
                    return;
                end
            end
        end

        function text = safe_extract_text(s, field_name, default_value)
            text = default_value;
            if isstruct(s) && isfield(s, field_name)
                val = s.(field_name);
                if isstring(val) || ischar(val)
                    text = char(string(val));
                end
            end
        end

        function token = bool_to_token(flag)
            if flag
                token = "__YES__";
            else
                token = "__NO__";
            end
        end

        function warn_once(identifier, message, varargin)
            persistent emitted_ids;
            if isempty(emitted_ids)
                emitted_ids = containers.Map('KeyType', 'char', 'ValueType', 'logical');
            end
            id = char(string(identifier));
            if isKey(emitted_ids, id)
                return;
            end
            emitted_ids(id) = true;
            SafeConsoleIO.warning(id, message, varargin{:});
        end
    end
end
