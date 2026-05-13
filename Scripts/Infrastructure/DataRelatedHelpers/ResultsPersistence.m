% ========================================================================
% ResultsPersistence - CSV/MAT I/O and Schema Management (Static Class)
% ========================================================================
% Handles all data persistence operations with schema migration support
% Used by: Analysis.m (all run modes)
%
% Usage: ResultsPersistence.append_master_csv(T_current, settings)
%
% Methods:
%   migrate_csv_schema(T_existing, T_current, csv_path, missing, extra)
%   append_master_csv(T_current, settings) - Append results to master CSV
%
% Created: 2026-02-06
% Part of: Tsunami Vortex Analysis Framework - Phase 2 Refactoring
% ========================================================================

classdef ResultsPersistence
    methods(Static)
        function T_existing = migrate_csv_schema(T_existing, T_current, csv_path, missing_in_existing, extra_in_existing)
            % Adds missing columns (matching types from T_current) and logs extras
            if ~isempty(missing_in_existing)
                fprintf("CSV schema mismatch: adding missing columns: %s\n", strjoin(missing_in_existing, ", "));
                for k = 1:numel(missing_in_existing)
                    col = char(missing_in_existing(k));
                    if ismember(col, T_current.Properties.VariableNames)
                        sample = T_current.(col);
                        cls = class(sample);
                        switch cls
                            case 'double'
                                T_existing.(col) = nan(height(T_existing), 1);
                            case 'single'
                                T_existing.(col) = single(nan(height(T_existing), 1));
                            case {'char', 'string'}
                                T_existing.(col) = repmat("", height(T_existing), 1);
                            case 'cell'
                                T_existing.(col) = repmat({[]}, height(T_existing), 1);
                            case 'datetime'
                                T_existing.(col) = repmat(datetime(NaT), height(T_existing), 1);
                            otherwise
                                T_existing.(col) = repmat({[]}, height(T_existing), 1);
                        end
                    else
                        T_existing.(col) = repmat({[]}, height(T_existing), 1);
                    end
                end
                try
                    writetable(T_existing, csv_path);
                catch WErr
                    warning(WErr.identifier, 'Failed to write migrated CSV: %s', WErr.message);
                end
            end
            if ~isempty(extra_in_existing)
                fprintf("Warning: CSV contains extra columns: %s (kept)\n", strjoin(extra_in_existing, ", "));
            end
        end
        
        function append_master_csv(T_current, settings)
            % Append current results to master CSV with schema migration
            if ~isfield(settings, 'results_dir') || isempty(settings.results_dir)
                return;
            end
            master_path = fullfile(settings.results_dir, "analysis_master.csv");
            if isfile(master_path)
                opts = detectImportOptions(master_path, 'TextType', 'string');
                if any(strcmpi(opts.VariableNames, "timestamp"))
                    opts = setvartype(opts, "timestamp", "datetime");
                    opts = setvaropts(opts, "timestamp", "InputFormat", "yyyy-MM-dd HH:mm:ss", "DatetimeLocale", "en_UK");
                end
                T_existing = readtable(master_path, opts);
                if ~ismember("timestamp", T_existing.Properties.VariableNames)
                    T_existing.timestamp = repmat(datetime(NaT), height(T_existing), 1);
                end
                vars_current = string(T_current.Properties.VariableNames);
                vars_existing = string(T_existing.Properties.VariableNames);
                missing_in_existing = setdiff(vars_current, vars_existing);
                extra_in_existing = setdiff(vars_existing, vars_current);
                if ~isempty(missing_in_existing) || ~isempty(extra_in_existing)
                    T_existing = ResultsPersistence.migrate_csv_schema(T_existing, T_current, master_path, missing_in_existing, extra_in_existing);
                end
                T_existing = T_existing(:, T_current.Properties.VariableNames);
                T_master = [T_existing; T_current];
            else
                T_master = T_current;
            end
            writetable(T_master, master_path);
        end
    end
end