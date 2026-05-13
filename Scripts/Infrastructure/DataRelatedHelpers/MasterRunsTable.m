classdef MasterRunsTable
    % MasterRunsTable - Append-safe master runs table across all methods/modes
    %
    % Purpose:
    %   Single CSV tracking all runs (Results/Runs_Table.csv)
    %   Append-safe with schema evolution support
    %   Optional Excel export with conditional formatting
    %
    % Usage:
    %   MasterRunsTable.append_run(run_id, Run_Config, Parameters, Results);
    %   MasterRunsTable.export_to_excel();  % Optional
    
    methods (Static)
        function append_run(run_id, Run_Config, Parameters, Results)
            % Append new run to master table
            
            % Get master table path
            table_path = PathBuilder.get_master_table_path();
            
            % Ensure Results directory exists
            results_dir = fileparts(table_path);
            if ~exist(results_dir, 'dir')
                mkdir(results_dir);
            end
            
            % Create row for this run
            row = MasterRunsTable.create_row(run_id, Run_Config, Parameters, Results);
            
            % Load existing table or create new
            if exist(table_path, 'file')
                existing = readtable(table_path, 'Delimiter', ',');
                % Schema evolution: add missing columns
                existing_fields = existing.Properties.VariableNames;
                row_fields = row.Properties.VariableNames;
                
                % Add new fields to existing table
                for i = 1:length(row_fields)
                    if ~ismember(row_fields{i}, existing_fields)
                        existing.(row_fields{i}) = MasterRunsTable.default_column_like(row.(row_fields{i}), height(existing));
                    end
                end
                
                % Add missing fields to new row
                for i = 1:length(existing_fields)
                    if ~ismember(existing_fields{i}, row_fields)
                        row.(existing_fields{i}) = MasterRunsTable.default_column_like(existing.(existing_fields{i}), height(row));
                    end
                end
                [existing, row] = MasterRunsTable.align_column_types(existing, row);
                
                % Append row
                master = [existing; row];
            else
                master = row;
            end
            
            % Write to CSV
            writetable(master, table_path, 'Delimiter', ',');
        end
        
        function export_to_excel()
            % Export master table to Excel with formatting (if available)
            
            table_path = PathBuilder.get_master_table_path();
            if ~exist(table_path, 'file')
                warning('MasterRunsTable:NoTable', 'Master table does not exist yet');
                return;
            end
            
            excel_path = strrep(table_path, '.csv', '.xlsx');
            
            try
                % Read CSV
                master = readtable(table_path, 'Delimiter', ',');
                
                % Write to Excel
                writetable(master, excel_path, 'Sheet', 'Runs');
                
                % Attempt conditional formatting (platform-dependent)
                try
                    MasterRunsTable.apply_excel_formatting(excel_path);
                catch ME
                    % Formatting not available on this platform
                    warning('MasterRunsTable:FormattingFailed', ...
                        'Excel formatting not available: %s', ME.message);
                end
                
            catch ME
                warning('MasterRunsTable:ExcelExportFailed', ...
                    'Could not export to Excel: %s', ME.message);
            end
        end
        
        function table_data = query(filters)
            % Query master table with filters
            % filters: struct with field-value pairs
            %
            % Example:
            %   data = MasterRunsTable.query(struct('method', 'FD', 'mode', 'Evolution'));
            
            table_path = PathBuilder.get_master_table_path();
            if ~exist(table_path, 'file')
                table_data = table();
                return;
            end
            
            % Read table
            table_data = readtable(table_path, 'Delimiter', ',');
            
            % Apply filters
            if ~isempty(filters)
                fields = fieldnames(filters);
                for i = 1:length(fields)
                    if ismember(fields{i}, table_data.Properties.VariableNames)
                        mask = strcmp(table_data.(fields{i}), filters.(fields{i}));
                        table_data = table_data(mask, :);
                    end
                end
            end
        end
    end
    
    methods (Static, Access = private)
        function row = create_row(run_id, Run_Config, Parameters, Results)
            % Create table row from run data
            
            % Core identifiers
            row_data = struct();
            row_data.run_id = {run_id};
            row_data.timestamp = {char(datetime('now', 'Format', 'yyyy-MM-dd HH:mm:ss'))};
            row_data.method = {Run_Config.method};
            row_data.mode = {Run_Config.mode};
            row_data.row_type = {MasterRunsTable.safe_extract_text(Results, 'row_type', 'run')};
            
            % Configuration
            if isfield(Run_Config, 'ic_type')
                row_data.ic_type = {Run_Config.ic_type};
            else
                row_data.ic_type = {''};
            end
            
            % Parameters (common ones)
            row_data.Nx = MasterRunsTable.safe_extract(Parameters, 'Nx', NaN);
            row_data.Ny = MasterRunsTable.safe_extract(Parameters, 'Ny', NaN);
            row_data.dt = MasterRunsTable.safe_extract(Parameters, 'dt', NaN);
            row_data.Tfinal = MasterRunsTable.safe_extract(Parameters, 'Tfinal', NaN);
            row_data.nu = MasterRunsTable.safe_extract(Parameters, 'nu', NaN);
            row_data.Lx = MasterRunsTable.safe_extract(Parameters, 'Lx', NaN);
            row_data.Ly = MasterRunsTable.safe_extract(Parameters, 'Ly', NaN);
            
            % Results (common metrics)
            row_data.wall_time_s = MasterRunsTable.safe_extract(Results, 'wall_time', NaN);
            row_data.final_time = MasterRunsTable.safe_extract(Results, 'final_time', NaN);
            row_data.total_steps = MasterRunsTable.safe_extract(Results, 'total_steps', NaN);
            row_data.max_omega = MasterRunsTable.safe_extract(Results, 'max_omega', NaN);
            row_data.final_energy = MasterRunsTable.safe_extract(Results, 'final_energy', NaN);
            row_data.final_enstrophy = MasterRunsTable.safe_extract(Results, 'final_enstrophy', NaN);
            
            phase_metrics = struct();
            if isfield(Results, 'phase_metrics') && isstruct(Results.phase_metrics)
                phase_metrics = Results.phase_metrics;
            end
            row_data.phase_id = {MasterRunsTable.safe_extract_text(phase_metrics, 'phase_id', ...
                MasterRunsTable.safe_extract_text(Run_Config, 'phase_id', ''))};
            row_data.phase_method = {MasterRunsTable.safe_extract_text(phase_metrics, 'method', Run_Config.method)};
            row_data.reference_strategy = {MasterRunsTable.safe_extract_text(phase_metrics, 'reference_strategy', '')};
            row_data.relative_vorticity_error_L2 = MasterRunsTable.safe_extract(phase_metrics, 'relative_vorticity_error_L2', NaN);
            row_data.relative_vorticity_error_Linf = MasterRunsTable.safe_extract(phase_metrics, 'relative_vorticity_error_Linf', NaN);
            row_data.observed_spatial_rate = MasterRunsTable.safe_extract(phase_metrics, 'observed_spatial_rate', NaN);
            row_data.observed_temporal_rate = MasterRunsTable.safe_extract(phase_metrics, 'observed_temporal_rate', NaN);
            row_data.peak_vorticity_ratio = MasterRunsTable.safe_extract(phase_metrics, 'peak_vorticity_ratio', NaN);
            row_data.centroid_drift = MasterRunsTable.safe_extract(phase_metrics, 'centroid_drift', NaN);
            row_data.core_radius_initial = MasterRunsTable.safe_extract(phase_metrics, 'core_radius_initial', NaN);
            row_data.core_radius_final = MasterRunsTable.safe_extract(phase_metrics, 'core_radius_final', NaN);
            row_data.core_anisotropy_final = MasterRunsTable.safe_extract(phase_metrics, 'core_anisotropy_final', NaN);
            row_data.circulation_drift = MasterRunsTable.safe_extract(phase_metrics, 'circulation_drift', NaN);
            row_data.kinetic_energy_drift = MasterRunsTable.safe_extract(phase_metrics, 'kinetic_energy_drift', NaN);
            row_data.enstrophy_drift = MasterRunsTable.safe_extract(phase_metrics, 'enstrophy_drift', NaN);
            row_data.observed_cfl = MasterRunsTable.safe_extract(phase_metrics, 'observed_cfl', NaN);
            row_data.runtime_wall_s_phase = MasterRunsTable.safe_extract(phase_metrics, 'runtime_wall_s', NaN);
            row_data.time_per_step_s = MasterRunsTable.safe_extract(phase_metrics, 'time_per_step_s', NaN);
            row_data.cost_accuracy = MasterRunsTable.safe_extract(phase_metrics, 'cost_accuracy', NaN);
            row_data.mesh_convergence_verdict = {MasterRunsTable.safe_extract_text(phase_metrics, 'mesh_convergence_verdict', '')};
            row_data.mesh_source_path = {MasterRunsTable.safe_extract_text(phase_metrics, 'mesh_source_path', '')};
            row_data.fd_relative_L2 = MasterRunsTable.safe_extract(phase_metrics, 'fd_relative_L2', NaN);
            row_data.spectral_relative_L2 = MasterRunsTable.safe_extract(phase_metrics, 'spectral_relative_L2', NaN);
            row_data.relative_L2_ratio_fd_over_spectral = MasterRunsTable.safe_extract(phase_metrics, 'relative_L2_ratio_fd_over_spectral', NaN);
            
            % Convert to table
            row = struct2table(row_data, 'AsArray', true);
        end
        
        function val = safe_extract(s, field, default)
            % Safely extract field or return default
            if isfield(s, field)
                val = s.(field);
            else
                val = default;
            end
        end

        function val = safe_extract_text(s, field, default)
            % Safely extract text field or return default
            if isstruct(s) && isfield(s, field) && ~isempty(s.(field))
                val = char(string(s.(field)));
            else
                val = char(string(default));
            end
        end

        function col = default_column_like(example, n)
            % Build a missing-column fill value compatible with example.
            if isnumeric(example)
                col = NaN(n, 1);
            elseif islogical(example)
                col = false(n, 1);
            elseif isstring(example)
                col = strings(n, 1);
            else
                col = repmat({''}, n, 1);
            end
        end

        function [existing, row] = align_column_types(existing, row)
            % Convert only mismatched columns to text so schema migration is append-safe.
            fields = row.Properties.VariableNames;
            for i = 1:numel(fields)
                name = fields{i};
                if ~ismember(name, existing.Properties.VariableNames)
                    continue;
                end
                if strcmp(class(existing.(name)), class(row.(name)))
                    continue;
                end
                existing.(name) = cellstr(string(existing.(name)));
                row.(name) = cellstr(string(row.(name)));
            end
        end
        
        function apply_excel_formatting(excel_path)
            % Apply conditional formatting to Excel (Windows + Excel COM only)
            
            % This requires Excel COM automation (Windows only)
            % Gracefully degrade if not available
            
            if ~ispc
                return;  % Not Windows
            end
            
            try
                % Create Excel COM object
                Excel = actxserver('Excel.Application');
                Excel.Visible = false;
                Workbook = Excel.Workbooks.Open(excel_path);
                Sheet = Workbook.Sheets.Item('Runs');
                
                % Apply conditional formatting to wall_time column
                % (Color scale: green=fast, red=slow)
                try
                    wall_time_col = find(strcmp(Sheet.Range('A1:Z1').Value, 'wall_time_s'));
                    if ~isempty(wall_time_col)
                        last_row = Sheet.UsedRange.Rows.Count;
                        range = Sheet.Range(sprintf('%s2:%s%d', ...
                            char('A' + wall_time_col - 1), ...
                            char('A' + wall_time_col - 1), last_row));
                        range.FormatConditions.AddColorScale(3);
                    end
                catch ME
                    warning('MasterRunsTable:ConditionalFormattingFailed', ...
                        'Excel conditional formatting failed; continuing without formatting: %s', ME.message);
                end
                
                % Save and close
                Workbook.Save();
                Workbook.Close();
                Excel.Quit();
                delete(Excel);
                
            catch ME
                warning('MasterRunsTable:ExcelAutomationFailed', ...
                    'Excel COM automation failed while formatting MasterRunsTable export: %s', ME.message);
            end
        end
    end
end
