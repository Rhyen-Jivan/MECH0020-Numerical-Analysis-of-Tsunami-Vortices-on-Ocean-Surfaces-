classdef RunReportGenerator
    % RunReportGenerator - Professional ANSYS/Abaqus-inspired run reports
    %
    % Purpose:
    %   Generate plain-text reports for each run/study.
    %   Include metadata, parameters, metrics, and output manifest.
    %
    % Scope note:
    %   This class is intentionally text-only (Report.txt). Rich HTML/PDF
    %   report generation is handled by the newer report pipeline utilities.
    %
    % Usage:
    %   RunReportGenerator.generate(run_id, Run_Config, Parameters, Settings, Results, paths);
    
    methods (Static)
        function generate(run_id, Run_Config, Parameters, Settings, Results, paths)
            % Generate complete run report
            % Save as Report.txt in reports directory.
            
            report_path = fullfile(paths.reports, 'Report.txt');
            
            % Open file for writing
            fid = fopen(report_path, 'w');
            if fid == -1
                error('RunReportGenerator:FileOpenFailed', ...
                    'Could not create report: %s', report_path);
            end
            
            try
                % Keep section order stable so downstream parsers can read
                % historical reports with simple text anchors.
                RunReportGenerator.write_header(fid, run_id);
                RunReportGenerator.write_metadata(fid);
                RunReportGenerator.write_configuration(fid, Run_Config);
                RunReportGenerator.write_parameters(fid, Parameters);
                RunReportGenerator.write_settings(fid, Settings);
                RunReportGenerator.write_results(fid, Results);
                RunReportGenerator.write_file_manifest(fid, paths);
                RunReportGenerator.write_footer(fid);
                
                fclose(fid);
            catch ME
                fclose(fid);
                rethrow(ME);
            end
        end
    end
    
    methods (Static, Access = private)
        function write_header(fid, run_id)
            % Write report header
            fprintf(fid, '═══════════════════════════════════════════════════════════════════\n');
            fprintf(fid, '  TSUNAMI VORTEX NUMERICAL SIMULATION - RUN REPORT\n');
            fprintf(fid, '═══════════════════════════════════════════════════════════════════\n');
            fprintf(fid, '\n');
            fprintf(fid, 'Run ID: %s\n', run_id);
            fprintf(fid, 'Generated: %s\n', char(datetime('now')));
            fprintf(fid, '\n');
        end
        
        function write_metadata(fid)
            % Write system and environment metadata
            fprintf(fid, '───────────────────────────────────────────────────────────────────\n');
            fprintf(fid, '  SYSTEM METADATA\n');
            fprintf(fid, '───────────────────────────────────────────────────────────────────\n');
            
            % MATLAB/OS metadata helps explain runtime differences between
            % machines when comparing benchmark or sustainability outputs.
            rel_info = matlabRelease;
            fprintf(fid, 'MATLAB Version: %s (%s)\n', version, rel_info.Release);
            
            % Operating system
            if ispc
                os_name = 'Windows';
            elseif ismac
                os_name = 'macOS';
            elseif isunix
                os_name = 'Linux';
            else
                os_name = 'Unknown';
            end
            fprintf(fid, 'Operating System: %s\n', os_name);
            
            % Machine/host (if available)
            try
                [~, hostname] = system('hostname');
                hostname = strtrim(hostname);
                if isempty(hostname)
                    fprintf(fid, 'Machine: unavailable (empty hostname response)\n');
                else
                    fprintf(fid, 'Machine: %s\n', hostname);
                end
            catch ME
                fprintf(fid, 'Machine: unavailable (%s)\n', strtrim(ME.message));
            end
            
            % Git commit (if available)
            try
                repo_root = PathBuilder.get_repo_root();
                [status, commit_hash] = system(sprintf('cd "%s" && git rev-parse --short HEAD', repo_root));
                if status == 0
                    fprintf(fid, 'Git Commit: %s\n', strtrim(commit_hash));
                else
                    fprintf(fid, 'Git Commit: unavailable (git exit status %d)\n', status);
                end
            catch ME
                if RunReportGenerator.is_missing_symbol_error(ME)
                    RunReportGenerator.attach_project_paths_from_here();
                    try
                        repo_root = PathBuilder.get_repo_root();
                        [status, commit_hash] = system(sprintf('cd "%s" && git rev-parse --short HEAD', repo_root));
                        if status == 0
                            fprintf(fid, 'Git Commit: %s\n', strtrim(commit_hash));
                        else
                            fprintf(fid, 'Git Commit: unavailable (git exit status %d)\n', status);
                        end
                    catch retryME
                        fprintf(fid, 'Git Commit: unavailable (%s)\n', strtrim(retryME.message));
                    end
                else
                    fprintf(fid, 'Git Commit: unavailable (%s)\n', strtrim(ME.message));
                end
            end
            
            fprintf(fid, '\n');
        end
        
        function write_configuration(fid, Run_Config)
            % Write run configuration
            fprintf(fid, '───────────────────────────────────────────────────────────────────\n');
            fprintf(fid, '  RUN CONFIGURATION\n');
            fprintf(fid, '───────────────────────────────────────────────────────────────────\n');
            
            fields = fieldnames(Run_Config);
            for i = 1:length(fields)
                val = Run_Config.(fields{i});
                if isnumeric(val)
                    fprintf(fid, '%-20s: %g\n', fields{i}, val);
                elseif ischar(val) || isstring(val)
                    fprintf(fid, '%-20s: %s\n', fields{i}, val);
                end
            end
            fprintf(fid, '\n');
        end
        
        function write_parameters(fid, Parameters)
            % Write physics and numerical parameters
            fprintf(fid, '───────────────────────────────────────────────────────────────────\n');
            fprintf(fid, '  PARAMETERS (Physics & Numerics)\n');
            fprintf(fid, '───────────────────────────────────────────────────────────────────\n');
            
            % Group parameters logically
            fprintf(fid, '\nDomain:\n');
            RunReportGenerator.write_field_if_exists(fid, Parameters, 'Lx');
            RunReportGenerator.write_field_if_exists(fid, Parameters, 'Ly');
            
            fprintf(fid, '\nGrid:\n');
            RunReportGenerator.write_field_if_exists(fid, Parameters, 'Nx');
            RunReportGenerator.write_field_if_exists(fid, Parameters, 'Ny');
            
            fprintf(fid, '\nTime Integration:\n');
            RunReportGenerator.write_field_if_exists(fid, Parameters, 'dt');
            RunReportGenerator.write_field_if_exists(fid, Parameters, 'Tfinal');
            
            fprintf(fid, '\nPhysics:\n');
            RunReportGenerator.write_field_if_exists(fid, Parameters, 'nu');
            RunReportGenerator.write_field_if_exists(fid, Parameters, 'ic_type');
            
            fprintf(fid, '\n');
        end
        
        function write_settings(fid, Settings)
            % Write operational settings
            fprintf(fid, '───────────────────────────────────────────────────────────────────\n');
            fprintf(fid, '  SETTINGS (Operational)\n');
            fprintf(fid, '───────────────────────────────────────────────────────────────────\n');
            
            fields = fieldnames(Settings);
            for i = 1:length(fields)
                val = Settings.(fields{i});
                if islogical(val)
                    fprintf(fid, '%-25s: %s\n', fields{i}, string(val));
                elseif isnumeric(val)
                    fprintf(fid, '%-25s: %g\n', fields{i}, val);
                elseif ischar(val) || isstring(val)
                    fprintf(fid, '%-25s: %s\n', fields{i}, val);
                end
            end
            fprintf(fid, '\n');
        end
        
        function write_results(fid, Results)
            % Write derived metrics and results summary
            fprintf(fid, '───────────────────────────────────────────────────────────────────\n');
            fprintf(fid, '  RESULTS SUMMARY\n');
            fprintf(fid, '───────────────────────────────────────────────────────────────────\n');
            
            if isfield(Results, 'final_time')
                fprintf(fid, 'Final Time: %.4f\n', Results.final_time);
            end
            if isfield(Results, 'total_steps')
                fprintf(fid, 'Total Steps: %d\n', Results.total_steps);
            end
            if isfield(Results, 'wall_time')
                fprintf(fid, 'Wall Time: %.2f s\n', Results.wall_time);
            end
            if isfield(Results, 'max_omega')
                fprintf(fid, 'Max Vorticity: %.4e\n', Results.max_omega);
            end
            if isfield(Results, 'final_energy')
                fprintf(fid, 'Final Energy: %.4e\n', Results.final_energy);
            end
            if isfield(Results, 'final_enstrophy')
                fprintf(fid, 'Final Enstrophy: %.4e\n', Results.final_enstrophy);
            end
            
            fprintf(fid, '\n');
        end
        
        function write_file_manifest(fid, paths)
            % Write file manifest (where outputs are stored)
            fprintf(fid, '───────────────────────────────────────────────────────────────────\n');
            fprintf(fid, '  FILE MANIFEST\n');
            fprintf(fid, '───────────────────────────────────────────────────────────────────\n');
            
            fprintf(fid, 'Base Directory: %s\n', paths.base);
            fprintf(fid, '\nOutput Directories:\n');
            
            fields = fieldnames(paths);
            for i = 1:length(fields)
                if strcmp(fields{i}, 'base') || strcmp(fields{i}, 'method') || ...
                   strcmp(fields{i}, 'mode') || strcmp(fields{i}, 'identifier')
                    continue;
                end
                
                val = paths.(fields{i});
                if ischar(val) || isstring(val)
                    % Include file counts to quickly spot empty directories
                    % caused by incomplete or interrupted runs.
                    if exist(val, 'dir')
                        files = dir(val);
                        file_count = sum(~[files.isdir]);
                        fprintf(fid, '  %-25s: %s (%d files)\n', fields{i}, val, file_count);
                    else
                        fprintf(fid, '  %-25s: %s (not created)\n', fields{i}, val);
                    end
                end
            end
            
            fprintf(fid, '\n');
        end
        
        function write_footer(fid)
            % Write report footer
            fprintf(fid, '═══════════════════════════════════════════════════════════════════\n');
            fprintf(fid, '  END OF REPORT\n');
            fprintf(fid, '═══════════════════════════════════════════════════════════════════\n');
        end
        
        function attach_project_paths_from_here()
            if exist('PathSetup', 'class') ~= 8
                util_dir = fileparts(mfilename('fullpath'));           % .../Scripts/Infrastructure/Utilities
                scripts_dir = fileparts(fileparts(util_dir));          % .../Scripts
                addpath(genpath(scripts_dir));
                utilities_dir = fullfile(fileparts(scripts_dir), 'utilities');
                if exist(utilities_dir, 'dir') == 7
                    addpath(utilities_dir);
                end
            end
            PathSetup.attach_and_verify();
        end

        function tf = is_missing_symbol_error(ME)
            tf = strcmp(ME.identifier, 'MATLAB:UndefinedFunction') || ...
                 strcmp(ME.identifier, 'MATLAB:UndefinedFunctionOrVariable') || ...
                 contains(ME.message, 'Undefined function') || ...
                 contains(ME.message, 'Unrecognized function or variable');
        end

        function write_field_if_exists(fid, s, field)
            % Helper to write field if it exists
            if isfield(s, field)
                val = s.(field);
                if isnumeric(val)
                    fprintf(fid, '  %-18s: %g\n', field, val);
                elseif ischar(val) || isstring(val)
                    fprintf(fid, '  %-18s: %s\n', field, val);
                end
            end
        end
    end
end
