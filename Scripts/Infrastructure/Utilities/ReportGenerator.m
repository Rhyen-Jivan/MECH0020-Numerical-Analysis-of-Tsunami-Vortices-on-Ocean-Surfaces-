% ========================================================================
% ReportGenerator - HTML Report Generation (Static Class)
% ========================================================================
% Generates publication-quality HTML reports for solver results
% Used by: Analysis.m (sweep/convergence modes)
%
% Usage: ReportGenerator.generate_solver_report(T, meta, settings, run_mode)
%
% Methods:
%   generate_solver_report(T, meta, settings, run_mode) - Main report generator
%   table_to_html(T) - Convert MATLAB table to HTML table
%   format_report_value(val) - Format value for HTML display
%   escape_html(txt) - Escape HTML special characters
%   collect_report_figures(settings, mode_str, max_figs) - Gather figure paths
%
% Created: 2026-02-06
% Part of: Tsunami Vortex Analysis Framework - Phase 2 Refactoring
% ========================================================================

classdef ReportGenerator
    methods(Static)
        function report_path = generate_solver_report(T, meta, settings, run_mode)
            % Generate comprehensive HTML solver report
            report_path = "";
            if ~isfield(settings, 'results_dir') || isempty(settings.results_dir)
                return;
            end
            report_dir = fullfile(settings.results_dir, "Reports");
            if ~exist(report_dir, 'dir')
                mkdir(report_dir);
            end
            ts = char(datetime('now', 'Format', 'yyyyMMdd_HHmmss'));
            report_name = sprintf('solver_report_%s_%s.html', string(run_mode), ts);
            report_path = fullfile(report_dir, report_name);
            fid = fopen(report_path, 'w');
            if fid < 0
                warning('Failed to create solver report: %s', report_path);
                return;
            end
            cleaner = onCleanup(@() fclose(fid));
            
            max_rows = 50; max_figs = 24;
            if isfield(settings, 'report')
                if isfield(settings.report, 'max_rows'); max_rows = settings.report.max_rows; end
                if isfield(settings.report, 'max_figures'); max_figs = settings.report.max_figures; end
            end
            
            mode_str = string(run_mode); run_count = height(T); ok_count = NaN;
            if ismember("run_ok", T.Properties.VariableNames)
                ok_count = sum(T.run_ok == true);
            end
            
            figs = ReportGenerator.collect_report_figures(settings, mode_str, max_figs);
            
            fprintf(fid, '<!DOCTYPE html><html><head><meta charset="utf-8">');
            fprintf(fid, '<title>Solver Report - %s</title>', ReportGenerator.escape_html(mode_str));
            fprintf(fid, '<style>body{font-family:Segoe UI,Arial,sans-serif;margin:24px;color:#222}');
            fprintf(fid, 'h1,h2{color:#0b3d91}table{border-collapse:collapse;width:100%%;font-size:12px}');
            fprintf(fid, 'th,td{border:1px solid #ddd;padding:6px}th{background:#f3f6fb}');
            fprintf(fid, '.kpi{display:flex;gap:16px;margin:12px 0}');
            fprintf(fid, '.kpi div{padding:10px 12px;border:1px solid #e3e7ef;border-radius:6px;background:#fafbfe}');
            fprintf(fid, '.img-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:12px}');
            fprintf(fid, 'img{max-width:100%%;border:1px solid #e3e7ef;border-radius:6px}</style>');
            fprintf(fid, '</head><body>');
            fprintf(fid, '<h1>Solver Report: %s</h1>', ReportGenerator.escape_html(mode_str));
            fprintf(fid, '<p><strong>Generated:</strong> %s</p>', ReportGenerator.escape_html(char(datetime("now"))));
            
            fprintf(fid, '<div class="kpi">');
            fprintf(fid, '<div><strong>Runs</strong><br>%d</div>', run_count);
            if isfinite(ok_count)
                fprintf(fid, '<div><strong>Successful</strong><br>%d</div>', ok_count);
            end
            fprintf(fid, '</div>');
            
            if isstruct(meta)
                fprintf(fid, '<h2>Metadata</h2><table><tbody>');
                meta_fields = fieldnames(meta);
                for i = 1:numel(meta_fields)
                    key = meta_fields{i}; val = meta.(key);
                    fprintf(fid, '<tr><th>%s</th><td>%s</td></tr>', ...
                        ReportGenerator.escape_html(key), ...
                        ReportGenerator.escape_html(ReportGenerator.format_report_value(val)));
                end
                fprintf(fid, '</tbody></table>');
            end
            
            fprintf(fid, '<h2>Results (first %d rows)</h2>', min(run_count, max_rows));
            T_view = T(1:min(run_count, max_rows), :);
            fprintf(fid, '%s', ReportGenerator.table_to_html(T_view));
            
            if ~isempty(figs)
                fprintf(fid, '<h2>Figures</h2><div class="img-grid">');
                for k = 1:numel(figs)
                    fprintf(fid, '<div><img src="%s" alt="figure"></div>', ReportGenerator.escape_html(figs(k)));
                end
                fprintf(fid, '</div>');
            end
            
            fprintf(fid, '</body></html>');
        end
        
        function html = table_to_html(T)
            % Convert MATLAB table to HTML table
            headers = T.Properties.VariableNames;
            html = '<table><thead><tr>';
            for i = 1:numel(headers)
                html = html + "<th>" + ReportGenerator.escape_html(headers{i}) + "</th>";
            end
            html = html + "</tr></thead><tbody>";
            for r = 1:height(T)
                html = html + "<tr>";
                for c = 1:numel(headers)
                    val = T{r, c};
                    html = html + "<td>" + ReportGenerator.escape_html(ReportGenerator.format_report_value(val)) + "</td>";
                end
                html = html + "</tr>";
            end
            html = html + "</tbody></table>";
        end
        
        function out = format_report_value(val)
            % Format value for HTML display
            if isempty(val); out = ""; return; end
            if iscell(val)
                if isscalar(val); out = ReportGenerator.format_report_value(val{1});
                else; out = mat2str(val); end
                return;
            end
            if isstring(val); out = strjoin(val, ", "); return; end
            if ischar(val); out = string(val); return; end
            if isnumeric(val)
                if isscalar(val); out = sprintf('%.6g', val);
                else; out = mat2str(val); end
                return;
            end
            if isdatetime(val); out = char(val); return; end
            out = string(val);
        end
        
        function txt = escape_html(txt)
            % Escape HTML special characters
            txt = string(txt);
            txt = replace(txt, "&", "&amp;");
            txt = replace(txt, "<", "&lt;");
            txt = replace(txt, ">", "&gt;");
            txt = replace(txt, '\"', "&quot;");
        end
        
        function figs = collect_report_figures(settings, mode_str, max_figs)
            % Gather figure paths for report inclusion
            figs = strings(0, 1);
            fig_root = "Figures";
            if isfield(settings, 'figures') && isfield(settings.figures, 'root_dir') && ~isempty(settings.figures.root_dir)
                fig_root = string(settings.figures.root_dir);
            end
            if ~exist(fig_root, 'dir'); return; end
            files = dir(fullfile(fig_root, '**', '*.png'));
            if isempty(files); return; end
            mode_tag = upper(string(mode_str));
            keep = false(numel(files), 1);
            for i = 1:numel(files)
                keep(i) = contains(upper(string(files(i).folder)), mode_tag);
            end
            files = files(keep);
            if isempty(files); return; end
            files = files(1:min(max_figs, numel(files)));
            figs = strings(numel(files), 1);
            for i = 1:numel(files)
                full_path = fullfile(files(i).folder, files(i).name);
                full_path = strrep(full_path, '\\\\', '/');
                figs(i) = "file:///" + full_path;
            end
        end
    end
end