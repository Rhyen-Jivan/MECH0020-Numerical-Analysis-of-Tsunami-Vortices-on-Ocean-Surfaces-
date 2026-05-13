classdef RunReportPipeline
    % RunReportPipeline - Build per-run report artifacts from a payload.
    %
    % Outputs generated in paths.reports:
    %   - report_payload.json
    %   - run_report.qmd
    %   - run_report.html
    %   - run_report.pdf

    methods (Static)
        function report_artifacts = generate(report_payload, paths, settings)
            if ~exist(paths.reports, 'dir')
                mkdir(paths.reports);
            end

            payload_path = '';
            if RunReportPipeline.json_enabled(settings)
                payload_path = fullfile(paths.reports, 'report_payload.json');
                RunReportPipeline.write_json(payload_path, report_payload);
            end

            qmd_path = fullfile(paths.reports, 'run_report.qmd');
            RunReportPipeline.write_qmd(qmd_path, report_payload, settings);

            html_path = fullfile(paths.reports, 'run_report.html');
            pdf_path = fullfile(paths.reports, 'run_report.pdf');

            [html_ok, pdf_ok, engine] = RunReportPipeline.render_quarto(qmd_path, html_path, pdf_path);

            if ~html_ok
                RunReportPipeline.write_html_fallback(html_path, report_payload);
                engine = 'fallback';
            end
            if ~pdf_ok
                RunReportPipeline.write_pdf_fallback(pdf_path, report_payload);
                if ~html_ok
                    engine = 'fallback';
                else
                    engine = [engine, '+pdf_fallback'];
                end
            end

            report_artifacts = struct();
            report_artifacts.payload_path = payload_path;
            report_artifacts.qmd_path = qmd_path;
            report_artifacts.html_path = html_path;
            report_artifacts.pdf_path = pdf_path;
            report_artifacts.engine = engine;
            report_artifacts.generated_at_utc = char(datetime('now', 'TimeZone', 'UTC', 'Format', 'yyyy-MM-dd''T''HH:mm:ss''Z'''));
        end
    end

    methods (Static, Access = private)
        function write_qmd(qmd_path, payload, settings)
            title_txt = RunReportPipeline.safe_field(payload, 'title', 'Simulation Run Report');
            template_txt = 'default';
            if isfield(settings, 'reporting') && isfield(settings.reporting, 'template')
                template_txt = char(string(settings.reporting.template));
            end

            lines = {};
            lines{end + 1} = '---'; %#ok<AGROW>
            lines{end + 1} = ['title: "', RunReportPipeline.escape_quotes(title_txt), '"']; %#ok<AGROW>
            lines{end + 1} = ['subtitle: "Template: ', RunReportPipeline.escape_quotes(template_txt), '"']; %#ok<AGROW>
            lines{end + 1} = 'format:'; %#ok<AGROW>
            lines{end + 1} = '  html: default'; %#ok<AGROW>
            lines{end + 1} = '  pdf: default'; %#ok<AGROW>
            lines{end + 1} = '---'; %#ok<AGROW>
            lines{end + 1} = ''; %#ok<AGROW>
            lines{end + 1} = '## Run Summary'; %#ok<AGROW>
            lines{end + 1} = ''; %#ok<AGROW>
            lines = [lines, RunReportPipeline.markdown_table(payload.summary)]; %#ok<AGROW>
            lines{end + 1} = ''; %#ok<AGROW>
            lines{end + 1} = '## Configuration'; %#ok<AGROW>
            lines{end + 1} = ''; %#ok<AGROW>
            lines = [lines, RunReportPipeline.markdown_table(payload.configuration)]; %#ok<AGROW>
            lines{end + 1} = ''; %#ok<AGROW>
            lines{end + 1} = '## Metrics'; %#ok<AGROW>
            lines{end + 1} = ''; %#ok<AGROW>
            lines = [lines, RunReportPipeline.markdown_table(payload.metrics)]; %#ok<AGROW>
            lines{end + 1} = ''; %#ok<AGROW>
            lines{end + 1} = '## Paths'; %#ok<AGROW>
            lines{end + 1} = ''; %#ok<AGROW>
            lines = [lines, RunReportPipeline.markdown_table(payload.paths)]; %#ok<AGROW>
            lines{end + 1} = ''; %#ok<AGROW>
            lines{end + 1} = '## Notes'; %#ok<AGROW>
            lines{end + 1} = ''; %#ok<AGROW>
            if RunReportPipeline.json_enabled(settings)
                lines{end + 1} = '- Payload source: `report_payload.json`'; %#ok<AGROW>
            else
                lines{end + 1} = '- Payload source: in-memory report payload (`save_json=false`).'; %#ok<AGROW>
            end
            lines{end + 1} = '- Rendering intent: Quarto HTML + PDF (fallbacks available).'; %#ok<AGROW>

            fid = fopen(qmd_path, 'w');
            if fid == -1
                error('RunReportPipeline:WriteFailed', 'Could not write QMD file: %s', qmd_path);
            end
            fprintf(fid, '%s\n', lines{:});
            fclose(fid);
        end

        function table_lines = markdown_table(s)
            keys = fieldnames(s);
            table_lines = {'| Field | Value |', '|---|---|'};
            for i = 1:numel(keys)
                key = keys{i};
                value = RunReportPipeline.format_value(s.(key));
                table_lines{end + 1} = ['| `', key, '` | ', value, ' |']; %#ok<AGROW>
            end
        end

        function [html_ok, pdf_ok, engine] = render_quarto(qmd_path, html_path, pdf_path)
            html_ok = false;
            pdf_ok = false;
            engine = 'quarto';

            [quarto_found, quarto_cmd] = RunReportPipeline.resolve_quarto_command();
            if ~quarto_found
                engine = 'fallback';
                return;
            end

            qmd_parent = fileparts(qmd_path);
            html_cmd = sprintf('%s render "%s" --to html --output "%s"', quarto_cmd, qmd_path, html_path);
            pdf_cmd = sprintf('%s render "%s" --to pdf --output "%s"', quarto_cmd, qmd_path, pdf_path);

            [status_html, ~] = RunReportPipeline.run_in_dir(html_cmd, qmd_parent);
            html_ok = (status_html == 0) && exist(html_path, 'file');

            [status_pdf, ~] = RunReportPipeline.run_in_dir(pdf_cmd, qmd_parent);
            pdf_ok = (status_pdf == 0) && exist(pdf_path, 'file');
        end

        function [ok, cmd] = resolve_quarto_command()
            ok = false;
            cmd = 'quarto';
            [status, ~] = system('quarto --version');
            if status == 0
                ok = true;
                return;
            end

            % Windows fallback for local Quarto installation.
            local_quarto = fullfile(getenv('LOCALAPPDATA'), 'Programs', 'Quarto', 'bin', 'quarto.exe');
            if exist(local_quarto, 'file')
                ok = true;
                cmd = ['"', local_quarto, '"'];
            end
        end

        function [status, output] = run_in_dir(command, working_dir)
            current_dir = pwd;
            cleanup = onCleanup(@() cd(current_dir));
            cd(working_dir);
            [status, output] = system(command);
            clear cleanup;
        end

        function write_html_fallback(html_path, payload)
            fid = fopen(html_path, 'w');
            if fid == -1
                error('RunReportPipeline:WriteFailed', 'Could not write HTML report: %s', html_path);
            end

            fprintf(fid, '<!doctype html>\n<html><head><meta charset="utf-8"><title>%s</title>', ...
                RunReportPipeline.escape_html(RunReportPipeline.safe_field(payload, 'title', 'Run Report')));
            fprintf(fid, '<style>body{font-family:Segoe UI,Arial,sans-serif;margin:2rem;}table{border-collapse:collapse;width:100%%;}th,td{border:1px solid #ddd;padding:0.5rem;}th{background:#f2f2f2;text-align:left;}h2{margin-top:1.5rem;}</style>');
            fprintf(fid, '</head><body>');
            fprintf(fid, '<h1>%s</h1>', RunReportPipeline.escape_html(RunReportPipeline.safe_field(payload, 'title', 'Run Report')));

            RunReportPipeline.write_html_table(fid, 'Run Summary', payload.summary);
            RunReportPipeline.write_html_table(fid, 'Configuration', payload.configuration);
            RunReportPipeline.write_html_table(fid, 'Metrics', payload.metrics);
            RunReportPipeline.write_html_table(fid, 'Paths', payload.paths);

            fprintf(fid, '</body></html>');
            fclose(fid);
        end

        function write_html_table(fid, title_txt, s)
            fprintf(fid, '<h2>%s</h2><table><tr><th>Field</th><th>Value</th></tr>', ...
                RunReportPipeline.escape_html(title_txt));
            keys = fieldnames(s);
            for i = 1:numel(keys)
                key = keys{i};
                value = RunReportPipeline.format_value(s.(key));
                fprintf(fid, '<tr><td><code>%s</code></td><td>%s</td></tr>', ...
                    RunReportPipeline.escape_html(key), RunReportPipeline.escape_html(value));
            end
            fprintf(fid, '</table>');
        end

        function write_pdf_fallback(pdf_path, payload)
            % Generate a compact text PDF using built-in plotting/export APIs.
            fig = figure('Visible', 'off', 'Position', [100, 100, 1000, 1300], 'Color', [1, 1, 1]);
            ax = axes('Parent', fig, 'Position', [0, 0, 1, 1], 'Visible', 'off');
            xlim(ax, [0, 1]);
            ylim(ax, [0, 1]);

            lines = RunReportPipeline.payload_lines(payload);
            text(0.03, 0.98, strjoin(lines, newline), ...
                'Parent', ax, ...
                'Interpreter', 'none', ...
                'FontName', 'Courier New', ...
                'FontSize', 9, ...
                'VerticalAlignment', 'top');

            try
                exportgraphics(fig, pdf_path, 'ContentType', 'vector');
            catch ME
                RunReportPipeline.warn_once('RunReportPipeline:ExportGraphicsFallback', ...
                    'exportgraphics failed for report PDF; falling back to print -dpdf: %s', ME.message);
                print(fig, pdf_path, '-dpdf');
            end
            close(fig);
        end

        function lines = payload_lines(payload)
            lines = {};
            lines{end + 1} = RunReportPipeline.safe_field(payload, 'title', 'Run Report'); %#ok<AGROW>
            lines{end + 1} = repmat('=', 1, 80); %#ok<AGROW>
            lines{end + 1} = 'Run Summary'; %#ok<AGROW>
            lines = [lines, RunReportPipeline.struct_lines(payload.summary), {''}]; %#ok<AGROW>
            lines{end + 1} = 'Configuration'; %#ok<AGROW>
            lines = [lines, RunReportPipeline.struct_lines(payload.configuration), {''}]; %#ok<AGROW>
            lines{end + 1} = 'Metrics'; %#ok<AGROW>
            lines = [lines, RunReportPipeline.struct_lines(payload.metrics), {''}]; %#ok<AGROW>
            lines{end + 1} = 'Paths'; %#ok<AGROW>
            lines = [lines, RunReportPipeline.struct_lines(payload.paths)]; %#ok<AGROW>
        end

        function lines = struct_lines(s)
            keys = fieldnames(s);
            lines = cell(1, numel(keys));
            for i = 1:numel(keys)
                key = keys{i};
                lines{i} = sprintf('%-30s : %s', key, RunReportPipeline.format_value(s.(key)));
            end
        end

        function out = format_value(value)
            if isstring(value) || ischar(value)
                out = RunReportPipeline.safe_string(value);
            elseif islogical(value)
                out = RunReportPipeline.safe_string(value);
            elseif isnumeric(value)
                if isscalar(value)
                    if isfinite(value)
                        out = num2str(value, '%.8g');
                    else
                        out = RunReportPipeline.safe_string(value);
                    end
                else
                    sz = size(value);
                    out = sprintf('[numeric %s]', mat2str(sz));
                end
            elseif iscell(value)
                out = sprintf('[cell %s]', mat2str(size(value)));
            else
                out = sprintf('[%s]', class(value));
            end
        end

        function out = safe_field(s, field_name, default)
            if isfield(s, field_name)
                out = s.(field_name);
            else
                out = default;
            end
        end

        function write_json(path_str, payload)
            encoded = jsonencode(payload);
            fid = fopen(path_str, 'w');
            if fid == -1
                error('RunReportPipeline:WriteFailed', 'Could not write JSON: %s', path_str);
            end
            fprintf(fid, '%s', encoded);
            fclose(fid);
        end

        function out = escape_quotes(in)
            out = strrep(RunReportPipeline.safe_string(in), '"', '\"');
        end

        function out = escape_html(in)
            out = RunReportPipeline.safe_string(in);
            out = strrep(out, '&', '&amp;');
            out = strrep(out, '<', '&lt;');
            out = strrep(out, '>', '&gt;');
            out = strrep(out, '"', '&quot;');
        end

        function out = safe_string(value)
            try
                sval = string(value);
                if isempty(sval)
                    out = '';
                    return;
                end
                if any(ismissing(sval))
                    out = 'missing';
                    return;
                end
                if isscalar(sval)
                    out = char(sval);
                else
                    out = strjoin(cellstr(sval(:)), ', ');
                end
            catch ME
                RunReportPipeline.warn_once('RunReportPipeline:SafeStringFallback', ...
                    'safe_string fallback used for value class %s: %s', class(value), ME.message);
                out = sprintf('[%s]', class(value));
            end
        end

        function tf = json_enabled(settings)
            tf = false;
            if isstruct(settings) && isfield(settings, 'save_json') && ~isempty(settings.save_json)
                tf = logical(settings.save_json);
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
            warning(id, message, varargin{:});
        end
    end
end
