classdef EnhancedReportGenerator
% ENHANCEDREPORTGENERATOR  Build Plotly.js interactive HTML reports.
%
% Generates a self-contained dark-themed HTML report from a simulation
% payload struct, with tabbed navigation, KPI cards, interactive Plotly.js
% time-series charts, embedded animations, and MathJax LaTeX rendering.
%
% Usage (called from UIController):
%   html = EnhancedReportGenerator.build_html(payload);
%
% Payload fields consumed:
%   .title           - report title string
%   .generated_at    - datetime string
%   .configuration   - struct of scalar/string config params
%   .metrics         - struct of named metric values
%   .solver          - solver method string
%   .monitor         - struct with fields: t, max_omega, energy, enstrophy, cpu
%   .animation_path  - (optional) absolute path to MP4/GIF animation
%   .run_id          - (optional) run identifier string

    methods(Static)

        function html = build_html(payload)
            % Build complete interactive HTML report from payload struct.
            html = char(strjoin([
                EnhancedReportGenerator.html_head(payload)
                EnhancedReportGenerator.html_body_open(payload)
                EnhancedReportGenerator.html_tab_nav()
                EnhancedReportGenerator.html_tab_summary(payload)
                EnhancedReportGenerator.html_tab_setup(payload)
                EnhancedReportGenerator.html_tab_monitoring(payload)
                EnhancedReportGenerator.html_tab_results(payload)
                EnhancedReportGenerator.html_body_close(payload)
            ], newline));
        end

        % ----------------------------------------------------------------
        function s = html_head(payload)
            title_str = EnhancedReportGenerator.esc(payload.title);
            s = string(sprintf([...
                '<!DOCTYPE html>\n' ...
                '<html lang="en">\n' ...
                '<head>\n' ...
                '<meta charset="UTF-8">\n' ...
                '<meta name="viewport" content="width=device-width, initial-scale=1.0">\n' ...
                '<title>%s</title>\n' ...
                '<script src="https://cdn.plot.ly/plotly-2.27.0.min.js" charset="utf-8"></script>\n' ...
                '<script src="https://cdn.jsdelivr.net/npm/mathjax@3/es5/tex-mml-chtml.js"></script>\n' ...
                '<style>\n%s\n</style>\n' ...
                '</head>\n'], title_str, EnhancedReportGenerator.css()));
        end

        % ----------------------------------------------------------------
        function s = html_body_open(payload)
            title_str    = EnhancedReportGenerator.esc(payload.title);
            gen_at       = EnhancedReportGenerator.esc(EnhancedReportGenerator.safe_field(payload, 'generated_at', ''));
            run_id       = EnhancedReportGenerator.safe_field(payload, 'run_id', '');
            solver       = EnhancedReportGenerator.safe_field(payload, 'solver', '');
            method_badge = '';
            if ~isempty(solver)
                method_badge = sprintf(' &nbsp;<span class="badge badge-blue">%s</span>', ...
                    EnhancedReportGenerator.esc(solver));
            end
            run_badge = '';
            if ~isempty(run_id)
                run_badge = sprintf(' &nbsp;<span class="badge badge-green">%s</span>', ...
                    EnhancedReportGenerator.esc(run_id));
            end
            s = string(sprintf([...
                '<body>\n' ...
                '<header>\n' ...
                '  <div class="container">\n' ...
                '    <h1>%s%s%s</h1>\n' ...
                '    <div class="meta">Generated: %s</div>\n' ...
                '  </div>\n' ...
                '</header>\n' ...
                '<div class="container">\n'], ...
                title_str, method_badge, run_badge, gen_at));
        end

        % ----------------------------------------------------------------
        function s = html_tab_nav()
            s = string([...
                '<nav class="nav-tabs">' ...
                '<button class="nav-tab active" onclick="showTab(''summary'')">Summary</button>' ...
                '<button class="nav-tab" onclick="showTab(''setup'')">Setup</button>' ...
                '<button class="nav-tab" onclick="showTab(''monitoring'')">Monitoring</button>' ...
                '<button class="nav-tab" onclick="showTab(''results'')">Results</button>' ...
                '</nav>']);
        end

        % ----------------------------------------------------------------
        function s = html_tab_summary(payload)
            % KPI cards + quick overview paragraph
            cfg     = EnhancedReportGenerator.safe_field(payload, 'configuration', struct());
            monitor = EnhancedReportGenerator.safe_field(payload, 'monitor', struct());

            % Extract KPI values
            kv = EnhancedReportGenerator.kpi_values(cfg, monitor);

            rows = [
                "<div id='tab-summary' class='tab-content active'>"
                "  <h2>Simulation Overview</h2>"
                "  <div class='kpi-grid'>"
            ];
            for k = 1:numel(kv)
                rows(end+1) = string(sprintf([ ...
                    '    <div class=''kpi-card'' style=''border-left-color:%s''>' ...
                    '      <div class=''kpi-label''>%s</div>' ...
                    '      <div class=''kpi-value'' style=''color:%s''>%s</div>' ...
                    '    </div>'], ...
                    kv(k).color, kv(k).label, kv(k).color, kv(k).value)); %#ok<AGROW>
            end
            rows = [rows; "  </div>"; "  <hr>"];

            % Method description block
            solver = EnhancedReportGenerator.safe_field(payload, 'solver', 'Unknown');
            rows(end+1) = string(sprintf([...
                '  <div class=''info-block''>' ...
                '    <h3>Method</h3><p>%s</p>' ...
                '  </div>'], EnhancedReportGenerator.esc(solver)));

            rows(end+1) = "</div>"; % close tab-content
            s = strjoin(rows, newline);
        end

        % ----------------------------------------------------------------
        function s = html_tab_setup(payload)
            cfg = EnhancedReportGenerator.safe_field(payload, 'configuration', struct());
            fn  = fieldnames(cfg);

            % Priority fields shown first
            priority = {'Nx','Ny','Lx','Ly','nu','dt','Tfinal','t_final','ic_type', ...
                'ic_coeff','method','analysis_method','delta','use_explicit_delta'};

            rows = [
                "<div id='tab-setup' class='tab-content'>"
                "  <h2>Simulation Configuration</h2>"
                "  <table>"
                "  <thead><tr><th>Parameter</th><th>Value</th></tr></thead>"
                "  <tbody>"
            ];

            % Write priority fields first
            written = {};
            for p = priority
                fname = p{1};
                if isfield(cfg, fname)
                    v = EnhancedReportGenerator.format_val(cfg.(fname));
                    rows(end+1) = string(sprintf("    <tr><td class='param-name'>%s</td><td>%s</td></tr>", ...
                        EnhancedReportGenerator.esc(fname), EnhancedReportGenerator.esc(v))); %#ok<AGROW>
                    written{end+1} = fname; %#ok<AGROW>
                end
            end

            % Write remaining fields (excluding non-scalar structs, vectors >10 elements)
            for k = 1:numel(fn)
                if ismember(fn{k}, written); continue; end
                v = cfg.(fn{k});
                if isstruct(v) || (isnumeric(v) && numel(v) > 10); continue; end
                vs = EnhancedReportGenerator.format_val(v);
                rows(end+1) = string(sprintf("    <tr><td class='param-name'>%s</td><td>%s</td></tr>", ...
                    EnhancedReportGenerator.esc(fn{k}), EnhancedReportGenerator.esc(vs))); %#ok<AGROW>
            end

            rows = [rows; "  </tbody></table>"; "</div>"];
            s = strjoin(rows, newline);
        end

        % ----------------------------------------------------------------
        function s = html_tab_monitoring(payload)
            monitor = EnhancedReportGenerator.safe_field(payload, 'monitor', struct());
            collectors = EnhancedReportGenerator.safe_field(payload, 'collectors', struct());
            has_data = isstruct(monitor) && isfield(monitor, 't') && numel(monitor.t) > 1;

            rows = [
                "<div id='tab-monitoring' class='tab-content'>"
                "  <h2>Live Monitor Time Series</h2>"
            ];

            if ~has_data
                rows(end+1) = "  <p class='muted'>No monitoring data recorded for this run.</p>";
            else
                t_json = jsonencode(monitor.t(:)');

                % Chart 1: Peak vorticity |ω|
                omega_json = jsonencode(EnhancedReportGenerator.safe_series(monitor, 'max_omega'));
                rows = [rows
                    "  <div class='plot-container'>"
                    "    <div id='chart-omega' style='height:280px;'></div>"
                    "  </div>"
                ];

                % Chart 2: Kinetic energy proxy
                energy_json = jsonencode(EnhancedReportGenerator.safe_series(monitor, 'energy'));
                rows = [rows
                    "  <div class='plot-container'>"
                    "    <div id='chart-energy' style='height:280px;'></div>"
                    "  </div>"
                ];

                % Chart 3: Enstrophy proxy
                enstr_json = jsonencode(EnhancedReportGenerator.safe_series(monitor, 'enstrophy'));
                rows = [rows
                    "  <div class='plot-container'>"
                    "    <div id='chart-enstr' style='height:280px;'></div>"
                    "  </div>"
                    "  <script>"
                    sprintf("  var t_data = %s;", t_json)
                    sprintf("  var omega_data = %s;", omega_json)
                    sprintf("  var energy_data = %s;", energy_json)
                    sprintf("  var enstr_data = %s;", enstr_json)
                    "  var layout_base = {"
                    "    paper_bgcolor:'#1a1d24', plot_bgcolor:'#262b36',"
                    "    font:{color:'#e8eaed', size:12},"
                    "    xaxis:{gridcolor:'#3a3f4b', title:{text:'Time (s)'}},"
                    "    yaxis:{gridcolor:'#3a3f4b'},"
                    "    margin:{l:60,r:20,t:40,b:50}"
                    "  };"
                    "  var plotly_config = {responsive:true, displayModeBar:true, modeBarButtonsToRemove:['lasso2d','select2d']};"
                    "  Plotly.newPlot('chart-omega', [{x:t_data, y:omega_data, type:'scatter', mode:'lines',"
                    "    name:'|omega|_max', line:{color:'#5bb4ff', width:2}}],"
                    "    Object.assign({}, layout_base, {title:{text:'Peak Vorticity |\\u03c9|', font:{color:'#5bb4ff'}},"
                    "      yaxis:{gridcolor:'#3a3f4b', title:{text:'max |\\u03c9|'}}}), plotly_config);"
                    "  Plotly.newPlot('chart-energy', [{x:t_data, y:energy_data, type:'scatter', mode:'lines',"
                    "    name:'Energy', line:{color:'#5cff8a', width:2}}],"
                    "    Object.assign({}, layout_base, {title:{text:'Kinetic Energy (proxy)', font:{color:'#5cff8a'}},"
                    "      yaxis:{gridcolor:'#3a3f4b', title:{text:'Energy'}}}), plotly_config);"
                    "  Plotly.newPlot('chart-enstr', [{x:t_data, y:enstr_data, type:'scatter', mode:'lines',"
                    "    name:'Enstrophy', line:{color:'#ffd166', width:2}}],"
                    "    Object.assign({}, layout_base, {title:{text:'Enstrophy (proxy)', font:{color:'#ffd166'}},"
                    "      yaxis:{gridcolor:'#3a3f4b', title:{text:'Enstrophy'}}}), plotly_config);"
                    "  </script>"
                ];
            end

            coverage_rows = EnhancedReportGenerator.safe_field(collectors, 'coverage_rows', struct([]));
            collector_bundle = ExternalCollectorDispatcher.collector_plot_bundle(monitor, struct(), struct());
            collector_panels = EnhancedReportGenerator.safe_field(collector_bundle, 'panels', struct([]));
            summary_lines = EnhancedReportGenerator.safe_field(collector_bundle, 'summary_lines', { ...
                'Collector Status:', '  MATLAB -> connected', '  HWiNFO -> off', '  iCUE -> off'});
            has_collector_plots = isstruct(collector_panels) && ~isempty(collector_panels) && ...
                any(arrayfun(@(panel) isfield(panel, 'traces') && ~isempty(panel.traces), collector_panels));

            rows(end+1) = "  <h3>Collector Comparison</h3>";
            collector_script = strings(0, 1);
            if has_collector_plots
                collector_script(end + 1) = "  <script>"; %#ok<AGROW>
                collector_script(end + 1) = "  var plotly_config = {responsive:true, displayModeBar:true, modeBarButtonsToRemove:['lasso2d','select2d']};"; %#ok<AGROW>
            end
            for k = 1:numel(collector_panels)
                panel = collector_panels(k);
                if ~isfield(panel, 'traces') || isempty(panel.traces)
                    rows(end+1) = string(sprintf([ ...
                        '<div class=''plot-container''><h4>%s</h4><p class=''muted''>%s</p></div>'], ...
                        EnhancedReportGenerator.esc(panel.title), ...
                        EnhancedReportGenerator.esc(panel.placeholder_text))); %#ok<AGROW>
                    continue;
                end

                plot_id = sprintf('chart-collector-%s', char(string(panel.id)));
                rows = [rows
                    "  <div class='plot-container'>"
                    string(sprintf("    <div id='%s' style='height:260px;'></div>", plot_id))
                    "  </div>"
                ];
                traces_json = jsonencode(EnhancedReportGenerator.collector_plotly_traces(panel.traces));
                layout_json = jsonencode(EnhancedReportGenerator.collector_plotly_layout(panel));
                collector_script(end + 1) = string(sprintf( ... %#ok<AGROW>
                    "  Plotly.newPlot('%s', %s, %s, plotly_config);", ...
                    plot_id, traces_json, layout_json));
            end
            if has_collector_plots
                collector_script(end + 1) = "  </script>"; %#ok<AGROW>
                for i = 1:numel(collector_script)
                    rows(end + 1) = collector_script(i); %#ok<AGROW>
                end
            end

            rows(end+1) = "  <h3>Collector Status</h3>";
            rows(end+1) = string(sprintf("<pre class='muted'>%s</pre>", ...
                EnhancedReportGenerator.join_html_lines(summary_lines)));
            if ~isempty(coverage_rows)
                rows(end+1) = "  <table>";
                rows(end+1) = "  <thead><tr><th>Metric</th><th>Domain</th><th>HWiNFO</th><th>iCUE</th><th>Preferred</th><th>Notes</th></tr></thead>";
                rows(end+1) = "  <tbody>";
                for k = 1:numel(coverage_rows)
                    rows(end+1) = string(sprintf( ...
                        '    <tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>', ...
                        EnhancedReportGenerator.esc(coverage_rows(k).raw_metric_name), ...
                        EnhancedReportGenerator.esc(coverage_rows(k).domain), ...
                        EnhancedReportGenerator.esc(string(coverage_rows(k).hwinfo_supported)), ...
                        EnhancedReportGenerator.esc(string(coverage_rows(k).icue_supported)), ...
                        EnhancedReportGenerator.esc(coverage_rows(k).preferred_source), ...
                        EnhancedReportGenerator.esc(coverage_rows(k).notes))); %#ok<AGROW>
                end
                rows(end+1) = "  </tbody></table>";
            end

            rows(end+1) = "</div>"; % close tab-content
            s = strjoin(rows, newline);
        end

        % ----------------------------------------------------------------
        function s = html_tab_results(payload)
            anim_path = EnhancedReportGenerator.safe_field(payload, 'animation_path', '');
            snap_paths = EnhancedReportGenerator.safe_field(payload, 'snapshot_paths', {});

            rows = [
                "<div id='tab-results' class='tab-content'>"
                "  <h2>Simulation Results</h2>"
            ];

            % Animation embed
            if ~isempty(anim_path) && exist(anim_path, 'file')
                [~, ~, ext] = fileparts(anim_path);
                anim_uri = ['file:///' strrep(anim_path, '\', '/')];
                if strcmpi(ext, '.gif')
                    rows(end+1) = string(sprintf([...
                        '  <div class=''animation-container''>' ...
                        '    <h3>Vorticity Evolution Animation</h3>' ...
                        '    <img src=''%s'' alt=''Vorticity animation'' style=''max-width:700px;border-radius:6px;''>' ...
                        '  </div>'], anim_uri));
                else
                    rows(end+1) = string(sprintf([...
                        '  <div class=''animation-container''>' ...
                        '    <h3>Vorticity Evolution Animation</h3>' ...
                        '    <video controls loop style=''max-width:700px;border-radius:6px;''>' ...
                        '      <source src=''%s'' type=''video/mp4''>' ...
                        '      Your browser does not support MP4 video.' ...
                        '    </video>' ...
                        '  </div>'], anim_uri));
                end
            else
                rows(end+1) = "  <p class='muted'>No animation file found. Enable animations and re-run.</p>";
            end

            % Snapshot PNG gallery
            if ~isempty(snap_paths) && iscell(snap_paths) && ~isempty(snap_paths{1})
                rows(end+1) = "  <h3>Vorticity Snapshots</h3>";
                rows(end+1) = "  <div class='snap-grid'>";
                for k = 1:numel(snap_paths)
                    if exist(snap_paths{k}, 'file')
                        img_uri = ['file:///' strrep(snap_paths{k}, '\', '/')];
                        rows(end+1) = string(sprintf(...
                            "    <div class='snap-card'><img src='%s' alt='t=%d'></div>", ...
                            img_uri, k)); %#ok<AGROW>
                    end
                end
                rows(end+1) = "  </div>";
            end

            rows(end+1) = "</div>"; % close tab-content
            s = strjoin(rows, newline);
        end

        % ----------------------------------------------------------------
        function s = html_body_close(payload) %#ok<INUSD>
            s = string([...
                '</div>' newline ...   % close .container
                '<footer>' ...
                '  <div class="container">' ...
                '    <p>MECH0020 Tsunami Vortex Analysis &mdash; UCL Mechanical Engineering</p>' ...
                '  </div>' ...
                '</footer>' newline ...
                '<script>' newline ...
                'function showTab(name) {' newline ...
                '  document.querySelectorAll(".tab-content").forEach(function(el){el.classList.remove("active");});' newline ...
                '  document.querySelectorAll(".nav-tab").forEach(function(el){el.classList.remove("active");});' newline ...
                '  var tab = document.getElementById("tab-" + name);' newline ...
                '  if (tab) tab.classList.add("active");' newline ...
                '  event.target.classList.add("active");' newline ...
                '}' newline ...
                '</script>' newline ...
                '</body>' newline ...
                '</html>']);
        end

        % ----------------------------------------------------------------
        %  Helpers
        % ----------------------------------------------------------------

        function kv = kpi_values(cfg, monitor)
            % Build KPI card data array: struct array with .label .value .color
            kv = struct('label', {}, 'value', {}, 'color', {});

            % Grid
            if isfield(cfg, 'Nx') && isfield(cfg, 'Ny')
                kv(end+1) = struct('label', 'Grid Resolution', ...
                    'value', sprintf('%d × %d', cfg.Nx, cfg.Ny), ...
                    'color', '#5bb4ff');
            end

            % Domain
            if isfield(cfg, 'Lx') && isfield(cfg, 'Ly')
                kv(end+1) = struct('label', 'Domain Size', ...
                    'value', sprintf('%.1f × %.1f', cfg.Lx, cfg.Ly), ...
                    'color', '#5bb4ff');
            end

            % Viscosity
            if isfield(cfg, 'nu')
                kv(end+1) = struct('label', '&nu; (viscosity)', ...
                    'value', sprintf('%.2e', cfg.nu), ...
                    'color', '#9aa0a6');
            end

            % Time
            if isfield(cfg, 'Tfinal')
                kv(end+1) = struct('label', 'T<sub>final</sub>', ...
                    'value', sprintf('%.2g s', cfg.Tfinal), ...
                    'color', '#9aa0a6');
            elseif isfield(cfg, 't_final')
                kv(end+1) = struct('label', 'T<sub>final</sub>', ...
                    'value', sprintf('%.2g s', cfg.t_final), ...
                    'color', '#9aa0a6');
            end

            % Peak vorticity (final value)
            if isstruct(monitor) && isfield(monitor, 'max_omega') && ~isempty(monitor.max_omega)
                omega_final = monitor.max_omega(end);
                kv(end+1) = struct('label', 'Final max|&omega;|', ...
                    'value', sprintf('%.4g', omega_final), ...
                    'color', '#ff6b6b');
            end

            % Iterations recorded
            if isstruct(monitor) && isfield(monitor, 't') && ~isempty(monitor.t)
                kv(end+1) = struct('label', 'Monitor Samples', ...
                    'value', sprintf('%d', numel(monitor.t)), ...
                    'color', '#5cff8a');
            end

            % IC type
            if isfield(cfg, 'ic_type')
                kv(end+1) = struct('label', 'IC Type', ...
                    'value', strrep(char(string(cfg.ic_type)), '_', ' '), ...
                    'color', '#ffd166');
            end
        end

        function y = safe_series(monitor, field)
            % Return numeric row vector for a monitor field, or [] if absent/empty
            if isfield(monitor, field) && ~isempty(monitor.(field))
                y = monitor.(field)(:)';
            else
                y = zeros(1, numel(monitor.t));
            end
        end

        function y = safe_nested_series(source_struct, field, n)
            if nargin < 3 || ~isfinite(n)
                n = 0;
            end
            if isstruct(source_struct) && isfield(source_struct, field) && ~isempty(source_struct.(field))
                y = source_struct.(field)(:)';
            else
                y = nan(1, n);
            end
        end

        function traces = collector_plotly_traces(trace_defs)
            traces = repmat(struct( ...
                'x', [], ...
                'y', [], ...
                'type', 'scatter', ...
                'mode', 'lines', ...
                'name', '', ...
                'line', struct(), ...
                'marker', struct()), 1, numel(trace_defs));
            for i = 1:numel(trace_defs)
                trace = trace_defs(i);
                mode = 'lines';
                marker = struct();
                if isfield(trace, 'marker') && ~strcmpi(char(string(trace.marker)), 'none')
                    mode = 'lines+markers';
                    marker = struct( ...
                        'symbol', char(string(trace.plotly_marker)), ...
                        'size', 7, ...
                        'color', char(string(trace.color_hex)));
                end
                traces(i) = struct( ...
                    'x', trace.x, ...
                    'y', trace.y, ...
                    'type', 'scatter', ...
                    'mode', mode, ...
                    'name', char(string(trace.label)), ...
                    'line', struct( ...
                        'color', char(string(trace.color_hex)), ...
                        'width', 2, ...
                        'dash', char(string(trace.plotly_dash))), ...
                    'marker', marker);
            end
        end

        function layout = collector_plotly_layout(panel)
            layout = struct( ...
                'paper_bgcolor', '#1a1d24', ...
                'plot_bgcolor', '#262b36', ...
                'font', struct('color', '#e8eaed', 'size', 12), ...
                'legend', struct('orientation', 'h'), ...
                'xaxis', struct('gridcolor', '#3a3f4b', 'title', struct('text', 'Time (s)')), ...
                'yaxis', struct('gridcolor', '#3a3f4b', 'title', struct('text', char(string(panel.ylabel)))), ...
                'margin', struct('l', 60, 'r', 20, 't', 40, 'b', 50), ...
                'title', struct('text', char(string(panel.title))));
        end

        function text = join_html_lines(lines)
            if isempty(lines)
                text = '';
                return;
            end
            if ischar(lines) || isstring(lines)
                text = EnhancedReportGenerator.esc(lines);
                return;
            end
            line_cells = cellfun(@(line) EnhancedReportGenerator.esc(line), ...
                cellstr(string(lines(:))), 'UniformOutput', false);
            text = strjoin(line_cells, newline);
        end

        function v = safe_field(s, fname, default)
            if isstruct(s) && isfield(s, fname) && ~isempty(s.(fname))
                v = s.(fname);
            else
                v = default;
            end
        end

        function txt = esc(val)
            % HTML-escape a scalar value to char
            txt = char(string(val));
            txt = strrep(txt, '&', '&amp;');
            txt = strrep(txt, '<', '&lt;');
            txt = strrep(txt, '>', '&gt;');
            txt = strrep(txt, '"', '&quot;');
        end

        function out = format_val(val)
            % Format a config value for HTML table display
            if isempty(val)
                out = '—';
            elseif isnumeric(val) && isscalar(val)
                if val == round(val)
                    out = sprintf('%d', val);
                else
                    out = sprintf('%.6g', val);
                end
            elseif isnumeric(val)
                out = ['[' num2str(val(:)', '%.4g ') ']'];
            elseif islogical(val)
                if val; out = 'true'; else; out = 'false'; end
            elseif ischar(val) || isstring(val)
                out = char(string(val));
            else
                out = class(val);
            end
        end

        % ----------------------------------------------------------------
        function css_str = css()
            css_str = [...
                ':root{' ...
                '--bg-dark:#0e1117;--bg-panel:#1a1d24;--bg-panel-alt:#262b36;' ...
                '--fg-text:#e8eaed;--fg-muted:#9aa0a6;' ...
                '--accent-cyan:#5bb4ff;--accent-green:#5cff8a;' ...
                '--accent-yellow:#ffd166;--accent-red:#ff6b6b;}' ...
                '*{margin:0;padding:0;box-sizing:border-box;}' ...
                'body{font-family:"Segoe UI",Arial,sans-serif;background:var(--bg-dark);' ...
                'color:var(--fg-text);line-height:1.6;font-size:14px;}' ...
                '.container{max-width:1300px;margin:0 auto;padding:0 20px 40px;}' ...
                'header{background:linear-gradient(135deg,#1a1d24,#262b36);' ...
                'padding:28px 20px 18px;border-bottom:3px solid var(--accent-cyan);' ...
                'margin-bottom:24px;}' ...
                'header h1{font-size:2em;color:var(--accent-cyan);font-weight:700;}' ...
                '.meta{color:var(--fg-muted);font-size:0.85em;margin-top:4px;}' ...
                '.badge{display:inline-block;padding:2px 10px;border-radius:12px;' ...
                'font-size:0.75em;font-weight:600;vertical-align:middle;}' ...
                '.badge-blue{background:#1e3a5f;color:#5bb4ff;}' ...
                '.badge-green{background:#1a3d2b;color:#5cff8a;}' ...
                'h2{font-size:1.4em;margin:28px 0 14px;padding-bottom:8px;' ...
                'border-bottom:2px solid #262b36;color:var(--fg-text);}' ...
                'h3{font-size:1.1em;margin:18px 0 8px;color:var(--accent-green);}' ...
                '.nav-tabs{display:flex;gap:6px;margin:0 0 20px;' ...
                'border-bottom:2px solid var(--bg-panel-alt);padding-bottom:0;}' ...
                '.nav-tab{padding:10px 22px;background:var(--bg-panel);color:var(--fg-muted);' ...
                'cursor:pointer;border:none;border-radius:6px 6px 0 0;font-size:0.95em;' ...
                'transition:all 0.2s;font-family:inherit;}' ...
                '.nav-tab:hover{background:var(--bg-panel-alt);color:var(--fg-text);}' ...
                '.nav-tab.active{background:var(--accent-cyan);color:#0e1117;font-weight:700;}' ...
                '.tab-content{display:none;}.tab-content.active{display:block;}' ...
                '.kpi-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));' ...
                'gap:16px;margin:18px 0;}' ...
                '.kpi-card{background:var(--bg-panel);padding:16px 20px;border-radius:8px;' ...
                'border-left:4px solid var(--accent-cyan);}' ...
                '.kpi-label{color:var(--fg-muted);font-size:0.82em;text-transform:uppercase;' ...
                'letter-spacing:0.05em;}' ...
                '.kpi-value{font-size:1.7em;font-weight:700;margin-top:4px;}' ...
                '.info-block{background:var(--bg-panel);padding:16px 20px;border-radius:8px;' ...
                'margin:12px 0;}' ...
                'table{width:100%;border-collapse:collapse;margin:12px 0;' ...
                'background:var(--bg-panel);border-radius:8px;overflow:hidden;}' ...
                'th,td{padding:10px 14px;text-align:left;border-bottom:1px solid var(--bg-panel-alt);}' ...
                'th{background:var(--bg-panel-alt);font-weight:600;color:var(--accent-cyan);' ...
                'font-size:0.85em;text-transform:uppercase;letter-spacing:0.05em;}' ...
                'td{color:var(--fg-text);}.param-name{color:var(--fg-muted);font-family:monospace;}' ...
                'tr:last-child td{border-bottom:none;}' ...
                '.plot-container{background:var(--bg-panel);padding:16px;border-radius:8px;' ...
                'margin:14px 0;}' ...
                '.animation-container{background:var(--bg-panel);padding:20px;border-radius:8px;' ...
                'text-align:center;margin:14px 0;}' ...
                '.animation-container video,.animation-container img{max-width:100%;border-radius:6px;}' ...
                '.snap-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));' ...
                'gap:12px;margin:14px 0;}' ...
                '.snap-card{background:var(--bg-panel);border-radius:6px;overflow:hidden;}' ...
                '.snap-card img{width:100%;display:block;}' ...
                'hr{border:none;border-top:1px solid var(--bg-panel-alt);margin:20px 0;}' ...
                'p.muted{color:var(--fg-muted);font-style:italic;margin:8px 0;}' ...
                'footer{background:var(--bg-panel);margin-top:40px;padding:16px 20px;' ...
                'border-top:1px solid var(--bg-panel-alt);color:var(--fg-muted);font-size:0.85em;}' ...
            ];
        end

    end % methods(Static)
end % classdef
