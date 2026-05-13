function payload = BuildRunReportPayload(run_id, solver, summary, configuration, monitor, results, paths, assets, compliance)
% BuildRunReportPayload  Canonical builder for report_payload_v3
%   payload = BuildRunReportPayload(...)
%   Required fields:
%     - run_id, solver, summary, configuration, monitor, results, paths, assets, compliance
%   Optional fields:
%     - title, generated_at, schema_version
%   All fields are top-level keys in the output struct.

    if nargin < 10, compliance = struct(); end
    if nargin < 9, assets = struct(); end
    if nargin < 8, paths = struct(); end
    if nargin < 7, results = struct(); end
    if nargin < 6, monitor = struct(); end
    if nargin < 5, configuration = struct(); end
    if nargin < 4, summary = struct(); end
    if nargin < 3, solver = ''; end
    if nargin < 2, run_id = ''; end

    payload = struct();
    payload.schema_version = 'report_payload_v3';
    payload.title = 'Simulation Run Report';
    payload.generated_at = datestr(now, 'yyyy-mm-dd HH:MM:SS');
    payload.run_id = run_id;
    payload.solver = solver;
    payload.summary = summary;
    payload.configuration = configuration;
    payload.monitor = monitor;
    payload.results = results;
    payload.paths = paths;
    payload.assets = assets;
    payload.compliance = compliance;
end
