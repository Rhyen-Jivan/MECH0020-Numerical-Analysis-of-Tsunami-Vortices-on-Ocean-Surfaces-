function output = run_hwinfo_pro_cli_proof_sample(duration_seconds, poll_rate_ms, output_root, launch_timeout_s, csv_timeout_s)
% run_hwinfo_pro_cli_proof_sample  One-shot HWiNFO Pro CSV logging proof.
%
% Usage:
%   out = run_hwinfo_pro_cli_proof_sample()
%   out = run_hwinfo_pro_cli_proof_sample(5, 1000)
%   out = run_hwinfo_pro_cli_proof_sample(5, 1000, 'C:\path\to\proof_root')
%   out = run_hwinfo_pro_cli_proof_sample(5, 1000, [], 20, 45)
%
% This helper is intended to be run inside an elevated MATLAB session when
% validating the HWiNFO Pro CSV-first phase telemetry workflow.

    if nargin < 1 || ~isnumeric(duration_seconds) || ~isfinite(duration_seconds)
        duration_seconds = 5;
    end
    if nargin < 2 || ~isnumeric(poll_rate_ms) || ~isfinite(poll_rate_ms)
        poll_rate_ms = 1000;
    end
    if nargin < 3 || isempty(output_root)
        output_root = fullfile(tempdir, 'tsunami_hwinfo_pro_cli_proof');
    end
    if nargin < 4 || ~isnumeric(launch_timeout_s) || ~isfinite(launch_timeout_s)
        launch_timeout_s = 20;
    end
    if nargin < 5 || ~isnumeric(csv_timeout_s) || ~isfinite(csv_timeout_s)
        csv_timeout_s = 45;
    end

    duration_seconds = max(3, round(double(duration_seconds)));
    poll_rate_ms = max(100, round(double(poll_rate_ms)));
    launch_timeout_s = max(5, double(launch_timeout_s));
    csv_timeout_s = max(5, double(csv_timeout_s));
    output_root = char(string(output_root));

    addpath(genpath(fullfile(fileparts(mfilename('fullpath')), '..')));

    hwinfo_path = local_resolve_hwinfo_path();
    if isempty(hwinfo_path)
        error('run_hwinfo_pro_cli_proof_sample:MissingExecutable', ...
            'HWiNFO executable could not be resolved from the canonical adapter paths.');
    end

    if exist(output_root, 'dir') == 7
        rmdir(output_root, 's');
    end
    mkdir(output_root);

    run_id = sprintf('hwinfo_proof_%s', char(datetime('now', 'Format', 'yyyyMMdd_HHmmss')));
    csv_path = fullfile(output_root, 'HWiNFO_Telemetry.csv');
    session_json_path = fullfile(output_root, 'HWiNFO_Pro_Session.json');
    batch_script_path = fullfile(output_root, 'hwinfo_pro_launch.cmd');

    config = struct( ...
        'exe_path', hwinfo_path, ...
        'csv_path', csv_path, ...
        'session_json_path', session_json_path, ...
        'batch_script_path', batch_script_path, ...
        'poll_rate_ms', poll_rate_ms, ...
        'launch_timeout_s', launch_timeout_s, ...
        'csv_timeout_s', csv_timeout_s, ...
        'force_stop_fallback', true, ...
        'write_direct', true, ...
        'timezone_name', char(string(datetime('now', 'TimeZone', 'local').TimeZone)));

    probe_response = HWiNFOProCLIController.probe_session(config);
    if ~(isstruct(probe_response) && isfield(probe_response, 'ok') && probe_response.ok)
        error('run_hwinfo_pro_cli_proof_sample:ProbeFailed', ...
            'HWiNFO probe failed: %s', local_pick_text(probe_response, {'message', 'status'}, 'probe failed'));
    end

    start_response = HWiNFOProCLIController.start_session(run_id, config);
    if ~(isstruct(start_response) && isfield(start_response, 'ok') && start_response.ok)
        error('run_hwinfo_pro_cli_proof_sample:StartFailed', ...
            'HWiNFO start failed: %s', local_pick_text(start_response, {'message', 'status'}, 'start failed'));
    end

    pause(duration_seconds);

    stop_response = HWiNFOProCLIController.stop_session(struct( ...
        'session_json_path', session_json_path));
    if ~(isstruct(stop_response) && isfield(stop_response, 'ok') && stop_response.ok)
        error('run_hwinfo_pro_cli_proof_sample:StopFailed', ...
            'HWiNFO stop failed: %s', local_pick_text(stop_response, {'message', 'status'}, 'stop failed'));
    end

    csv_table = readtable(csv_path);
    output = struct( ...
        'run_id', run_id, ...
        'output_root', output_root, ...
        'csv_path', csv_path, ...
        'session_json_path', session_json_path, ...
        'batch_script_path', batch_script_path, ...
        'launch_timeout_s', launch_timeout_s, ...
        'csv_timeout_s', csv_timeout_s, ...
        'row_count', height(csv_table), ...
        'column_count', width(csv_table), ...
        'column_names', {csv_table.Properties.VariableNames}, ...
        'probe_response', probe_response, ...
        'start_response', start_response, ...
        'stop_response', stop_response);

    fprintf('HWiNFO Pro CLI proof complete | rows=%d | cols=%d | csv=%s\n', ...
        output.row_count, output.column_count, output.csv_path);
end

function hwinfo_path = local_resolve_hwinfo_path()
    hwinfo_path = '';
    candidates = ExternalCollectorAdapters.default_paths('hwinfo');
    for i = 1:numel(candidates)
        if exist(candidates{i}, 'file') == 2
            hwinfo_path = char(string(candidates{i}));
            return;
        end
    end
end

function value = local_pick_text(s, keys, fallback)
    value = fallback;
    if ~isstruct(s)
        return;
    end
    for i = 1:numel(keys)
        key = keys{i};
        if isfield(s, key)
            value = char(string(s.(key)));
            return;
        end
    end
end
