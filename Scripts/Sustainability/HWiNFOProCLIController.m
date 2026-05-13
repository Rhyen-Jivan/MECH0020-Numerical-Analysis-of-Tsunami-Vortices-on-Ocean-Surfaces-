classdef HWiNFOProCLIController
    % HWiNFOProCLIController - MATLAB wrapper for the Pro CLI CSV controller.
    %
    % Runtime role:
    %   - Launch one HWiNFO Pro logging session for a phase workflow
    %   - Stop the session gracefully and validate the raw CSV
    %   - Normalize raw HWiNFO CSV logs into canonical telemetry tables

    methods (Static)
        function response = start_session(run_id, config)
            if nargin < 1
                run_id = '';
            end
            if nargin < 2 || ~isstruct(config)
                config = struct();
            end
            response = HWiNFOProCLIController.invoke_python('start', run_id, config);
        end

        function response = stop_session(config)
            if nargin < 1 || ~isstruct(config)
                config = struct();
            end
            response = HWiNFOProCLIController.invoke_python('stop', '', config);
        end

        function response = probe_session(config)
            if nargin < 1 || ~isstruct(config)
                config = struct();
            end
            response = HWiNFOProCLIController.invoke_python('probe', '', config);
        end

        function response = normalize_csv_dataset(config)
            if nargin < 1 || ~isstruct(config)
                config = struct();
            end
            response = HWiNFOProCLIController.invoke_python('normalize', '', config);
        end
    end

    methods (Static, Access = private)
        function response = invoke_python(command_name, run_id, config)
            script_path = fullfile(fileparts(mfilename('fullpath')), 'hwinfo_pro_cli.py');
            if exist(script_path, 'file') ~= 2
                error('HWiNFOProCLIController:MissingScript', ...
                    'hwinfo_pro_cli.py not found at: %s', script_path);
            end

            python_exe = HWiNFOProCLIController.resolve_python_executable();
            config_path = [tempname, '.json'];
            cleanup_obj = onCleanup(@() HWiNFOProCLIController.safe_delete(config_path)); %#ok<NASGU>
            HWiNFOProCLIController.write_json_file(config_path, config);

            args = { ...
                HWiNFOProCLIController.quote_arg(python_exe), ...
                HWiNFOProCLIController.quote_arg(script_path), ...
                command_name, ...
                '--config', HWiNFOProCLIController.quote_arg(config_path)};
            if nargin >= 2 && ~isempty(char(string(run_id)))
                args{end + 1} = '--run-id'; %#ok<AGROW>
                args{end + 1} = HWiNFOProCLIController.quote_arg(char(string(run_id))); %#ok<AGROW>
            end

            command_line = strjoin(args, ' ');
            [status, output_text] = system(command_line);
            response = HWiNFOProCLIController.decode_response(output_text, status, command_line);
        end

        function response = decode_response(output_text, status, command_line)
            response = struct( ...
                'ok', false, ...
                'status', 'python_runtime_failed', ...
                'message', '', ...
                'command_line', command_line, ...
                'exit_code', double(status));
            if nargin < 1
                return;
            end

            raw_text = strtrim(char(string(output_text)));
            if isempty(raw_text)
                response.message = sprintf('Python controller returned no output (exit code %d).', status);
                return;
            end

            try
                payload = jsondecode(raw_text);
                if isstruct(payload)
                    response = payload;
                else
                    response.message = raw_text;
                end
            catch
                response.message = raw_text;
            end

            if ~isfield(response, 'ok')
                response.ok = status == 0;
            end
            if ~isfield(response, 'exit_code')
                response.exit_code = double(status);
            end
            if ~isfield(response, 'command_line')
                response.command_line = command_line;
            end
            if ~isfield(response, 'status') || isempty(response.status)
                response.status = ternary(response.ok, 'ok', 'python_runtime_failed');
            end
            if ~isfield(response, 'message')
                response.message = raw_text;
            end
        end

        function python_exe = resolve_python_executable()
            python_exe = '';
            try
                pe = pyenv;
                if isprop(pe, 'Executable')
                    python_exe = char(string(pe.Executable));
                elseif isprop(pe, 'Version')
                    python_exe = char(string(pe.Version));
                end
            catch
                python_exe = '';
            end

            if ~isempty(strtrim(python_exe)) && exist(python_exe, 'file') == 2
                return;
            end

            fallback = 'C:\Python311\python.exe';
            if exist(fallback, 'file') == 2
                python_exe = fallback;
                return;
            end

            python_exe = 'python';
        end

        function write_json_file(target_path, payload)
            fid = fopen(target_path, 'w');
            if fid == -1
                error('HWiNFOProCLIController:ConfigWriteFailed', ...
                    'Could not open temporary Python config file: %s', target_path);
            end
            cleanup_obj = onCleanup(@() fclose(fid)); %#ok<NASGU>
            fprintf(fid, '%s', jsonencode(payload));
        end

        function safe_delete(target_path)
            if exist(target_path, 'file') == 2
                try
                    delete(target_path);
                catch
                end
            end
        end

        function quoted = quote_arg(value)
            text = char(string(value));
            text = strrep(text, '"', '\"');
            quoted = ['"', text, '"'];
        end
    end
end

function value = ternary(condition, true_value, false_value)
    if condition
        value = true_value;
    else
        value = false_value;
    end
end
