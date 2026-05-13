classdef HardwareMonitorBridge < handle
    % HardwareMonitorBridge - MATLAB/Python bridge for external collectors.

    properties (SetAccess = private)
        python_script_path
        py_runtime
        is_logging = false
        run_id = ''
        last_sample = struct()
        last_status = struct()
    end

    methods
        function obj = HardwareMonitorBridge(varargin)
            if nargin > 0
                obj.python_script_path = char(string(varargin{1}));
            else
                obj.python_script_path = fullfile(fileparts(mfilename('fullpath')), 'hardware_monitor.py');
            end
            if ~isfile(obj.python_script_path)
                error('HardwareMonitorBridge:MissingRuntime', ...
                    'hardware_monitor.py not found at: %s', obj.python_script_path);
            end

            obj.ensure_python_loaded();
            script_dir = fileparts(obj.python_script_path);
            if count(py.sys.path, script_dir) == 0
                insert(py.sys.path, int32(0), script_dir);
            end

            try
                mod = py.importlib.import_module('hardware_monitor');
                py.importlib.reload(mod);
            catch ME
                error('HardwareMonitorBridge:PythonImportFailed', ...
                    'Failed to import hardware_monitor module: %s', ME.message);
            end

            obj.py_runtime = py.hardware_monitor.CollectorRuntimeFacade(pyargs( ...
                'output_dir', '../../sensor_logs', 'interval', 0.5));
            obj.last_sample = struct();
            obj.last_status = struct('hwinfo', 'disabled', 'icue', 'disabled');
        end

        function response = start_live_session(obj, run_id, settings)
            if nargin < 2
                run_id = '';
            end
            if nargin < 3 || ~isstruct(settings)
                settings = struct();
            end
            obj.run_id = char(string(run_id));
            settings_json = char(jsonencode(obj.make_json_safe(settings)));
            try
                raw = obj.py_runtime.start_session_json(py.str(obj.run_id), py.str(settings_json));
            catch
                raw = obj.py_runtime.start_session_json(obj.run_id, settings_json);
            end
            response = obj.decode_json(raw);
            obj.is_logging = true;
        end

        function sample = poll_latest_sample(obj)
            if isempty(obj.py_runtime)
                sample = struct();
                return;
            end
            raw = obj.py_runtime.read_latest_json();
            sample = obj.decode_json(raw);
            obj.last_sample = sample;
            if isfield(sample, 'collector_status') && isstruct(sample.collector_status)
                obj.last_status = sample.collector_status;
            end
        end

        function status = get_source_status(obj)
            if isempty(obj.py_runtime)
                status = struct('hwinfo', 'disabled', 'icue', 'disabled');
                return;
            end
            raw = obj.py_runtime.source_status_json();
            status = obj.decode_json(raw);
            obj.last_status = status;
        end

        function coverage = describe_coverage(obj)
            if isempty(obj.py_runtime)
                coverage = struct();
                return;
            end
            raw = obj.py_runtime.describe_coverage_json();
            coverage = obj.decode_json(raw);
        end

        function summary = stop_live_session(obj)
            if isempty(obj.py_runtime)
                summary = struct();
                return;
            end
            raw = obj.py_runtime.stop_session_json();
            summary = obj.decode_json(raw);
            obj.is_logging = false;
        end

        % -----------------------------------------------------------------
        % Backward-compatible aliases for the legacy sustainability bridge.
        % -----------------------------------------------------------------
        function response = start_logging(obj, experiment_name)
            response = obj.start_live_session(experiment_name, struct());
        end

        function summary = stop_logging(obj)
            summary = obj.stop_live_session();
        end

        function stats = get_statistics(obj)
            stats = struct();
            if ~isstruct(obj.last_sample) || isempty(fieldnames(obj.last_sample))
                return;
            end
            metrics = obj.pick_struct_field(obj.last_sample, 'metrics');
            stats.cpu_proxy = obj.pick_numeric_field(metrics, 'cpu_proxy');
            stats.gpu_series = obj.pick_numeric_field(metrics, 'gpu_series');
            stats.cpu_temp_c = obj.pick_numeric_field(metrics, 'cpu_temp_c');
            stats.power_w = obj.pick_numeric_field(metrics, 'power_w');
            stats.memory_series = obj.pick_numeric_field(metrics, 'memory_series');
        end

        function report = generate_report(obj, output_file)
            report = struct();
            report.run_id = obj.run_id;
            report.generated_at_utc = char(datetime('now', 'TimeZone', 'UTC', ...
                'Format', 'yyyy-MM-dd''T''HH:mm:ss''Z'''));
            report.last_sample = obj.last_sample;
            report.collector_status = obj.last_status;
            if nargin > 1 && ~isempty(output_file)
                fid = fopen(output_file, 'w');
                if fid ~= -1
                    cleaner = onCleanup(@() fclose(fid));
                    fprintf(fid, '%s', jsonencode(report));
                    clear cleaner;
                end
            end
        end
    end

    methods (Access = private)
        function ensure_python_loaded(~)
            pe = pyenv;
            default_python = 'C:\Python311\python.exe';
            if pe.Status == "NotLoaded"
                if isfile(default_python)
                    try
                        pyenv('Version', default_python);
                    catch ME
                        error('HardwareMonitorBridge:PythonUnavailable', ...
                            'Python could not be loaded via %s: %s', default_python, ME.message);
                    end
                elseif strlength(string(pe.Version)) == 0
                    error('HardwareMonitorBridge:PythonUnavailable', ...
                        'Python is not loaded and the default interpreter was not found at %s.', ...
                        default_python);
                end
            end
        end

        function decoded = decode_json(~, value)
            try
                text = char(py.str(value));
            catch
                text = char(string(value));
            end
            if isempty(text)
                decoded = struct();
                return;
            end
            decoded = jsondecode(text);
        end

        function out = pick_struct_field(~, s, field_name)
            out = struct();
            if isstruct(s) && isfield(s, field_name) && isstruct(s.(field_name))
                out = s.(field_name);
            end
        end

        function value = pick_numeric_field(~, s, field_name)
            value = NaN;
            if isstruct(s) && isfield(s, field_name)
                candidate = s.(field_name);
                if isnumeric(candidate) && isscalar(candidate)
                    value = candidate;
                end
            end
        end

        function cleaned = make_json_safe(obj, value) %#ok<INUSD>
            if isa(value, 'function_handle') || isa(value, 'handle')
                cleaned = [];
                return;
            end
            if isa(value, 'datetime') || isa(value, 'duration') || isa(value, 'calendarDuration')
                cleaned = cellstr(string(value(:)));
                if isscalar(value)
                    cleaned = cleaned{1};
                end
                return;
            end
            if isa(value, 'categorical')
                cleaned = cellstr(string(value(:)));
                if isscalar(value)
                    cleaned = cleaned{1};
                end
                return;
            end
            if istable(value)
                try
                    if isa(value, 'timetable')
                        value = timetable2table(value, 'ConvertRowTimes', true);
                    end
                    cleaned = obj.make_json_safe(table2struct(value));
                catch
                    cleaned = sprintf('%dx%d table', size(value, 1), size(value, 2));
                end
                return;
            end
            if isa(value, 'containers.Map')
                cleaned = struct();
                try
                    keys = value.keys;
                    for i = 1:numel(keys)
                        key = matlab.lang.makeValidName(char(string(keys{i})));
                        cleaned.(key) = obj.make_json_safe(value(keys{i}));
                    end
                catch
                    cleaned = struct();
                end
                return;
            end
            if isstruct(value)
                cleaned = struct();
                names = fieldnames(value);
                for i = 1:numel(names)
                    key = names{i};
                    candidate = value.(key);
                    if isa(candidate, 'function_handle') || isa(candidate, 'handle')
                        continue;
                    end
                    cleaned.(key) = obj.make_json_safe(candidate);
                end
                return;
            end
            if iscell(value)
                cleaned = cell(size(value));
                for i = 1:numel(value)
                    cleaned{i} = obj.make_json_safe(value{i});
                end
                return;
            end
            if isa(value, 'string')
                cleaned = char(join(value(:).', '|'));
                return;
            end
            try
                jsonencode(value);
                cleaned = value;
            catch
                try
                    text_value = char(join(string(value(:).'), '|'));
                    if isempty(strtrim(text_value))
                        cleaned = [];
                    else
                        cleaned = text_value;
                    end
                catch
                    cleaned = [];
                end
            end
        end
    end
end
