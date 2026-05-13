classdef SystemProfileCollector
    % SystemProfileCollector - Gather machine metadata and collector status.

    methods (Static)
        function profile = collect(Settings)
            profile = struct();
            profile.timestamp_utc = char(datetime('now', 'TimeZone', 'UTC', 'Format', 'yyyy-MM-dd''T''HH:mm:ss''Z'''));
            profile.machine_id = SystemProfileCollector.resolve_machine_id(Settings);
            profile.machine_label = SystemProfileCollector.resolve_machine_label(Settings, profile.machine_id);
            profile.hostname = SystemProfileCollector.first_nonempty({getenv('COMPUTERNAME'), getenv('HOSTNAME')}, 'unknown_host');
            profile.os = SystemProfileCollector.resolve_os();
            profile.matlab_version = version;
            profile.matlab_release = char(version('-release'));
            profile.cpu_arch = computer('arch');
            profile.cpu_cores = feature('numcores');
            profile.ram_total_gb = SystemProfileCollector.get_total_ram_gb();

            collectors = SystemProfileCollector.resolve_collectors(Settings);
            profile.collectors = collectors;
            profile.source_quality = SystemProfileCollector.source_quality(collectors);
        end
    end

    methods (Static, Access = private)
        function machine_id = resolve_machine_id(Settings)
            requested = '';
            if isfield(Settings, 'sustainability') && isfield(Settings.sustainability, 'machine_id')
                requested = char(string(Settings.sustainability.machine_id));
            end
            if isempty(requested) || strcmpi(requested, 'auto')
                machine_id = SystemProfileCollector.first_nonempty( ...
                    {getenv('COMPUTERNAME'), getenv('HOSTNAME')}, ...
                    'unknown_machine');
            else
                machine_id = requested;
            end
        end

        function machine_label = resolve_machine_label(Settings, machine_id)
            machine_label = '';
            if isfield(Settings, 'sustainability') && isfield(Settings.sustainability, 'machine_label')
                machine_label = char(string(Settings.sustainability.machine_label));
            end
            if isempty(machine_label)
                machine_label = machine_id;
            end
        end

        function os_name = resolve_os()
            if ispc
                os_name = 'Windows';
            elseif ismac
                os_name = 'macOS';
            elseif isunix
                os_name = 'Linux';
            else
                os_name = 'Unknown';
            end
        end

        function ram_gb = get_total_ram_gb()
            ram_gb = NaN;
            if ispc
                try
                    mem = memory;
                    ram_gb = mem.MaxPossibleArrayBytes / 1024 / 1024 / 1024;
                catch ME
                    SystemProfileCollector.warn_once('SystemProfileCollector:RamProbeFailed', ...
                        'RAM probe failed; recording NaN in system profile: %s', ME.message);
                    ram_gb = NaN;
                end
            end
        end

        function collectors = resolve_collectors(Settings)
            collectors = struct();
            collectors.matlab = true;
            collectors.hwinfo = false;
            collectors.icue = false;
            collectors.hwinfo_source = '';
            collectors.icue_source = '';
            collectors.hwinfo_path = '';
            collectors.icue_path = '';

            if ~isfield(Settings, 'sustainability') || ~isstruct(Settings.sustainability)
                return;
            end

            collector_paths = struct('hwinfo', '', 'icue', '');
            if isfield(Settings.sustainability, 'collector_paths') && isstruct(Settings.sustainability.collector_paths)
                collector_paths = Settings.sustainability.collector_paths;
            end
            collector_runtime = struct();
            if isfield(Settings.sustainability, 'collector_runtime') && isstruct(Settings.sustainability.collector_runtime)
                collector_runtime = Settings.sustainability.collector_runtime;
            end

            if isfield(Settings.sustainability, 'external_collectors')
                flags = Settings.sustainability.external_collectors;
                if isfield(flags, 'hwinfo'), collectors.hwinfo = logical(flags.hwinfo); end
                if isfield(flags, 'icue'), collectors.icue = logical(flags.icue); end
            end

            if exist('ExternalCollectorAdapters', 'class') == 8 || exist('ExternalCollectorAdapters', 'file') == 2
                hwinfo = ExternalCollectorAdapters.extract_snapshot('hwinfo', collectors.hwinfo, collector_paths.hwinfo, collector_runtime);
                icue = ExternalCollectorAdapters.extract_snapshot('icue', collectors.icue, collector_paths.icue, collector_runtime);

                collectors.hwinfo = hwinfo.available;
                collectors.icue = icue.available;
                collectors.hwinfo_source = hwinfo.status;
                collectors.icue_source = icue.status;
                collectors.hwinfo_path = hwinfo.path;
                collectors.icue_path = icue.path;
                return;
            end

            [collectors.hwinfo, collectors.hwinfo_source] = ...
                SystemProfileCollector.check_collector_path(collector_paths, 'hwinfo', collectors.hwinfo);
            [collectors.icue, collectors.icue_source] = ...
                SystemProfileCollector.check_collector_path(collector_paths, 'icue', collectors.icue);
        end

        function [enabled, source_path] = check_collector_path(path_struct, field_name, default_enabled)
            enabled = default_enabled;
            source_path = '';
            if isfield(path_struct, field_name)
                candidate = char(string(path_struct.(field_name)));
                if ~isempty(candidate)
                    if exist(candidate, 'file')
                        enabled = true;
                        source_path = candidate;
                    else
                        enabled = false;
                    end
                end
            end
        end

        function quality = source_quality(collectors)
            if collectors.hwinfo || collectors.icue
                quality = 'enriched_external_collectors';
            else
                quality = 'baseline_matlab_only';
            end
        end

        function out = first_nonempty(candidates, fallback)
            out = fallback;
            for i = 1:numel(candidates)
                candidate = char(string(candidates{i}));
                if ~isempty(candidate)
                    out = candidate;
                    return;
                end
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
            SafeConsoleIO.warning(id, message, varargin{:});
        end
    end
end
