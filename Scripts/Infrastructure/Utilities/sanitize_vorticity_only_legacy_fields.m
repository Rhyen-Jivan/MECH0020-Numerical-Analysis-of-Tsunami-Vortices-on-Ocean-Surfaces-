function cfg = sanitize_vorticity_only_legacy_fields(cfg, source_label, error_namespace)
% sanitize_vorticity_only_legacy_fields Enforce the vorticity-only runtime contract.
%
% Legacy wave-runtime fields are accepted only when they do not request a
% non-vorticity solver. Accepted stale fields are stripped from the active
% runtime payload so downstream config exports stay vorticity-only.

    if ~isstruct(cfg)
        return;
    end

    if nargin < 2 || strlength(string(source_label)) == 0
        source_label = 'runtime';
    end
    if nargin < 3 || strlength(string(error_namespace)) == 0
        error_namespace = 'VorticityOnlyRuntime';
    end

    if isfield(cfg, 'wave_model') && ~isempty(cfg.wave_model)
        wave_model = lower(char(string(cfg.wave_model)));
        if strcmp(wave_model, 'none')
            wave_model = 'vorticity';
        elseif strcmp(wave_model, 'nswe')
            wave_model = 'nswe_2d';
        end
        if ~strcmp(wave_model, 'vorticity')
            error_id = sprintf('%s:LegacyWaveModelUnsupported', char(string(error_namespace)));
            error(error_id, ...
                'Configuration from %s requested legacy wave model "%s". The runtime is vorticity-streamfunction only.', ...
                char(string(source_label)), wave_model);
        end
    end

    if isfield(cfg, 'wave_solver_level') && ~isempty(cfg.wave_solver_level)
        wave_solver_level = lower(char(string(cfg.wave_solver_level)));
        if ~strcmp(wave_solver_level, 'none')
            error_id = sprintf('%s:LegacyWaveSolverLevelUnsupported', char(string(error_namespace)));
            error(error_id, ...
                'Configuration from %s requested legacy wave_solver_level "%s". The runtime is vorticity-streamfunction only.', ...
                char(string(source_label)), wave_solver_level);
        end
    end

    legacy_fields = {'wave_model', 'wave_solver_level', 'nswe_initial_condition'};
    for i = 1:numel(legacy_fields)
        field_name = legacy_fields{i};
        if isfield(cfg, field_name)
            cfg = rmfield(cfg, field_name);
        end
    end
end
