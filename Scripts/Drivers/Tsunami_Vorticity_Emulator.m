function Tsunami_Vorticity_Emulator(varargin)
% Tsunami_Vorticity_Emulator - Compatibility launcher for UI and standard startup.
%
% Canonical user-facing runtime:Also 
%   UIController -> execute_single_run -> Build_Run_Config ->
%   ParallelSimulationExecutor -> RunDispatcher -> workflow or ModeDispatcher
%
% Compatibility flow in this launcher:
%   1) Attach repository paths
%   2) Select mode: UI, Standard, or Interactive startup dialog 
%   3) Load defaults from create_default_parameters.m
%   4) Build Run_Config for standard compatibility runs
%   5) Dispatch through ModeDispatcher
%
%
% Canonical defaults and runtime policy:
%   - Scripts/Infrastructure/Initialisers/create_default_parameters.m
%   - Scripts/Infrastructure/Initialisers/Settings.m
%
% Usage examples:
%   Tsunami_Vorticity_Emulator()
%   Tsunami_Vorticity_Emulator('Mode', 'UI')
%   Tsunami_Vorticity_Emulator('Mode', 'Standard', 'Method', 'FD', 'SimMode', 'Evolution')

    opts = parse_options(varargin{:});
    repo_root = setup_paths();
    SafeConsoleIO.reset_stream_failure();
    apply_dark_plot_defaults();
    Directory_Check(repo_root, 'Verbose', true);

    switch lower(opts.Mode)
        case 'ui'
            run_ui_mode();
        case 'standard'
            warn_deprecated_non_ui_mode('Standard');
            run_standard_mode(opts);
        case 'interactive'
            warn_deprecated_non_ui_mode('Interactive');
            run_interactive_mode(opts);
        otherwise
            error('TVE:InvalidMode', ...
                'Unknown mode ''%s''. Valid: UI, Standard, Interactive.', opts.Mode);
    end
end

function apply_dark_plot_defaults()
% apply_dark_plot_defaults - Global dark-mode defaults for all MATLAB figures.
    set(groot, ...
        'defaultFigureColor', [0.09 0.10 0.13], ...
        'defaultAxesColor', [0.09 0.10 0.13], ...
        'defaultAxesXColor', [0.90 0.92 0.95], ...
        'defaultAxesYColor', [0.90 0.92 0.95], ...
        'defaultAxesZColor', [0.90 0.92 0.95], ...
        'defaultAxesGridColor', [0.40 0.43 0.47], ...
        'defaultTextColor', [0.90 0.92 0.95], ...
        'defaultLegendTextColor', [0.90 0.92 0.95], ...
        'defaultLegendColor', [0.12 0.13 0.16], ...
        'defaultLegendEdgeColor', [0.40 0.43 0.47]);
end


function opts = parse_options(varargin)
    p = inputParser;
    addParameter(p, 'Mode', 'Interactive', @ischar);
    addParameter(p, 'Method', 'FD', @ischar);
    addParameter(p, 'SimMode', 'Evolution', @ischar);
    addParameter(p, 'IC', '', @ischar);
    addParameter(p, 'Nx', 0, @isnumeric);
    addParameter(p, 'Ny', 0, @isnumeric);
    addParameter(p, 'dt', 0, @isnumeric);
    addParameter(p, 'Tfinal', 0, @isnumeric);
    addParameter(p, 'nu', 0, @isnumeric);
    addParameter(p, 'SaveFigs', -1, @isnumeric);
    addParameter(p, 'SaveData', -1, @isnumeric);
    addParameter(p, 'Monitor', -1, @isnumeric);
    addParameter(p, 'NoPrompt', false, @islogical);
    parse(p, varargin{:});
    opts = p.Results;
end

function repo_root = setup_paths()
    script_dir = fileparts(mfilename('fullpath'));
    repo_root = fileparts(fileparts(script_dir));
    scripts_dir = fullfile(repo_root, 'Scripts');
    infra_dir = fullfile(scripts_dir, 'Infrastructure');
    if exist(infra_dir, 'dir') == 7
        addpath(infra_dir); % Bootstrap PathSetup visibility without broad recursive pathing
    end
    repo_root = PathSetup.attach_and_verify();
end

function run_interactive_mode(opts)
    ColorPrintf.header('TSUNAMI VORTICITY EMULATOR');
    refresh_ui_class_definitions();
    app = instantiate_ui_controller_quietly(); %#ok<NASGU>

    if isappdata(0, 'ui_mode') && strcmp(getappdata(0, 'ui_mode'), 'traditional')
        rmappdata(0, 'ui_mode');
        ColorPrintf.info('Startup dialog selected Standard mode.');
        run_standard_mode(opts);
    else
        ColorPrintf.info('UI mode selected. Continue inside the UI.');
    end
end

function run_ui_mode()
    ColorPrintf.header('TSUNAMI VORTICITY EMULATOR - UI MODE');
    refresh_ui_class_definitions();
    app = instantiate_ui_controller_quietly(); %#ok<NASGU>
    ColorPrintf.success('UI launched.');
end

function app = instantiate_ui_controller_quietly()
    app = [];
    startup_output = evalc('app = UIController();');
    emit_filtered_ui_startup_output(startup_output);
end

function emit_filtered_ui_startup_output(startup_output)
    if nargin < 1 || strlength(string(startup_output)) == 0
        return;
    end

    lines = splitlines(string(startup_output));
    noise_patterns = ["useCS", "ans =", "logical", "HandleVisibility"];
    for i = 1:numel(lines)
        line_text = strtrim(char(lines(i)));
        if isempty(line_text)
            continue;
        end
        if any(contains(line_text, noise_patterns))
            continue;
        end
        if ~isempty(regexp(line_text, '^(true|false|0|1)$', 'once'))
            continue;
        end
        fprintf('%s\n', line_text);
    end
end

function refresh_ui_class_definitions()
% refresh_ui_class_definitions - Ensure edited classdef files reload on UI launch.
%
% During active development MATLAB can hold a stale UIController class
% definition in memory even after the file on disk has changed. Clearing
% classes here forces UI launches to use the current repository version.
% Skip the destructive refresh if a live UI/app object is already in memory,
% otherwise MATLAB emits noisy "Cannot clear this class" warnings.
    if ui_refresh_blocked_by_live_objects()
        rehash;
        return;
    end
    evalc('clear classes'); %#ok<EVLC>
    rehash;
end

function tf = ui_refresh_blocked_by_live_objects()
    tf = false;

    try
        base_vars = evalin('base', 'whos');
        if isstruct(base_vars) && ~isempty(base_vars)
            tf = any(strcmp({base_vars.class}, 'UIController'));
            if tf
                return;
            end
        end
    catch
    end

    try
        figs = findall(groot, 'Type', 'figure');
        for i = 1:numel(figs)
            if ~isvalid(figs(i)) || ~isprop(figs(i), 'Name')
                continue;
            end
            fig_name = char(string(figs(i).Name));
            if contains(fig_name, 'Tsunami Vorticity', 'IgnoreCase', true)
                tf = true;
                return;
            end
        end
    catch
    end
end

function warn_deprecated_non_ui_mode(mode_name)
    warning('TVE:DeprecatedMode', ...
        ['Mode ''%s'' is compatibility-only and is scheduled for removal. ' ...
         'Use UI mode as the canonical runtime path.'], mode_name);
end

function run_standard_mode(opts)
    ColorPrintf.header('TSUNAMI VORTICITY EMULATOR - STANDARD MODE');

    params = create_default_parameters();
    settings = Settings();
    [params, settings] = ensure_time_sampling(params, settings);

    show_standard_mode_summary(params, settings);

    has_overrides = opts.Nx > 0 || opts.Ny > 0 || opts.dt > 0 || opts.Tfinal > 0 || ...
        opts.nu > 0 || opts.SaveFigs >= 0 || opts.SaveData >= 0 || opts.Monitor >= 0 || ...
        ~isempty(opts.IC) || ~strcmpi(opts.Method, 'FD') || ~strcmpi(opts.SimMode, 'Evolution');

    prompt_enabled = usejava('desktop') && ~opts.NoPrompt && ~has_overrides;
    if prompt_enabled
        [params, settings, user_abort] = confirm_or_adjust_parameters(params, settings);
        if user_abort
            ColorPrintf.warn('Run cancelled.');
            return;
        end
    end

    [params, settings] = apply_runtime_overrides(params, settings, opts);
    [params, settings] = ensure_time_sampling(params, settings);

    if isempty(opts.IC)
        ic_type = params.ic_type;
    else
        ic_type = opts.IC;
    end

    run_config = Build_Run_Config(opts.Method, opts.SimMode, ic_type);
    print_run_configuration(run_config, params, settings);

    try
        [results, paths] = ModeDispatcher(run_config, params, settings);
        print_run_results(results, paths);
    catch ME
        ErrorHandler.log('ERROR', 'RUN-EXEC-0003', ...
            'message', sprintf('Simulation failed: %s', ME.message), ...
            'file', mfilename, ...
            'context', struct('error_id', ME.identifier));
        rethrow(ME);
    end
end

function [params, settings, user_abort] = confirm_or_adjust_parameters(params, settings)
    user_abort = false;

    fprintf('Review defaults from:\n');
    fprintf('  - Scripts/Infrastructure/Initialisers/create_default_parameters.m\n');
    fprintf('  - Scripts/Infrastructure/Initialisers/Settings.m\n\n');

    response = input('Are these parameters correct? [Y/n]: ', 's');
    if isempty(response) || strcmpi(response, 'y')
        return;
    end

    fprintf('\nChoose an option:\n');
    fprintf('  1) Reload create_default_parameters defaults\n');
    fprintf('  2) Abort\n');

    choice = input('Selection [1]: ', 's');
    if isempty(choice)
        choice = '1';
    end

    switch choice
        case '1'
            fallback = create_default_parameters();
            params = overlay_struct(params, fallback);
            if isfield(fallback, 'create_animations')
                settings.animation_enabled = logical(fallback.create_animations);
            end
            if isfield(fallback, 'animation_fps')
                settings.animation_frame_rate = fallback.animation_fps;
            end
            fprintf('\nLoaded defaults from create_default_parameters.m\n\n');

        otherwise
            user_abort = true;
    end
end

function [params, settings] = apply_runtime_overrides(params, settings, opts)
    if opts.Nx > 0, params.Nx = opts.Nx; end
    if opts.Ny > 0, params.Ny = opts.Ny; end
    if opts.dt > 0, params.dt = opts.dt; end
    if opts.Tfinal > 0, params.Tfinal = opts.Tfinal; end
    if opts.nu > 0, params.nu = opts.nu; end
    if ~isempty(opts.IC), params.ic_type = opts.IC; end

    if opts.SaveFigs >= 0, settings.save_figures = logical(opts.SaveFigs); end
    if opts.SaveData >= 0, settings.save_data = logical(opts.SaveData); end
    if opts.Monitor >= 0, settings.monitor_enabled = logical(opts.Monitor); end
end

function [params, settings] = ensure_time_sampling(params, settings)
    % Normalize media aliases <-> canonical structs before sampling math.
    if isfield(settings, 'media') && isstruct(settings.media)
        if isfield(settings.media, 'fps') && settings.media.fps > 0
            settings.animation_frame_rate = settings.media.fps;
        end
        if isfield(settings.media, 'duration_s') && settings.media.duration_s > 0
            settings.animation_duration_s = settings.media.duration_s;
        end
        if isfield(settings.media, 'frame_count') && settings.media.frame_count >= 2
            settings.animation_frame_count = settings.media.frame_count;
        end
        if isfield(settings.media, 'format') && ~isempty(settings.media.format)
            settings.animation_format = char(string(settings.media.format));
        end
    end

    if isfield(params, 'media') && isstruct(params.media)
        if isfield(params.media, 'num_frames') && params.media.num_frames >= 2
            params.num_animation_frames = params.media.num_frames;
        end
        if isfield(params.media, 'duration_s') && params.media.duration_s > 0
            params.animation_duration_s = params.media.duration_s;
        end
        if isfield(params.media, 'fps') && params.media.fps > 0
            settings.animation_frame_rate = params.media.fps;
        end
        if isfield(params.media, 'format') && ~isempty(params.media.format)
            params.animation_format = char(string(params.media.format));
        end
    end

    if ~isfield(params, 'num_plot_snapshots') || params.num_plot_snapshots < 1
        if isfield(params, 'num_snapshots') && params.num_snapshots > 0
            params.num_plot_snapshots = params.num_snapshots;
        else
            params.num_plot_snapshots = 9;
        end
    end

    if ~isfield(settings, 'animation_frame_rate') || settings.animation_frame_rate <= 0
        settings.animation_frame_rate = 24;
    end

    if ~isfield(settings, 'animation_duration_s') || settings.animation_duration_s <= 0
        if isfield(params, 'animation_duration_s') && params.animation_duration_s > 0
            settings.animation_duration_s = params.animation_duration_s;
        else
            settings.animation_duration_s = max(double(params.Tfinal), 0.1);
        end
    end
    params.animation_duration_s = settings.animation_duration_s;

    explicit_animation_frames = NaN;
    if isfield(params, 'animation_num_frames') && isnumeric(params.animation_num_frames) && ...
            isscalar(params.animation_num_frames) && isfinite(params.animation_num_frames)
        explicit_animation_frames = double(params.animation_num_frames);
    elseif isfield(params, 'num_animation_frames') && isnumeric(params.num_animation_frames) && ...
            isscalar(params.num_animation_frames) && isfinite(params.num_animation_frames)
        explicit_animation_frames = double(params.num_animation_frames);
    elseif isfield(settings, 'animation_frame_count') && isnumeric(settings.animation_frame_count) && ...
            isscalar(settings.animation_frame_count) && isfinite(settings.animation_frame_count)
        explicit_animation_frames = double(settings.animation_frame_count);
    elseif isfield(settings, 'media') && isstruct(settings.media) && ...
            isfield(settings.media, 'frame_count') && isnumeric(settings.media.frame_count) && ...
            isscalar(settings.media.frame_count) && isfinite(settings.media.frame_count)
        explicit_animation_frames = double(settings.media.frame_count);
    end

    if ~isfield(settings, 'animation_gif_min_frames') || settings.animation_gif_min_frames < 2
        if isfield(params, 'animation_gif_min_frames') && params.animation_gif_min_frames >= 2
            settings.animation_gif_min_frames = params.animation_gif_min_frames;
        else
            settings.animation_gif_min_frames = 2;
        end
    end

    if isfinite(explicit_animation_frames)
        params.num_animation_frames = max(2, round(explicit_animation_frames));
    else
        params.num_animation_frames = max(2, round(settings.animation_duration_s * settings.animation_frame_rate));
    end
    settings.animation_gif_min_frames = min(max(2, round(double(settings.animation_gif_min_frames))), ...
        params.num_animation_frames);
    params.animation_gif_min_frames = settings.animation_gif_min_frames;

    settings.animation_frame_count = params.num_animation_frames;
    settings.animation_format = 'mp4+gif';
    settings.animation_export_format = 'mp4+gif';
    settings.animation_export_formats = {'mp4', 'gif'};
    params.animation_format = 'mp4+gif';
    params.animation_export_format = 'mp4+gif';
    params.animation_export_formats = {'mp4', 'gif'};

    params.plot_snap_times = linspace(0, params.Tfinal, params.num_plot_snapshots);
    params.animation_times = linspace(0, params.Tfinal, params.num_animation_frames);
    params.snap_times = params.plot_snap_times;
    params.num_snapshots = params.num_plot_snapshots;

    % Keep canonical structs synced for downstream scripts.
    if ~isfield(settings, 'media') || ~isstruct(settings.media)
        settings.media = struct();
    end
    settings.media.fps = settings.animation_frame_rate;
    settings.media.frame_count = settings.animation_frame_count;
    settings.media.duration_s = settings.animation_duration_s;
    settings.media.format = 'mp4+gif';
    settings.media.formats = {'mp4', 'gif'};
    settings.media.gif_min_frame_count = settings.animation_gif_min_frames;
    settings.media.quality = settings.animation_quality;
    settings.media.codec = settings.animation_codec;

    if ~isfield(params, 'media') || ~isstruct(params.media)
        params.media = struct();
    end
    params.media.fps = settings.animation_frame_rate;
    params.media.num_frames = params.num_animation_frames;
    params.media.duration_s = settings.animation_duration_s;
    params.media.format = 'mp4+gif';
    params.media.formats = {'mp4', 'gif'};
    params.media.gif_min_frame_count = min(params.animation_gif_min_frames, params.num_animation_frames);
    params.media.quality = params.animation_quality;
    params.media.codec = params.animation_codec;

    % Keep sustainability collector path hints aligned between parameters
    % (editable policy) and settings (runtime policy used by ledger hooks).
    if isfield(params, 'sustainability') && isstruct(params.sustainability)
        if ~isfield(settings, 'sustainability') || ~isstruct(settings.sustainability)
            settings.sustainability = struct();
        end
        if isfield(params.sustainability, 'collector_paths')
            settings.sustainability.collector_paths = params.sustainability.collector_paths;
        end
    end
end

function show_standard_mode_summary(params, settings)
    fprintf('------------------------------------------------------------\n');
    fprintf('Default Parameter Snapshot\n');
    fprintf('------------------------------------------------------------\n');
    fprintf('Method defaults:           %s\n', char(string(params.default_method)));
    fprintf('Mode default:              %s\n', char(string(params.default_mode)));
    fprintf('Grid (Nx x Ny):            %d x %d\n', params.Nx, params.Ny);
    fprintf('Domain (Lx x Ly):          %.3f x %.3f\n', params.Lx, params.Ly);
    fprintf('Time (dt, Tfinal):         %.6f, %.3f\n', params.dt, params.Tfinal);
    fprintf('Viscosity nu:              %.6e\n', params.nu);
    fprintf('IC type:                   %s\n', char(string(params.ic_type)));
    fprintf('9-tile snapshots:          %d (separate from animation)\n', params.num_plot_snapshots);
    fprintf('Animation frame rate:      %.2f fps\n', settings.animation_frame_rate);
    fprintf('Animation frame count:     %d\n', params.num_animation_frames);
    fprintf('Save figures/data/reports: %d / %d / %d\n', ...
        settings.save_figures, settings.save_data, settings.save_reports);
    fprintf('------------------------------------------------------------\n\n');
end

function print_run_configuration(run_config, params, settings)
    ColorPrintf.section('RUN CONFIGURATION');
    fprintf('Method:          %s\n', run_config.method);
    fprintf('Mode:            %s\n', run_config.mode);
    fprintf('IC:              %s\n', run_config.ic_type);
    fprintf('Grid:            %d x %d\n', params.Nx, params.Ny);
    fprintf('dt / Tfinal:     %.6f / %.3f\n', params.dt, params.Tfinal);
    fprintf('Plot snapshots:  %d\n', params.num_plot_snapshots);
    fprintf('Animation fps:   %.2f\n', settings.animation_frame_rate);
    fprintf('\n');
end

function print_run_results(results, paths)
    ColorPrintf.header('SIMULATION COMPLETE');
    if isfield(results, 'run_id')
        fprintf('Run ID:       %s\n', results.run_id);
    end
    if isfield(results, 'wall_time')
        fprintf('Wall Time:    %.2f s\n', results.wall_time);
    end
    if isfield(results, 'max_omega')
        fprintf('Max |omega|:  %.6e\n', results.max_omega);
    end
    if isfield(paths, 'base')
        fprintf('Output Dir:   %s\n', paths.base);
    end
    fprintf('\n');
end

function out = overlay_struct(base, patch)
    out = base;
    fields = fieldnames(patch);
    for i = 1:numel(fields)
        out.(fields{i}) = patch.(fields{i});
    end
end
