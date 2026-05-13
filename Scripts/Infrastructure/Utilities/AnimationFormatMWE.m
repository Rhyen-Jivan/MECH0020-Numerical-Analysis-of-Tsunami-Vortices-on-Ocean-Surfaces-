function summary = AnimationFormatMWE(varargin)
% AnimationFormatMWE - Minimal animation container benchmark (GIF/MP4/AVI).
%
% Purpose:
%   Provide a reproducible sanity check for animation export behavior and
%   file characteristics before selecting a project default format.
%
% Name-Value options:
%   'OutputDir'   - directory for generated files (default: Artifacts/tests/media_mwe/<timestamp>)
%   'Frames'      - number of frames to render (default: 90)
%   'Width'       - frame width in pixels (default: 960)
%   'Height'      - frame height in pixels (default: 540)
%   'FrameRate'   - target fps for video containers (default: 24)
%   'Quality'     - quality for MP4 writer (default: 95)
%   'Formats'     - cell array from {'mp4','avi','gif'} (default: all)
%
% Output:
%   summary struct with fields:
%     .output_dir
%     .frame_count
%     .frame_rate
%     .quality
%     .results (struct array per format)
%     .recommended_format
%     .summary_json_path
%
% Example:
%   out = AnimationFormatMWE('FrameRate', 30, 'Quality', 100);

    p = inputParser;
    addParameter(p, 'OutputDir', '', @(x) ischar(x) || isstring(x));
    addParameter(p, 'Frames', 90, @(x) isnumeric(x) && isscalar(x) && x >= 2);
    addParameter(p, 'Width', 960, @(x) isnumeric(x) && isscalar(x) && x >= 64);
    addParameter(p, 'Height', 540, @(x) isnumeric(x) && isscalar(x) && x >= 64);
    addParameter(p, 'FrameRate', 24, @(x) isnumeric(x) && isscalar(x) && x > 0);
    addParameter(p, 'Quality', 95, @(x) isnumeric(x) && isscalar(x) && x >= 0 && x <= 100);
    addParameter(p, 'Formats', {'mp4', 'avi', 'gif'}, @iscell);
    parse(p, varargin{:});

    frames = round(p.Results.Frames);
    width = round(p.Results.Width);
    height = round(p.Results.Height);
    frame_rate = p.Results.FrameRate;
    quality = round(p.Results.Quality);
    formats = normalize_formats(p.Results.Formats);

    if isempty(p.Results.OutputDir)
        repo_root = find_repo_root();
        timestamp = char(datetime('now', 'Format', 'yyyyMMdd_HHmmss'));
        output_dir = fullfile(repo_root, 'Artifacts', 'tests', 'media_mwe', timestamp);
    else
        output_dir = char(string(p.Results.OutputDir));
    end

    if ~exist(output_dir, 'dir')
        mkdir(output_dir);
    end

    frame_bank = build_synthetic_frame_bank(frames, width, height);
    results = repmat(struct( ...
        'format', '', ...
        'success', false, ...
        'file_path', '', ...
        'file_size_bytes', NaN, ...
        'elapsed_s', NaN, ...
        'effective_fps', NaN, ...
        'error_message', ''), 1, numel(formats));

    fprintf('[MEDIA-MWE] Output directory: %s\n', output_dir);
    for idx = 1:numel(formats)
        fmt = formats{idx};
        fprintf('[MEDIA-MWE] Testing format: %s\n', upper(fmt));
        result = export_format(fmt, frame_bank, output_dir, frame_rate, quality);
        results(idx) = result;
        if result.success
            fprintf('[MEDIA-MWE]   OK  | %.2f s | %.2f fps | %.2f MB\n', ...
                result.elapsed_s, result.effective_fps, result.file_size_bytes / 1024 / 1024);
        else
            fprintf('[MEDIA-MWE]   FAIL | %s\n', result.error_message);
        end
    end

    recommended = pick_recommended_format(results);

    summary = struct();
    summary.output_dir = output_dir;
    summary.frame_count = frames;
    summary.frame_rate = frame_rate;
    summary.quality = quality;
    summary.results = results;
    summary.recommended_format = recommended;

    summary_json_path = fullfile(output_dir, 'animation_format_summary.json');
    write_json(summary_json_path, summary);
    summary.summary_json_path = summary_json_path;

    if isempty(recommended)
        fprintf('[MEDIA-MWE] No successful format detected.\n');
    else
        fprintf('[MEDIA-MWE] Recommended format: %s\n', upper(recommended));
    end
end

function formats = normalize_formats(raw_formats)
    supported = {'mp4', 'avi', 'gif'};
    formats = {};
    for i = 1:numel(raw_formats)
        token = lower(char(string(raw_formats{i})));
        if ismember(token, supported) && ~ismember(token, formats)
            formats{end + 1} = token; %#ok<AGROW>
        end
    end
    if isempty(formats)
        formats = supported;
    end
end

function frame_bank = build_synthetic_frame_bank(frame_count, width, height)
    % Create a deterministic synthetic vorticity-like sequence.
    [X, Y] = meshgrid(linspace(-1, 1, width), linspace(-1, 1, height));
    frame_bank = cell(1, frame_count);

    for k = 1:frame_count
        phase = 2 * pi * (k - 1) / max(1, frame_count - 1);
        field = exp(-3 * (X.^2 + Y.^2)) .* sin(8 * X + phase) .* cos(6 * Y - 0.5 * phase);
        img = uint8(255 * mat2gray(field));
        rgb = cat(3, img, uint8(255 - img), uint8(0.5 * img));
        frame_bank{k} = rgb;
    end
end

function result = export_format(fmt, frame_bank, output_dir, fps, quality)
    result = struct( ...
        'format', fmt, ...
        'success', false, ...
        'file_path', '', ...
        'file_size_bytes', NaN, ...
        'elapsed_s', NaN, ...
        'effective_fps', NaN, ...
        'error_message', '');

    frame_count = numel(frame_bank);
    base_name = sprintf('animation_mwe_%s', fmt);
    timer_start = tic;

    try
        switch fmt
            case 'mp4'
                result.file_path = fullfile(output_dir, [base_name, '.mp4']);
                writer = VideoWriter(result.file_path, 'MPEG-4');
                writer.FrameRate = fps;
                writer.Quality = quality;
                open(writer);
                for i = 1:frame_count
                    writeVideo(writer, frame_bank{i});
                end
                close(writer);

            case 'avi'
                result.file_path = fullfile(output_dir, [base_name, '.avi']);
                writer = VideoWriter(result.file_path, 'Motion JPEG AVI');
                writer.FrameRate = fps;
                open(writer);
                for i = 1:frame_count
                    writeVideo(writer, frame_bank{i});
                end
                close(writer);

            case 'gif'
                result.file_path = fullfile(output_dir, [base_name, '.gif']);
                delay_time = 1 / fps;
                for i = 1:frame_count
                    [indexed, cmap] = rgb2ind(frame_bank{i}, 256);
                    if i == 1
                        imwrite(indexed, cmap, result.file_path, 'gif', 'LoopCount', inf, 'DelayTime', delay_time);
                    else
                        imwrite(indexed, cmap, result.file_path, 'gif', 'WriteMode', 'append', 'DelayTime', delay_time);
                    end
                end
        end

        result.elapsed_s = toc(timer_start);
        if result.elapsed_s > 0
            result.effective_fps = frame_count / result.elapsed_s;
        else
            result.effective_fps = NaN;
        end

        info = dir(result.file_path);
        if ~isempty(info)
            result.file_size_bytes = info.bytes;
        end
        result.success = true;

    catch ME
        result.success = false;
        result.elapsed_s = toc(timer_start);
        result.error_message = ME.message;
    end
end

function recommended = pick_recommended_format(results)
    recommended = '';
    preferred_order = {'mp4', 'avi', 'gif'};
    for i = 1:numel(preferred_order)
        fmt = preferred_order{i};
        match = find(strcmp({results.format}, fmt), 1, 'first');
        if ~isempty(match) && results(match).success
            recommended = fmt;
            return;
        end
    end
end

function write_json(path_str, payload)
    encoded = jsonencode(payload);
    fid = fopen(path_str, 'w');
    if fid == -1
        error('AnimationFormatMWE:WriteFailed', 'Could not write JSON summary: %s', path_str);
    end
    fprintf(fid, '%s', encoded);
    fclose(fid);
end

function repo_root = find_repo_root()
    if exist('PathBuilder', 'class') == 8 || exist('PathBuilder', 'file') == 2
        repo_root = PathBuilder.get_repo_root();
        return;
    end

    current = pwd;
    while true
        if exist(fullfile(current, '.git'), 'dir')
            repo_root = current;
            return;
        end
        parent = fileparts(current);
        if strcmp(parent, current)
            repo_root = pwd;
            return;
        end
        current = parent;
    end
end
