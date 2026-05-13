classdef RunIDGenerator
    % RunIDGenerator - Generate and parse canonical run identifiers.
    %
    % Canonical format:
    %   YYYY-MM-DD__HH-mm-ss__M-{method}__MO-{mode}__BC-{case}__
    %   SIDES-T{top}-B{bottom}-L{left}-R{right}__R-{4char}
    %
    % Examples:
    %   2026-02-20__16-41-09__M-FD__MO-EVOLUTION__BC-LID-DRIVEN-CAVITY__
    %   SIDES-TDRV-BNOS-LNOS-RNOS__R-7XQ2

    methods (Static)
        function run_id = generate(Run_Config, Parameters)
            % Generate canonical, human-readable run identifier.
            if nargin < 1 || ~isstruct(Run_Config)
                Run_Config = struct();
            end
            if nargin < 2 || ~isstruct(Parameters)
                Parameters = struct();
            end

            stamp = datetime('now');
            date_part = char(string(stamp, 'yyyy-MM-dd'));
            time_part = char(string(stamp, 'HH-mm-ss'));

            method = RunIDGenerator.pick_text(Run_Config, Parameters, {'method'}, 'UNK');
            mode = RunIDGenerator.pick_text(Run_Config, Parameters, {'mode', 'run_mode_internal'}, 'UNK');
            bc_case = RunIDGenerator.pick_text(Parameters, Run_Config, ...
                {'bc_case', 'boundary_condition_case'}, 'UNKNOWN');

            [bc_top, bc_bottom, bc_left, bc_right] = RunIDGenerator.pick_bc_sides(Parameters, Run_Config);

            % Keep tokens compact so Windows paths remain under MAT/HDF5 limits.
            method_tok = RunIDGenerator.limit_token(RunIDGenerator.sanitize_token(method), 8);
            mode_tok = RunIDGenerator.limit_token(RunIDGenerator.sanitize_token(mode), 12);
            bc_case_tok = RunIDGenerator.limit_token(RunIDGenerator.sanitize_token(bc_case), 10);
            top_tok = RunIDGenerator.side_token(bc_top);
            bottom_tok = RunIDGenerator.side_token(bc_bottom);
            left_tok = RunIDGenerator.side_token(bc_left);
            right_tok = RunIDGenerator.side_token(bc_right);
            random_tok = RunIDGenerator.random_suffix(4);

            run_id = sprintf('%s__%s__M-%s__MO-%s__BC-%s__SIDES-T%s-B%s-L%s-R%s__R-%s', ...
                date_part, time_part, method_tok, mode_tok, bc_case_tok, ...
                top_tok, bottom_tok, left_tok, right_tok, random_tok);
        end

        function storage_id = make_storage_id(run_id)
            % Create a compact deterministic disk identifier for long run IDs.
            token = strtrim(char(string(run_id)));
            if isempty(token)
                storage_id = 'r_run_0000';
                return;
            end

            maybe_canonical = ~isempty(regexp(token, ...
                '^\d{4}-\d{2}-\d{2}__\d{2}-\d{2}-\d{2}__M-', 'once'));
            if maybe_canonical
                info = RunIDGenerator.parse(token);
                if isstruct(info) && isfield(info, 'format') && strcmpi(char(string(info.format)), 'canonical_v2')
                    date_token = regexprep(char(string(info.date)), '-', '');
                    time_token = regexprep(char(string(info.time)), '-', '');
                    if numel(date_token) >= 8
                        date_token = date_token(3:8);
                    end
                    random_token = lower(char(string(info.random)));
                    storage_id = sprintf('r_%s%s_%s', date_token, time_token, random_token);
                    return;
                end
            end

            disk_token = lower(regexprep(token, '[^a-zA-Z0-9]+', '_'));
            disk_token = regexprep(disk_token, '_{2,}', '_');
            disk_token = regexprep(disk_token, '^_|_$', '');
            if isempty(disk_token)
                disk_token = 'run';
            end
            disk_token = RunIDGenerator.limit_token(disk_token, 12);
            checksum = RunIDGenerator.base36_checksum(token, 4);
            storage_id = sprintf('r_%s_%s', disk_token, checksum);
        end

        function info = parse(run_id)
            % Parse canonical or legacy run-id formats.
            info = struct();
            if nargin < 1 || isempty(run_id)
                return;
            end
            token = char(string(run_id));

            canonical_expr = ['^(?<date>\d{4}-\d{2}-\d{2})__(?<time>\d{2}-\d{2}-\d{2})' ...
                '__M-(?<method>[^_]+)__MO-(?<mode>[^_]+)__BC-(?<bc_case>[^_]+)' ...
                '__SIDES-T(?<bc_top>[^-]+)-B(?<bc_bottom>[^-]+)-L(?<bc_left>[^-]+)-R(?<bc_right>[^_]+)' ...
                '__R-(?<random>[A-Z0-9]{4})$'];
            named = regexp(token, canonical_expr, 'names', 'once');
            if ~isempty(named)
                info.format = 'canonical_v2';
                info.date = named.date;
                info.time = named.time;
                info.timestamp = [named.date ' ' strrep(named.time, '-', ':')];
                info.method = named.method;
                info.mode = named.mode;
                info.bc_case = named.bc_case;
                info.bc_top = named.bc_top;
                info.bc_bottom = named.bc_bottom;
                info.bc_left = named.bc_left;
                info.bc_right = named.bc_right;
                info.random = named.random;
                return;
            end

            % Legacy fallback parser.
            parts = strsplit(token, '_');
            if numel(parts) >= 3
                info.format = 'legacy';
                info.timestamp = parts{1};
                info.method = parts{2};
                info.mode = parts{3};
                if numel(parts) >= 4
                    info.ic_type = parts{4};
                end
                if numel(parts) >= 5
                    info.grid_str = parts{5};
                end
                if numel(parts) >= 6
                    info.dt_str = parts{6};
                end
                return;
            end

            warning('RunIDGenerator:InvalidFormat', 'Could not parse run_id: %s', token);
        end

        function filename = make_figure_filename(run_id, figure_type, variant)
            % Create standardized figure filename.
            if nargin < 3
                variant = '';
            end

            if isempty(variant)
                filename = sprintf('%s__%s.png', run_id, figure_type);
            else
                filename = sprintf('%s__%s__%s.png', run_id, figure_type, variant);
            end
        end

        function run_id = extract_from_filename(filename)
            % Extract run_id from figure filename.
            [~, name, ~] = fileparts(filename);
            run_id = '';

            % Canonical v2 includes "__R-XXXX" token; keep everything before
            % figure suffix separators when present.
            tok = regexp(name, '^(?<rid>.+__R-[A-Z0-9]{4})(?:__.*)?$', 'names', 'once');
            if ~isempty(tok) && isfield(tok, 'rid')
                run_id = tok.rid;
                return;
            end

            % Legacy fallback: first underscore-separated token.
            parts = strsplit(name, '_');
            if ~isempty(parts)
                run_id = parts{1};
            end
        end
    end

    methods (Static, Access = private)
        function out = pick_text(primary, secondary, keys, fallback)
            out = fallback;
            src = {primary, secondary};
            for si = 1:numel(src)
                s = src{si};
                if ~isstruct(s)
                    continue;
                end
                for ki = 1:numel(keys)
                    key = keys{ki};
                    if isfield(s, key) && ~isempty(s.(key))
                        out = char(string(s.(key)));
                        return;
                    end
                end
            end
        end

        function [top, bottom, left, right] = pick_bc_sides(primary, secondary)
            top = RunIDGenerator.pick_text(primary, secondary, {'bc_top'}, 'UNK');
            bottom = RunIDGenerator.pick_text(primary, secondary, {'bc_bottom'}, 'UNK');
            left = RunIDGenerator.pick_text(primary, secondary, {'bc_left'}, 'UNK');
            right = RunIDGenerator.pick_text(primary, secondary, {'bc_right'}, 'UNK');
        end

        function tok = side_token(side_value)
            token = lower(strtrim(char(string(side_value))));
            if contains(token, 'periodic')
                tok = 'PER';
            elseif contains(token, 'no-slip') || contains(token, 'noslip') || contains(token, 'dirichlet')
                tok = 'NOS';
            elseif contains(token, 'driven') || contains(token, 'neumann')
                tok = 'DRV';
            else
                tok = RunIDGenerator.sanitize_token(side_value);
                if numel(tok) > 3
                    tok = tok(1:3);
                end
            end
        end

        function tok = sanitize_token(raw)
            tok = upper(char(string(raw)));
            tok = strtrim(tok);
            if isempty(tok)
                tok = 'UNK';
                return;
            end
            tok = regexprep(tok, '[^A-Z0-9]+', '-');
            tok = regexprep(tok, '-{2,}', '-');
            tok = regexprep(tok, '^-|-$', '');
            if isempty(tok)
                tok = 'UNK';
            end
        end

        function suffix = random_suffix(nchars)
            alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
            if nargin < 1 || ~isfinite(nchars) || nchars < 1
                nchars = 4;
            end
            nchars = round(nchars);
            idx = randi(numel(alphabet), 1, nchars);
            suffix = alphabet(idx);
        end

        function tok = limit_token(tok, max_len)
            tok = char(string(tok));
            if nargin < 2 || ~isfinite(max_len) || max_len < 1
                return;
            end
            max_len = max(1, round(double(max_len)));
            if numel(tok) > max_len
                tok = tok(1:max_len);
                tok = regexprep(tok, '-+$', '');
            end
            if isempty(tok)
                tok = 'UNK';
            end
        end

        function checksum = base36_checksum(token, nchars)
            alphabet = '0123456789abcdefghijklmnopqrstuvwxyz';
            if nargin < 2 || ~isfinite(nchars) || nchars < 1
                nchars = 4;
            end
            nchars = max(1, round(double(nchars)));
            raw = double(char(string(token)));
            weights = 1:numel(raw);
            value = mod(sum(raw .* weights), 36^nchars);
            checksum = repmat('0', 1, nchars);
            for idx = nchars:-1:1
                digit = mod(value, 36);
                checksum(idx) = alphabet(digit + 1);
                value = floor(value / 36);
            end
        end
    end
end
