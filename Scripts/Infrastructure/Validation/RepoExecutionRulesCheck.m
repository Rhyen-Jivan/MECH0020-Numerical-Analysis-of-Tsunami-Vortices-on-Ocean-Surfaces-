function result = RepoExecutionRulesCheck(varargin)
% RepoExecutionRulesCheck Validate Phase 0A repository execution rules.
%
% Contract-critical rules fail by default. Naming/schema/defaults migration
% debt is reported as warnings until the repo-wide rename/schema phases are
% executed and CI is switched to fail on warnings as well.

    opts = parse_inputs(varargin{:});
    repo_root = char(string(opts.RepoRoot));
    prev_dir = pwd;
    cd(repo_root);
    cleanup_dir = onCleanup(@() cd(prev_dir)); %#ok<NASGU>

    violations = repmat(make_violation('', 'warning', '', 0, '', ''), 0, 1);
    files = collect_files(repo_root);

    [exceptions, v_ex] = load_exceptions(repo_root, opts.ExceptionsPath);
    violations = [violations; v_ex(:)]; %#ok<AGROW>

    v_manifest = validate_rename_manifest(repo_root, opts.RenameManifestPath);
    violations = [violations; v_manifest(:)]; %#ok<AGROW>

    v = check_filenames(files, opts);              violations = [violations; v(:)]; %#ok<NASGU,AGROW>
    v = check_schema_keys(files, opts);            violations = [violations; v(:)]; %#ok<NASGU,AGROW>
    v = check_defaults_sourcing(repo_root, opts);  violations = [violations; v(:)]; %#ok<NASGU,AGROW>
    v = check_fallback_realism(repo_root, opts);   violations = [violations; v(:)]; %#ok<NASGU,AGROW>
    v = check_legacy_refs(files);                  violations = [violations; v(:)]; %#ok<NASGU,AGROW>
    v = check_path_hygiene(repo_root);             violations = [violations; v(:)]; %#ok<NASGU,AGROW>
    v = check_dispatch_fallbacks(repo_root, opts); violations = [violations; v(:)]; %#ok<NASGU,AGROW>

    violations = apply_exceptions(violations, exceptions);
    summary = summarize(violations);

    result = struct();
    result.metadata = struct( ...
        'checker', 'RepoExecutionRulesCheck', ...
        'phase', '0A', ...
        'generated_at', datestr(now, 'yyyy-mm-ddTHH:MM:SS'), ...
        'repo_root', norm_path(repo_root), ...
        'file_count', numel(files), ...
        'enforce_naming_rules', logical(opts.EnforceNamingRules));
    result.summary = summary;
    result.violations = violations;

    if opts.WriteReports
        result.report_paths = write_reports(repo_root, opts.ReportDir, result);
    else
        result.report_paths = struct('json', '', 'markdown', '');
    end

    fprintf('[RepoExecutionRulesCheck] Findings=%d | Errors=%d (non-exempt=%d) | Warnings=%d (non-exempt=%d)\n', ...
        summary.total, summary.errors, summary.non_exempt_errors, summary.warnings, summary.non_exempt_warnings);
    if opts.WriteReports
        fprintf('[RepoExecutionRulesCheck] Reports: %s | %s\n', result.report_paths.json, result.report_paths.markdown);
    end

    should_fail = summary.non_exempt_errors > 0 || (opts.FailOnWarning && summary.non_exempt_warnings > 0);
    if opts.FailOnViolation && should_fail
        error('RepoExecutionRulesCheck:RuleViolation', ...
            'Phase 0A rules failed (%d non-exempt errors, %d non-exempt warnings).', ...
            summary.non_exempt_errors, summary.non_exempt_warnings);
    end
end

function opts = parse_inputs(varargin)
    p = inputParser;
    addParameter(p, 'RepoRoot', pwd, @(x) ischar(x) || isstring(x));
    addParameter(p, 'FailOnViolation', true, @(x) islogical(x) || isnumeric(x));
    addParameter(p, 'FailOnWarning', false, @(x) islogical(x) || isnumeric(x));
    addParameter(p, 'EnforceNamingRules', false, @(x) islogical(x) || isnumeric(x));
    addParameter(p, 'WriteReports', true, @(x) islogical(x) || isnumeric(x));
    addParameter(p, 'ReportDir', fullfile('Artifacts', 'Validation'), @(x) ischar(x) || isstring(x));
    addParameter(p, 'ExceptionsPath', fullfile('settings', 'repo_execution_rules_exceptions.json'), @(x) ischar(x) || isstring(x));
    addParameter(p, 'RenameManifestPath', fullfile('settings', 'repo_execution_rules_rename_manifest.json'), @(x) ischar(x) || isstring(x));
    parse(p, varargin{:});
    opts = p.Results;
    opts.FailOnViolation = logical(opts.FailOnViolation);
    opts.FailOnWarning = logical(opts.FailOnWarning);
    opts.EnforceNamingRules = logical(opts.EnforceNamingRules);
    opts.WriteReports = logical(opts.WriteReports);
end

function files = collect_files(repo_root)
    roots = {'Scripts','tests','utilities','docs','Markdowns'};
    excludes = {'Scripts/Legacy/','docs/archive/','Results/','Data/','Artifacts/','Research Papers/'};
    files = struct('path', {}, 'ext', {}, 'stem', {}, 'name', {});
    seen = containers.Map('KeyType','char','ValueType','logical');

    for i = 1:numel(roots)
        abs_root = fullfile(repo_root, roots{i});
        if ~isfolder(abs_root), continue; end
        listing = dir(fullfile(abs_root, '**', '*'));
        for j = 1:numel(listing)
            if listing(j).isdir, continue; end
            rel = rel_path(fullfile(listing(j).folder, listing(j).name), repo_root);
            if any(cellfun(@(p) startsWith(lower(rel), lower(p)), excludes)), continue; end
            if isKey(seen, rel), continue; end
            seen(rel) = true;
            [~, stem, ext] = fileparts(rel);
            files(end+1) = struct('path', rel, 'ext', lower(ext), 'stem', stem, 'name', [stem ext]); %#ok<AGROW>
        end
    end

    for rf = {'AGENTS.md','README.md'}
        rel = rf{1};
        abs = fullfile(repo_root, rel);
        if isfile(abs) && ~isKey(seen, rel)
            [~, stem, ext] = fileparts(rel);
            files(end+1) = struct('path', rel, 'ext', lower(ext), 'stem', stem, 'name', [stem ext]); %#ok<AGROW>
        end
    end
end

function out = check_filenames(files, opts)
    out = empty_findings();
    acronyms = {'UI','IC','BC','FD','FV','GPU','CPU','FFT','ID'};
    for i = 1:numel(files)
        f = files(i);
        if any(strcmpi(f.path, {'AGENTS.md','README.md'})), continue; end

        if strcmp(f.ext, '.md') && (startsWith(f.path, 'docs/') || startsWith(f.path, 'Markdowns/'))
            if contains(f.name, ' ') || any(double(f.name) > 127) || ~is_title_case_doc(f.stem)
                out(end+1) = make_violation('RDOC001', sev('RDOC001', opts), f.path, 0, ... %#ok<AGROW>
                    'Docs/Markdown filename should be Title_Case with underscores (ASCII, no spaces).', f.name);
            end
        end

        if strcmp(f.ext, '.m') && startsWith(f.path, 'tests/')
            if ~is_test_entrypoint_candidate(f.path, f.stem)
                continue;
            end
            if ~(startsWith(f.stem, 'Test') || startsWith(f.stem, 'Run')) || isempty(regexp(f.stem, '^[A-Z][A-Za-z0-9]*$', 'once'))
                out(end+1) = make_violation('RTEST001', sev('RTEST001', opts), f.path, 0, ... %#ok<AGROW>
                    'MATLAB test file must be PascalCase and begin with Test or Run.', f.name);
            end
        end

        if any(strcmp(f.ext, {'.py','.ps1','.sh'})) && isempty(regexp(f.stem, '^[a-z0-9]+(_[a-z0-9]+)*$', 'once'))
            out(end+1) = make_violation('RNAME001', sev('RNAME001', opts), f.path, 0, ... %#ok<AGROW>
                'Script filename should use lower_snake_case.', f.name);
        end

        if strcmp(f.ext, '.m') && (contains(f.name,' ') || contains(f.name,'-'))
            out(end+1) = make_violation('RNAME001', sev('RNAME001', opts), f.path, 0, ... %#ok<AGROW>
                'MATLAB filename must not contain spaces/hyphens.', f.name);
        end

        if strcmp(f.ext, '.m')
            for a = 1:numel(acronyms)
                good = acronyms{a};
                bad = [upper(good(1)) lower(good(2:end))];
                if contains(f.stem, bad) && ~contains(f.stem, good)
                    out(end+1) = make_violation('RCASE001', sev('RCASE001', opts), f.path, 0, ... %#ok<AGROW>
                        'Approved acronym capitalization not preserved in PascalCase name.', ...
                        sprintf('Found "%s" in "%s", expected "%s".', bad, f.stem, good));
                end
            end
        end
    end
end

function out = check_schema_keys(files, opts)
    out = empty_findings();
    prefixes = {'Scripts/UI/','Scripts/Modes/','Scripts/Sustainability/','Scripts/Plotting/','Scripts/Infrastructure/Dispatchers/','Scripts/Infrastructure/Utilities/'};
    % Require token to be a standalone identifier (not suffix of ax_progress/results_empty_state/monitor_state).
    re = '(^|[^A-Za-z0-9_])(payload|progress|telemetry|metadata|summary|state)\.([A-Za-z][A-Za-z0-9_]*)';
    for i = 1:numel(files)
        f = files(i);
        if ~strcmp(f.ext,'.m') || ~any(cellfun(@(p) startsWith(f.path,p), prefixes)), continue; end
        txt = try_read(f.path);
        if isempty(txt), continue; end
        lines = cellstr(splitlines(strrep(txt, sprintf('\r'), '')));
        for ln = 1:numel(lines)
            toks = regexp(lines{ln}, re, 'tokens');
            for t = 1:numel(toks)
                key = toks{t}{3};
                if isempty(regexp(key, '^[a-z0-9]+(_[a-z0-9]+)*$', 'once'))
                    out(end+1) = make_violation('RSCM001', sev('RSCM001', opts), f.path, ln, ... %#ok<AGROW>
                        'Internal schema keys should use lower_snake_case.', sprintf('%s.%s', toks{t}{2}, key));
                end
            end
        end
    end
end

function out = check_defaults_sourcing(repo_root, opts)
    out = empty_findings();
    ui = 'Scripts/UI/UIController.m';
    cdp = 'Scripts/Infrastructure/Initialisers/create_default_parameters.m';
    if ~isfile(fullfile(repo_root, cdp))
        out(end+1) = make_violation('RDEF001', 'error', cdp, 0, ... %#ok<AGROW>
            'Missing create_default_parameters.m (contract-critical internal dependency).', '');
        return;
    end
    if ~isfile(fullfile(repo_root, ui))
        out(end+1) = make_violation('RDEF001', 'error', ui, 0, ... %#ok<AGROW>
            'Missing UIController.m; cannot validate defaults sourcing.', '');
        return;
    end
    txt = fileread(fullfile(repo_root, ui));
    if ~contains(txt, 'create_default_parameters')
        out(end+1) = make_violation('RDEF001', 'error', ui, 0, ... %#ok<AGROW>
            'UIController does not reference create_default_parameters.m.', '');
    end
    % Bootstrap warning: detect literal fallback defaults in UI seed/default paths.
    fallback_count = count(txt, 'pick_field(base');
    if fallback_count > 0
        out(end+1) = make_violation('RDEF001', sev('RDEF001', opts), ui, 0, ... %#ok<AGROW>
            'UIController contains literal fallback defaults (migration debt).', ...
            sprintf('Detected %d pick_field(base, ..., fallback) calls.', fallback_count));
    end
end

function out = check_fallback_realism(repo_root, opts)
    out = empty_findings();
    files = {'Scripts/UI/UIController.m','Scripts/Modes/Convergence/run_adaptive_convergence.m','Scripts/Infrastructure/Dispatchers/ModeDispatcher.m'};
    for i = 1:numel(files)
        rel = files{i};
        abs = fullfile(repo_root, rel);
        if ~isfile(abs), continue; end
        txt = fileread(abs);
        has_cdp_gate = ~isempty(regexp(txt, "if\s+exist\(\s*'create_default_parameters'\s*,\s*'file'\s*\)", 'once'));
        cdp_fail_fast = ~isempty(regexp(txt, ...
            "if\s+exist\(\s*'create_default_parameters'\s*,\s*'file'\s*\)[\s\S]{0,800}?error\s*\(", 'once'));
        if has_cdp_gate && ~cdp_fail_fast
            out(end+1) = make_violation('RFBK001', sev('RFBK001', opts), rel, 0, ... %#ok<AGROW>
                'Possible internal fallback gating around create_default_parameters detected.', 'Review for fail-fast behavior.');
        end
        has_pathbuilder_gate = ~isempty(regexp(txt, "if\s+exist\(\s*'PathBuilder'\s*,", 'once'));
        pathbuilder_fail_fast = ~isempty(regexp(txt, ...
            "if\s+exist\(\s*'PathBuilder'\s*,[\s\S]{0,1600}?error\s*\(", 'once'));
        if has_pathbuilder_gate && ~pathbuilder_fail_fast
            out(end+1) = make_violation('RFBK001', sev('RFBK001', opts), rel, 0, ... %#ok<AGROW>
                'Possible internal fallback gating around PathBuilder detected.', ...
                'Packaged internal path/artifact builders should fail fast when unavailable.');
        end
    end
end

function out = check_legacy_refs(files)
    out = empty_findings();
    skip_prefixes = {'Scripts/Infrastructure/Validation/'};
    for i = 1:numel(files)
        f = files(i);
        if ~(startsWith(f.path,'Scripts/') || startsWith(f.path,'tests/') || startsWith(f.path,'utilities/')), continue; end
        if any(cellfun(@(p) startsWith(f.path, p), skip_prefixes)), continue; end
        if ~any(strcmp(f.ext, {'.m','.md','.json','.py','.ps1','.sh','.txt'})), continue; end
        txt = try_read(f.path);
        if isempty(txt), continue; end
        lines = cellstr(splitlines(strrep(txt, sprintf('\r'), '')));
        for ln = 1:numel(lines)
            if contains(lines{ln}, 'Scripts/Legacy') || contains(lines{ln}, 'Scripts\Legacy')
                out(end+1) = make_violation('RLEG001', 'error', f.path, ln, ... %#ok<AGROW>
                    'Active code/tests/utilities should not reference Scripts/Legacy.', strtrim(lines{ln}));
            end
        end
    end
end

function out = check_path_hygiene(repo_root)
    out = empty_findings();
    targets = {'Scripts/Infrastructure/PathSetup.m','Scripts/Infrastructure/Dispatchers/ModeDispatcher.m','Scripts/Drivers/Tsunami_Vorticity_Emulator.m'};
    for i = 1:numel(targets)
        rel = targets{i};
        abs = fullfile(repo_root, rel);
        if ~isfile(abs), continue; end
        txt = fileread(abs);
        if contains(txt, 'genpath(') && ~(contains(txt,'Legacy') || contains(txt,'pruned_genpath') || contains(txt,'split_genpath'))
            lines = cellstr(splitlines(strrep(txt, sprintf('\r'), '')));
            for ln = 1:numel(lines)
                if contains(lines{ln}, 'genpath(')
                    out(end+1) = make_violation('RPATH001', 'error', rel, ln, ... %#ok<AGROW>
                        'genpath() used without visible legacy/archive pruning.', 'Exclude Scripts/Legacy from active runtime path setup.');
                end
            end
        end
    end
end

function out = check_dispatch_fallbacks(repo_root, opts)
    out = empty_findings();
    ddir = fullfile(repo_root, 'Scripts', 'Infrastructure', 'Dispatchers');
    if ~isfolder(ddir), return; end
    listing = dir(fullfile(ddir, '*.m'));
    for i = 1:numel(listing)
        rel = rel_path(fullfile(listing(i).folder, listing(i).name), repo_root);
        txt = fileread(fullfile(listing(i).folder, listing(i).name));
        lines = cellstr(splitlines(strrep(txt, sprintf('\r'), '')));
        current_fn = '';
        if contains(txt, 'Keep original (will error')
            out(end+1) = make_violation('RDISP001', sev('RDISP001', opts), rel, 0, ... %#ok<AGROW>
                'Dispatcher normalization appears to defer invalid-state handling.', 'Prefer explicit failure at normalization point.');
        end
        for ln = 1:numel(lines)
            fn_tok = regexp(lines{ln}, '^\s*function\b(?:\s+\[[^\]]*\]\s*=\s*|\s+[^=]*=\s*)?\s*([A-Za-z][A-Za-z0-9_]*)\s*\(', 'tokens', 'once');
            if ~isempty(fn_tok)
                current_fn = fn_tok{1};
            end
            if ~contains(strtrim(lines{ln}), 'otherwise'), continue; end
            if strcmp(rel, 'Scripts/Infrastructure/Dispatchers/BCDispatcher.m') && ...
                    any(strcmp(current_fn, {'compute_capability', 'side_display_label'}))
                continue;
            end
            window = strjoin(lines(ln:min(ln+6, numel(lines))), newline);
            if strcmp(rel, 'Scripts/Infrastructure/Dispatchers/BCDispatcher.m')
                if (contains(window, 'capability.supported = false;') && contains(window, 'capability.reason')) || ...
                        contains(window, 'No-slip (')
                    continue;
                end
            end
            if ~(contains(window, 'error(') || contains(window, 'ErrorHandler.throw') || contains(window, 'rethrow('))
                out(end+1) = make_violation('RDISP001', sev('RDISP001', opts), rel, ln, ... %#ok<AGROW>
                    'Dispatcher switch otherwise branch may not fail explicitly.', 'Review for silent fallback.');
            end
        end
    end
end

function [exceptions, out] = load_exceptions(repo_root, rel_json)
    out = empty_findings();
    exceptions = struct('rule_id', {}, 'path', {}, 'scope', {}, 'owner', {}, 'expires_on', {}, 'status', {});
    abs = fullfile(repo_root, rel_json);
    if ~isfile(abs)
        out(end+1) = make_violation('REXC001', 'error', rel_json, 0, 'Exception registry file missing.', ''); %#ok<AGROW>
        return;
    end
    try
        data = jsondecode(fileread(abs));
    catch ME
        out(end+1) = make_violation('REXC001', 'error', rel_json, 0, 'Exception registry JSON invalid.', ME.message); %#ok<AGROW>
        return;
    end
    if ~isstruct(data) || ~isfield(data, 'exceptions')
        out(end+1) = make_violation('REXC001', 'error', rel_json, 0, 'Exception registry missing "exceptions" array.', ''); %#ok<AGROW>
        return;
    end
    if isempty(data.exceptions), return; end
    if ~isstruct(data.exceptions)
        out(end+1) = make_violation('REXC001', 'error', rel_json, 0, '"exceptions" must be an array of objects.', ''); %#ok<AGROW>
        return;
    end
    req = {'rule_id','path','scope','justification','owner','expires_on','status'};
    seen = containers.Map('KeyType','char','ValueType','logical');
    today_dt = datetime('today');
    for i = 1:numel(data.exceptions)
        e = data.exceptions(i);
        miss = req(~isfield(e, req));
        if ~isempty(miss)
            out(end+1) = make_violation('REXC001', 'error', rel_json, 0, ... %#ok<AGROW>
                'Exception entry missing required fields.', sprintf('Entry %d missing: %s', i, strjoin(miss, ', ')));
            continue;
        end
        ex = struct();
        ex.rule_id = char(string(e.rule_id));
        ex.path = norm_path(char(string(e.path)));
        ex.scope = lower(strtrim(char(string(e.scope))));
        ex.owner = char(string(e.owner));
        ex.expires_on = char(string(e.expires_on));
        ex.status = lower(strtrim(char(string(e.status))));
        if ~ismember(ex.scope, {'path','prefix'}) || ~ismember(ex.status, {'active','inactive','retired'})
            out(end+1) = make_violation('REXC001', 'error', rel_json, 0, ... %#ok<AGROW>
                'Exception entry has invalid scope/status.', sprintf('Entry %d', i));
            continue;
        end
        try
            exp_dt = datetime(ex.expires_on, 'InputFormat', 'yyyy-MM-dd');
        catch
            out(end+1) = make_violation('REXC001', 'error', rel_json, 0, ... %#ok<AGROW>
                'Exception entry has invalid expires_on date.', sprintf('Entry %d date=%s', i, ex.expires_on));
            continue;
        end
        if strcmp(ex.status, 'active') && exp_dt < today_dt
            out(end+1) = make_violation('REXC001', 'error', rel_json, 0, ... %#ok<AGROW>
                'Active exception is expired.', sprintf('%s %s expired %s', ex.rule_id, ex.path, ex.expires_on));
        end
        if strcmp(ex.status, 'active')
            key = lower([ex.rule_id '|' ex.scope '|' ex.path]);
            if isKey(seen, key)
                out(end+1) = make_violation('REXC001', 'error', rel_json, 0, ... %#ok<AGROW>
                    'Duplicate active exception entry.', key);
            else
                seen(key) = true;
            end
        end
        exceptions(end+1) = ex; %#ok<AGROW>
    end
end

function out = validate_rename_manifest(repo_root, rel_json)
    out = empty_findings();
    abs = fullfile(repo_root, rel_json);
    if ~isfile(abs)
        out(end+1) = make_violation('RNAME001', 'warning', rel_json, 0, 'Rename manifest file missing.', ''); %#ok<AGROW>
        return;
    end
    try
        data = jsondecode(fileread(abs));
    catch ME
        out(end+1) = make_violation('RNAME001', 'warning', rel_json, 0, 'Rename manifest JSON invalid.', ME.message); %#ok<AGROW>
        return;
    end
    if ~isstruct(data) || ~all(isfield(data, {'schema_version','policy','entries'}))
        out(end+1) = make_violation('RNAME001', 'warning', rel_json, 0, 'Rename manifest missing required fields.', ''); %#ok<AGROW>
        return;
    end
    if isempty(data.entries), return; end
    if ~isstruct(data.entries)
        out(end+1) = make_violation('RNAME001', 'warning', rel_json, 0, 'Rename manifest entries must be objects.', ''); %#ok<AGROW>
        return;
    end
    tgt = containers.Map('KeyType','char','ValueType','double');
    for i = 1:numel(data.entries)
        e = data.entries(i);
        if ~all(isfield(e, {'old_path','new_path','status'}))
            out(end+1) = make_violation('RNAME001', 'warning', rel_json, 0, 'Rename manifest entry missing required fields.', sprintf('Entry %d', i)); %#ok<AGROW>
            continue;
        end
        k = lower(norm_path(char(string(e.new_path))));
        if isKey(tgt, k), tgt(k)=tgt(k)+1; else, tgt(k)=1; end
    end
    keys_ = tgt.keys;
    for i = 1:numel(keys_)
        if tgt(keys_{i}) > 1
            out(end+1) = make_violation('RNAME001', 'warning', rel_json, 0, ... %#ok<AGROW>
                'Rename manifest contains duplicate target path(s).', keys_{i});
        end
    end
end

function violations = apply_exceptions(violations, exceptions)
    for i = 1:numel(violations)
        for j = 1:numel(exceptions)
            ex = exceptions(j);
            if ~strcmp(ex.status,'active'), continue; end
            if ~strcmpi(violations(i).rule_id, ex.rule_id), continue; end
            p = lower(norm_path(violations(i).path));
            x = lower(norm_path(ex.path));
            match = (strcmp(ex.scope,'path') && strcmp(p,x)) || (strcmp(ex.scope,'prefix') && startsWith(p,x));
            if match
                violations(i).exempted = true;
                violations(i).exception_owner = ex.owner;
                violations(i).exception_expires_on = ex.expires_on;
                break;
            end
        end
    end
end

function s = summarize(v)
    s = struct('total',0,'errors',0,'warnings',0,'exempted',0,'non_exempt_errors',0,'non_exempt_warnings',0);
    if isempty(v), return; end
    sevv = {v.severity};
    ex = [v.exempted];
    s.total = numel(v);
    s.errors = sum(strcmp(sevv,'error'));
    s.warnings = sum(strcmp(sevv,'warning'));
    s.exempted = sum(ex);
    s.non_exempt_errors = sum(strcmp(sevv,'error') & ~ex);
    s.non_exempt_warnings = sum(strcmp(sevv,'warning') & ~ex);
end

function paths = write_reports(repo_root, report_dir, result)
    if ~is_absolute(char(report_dir))
        out_dir = fullfile(repo_root, char(report_dir));
    else
        out_dir = char(report_dir);
    end
    if ~isfolder(out_dir), mkdir(out_dir); end
    json_file = fullfile(out_dir, 'repo_execution_rules_report.json');
    md_file = fullfile(out_dir, 'repo_execution_rules_report.md');

    try
        json_text = jsonencode(result, 'PrettyPrint', true);
    catch
        json_text = jsonencode(result);
    end
    write_text(json_file, json_text);
    write_text(md_file, render_md(result));

    paths = struct('json', rel_or_abs(json_file, repo_root), 'markdown', rel_or_abs(md_file, repo_root));
end

function txt = render_md(result)
    s = result.summary;
    lines = { ...
        '# Repo Execution Rules Check Report', ...
        '', ...
        sprintf('- Generated: `%s`', result.metadata.generated_at), ...
        sprintf('- File count: `%d`', result.metadata.file_count), ...
        sprintf('- Errors: `%d` (non-exempt `%d`)', s.errors, s.non_exempt_errors), ...
        sprintf('- Warnings: `%d` (non-exempt `%d`)', s.warnings, s.non_exempt_warnings), ...
        '', ...
        '## Findings', ...
        '', ...
        '| Rule | Severity | Path | Line | Exempted | Message |', ...
        '| --- | --- | --- | ---: | --- | --- |'};
    for i = 1:numel(result.violations)
        v = result.violations(i);
        lines{end+1} = sprintf('| `%s` | %s | `%s` | %d | %s | %s |', ... %#ok<AGROW>
            v.rule_id, v.severity, strrep(v.path,'|','\|'), v.line, tf(v.exempted), strrep(v.message,'|','\|'));
        if ~isempty(v.details)
            lines{end+1} = sprintf('|  |  |  |  |  | %s |', strrep(v.details,'|','\|')); %#ok<AGROW>
        end
    end
    if isempty(result.violations)
        lines{end+1} = '| _none_ |  |  |  |  | No findings |'; %#ok<AGROW>
    end
    txt = strjoin(lines, newline);
end

function v = make_violation(rule_id, severity, path_, line_, msg, details)
    if nargin < 6, details = ''; end
    v = struct('rule_id', char(rule_id), 'severity', lower(char(severity)), 'path', norm_path(path_), ...
        'line', double(line_), 'message', char(msg), 'details', char(details), ...
        'exempted', false, 'exception_owner', '', 'exception_expires_on', '');
end

function out = empty_findings()
    out = repmat(make_violation('', 'warning', '', 0, '', ''), 0, 1);
end

function s = sev(rule_id, opts)
    if ismember(rule_id, {'RNAME001','RCASE001','RTEST001','RDOC001','RSCM001'})
        if opts.EnforceNamingRules, s = 'error'; else, s = 'warning'; end
    elseif ismember(rule_id, {'RDEF001','RFBK001','RDISP001'})
        s = 'warning'; % bootstrap debt tracking until Step 7/8
    else
        s = 'error';
    end
end

function tf_ = is_test_entrypoint_candidate(rel_path, stem)
    rel_path = norm_path(rel_path);
    entrypoint_stems = {
        'RunComprehensiveTestSuite', ...
        'RunDebugFilterTest', ...
        'RunVerifyRefactoredArchitecture'};
    helper_prefixes = {
        'tests/ui/ensure_', ...
        'tests/ui/capture_', ...
        'tests/ui/probe_'};
    if any(cellfun(@(p) startsWith(rel_path, p), helper_prefixes))
        tf_ = false;
        return;
    end
    if any(strcmp(stem, {'Get_Test_Cases', 'static_analysis'}))
        tf_ = false;
        return;
    end
    tf_ = startsWith(stem, 'Test') || startsWith(stem, 'Run') || ...
          startsWith(stem, 'test_') || startsWith(stem, 'TEST_') || ...
          startsWith(stem, 'run_') || startsWith(stem, 'Run_') || ...
          any(strcmp(stem, entrypoint_stems));
end

function tf_ = is_title_case_doc(stem)
    if contains(stem, ' '), tf_ = false; return; end
    parts = regexp(stem, '_', 'split');
    if isempty(parts), tf_ = false; return; end
    tf_ = true;
    for i = 1:numel(parts)
        p = parts{i};
        if isempty(p), tf_ = false; return; end
        if isempty(regexp(p, '^[0-9]+$', 'once')) && isempty(regexp(p, '^[A-Z][A-Za-z0-9]*$', 'once'))
            tf_ = false; return;
        end
    end
end

function txt = try_read(rel_path)
    try
        txt = fileread(rel_path);
    catch
        txt = '';
    end
end

function out = norm_path(in)
    out = regexprep(strrep(char(string(in)), '\', '/'), '/+', '/');
end

function rel = rel_path(abs_path, repo_root)
    absn = norm_path(abs_path);
    rootn = norm_path(repo_root);
    pref = [rootn '/'];
    if startsWith(lower(absn), lower(pref))
        rel = absn(numel(pref)+1:end);
    else
        rel = absn;
    end
end

function tf_ = is_absolute(p)
    tf_ = ~isempty(regexp(char(string(p)), '^[A-Za-z]:[\\/]', 'once')) || startsWith(char(string(p)), '/');
end

function out = rel_or_abs(abs_path, repo_root)
    if isfile(abs_path)
        out = rel_path(abs_path, repo_root);
    else
        out = norm_path(abs_path);
    end
end

function write_text(path_, txt)
    fid = fopen(path_, 'w');
    if fid < 0
        error('RepoExecutionRulesCheck:WriteFailed', 'Could not write %s', path_);
    end
    c = onCleanup(@() fclose(fid)); %#ok<NASGU>
    fwrite(fid, txt, 'char');
end

function s = tf(x)
    if x, s = 'true'; else, s = 'false'; end
end
