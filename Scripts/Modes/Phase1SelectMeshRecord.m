function record = Phase1SelectMeshRecord(results)
%PHASE1SELECTMESHRECORD Select the authoritative Phase 1 mesh record.
% Keep this helper shared so Phase 1 runtime code and targeted contracts
% resolve the selected mesh record through the same logic.

    record = [];
    if ~isstruct(results) || ~isfield(results, 'run_records') || isempty(results.run_records)
        return;
    end

    records = results.run_records(:);
    primary_stage = '';
    if isfield(results, 'stage_summaries') && ~isempty(results.stage_summaries)
        names = {results.stage_summaries.stage_name};
        idx = find(~strcmp(names, 'temporal'), 1, 'first');
        if ~isempty(idx)
            primary_stage = names{idx};
        end
    end
    if isempty(primary_stage)
        primary_stage = local_pick_text(results, {'refinement_axis'}, '');
    end
    if ~isempty(primary_stage) && isfield(records, 'study_stage')
        subset = records(strcmp({records.study_stage}, primary_stage));
        if ~isempty(subset)
            records = subset(:);
        end
    end

    summary = local_pick_struct(results, {'summary'}, struct());
    selected_index = round(double(local_pick_numeric(summary, {'selected_mesh_index'}, NaN)));
    if isfinite(selected_index) && selected_index >= 1 && selected_index <= numel(records)
        record = records(selected_index);
        if ~isfield(record, 'selection_reason') || isempty(record.selection_reason)
            record.selection_reason = local_pick_text(summary, {'selection_reason'}, '');
        end
        return;
    end

    if isfield(records, 'convergence_verdict')
        first_converged = find(strcmpi({records.convergence_verdict}, 'converged'), 1, 'first');
        if ~isempty(first_converged)
            record = records(first_converged);
            record.selection_reason = 'first_converged';
            return;
        end
    end

    record = records(end);
    record.selection_reason = 'finest_mesh_fallback';
end

function value = local_pick_numeric(source, field_names, fallback)
    value = fallback;
    if ~isstruct(source)
        return;
    end
    for i = 1:numel(field_names)
        key = field_names{i};
        if isfield(source, key) && isnumeric(source.(key)) && isscalar(source.(key)) && isfinite(source.(key))
            value = double(source.(key));
            return;
        end
    end
end

function value = local_pick_text(source, field_names, fallback)
    value = fallback;
    if ~isstruct(source)
        return;
    end
    for i = 1:numel(field_names)
        key = field_names{i};
        if isfield(source, key) && ~isempty(source.(key))
            value = char(string(source.(key)));
            return;
        end
    end
end

function value = local_pick_struct(source, field_names, fallback)
    value = fallback;
    if ~isstruct(source)
        return;
    end
    for i = 1:numel(field_names)
        key = field_names{i};
        if isfield(source, key) && isstruct(source.(key))
            value = source.(key);
            return;
        end
    end
end
