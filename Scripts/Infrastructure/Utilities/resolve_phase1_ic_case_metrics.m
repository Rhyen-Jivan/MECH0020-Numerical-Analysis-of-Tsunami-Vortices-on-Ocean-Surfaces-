function metrics = resolve_phase1_ic_case_metrics(case_entry, method_key)
% resolve_phase1_ic_case_metrics  Resolve or reconstruct Phase 1 IC-study mismatch metrics.

    metrics = empty_case_metrics_local();
    if nargin < 1 || ~isstruct(case_entry)
        return;
    end
    if nargin < 2 || strlength(string(method_key)) == 0
        method_key = 'fd';
    end

    method_key = normalize_method_key_local(method_key);
    peer_key = 'spectral';
    if strcmpi(method_key, 'spectral')
        peer_key = 'fd';
    end

    method_entry = pick_struct_local(case_entry, {method_key}, struct());
    stored_metrics = pick_struct_local(method_entry, {'metrics'}, struct());
    metrics = merge_metrics_local(metrics, stored_metrics, false);
    metrics = apply_metric_aliases_local(metrics);
    if phase1_case_metrics_complete_local(metrics)
        return;
    end

    method_output = pick_struct_local(method_entry, {'output'}, struct());
    peer_entry = pick_struct_local(case_entry, {peer_key}, struct());
    peer_output = pick_struct_local(peer_entry, {'output'}, struct());
    fallback_metrics = compute_case_metrics_local(method_output, peer_output);
    metrics = merge_metrics_local(metrics, fallback_metrics, true);
    metrics = apply_metric_aliases_local(metrics);
end

function method_key = normalize_method_key_local(method_key)
    method_key = lower(strtrim(char(string(method_key))));
    if strcmp(method_key, 'sm')
        method_key = 'spectral';
    end
end

function metrics = empty_case_metrics_local()
    metrics = struct( ...
        'cross_method_mismatch_l2', NaN, ...
        'cross_method_mismatch_linf', NaN, ...
        'relative_vorticity_error_L2', NaN, ...
        'relative_vorticity_error_Linf', NaN, ...
        'cross_method_streamfunction_relative_l2_mismatch', NaN, ...
        'cross_method_speed_relative_l2_mismatch', NaN, ...
        'cross_method_velocity_vector_relative_l2_mismatch', NaN, ...
        'cross_method_streamline_direction_relative_l2_mismatch', NaN, ...
        'cross_method_mse', NaN, ...
        'cross_method_rmse', NaN, ...
        'nan_inf_flag', true);
end

function tf = phase1_case_metrics_complete_local(metrics)
    tf = isfinite(pick_numeric_local(metrics, {'cross_method_mismatch_l2'}, NaN)) && ...
        isfinite(pick_numeric_local(metrics, {'cross_method_streamfunction_relative_l2_mismatch'}, NaN)) && ...
        isfinite(pick_numeric_local(metrics, {'cross_method_speed_relative_l2_mismatch'}, NaN)) && ...
        isfinite(pick_numeric_local(metrics, {'cross_method_velocity_vector_relative_l2_mismatch'}, NaN)) && ...
        isfinite(pick_numeric_local(metrics, {'cross_method_streamline_direction_relative_l2_mismatch'}, NaN));
end

function metrics = merge_metrics_local(metrics, source_metrics, fill_missing_only)
    if nargin < 3
        fill_missing_only = false;
    end
    if ~isstruct(source_metrics)
        return;
    end

    source_fields = fieldnames(source_metrics);
    for i = 1:numel(source_fields)
        field_name = source_fields{i};
        source_value = source_metrics.(field_name);
        if ~isfield(metrics, field_name)
            metrics.(field_name) = source_value;
            continue;
        end
        if ~fill_missing_only || metric_value_missing_local(metrics.(field_name))
            metrics.(field_name) = source_value;
        end
    end
end

function tf = metric_value_missing_local(value)
    if isempty(value)
        tf = true;
        return;
    end
    if isnumeric(value)
        tf = ~(isscalar(value) && isfinite(double(value)));
        return;
    end
    tf = false;
end

function metrics = apply_metric_aliases_local(metrics)
    metrics.relative_vorticity_error_L2 = pick_numeric_local(metrics, ...
        {'relative_vorticity_error_L2'}, pick_numeric_local(metrics, {'cross_method_mismatch_l2'}, NaN));
    metrics.relative_vorticity_error_Linf = pick_numeric_local(metrics, ...
        {'relative_vorticity_error_Linf'}, pick_numeric_local(metrics, {'cross_method_mismatch_linf'}, NaN));
end

function metrics = compute_case_metrics_local(method_output, peer_output)
    metrics = empty_case_metrics_local();
    method_results = pick_struct_local(method_output, {'results'}, struct());
    peer_results = pick_struct_local(peer_output, {'results'}, struct());
    method_analysis = require_analysis_local(method_results);
    peer_analysis = require_analysis_local(peer_results);
    if isempty(fieldnames(method_analysis)) || isempty(fieldnames(peer_analysis))
        return;
    end

    method_omega = extract_omega_field_local(method_analysis);
    peer_omega = extract_omega_field_local(peer_analysis);
    if isempty(method_omega) || isempty(peer_omega)
        return;
    end

    method_state = resolve_comparison_state_local(method_analysis, method_omega);
    peer_state = resolve_comparison_state_local(peer_analysis, peer_omega);
    peer_state_on_method = remap_comparison_state_local(peer_state, peer_analysis, method_analysis, size(method_omega));
    mismatch = compute_snapshot_mismatch_local(method_state, peer_state_on_method);
    diff_field = method_state.omega - peer_state_on_method.omega;

    metrics.cross_method_mismatch_l2 = mismatch.vorticity_relative_l2;
    metrics.cross_method_mismatch_linf = field_relative_linf_local(diff_field, peer_state_on_method.omega);
    metrics.cross_method_streamfunction_relative_l2_mismatch = mismatch.streamfunction_relative_l2;
    metrics.cross_method_speed_relative_l2_mismatch = mismatch.speed_relative_l2;
    metrics.cross_method_velocity_vector_relative_l2_mismatch = mismatch.velocity_vector_relative_l2;
    metrics.cross_method_streamline_direction_relative_l2_mismatch = mismatch.streamline_direction_relative_l2;
    metrics.cross_method_mse = mean(diff_field(:).^2, 'omitnan');
    metrics.cross_method_rmse = sqrt(metrics.cross_method_mse);
    metrics.nan_inf_flag = logical(mismatch.nan_inf_flag);
end

function analysis = require_analysis_local(results)
    analysis = struct();
    if isstruct(results) && isfield(results, 'analysis') && isstruct(results.analysis)
        analysis = results.analysis;
    end
    if analysis_has_state_fields_local(analysis)
        return;
    end
    data_path = pick_text_local(results, {'data_path'}, '');
    if strlength(string(data_path)) == 0 || exist(data_path, 'file') ~= 2
        return;
    end
    loaded_data = load(data_path, 'analysis', 'Results');
    if isfield(loaded_data, 'analysis') && isstruct(loaded_data.analysis)
        analysis = loaded_data.analysis;
        return;
    end
    if isfield(loaded_data, 'Results') && isstruct(loaded_data.Results) && ...
            isfield(loaded_data.Results, 'analysis') && isstruct(loaded_data.Results.analysis)
        analysis = loaded_data.Results.analysis;
    end
end

function tf = analysis_has_state_fields_local(analysis)
    tf = isstruct(analysis) && ...
        ((isfield(analysis, 'omega_snaps') && ~isempty(analysis.omega_snaps)) || ...
        (isfield(analysis, 'omega') && ~isempty(analysis.omega)));
end

function omega = extract_omega_field_local(analysis)
    omega = [];
    if ~isstruct(analysis)
        return;
    end
    if isfield(analysis, 'omega_snaps') && ~isempty(analysis.omega_snaps)
        omega_cube = double(analysis.omega_snaps);
        if ndims(omega_cube) == 2
            omega = omega_cube;
        else
            omega = omega_cube(:, :, end);
        end
        return;
    end
    if isfield(analysis, 'omega') && ~isempty(analysis.omega)
        omega_value = double(analysis.omega);
        if ndims(omega_value) == 2
            omega = omega_value;
        else
            omega = omega_value(:, :, end);
        end
    end
end

function state = resolve_comparison_state_local(analysis, omega_slice)
    omega_slice = double(omega_slice);
    [X, Y] = analysis_grid_local(analysis, size(omega_slice));
    x_vec = X(1, :);
    y_vec = Y(:, 1);
    psi_slice = extract_optional_final_slice_local(analysis, 'psi', size(omega_slice));
    u_slice = extract_optional_final_slice_local(analysis, 'u', size(omega_slice));
    v_slice = extract_optional_final_slice_local(analysis, 'v', size(omega_slice));
    [psi_reconstructed, u_reconstructed, v_reconstructed] = velocity_from_omega_slice_local(omega_slice, x_vec, y_vec);
    if isempty(psi_slice)
        psi_slice = psi_reconstructed;
    end
    if isempty(u_slice) || isempty(v_slice)
        u_slice = u_reconstructed;
        v_slice = v_reconstructed;
    end
    psi_slice = center_streamfunction_field_local(psi_slice);
    psi_slice(~isfinite(psi_slice)) = 0;
    u_slice(~isfinite(u_slice)) = 0;
    v_slice(~isfinite(v_slice)) = 0;
    state = struct( ...
        'omega', omega_slice, ...
        'psi', psi_slice, ...
        'u', double(u_slice), ...
        'v', double(v_slice), ...
        'speed', hypot(double(u_slice), double(v_slice)));
end

function slice = extract_optional_final_slice_local(analysis, field_name, target_size)
    slice = [];
    cube = extract_optional_snapshot_cube_local(analysis, field_name);
    if isempty(cube)
        return;
    end
    if nargin >= 3 && ~isempty(target_size) && ...
            (size(cube, 1) ~= target_size(1) || size(cube, 2) ~= target_size(2))
        return;
    end
    if size(cube, 3) <= 1
        slice = cube(:, :, 1);
    else
        slice = cube(:, :, end);
    end
end

function cube = extract_optional_snapshot_cube_local(analysis, field_name)
    cube = [];
    if ~isstruct(analysis)
        return;
    end
    snap_name = sprintf('%s_snaps', char(string(field_name)));
    if isfield(analysis, snap_name) && ~isempty(analysis.(snap_name))
        cube = double(analysis.(snap_name));
        if ndims(cube) == 2
            cube = reshape(cube, size(cube, 1), size(cube, 2), 1);
        end
        return;
    end
    if isfield(analysis, field_name) && ~isempty(analysis.(field_name))
        raw_value = double(analysis.(field_name));
        if ndims(raw_value) == 2
            cube = reshape(raw_value, size(raw_value, 1), size(raw_value, 2), 1);
        elseif ndims(raw_value) >= 3
            cube = raw_value;
        end
    end
end

function remapped_state = remap_comparison_state_local(source_state, source_analysis, target_analysis, target_size)
    remapped_state = struct();
    remapped_state.omega = interpolate_field_to_analysis_local(source_state.omega, source_analysis, target_analysis, target_size);
    remapped_state.psi = center_streamfunction_field_local( ...
        interpolate_field_to_analysis_local(source_state.psi, source_analysis, target_analysis, target_size));
    remapped_state.u = interpolate_field_to_analysis_local(source_state.u, source_analysis, target_analysis, target_size);
    remapped_state.v = interpolate_field_to_analysis_local(source_state.v, source_analysis, target_analysis, target_size);
    remapped_state.u(~isfinite(remapped_state.u)) = 0;
    remapped_state.v(~isfinite(remapped_state.v)) = 0;
    remapped_state.speed = hypot(remapped_state.u, remapped_state.v);
end

function remapped_field = interpolate_field_to_analysis_local(source_field, source_analysis, target_analysis, target_size)
    [Xr, Yr] = analysis_grid_local(source_analysis, size(source_field));
    [Xm, Ym] = analysis_grid_local(target_analysis, target_size);
    remapped_field = interp2(Xr, Yr, double(source_field), Xm, Ym, 'linear', NaN);
    if any(~isfinite(remapped_field(:)))
        remapped_field = interp2(Xr, Yr, double(source_field), Xm, Ym, 'nearest', 0);
    end
end

function mismatch = compute_snapshot_mismatch_local(method_state, peer_state)
    diff_omega = method_state.omega - peer_state.omega;
    diff_psi = center_streamfunction_field_local(method_state.psi) - center_streamfunction_field_local(peer_state.psi);
    diff_speed = method_state.speed - peer_state.speed;
    diff_u = method_state.u - peer_state.u;
    diff_v = method_state.v - peer_state.v;
    mismatch = struct();
    mismatch.vorticity_relative_l2 = field_relative_l2_local(diff_omega, peer_state.omega);
    mismatch.streamfunction_relative_l2 = field_relative_l2_local(diff_psi, center_streamfunction_field_local(peer_state.psi));
    mismatch.speed_relative_l2 = field_relative_l2_local(diff_speed, peer_state.speed);
    mismatch.velocity_vector_relative_l2 = vector_field_relative_l2_local(diff_u, diff_v, peer_state.u, peer_state.v);
    mismatch.streamline_direction_relative_l2 = streamline_direction_relative_l2_local(method_state.u, method_state.v, peer_state.u, peer_state.v);
    mismatch.nan_inf_flag = any(~isfinite(diff_omega(:))) || any(~isfinite(diff_psi(:))) || ...
        any(~isfinite(diff_u(:))) || any(~isfinite(diff_v(:)));
end

function value = field_relative_l2_local(diff_field, reference_field)
    value = NaN;
    diff_vec = double(diff_field(:));
    ref_vec = double(reference_field(:));
    valid = isfinite(diff_vec) & isfinite(ref_vec);
    if ~any(valid)
        return;
    end
    value = safe_ratio_local(norm(diff_vec(valid)), norm(ref_vec(valid)));
end

function value = field_relative_linf_local(diff_field, reference_field)
    value = NaN;
    diff_vec = double(diff_field(:));
    ref_vec = double(reference_field(:));
    valid = isfinite(diff_vec) & isfinite(ref_vec);
    if ~any(valid)
        return;
    end
    value = safe_ratio_local(max(abs(diff_vec(valid))), max(abs(ref_vec(valid))));
end

function value = vector_field_relative_l2_local(diff_u, diff_v, ref_u, ref_v)
    value = NaN;
    diff_u = double(diff_u(:));
    diff_v = double(diff_v(:));
    ref_u = double(ref_u(:));
    ref_v = double(ref_v(:));
    valid = isfinite(diff_u) & isfinite(diff_v) & isfinite(ref_u) & isfinite(ref_v);
    if ~any(valid)
        return;
    end
    diff_norm = sqrt(sum(diff_u(valid).^2 + diff_v(valid).^2, 'omitnan'));
    ref_norm = sqrt(sum(ref_u(valid).^2 + ref_v(valid).^2, 'omitnan'));
    value = safe_ratio_local(diff_norm, ref_norm);
end

function value = streamline_direction_relative_l2_local(u_method, v_method, u_peer, v_peer)
    [dir_method_u, dir_method_v, valid_method] = normalize_velocity_direction_local(u_method, v_method);
    [dir_peer_u, dir_peer_v, valid_peer] = normalize_velocity_direction_local(u_peer, v_peer);
    valid = valid_method & valid_peer;
    if ~any(valid(:))
        value = NaN;
        return;
    end
    value = vector_field_relative_l2_local( ...
        dir_method_u(valid) - dir_peer_u(valid), ...
        dir_method_v(valid) - dir_peer_v(valid), ...
        dir_peer_u(valid), dir_peer_v(valid));
end

function [dir_u, dir_v, valid] = normalize_velocity_direction_local(u_slice, v_slice)
    u_slice = double(u_slice);
    v_slice = double(v_slice);
    speed = hypot(u_slice, v_slice);
    speed_peak = max(speed(:), [], 'omitnan');
    if ~isfinite(speed_peak)
        speed_peak = 1.0e-12;
    end
    threshold = max(speed_peak, 1.0e-12) * 1.0e-6;
    valid = isfinite(speed) & speed > threshold;
    dir_u = zeros(size(u_slice));
    dir_v = zeros(size(v_slice));
    dir_u(valid) = u_slice(valid) ./ speed(valid);
    dir_v(valid) = v_slice(valid) ./ speed(valid);
end

function centered = center_streamfunction_field_local(field)
    centered = double(field);
    finite_values = centered(isfinite(centered));
    if isempty(finite_values)
        centered(:) = 0;
        return;
    end
    centered = centered - mean(finite_values, 'omitnan');
end

function [psi, u, v] = velocity_from_omega_slice_local(omega_slice, x_vec, y_vec)
    omega_slice = double(omega_slice);
    omega_slice(~isfinite(omega_slice)) = 0;
    ny = size(omega_slice, 1);
    nx = size(omega_slice, 2);
    dx = max(mean(diff(double(x_vec))), eps);
    dy = max(mean(diff(double(y_vec))), eps);
    omega_zero_mean = omega_slice - mean(omega_slice(:), 'omitnan');
    omega_hat = fft2(omega_zero_mean);
    kx = (2 * pi / (nx * dx)) * [0:floor(nx / 2), -floor((nx - 1) / 2):-1];
    ky = (2 * pi / (ny * dy)) * [0:floor(ny / 2), -floor((ny - 1) / 2):-1];
    [KX, KY] = meshgrid(kx, ky);
    k2 = KX.^2 + KY.^2;
    psi_hat = zeros(size(omega_hat));
    active_modes = k2 > 0;
    psi_hat(active_modes) = -omega_hat(active_modes) ./ k2(active_modes);
    psi = real(ifft2(psi_hat));
    u = -real(ifft2(1i * KY .* psi_hat));
    v = real(ifft2(1i * KX .* psi_hat));
end

function value = safe_ratio_local(numerator, denominator)
    value = NaN;
    if ~(isfinite(numerator) && isfinite(denominator))
        return;
    end
    value = numerator / max(denominator, 1.0e-12);
end

function [X, Y] = analysis_grid_local(analysis, field_size)
    ny = field_size(1);
    nx = field_size(2);
    if isfield(analysis, 'x') && numel(analysis.x) == nx
        x = double(analysis.x(:)).';
    else
        Lx = pick_numeric_local(analysis, {'Lx'}, nx);
        x = linspace(-Lx / 2, Lx / 2, nx);
    end
    if isfield(analysis, 'y') && numel(analysis.y) == ny
        y = double(analysis.y(:));
    else
        Ly = pick_numeric_local(analysis, {'Ly'}, ny);
        y = linspace(-Ly / 2, Ly / 2, ny).';
    end
    [X, Y] = meshgrid(x, y);
end

function value = pick_numeric_local(source, field_names, fallback)
    if nargin < 3
        fallback = NaN;
    end
    value = fallback;
    if ~isstruct(source)
        return;
    end
    for i = 1:numel(field_names)
        field_name = char(string(field_names{i}));
        if ~isfield(source, field_name)
            continue;
        end
        candidate = source.(field_name);
        if isnumeric(candidate) && isscalar(candidate) && isfinite(double(candidate))
            value = double(candidate);
            return;
        end
    end
end

function value = pick_struct_local(source, field_names, fallback)
    if nargin < 3
        fallback = struct();
    end
    value = fallback;
    if ~isstruct(source)
        return;
    end
    for i = 1:numel(field_names)
        field_name = char(string(field_names{i}));
        if isfield(source, field_name) && isstruct(source.(field_name))
            value = source.(field_name);
            return;
        end
    end
end

function value = pick_text_local(source, field_names, fallback)
    if nargin < 3
        fallback = '';
    end
    value = char(string(fallback));
    if ~isstruct(source)
        return;
    end
    for i = 1:numel(field_names)
        field_name = char(string(field_names{i}));
        if isfield(source, field_name) && strlength(string(source.(field_name))) > 0
            value = char(string(source.(field_name)));
            return;
        end
    end
end
