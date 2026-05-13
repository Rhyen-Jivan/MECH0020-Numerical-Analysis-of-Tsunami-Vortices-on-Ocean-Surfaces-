function scenario = normalize_bathymetry_scenario_token(raw_scenario)
% normalize_bathymetry_scenario_token Map legacy bathymetry aliases to canonical tokens.

    scenario = lower(strtrim(char(string(raw_scenario))));
    if isempty(scenario)
        scenario = 'flat_2d';
        return;
    end

    switch scenario
        case {'flat', 'flat2d'}
            scenario = 'flat_2d';
        case {'wavebed_2d', 'wave_profile', 'wave_profile2d'}
            scenario = 'wave_profile_2d';
        case {'shelf_slope_2d', 'shore_runup', 'shore_runup2d'}
            scenario = 'shore_runup_2d';
        case {'tsunami_runup_composite', 'tsunami_runup_composite2d', 'composite_runup_2d'}
            scenario = 'tsunami_runup_composite_2d';
        case {'tohoku_profile', 'tohoku_profile2d', 'tohoku_2d'}
            scenario = 'tohoku_profile_2d';
        case {'reef_barrier_2d', 'reef', 'reef2d'}
            scenario = 'reef_2d';
        case {'trench_cliff_2d', 'recess', 'recess2d'}
            scenario = 'recess_2d';
        case {'linear_elevation', 'linear_elevation2d'}
            scenario = 'linear_elevation_2d';
        case {'custom_points', 'custom_points2d'}
            scenario = 'custom_points_2d';
        case {'flat3d', 'flat_plane_3d'}
            scenario = 'flat_3d';
        case {'wavebed_3d', 'wave_profile_3d'}
            scenario = 'wavebed_3d';
        case {'seamount_basin', 'seamount_basin3d'}
            scenario = 'seamount_basin_3d';
        case {'canyon_undercut', 'canyon_undercut3d'}
            scenario = 'canyon_undercut_3d';
        case {'reef_lagoon', 'reef_lagoon3d'}
            scenario = 'reef_lagoon_3d';
    end
end
