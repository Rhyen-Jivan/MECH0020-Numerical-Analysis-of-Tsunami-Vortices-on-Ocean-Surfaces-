function tf = bathymetry_blocks_periodic_bc(scenario_id)
% bathymetry_blocks_periodic_bc True when non-flat bathymetry forbids periodic BCs.

    scenario = normalize_bathymetry_scenario_token(scenario_id);
    tf = ~any(strcmp(scenario, {'flat_2d', 'flat_3d'}));
end
