function [x0_list, y0_list] = disperse_vortices(n_vortices, pattern, Lx, Ly, min_dist)
% DISPERSE_VORTICES Generate spatial positions for multiple vortices
%
% This helper function generates positions for n_vortices based on a
% specified spatial distribution pattern. Used by IC functions to
% support multi-vortex configurations.
%
% Inputs:
%   n_vortices  - Number of vortices (int, >= 1)
%   pattern     - Distribution pattern: 'single', 'circular', 'grid', or 'random'
%   Lx, Ly      - Domain dimensions
%   min_dist    - Minimum separation distance (for 'random' pattern)
%
% Outputs:
%   x0_list, y0_list - Vectors of vortex center positions
%
% Examples:
%   [x, y] = disperse_vortices(1, 'single', 10, 10);           % Single vortex at origin
%   [x, y] = disperse_vortices(3, 'circular', 10, 10);         % 3 vortices in circle
%   [x, y] = disperse_vortices(4, 'grid', 10, 10);             % 2x2 grid of vortices
%   [x, y] = disperse_vortices(5, 'random', 10, 10, 2.0);      % 5 random vortices, min sep 2m
%
% Author: Multi-Vortex IC Support
% Date: February 2026

    % Ensure n_vortices is valid
    if ~isnumeric(n_vortices) || n_vortices < 1
        n_vortices = 1;
    end
    n_vortices = round(n_vortices);
    
    % Ensure pattern is valid
    if ~ischar(pattern) && ~isstring(pattern)
        pattern = 'grid';
    end
    pattern = lower(char(pattern));
    
    % Initialize output
    x0_list = zeros(1, n_vortices);
    y0_list = zeros(1, n_vortices);
    
    % Single vortex at origin
    if n_vortices == 1 || strcmp(pattern, 'single')
        x0_list(1) = 0;
        y0_list(1) = 0;
        return;
    end
    
    % Circular arrangement: vortices on circle around origin
    if strcmp(pattern, 'circular')
        theta = linspace(0, 2*pi, n_vortices+1);
        theta(end) = [];  % Remove duplicate endpoint
        
        % Radius is 1/4 of domain size
        radius = min(Lx, Ly) / 4;
        
        x0_list = radius * cos(theta);
        y0_list = radius * sin(theta);
        return;
    end
    
    % Grid arrangement: rectangular lattice
    if strcmp(pattern, 'grid')
        % Calculate grid dimensions
        n_cols = ceil(sqrt(n_vortices));
        n_rows = ceil(n_vortices / n_cols);
        
        % Spacing between vortices
        spacing_x = Lx / (n_cols + 1);
        spacing_y = Ly / (n_rows + 1);
        
        % Create grid positions
        k = 1;
        for i = 1:n_rows
            for j = 1:n_cols
                if k <= n_vortices
                    x0_list(k) = j * spacing_x - Lx/2;
                    y0_list(k) = i * spacing_y - Ly/2;
                    k = k + 1;
                end
            end
        end
        return;
    end
    
    % Random arrangement with minimum separation
    if strcmp(pattern, 'random')
        if nargin < 5 || isempty(min_dist)
            min_dist = max(Lx, Ly) / 10;  % Default: 10% of domain size
        end
        
        % Pre-allocate output arrays
        x0_list = zeros(1, n_vortices);
        y0_list = zeros(1, n_vortices);
        placed_count = 0;
        
        max_attempts = 10000;
        safety_counter = 0;
        
        for i = 1:n_vortices
            placed = false;
            attempts = 0;
            
            while ~placed && attempts < max_attempts
                % Random position in domain
                x_new = (rand - 0.5) * Lx * 0.9;
                y_new = (rand - 0.5) * Ly * 0.9;
                
                % Check minimum distance to existing vortices
                if placed_count == 0
                    valid = true;
                else
                    distances = sqrt((x0_list(1:placed_count) - x_new).^2 + ...
                                     (y0_list(1:placed_count) - y_new).^2);
                    valid = all(distances >= min_dist);
                end
                
                if valid
                    placed_count = placed_count + 1;
                    x0_list(placed_count) = x_new;
                    y0_list(placed_count) = y_new;
                    placed = true;
                end
                
                attempts = attempts + 1;
                safety_counter = safety_counter + 1;
                
                if safety_counter > max_attempts * n_vortices
                    warning('disperse_vortices: Could not place all vortices with minimum separation.');
                    break;
                end
            end
        end
        
        % Trim to actually placed vortices
        x0_list = x0_list(1:placed_count);
        y0_list = y0_list(1:placed_count);
        
        return;
    end
    
    % Default: use grid pattern if invalid pattern name
    warning('disperse_vortices: Unknown pattern "%s", using "grid"', pattern);
    [x0_list, y0_list] = disperse_vortices(n_vortices, 'grid', Lx, Ly);
    
end
