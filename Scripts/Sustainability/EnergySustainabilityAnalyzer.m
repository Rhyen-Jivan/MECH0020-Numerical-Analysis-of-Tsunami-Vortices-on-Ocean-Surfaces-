%% Energy Scaling & Sustainability Analysis
%  =========================================
%
%  Purpose: Analyze how energy consumption scales with simulation complexity
%           Build predictive models for computational sustainability
%           Generate sustainability reports and visualizations
%
%  Main Functions:
%    - build_scaling_model()  : Create E = A * C^α power-law model
%    - predict_energy()       : Predict energy for new complexity
%    - plot_scaling()         : Visualize energy scaling relationship
%    - sustainability_score() : Compute efficiency metrics
%
% =========================================

classdef EnergySustainabilityAnalyzer < handle
    % Analyze and model computational energy consumption
    
    properties
        scaling_model          % Power-law model coefficients
        data_points            % (complexity, energy) pairs
        model_type = 'power'   % Type: 'power', 'linear', 'quadratic'
        r_squared              % Goodness of fit
        residuals              % Prediction errors
    end
    
    methods
        function obj = EnergySustainabilityAnalyzer()
            % Initialize analyzer
            obj.data_points = [];
            obj.scaling_model = [];
            obj.r_squared = 0;
            obj.residuals = [];
        end
        
        function add_data_point(obj, complexity, energy_joules)
            % Add (complexity, energy) measurement
            %
            % Args:
            %   complexity (float): Computational complexity metric (e.g., grid points)
            %   energy_joules (float): Energy consumed (Joules)
            
            obj.data_points = [obj.data_points; complexity, energy_joules];
        end
        
        function add_data_from_log(obj, log_file, complexity)
            % Load energy data from hardware monitor log file
            %
            % Args:
            %   log_file (string): Path to CSV log file
            %   complexity (float): Complexity for this run
            
            try
                T = readtable(log_file);
                
                % Compute energy integral
                valid_powers = T.power_consumption(~isnan(T.power_consumption));
                if ~isempty(valid_powers)
                    dt = diff(T.timestamp(1:length(valid_powers)));
                    energy_joules = sum(valid_powers(1:end-1) .* dt);
                    
                    obj.add_data_point(complexity, energy_joules);
                    fprintf('Added: Complexity=%.0f, Energy=%.1f J\n', complexity, energy_joules);
                else
                    warning('No valid power measurements in %s', log_file);
                end
            catch ME
                error('Failed to load log: %s', ME.message);
            end
        end
        
        function build_scaling_model(obj)
            % Build power-law model: E = A * C^α
            %
            % Uses least-squares fitting on log-log scale:
            %   ln(E) = ln(A) + α*ln(C)
            
            if size(obj.data_points, 1) < 2
                error('Need at least 2 data points');
            end
            
            C = obj.data_points(:, 1);  % Complexity
            E = obj.data_points(:, 2);  % Energy
            
            % Log-log transformation
            ln_C = log(C);
            ln_E = log(E);
            
            % Least squares fit
            A_matrix = [ones(length(C), 1), ln_C];
            coeffs = A_matrix \ ln_E;  % Linear regression
            
            ln_A = coeffs(1);
            alpha = coeffs(2);
            
            A = exp(ln_A);
            
            obj.scaling_model = struct('A', A, 'alpha', alpha);
            obj.model_type = 'power';
            
            % Compute R²
            E_predicted = A * C.^alpha;
            SS_res = sum((E - E_predicted).^2);
            SS_tot = sum((E - mean(E)).^2);
            obj.r_squared = 1 - (SS_res / SS_tot);
            obj.residuals = E - E_predicted;
            
            fprintf('\n%s\n', repmat('=', 1, 70));
            fprintf('POWER-LAW ENERGY SCALING MODEL\n');
            fprintf('%s\n', repmat('=', 1, 70));
            fprintf('Model: E = A * C^α\n\n');
            fprintf('Parameters:\n');
            fprintf('  A (coefficient):  %.6f\n', A);
            fprintf('  α (exponent):     %.4f\n\n', alpha);
            fprintf('Interpretation:\n');
            
            if alpha < 1.0
                fprintf('  ✓ SUB-LINEAR:   Energy scales SLOWER than complexity\n');
                fprintf('                  (GOOD: improved efficiency at scale)\n');
            elseif alpha < 1.5
                fprintf('  ~ LINEAR:       Energy scales WITH complexity\n');
                fprintf('                  (EXPECTED: typical numerical methods)\n');
            else
                fprintf('  ✗ SUPER-LINEAR: Energy scales FASTER than complexity\n');
                fprintf('                  (BAD: reduced efficiency at scale)\n');
            end
            
            fprintf('\nModel Quality:\n');
            fprintf('  R² (fit quality):  %.4f', obj.r_squared);
            if obj.r_squared > 0.95
                fprintf(' (Excellent)\n');
            elseif obj.r_squared > 0.80
                fprintf(' (Good)\n');
            elseif obj.r_squared > 0.60
                fprintf(' (Fair)\n');
            else
                fprintf(' (Poor)\n');
            end
            
            fprintf('%s\n\n', repmat('=', 1, 70));
        end
        
        function E_pred = predict_energy(obj, complexity)
            % Predict energy consumption for given complexity
            %
            % Args:
            %   complexity (float or array): Complexity value(s)
            %
            % Returns:
            %   E_pred (float or array): Predicted energy (Joules)
            
            if isempty(obj.scaling_model)
                error('Build model first using build_scaling_model()');
            end
            
            A = obj.scaling_model.A;
            alpha = obj.scaling_model.alpha;
            
            E_pred = A * complexity.^alpha;
        end
        
        function sustainability_metrics = compute_sustainability_metrics(obj)
            % Compute efficiency metrics from data
            %
            % Returns:
            %   sustainability_metrics (struct): Various efficiency metrics
            
            if isempty(obj.data_points)
                error('No data points available');
            end
            
            C = obj.data_points(:, 1);
            E = obj.data_points(:, 2);
            
            sustainability_metrics = struct();
            
            % Energy efficiency (Joules per unit complexity)
            efficiency = E ./ C;
            sustainability_metrics.efficiency = efficiency;
            sustainability_metrics.efficiency_mean = mean(efficiency);
            sustainability_metrics.efficiency_trend = (efficiency(end) - efficiency(1)) / efficiency(1) * 100;
            
            % Total energy consumption
            sustainability_metrics.total_energy_joules = sum(E);
            sustainability_metrics.total_energy_kwh = sum(E) / 3.6e6;
            
            % Average complexity
            sustainability_metrics.avg_complexity = mean(C);
            
            % Energy per unit work (energy/flops would require FLOP count)
            sustainability_metrics.min_efficiency = min(efficiency);
            sustainability_metrics.max_efficiency = max(efficiency);
            
            % Sustainability score (0-100): how close to linear scaling
            if ~isempty(obj.scaling_model)
                alpha = obj.scaling_model.alpha;
                % Linear (α=1) is baseline. 0 < α < 1 is better
                % Score: 100 if α = 0 (constant), 80 if α = 0.5, 0 if α > 2
                sustainability_metrics.sustainability_score = max(0, 100 * (2 - alpha) / 2);
            end
            
            % Carbon footprint estimate (rough: ~0.5 kg CO2 per kWh for grid electricity)
            co2_per_kwh = 0.5;
            sustainability_metrics.co2_emissions_kg = sustainability_metrics.total_energy_kwh * co2_per_kwh;
            
            fprintf('\n%s\n', repmat('=', 1, 70));
            fprintf('SUSTAINABILITY METRICS\n');
            fprintf('%s\n', repmat('=', 1, 70));
            fprintf('\nEnergy Consumption:\n');
            fprintf('  Total:            %.1f J (%.3f kWh)\n', ...
                sustainability_metrics.total_energy_joules, ...
                sustainability_metrics.total_energy_kwh);
            fprintf('  CO2 Emissions:    %.3f kg CO2\n', ...
                sustainability_metrics.co2_emissions_kg);
            
            fprintf('\nEfficiency Metrics:\n');
            fprintf('  Avg Efficiency:   %.3f J/unit complexity\n', ...
                sustainability_metrics.efficiency_mean);
            fprintf('  Min Efficiency:   %.3f J/unit\n', ...
                sustainability_metrics.min_efficiency);
            fprintf('  Max Efficiency:   %.3f J/unit\n', ...
                sustainability_metrics.max_efficiency);
            fprintf('  Efficiency Trend: %+.1f%% (trend over runs)\n', ...
                sustainability_metrics.efficiency_trend);
            
            if isfield(sustainability_metrics, 'sustainability_score')
                fprintf('\nSustainability Score: %.1f / 100\n', ...
                    sustainability_metrics.sustainability_score);
            end
            
            fprintf('%s\n\n', repmat('=', 1, 70));
        end
        
        function fig = plot_scaling(obj, varargin)
            % Plot energy scaling relationship with model
            %
            % Optional args: ('title', value), ('xlabel', value), etc.
            
            if isempty(obj.data_points)
                error('No data points to plot');
            end
            
            % Parse optional arguments
            p = inputParser;
            addParameter(p, 'title', 'Energy vs Computational Complexity', @isstring);
            addParameter(p, 'xlabel', 'Complexity (Grid Points)', @isstring);
            addParameter(p, 'ylabel', 'Energy (Joules)', @isstring);
            addParameter(p, 'figsize', [1200 800], @isnumeric);
            parse(p, varargin{:});
            
            C = obj.data_points(:, 1);
            E = obj.data_points(:, 2);
            
            fig = figure('Name', 'Energy Scaling Analysis');
            set(fig, 'Position', [100 100 p.Results.figsize(1) p.Results.figsize(2)]);
            
            % Plot 1: Linear scale
            subplot(2, 2, 1);
            plot(C, E, 'o-', 'LineWidth', 2, 'MarkerSize', 8);
            grid on;
            xlabel(p.Results.xlabel);
            ylabel(p.Results.ylabel);
            title('Linear Scale');
            
            % Add model fit if available
            if ~isempty(obj.scaling_model)
                hold on;
                C_range = linspace(min(C), max(C), 100);
                E_fit = obj.predict_energy(C_range);
                plot(C_range, E_fit, '--', 'LineWidth', 2, 'DisplayName', ...
                    sprintf('Fit: E=%.3f·C^{%.3f}', obj.scaling_model.A, obj.scaling_model.alpha));
                legend;
                hold off;
            end
            
            % Plot 2: Log-log scale
            subplot(2, 2, 2);
            loglog(C, E, 'o-', 'LineWidth', 2, 'MarkerSize', 8);
            grid on;
            xlabel(p.Results.xlabel);
            ylabel(p.Results.ylabel);
            title('Log-Log Scale (Power Law)');
            
            if ~isempty(obj.scaling_model)
                hold on;
                C_range = linspace(min(C), max(C), 100);
                E_fit = obj.predict_energy(C_range);
                loglog(C_range, E_fit, '--', 'LineWidth', 2);
                legend('Data', 'Power-law fit');
                hold off;
            end
            
            % Plot 3: Efficiency trend
            subplot(2, 2, 3);
            efficiency = E ./ C;
            plot(1:length(C), efficiency, 'o-', 'LineWidth', 2, 'MarkerSize', 8);
            grid on;
            xlabel('Run Index');
            ylabel('Energy Efficiency (J / unit complexity)');
            title('Efficiency Trend');
            
            % Plot 4: Residuals (if model exists)
            subplot(2, 2, 4);
            if ~isempty(obj.scaling_model)
                E_pred = obj.predict_energy(C);
                resid_vals = E - E_pred;
                plot(C, resid_vals, 'o', 'MarkerSize', 8);
                hold on;
                plot(C, zeros(size(C)), '--k', 'LineWidth', 1);
                hold off;
                grid on;
                xlabel(p.Results.xlabel);
                ylabel('Residual (J)');
                title(sprintf('Model Residuals (R² = %.4f)', obj.r_squared));
            else
                text(0.5, 0.5, 'Build model to see residuals', ...
                    'HorizontalAlignment', 'center', 'VerticalAlignment', 'center');
                axis off;
            end
            
            sgtitle(p.Results.title);
        end
        
        function report = generate_sustainability_report(obj, output_file)
            % Generate comprehensive sustainability report
            %
            % Args:
            %   output_file (string, optional): File to save report
            %
            % Returns:
            %   report (struct): Report data
            
            report = struct();
            report.timestamp = datetime('now');
            report.num_datapoints = size(obj.data_points, 1);
            
            % Scaling model
            if ~isempty(obj.scaling_model)
                report.scaling_model = obj.scaling_model;
                report.r_squared = obj.r_squared;
            end
            
            % Metrics
            report.metrics = obj.compute_sustainability_metrics();
            report.data_points = obj.data_points;
            
            % Save to JSON if specified
            if nargin > 1 && strlength(output_file) > 0
                try
                    json_str = jsonencode(report);
                    fid = fopen(output_file, 'w');
                    fprintf(fid, '%s', json_str);
                    fclose(fid);
                    fprintf('Report saved: %s\n', output_file);
                catch ME
                    warning(ME.identifier, 'Failed to save report: %s', ME.message);
                end
            end
        end
        
        function summary_table = summarize_data(obj)
            % Create summary table of all data points
            %
            % Returns:
            %   summary_table (table): Summary with complexity, energy, efficiency
            
            if isempty(obj.data_points)
                error('No data points');
            end
            
            C = obj.data_points(:, 1);
            E = obj.data_points(:, 2);
            efficiency = E ./ C;
            
            summary_table = table(C, E, efficiency, ...
                'VariableNames', {'Complexity', 'Energy_J', 'Efficiency_J_per_unit'});
            
            disp(summary_table);
        end
    end
end


%% Example Usage
% ============================================
%
% % Create analyzer
% analyzer = EnergySustainabilityAnalyzer();
%
% % Add measurements from simulation runs
% analyzer.add_data_from_log('../../sensor_logs/EVOLUTION_32x32_20260127_120000_sensors.csv', 32^2);
% analyzer.add_data_from_log('../../sensor_logs/EVOLUTION_64x64_20260127_120500_sensors.csv', 64^2);
% analyzer.add_data_from_log('../../sensor_logs/EVOLUTION_128x128_20260127_121000_sensors.csv', 128^2);
%
% % Build power-law model
% analyzer.build_scaling_model();
%
% % Compute sustainability metrics
% metrics = analyzer.compute_sustainability_metrics();
%
% % Plot results
% fig = analyzer.plot_scaling('title', 'Vorticity Solver Energy Scaling');
%
% % Predict energy for new complexity
% E_256x256 = analyzer.predict_energy(256^2);
% fprintf('Predicted energy for 256×256 grid: %.1f J\n', E_256x256);
%
% % Generate report
% analyzer.generate_sustainability_report('sustainability_report.json');
% ============================================
