# Convergence Mode

This directory contains components for intelligent adaptive mesh convergence studies.

## Components

- **AdaptiveConvergenceAgent.m** - Intelligent convergence study controller
  - Preflight testing to gather training data
  - Pattern recognition for grid refinement behavior
  - Adaptive jump factor computation
  - Physical quantity tracking (vorticity, enstrophy, velocity)
  - Cost-optimized convergence path selection

- **run_adaptive_convergence.m** - Standalone runner for the agent
  - Entry point for command-line convergence studies
  - Sets up paths and parameters
  - Creates agent and executes study

## Usage

### From MATLAB Command Line
```matlab
cd Scripts/Modes/Convergence
run_adaptive_convergence
```

### From UI
Select "Convergence" mode in the Configuration tab. The UI will route through ModeDispatcher which uses `Scripts/Modes/Convergence/mode_convergence.m` for standard convergence, or you can run the adaptive agent separately.

## Relationship to Other Modes

- **mode_convergence.m** (this directory): Standard grid-sweep convergence
- **AdaptiveConvergenceAgent.m** (this directory): Learning-based adaptive convergence

The adaptive agent is more sophisticated and learns from preflight tests, while `mode_convergence.m` does fixed grid doubling.

## Output

Results are saved under `Results/FD/Convergence/<study_id>/` including:
- Convergence trace CSV
- Preflight figures
- Final configuration recommendations
