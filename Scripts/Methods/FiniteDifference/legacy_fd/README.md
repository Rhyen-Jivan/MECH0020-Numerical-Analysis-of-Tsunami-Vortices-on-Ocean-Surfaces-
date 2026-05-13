# legacy_fd

## Purpose
Archived legacy finite-difference implementation retained for reference and parity checks.

## MATLAB Files
- Finite_Difference_Analysis.m

## Notes
- Keep files in this directory focused on one responsibility area.
- Prefer shared infrastructure/helpers over duplicating logic in multiple locations.
- Legacy live-monitor helpers for this monolith are archived with the
  legacy monitor support files.
- `PathSetup.pruned_genpath()` keeps this tree off the active runtime path.
