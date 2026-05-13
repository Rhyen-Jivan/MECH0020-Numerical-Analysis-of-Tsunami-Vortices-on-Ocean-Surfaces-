# Compatibility Infrastructure

## Purpose

Provides method/mode compatibility checking to prevent invalid simulation configurations.

## Main Module

### `compatibility_matrix.m`

**Single source of truth** for all method/mode compatibility rules.

```matlab
[status, reason] = compatibility_matrix(method, mode)
```

**Inputs:**
- `method` - Method name ('FD', 'Spectral', 'FV')
- `mode` - Mode name ('Evolution', 'Convergence', 'ParameterSweep', 'Plotting')

**Outputs:**
- `status` - One of:
  - `'supported'` - Fully implemented and tested
  - `'experimental'` - Implemented but not fully validated
  - `'blocked'` - Not compatible or not yet implemented
- `reason` - Explanation string (for `blocked` or `experimental` status)

## Compatibility Rules

### Finite Difference + All Modes
- ✅ **Evolution:** Supported
- ✅ **Convergence:** Supported
- ✅ **ParameterSweep:** Supported
- ✅ **Plotting:** Supported
- ⚠️ **VariableBathymetry:** Experimental

### Spectral + Modes
- ❌ **Evolution:** Blocked (not implemented)
- ❌ **Convergence:** Blocked (not implemented)
- ❌ **ParameterSweep:** Blocked (not implemented)
- ✅ **Plotting:** Supported (method-agnostic)
- ❌ **VariableBathymetry:** Blocked (incompatible with spectral)

### Finite Volume + Modes
- ❌ **Evolution:** Blocked (not implemented)
- ❌ **Convergence:** Blocked (not implemented)
- ❌ **ParameterSweep:** Blocked (not implemented)
- ✅ **Plotting:** Supported (method-agnostic)
- ⚠️ **VariableBathymetry:** Experimental

## Usage in Mode Scripts

Mode scripts SHOULD check compatibility early:

```matlab
function [Results, paths] = mode_evolution(Run_Config, Parameters, Settings)
    % Compatibility check
    [status, reason] = compatibility_matrix(Run_Config.method, 'Evolution');

    if strcmp(status, 'blocked')
        error('Incompatible: %s\nReason: %s', Run_Config.method, reason);
    elseif strcmp(status, 'experimental')
        warning('Experimental: %s\nReason: %s', Run_Config.method, reason);
    end

    % Continue with simulation...
end
```

## Usage in Driver

`Tsunami_Simulator.m` checks compatibility BEFORE launching any mode:

```matlab
[compat_status, compat_reason] = compatibility_matrix(method_name, mode_name);

switch lower(compat_status)
    case 'blocked'
        error('INCOMPATIBLE: %s + %s\nReason: %s', method_name, mode_name, compat_reason);
    case 'experimental'
        fprintf('⚠ WARNING: %s + %s is EXPERIMENTAL\n', method_name, mode_name);
        % Prompt user to continue or abort
    case 'supported'
        fprintf('✓ Compatibility confirmed\n');
end
```

## Updating Compatibility Rules

To add a new method or mode:

1. Edit `compatibility_matrix.m`
2. Add new cases to the appropriate switch statement
3. Set `status` and `reason` appropriately
4. Test with `Tsunami_Simulator` to ensure early blocking works

## Design Philosophy

**Fail Early, Fail Clearly**

- Block invalid combinations BEFORE simulation starts
- Provide clear error messages with actionable remediation
- Experimental combinations require explicit user consent
- Consistent enforcement across all entry points (Driver, Modes)
