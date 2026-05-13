function opts = Plot_Defaults()
%PLOT_DEFAULTS Central defaults for plotting utilities.
%   opts = Plot_Defaults() returns a struct of reusable defaults used by
%   Plot_Format, Legend_Format, Plot_Saver, and Plot_Format_And_Save.

    opts = PlotStyleRegistry.defaults();
end
