function Plot_Format(X_Label_String, Y_Label_String, Title_String, FontSizes, Axis_LineWidth)
%PLOT_FORMAT Standard plot styling for LaTeX-ready figures.
%   Plot_Format(xlabel, ylabel, title, FontSizes, Axis_LineWidth)
%   - FontSizes: cell {xFont, yFont, titleFont} or "Default" (falls back to Plot_Defaults)
%   - Axis_LineWidth: numeric; defaults from Plot_Defaults if omitted/empty

    if nargin < 4
        FontSizes = [];
    end
    if nargin < 5
        Axis_LineWidth = [];
    end

    PlotStyleRegistry.Plot_Format_Compat( ...
        X_Label_String, Y_Label_String, Title_String, FontSizes, Axis_LineWidth);
end
