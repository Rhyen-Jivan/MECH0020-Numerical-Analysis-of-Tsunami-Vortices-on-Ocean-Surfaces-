function Legend_Handle = Legend_Format(Legend_Entries, FontSize, Orientation, NumColumns, NumRows, AutoLocation, BoxOption, Padding, LocationOverride)
%LEGEND_FORMAT Styled legend wrapper (deprecated path).
%   Delegates legend style policy to PlotStyleRegistry.

    if nargin < 2, FontSize = []; end
    if nargin < 3, Orientation = []; end
    if nargin < 4, NumColumns = []; end
    if nargin < 5, NumRows = []; end
    if nargin < 6, AutoLocation = []; end
    if nargin < 7, BoxOption = []; end
    if nargin < 8, Padding = []; end
    if nargin < 9, LocationOverride = []; end

    Legend_Handle = PlotStyleRegistry.Legend_Format_Compat( ...
        Legend_Entries, FontSize, Orientation, NumColumns, NumRows, ...
        AutoLocation, BoxOption, Padding, LocationOverride);
end
