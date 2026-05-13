function Plot_Saver(Current_Figure, File_Name, Resolution_Preset)
%PLOT_SAVER Save figure to disk with DPI presets; auto-creates folders.
%   Plot_Saver(fig, File_Name, Resolution_Preset)
%   - File_Name can include subfolders and/or extension.
%   - Resolution_Preset: Laptop|Secondary|High|Low (default Laptop).

    if nargin < 3 || isempty(Resolution_Preset)
        Resolution_Preset = "Laptop";
    end

    % Resolve DPI preset from canonical style registry.
    DPI = PlotStyleRegistry.resolve_dpi(Resolution_Preset);

    % Determine output path and ensure extension
    % Convert to char array to ensure consistency
    File_Name = char(File_Name);
    [parent, name, ext] = fileparts(File_Name);
    
    if isempty(ext)
        ext = '.png';
    end
    if isempty(parent)
        parent = 'Figures';
    end
    
    out_dir = parent;
    if ~isfolder(out_dir)
        mkdir(out_dir);
    end
    
    % Construct file path using proper concatenation
    File_Path = fullfile(out_dir, [name ext]);

    exportgraphics(Current_Figure, File_Path, "Resolution", DPI);
end
