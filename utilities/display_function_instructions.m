function txt = display_function_instructions(funcName)
    % DISPLAY_FUNCTION_INSTRUCTIONS
    %
    % Displays structured usage instructions for a framework utility function.
    % Intended for use in scripts, templates, and interactive sessions.
    %
    % Example:
    %   display_function_instructions("Plot_Format")

    if nargin < 1
        error("Function name must be provided.");
    end

    funcName = string(funcName);

    % -------------------------------------------------------------------------
    % Instruction database (authoritative, manual, framework-level)
    % -------------------------------------------------------------------------
    db = struct;

    % ===================== Plot_Format =====================
    db.Plot_Format = [
        "PLOT_FORMAT — Plot formatting utility"
        ""
        "Purpose:"
        "Applies a consistent visual standard to the current axes (gca)."
        "Centralises axis formatting, fonts, grids, and LaTeX interpreters."
        ""
        "Syntax:"
        "Plot_Format(X_Label_String, Y_Label_String, Title_String, FontSizes, Axis_LineWidth)"
        ""
        "Typical usage:"
        "plot(x, y, 'LineWidth', 1.3);"
        "Plot_Format('$t$', '$y$', 'Time response', 'Default', 1.2);"
        ""
        "Notes:"
        "• Operates on the current axes (gca)."
        "• Passing 'Default' applies font sizes {20, 20, 25}."
        "• Manual xlabel/ylabel/title calls are unnecessary."
        "• Major and minor grids are enabled by default."
    ];

    % ===================== Legend_Format =====================
    db.Legend_Format = [
        "LEGEND_FORMAT — Legend formatting utility"
        ""
        "Purpose:"
        "Automatically formats and places legends to minimise data overlap."
        ""
        "Syntax:"
        "Legend_Format(Legend_Entries, FontSize, Orientation, NumColumns, NumRows, AutoLocation, BoxOption, Padding)"
        ""
        "Typical usage:"
        "plot(x, y1); hold on;"
        "plot(x, y2); hold off;"
        "Legend_Format({'Case A','Case B'}, 18, 'vertical', 1, 2, true);"
        ""
        "Notes:"
        "• Legend placement is data-driven using density estimation."
        "• Supports vertical and horizontal layouts."
        "• Legend entries are rendered using LaTeX."
    ];

    % ===================== Plot_Saver =====================
    db.Plot_Saver = [
        "PLOT_SAVER — Figure export utility"
        ""
        "Purpose:"
        "Controls figure saving explicitly to support loop-based workflows."
        ""
        "Syntax:"
        "Plot_Saver(Current_Figure, File_Name, Save_Flag)"
        ""
        "Typical usage:"
        "Save_Flag = true;"
        "Plot_Saver(gcf, 'response_plot', Save_Flag);"
        ""
        "Notes:"
        "• Figures are saved only when Save_Flag is true."
        "• Output directory is created automatically if missing."
        "• File formats and extensions are handled internally."
    ];

    % ===================== AutoPlot =====================
    db.AutoPlot = [
        "AUTOPLOT — High-level plotting orchestrator"
        ""
        "Purpose:"
        "Automatically generates formatted, tiled plots from structured data."
        ""
        "Syntax:"
        "AutoPlot(DataStruct, FieldChain, Tile_Rows, Tile_Cols, File_BaseName, Resolution_Preset)"
        ""
        "Typical usage:"
        "AutoPlot(Data, {'Results','Sensor'}, 2, 2, 'sensor_plots', 'High');"
        ""
        "Notes:"
        "• Assumes explicit X and Y fields in the data structure."
        "• Applies legends selectively to avoid clutter."
        "• Returns figure, axes, line, and legend handles."
    ];

    % -------------------------------------------------------------------------
    % Dispatch
    % -------------------------------------------------------------------------
    if funcName == "all"

        keys = fieldnames(db);

        fullText = strings(0,1);

        for k = 1:numel(keys)
            fullText = [fullText; db.(keys{k}); ""]; %#ok<AGROW>
            fullText = [fullText; "--------------------------------------------------"; ""]; %#ok<AGROW>
        end

        txt = join(fullText, newline);
        fprintf("\n%s\n\n", txt);
        return
    end

    if ~isfield(db, funcName)
        error("No instructions available for function: %s", funcName);
    end

    lines = db.(funcName);
    txt = join(lines, newline);
    fprintf("\n%s\n\n", txt);


end
