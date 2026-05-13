function cfg = UI_Layout_Config(app)
% UI_Layout_Config Canonical layout config entrypoint for UI surfaces.
%
% Usage:
%   cfg = UI_Layout_Config();        % test/tooling access
%   cfg = UI_Layout_Config(app);     % UIController runtime access

    if nargin >= 1 && isa(app, 'UIController')
        cfg = app.get_layout_config();
        return;
    end

    % No caller app provided: build once from the canonical UIController
    % configuration path, then cleanup the temporary controller.
    tmp = UIController();
    cleanup_obj = onCleanup(@() local_cleanup_controller(tmp)); %#ok<NASGU>
    cfg = tmp.layout_cfg;
end

function local_cleanup_controller(app)
    if isempty(app) || ~isvalid(app)
        return;
    end
    try
        app.cleanup();
    catch
        if isprop(app, 'fig') && ~isempty(app.fig) && isvalid(app.fig)
            delete(app.fig);
        end
    end
end
