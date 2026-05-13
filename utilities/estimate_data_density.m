function Density = estimate_data_density(Ax, Legend_Pos, Padding)
    Lines = findobj(Ax, "Type", "Line");
    Density = 0;
    if isempty(Lines)
        return
    end
    AxPos = Ax.Position;
    Lx1 = Legend_Pos(1) - Padding*Legend_Pos(3);
    Lx2 = Legend_Pos(1) + (1+Padding)*Legend_Pos(3);
    Ly1 = Legend_Pos(2) - Padding*Legend_Pos(4);
    Ly2 = Legend_Pos(2) + (1+Padding)*Legend_Pos(4);
    for i = 1:numel(Lines)
        X = Lines(i).XData;
        Y = Lines(i).YData;
        if numel(X) > 200
            Step = ceil(numel(X)/200);
            X = X(1:Step:end);
            Y = Y(1:Step:end);
        end
        Px = AxPos(1) + AxPos(3)*(X - Ax.XLim(1)) / diff(Ax.XLim);
        Py = AxPos(2) + AxPos(4)*(Y - Ax.YLim(1)) / diff(Ax.YLim);
        Mask = (Px >= Lx1) & (Px <= Lx2) & (Py >= Ly1) & (Py <= Ly2);
        Density = Density + sum(Mask);
    end
end