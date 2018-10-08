package cn.mastercom.bigdata.locuser_v2;

import java.util.HashMap;
import java.util.Map;

public class FingerLevel
{
    public String fkey = "";
    // 不同楼层
    public Map<Integer, FingerGrid> leveldic = new HashMap<Integer, FingerGrid>();

    public FingerLevel(String ftype, int xx, int yy)
    {
        fkey = ftype + "," + String.valueOf(xx) + "," + String.valueOf(yy);
    }

    public void SetSCell(SimuGrid sg, FingerTable ft)
    {
    	FingerGrid fg = null;
        if (!leveldic.containsKey(sg.level))
        {
        	fg = new FingerGrid(fkey, sg.level);
        	fg.ft = ft;
            leveldic.put(sg.level, fg);
        }
        else
        {
        	fg = leveldic.get(sg.level);
        }

        fg.SetSCell(sg);
    }

    public FingerGrid AddNCell(SimuGrid sg)
    {
        if (leveldic.containsKey(sg.level))
        {
            FingerGrid fg = leveldic.get(sg.level);

            fg.SetNCell(sg);

            return fg;
        }

        return null;
    }

    public FingerGrid SetNCell(SimuGrid sg, FingerTable ft)
    {
    	FingerGrid fg = null;
        if (!leveldic.containsKey(sg.level))
        {
        	fg = new FingerGrid(fkey, sg.level);
        	fg.ft = ft;
            leveldic.put(sg.level, fg);
        }
        else
        {
        	fg = leveldic.get(sg.level);
        }

        fg.SetNCell(sg);

        return fg;
    }
}
