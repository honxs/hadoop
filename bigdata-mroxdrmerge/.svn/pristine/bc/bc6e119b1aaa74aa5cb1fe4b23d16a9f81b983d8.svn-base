package cn.mastercom.bigdata.locuser;

import java.util.HashMap;
import java.util.Map;

public class CfgSimu
{    
	private CfgSimuFile cfile = new CfgSimuFile();
    public Map<String, GridData> ingrids = new HashMap<String, GridData>();
    public Map<String, GridData> otgrids = new HashMap<String, GridData>();    
    Map<String, Integer> figs = null;
    private int lastEci = 0;
    public void Clear()
    {
    	figs = null;
        ingrids.clear();
        otgrids.clear();
    }

    public void GetSimu(int eci, CfgInfo cf, Map<String, Integer> figss)
    {
    	if(eci == lastEci)
    		return;
    	
        Clear();

        figs = figss;
        // 取指纹库
        cfile.GetSimu(eci, cf, this);
        
        SetSimu();
        lastEci = eci;
    }
    
    private void SetSimu()
    {
        for (GridData ls : otgrids.values())
        {
            //ls.cells = ls.cells.OrderByDescending(o => o.Value.rsrp).ToDictionary(o => o.Key, p => p.Value);
            for(SimuData sd : ls.cells.values())
            {
                if (sd.isscell == 1)
                {
                    ls.scell = sd;
                    break;
                }
            }
        }
        
        for (GridData ls : ingrids.values())
        {
            //ls.cells = ls.cells.OrderByDescending(o => o.Value.rsrp).ToDictionary(o => o.Key, p => p.Value);
            for(SimuData sd : ls.cells.values())
            {
                if (sd.isscell == 1)
                {
                    ls.scell = sd;
                    break;
                }
            }
        }
    }
    
    public void SimuFallBack(Object sData)
    {
        GridData gd = (GridData)sData;
        SimuData sd = gd.scell;
        if (sd.isscell == 0)
        {
            gd.scell = null;
        }
        
        // 0319 过滤指纹库
        String sfig1 = String.valueOf(sd.ieci) + "_0_0";
        String sfig2 = "0_" + String.valueOf(sd.iearfcn) + "_" + String.valueOf(sd.ipci);
        
        if ((!figs.containsKey(sfig1)) && (!figs.containsKey(sfig2)))
        {
        	return;
        }        

        String sKey = String.valueOf(gd.ilongitude) + "-" + String.valueOf(gd.ilatitude) + "-" + String.valueOf(gd.bid) + "-" + String.valueOf(gd.level) + "-" + String.valueOf(gd.radius);

        Map<String, GridData> grids = null;
        
        if (gd.bid == -1)
        {
        	grids = otgrids;
        }
        else
        {
        	grids = ingrids;
        }        
        
        if (!grids.containsKey(sKey))
        {
        	grids.put(sKey, gd);
        }

        GridData ls = grids.get(sKey);
        if(ls.scell == null && gd.scell!=null)
        {
        	ls.scell = gd.scell;
        }

        int nKey = 0;
        if (sd.isscell == 1)
        {
            nKey = sd.ieci * (-1);
        }
        else
        {
            nKey = sd.ipci * 65536 + sd.iearfcn;
        }

        if (!ls.cells.containsKey(nKey))
        {
            ls.cells.put(nKey, sd);
        }
        else
        {
            SimuData sim = ls.cells.get(nKey);
            if (sim.rsrp < sd.rsrp)
            {
                ls.cells.put(nKey, sd);
            }
        }
    }
}
