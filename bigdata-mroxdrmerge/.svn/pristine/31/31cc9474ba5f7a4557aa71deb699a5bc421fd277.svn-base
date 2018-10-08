package cn.mastercom.bigdata.locuser_v2;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import cn.mastercom.bigdata.mroxdrmerge.MainModel;

public class CfgNCell
{
    private int cureci = 0;
    private Map<Integer, CellN> cfgncell = new HashMap<Integer, CellN>();
    
    public void Clear()
    {
        cureci = 0;
        cfgncell.clear();
    }

    public boolean Init(ReportProgress rptProgress)
    {
        // 动态加载
        Clear();
        return true;
    }

    public CellN GetNCell(int cityid, int eci, int pciearfcn)
    {
        if (eci != cureci)
        {
            Clear();

            if (!GetNCell(cityid, eci, null))
            {
                return null;
            }

            cureci = eci;
        }

        if (cfgncell.containsKey(pciearfcn))
        {
            return cfgncell.get(pciearfcn);
        }

        return null;
    }

    private boolean GetNCell(int cityid, int eci, ReportProgress rptProgress)
    {
        String fname = MainModel.GetInstance().getAppConfig().getSimuLocConfigPath() + "/" + String.valueOf(cityid) + "/ncell/" + "NEICONFIG_" + String.valueOf(eci) + ".txt";

        BufferedReader sr = CfgInfo.getReader(fname, rptProgress);
        if (sr == null)
        {
        	return false;        	
        }

		try
		{
            int i = 0;
            String sline = sr.readLine();
            while (sline != null)
            {
            	String[] recs = sline.split("\t", -1);
                if (recs.length != 8)
                {
                    sline = sr.readLine();
                    continue;
                }

                i = 0;

                try
                {
                    CellN cn = new CellN();

                    cn.eci = Integer.parseInt(recs[i++]);
                    cn.pci = Integer.parseInt(recs[i++]);
                    cn.arfcn = Integer.parseInt(recs[i++]);
                    cn.neci = Integer.parseInt(recs[i++]);
                    cn.longitude = (int)(Double.parseDouble(recs[i++]) * 10000000);
                    cn.latitude = (int)(Double.parseDouble(recs[i++]) * 10000000);
                    cn.isindoor = Integer.parseInt(recs[i++]);
                    cn.distance = Double.parseDouble(recs[i++]);

                    if (cn.arfcn != 0 && cn.eci != 0)
                    {
                        if (!cfgncell.containsKey(cn.pci * 65536 + cn.arfcn))
                        {
                            cfgncell.put(cn.pci * 65536 + cn.arfcn, cn);
                        }
                    }
                }
                catch (Exception ee)
                {
                    
                }
                sline = sr.readLine();
            }
		}         
        catch (Exception ee)
        {
        	cfgncell.clear();
        }

		if (sr != null)
		{
			try
			{
				sr.close();
			}
			catch (IOException e)
			{
				//e.printStackTrace();
			}
		}
		
        return cfgncell.size() > 0;
    }	
}
