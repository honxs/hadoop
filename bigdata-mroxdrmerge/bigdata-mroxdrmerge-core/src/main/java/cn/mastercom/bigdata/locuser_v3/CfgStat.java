package cn.mastercom.bigdata.locuser_v3;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import cn.mastercom.bigdata.mroxdrmerge.MainModel;

public class CfgStat
{
    private Map<Integer, HashMap<Integer, CellS>> cfgstat = new HashMap<Integer, HashMap<Integer, CellS>>();     
    
    public void Clear()
    {
        cfgstat.clear();
    }

    public boolean Init(ReportProgress rptProgress)
    {
    	rptProgress.writeLog(0, "cellstat count = " + String.valueOf(cfgstat.size()));
    	for (Entry<Integer, HashMap<Integer, CellS>> hm : cfgstat.entrySet())
		{
    		rptProgress.writeLog(0, "cellstat cityid = " + String.valueOf(hm.getKey()) + ", count = " + String.valueOf(hm.getValue().size()));
		}
		return true;
    }

    private boolean ReadStat(int cityid)
    {
    	boolean bo = ReadStat("STAT_OUT_ECI.txt", 0, cityid);
    	boolean bi = ReadStat("STAT_IN_ECI.txt", 1, cityid);            
        return ((cfgstat.size() > 0) && bo && bi) ;
    }

    private boolean ReadStat(String filename, int ntype,  int cityid)
    {
        HashMap<Integer, CellS> cin = null;

        if (cfgstat.containsKey(cityid))
        {
            cin = cfgstat.get(cityid);
        }
        else
        {
            cin = new HashMap<Integer, CellS>();

            cfgstat.put(cityid, cin);
        }
        
        String fname = MainModel.GetInstance().getAppConfig().getSimuLocConfigPath() + "/" + String.valueOf(cityid) + "/stat/" + filename;
        CfgInfo.rProgress.writeLog(0, fname);
        BufferedReader sr = CfgInfo.getReader(fname, null);
        
        if (sr == null)
        {
        	CfgInfo.rProgress.writeLog(0, "open error : " + fname);
        	return false;        	
        }
		
        boolean brt = true;
        
		try
		{	
            int i = 0;
            String sline = sr.readLine();
            while (sline != null)
            {
            	String[] recs = sline.split("\t", -1);
                if (recs.length != 63)
                {
                    sline = sr.readLine();
                    continue;
                }

                i = 0;
                try
                {
                    CellX cx = new CellX();

                    int eci = Integer.parseInt(recs[i++]);

                    cx.Kcount = Integer.parseInt(recs[i++]);
                    for (int kk = 0; kk < 61; kk++)
                    {
                        cx.K_m_n[kk] = Integer.parseInt(recs[i++]);
                    }

                    if (!cin.containsKey(eci))
                    {
                    	cin.put(eci, new CellS());
                    }

                    CellS cs = cin.get(eci);

                    CellOI coi = new CellOI();
                    coi.Init(cx);

                    if (ntype == 1)
                    {
                        cs.ci = coi;
                    }
                    else
                    {
                        cs.co = coi;
                    }
                }
                catch (Exception ee)
                {
                	CfgInfo.rProgress.writeLog(0, ee.getMessage());
                }

                sline = sr.readLine();
            }
		}
        catch (Exception ee)
        {
        	brt = false;
        	CfgInfo.rProgress.writeLog(0, ee.getMessage());
        }

		if (sr != null)
		{
			try
			{
				sr.close();					
			}
			catch (IOException e)
			{
				CfgInfo.rProgress.writeLog(0, e.getMessage());
			}
		}
		
		return brt;
    }

    public CellS GetStat(int cityid, int eci)
    {
        if (!cfgstat.containsKey(cityid))
        {
            ReadStat(cityid);
        }

        HashMap<Integer, CellS> cin = cfgstat.get(cityid);
        
        if (cin.containsKey(eci))
        {
            return cin.get(eci);
        }
        return null;
    }
}
