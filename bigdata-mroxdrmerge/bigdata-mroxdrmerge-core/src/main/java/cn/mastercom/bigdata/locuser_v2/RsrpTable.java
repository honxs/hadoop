package cn.mastercom.bigdata.locuser_v2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class RsrpTable
{
    // 最强邻小区数统计
    public Map<Integer, Integer> ecinum = new HashMap<Integer, Integer>();
    public Map<Integer, EGTable> egtable = new HashMap<Integer, EGTable>();

    public void Clear()
    {
        ecinum.clear();
        egtable.clear();
    }

    public void InitRtable(int neci)
    {
        if (!ecinum.containsKey(neci))
        {
            ecinum.put(neci, 1);
        }
        else
        {
        	int nn = ecinum.get(neci);
            ecinum.put(neci, nn + 1);
        }
    }

    public void InitTop()
    {
        egtable.clear();

        if (ecinum.size() == 0)
        {
            return;
        }
        
        List<Entry<Integer, Integer>> kv = new ArrayList<Entry<Integer, Integer>>(ecinum.entrySet());
        
		Collections.sort(kv, new Comparator<Entry<Integer, Integer>>()
		{
			public int compare(Entry<Integer, Integer> x, Entry<Integer, Integer> y)
			{
				return (y.getValue() - x.getValue());
			}
		});
		// top 80
        for (int ii = 0; ii < kv.size() && ii < 80; ii++)
        {
            egtable.put(kv.get(ii).getKey(), new EGTable());
        }

        ecinum.clear();
    }

    public void SetGrid(double srsrp, int neci, double nrsrp, FingerGrid fg)
    {
        if (!egtable.containsKey(neci))
        {
            return;
        }

        EGTable gt = egtable.get(neci);

        gt.SetGrid(fg, srsrp, nrsrp);            
    }

    public ArrayList<FingerGrid> GetGrid(double srsrp, int neci, double nrsrp)
    {
        // -30~-180
        if (srsrp > -30 || nrsrp > -30 || srsrp < -180 || nrsrp < -180)
        {
            return null;
        }

        double ssrsrp = srsrp;
        if (ssrsrp > -60)
        {
            ssrsrp = -60;
        }
        else if (ssrsrp <= -120)
        {
            ssrsrp = -120;
        }

        double nnrsrp = nrsrp;
        if (nnrsrp > -60)
        {
            nnrsrp = -60;
        }
        else if (nnrsrp <= -120)
        {
            nnrsrp = -120;
        }
        
        if (!egtable.containsKey(neci))
        {
            return null;
        }

        EGTable gt = egtable.get(neci);
        
        ArrayList<FingerGrid> fgrid = null;

         for (double ii = ssrsrp - 8; ii < ssrsrp + 10; ii += 2)
        {
            for (double jj = nnrsrp - 8; jj < nnrsrp + 10; jj += 2)
            {
            	ArrayList<FingerGrid> ff = gt.GetGrid(ii, jj);

                if (ff != null)
                {
                    if (fgrid == null)
                    {
                        fgrid = new ArrayList<FingerGrid>();
                    }

                    fgrid.addAll(ff);
                }
            }
        }           

        return fgrid;
    }
}
