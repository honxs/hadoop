package cn.mastercom.bigdata.locuser_v3;

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
    public Map<Integer, EGTable> egtable = new HashMap<Integer, EGTable>(); //用于保存二维矩阵的grid
    //add by yht 一维矩阵
    List<ArrayList<FingerGrid>> fggrid = new ArrayList<ArrayList<FingerGrid>>(); //用来保存一维矩阵的grid
    public int ispecialdeal = 0; //特殊处理，即找不到grid的情况下，强行弄一个最近的

    public  RsrpTable()
    {
        InitFgtable();
    }
    //add by yht 一维矩阵
    public void InitFgtable()
    {
        if (fggrid.size() > 0)
        {
            return;
        }
        for (int ii = 0; ii < (-60 + 120 + 1); ii++)
        {
        	ArrayList<FingerGrid> fglist = new ArrayList<FingerGrid>();
            fggrid.add(fglist);
        }
    }
    //add by yht 增加获取一维矩阵
    public void SetGrid(double srsrp, FingerGrid fg)
    {
        double ssrsrp = srsrp;
        if (ssrsrp > -60)
        {
            ssrsrp = -60;
        }
        else if (ssrsrp <= -120)
        {
            ssrsrp = -120;
        }
        int lindex = (int)((ssrsrp) * (-1)) - 60;
        if (lindex < 0 || lindex >= fggrid.size())
        {
            return;
        }
        fggrid.get(lindex).add(fg);
    }
    public ArrayList<FingerGrid> GetGrid(double srsrp)
    {
        // -30~-180
        if (srsrp > -30 || srsrp < -180)
        {
            return null;
        }

        int ssrsrp = (int)srsrp;
        if (ssrsrp > -54)
        {
            ssrsrp = -54;
        }
        else if (ssrsrp <= -126)
        {
            ssrsrp = -126;
        }
        ssrsrp = ssrsrp * (-1) - 60;
        int lindex_s = ssrsrp - 6;
        int lindex_e = ssrsrp + 6;
        if (lindex_s < 0)
        {
            lindex_s = 0;
        }
        if (lindex_e >= 60)
        {
            lindex_e = 60;
        }


        ArrayList<FingerGrid> lfgrid = new ArrayList<FingerGrid>();

        for (int ii = lindex_s; ii <= lindex_e; ii++)
        {
            lfgrid.addAll(fggrid.get(ii));
        }
        if (lfgrid.size() == 0)
        {
            for (int ii = lindex_e + 1; ii <= fggrid.size() - 1; ii++)
            {
                lfgrid.addAll(fggrid.get(ii));
                if (lfgrid.size() == 0)
                {
                    continue;
                }
                else
                {
                    //特殊处理标识
                    ispecialdeal = 1;
                    break;
                }
            }
        }
        if (lfgrid.size() == 0)
        {
            return null;
        }
        return lfgrid;
    }
    
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

    //获取二维矩阵
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
            //如果二维矩阵获取不到，就获取一维矩阵的
            return GetGrid(srsrp);
        }

        EGTable gt = egtable.get(neci);
        
        ArrayList<FingerGrid> fgrid = null;

        for (double ii = ssrsrp - 6; ii < ssrsrp + 6; ii += 2)
        {
            for (double jj = nnrsrp - 10; jj < nnrsrp + 15; jj += 2)
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
		//如果二维矩阵获取不到，就获取一维矩阵的
		if (fgrid == null)
		{
		    fgrid = GetGrid(srsrp);
		}
         
        return fgrid;
    }
}
