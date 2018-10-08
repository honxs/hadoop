package cn.mastercom.bigdata.locuser_v2;

import java.util.ArrayList;
import java.util.List;

public class FingerTable
{
    private String ftype = "";
    private int fspan = 0; // 3000 or 300
    private int gsize = 0; // 10 or 5
    private int fsize = 0;

    private SimuFile sfile = null;
    // 经纬度二维矩阵
    private int[][] gridvector = null;
    private List<FingerLevel> gridlist = new ArrayList<FingerLevel>();
    // 指纹库中小区    
    public FingerN[][] necis = new FingerN[65536][]; 

    public FingerTable(String stype, String sname, String spath, int nspan, int nsize)
    {
        gsize = nsize;
        ftype = stype;
        fspan = nspan;
        fsize = fspan * 2 / gsize;
        sfile = new SimuFile(sname, spath);
        gridvector = new int[fsize][fsize];
    	for(int i = 0 ; i < gridvector.length; i++ )
    	{
    		for(int j = 0 ; j < gridvector[i].length; j++)
    		{
    			gridvector[i][j] = 0;
    		}
    	}
    }

    public void Clear()
    {
    	for(int i = 0 ; i < necis.length; i++ )
    	{
    		necis[i] = null;
    	}        
        gridlist.clear();
    	for(int i = 0 ; i < gridvector.length; i++ )
    	{
    		for(int j = 0 ; j < gridvector[i].length; j++)
    		{
    			gridvector[i][j] = 0;
    		}
    	}
    }

    public List<ArrayList<SimuGrid>> Init(MrSplice splice, CfgInfo cInfo, List<ArrayList<SimuGrid>> sgs, int nn)
    {
        // 取过就不取了
        if (gridlist.size() > 0)
        {
            return sgs;
        }    	

        InsertNeci(splice.cityid, splice.scell.cell, cInfo);
        
        if (sgs == null)
        {
            sgs = sfile.ReadFile(splice.cityid, splice.scell.cell.eci);
        }        
        if (sgs == null)
        {
            return sgs;
        }
        if (sgs.get(nn) == null)
        {
            return sgs;
        }        
        // out:所有格子为同一个小区
        // in: 格子为不同小区，存在相同位置的格子
        for (int ii = 0; ii < sgs.get(nn).size(); ii++)
        {
            FingerLevel fl = null;
            // 经度维度：x1按照定位精度圆整-x按照定位精度圆整+（3000/定位精度）
            // 纬度维度：y1按照定位精度圆整-y按照定位精度圆整+（3000/定位精度）
            int xx = sgs.get(nn).get(ii).longitude / (gsize * 100) - splice.scell.cell.longitude / (gsize * 100) + (fspan / gsize);
            int yy = sgs.get(nn).get(ii).latitude / (gsize * 90) - splice.scell.cell.latitude / (gsize * 90) + (fspan / gsize);

            if (xx < 0 || xx >= fsize || yy < 0 || yy >= fsize)
            {
                // 超出范围
                continue;
            }

            // 0为无值
            if (gridvector[xx][yy] == 0)
            {
                fl = new FingerLevel(ftype, xx, yy);
                // 实际位置 + 1
                gridvector[xx][yy] = gridlist.size() + 1;
                gridlist.add(fl);
            }
            else
            {
                // 实际位置 - 1
                fl = gridlist.get(gridvector[xx][yy] - 1);
            }

            if (ftype.equals("OUT_OUT") || ftype.equals("OUT_IN"))
            {
                fl.SetSCell(sgs.get(nn).get(ii), this);                    
            }
            else
            {
                fl.SetNCell(sgs.get(nn).get(ii), this);
            }
        }   
        return sgs;
    }

    public boolean InsertNeci(int cityid, Mrcell mc, CfgInfo cInfo)
    {
        int earfcn = mc.pciarfcn % 65536;
        int pci = mc.pciarfcn / 65536;
        
        if (earfcn < 0 || pci < 0)
        {
        	return true;
        }
        
        if (pci < 505) // max pci is 504
        {
            if (necis[earfcn] != null)
            {
                if (necis[earfcn][pci] != null)
                {
                    if (necis[earfcn][pci].ishit == 1)
                    {
                        return true;
                    }
                }
            }
            else
            {
                necis[earfcn] = new FingerN[505];                    
            }

            if (necis[earfcn][pci] == null)
            {
                necis[earfcn][pci] = new FingerN();
            }
            
            necis[earfcn][pci].ishit = 1;
			//if ((necis[earfcn][pci].cs == null) && (mc.eci > 0))
			//{            	
			//    necis[earfcn][pci].cs = CfgInfo.cs.GetStat(cityid, mc.eci);
			//}            
        }

        return false;
    }
    
    public List<ArrayList<SimuGrid>> AddCell(Mrcell mc, MrSplice splice, RsrpTable rtable, CfgInfo cInfo, List<ArrayList<SimuGrid>> sgs, int nn)
    {
        if (gridlist.size() == 0)
        {
        	return sgs;
        }

        // 取过就不取了
        if (InsertNeci(splice.cityid, mc, cInfo))
        {
        	return sgs;
        }

        if (sgs == null)
        {
            sgs = sfile.ReadFile(splice.cityid, mc.eci);
        }
        if (sgs == null)
        {
        	return sgs;
        }
        if (sgs.get(nn) == null)
        {
        	return sgs;
        }
        // 所有格子为同一个小区
        for (int ii = 0; ii < sgs.get(nn).size(); ii++)
        {
            int xx = sgs.get(nn).get(ii).longitude / (gsize * 100) - splice.scell.cell.longitude / (gsize * 100) + (fspan / gsize);
            int yy = sgs.get(nn).get(ii).latitude / (gsize * 90) - splice.scell.cell.latitude / (gsize * 90) + (fspan / gsize);

            if (xx < 0 || xx >= fsize || yy < 0 || yy >= fsize)
            {
                // 超出范围
                continue;
            }
            // 主服有的才要
            if (gridvector[xx][yy] > 0)
            {
                FingerLevel fl = gridlist.get(gridvector[xx][yy] - 1);

                FingerGrid fg = fl.AddNCell(sgs.get(nn).get(ii));

                if (fg != null)
                {
                    // 这里的scell肯定有
                    rtable.SetGrid(fg.scell.rsrp, sgs.get(nn).get(ii).eci, sgs.get(nn).get(ii).rsrp, fg);
                }
            }
        }
        return sgs;
    }
    
    public void GetGrid(List<FingerGrid> fgrids)
    {
        for (FingerLevel fgrid : gridlist)
        {
            fgrids.addAll(fgrid.leveldic.values());
        }
    }

    public void PrintVector()
    {
        String ss = "";
        for (int jj = 0; jj < 600; jj++)
        {
            ss = "";
            for (int kk = 0; kk < 600; kk++)
            {
                if (gridvector[jj][kk] == 0)
                    ss += "0";
                else
                {
                    int aa = gridlist.get(gridvector[jj][kk] - 1).leveldic.size();
                    if (aa > 9)
                    {
                        aa = 9;
                    }

                    ss += String.valueOf(aa);
                }
            }
            System.out.println(ss);
        }
    }
}
