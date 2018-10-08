package cn.mastercom.bigdata.locuser_v3;

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
    //add by yht,用于判断栅格的文件是否已经读过了，以免重复多次读取
    private int isfilereaded = 0;    
    // 指纹库中小区    
    public FingerN[][] necis = new FingerN[65536][]; 

    public int GetGridCount()
    {
        return gridlist.size();
    }
    
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

    public List<ArrayList<SimuGrid>> Init(MrSplice splice, CfgInfo cInfo, List<ArrayList<SimuGrid>> sgs, int nn, RsrpTable rtable)
    {
        // 取过就不取了
        if (gridlist.size() > 0)
        {
            return sgs;
        }    	
        if (isfilereaded == 1)
        {
            return null;
        }
        isfilereaded = 1; //add by yht 一个fingertable 只初始化一次gridlist
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
                //add by yht 用于应对所有的指纹库都超标的情况，构造一个虚无的中心点放进去
                if ((ii + 1 == sgs.get(nn).size()) && (gridlist.size() == 0))
                {
                   // Console.WriteLine("读取指纹失败,eci:" + splice.scell.cell.eci.ToString());
                    continue;
                }
                else
                {
                    // 超出范围
                    continue;
                }
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
                fl.SetSCell(sgs.get(nn).get(ii), this, rtable);                    
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

    public void GetGrid(List<FingerGrid> fgrids, MrSplice msp)
    {
        if (msp.longitude <= 0 || msp.latitude <= 0)
        {
            return;
        }

        int xx = msp.longitude / (gsize * 100) - msp.scell.cell.longitude / (gsize * 100) + (fspan / gsize);
        int yy = msp.latitude / (gsize * 90) - msp.scell.cell.latitude / (gsize * 90) + (fspan / gsize);

        if (xx < 0 || xx >= fsize || yy < 0 || yy >= fsize)
        {
            // 超出范围
            return;
        }
        int lo_range = 3;
        for (int ii = xx - lo_range; ii <= xx + lo_range; ii++)
        {
            if (ii < 0 || ii >= fsize)
            {
                continue;
            }

            for (int jj = yy - lo_range; jj <= yy + lo_range; jj++)
            {
                if (jj < 0 || jj >= fsize)
                {
                    continue;
                }

                if (gridvector[ii][jj] > 0)
                {
                    if ((msp.buildingid > 0) && (msp.ilevel > 0))//如果是室内的，则将楼宇内的整个所有楼层的栅格都弄上去了
                    {
                        for (int kk = msp.ilevel - 10; kk <= msp.ilevel + 10; kk += 5) //5默认为1层
                        {
                            if (gridlist.get(gridvector[ii][jj] - 1).leveldic.containsKey(kk))
                            {
                                FingerGrid lofingergrid = gridlist.get(gridvector[ii][jj] - 1).leveldic.get(kk);
                                if (lofingergrid != null)
                                {
                                    fgrids.add(lofingergrid);
                                }
                            }
                        }
                    }
                    else
                    {
                        fgrids.addAll(gridlist.get(gridvector[ii][jj] - 1).leveldic.values());
                    }
                }
            }
        }
    }

    public void PrintVector(int fsize)
    {
        String ss = "";
        for (int kk = fsize - 1; kk >= 0; kk--)
        {
            ss = "";
            for (int jj = 0; jj < fsize; jj++)
            {
                if (gridvector[jj][kk] == 0)
                    ss += " ";
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
