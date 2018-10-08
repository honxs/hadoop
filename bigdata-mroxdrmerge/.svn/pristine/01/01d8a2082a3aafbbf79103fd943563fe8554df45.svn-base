package cn.mastercom.bigdata.locuser_v3;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FingerGrid
{
    public String fkey = "";
    public int longitude = 0;
    public int latitude = 0;
    public int buildingid = 0;
    public int isspec = 0;
    public FingerTable ft = null;
    public SimuGrid scell = null;
    public Map<Integer, SimuGrid> ncells = new HashMap<Integer, SimuGrid>();
    public Map<Integer, SmFreq> sarfcns = new HashMap<Integer, SmFreq>();

    public static int HITYPE = (0x00000001 << 13); // 高精度
    public static int ROADTY = (0x00000001 << 14);

    public FingerGrid(String levelkey, int level)
    {
        fkey = levelkey + "," + String.valueOf(level);
    }

    public void SetSCell(SimuGrid sg)
    {
        scell = sg;
        longitude = sg.longitude;
        latitude = sg.latitude;
        buildingid = sg.buildingid;
        isspec |= sg.isspec;
        if (!sarfcns.containsKey(sg.arfcn))
        {
            SmFreq sf = new SmFreq();
            sf.rsrp = sg.arfcnrsrp;
            sf.num[0] = sg.num[0];
            sf.num[1] = sg.num[1] - sg.num[0];
            sf.num[2] = sg.num[2] - sg.num[1];
            sf.num[3] = sg.num[3] - sg.num[2];
            sf.num[4] = sg.num[4] - sg.num[3];
            sf.num[5] = sg.num[5] - sg.num[4];
            sf.num[6] = sg.num[6] - sg.num[5];
            sarfcns.put(sg.arfcn, sf);
        }        
    }
    public void SetNCell(SimuGrid sg)
    {
        if (ncells.containsKey(sg.eci))
        {
            return;
        }
        ncells.put(sg.eci, sg);
        longitude = sg.longitude;
        latitude = sg.latitude;
        buildingid = sg.buildingid;
        isspec |= sg.isspec;
        if (!sarfcns.containsKey(sg.arfcn))
        {
            SmFreq sf = new SmFreq();
            sf.rsrp = sg.arfcnrsrp;
            sf.num[0] = sg.num[0];
            sf.num[1] = sg.num[1] - sg.num[0];
            sf.num[2] = sg.num[2] - sg.num[1];
            sf.num[3] = sg.num[3] - sg.num[2];
            sf.num[4] = sg.num[4] - sg.num[3];
            sf.num[5] = sg.num[5] - sg.num[4];
            sf.num[6] = sg.num[6] - sg.num[5];
            sarfcns.put(sg.arfcn, sf);
        }
    }

    public double GetPrint_cluster(MrSplice splice, RefInt ccount, CfgInfo cInfo, int maxcount, FingerN[][] necis)
    {
        ccount.value = 0;
        double frate = -1;
        double rsrp = 0;
        double srsrp = -1000000;

        double maxcomrsrp = -1000000; // 同站差值
        int maxcomeci = 0;

        double maxrsrp = -1000000; // 最强小区(主服邻区一起)
        SimuGrid maxsg = null;

        // 室外
        if (scell != null)
        {
            srsrp = splice.scell.cell.rsrp_avg;
            //1.服务小区信号出现概率；信号强度概率 * 概率权重系数
            frate = GetOccurRate_cluster(splice.cityid, srsrp, scell.rsrp, scell.buildingid, cInfo, scell.eci, splice.scell.cell.pciarfcn, necis);
            if (frate == 0)
            {
                return 0;
            }
            frate = frate * frate * frate;
            //3.服务小区最强信号概率；// 170626
			//frate = frate * GetMaxRsrpRate(srsrp, scell.maxrsrp);
			//if (frate == 0)
			//{
			//    return 0;
			//}
            maxrsrp = srsrp;
            maxsg = scell;
        }
        
        int ncur = splice.ncells.size();
        for (Map.Entry<Integer, MrPoint> kp : splice.ncells.entrySet()) 
        {
            if (maxcount - ccount.value > ncur)
            {
            	return 0.0001; //原先是0，更改为0.0001
            }

            ncur--;

            if (kp.getValue().cell.isindoor == 1)
            {
                continue;
            }

            if (!ncells.containsKey(kp.getKey()))
            {
                continue;
            }
            ccount.value++;

            SimuGrid sg = ncells.get(kp.getKey());
            rsrp = kp.getValue().cell.rsrp_avg;
            // 最强
            if (maxrsrp < rsrp)
            {
                maxrsrp = rsrp;
                maxsg = sg;
            }

            if (scell != null)
            {
                // 同站
                if (scell.eci / 256 == sg.eci / 256)
                {
                    if (rsrp > maxcomrsrp)
                    {
                        maxcomrsrp = rsrp;
                        maxcomeci = sg.eci;
                    }
                }
            }
            //4.所有邻区信号出现概率；
            double lorate = GetOccurRate_cluster(splice.cityid, rsrp, sg.rsrp, sg.buildingid, cInfo, sg.eci, kp.getValue().cell.pciarfcn, necis);
            frate = SetRate(frate, lorate);
            if (rsrp == splice.nocell.cell.rsrp_avg) //是最强的邻区 如果是最强的邻区，则再算一遍，将权重提升
            {
                frate = SetRate(frate, lorate);
            }
            if (frate == 0)
            {
                return 0;
            }
        }

        //2.最强小区最强信号概率；
        if (maxsg != null)
        {
        	frate = SetRate(frate, GetMaxRsrpRate(maxrsrp, maxsg.maxrsrp));        
	        if (frate == 0)
	        {
	            return 0;
	        }
        }
        
        //5.同站差值信号概率；
        if ((maxcomrsrp != -1000000) && (scell != null))
        {
            if (maxcomeci == scell.comeci1)
            {
                frate = frate * GetCommRate(srsrp - maxcomrsrp, scell.comeci1diff);
            }
            else if (maxcomeci == scell.comeci2)
            {
                frate = frate * GetCommRate(srsrp - maxcomrsrp, scell.comeci2diff);
            }

            if (frate == 0)
            {
                return 0;
            }
        }
        
        //到了这个地方，如果前面的还没有概率，则不用再费劲了
        if (frate <= 0)
        {
            return frate;
        }
        
        Map<Integer/*arfcn*/, ArrayList<CoFreq>> nfreqs = new HashMap<Integer, ArrayList<CoFreq>>();
        SetFreqs(nfreqs, splice);

        //7.小区顺序出现概率,不含主服
        frate = frate * GetSeqRate(nfreqs);
        if (frate == 0)
        {
            return 0;
        }

        //6.小区同频点最大概率,加了主服
        frate = frate * GetCofqRate(nfreqs, splice); 
        if (frate == 0)
        {
            return 0;
        }

        //8.后续信号测量不到的信号概率
        frate = frate * GetNosigRate(splice);
        if (frate == 0)
        {
            return 0;
        }
        
        // 经验限制：楼宇有室分的情况，如果最强小区场强<-95dB且切片邻区中无该楼室分信号，则不能定到该楼宇内
        frate = frate * Forbidden(splice);

        return frate;
    }
    //进行经验限制
    public double Forbidden(MrSplice splice)
    {    
        // 经验限制：楼宇有室分的情况，如果最强小区场强<-95dB且切片邻区中无该楼室分信号，则不能定到该楼宇内
    	if ((splice.scell.cell.rsrp_avg < -95) && (splice.scell.cell.rsrp_avg != -1000000))
        {
        	if ((buildingid > 0) && (splice.scell.cell.isindoor == 0))
            {
                List<Integer> becis = CfgInfo.ci.GetIn(splice.cityid, buildingid);
                if (becis.size() == 0)
                {
                    return 1;
                }
                else
                {
                    for (Integer beci : becis)
                    {
                        if (splice.ncells.containsKey(beci))
                        {
                            return 1;
                        }
                    }
                    return 0;
                }                    
            }
        }
        return 1;
    }
    public double GetPrint(MrSplice splice, RefInt ccount, CfgInfo cInfo, int maxcount, FingerN[][] necis)
    {
        ccount.value = 0;
        double frate = -1; //这里是初始值
        double rsrp = 0;
        double srsrp = -1000000;

        double maxcomrsrp = -1000000; // 同站差值
        int maxcomeci = 0;

        double maxrsrp = -1000000; // 最强小区(主服邻区一起)
        SimuGrid maxsg = null;
        
        // 室外
        if (scell != null)
        {
            srsrp = splice.scell.cell.rsrp_avg;
            //1.服务小区信号出现概率；信号强度概率 * 概率权重系数
            frate = GetOccurRate(splice.cityid, srsrp, scell.rsrp, scell.buildingid, cInfo, scell.eci, splice.scell.cell.pciarfcn, necis);
            if (frate == 0)
            {
                return 0;
            }
            frate = frate * frate * frate;
            //3.服务小区最强信号概率；// 170626
            //frate = frate * GetMaxRsrpRate(srsrp, scell.maxrsrp);
            //if (frate == 0)
            //{
            //    return 0;
            //}
            maxrsrp = srsrp;
            maxsg = scell;
        }
        
        int ncur = splice.ncells.size();
        for (Map.Entry<Integer, MrPoint> kp : splice.ncells.entrySet())
        {
            if (maxcount - ccount.value > ncur)
            {
                return 0;
            }

            ncur--;

            if ((kp.getValue().cell.isindoor == 1) || (kp.getValue().cell.eci < 0))
            {
                continue;
            }

            if (!ncells.containsKey(kp.getKey()))
            {
                frate = SetRate(frate, GetFingerNotExistsRate(kp.getValue().cell.rsrp_avg));
                continue;
            }
            ccount.value++;
 
            SimuGrid sg = ncells.get(kp.getKey());
            rsrp = kp.getValue().cell.rsrp_avg;
            // 最强
            if (maxrsrp < rsrp)
            {
                maxrsrp = rsrp;
                maxsg = sg;
            }

            if (scell != null)
            {
                // 同站
                if (scell.eci / 256 == sg.eci / 256)
                {
                    if (rsrp > maxcomrsrp)
                    {
                        maxcomrsrp = rsrp;
                        maxcomeci = sg.eci;
                    }
                }
            }
            //4.所有邻区信号出现概率；
            double lorate = GetOccurRate_cluster(splice.cityid, rsrp, sg.rsrp, sg.buildingid, cInfo, sg.eci, kp.getValue().cell.pciarfcn, necis);
            //针对室内站来说rate值可能还没有初始化
            frate = SetRate(frate, lorate);
            if (rsrp == splice.nocell.cell.rsrp_avg) //是最强的邻区 如果是最强的邻区，则再算一遍，将权重提升
            {
                frate = SetRate(frate, lorate);
            }
            if (frate == 0)
            {
                return 0;
            }
        }

        //2.最强小区最强信号概率；
        if (maxsg != null)
        {
            frate = SetRate(frate, GetMaxRsrpRate(maxrsrp, maxsg.maxrsrp));
            if (frate == 0)
            {
                return 0;
            }
        }

        //5.同站差值信号概率；
        if ((maxcomrsrp != -1000000) && (scell != null))
        {
            if (maxcomeci == scell.comeci1)
            {
                frate = SetRate(frate,  GetCommRate(srsrp - maxcomrsrp, scell.comeci1diff));
            }
            else if (maxcomeci == scell.comeci2)
            {
                frate = SetRate(frate,  GetCommRate(srsrp - maxcomrsrp, scell.comeci2diff));
            }

            if (frate == 0)
            {
                return 0;
            }
        }

        //到了这个地方，如果前面的还没有概率，则不用再费劲了
        if (frate <= 0)
        {
            return frate;
        }

        Map<Integer/*arfcn*/, ArrayList<CoFreq>> nfreqs = new HashMap<Integer, ArrayList<CoFreq>>();
        SetFreqs(nfreqs, splice);

        //7.小区顺序出现概率,不含主服
        frate = SetRate(frate, GetSeqRate(nfreqs));
        //frate = frate * GetSeqRate(ref nfreqs);
        if (frate == 0)
        {
            return 0;
        }

        //6.小区同频点最大概率,加了主服
        frate = SetRate(frate, GetCofqRate(nfreqs, splice));
        //frate = frate * GetCofqRate(ref nfreqs, ref splice); 
        if (frate == 0)
        {
            return 0;
        }

        //8.后续信号测量不到的信号概率
        frate = SetRate(frate, GetNosigRate(splice));
        //frate = frate * GetNosigRate(splice);
        if (frate == 0)
        {
            return 0;
        }

        // 经验限制：楼宇有室分的情况，如果最强小区场强<-95dB且切片邻区中无该楼室分信号，则不能定到该楼宇内
        //if ((maxrsrp < -95) && (maxrsrp != -1000000))
        //{
        //    if (buildingid > 0)
        //    {
        //        List<int> becis = CfgInfo.ci.GetIn(splice.cityid, buildingid);
        //        if (becis.Count == 0)
        //        {
        //            return frate;
        //        }
        //        else
        //        {
        //            foreach (int beci in becis)
        //            {
        //                if (splice.ncells.ContainsKey(beci))
        //                {
        //                    return frate;
        //                }
        //            }
        //            return 0;
        //        }
        //    }
        //}

        frate = frate * Forbidden(splice);
        return frate;            
    }
    
    private void SetFreqs(Map<Integer, ArrayList<CoFreq>> freqs, MrSplice splice)
    {
        for (MrPoint mp : splice.ncells.values())
        {
            int arfcn = mp.cell.pciarfcn % 65536;
            if (!freqs.containsKey(arfcn))
            {
                freqs.put(arfcn, new ArrayList<CoFreq>());
            }

            List<CoFreq> lcells = freqs.get(arfcn);
            CoFreq cf = new CoFreq();
            cf.eci = mp.cell.eci;
            cf.rsrp = mp.cell.rsrp_avg;
            lcells.add(cf);
        }

        for (ArrayList<CoFreq> kp : freqs.values())
        {
            if (kp.size() <= 1)
            {
                continue;
            }
            // 降序
    		Collections.sort(kp, new Comparator<CoFreq>()
    		{
    			public int compare(CoFreq x, CoFreq y)
    			{
    				double ss = y.rsrp - x.rsrp;
    				if (ss > 0)
    				{
    					return 1;
    				}
    				else if (ss == 0)
    				{
    					return 0;
    				}
    				
    				return -1;
    			}
    		});
        }
    }
    
    private double SetRate(double frate, double crate)
    {
        if (crate == 0)
        {
            return 0;
        }
        //这里是指如果外面输入的是一个初始值，则填新的值
        if (frate == -1)
        {
            frate = crate;
        }
        else
        {
            frate = frate * crate;
        }

        return frate;
    }

    private double GetMaxRsrpRate(double rsrp, double maxrsrp)
    {
        if (rsrp > -30 || maxrsrp > -30 || maxrsrp < -180 || rsrp < -180)
        {
            return 0;
        }
        
        double srsrp = rsrp;
        if (srsrp > -60)
        {
            srsrp = -60;
        }
        else if (srsrp <= -120)
        {
            srsrp = -119;
        }
        double smaxrsrp = maxrsrp;
        if (smaxrsrp > -60)
        {
            smaxrsrp = -60;
        }
        else if (smaxrsrp <= -120)
        {
            smaxrsrp = -119;
        }
        
        // 170626
        if (srsrp >= smaxrsrp)
        {
            return 1;
        }
                
        return ((srsrp - (-120)) * 1.0 / (smaxrsrp - (-120)));
    }
    private double GetCommRate(double ccomdiff, double scomdiff)
    {
        double comdiff = Math.abs(ccomdiff - scomdiff);

        if (comdiff < 4)
        {
            return 1;
        }
        else if (comdiff < 10)
        {
            return (((10 - comdiff) * 80.0 / 6 + 20) / 100.0);
        }
        else
        {
        	return 0.00001;
        }
    }
    private double GetOccurRate_cluster(int cityid, double rsrp, double simursrp, int buildid, CfgInfo cinfo, int eci, int pcifcn, FingerN[][] necis)
    {
        double ff = GetRsrpRate_cluster(rsrp, simursrp);

        return ff;
    }    
    private double GetOccurRate(int cityid, double rsrp, double simursrp, int buildid, CfgInfo cinfo, int eci, int pcifcn, FingerN[][] necis)
    {
        double ff = GetRsrpRate(rsrp, simursrp);
        
        return ff;
        /* GetWeightRate 返回1，下面的都没用了。涉及FingerTable129行
        if (ff == 0)
        {
            return 0;
        }
        
        CellS cs = null;
        int earfcn = pcifcn % 65536;
        int pci = pcifcn / 65536;
        if (pci < 505) // max pci is 504
        {
            if (necis[earfcn] != null)
            {
            	if (necis[earfcn][pci] != null)
            	{
            		cs = necis[earfcn][pci].cs;
            	}
            }
        }

        if (cs == null)
        {
            cs = CfgInfo.cs.GetStat(cityid, eci);
        }

        return ff * GetWeightRate(rsrp, buildid, cs);
        */
    }
    private double GetRsrpRate_cluster(double rsrp, double simursrp)
    {
        //有时候太大，或者是太小，都找不到对应的栅格；
        if (rsrp > -60)
        {
            rsrp = -60;
        }
        else if (rsrp < -125)
        {
            rsrp = -125;
        } 
        double rdiff = rsrp - simursrp;
        if (rdiff > 10)
        {
            //10~60  0.0001~0.01
            return (rdiff - 10) * 0.0099 / 50 + 0.0001;
        }
        else if (rdiff >= 4)
        {
            return (((10 - rdiff) * 80.0 / 6 + 19) / 100.0);
        }
        else if (rdiff > -4) //预留1个百分点出来
        {
            return (((4 - Math.abs(rdiff)) * 1.0 / 4 + 99) / 100.0);
        }
        else if (rdiff > -15)
        {
            return (((15 + rdiff) * 80.0 / 11 + 19) / 100.0);
        }
        else
        {
            //-15~-60  0.0001~0.01
            return (rdiff + 60) * 0.0099 / 45 + 0.0001;
        }
    }    
    private double GetRsrpRate(double rsrp, double simursrp)
    {
        //有时候太大，或者是太小，都找不到对应的栅格；
        if (rsrp > -60)
        {
            rsrp = -60;
        }
        else if (rsrp < -125)
        {
            rsrp = -125;
        }    	
        double rdiff = rsrp - simursrp;
        
        if (buildingid > 0)
        {
	        if (simursrp >= -105 && rdiff >= 8)
	        {
	            return 0;
	        }
        }
        
        if (rdiff > 10)
        {
            if ((rsrp > -50) && (simursrp >= -60))
            {
                return 1;
            }        	
            return 0;
        }
        else if (rdiff >= 4)
        {
            return (((10 - rdiff) * 80.0 / 6 + 19) / 100.0);
        }
        else if (rdiff > -4) //预留1个百分点出来
        {
        	return (((4 - Math.abs(rdiff)) * 1.0 / 4 + 99) / 100.0);
        }
        else if (rdiff > -15)
        {
            return (((15 + rdiff) * 80.0 / 11 + 19) / 100.0);
        }
        else
        {
            if ((rsrp < -125) && (simursrp <= -119))
            {
                return 1;
            }        	
            return 0;
        }
    }
    /*
    private double GetWeightRate(double k, int buildid, CellS cs)
    {
        // 170626
        return 1;        
        
        if (cs == null)
        {
            return 0;
        }

        //a5~a10~a15 系数为1；
        //a1~a5系数为0.2~1 线性递增
        //a15~a24系数为1~0.1线性递减
        if (buildid > 0)
        {
            if (cs.ci == null)
            {
                return 0;
            }

            return cs.ci.GetValue(k);
        }
        else
        {
            if (cs.co == null)
            {
                return 0;
            }

            return cs.co.GetValue(k);
        }        
    }*/
    public SimuGrid GetGrid()
    {
        if (scell != null)
        {
            return scell;
        }
        else
        {
            if (ncells.size() > 0)
                return ncells.entrySet().iterator().next().getValue();
            else
                return null;
        }
    }    

    public double GetCofqRate(Map<Integer, ArrayList<CoFreq>> freqs, MrSplice splice)
    {
        // 切片同频最强rsrp
        if (splice.scell != null)
        {
            int arf = splice.scell.cell.pciarfcn % 65536;
            if (!freqs.containsKey(arf))
            {
                freqs.put(arf, new ArrayList<CoFreq>());
            }
            List<CoFreq> lcf = freqs.get(arf);
            CoFreq cf = new CoFreq();
            cf.eci = splice.scell.cell.eci;
            cf.rsrp = splice.scell.cell.rsrp_avg;
            lcf.add(cf);                
    		Collections.sort(lcf, new Comparator<CoFreq>()
    		{
    			public int compare(CoFreq x, CoFreq y)
    			{
    				double ss = y.rsrp - x.rsrp;
    				if (ss > 0)
    				{
    					return 1;
    				}
    				else if (ss == 0)
    				{
    					return 0;
    				}
    				
    				return -1;
    			}
    		});
        }

        double frate = 1;
        double crate = 0;
        // (RSRP-(-120))/(仿真栅格最强RSRP-(-120))
        for (Map.Entry<Integer, ArrayList<CoFreq>> kp : freqs.entrySet())
        {
            if (sarfcns.containsKey(kp.getKey()))
            {
                double srsrp = sarfcns.get(kp.getKey()).rsrp;
                double krsrp = kp.getValue().get(0).rsrp;
                if (krsrp > -60)
                {
                    krsrp = -60;
                }
                else if (krsrp <= -120)
                {
                    krsrp = -119;
                }
                if (srsrp > -60)
                {
                    srsrp = -60;
                }
                else if (srsrp <= -120)
                {
                    srsrp = -119;
                }
                    
                if (krsrp >= srsrp)
                {
                    crate = 1;
                }
                else
                {
                    crate = (krsrp - (-120)) / (srsrp - (-120));
                }
                
                frate = frate * crate;
            }            
        }

        return frate;
    }

    public double GetSeqRate(Map<Integer, ArrayList<CoFreq>> freqs)
    {
        // 只算邻区
        double frate = 1;
        double crate = 0;
        int n = 0;
        int k = 0;
        for (ArrayList<CoFreq> kp : freqs.values())
        {
            if (kp.size() <= 1)
            {
                continue;
            }

            for (int m = 1; m < kp.size(); m++)
            {
                if (!ncells.containsKey(kp.get(m).eci))
                {
                    continue;
                }

                SimuGrid sg = ncells.get(kp.get(m).eci);

                k = GetCount(kp.get(m).rsrp);
                if (k < 0)
                {
                    n = 0;
                }
                else
                {
                    n = sg.num[k];
                }

                crate = 1 - (n - (3 + m)) * 0.02;
                if (crate > 1)
                {
                    crate = 1;
                }
                else if (crate < 0)
                {
                    crate = 0;
                }

                frate = frate * crate;
            }
        }
        
        return frate;
    }

    private int GetCount(double rsrp)
    {
        if (rsrp >= -80)
        {
            return -1;
        }

        if (rsrp < -100)
        {
            return 4;
        }

        return (6 - (int)((rsrp + 105) / 5) - 2);            
    }

    // 假如最弱频点场强为-89dB，则分析当前分段即-90，加上下一分段-95分段的所有的小区个数。
    // 假如是n个，则概率为：1-(n-3 )*0.02。（分段小于或等于-105的不算，即如果是-97dB，则
    // 只算-100分段的个数，如果是-101dB，就不用算了）
    // 仅针对切片邻区数<=1 的切片进行处理
    private double GetNosigRate(MrSplice splice)
    {
        if (splice.ncells.size() > 1 || splice.scell == null)
        {
            return 1;
        }

        double frate = 1;

        int sfreq = splice.scell.cell.pciarfcn % 65536;
        if (splice.ncells.size() == 1)
        {
        	MrPoint pt = splice.ncells.entrySet().iterator().next().getValue();
            int nfreq = pt.cell.pciarfcn % 65536;
            if (sfreq == nfreq)
            {
                double rsrp = 0;
                if (splice.scell.cell.rsrp_avg < pt.cell.rsrp_avg)
                {
                    rsrp = splice.scell.cell.rsrp_avg;
                }
                else
                {
                    rsrp = pt.cell.rsrp_avg;
                }
                frate = frate * GetRate(sfreq, rsrp);
            }
            else
            {
                frate = frate * GetRate(sfreq, splice.scell.cell.rsrp_avg);
                frate = frate * GetRate(nfreq, pt.cell.rsrp_avg);
            }
        }
        else
        {
            frate = frate * GetRate(sfreq, splice.scell.cell.rsrp_avg);
        }

        return frate;
    }

    private double GetRate(int freq, double rsrp)
    {
        if (rsrp < -100)
        {
            return 1;
        }
        double crate = 0;            

        int k = 0;
        int n = 0;

        if (!sarfcns.containsKey(freq))
        {
            return 1;
        }
        SmFreq sf = sarfcns.get(freq);

        k = GetNum(rsrp);
        n = sf.num[k];
        if (k <= 4)
        {
            n += sf.num[k + 1];
        }

        crate = 1 - (n - 3) * 0.02;
        if (crate > 1)
        {
            crate = 1;
        }
        else if (crate < 0)
        {
            crate = 0;
        }

        return crate;
    }

    private int GetNum(double rsrp)
    {
        if (rsrp >= -80)
        {
            return 0;
        }
        else
        {
            return (6 - (int)((rsrp + 105) / 5));
        }
    }
    // add by yht 如果指纹库里没有，则增加一个没有的概率
    private double GetFingerNotExistsRate(double rsrp)
    {
        double lo_rsrp = rsrp;
        if (lo_rsrp > -60)
        {
            lo_rsrp = -60;
        }
        else if (lo_rsrp < -120)
        {
            lo_rsrp = -120;
        }
        //统计数据 -60~-120 概率为 0.0001~0.01
        double lo_rate = 0;
        lo_rate = (lo_rsrp * (-1) + 60) * 0.0099 / 60 + 0.0001;
        return lo_rate;
    }    
}
