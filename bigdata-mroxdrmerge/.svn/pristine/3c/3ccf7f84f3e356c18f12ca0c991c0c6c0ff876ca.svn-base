package cn.mastercom.bigdata.locuser_v3;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SamScsp
{
    public void GetScsp(DataUnit dataUnit, ReportProgress rptProgress)
    {
        rptProgress.writeLog(0, "小区数量：" + String.valueOf(dataUnit.eciUnits.size()));

        String sNum = String.valueOf(dataUnit.siCount());
        rptProgress.writeLog(0, "S1APID数量：" + sNum);

        long nCount = 0;
        for (EciUnit em : dataUnit.eciUnits.values())
        {
            //增加aoata指纹库设置
            em.aoataprint.SetSCell(em.eci);
            
            rptProgress.writeLog(0, "PeakValue..." + String.valueOf(em.eci));
            em.SetPeak();
            
            for (MrUser mu : em.muser.values())
            {
                if ((nCount++) % 2000 == 0)
                {
                    rptProgress.writeLog(-1, String.valueOf(nCount) + "-" + sNum);
                }

                List<MrSam> mm = mu.samples;
                // 按时间排序           
        		Collections.sort(mm, new Comparator<MrSam>()
        		{
        			public int compare(MrSam x, MrSam y)
        			{
        				return (x.itime - y.itime);
        			}
        		});

                // 取一个段落
                int ii = 0;
                int jj = 0;
                for (jj = 0; jj < mm.size() - 1; jj++)
                {
                    if (mm.get(jj + 1).itime - mm.get(jj).itime <= 20)
                    {
                        continue;
                    }

                    DoSection(mm, ii, jj, em, mu);

                    ii = jj + 1;
                }

                DoSection(mm, ii, jj, em, mu);
            }
            
            em.aoataprint.WritePrint();
        }

        rptProgress.writeLog(0, "切片数量：" + String.valueOf(dataUnit.spCount()));
    }

    private void DoSection(List<MrSam> mm, int spos, int epos, EciUnit eunit, MrUser mu)
    {
        // 构造矩阵表
    	Map<Integer/*小区*/, TimePoint/*全部聚合时间点*/> cells = new HashMap<Integer, TimePoint>();

    	MakeMatrix(cells, mm, spos, epos);

        if (cells.size() > 0)
        {
            ClusterPos loclusterpos = new ClusterPos();

            MrSec msc = new MrSec();
            mu.sections.add(msc);        	
            // 每个时间段判断所有小区的场强最大-最小 >= 8dB切为一个片
            int jj = 0;
            int kk = 0;
            RefInt ismoving = new RefInt();
            int nCount = epos - spos + 1; // 时间点数量
            boolean bNc = false; // 是否有邻区
            int nn = 0;
            for (kk = 0; kk < nCount; kk++)
            {
            	nn = kk;
                // 针对一个时间点，处理所有小区
                for (TimePoint cell : cells.values())
                {
                    //  只有一个点的这里不会为true
                    if (GetStatus(kk, cell) && bNc)
                    {
                        if (ismoving.value <= 2)
                        {
                            ismoving.value = 2;
                        }
                        // 里面清理maxmin
                    	DoSplice(jj, kk - 1, cells, mm, spos, epos, eunit, msc, ismoving, loclusterpos);
                        jj = kk;
                        // 要从当前点重新开始计算
                        kk = kk - 1;
                        
                        bNc = false;

                        break;
                    }
                }
                
                for (TimePoint cell : cells.values())
                {
                    if (cell.celltimes.get(nn).isscell == 0 && cell.celltimes.get(nn).cell.eci != -1)
                    {
                        bNc = true;
                        break;
                    }
                }
            }

            // 最后一段
            DoSplice(jj, kk - 1, cells, mm, spos, epos, eunit, msc, ismoving, loclusterpos);
            
			//if (ismoving.value == 2)
			//{
			//    if (!emc)
			//    {
			//        ismoving.value = 0;
			//    }
			//}
          
            for (int ii = 0; ii < msc.splices.size(); ii++)
            {
            	msc.splices.get(ii).moving = ismoving.value;
            }                
            msc.postinfo = loclusterpos.GetPos();
        }  
    }
    
    private boolean MakeMatrix(Map<Integer, TimePoint> mmcells, List<MrSam> mm, int spos, int epos)
    {
        //       time1 time2 ....
        //cell1
        //cell2
        //....    
    	boolean emc = false;
        for (int ii = spos; ii <= epos; ii++)
        {
            // scell
            InsertMatrix(mm.get(ii).scell, 1, mmcells, ii - spos, mm, spos, epos);
            // ncell
            double nrsrp = -1000000;
            
            for (Mrcell ncell : mm.get(ii).ncells)
            {
                // 未找到的小区,用pciarfcn，这里扔掉可能造成切片错误,取指纹库哪里扔掉
                if (ncell.eci == 0)
                {
                    ncell.eci = (-1) * ncell.pciarfcn;
                }

                InsertMatrix(ncell, 0, mmcells, ii - spos, mm, spos, epos);
                
                if (ncell.isindoor == 0)
                {
                    if (ncell.rsrp_cnt > 0)
                    {
                        if (ncell.rsrp_avg / ncell.rsrp_cnt > nrsrp)
                        {
                            nrsrp = ncell.rsrp_avg / ncell.rsrp_cnt;
                        }
                    }
                }
            }
            
            if (nrsrp != -1000000 && mm.get(ii).scell.rsrp_cnt > 0)
            {
                if (nrsrp > mm.get(ii).scell.rsrp_avg / mm.get(ii).scell.rsrp_cnt)
                {
                    emc = true;
                }
                else
                {
                    emc = false;
                }
            }
        }
        
        return emc;
    }

    private void InsertMatrix(Mrcell one, int isscell, Map<Integer, TimePoint> mmcells, int cpos, List<MrSam> mm, int spos, int epos)
    {
    	TimePoint ll = null;
        if (!mmcells.containsKey(one.eci))
        {
            // 所有时间位都设置上
            ll = new TimePoint();
            ll.eci = mm.get(spos).scell.eci;
            ll.s1apid = mm.get(spos).s1apid;
            
            for (int kk = spos; kk <= epos; kk++)
            {
                MrPoint lcc = new MrPoint();
                lcc.itime = mm.get(kk).itime; // 聚合时间点
                lcc.cell.eci = -1; // 用于判断是否为填充点
                ll.celltimes.add(lcc);
            }
            mmcells.put(one.eci, ll);
        }
        else
        {
        	ll = mmcells.get(one.eci);
        }
        // 当前时间点位置是cpos
        MrPoint lc = ll.celltimes.get(cpos);
        lc.ta = mm.get(cpos + spos).ta;
        lc.aoa = mm.get(cpos + spos).aoa;
        lc.sinr = mm.get(cpos + spos).sinr;
        lc.isroad = mm.get(cpos + spos).isroad;
        lc.isscell = isscell;
        lc.cell.btime = one.btime;
        lc.cell.etime = one.etime;
        lc.cell.eci = one.eci;
        lc.cell.pciarfcn = one.pciarfcn;
        lc.cell.isindoor = one.isindoor;
        lc.cell.longitude = one.longitude;
        lc.cell.latitude = one.latitude;
        lc.cell.buildingid = one.buildingid;
        lc.cell.ilevel = one.ilevel;
        lc.cell.rsrp_cnt = one.rsrp_cnt;
        lc.cell.rsrp_avg = one.rsrp_avg;
        lc.cell.rsrq_avg = one.rsrq_avg;
        lc.cell.rsrq_cnt = one.rsrq_cnt;
    }

    private boolean GetStatus(int bb, TimePoint tp)
    {
        if (tp.celltimes.get(bb).cell.eci == -1) // 填充点
        {
            return false;
        }

        if (tp.rsrp_max == -1000000)
        {
            if (tp.celltimes.get(bb).cell.rsrp_cnt > 0)
            {
                tp.rsrp_max = tp.celltimes.get(bb).cell.rsrp_avg / tp.celltimes.get(bb).cell.rsrp_cnt;
                tp.rsrp_min = tp.rsrp_max;
            }
        }
        else
        {
            if (tp.celltimes.get(bb).cell.rsrp_cnt > 0)
            {
                tp.rsrp_max = Math.max(tp.celltimes.get(bb).cell.rsrp_avg / tp.celltimes.get(bb).cell.rsrp_cnt, tp.rsrp_max);
                tp.rsrp_min = Math.min(tp.celltimes.get(bb).cell.rsrp_avg / tp.celltimes.get(bb).cell.rsrp_cnt, tp.rsrp_min);
            }

            if (Math.abs(tp.rsrp_max - tp.rsrp_min) > 8)
            {
                return true;
            }
        }

        return false;
    }

    private void DoSplice(int sp, int ep, Map<Integer, TimePoint> nls, List<MrSam> mm, int spos, int epos, EciUnit eunit, MrSec msc, RefInt ismoving, ClusterPos loclusterpos)
    {
        MrSplice splice = new MrSplice();
        splice.section_btime = mm.get(spos).itime;
        splice.section_etime = mm.get(epos).itime;        
        TimePoint mp = nls.entrySet().iterator().next().getValue();        
        splice.splice_btime = mp.celltimes.get(sp).itime;
        splice.splice_etime = mp.celltimes.get(ep).itime;
        splice.eci = mp.eci;
        splice.s1apid = mp.s1apid;
        splice.itime = splice.splice_btime;
        splice.cityid = mm.get(spos).cityid;
        if((mm.get(epos).ncells.size() > 0) && (ismoving.value == 2))
        {
            if (mm.get(epos).scell.rsrp_avg < mm.get(epos).ncells.get(0).rsrp_avg)
            {
                ismoving.value = 3; //指标有跳变，且最后的采样点的邻区场强大于服务小区场强
            }
        }
        splice.moving = ismoving.value;

        ClusterPos lclusterpos = new ClusterPos();

        for (int kk = spos; kk <= epos; kk++)
        {
            //设置aoata的指纹库学习
            eunit.aoataprint.Study(mm.get(kk));
            
            if ((mm.get(kk).longitude > 0) || mm.get(kk).org_loctp == "ll") //处理GPS定位的，有可能没有经纬度，但室外是确认的
            {
                lclusterpos.SetPos(mm.get(kk).org_loctp, mm.get(kk).org_indr, mm.get(kk).org_label, mm.get(kk).buildingid, mm.get(kk).longitude, mm.get(kk).latitude, mm.get(kk).ilevel);
            }
        }
        splice.posinfo = lclusterpos.GetPos();
        if (splice.posinfo != null)
        {
            splice.longitude = (int)splice.posinfo.ilongitude;
            splice.latitude = (int)splice.posinfo.ilatitude;
            splice.buildingid = splice.posinfo.iBuildingID;
            splice.ilevel = splice.posinfo.iLevel;
            splice.doortype = splice.posinfo.doortype;
        }
        
        Map<Integer, MrPoint> ncells = new HashMap<Integer, MrPoint>();
        
        for (TimePoint lm : nls.values())
        {
            // 清理切片统计
            lm.rsrp_max = -1000000;
            lm.rsrp_min = -1000000;

            MrPoint pcell = null;
            for (int ii = sp; ii <= ep; ii++)
            {
				//// 未找到的小区扔掉
				//if (lm.celltimes.get(ii).cell.eci < 0)
				//{
				//    continue;
				//}
                //邻区没有频点的过滤掉
                if (lm.celltimes.get(ii).cell.pciarfcn == 0)
                {
                    continue;
                }
                
                if (splice.ta == -1)
                {
                    splice.ta = lm.celltimes.get(ii).ta;
                }
                else if (splice.ta > lm.celltimes.get(ii).ta && lm.celltimes.get(ii).ta != -1)
                {
                    splice.ta = lm.celltimes.get(ii).ta;
                }

                if (lm.celltimes.get(ii).aoa != -1000000)
                {
                    splice.aoa += lm.celltimes.get(ii).aoa;
                    splice.naoa++;
                }

                if (lm.celltimes.get(ii).sinr != -1000000)
                {
                    if (splice.sinr == -1000000)
                    {
                        splice.sinr = lm.celltimes.get(ii).sinr;
                        splice.sinr_c = 1;
                    }
                    else
                    {
                        splice.sinr += lm.celltimes.get(ii).sinr;
                        splice.sinr_c++;
                    }
                }                
                
                splice.isroad |= lm.celltimes.get(ii).isroad;

                if (pcell == null)
                {
                    pcell = new MrPoint();

                    pcell.itime = lm.celltimes.get(ii).itime;
                    pcell.isscell = lm.celltimes.get(ii).isscell;
                    pcell.cell.btime = lm.celltimes.get(ii).cell.btime;
                    pcell.cell.etime = lm.celltimes.get(ii).cell.etime;
                    pcell.cell.isindoor = lm.celltimes.get(ii).cell.isindoor;
                    pcell.cell.longitude = lm.celltimes.get(ii).cell.longitude;
                    pcell.cell.latitude = lm.celltimes.get(ii).cell.latitude;
                    pcell.cell.buildingid = lm.celltimes.get(ii).cell.buildingid;
                    pcell.cell.ilevel = lm.celltimes.get(ii).cell.ilevel;
                    pcell.cell.eci = lm.celltimes.get(ii).cell.eci;
                    pcell.cell.pciarfcn = lm.celltimes.get(ii).cell.pciarfcn;
                    pcell.cell.rsrp_cnt = lm.celltimes.get(ii).cell.rsrp_cnt;
                    pcell.cell.rsrp_avg = lm.celltimes.get(ii).cell.rsrp_avg;
                    pcell.cell.rsrq_cnt = lm.celltimes.get(ii).cell.rsrq_cnt;
                    pcell.cell.rsrq_avg = lm.celltimes.get(ii).cell.rsrq_avg;
                    pcell.ta = lm.celltimes.get(ii).ta;
                    pcell.aoa = lm.celltimes.get(ii).aoa;
                }
                else
                {
                    pcell.cell.etime = lm.celltimes.get(ii).cell.etime;
                    pcell.cell.rsrp_cnt += lm.celltimes.get(ii).cell.rsrp_cnt;
                    if (pcell.cell.rsrp_avg == -1000000)
                        pcell.cell.rsrp_avg = lm.celltimes.get(ii).cell.rsrp_avg;
                    else
                    {
                        if (lm.celltimes.get(ii).cell.rsrp_avg != -1000000)
                            pcell.cell.rsrp_avg += lm.celltimes.get(ii).cell.rsrp_avg;
                    }
                    if (pcell.cell.rsrq_avg == -1000000)
                        pcell.cell.rsrq_avg = lm.celltimes.get(ii).cell.rsrq_avg;
                    else
                    {
                        if (lm.celltimes.get(ii).cell.rsrq_avg != -1000000)
                            pcell.cell.rsrq_avg += lm.celltimes.get(ii).cell.rsrq_avg;
                    }                    
                }
            }

            if (pcell != null)
            {
                // 直接算成平均值
                if (pcell.cell.rsrp_cnt > 0)
                {
                    pcell.cell.rsrp_avg = pcell.cell.rsrp_avg / pcell.cell.rsrp_cnt;
                }
                if (pcell.cell.rsrq_cnt > 0)
                {
                    pcell.cell.rsrq_avg = pcell.cell.rsrq_avg / pcell.cell.rsrq_cnt;
                }

                if (pcell.isscell == 1)
                {
                    splice.scell = pcell;
                }
                else
                {                	
                	// 不含主服
                    if (ncells.containsKey(pcell.cell.eci))
                    {
                        MrPoint mrp = ncells.get(pcell.cell.eci);
                        if (mrp.cell.rsrp_avg < pcell.cell.rsrp_avg)
                        {
                            ncells.put(pcell.cell.eci, pcell);
                        }
                    }
                    else
                    {
                        ncells.put(pcell.cell.eci, pcell);
                    }                	
                	
/*                    
                    splice.ncells.put(pcell.cell.eci, pcell);

                    if (pcell.cell.isindoor == 1)
                    {
                        if (splice.nicell == null)
                        {
                            splice.nicell = pcell;
                        }
                        else
                        {
                            // 最强室分
                            if (splice.nicell.cell.rsrp_avg < pcell.cell.rsrp_avg)
                            {
                                splice.nicell = pcell;
                            }
                        }
                    }
                    else
                    {
                        if (pcell.cell.rsrp_avg > -95)
                        {                                
                            splice.nout95++;
                        }
                        if (splice.nocell == null)
                        {
                            splice.nocell = pcell;
                        }
                        else
                        {
                            // 最强室外
                            if (splice.nocell.cell.rsrp_avg < pcell.cell.rsrp_avg)
                            {
                                splice.nocell = pcell;
                            }
                        }
                    }*/
                }
            }
        }
        
        if (splice.naoa > 0)
        {
            splice.aoa = splice.aoa / splice.naoa;
        }    
        
        // 20170909 切片保留最强5邻区
        List<MrPoint> lncells = new ArrayList<MrPoint>(ncells.values());
		Collections.sort(lncells, new Comparator<MrPoint>()
		{
			public int compare(MrPoint x, MrPoint y)
			{
				double ss = y.cell.rsrp_avg - x.cell.rsrp_avg;
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
        
        for (MrPoint pcell : lncells)
        {
            splice.ncells.put(pcell.cell.eci, pcell);
        
            if (pcell.cell.isindoor == 1)
            {
                if (splice.nicell == null)
                {
                    splice.nicell = pcell;
                }
                else
                {
                    // 最强室分
                    if (splice.nicell.cell.rsrp_avg < pcell.cell.rsrp_avg)
                    {
                        splice.nicell = pcell;
                    }
                }
            }
            else
            {
                if (pcell.cell.rsrp_avg > -95)
                {                                
                    splice.nout95++;
                }
                if (splice.nocell == null)
                {
                    splice.nocell = pcell;
                }
                else
                {
                    // 最强室外
                    //modified by yht 有可能最强邻区是识别不了的 
                    if ((splice.nocell.cell.rsrp_avg < pcell.cell.rsrp_avg) || (splice.nocell.cell.eci < 0 && pcell.cell.eci > 0))
                    {
                        splice.nocell = pcell;
                    }
                }
            }      
            
            if (splice.ncells.size() >= 5)
            {
                break;
            }            
        }
        ncells.clear();     
        
        if (splice.ncells.size() > 0)
        {
            // 主服室分不处理
            //modified by yht 最强邻区可能是不能识别的eci
            if (splice.nocell != null && splice.scell.cell.isindoor == 0 && splice.nocell.cell.eci > 0)
            {
                eunit.rtable.InitRtable(splice.nocell.cell.eci);
            }
        }   

        if (splice.scell != null)
        {
            if (splice.doortype == 0)
            {
                DoorType.SetDoorType(splice, eunit);
            }

            if (splice.doortype == 1)
            {
                msc.innum++;
            }
            else if (splice.doortype == 2)
            {
                msc.otnum++;
            }
            if (splice.posinfo != null)
            {
                loclusterpos.SetPos(splice.posinfo);
                msc.ottcnt++;
            }

            msc.splices.add(splice);

            //add by yht  将0,1,2的都加上
            if ((lncells.size() >= 2) && (msc.maxneighbournum < 2))
            {
                if (msc.splice != null)
                {
                    msc.splice.iscenter = 0;
                }
                msc.maxneighbournum = 2;
                msc.splice = splice;
                msc.splice.iscenter = 1;
            }
            else if ((lncells.size() == 1) && (msc.maxneighbournum < 1))
            {
                if (msc.splice != null)
                {
                    msc.splice.iscenter = 0;
                }
                msc.maxneighbournum = 1;
                msc.splice = splice;
                msc.splice.iscenter = 1; 
            }
            else if ((lncells.size() == 0) && (msc.maxneighbournum == 0) && (msc.splice == null))
            {
                msc.splice = splice;
                msc.splice.iscenter = 1;
            }

            //if (lncells.Count >= 2)
            //{
            //    if ((msc.splice == null) || (msc.splice != null && msc.splice.longitude <= 0 && splice.longitude > 0))
            //    {
            //        msc.splice = splice;
            //        msc.n1cell = lncells[0].Value;
            //        msc.n2cell = lncells[1].Value;
            //        msc.splice.iscenter = 1;
            //    }
            //}
        }
        lncells.clear();
    }
}
