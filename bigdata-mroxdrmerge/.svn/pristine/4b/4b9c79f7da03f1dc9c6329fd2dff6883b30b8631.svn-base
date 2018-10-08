package cn.mastercom.bigdata.locuser_v3;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class SamLocs
{
    public int GetAOADiff(int aoa1, int aoa2)
    {
        int diff = Math.abs(aoa1 - aoa2);
        if (diff > 730)
        {
            return -1; //表明其中有数据没有aoa
        }
        else if (diff > 360)
        {
            return 720 - diff;
        }
        else
        {
            return diff;
        }
    }
    //聚类无服务小区的
    private void Cluster_0(ArrayList<MrCluster> secs, ArrayList<MrSec> Mrsec_0, RefInt isecs_cnt_0, int diffline0)
    {
        //聚类邻区没有的
        for (int ii = 0; ii < Mrsec_0.size(); ii++)
        {
            // 之前已经有归宿了，略过
            if (Mrsec_0.get(ii).bloc > 0)
            {
                continue;
            }

            Mrsec_0.get(ii).bloc = 1;
            MrCluster lsc = new MrCluster();
            lsc.innum = Mrsec_0.get(ii).innum;
            lsc.otnum = Mrsec_0.get(ii).otnum;
            lsc.ottcnt = Mrsec_0.get(ii).ottcnt;
            lsc.sections.add(Mrsec_0.get(ii));
            secs.add(lsc);
            isecs_cnt_0.value++;

            for (int jj = ii + 1; jj < Mrsec_0.size(); jj++)
            {
                MrSec lmsc = Mrsec_0.get(ii);
                MrSec omsc = Mrsec_0.get(jj);
                if (omsc.bloc > 0)// 之前已经有归宿了，略过
                {
                    continue;
                }
                int liscombined = 0;
                for (MrSplice lmrsplice_ii : lmsc.splices)
                {
                    if (liscombined > 0)
                    {
                        break;
                    }
                    //根据里面的每一个splice进行对比
                    for (MrSplice lmrsplice : omsc.splices)
                    {
                        if (Math.abs(lmsc.splice.scell.cell.rsrp_avg - lmrsplice.scell.cell.rsrp_avg) <= diffline0)
                        {
                            if ((Math.abs(lmrsplice.ta - lmrsplice_ii.ta) <= 1 || Math.abs(lmrsplice.ta - lmrsplice_ii.ta) > 1000)
                                && (GetAOADiff(lmrsplice.aoa, lmrsplice_ii.aoa) <= 20)  //aoa包含了其中没有数据的情况                                     
                                )//再按照AOA+TA进行聚类
                            {
                                // 找到组织了
                                lsc.sections.add(omsc);
                                lsc.innum += omsc.innum;
                                lsc.otnum += omsc.otnum;
                                lsc.ottcnt += omsc.ottcnt;
                                omsc.bloc = 1;
                                break;
                            }
                            else
                            {
                                continue;
                            }
                        }
                    }
                }
            }
        }
    }
    //聚类一个邻区的
    private void Cluster_1(ArrayList<MrCluster> secs, ArrayList<MrSec> Mrsec_1, RefInt isecs_cnt_1, int diffline1)
    {
        //聚类邻区数只有1个的
        for (int ii = 0; ii < Mrsec_1.size(); ii++)
        {
            // 之前已经有归宿了，略过
            if (Mrsec_1.get(ii).bloc > 0)
            {
                continue;
            }

            Mrsec_1.get(ii).bloc = 1;
            MrCluster lsc = new MrCluster();
            lsc.innum = Mrsec_1.get(ii).innum;
            lsc.otnum = Mrsec_1.get(ii).otnum;
            lsc.ottcnt += Mrsec_1.get(ii).ottcnt;
            lsc.sections.add(Mrsec_1.get(ii));
            secs.add(lsc);
            isecs_cnt_1.value++;

            for (int jj = ii + 1; jj < Mrsec_1.size(); jj++)
            {
                MrSec lmsc = Mrsec_1.get(ii);
                MrSec omsc = Mrsec_1.get(jj);
                if (omsc.bloc > 0)// 之前已经有归宿了，略过
                {
                    continue;
                }

                int liscombined = 0;
                for (int mm = 0; mm < lmsc.splices.size(); mm++)
                {
                	MrSplice lmrsplice_ii = lmsc.splices.get(mm);
                    if (liscombined > 0)
                    {
                        break;
                    }
                    if (lmrsplice_ii.ncells.size() < 1)
                    {
                        continue;
                    }

                    List<MrPoint> lncells_ii = new ArrayList<MrPoint>(lmrsplice_ii.ncells.values());                    
            		Collections.sort(lncells_ii, new Comparator<MrPoint>()
            		{
            			public int compare(MrPoint x, MrPoint y)
            			{
            				if (y.cell.rsrp_avg > x.cell.rsrp_avg) return 1;
            				else if (y.cell.rsrp_avg == x.cell.rsrp_avg) return 0;
            				else return -1;
            			}
            		});
                    
                    //根据里面的每一个splice进行对比
                    for (int nn = 0; nn < omsc.splices.size(); nn++)
                    {
                    	MrSplice lmrsplice = omsc.splices.get(nn);
                        if (lmrsplice.ncells.size() < 1)
                        {
                            continue;
                        }
                        if (Math.abs(lmrsplice_ii.scell.cell.rsrp_avg - lmrsplice.scell.cell.rsrp_avg) > diffline1)
                        {
                            continue;
                        }

                        List<MrPoint> lncells = new ArrayList<MrPoint>(lmrsplice.ncells.values());                        
                        if ((lncells_ii.get(0).cell.eci == lncells.get(0).cell.eci)
                            && (Math.abs(lncells_ii.get(0).cell.rsrp_avg - lncells.get(0).cell.rsrp_avg) <= diffline1)
                            && (Math.abs(lmrsplice.ta - lmrsplice_ii.ta) <= 1 || Math.abs(lmrsplice.ta - lmrsplice_ii.ta) > 1000)
                            && (GetAOADiff(lmrsplice.aoa, lmrsplice_ii.aoa) <= 20)  //aoa包含了其中没有数据的情况            
                                )
                        {
                            // 找到组织了
                            liscombined = 1;
                            lsc.sections.add(omsc);
                            lsc.innum += omsc.innum;
                            lsc.otnum += omsc.otnum;
                            lsc.ottcnt += omsc.ottcnt;
                            omsc.bloc = 1;
                            break;
                        }
                    }
                }
            }
        }
    }

    private void Cluster_2(ArrayList<MrCluster> secs, ArrayList<MrSec> Mrsec_2, RefInt isecs_cnt_2, int diffline2)
    {
        //聚类邻区数有两个的
        for (int ii = 0; ii < Mrsec_2.size(); ii++)
        {
            // 之前已经有归宿了，略过
            if (Mrsec_2.get(ii).bloc > 0)
            {
                continue;
            }
            Mrsec_2.get(ii).bloc = 1;
            MrCluster lsc = new MrCluster();
            lsc.innum = Mrsec_2.get(ii).innum;
            lsc.otnum = Mrsec_2.get(ii).otnum;
            lsc.ottcnt = Mrsec_2.get(ii).ottcnt;
            lsc.sections.add(Mrsec_2.get(ii));
            secs.add(lsc);
            isecs_cnt_2.value++;
            for (int jj = ii + 1; jj < Mrsec_2.size(); jj++)
            {
                MrSec lmsc = Mrsec_2.get(ii);
                MrSec omsc = Mrsec_2.get(jj);
                if (omsc.bloc > 0)// 之前已经有归宿了，略过
                {
                    continue;
                }
                int liscombined = 0;
                for (int mm = 0; mm < lmsc.splices.size(); mm++)
                {
                	MrSplice lmrsplice_ii = lmsc.splices.get(mm);
                    if (liscombined > 0)
                    {
                        break;
                    }
                    if (lmrsplice_ii.ncells.size() < 2)
                    {
                        continue;
                    }
                    List<MrPoint> lncells_ii = new ArrayList<MrPoint>(lmrsplice_ii.ncells.values());
            		Collections.sort(lncells_ii, new Comparator<MrPoint>()
            		{
            			public int compare(MrPoint x, MrPoint y)
            			{
            				if (y.cell.rsrp_avg > x.cell.rsrp_avg) return 1;
            				else if (y.cell.rsrp_avg == x.cell.rsrp_avg) return 0;
            				else return -1;
            			}
            		});

                    for (int nn = 0; nn < omsc.splices.size(); nn++)
                    {
                    	MrSplice lmrsplice = omsc.splices.get(nn);
                        if (lmrsplice.ncells.size() < 2)
                        {
                            continue;
                        }

                        if (Math.abs(lmrsplice_ii.scell.cell.rsrp_avg - lmrsplice.scell.cell.rsrp_avg) > diffline2)
                        {
                            continue;
                        }
                        List<MrPoint> lncells = new ArrayList<MrPoint>(lmrsplice.ncells.values());
                		Collections.sort(lncells, new Comparator<MrPoint>()
                		{
                			public int compare(MrPoint x, MrPoint y)
                			{
                				if (y.cell.rsrp_avg > x.cell.rsrp_avg) return 1;
                				else if (y.cell.rsrp_avg == x.cell.rsrp_avg) return 0;
                				else return -1;
                			}
                		});
                        {

                            if (((lncells_ii.get(0).cell.eci == lncells.get(0).cell.eci) && (Math.abs(lncells_ii.get(0).cell.rsrp_avg - lncells.get(0).cell.rsrp_avg) <= diffline2)
                                    && (lncells_ii.get(1).cell.eci == lncells.get(1).cell.eci) && (Math.abs(lncells_ii.get(1).cell.rsrp_avg - lncells.get(1).cell.rsrp_avg) <= diffline2)
                                 )
                                 ||
                                ((lncells_ii.get(0).cell.eci == lncells.get(1).cell.eci) && (Math.abs(lncells_ii.get(0).cell.rsrp_avg - lncells.get(1).cell.rsrp_avg) <= diffline2)
                                    && (lncells_ii.get(1).cell.eci == lncells.get(0).cell.eci) && (Math.abs(lncells_ii.get(1).cell.rsrp_avg - lncells.get(0).cell.rsrp_avg) <= diffline2)
                                 )
                                )
                            {
                                // 找到组织了
                                liscombined = 1;
                                lsc.sections.add(omsc);
                                lsc.innum += omsc.innum;
                                lsc.otnum += omsc.otnum;
                                lsc.ottcnt += omsc.ottcnt;
                                omsc.bloc = 1;
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
    //在定位前，对所有的切片进行分类
    private void ClusterSet(ArrayList<MrCluster> secs, EciUnit eunit, RefInt splicecnt_2, RefInt splicecnt_1, RefInt splicecnt_0, RefInt isecs_cnt_2, RefInt isecs_cnt_1, RefInt isecs_cnt_0)
    {
        //add by yht --先将2,1,0区分出来
    	ArrayList<MrSec> Mrsec_2 = new ArrayList<MrSec>();
    	ArrayList<MrSec> Mrsec_1 = new ArrayList<MrSec>();
    	ArrayList<MrSec> Mrsec_0 = new ArrayList<MrSec>();
        int diffline2 = 5; //场强差值门限，用于2个邻区的情况
        int diffline1 = 5; //场强差值门限，用于1个邻区的情况
        int diffline0 = 3; //场强差值门限，用于0个邻区的情况
        Mrsec_0.clear();
        Mrsec_1.clear();
        Mrsec_2.clear();
        //int isecs_cnt_2 = 0;
        //int isecs_cnt_1 = 0;
        //int isecs_cnt_0 = 0;

        for (MrUser mu : eunit.muser.values()) //按小区中的用户进行遍历
        {
            for (int ii = 0; ii < mu.sections.size(); ii++) //按每个用户的session进行遍历
            {
                MrSec msc = mu.sections.get(ii);
                if (msc.maxneighbournum >= 2)
                {
                    Mrsec_2.add(msc);
                }
                else if (msc.maxneighbournum == 1)
                {
                    Mrsec_1.add(msc);
                }
                else
                {
                    Mrsec_0.add(msc);
                }
            }
        }

        Cluster_2(secs, Mrsec_2, isecs_cnt_2, diffline2);
        Cluster_1(secs, Mrsec_1, isecs_cnt_1, diffline1);
        Cluster_0(secs, Mrsec_0, isecs_cnt_0, diffline0);
        splicecnt_2.value = Mrsec_2.size();
        splicecnt_1.value = Mrsec_1.size();
        splicecnt_0.value = Mrsec_0.size();
    }		
    public void SetLocs(DataUnit dataUnit, CfgInfo cInfo, ReportProgress rptProgress)
    {
        long cNum = dataUnit.scCount();
        if (cNum == 0)
        {
            return;
        }

        rptProgress.writeLog(0, "总section数量=" + String.valueOf(cNum));

        long pNum = dataUnit.spCount();

        for (EciUnit eunit : dataUnit.eciUnits.values())//按小区遍历
        {
        	RefInt cLoc = new RefInt();
        	RefInt fLoc = new RefInt();
        	//long sNum = 0;
            //long nCount = 0;
        	ArrayList<MrCluster> secs = new ArrayList<MrCluster>();
            secs.clear();
            eunit.finger.Clear();
            eunit.rtable.InitTop();

            int buildid = CfgInfo.ci.GetId(eunit.cityid, eunit.eci);

            int lo_isindoor = 0; //用于写日志


            //add by yht --先将2,1,0区分出来
            //List<MrSec> Mrsec_2 = new List<MrSec>();
            //List<MrSec> Mrsec_1 = new List<MrSec>();
            //List<MrSec> Mrsec_0 = new List<MrSec>();
            //int diffline2 = 5; //场强差值门限，用于2个邻区的情况
            //int diffline1 = 5; //场强差值门限，用于1个邻区的情况
            //int diffline0 = 3; //场强差值门限，用于0个邻区的情况
            //Mrsec_0.Clear();
            //Mrsec_1.Clear();
            //Mrsec_2.Clear();
            RefInt isecs_cnt_2 = new RefInt();
            RefInt isecs_cnt_1 = new RefInt();
            RefInt isecs_cnt_0 = new RefInt();
            RefInt splicecnt_2 = new RefInt();
            RefInt splicecnt_1 = new RefInt();
            RefInt splicecnt_0 = new RefInt();

            //先聚类，包含2个邻区的，1个邻区的和无邻区的
            ClusterSet(secs, eunit, splicecnt_2, splicecnt_1, splicecnt_0, isecs_cnt_2, isecs_cnt_1, isecs_cnt_0);

            //这个用于写日志的
            lo_isindoor = secs.get(0).sections.get(0).splices.get(0).scell.cell.isindoor;

            //rptProgress(0, "总切片数=" + pNum.ToString() + ";无组织切片数=" + sNum.ToString());

            RefInt tNum = new RefInt();
            locloginfo lloginfo = new locloginfo();
            // 定位每个聚类
            if (secs.size() > 0)
            {
                if (eunit.finger.ReadSCellFinger(secs.get(0).sections.get(0).splices.get(0), cInfo, eunit.rtable) == false) //这个小区有问题，读取不了指纹
                {
                    //后续的都不处理了，直接进行下一个小区的处理
                    rptProgress.writeLog(0, "[ERROR]-----ECI:" + String.valueOf(eunit.eci) + "(" + String.valueOf(lo_isindoor) + ")"
                                 + ";小区指纹读取失败");
                    rptProgress.writeLog(0, "ECI:" + String.valueOf(eunit.eci)
                                 + ";聚类数=" + String.valueOf(secs.size())
                                 + ";dotp_err:" + String.valueOf(lloginfo.idoortype_err)
                                 + ";" + String.valueOf(lloginfo.idoortype_err_2) + "-" + String.valueOf(lloginfo.idoortype_err_1) + "-" + String.valueOf(lloginfo.idoortype_err_0)
                                 + ";邻2以上=" + String.valueOf(splicecnt_2.value)
                                 + "/" + String.valueOf(isecs_cnt_2.value)
                                 + ";邻1=" + String.valueOf(splicecnt_1.value)
                                 + "/" + String.valueOf(isecs_cnt_1.value)
                                 + ";邻0=" + String.valueOf(splicecnt_0.value)
                                 + "/" + String.valueOf(isecs_cnt_0.value)
                                 );
                    continue;
                }

                // 初始指纹库
                //DateTime ltm1 = DateTime.Now;
                for (MrCluster lmsc : secs)
                {
                    for (MrSec msc : lmsc.sections)
                    {
                        for (int jj = 0; jj < msc.splices.size(); jj++)
                        {
                            //                                msc.splices[jj].doortype = 0;
                            eunit.finger.AddLocf(msc.splices.get(jj), cInfo, eunit.rtable);
                        }
                    }
                }
                //DateTime ltm2 = DateTime.Now;
                //TimeSpan ts = ltm2 - ltm1;

                //lloginfo.iinitsimuspan += ts.TotalMilliseconds;
                if (secs.get(0).sections.get(0).splices.get(0).scell.cell.longitude > 0) //有指纹库的进行定位
                {
                    for (MrCluster lmsc : secs)
                    {
                        if (lmsc.ottcnt > 0)
                        {
                            //获取每个分类的中心经纬度
                            ClusterPos loclusterpos = new ClusterPos();
                            for (int ii = 0; ii < lmsc.sections.size(); ii++)
                            {
                                loclusterpos.SetPos(lmsc.sections.get(ii).postinfo);
                            }
                            lmsc.postinfo = loclusterpos.GetPos();
                        }
                        int lodoortype = 0;
                        if (tNum.value % 200 == 0)
                        {
                            rptProgress.writeLog(-1, String.valueOf(pNum) + "\r\n" + String.valueOf(tNum.value) + "\r\n" + String.valueOf(fLoc.value) + "\r\n" + String.valueOf(cLoc.value));
                        }
                        if (lmsc.postinfo == null)
                        {
                            if (lmsc.innum > lmsc.otnum)
                            {
                                lmsc.sections.get(0).splice.doortype = 1;
                            }
                            else
                            {
                                lmsc.sections.get(0).splice.doortype = 2;
                            }
                            lodoortype = lmsc.sections.get(0).splice.doortype;
                        }
                        else
                        {
                            lodoortype = lmsc.postinfo.doortype;
                            lmsc.sections.get(0).splice.doortype = lmsc.postinfo.doortype;
                        }

                        ExtLocs(lmsc, buildid, eunit, cInfo, rptProgress, tNum, cLoc, fLoc, lloginfo);

                        if (lmsc.sections.get(0).splice.doortype != lodoortype)
                        {
                            if (lmsc.postinfo != null)
                            {
                                lloginfo.idoortype_err++;
                                lloginfo.idoortype_err--;
                            }                        	
                            lloginfo.idoortype_err++;
                            if (lmsc.sections.get(0).maxneighbournum >= 2)
                            {
                                lloginfo.idoortype_err_2++;
                            }
                            else if (lmsc.sections.get(0).maxneighbournum == 1)
                            {
                                lloginfo.idoortype_err_1++;
                            }
                            else
                            {
                                lloginfo.idoortype_err_0++;
                            }
                        }
                    }
                }
                else //这里应该是没有找到对应的主服小区
                {
                    //Console.WriteLine("connot recognize scell : " + eunit.eci.ToString());
                    rptProgress.writeLog(0, "connot recognize scell : " + String.valueOf(eunit.eci));
                }
            }

            rptProgress.writeLog(0, "定位过程：ECI:" + String.valueOf(eunit.eci) + "(" + String.valueOf(lo_isindoor) + ")"
                         + ";聚类数(仿真-ott-ott转仿真)=" + String.valueOf(secs.size())
                         + ";" + String.valueOf(lloginfo.iloctype_simu) + "-" + String.valueOf(lloginfo.iloctype_ott) + "-" + String.valueOf(lloginfo.iloctype_simuott)
                         + ";dotp_err(2-1-0):" + String.valueOf(lloginfo.idoortype_err)
                         + ";" + String.valueOf(lloginfo.idoortype_err_2) + "-" + String.valueOf(lloginfo.idoortype_err_1) + "-" + String.valueOf(lloginfo.idoortype_err_0)
                         + ";邻2以上(切/聚)=" + String.valueOf(splicecnt_2.value)
                         + "/" + String.valueOf(isecs_cnt_2.value)
                         + ";邻1=" + String.valueOf(splicecnt_1.value)
                         + "/" + String.valueOf(isecs_cnt_1.value)
                         + ";邻0=" + String.valueOf(splicecnt_0.value)
                         + "/" + String.valueOf(isecs_cnt_0.value)
                         );
            rptProgress.writeLog(0, "定位时间：ECI:" + String.valueOf(eunit.eci) + "(" + String.valueOf(lo_isindoor) + ")"
                         + ";initsimutime(初始指纹):" + String.valueOf(((int)lloginfo.iinitsimuspan)) + "(ms)"
                         + ";locatime(聚定个数时间):" + String.valueOf(((int)lloginfo.ilocatetime)) + "(ms)-" + String.valueOf(lloginfo.ilocatecnt)
                         + ";tryloctime(试定个数时间):" + String.valueOf(((int)lloginfo.itrylocattime)) + "(ms)-" + String.valueOf(lloginfo.itrylocatecnt)
                         );

            rptProgress.writeLog(0, "定位结果：ECI:" + String.valueOf(eunit.eci) + "(" + String.valueOf(lo_isindoor) + ")"
                         + ";室内-室外-未知-未定位-随机定位：" + String.valueOf(lloginfo.iindoorcnt)
                         + "-" + String.valueOf(lloginfo.ioutdoorcnt)
                         + "-" + String.valueOf(lloginfo.iunknowncnt)
                         + "-" + String.valueOf(lloginfo.iunlocated)
                         + "-" + String.valueOf(lloginfo.irandomloc)
                         );

            //rptProgress(-1, pNum.ToString() + "\r\n" + sNum.ToString() + "\r\n" + fLoc.ToString() + "\r\n" + cLoc.ToString());
            //rptProgress(0, "独立定位数量=" + (tNum - fLoc).ToString() + ";聚类定位数量=" + fLoc.ToString() + ";小区定位数量=" + cLoc.ToString());

            secs.clear();
            eunit.finger.Clear();
        }
    }    	
    	
    private void SetLocs(int msp, MrSplice splice, int buildid, EciUnit eunit, CfgInfo cInfo, ReportProgress rptProgress, RefInt cLoc, RefInt fLoc)
    {
        try
        {
            if (splice.longitude == 0
                || (splice.longitude > 0 && splice.doortype == 1 && (splice.buildingid <= 0 || splice.ilevel < 0))
                )
            {
                if (msp == 1)
                {
                    eunit.finger.SetLocf1(splice, cInfo, eunit.rtable);
                    if (splice.longitude > 100000)
                    {
                        splice.iscenter = 2; //表明已定上
                    }
                }
                else
                {
                    fLoc.value++;
                    eunit.finger.SetLocf2(splice, cInfo, eunit.rtable);
                    //如果定位不上，则随机在周边选取一个
                    if (splice.longitude <= 0)
                    {
                        //Console.WriteLine("cannot located!");
                        //rptProgress(0, "cannot located!" + splice.scell.cell.eci.ToString());
                    }
                }
            }
        }
        catch (Exception ex)
        {
            rptProgress.writeLog(0, ex.getMessage());
        }

        //if (splice.longitude == 0)
        //{
        //    cLoc++;
        //    eunit.finger.SetLocc(splice, buildid);
        //}
    }

    public void ExtLocs(MrCluster mcluster, int buildid, EciUnit eunit, CfgInfo cInfo, ReportProgress rptProgress
        , RefInt tNum, RefInt cLoc, RefInt fLoc, locloginfo lloginfo)
    {
        List<MrSec> lmsc = mcluster.sections;
        //SetLocs(1, lmsc[0].splice, buildid, eunit, ref cInfo, rptProgress, ref cLoc, ref fLoc);
        int lodoortype;
        int lodoortype_right = 0;//判断正确的个数
        int lodoortype_err = 0;//判断错误的个数

        lodoortype = lmsc.get(0).splice.doortype;

        int loindex = -1;
        int loishaveott = 0;

        int loilongitude = 0;//临时记录OTT是否有经纬度

        if (mcluster.postinfo != null)//有OTT的经纬度
        {
            loishaveott = 1;
            if (mcluster.postinfo.ilongitude < 10000)
            {
                loishaveott = 0;
                lodoortype = mcluster.postinfo.doortype;
            }
            else
            {            
	            loindex = 0;//将OTT的定位结果赋值到0上，供后续初始化
	            if (mcluster.postinfo.doortype == 0)
	            {
	                if (lmsc.get(loindex).splice.scell.cell.isindoor == 1)
	                {
	                    mcluster.postinfo.doortype = 1;
	                }
	                else
	                {
	                    mcluster.postinfo.doortype = lodoortype;
	                }
	            }
	            lmsc.get(loindex).splice.doortype = mcluster.postinfo.doortype;
	            lmsc.get(loindex).splice.longitude = (int)mcluster.postinfo.ilongitude;
	            lmsc.get(loindex).splice.latitude = (int)mcluster.postinfo.ilatitude;
	            lmsc.get(loindex).splice.buildingid = mcluster.postinfo.iBuildingID;
	            lmsc.get(loindex).splice.ilevel = mcluster.postinfo.iLevel;
	            lodoortype = mcluster.postinfo.doortype;
	            eunit.finger.InitGrid(lmsc.get(loindex).splice);
	
	            loilongitude = (int)mcluster.postinfo.ilongitude;
            
	            int clusterloctype = 1; //ott定位
	            if (eunit.finger.GetGridCount() <= 0)
	            {
	                clusterloctype = 2; //有ott转0定位
	                loishaveott = 0;
	            }
	
	            if (clusterloctype == 2)
	            {
	                lloginfo.iloctype_simuott++;
	            }
	            else
	            {
	                lloginfo.iloctype_ott++;
	            }
            }
        }
        //DateTime tm_tryloc = DateTime.Now;
        if ((loishaveott == 0) || (loilongitude == 0))//无OTT，需要尝试定位
        {
            lloginfo.iloctype_simu++;

            int lotimes = 0; //室内外判断的次数
            lotimes = lmsc.size() / 3;
            if (lotimes > 3)
            {
                lotimes = 3;
            }
            else if (lotimes == 0)
            {
                lotimes = 1;
            }

            int lsplicecnt = 0;
            for (int ii = 0; ii < lmsc.size(); ii++)
            {
                lsplicecnt += lmsc.get(ii).splices.size();
                lmsc.get(ii).splice.doortype = lodoortype;
                if (lmsc.get(ii).splice.posinfo == null)
                {
                    lmsc.get(ii).splice.posinfo = mcluster.postinfo;
                }
                SetLocs(1, lmsc.get(ii).splice, buildid, eunit, cInfo, rptProgress, cLoc, fLoc);
                lloginfo.itrylocatecnt++;

                if (lmsc.get(ii).splice.iscenter == 2)
                {
                    lmsc.get(ii).splice.iscenter = 1;
                    if (lmsc.get(ii).splice.doortype != lodoortype) //前面的判断不对
                    {
                        lodoortype_err++;
                        if (lodoortype_err >= lotimes)
                        {
                            lmsc.get(ii).splice.iscenter = 1;
                            loindex = ii;
                            break;
                        }
                    }
                    else
                    {
                        lodoortype_right++;
                        if (lodoortype_right >= lotimes)
                        {
                            lmsc.get(ii).splice.iscenter = 1;
                            loindex = ii;
                            break;
                        }
                    }
                }
            }
            if (loindex == -1)
            {
                lloginfo.iunlocated += lsplicecnt;
                //一个都定位不了
                //Console.WriteLine("一个都定位不了");
                //rptProgress(0, "一个都定位不了:" + lmsc[0].splices[0].scell.cell.eci.ToString());
                return;
            }
            // 定位
            lodoortype = lmsc.get(loindex).splice.doortype;
            eunit.finger.InitGrid(lmsc.get(loindex).splice);
        }
        //int loncellnum = 0;
        //for (int ii = 0; ii < eunit.finger.lfgrids.Count; ii++)
        //{
        //    loncellnum += eunit.finger.lfgrids[ii].ncells.Count;
        //}
        // 初始指纹库
        //DateTime ltm1 = DateTime.Now;
        //lloginfo.itrylocattime += (ltm1 - tm_tryloc).TotalMilliseconds;

        //    foreach (MrSec msc in lmsc)
        //    {
        //        for (int jj = 0; jj < msc.splices.Count; jj++)
        //        {
        //            msc.splices[jj].doortype = lodoortype;
        //            eunit.finger.AddLocf(msc.splices[jj], cInfo, eunit.rtable);
        //        }
        //    }
        //DateTime ltm2 = DateTime.Now;
        //TimeSpan ts = ltm2 - ltm1;

        //iinitsimuspan += ts.TotalMilliseconds;

        //int loncellnum1 = 0;
        //for (int ii = 0; ii < eunit.finger.lfgrids.Count; ii++)
        //{
        //    loncellnum1 += eunit.finger.lfgrids[ii].ncells.Count;

        //}
        //if (loncellnum < loncellnum1)
        //{
        //    loncellnum++;
        //}

        for (MrSec msc : lmsc)
        {
            for (int jj = 0; jj < msc.splices.size(); jj++)
            {
                if (msc.splices.get(jj).doortype == 1)
                {
                    lloginfo.iindoorcnt++;//室内个数
                }
                else if (msc.splices.get(jj).doortype == 2)
                {
                    lloginfo.ioutdoorcnt++;//室外个数
                }
                else
                {
                    lloginfo.iunknowncnt++;//未知个数
                }

                if (msc.splices.get(jj).loctype == 3)//随机定位个数
                {
                    lloginfo.irandomloc++;
                }

                tNum.value++;
                if ((msc.splices.get(jj).longitude > 100000) && (msc.splices.get(jj).latitude > 100000))
                {
                    if ((msc.splices.get(jj).buildingid > 0) && (msc.splices.get(jj).ilevel >= 0)) //室内已经定位的过滤
                    {
                        continue;
                    }
                    if ((msc.splices.get(jj).buildingid < 0) && (lodoortype == 2)) //室外已经定位的过滤
                    {
                        continue;
                    }
                }
                msc.splices.get(jj).doortype = lodoortype;
                SetLocs(2, msc.splices.get(jj), buildid, eunit, cInfo, rptProgress, cLoc, fLoc);

                if (msc.splices.get(jj).loctype == 3)//随机定位个数
                {
                    lloginfo.irandomloc++;
                }
                else if (msc.splices.get(jj).longitude < 100000)
                {
                    lloginfo.iunlocated++;//未定位个数
                }

                lloginfo.ilocatecnt++;
            }
        }
        //记录时间间隔
        //lloginfo.ilocatetime += (DateTime.Now - ltm2).TotalMilliseconds;

        eunit.finger.ClearGrid();
    }	
}
