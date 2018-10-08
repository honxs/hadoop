package cn.mastercom.bigdata.locuser_v3;

import java.util.ArrayList;
import java.util.List;

/*
1.1 来一个经纬度，记录；定位方式：high定位、mid定位、low定位
                       室内外标签：移动的为室外、常驻的为室内、其余为未知
     区分：室外high 室外mid 室内 high 室内 mid 未知 high 未知 mid 六大组
           每大组区分 3个聚类小组
1.2 再来一个经纬度，看与已有聚类经纬度的距离，低于100米，进行聚类，聚类后进行聚类平均经纬度计算，高于100米，则新开聚类；
1.3 如果超过三个聚类后，第四个聚类删除，表示没有；
1.4 最后选择聚类的最优解作为中心经纬度；
最优解选取规则：
室外high（GPS）> 室内high（常驻静止用户）-> 室外mid（运动态的）->其余
同一小组内以3个聚类的采样多为准，其余中的以采样多为准；
*/
public class ClusterPos
{
    public ClusterPos()
    {
        for (int ii = 0; ii < 5; ii++)
        {
        	ArrayList<PosInfo> lpostlistsub = new ArrayList<PosInfo>();
            PostList.add(lpostlistsub);
        }
    }

    private List<ArrayList<PosInfo>> PostList = new ArrayList<ArrayList<PosInfo>>();
    private int outchoice = -1; //输出选择，-1无,0outhigh,1inhith,2outmid,3inmid,4other
    /*
     室外high（GPS）> 室内high（常驻静止用户）-> 室外mid（运动态的）->其余
    同一小组内以3个聚类的采样多为准，其余中的以采样多为准；
     */
    public PosInfo GetPos()
    {
        if(outchoice == -1)
        {
            return null;
        }
        ArrayList<PosInfo> lpostlist = PostList.get(outchoice);

        int lmaxcnt = -1;
        int lmaxindex = -1;
        for (int ii = 0; ii < lpostlist.size(); ii++)
        {
            if (lmaxcnt < lpostlist.get(ii).count)
            {
                lmaxcnt = lpostlist.get(ii).count;
                lmaxindex = ii;
            }
        }
        if (lmaxindex >= 0)
        {
            PosInfo lposinfo = lpostlist.get(lmaxindex);
            
            PostList.clear();
            return lposinfo;
        }
        else
        {
            return null;
        }
       
    }

    public void SetPos(PosInfo iposinfo)
    { 	
        if (iposinfo == null)
        {
            return;
        }
        if (iposinfo.doortype == 2)
        {
            if (iposinfo.accuracy == 2)
            {
                SetPos("ll", 0, "high", iposinfo.iBuildingID, iposinfo.ilongitude, iposinfo.ilatitude, iposinfo.iLevel);
            }
            else
            {
                SetPos("", 0, "high", iposinfo.iBuildingID, iposinfo.ilongitude, iposinfo.ilatitude, iposinfo.iLevel);
            }
        }
        else
        {
            SetPos("", 1, "", iposinfo.iBuildingID, iposinfo.ilongitude, iposinfo.ilatitude, iposinfo.iLevel);
        }
    }

    public void SetPos(String org_loctp, int org_indr, String org_label, int iBuildingID, long ilong, long ilat, int ilevel)
    {
        int laccu = 0;
        int ldoortype = 0;
        //考虑到用户填了GPS定位类型，但是没有经纬度的情况，这种情况确认用户是在室外的	
        if ((ilong < 100000) || (ilat < 100000))
        {
            if(org_loctp != "ll")
            {
                return;
            }
            else
            {
                //调试用，下面的赋值没有意义
                ldoortype = 0; 
            }
        }


        ArrayList<PosInfo> lposinfolist = null;
        if (org_loctp == "ll") //GPS定位的
        {
            outchoice = 0;
            laccu = 2;//高精确度
            ldoortype = 2;
        }
        else if((ilevel >= 0) && (iBuildingID > 0)) //室内定位已经区分楼层了
        {
            if((outchoice < 1)&&(outchoice >= 0))
            {
                return;
            }
            outchoice = 1;
            laccu = 2;//高精度
            ldoortype = 1;
        }
        else if (org_label == "high")//高速  高速运动的
        {
            if ((outchoice < 2)&&(outchoice >= 0))
            {
                return; //有更精确的数据了
            }
            outchoice = 2;
            laccu = 1;//中精确度
            ldoortype = 2;
        }
        else if (iBuildingID > 0)
        {
            if ((outchoice < 3)&&(outchoice >= 0))
            {
                return;
            }
            outchoice = 3;
            laccu = 1;//中精确度
            ldoortype = 1;
        }
        else
        {
            if ((outchoice < 4)&&(outchoice >= 0))
            {
                return;
            }
            outchoice = 4;
            laccu = 1;
            ldoortype = 0;
        }
        lposinfolist = PostList.get(outchoice);

        int islocated = 0;
        for (int ii = 0; ii < lposinfolist.size(); ii++)
        {
            if (ldoortype == 1) //室内
            {
                if (lposinfolist.get(ii).iBuildingID == iBuildingID) //在一个楼里面
                {
                    lposinfolist.get(ii).ilongitude = (lposinfolist.get(ii).ilongitude * lposinfolist.get(ii).count + ilong) / (lposinfolist.get(ii).count + 1);
                    lposinfolist.get(ii).ilatitude = (lposinfolist.get(ii).ilatitude * lposinfolist.get(ii).count + ilat) / (lposinfolist.get(ii).count + 1);
                    lposinfolist.get(ii).count++;
                    if (ilevel >= 0)
                    {
                        if (lposinfolist.get(ii).iLevel >= 0)
                        {
                            lposinfolist.get(ii).iLevel = (lposinfolist.get(ii).iLevel * lposinfolist.get(ii).ilevelcnt + ilevel) / (lposinfolist.get(ii).ilevelcnt + 1);
                        }
                        else
                        {
                            lposinfolist.get(ii).iLevel = ilevel;
                        }
                        lposinfolist.get(ii).ilevelcnt++;
                    }
                    islocated = 1;
                    break;
                }
            }
            else //按照室外判断
            {
                if ((ilong > 10000) && (ilat > 10000))
                {
                    if (lposinfolist.get(ii).ilongitude > 10000)
                    {      	
		                if (Math.abs(ilong - lposinfolist.get(ii).ilongitude) <= 10000 && Math.abs(ilat - lposinfolist.get(ii).ilatitude) <= 10000) //能够聚类
		                {
		                    lposinfolist.get(ii).ilongitude = (lposinfolist.get(ii).ilongitude * lposinfolist.get(ii).count + ilong) / (lposinfolist.get(ii).count + 1);
		                    lposinfolist.get(ii).ilatitude = (lposinfolist.get(ii).ilatitude * lposinfolist.get(ii).count + ilat) / (lposinfolist.get(ii).count + 1);
		                    lposinfolist.get(ii).count++;
		                    islocated = 1;		                    
		                }
                        else
                        {
                            continue;
                        }
                    }
                    else
                    {
                        //只要里面有经纬度不是0的,则将经纬度赋值过来
                        lposinfolist.get(ii).ilongitude = ilong;
                        lposinfolist.get(ii).ilatitude = ilat;
                        lposinfolist.get(ii).count++;
                    }
                }
                //经纬度是空的，且进入了循环，则不用特殊处理了，直接break
                islocated = 1;
                break;		                
            }                
        }
        if((islocated == 0) && (lposinfolist.size() < 3))
        {
            //新起一个
            PosInfo lposinfo = new PosInfo();
            lposinfo.accuracy = laccu;
            lposinfo.doortype = ldoortype;
            lposinfo.iBuildingID = iBuildingID;
            if((ilong > 10000) && (ilat > 10000))
            {
	            lposinfo.count = 1;
	            lposinfo.ilongitude = ilong;
	            lposinfo.ilatitude = ilat;
            }
            if (iBuildingID > 0)
            {
                if (ilevel >= 0)
                {
                    lposinfo.iLevel = ilevel;
                    lposinfo.ilevelcnt++;
                }
            }
            lposinfolist.add(lposinfo);
        }
    }
}
