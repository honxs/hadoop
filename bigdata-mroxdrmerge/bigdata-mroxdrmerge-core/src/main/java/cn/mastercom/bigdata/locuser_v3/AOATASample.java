package cn.mastercom.bigdata.locuser_v3;

import java.util.ArrayList;
import java.util.List;

public class AOATASample
{
    //aoata的样本记录
    public int iECI = -1;
    public int iAOA = -1; 
    public int iTA = -1;
    public int iRSRP = -1; //场强按照5dB进行分段
    public List<ArrayList<PosInfo>> PostList = new ArrayList<ArrayList<PosInfo>>();
    //public int iisindoor = -1; //是否室内
    //public int ibuildingid = -1; //楼宇ID
    //public int ilevel = -1; //楼高
    //public int isamplenum = 0; //参与指纹的样本数
    //public int ilongitude = 0; //中心经度
    //public int ilatitude = 0; //中心纬度
    //public ClusterPos clusterpos = new ClusterPos();

    public AOATASample()
    {
        for (int ii = 0; ii < 3; ii++) //初始定位室外、室内、未知各支持3个聚类
        {
        	ArrayList<PosInfo> lpostlistsub = new ArrayList<PosInfo>();
            PostList.add(lpostlistsub);
        }
    }
    //判断室内外 2 室外，1 室内，0 未知
    public int IndoorJudge(String org_loctp, int org_indr, String org_label, int iBuildingID)
    {
        if ((org_loctp == "ll") || (org_label=="high"))
        {
            return 2; //返回室外
        }
        if (iBuildingID > 0)
        {
            return 1;
        }
        return 0;
    }
    //如果确定是室外，则为室外，确定为室内的选室内，其它按顺序
    public PosInfo GetPos(String org_label)
    {
        if (org_label == "high")
        {
            //室外
            if (PostList.get(0).size() > 0)
            {
                return PostList.get(0).get(0);
            }
        }
        if (PostList.get(0).size() > 0)
        {
            return PostList.get(0).get(0);
        }
        else if (PostList.get(1).size() > 0)
        {
            return PostList.get(1).get(0);
        }
        else if (PostList.get(2).size() > 0)
        {
            return PostList.get(2).get(0);
        }
        return null;
        
    }
    //对不知道室内外的进行处理,看是否能够与现有的匹配上
    public int CheckPos_0(String org_loctp, int org_indr, String org_label, int iBuildingID, long ilong, long ilat, int ilevel)
    {
        List<PosInfo> lposinfolist = null;
        lposinfolist = PostList.get(1);
        for (int ii = 0; ii < lposinfolist.size(); ii++)
        {
            if (Math.abs(ilong - lposinfolist.get(ii).ilongitude) <= 10000 && Math.abs(ilat - lposinfolist.get(ii).ilatitude) <= 10000) //能够聚类
            {
                return 1;
            }
        }
        lposinfolist = PostList.get(2);
        for (int ii = 0; ii < lposinfolist.size(); ii++)
        {
            if (Math.abs(ilong - lposinfolist.get(ii).ilongitude) <= 10000 && Math.abs(ilat - lposinfolist.get(ii).ilatitude) <= 10000) //能够聚类
            {
                return 1;
            }
        }
        return 0;
    }
    public int CheckPos(int doortype,int sn )
    {
        List<PosInfo> lposinfolist = null;
        lposinfolist = PostList.get(doortype);
        List<PosInfo> lposinfolist_0 = null;
        lposinfolist_0 = PostList.get(0);
        int lodelcnt = 0;
        for (int ii = lposinfolist_0.size()-1; ii >= 0; ii--)
        {
            if (Math.abs(lposinfolist_0.get(ii).ilongitude - lposinfolist.get(sn).ilongitude) <= 10000 && Math.abs(lposinfolist_0.get(ii).ilatitude - lposinfolist.get(sn).ilatitude) <= 10000) //能够聚类
            {
                lposinfolist_0.remove(ii);
                lodelcnt++;
            }
        }
        return lodelcnt;
    }

    public void SetPos(String org_loctp, int org_indr, String org_label, int iBuildingID, long ilong, long ilat, int ilevel)
    {
        int laccu = 0;
        int ldoortype = IndoorJudge(org_loctp,org_indr,org_label,iBuildingID);

        if (ldoortype == 0)
        {
            //先查一下是否有已知室内外的能够进行匹配
            if (CheckPos_0(org_loctp, org_indr, org_label, iBuildingID, ilong, ilat, ilevel) > 0)
            {
                return;//已经可以匹配上
            }
        }
        
        if (org_loctp == "ll")
        {
            laccu = 2;
        }

        List<PosInfo> lposinfolist = null;
        lposinfolist = PostList.get(ldoortype);

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
                if (Math.abs(ilong - lposinfolist.get(ii).ilongitude) <= 10000 && Math.abs(ilat - lposinfolist.get(ii).ilatitude) <= 10000) //能够聚类
                {
                    if(lposinfolist.get(ii).accuracy > laccu)
                    {
                        continue;
                    }
                    else if(lposinfolist.get(ii).accuracy < laccu)
                    {
                        //直接用新的高精度替换
                        lposinfolist.get(ii).ilongitude = ilong;
                        lposinfolist.get(ii).ilatitude = ilat;
                        lposinfolist.get(ii).count = 1;
                        lposinfolist.get(ii).accuracy = laccu;
                        islocated = 1;
                    }
                    else
                    {
                        lposinfolist.get(ii).ilongitude = (lposinfolist.get(ii).ilongitude * lposinfolist.get(ii).count + ilong) / (lposinfolist.get(ii).count + 1);
                        lposinfolist.get(ii).ilatitude = (lposinfolist.get(ii).ilatitude * lposinfolist.get(ii).count + ilat) / (lposinfolist.get(ii).count + 1);
                        lposinfolist.get(ii).count++;
                        islocated = 1;
                    }

                }
            }
        }
        if ((islocated == 0) && (lposinfolist.size() < 3))
        {
            //新起一个
            PosInfo lposinfo = new PosInfo();
            lposinfo.accuracy = laccu;
            lposinfo.doortype = ldoortype;
            lposinfo.iBuildingID = iBuildingID;
            if ((ilong > 10000) && (ilat > 10000))
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

            if (ldoortype != 0)
            {
                CheckPos(ldoortype, lposinfolist.size()-1);
            }
        }
    }
}
