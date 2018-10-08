package cn.mastercom.bigdata.locuser_v3;

import java.util.ArrayList;
import java.util.List;

public class AOATAPrint
{
    public static boolean bStudy = false;

    public CellC c_scell = new CellC();
    //AOATA指纹 AOA 720  ; TA 200 ;RSRP:5dB一格
    public AOATASample[][][] vec_AOATAPrint = new AOATASample[720][200][12];

    public List<String> data = new ArrayList<String>();
    
    public int SetSCell(int eci)
    {
        if (!bStudy)
        {
            return 0;
        }
        CfgInfo.cc.GetCell(eci);
        c_scell = CfgInfo.cc.GetCell(eci);
        if (c_scell == null)
        {
            return -1;
        }
        return 1;
    }
    //样本数据清洗,-1为清洗掉了
    public int DataWash(MrSam ms)
    {
        if (ms.longitude < 10000 || ms.latitude < 10000)
        {
            return -1;
        }
        if ((ms.ta < 0) || (ms.aoa < 0) || ms.ta >= 200 || ms.aoa >= 720)
        {
            return -1;
        }
        int lolongdif = Math.abs(ms.longitude - c_scell.longitude);
        int lolatdif = Math.abs(ms.latitude - c_scell.latitude);
        //与服务小区距离超过5公里进行清洗
        if ((lolongdif >= 500000) || (lolatdif >= 500000))
        {
            //需要清洗
            ms.longitude = 0;
            ms.latitude = 0;
            ms.org_loctp = "";
            ms.org_label = "";
            return -1;
        }
        int lotadiff = (ms.ta+8)*8000;
       //与服务小区距离超过2~5公里,如果TA异常(服务小区的距离超过了 80*（TA+8）)或者如果与第一强邻区的距离超过了与服务小区的距离，则清洗
        if ((lolongdif >= 200000) || (lolatdif >= 200000))
        {
            if(ms.ncells.size() > 0)
            {
                if(ms.ncells.get(0).longitude > 0)
                {
                   if( (Math.abs(ms.ncells.get(0).longitude-ms.longitude) + Math.abs(ms.ncells.get(0).latitude-ms.latitude))
                        > (lolatdif + lolatdif))
                   {
                       //这里需要清洗了
                       //需要清洗
                        ms.longitude = 0;
                        ms.latitude = 0;
                        ms.org_loctp = "";
                        ms.org_label = "";
                        return -1;
                   }
                }
            }
            else if(lotadiff < lolongdif || lotadiff < lolatdif)
            {
                //需要清洗
                ms.longitude = 0;
                ms.latitude = 0;
                ms.org_loctp = "";
                ms.org_label = "";
                return -1;
            }
            else
            {
                return 1;
            }
        }
        //与服务小区的距离在600米到2公里之间，如果与服务小区距离超过了 80*（TA+8）进行清洗
        if ((Math.abs(ms.longitude - c_scell.longitude) >= 60000) || (Math.abs(ms.latitude - c_scell.latitude) >= 60000))
        {
            if (lotadiff < lolongdif || lotadiff < lolatdif)
            {
                //需要清洗
                ms.longitude = 0;
                ms.latitude = 0;
                ms.org_loctp = "";
                ms.org_label = "";
                return -1;
            }
        }

        return 1;
    }

    public void Study(MrSam msample)
    {
        if (!bStudy)
        {
            return;
        }

        AddSample(msample);
        GetLocation(msample);
    }

    //增加样本以供AOATA指纹学习
    public void AddSample(MrSam msample)
    {
        //数据先清洗
        if (DataWash(msample) <= 0)
        {
            return;
        }
        int loaoa = msample.aoa;
        int lota = msample.ta;
        int lorsrp = (int)(msample.scell.rsrp_avg * (-1) - 60) / 5;
        if(lorsrp < 0)
        {
            lorsrp = 0;
        }
        else if(lorsrp >= 12)
        {
            lorsrp = 11;
        }

        if (vec_AOATAPrint[loaoa][lota][lorsrp] == null)
        {
            vec_AOATAPrint[loaoa][lota][lorsrp] = new AOATASample();
            vec_AOATAPrint[loaoa][lota][lorsrp].iECI = c_scell.eci;
            vec_AOATAPrint[loaoa][lota][lorsrp].iTA = lota;
            vec_AOATAPrint[loaoa][lota][lorsrp].iAOA = loaoa;
            vec_AOATAPrint[loaoa][lota][lorsrp].iRSRP = lorsrp;
        }
        //else
        //{
        //    AOATASample loaoatasample = new AOATASample();
        //    vec_AOATAPrint[loaoa, lota, lorsrp].Add(loaoatasample);
        //}
        vec_AOATAPrint[loaoa][lota][lorsrp].SetPos(msample.org_loctp, msample.org_indr, msample.org_label, msample.buildingid, msample.longitude, msample.latitude, msample.ilevel);
    }
    //从AOATA指纹中获取位置
    public int GetLocation(MrSplice msplice)
    {
        return 1;
    }
    //直接将经纬度赋值到采样点上去
    public int GetLocation(MrSam msample)
    {
        if ((msample.ta < 0) || (msample.aoa < 0) || msample.ta >= 200 || msample.aoa >= 720)
        {
            return -1;
        }
        if ((msample.longitude > 1000) && (msample.latitude > 1000))
        {
            return -1;
        }
        int loaoa = msample.aoa;
        int lota = msample.ta;
        int lorsrp = (int)(msample.scell.rsrp_avg * (-1) - 60) / 5;
        if (lorsrp < 0)
        {
            lorsrp = 0;
        }
        else if (lorsrp >= 12)
        {
            lorsrp = 11;
        }

        if (vec_AOATAPrint[loaoa][lota][lorsrp] == null)
        {
            return -1;
        }
        PosInfo loposinfo = vec_AOATAPrint[loaoa][lota][lorsrp].GetPos(msample.org_label);
        if (loposinfo != null)
        {
            msample.longitude = (int)loposinfo.ilongitude;
            msample.latitude = (int)loposinfo.ilatitude;
            msample.buildingid = loposinfo.iBuildingID;
            msample.ilevel = loposinfo.iLevel;
        }
        return 1;
    }
    //读取AOATA指纹库
    public int ReadPrint()
    {
        return 1;
    }
    //写AOATA指纹库
    public void WritePrint()
    {
        if (!bStudy)
        {
            return;
        }

        //Console.WriteLine("cell's aoata print:" + c_scell.eci.ToString());

        for (int ii = 0; ii < 720; ii++)
        {
            for (int jj = 0; jj < 200; jj++)
            {
                for (int kk = 0; kk < 12; kk++)
                {                	
                    if (vec_AOATAPrint[ii][jj][kk] != null)
                    {
                    	String sValue = "";
                    	
                    	sValue = String.valueOf(vec_AOATAPrint[ii][jj][kk].iECI)
                    			+ "\t" + String.valueOf(vec_AOATAPrint[ii][jj][kk].iTA)
                    			+ "\t" + String.valueOf(vec_AOATAPrint[ii][jj][kk].iAOA)
                    			+ "\t" + String.valueOf((vec_AOATAPrint[ii][jj][kk].iRSRP*5));

                        ArrayList<PosInfo> loposlist = vec_AOATAPrint[ii][jj][kk].PostList.get(0);
                        if (loposlist.size() > 0)
                        {                        
                        	for (int ll = 0; ll < loposlist.size(); ll++)
	                        {
	                        	sValue += ("\t" + String.valueOf(loposlist.get(ll).accuracy)
	                                + "\t" + String.valueOf(loposlist.get(ll).count)
	                                + "\t" + String.valueOf(loposlist.get(ll).doortype)
	                                + "\t" + String.valueOf(loposlist.get(ll).iBuildingID)
	                                + "\t" + String.valueOf(loposlist.get(ll).iLevel)
	                                + "\t" + String.valueOf(loposlist.get(ll).ilongitude)
	                                + "\t" + String.valueOf(loposlist.get(ll).ilatitude));
	                        }
                        }
                        for (int m = 0; m < 3 - loposlist.size(); m++)
                        {
                        	sValue += ("\t" + "\t" + "\t" + "\t" + "\t" + "\t" + "\t");
                        }
                        
                        loposlist = vec_AOATAPrint[ii][jj][kk].PostList.get(1);
                        if (loposlist.size() > 0)
                        {
                            for (int ll = 0; ll < loposlist.size(); ll++)
                            {
                            	sValue += ("\t" + String.valueOf(loposlist.get(ll).accuracy)
                                    + "\t" + String.valueOf(loposlist.get(ll).count)
                                    + "\t" + String.valueOf(loposlist.get(ll).doortype)
                                    + "\t" + String.valueOf(loposlist.get(ll).iBuildingID)
                                    + "\t" + String.valueOf(loposlist.get(ll).iLevel)
                                    + "\t" + String.valueOf(loposlist.get(ll).ilongitude)
                                    + "\t" + String.valueOf(loposlist.get(ll).ilatitude));
                            }
                        }
                        for (int m = 0; m < 3 - loposlist.size(); m++)
                        {
                        	sValue += ("\t" + "\t" + "\t" + "\t" + "\t" + "\t" + "\t");
                        }                        
                        
                        loposlist = vec_AOATAPrint[ii][jj][kk].PostList.get(2);
                        if (loposlist.size() > 0)
                        {
                            for (int ll = 0; ll < loposlist.size(); ll++)
                            {
                            	sValue += ("\t" + String.valueOf(loposlist.get(ll).accuracy)
                                    + "\t" + String.valueOf(loposlist.get(ll).count)
                                    + "\t" + String.valueOf(loposlist.get(ll).doortype)
                                    + "\t" + String.valueOf(loposlist.get(ll).iBuildingID)
                                    + "\t" + String.valueOf(loposlist.get(ll).iLevel)
                                    + "\t" + String.valueOf(loposlist.get(ll).ilongitude)
                                    + "\t" + String.valueOf(loposlist.get(ll).ilatitude));
                            }
                        }
                        for (int m = 0; m < 3 - loposlist.size(); m++)
                        {
                        	sValue += ("\t" + "\t" + "\t" + "\t" + "\t" + "\t" + "\t");
                        }
                        
                        data.add(sValue);
                    }
                }
            }
        }
        
        vec_AOATAPrint = null;
    }
}
