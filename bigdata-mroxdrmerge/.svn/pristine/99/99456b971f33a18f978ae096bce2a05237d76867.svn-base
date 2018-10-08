package cn.mastercom.bigdata.locuser_v3;

import java.util.List;

public class CellPorp
{
    public void SetCell(CfgInfo cInfo, DataUnit dataUnit)
    {
        CellC sc = null;

        for (EciUnit em : dataUnit.eciUnits.values())
        {
            int isSrvOK = 1;
            for (MrUser ms : em.muser.values())
            {
                if (isSrvOK == 0)
                {
                    break; //整个服务小区都找不到
                }
                List<MrSam> mm = ms.samples;
                
                sc = null;
                for (int ii = 0; ii < mm.size(); ii++)
                {
                    if (sc == null)
                    {
                        sc = CfgInfo.cc.GetCell(mm.get(ii).scell.eci);
                        if (sc == null)
                        {
                            isSrvOK = 0;
                            break;
                        }
                    }
                    if (sc.cityid > 0)
                    {
                        em.cityid = sc.cityid;
                    }
                    
                    mm.get(ii).cityid = sc.cityid;
                    mm.get(ii).scell.isindoor = sc.isindoor;
                    mm.get(ii).scell.longitude = sc.longitude;
                    mm.get(ii).scell.latitude = sc.latitude;

                    for (Mrcell mc : mm.get(ii).ncells)
                    {
                        CellN cc = cInfo.cn.GetNCell(sc.cityid, mm.get(ii).scell.eci, mc.pciarfcn);
                        if (cc != null)
                        {
                            mc.eci = cc.neci;
                            mc.isindoor = cc.isindoor;
                            mc.longitude = cc.longitude;
                            mc.latitude = cc.latitude;
                        }
                        else
                        {
                            CellC cl = CfgInfo.cc.GetNCell(sc.cityid, sc.eci, mc.pciarfcn, sc.longitude / 10000000.0, sc.latitude / 10000000.0);
                            if (cl != null)
                            {
                                mc.eci = cl.eci;
                                mc.isindoor = cl.isindoor;
                                mc.longitude = cl.longitude;
                                mc.latitude = cl.latitude;
                            }
                        }
                    }
                }
            }
        }
    }
}
