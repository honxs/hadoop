package cn.mastercom.bigdata.locuser_v2;

public class SamLocs
{
    public void SetLocs(DataUnit dataUnit, CfgInfo cInfo, ReportProgress rptProgress)
    {
        if (dataUnit.spCount() == 0)
        {
            return;
        }

        String sNum = String.valueOf(dataUnit.spCount());
        long cNum = 0;
        long nCount = 0;
        long nClnum = 0;
        long nLonum = 0;
        long[] ndr = new long[3];
        long[] mdr = new long[3];
        for (EciUnit eunit : dataUnit.eciUnits.values())
        {
            cNum = eunit.splices.size();
            nClnum = 0;
            nLonum = 0;
            ndr[0] = 0; ndr[1] = 0; ndr[2] = 0;
            mdr[0] = 0; mdr[1] = 0; mdr[2] = 0;
            
            eunit.finger.Clear();
            eunit.rtable.InitTop();
            int buildid = CfgInfo.ci.GetId(eunit.splices.get(0).cityid, eunit.eci);
            for (int ii = 0; ii < eunit.splices.size(); ii++)
            {
                if ((nCount++) % 500 == 0)
                {
                	rptProgress.writeLog(-1, sNum + "\t" + String.valueOf(nCount) + "\t" + String.valueOf(nClnum));
                }

	            try
	            {                    
                    MrSplice splice = eunit.splices.get(ii);
                    // 无邻区的和全室分的不要20170810
					//if (splice.ncells.size() == 0 || splice.nocell == null)
					//{
					//    nClnum++;
					//    eunit.finger.SetLocc(splice, buildid);
					//    continue;
					//}

                    DoorType.SetDoorType(splice, eunit);

                    ndr[splice.doortype]++;
                    
                    eunit.finger.SetLocf(splice, cInfo, eunit.rtable);    
                    
                    mdr[splice.doortype]++;
                    
                    if (splice.longitude == 0)
                    {
                    	nLonum++;
                        eunit.finger.SetLocc(splice, buildid);
                    }
	            }
	            catch (Exception ex)
	            {
	            	rptProgress.writeLog(0,"bb:" + ex.getMessage());
	            }                 
	        }

            rptProgress.writeLog(0, "total=" + String.valueOf(cNum) + "; noncell=" + String.valueOf(nClnum) + "; loc=" + String.valueOf(cNum - nLonum - nClnum) + "; unloc=" + String.valueOf(nLonum));
            rptProgress.writeLog(0, "total=" + String.valueOf(cNum - nClnum) + "; indr=" + String.valueOf(ndr[1]) + "; otdr=" + String.valueOf(ndr[2]) + "; undr=" + String.valueOf(ndr[0]));
            rptProgress.writeLog(0, "total=" + String.valueOf(cNum - nClnum) + "; indr2=" + String.valueOf(mdr[1]) + "; otdr2=" + String.valueOf(mdr[2]) + "; undr2=" + String.valueOf(mdr[0]));

            eunit.finger.Clear();                  
        }
    }
}
