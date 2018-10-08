package cn.mastercom.bigdata.locuser_v2;

import java.util.ArrayList;

public class SamList
{
    // 采样点是按时间排序的
    public void SetSams(MrSam sam, DataUnit dataUnit)
    {
    	EciUnit eu = null;
        if (!dataUnit.eciUnits.containsKey(sam.scell.eci))
        {
        	eu = new EciUnit();
            dataUnit.eciUnits.put(sam.scell.eci, eu);
        }
        else
        {
        	eu = dataUnit.eciUnits.get(sam.scell.eci);
        }
        
        eu.eci = sam.scell.eci;
        
        ArrayList<MrSam> lsam = null;
        if (!eu.samples.containsKey(sam.s1apid))
        {
        	lsam = new ArrayList<MrSam>();
            eu.samples.put(sam.s1apid, lsam);
        }
        else
        {
        	lsam = eu.samples.get(sam.s1apid);
        }

        lsam.add(sam);
    }
}
