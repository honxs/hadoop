package cn.mastercom.bigdata.locuser_v3;

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
        
        MrUser mu = null;
        if (!eu.muser.containsKey(sam.s1apid))
        {
        	mu = new MrUser();
            eu.muser.put(sam.s1apid, mu);
        }
        else
        {
        	mu = eu.muser.get(sam.s1apid);
        }

        mu.samples.add(sam);
    }
}
