package cn.mastercom.bigdata.locuser_v2;

import java.util.HashMap;
import java.util.Map;

public class DataUnit
{
    // 每个小区数据单元
    public Map<Integer, EciUnit> eciUnits = new HashMap<Integer, EciUnit>();

    public void Clear()
    {
        eciUnits.clear();
    }
    // s1apid数量
    public long siCount()
    {
    	long nCount = 0;
    	
    	for (Map.Entry<Integer, EciUnit> eu : eciUnits.entrySet())
        {
            nCount += eu.getValue().samples.size();
        }
        return nCount;
    }
    // splice数量
    public long spCount()
    {
    	long nCount = 0;
    	for (Map.Entry<Integer, EciUnit> eu : eciUnits.entrySet())
        {
            nCount += eu.getValue().splices.size();
        }
        return nCount;
    }
}
