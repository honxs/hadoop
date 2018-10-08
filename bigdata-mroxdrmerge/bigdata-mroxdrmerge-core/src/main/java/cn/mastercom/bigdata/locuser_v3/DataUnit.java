package cn.mastercom.bigdata.locuser_v3;

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
    	
    	for (EciUnit eu : eciUnits.values())
        {
            nCount += eu.siCount();
        }
        return nCount;
    }
    // splice数量
    public long spCount()
    {
    	long nCount = 0;
    	for (EciUnit eu : eciUnits.values())
        {
            nCount += eu.spCount();
        }
        return nCount;
    }
    // section数量
    public long scCount()
    {
    	long nCount = 0;
    	for (EciUnit eu : eciUnits.values())
        {
            nCount += eu.scCount();
        }
        return nCount;
    }
}
