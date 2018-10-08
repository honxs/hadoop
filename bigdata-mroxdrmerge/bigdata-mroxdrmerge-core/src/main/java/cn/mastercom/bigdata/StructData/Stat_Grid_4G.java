package cn.mastercom.bigdata.StructData;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import cn.mastercom.bigdata.util.data.MyInt;

public class Stat_Grid_4G implements Serializable
{
	public int icityid;
	public int startTime;
	public int endTime;
	public int iduration;
	public int idistance;
	public int isamplenum;

	public int itllongitude;
	public int itllatitude;
	public int ibrlongitude;
	public int ibrlatitude;
	public Stat_Sample_4G tStat;
	
    public int UserCount_4G;
    public int UserCount_3G;
    public int UserCount_2G;
    public int UserCount_4GFall;
    public int XdrCount;
    public int MrCount;
    
    
    public Map<Long, MyInt> imsiMap;
	
	public Stat_Grid_4G()
	{
		tStat = new Stat_Sample_4G();
		imsiMap = new HashMap<Long, MyInt>();
		Clear();
	}
	
	public void Clear()
	{		
		UserCount_4G = 0;
		UserCount_3G = 0;
		UserCount_2G = 0;
		UserCount_4GFall = 0;
		XdrCount = 0;
		MrCount = 0;
		
		tStat.Clear();
		imsiMap.clear();
	};
	
	public static Stat_Grid_4G FillData(String[] values, int startPos)
	{
		int i = startPos;
		
		Stat_Grid_4G item = new Stat_Grid_4G();
		item.icityid = Integer.parseInt(values[i++]);          
		item.startTime = Integer.parseInt(values[i++]); 
		item.endTime = Integer.parseInt(values[i++]);   
		item.iduration = Integer.parseInt(values[i++]); 
		item.idistance = Integer.parseInt(values[i++]); 
		item.isamplenum = Integer.parseInt(values[i++]);
		item.itllongitude = Integer.parseInt(values[i++]);
		item.itllatitude = Integer.parseInt(values[i++]);
		item.ibrlongitude = Integer.parseInt(values[i++]);
		item.ibrlatitude = Integer.parseInt(values[i++]);      
		item.tStat.RSRP_nTotal = Integer.parseInt(values[i++]); 
		item.tStat.RSRP_nSum = Long.parseLong(values[i++]);
		
		for (int j = 0; j < item.tStat.RSRP_nCount.length; j++)
		{
			item.tStat.RSRP_nCount[j] = Integer.parseInt(values[i++]);
		}
		
		item.tStat.SINR_nTotal = Integer.parseInt(values[i++]);
		item.tStat.SINR_nSum = Long.parseLong(values[i++]);
		
		for (int j = 0; j < item.tStat.SINR_nCount.length; j++)
		{
			item.tStat.SINR_nCount[j] = Integer.parseInt(values[i++]);
		}		
		
        item.tStat.RSRP100_SINR0 = Integer.parseInt(values[i++]);
        item.tStat.RSRP105_SINR0 = Integer.parseInt(values[i++]);
        item.tStat.RSRP110_SINR3 = Integer.parseInt(values[i++]);
        item.tStat.RSRP110_SINR0 = Integer.parseInt(values[i++]);
        
        item.tStat.UpLen = Long.parseLong(values[i++]);        
        item.tStat.DwLen = Long.parseLong(values[i++]);        
        item.tStat.DurationU = Float.parseFloat(values[i++]);
        item.tStat.DurationD = Float.parseFloat(values[i++]);
        item.tStat.AvgUpSpeed = Float.parseFloat(values[i++]);   
        item.tStat.MaxUpSpeed = Float.parseFloat(values[i++]);   
        item.tStat.AvgDwSpeed = Float.parseFloat(values[i++]);   
        item.tStat.MaxDwSpeed = Float.parseFloat(values[i++]);   
                                                          
        item.tStat.UpLen_1M = Long.parseLong(values[i++]);     
        item.tStat.DwLen_1M = Long.parseLong(values[i++]);     
        item.tStat.DurationU_1M = Float.parseFloat(values[i++]);
        item.tStat.DurationD_1M = Float.parseFloat(values[i++]);
        item.tStat.AvgUpSpeed_1M = Float.parseFloat(values[i++]);
        item.tStat.MaxUpSpeed_1M = Float.parseFloat(values[i++]);
        item.tStat.AvgDwSpeed_1M = Float.parseFloat(values[i++]);
        item.tStat.MaxDwSpeed_1M = Float.parseFloat(values[i++]);
        
        item.UserCount_4G = Integer.parseInt(values[i++]); 
        item.UserCount_3G = Integer.parseInt(values[i++]); 
        item.UserCount_2G = Integer.parseInt(values[i++]);    
        item.UserCount_4GFall = Integer.parseInt(values[i++]); 
        item.XdrCount = Integer.parseInt(values[i++]); 
        item.MrCount = Integer.parseInt(values[i++]); 
        if(i<=values.length-8)
		{
	        item.tStat.RSRQ_nTotal = Integer.parseInt(values[i++]);
			item.tStat.RSRQ_nSum   = Integer.parseInt(values[i++]);
			
			for (int j = 0; j < item.tStat.RSRQ_nCount.length; j++)
			{
				item.tStat.RSRQ_nCount[j] = Integer.parseInt(values[i++]);
			}
			item.tStat.RSRP_nCount7   = Integer.parseInt(values[i++]);
		}
		return item;
	}
	
}
