package cn.mastercom.bigdata.StructData;

import java.io.Serializable;

public class Stat_Cell_4G implements Serializable
{
	public int icityid;
	public int startTime;
	public int endTime;
	public int iduration;
	public int idistance;
	public int isamplenum;

	public int iLAC;
	public int wRAC;
	public long iCI;
	
	public int xdrCount;
	public int mroCount;
	public int mroxdrCount;
	
	public int mreCount;
	public int mrexdrCount;
	
	public int origLocXdrCount;//翰信提供有经纬度的点数
	public int totalLocXdrCount;//所有有经纬度的点数
	public int validLocXdrCount;//具有有效经纬度的点数
	public int dtXdrCount;//高速有经纬度的点数
	public int cqtXdrCount;//室分有经纬度的点数
	public int dtexXdrCount;//慢速有经纬度的点数

	
	public Stat_Sample_4G tStat;
	
	public int sfcnJamSamCount;
	public int sdfcnJamSamCount;
	
	public Stat_Cell_4G()
	{
		tStat = new Stat_Sample_4G();
		Clear();
	}
	
	public void Clear()
	{
		sfcnJamSamCount = 0;
		sdfcnJamSamCount = 0;
		
		tStat.Clear();
	};
	
	public static Stat_Cell_4G FillData(String[] values, int startPos)
	{
		int i = startPos;
		
		Stat_Cell_4G item = new Stat_Cell_4G();
		item.icityid = Integer.parseInt(values[i++]);     
		item.iLAC = Integer.parseInt(values[i++]); 
		item.iCI = Long.parseLong(values[i++]); 
		item.startTime = Integer.parseInt(values[i++]); 
		item.endTime = Integer.parseInt(values[i++]);   
		item.iduration = Integer.parseInt(values[i++]); 
		item.idistance = Integer.parseInt(values[i++]); 
		item.isamplenum = Integer.parseInt(values[i++]);
		
		item.xdrCount = Integer.parseInt(values[i++]);
		item.mroCount = Integer.parseInt(values[i++]);
		item.mroxdrCount = Integer.parseInt(values[i++]);
		
		item.mreCount = Integer.parseInt(values[i++]);
		item.mrexdrCount = Integer.parseInt(values[i++]);
		
		item.origLocXdrCount = Integer.parseInt(values[i++]);
		item.totalLocXdrCount = Integer.parseInt(values[i++]);
		item.validLocXdrCount = Integer.parseInt(values[i++]);
		item.dtXdrCount = Integer.parseInt(values[i++]);
		item.cqtXdrCount = Integer.parseInt(values[i++]);
		item.dtexXdrCount = Integer.parseInt(values[i++]);
		
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
        
        item.sfcnJamSamCount = Integer.parseInt(values[i++]);
        item.sdfcnJamSamCount = Integer.parseInt(values[i++]);   
        
        item.tStat.RSRQ_nTotal = Integer.parseInt(values[i++]);
		item.tStat.RSRQ_nSum   = Integer.parseInt(values[i++]);
		
		for (int j = 0; j < item.tStat.RSRQ_nCount.length; j++)
		{
			item.tStat.RSRQ_nCount[j] = Integer.parseInt(values[i++]);
		}
		item.tStat.RSRP_nCount7   = Integer.parseInt(values[i++]);
		
		return item;
	}
}
