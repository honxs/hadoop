package cn.mastercom.bigdata.StructData;

import java.io.Serializable;

public class Stat_Grid_Freq_4G implements Serializable
{
	public int icityid;
	public int freq;
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
	
	public Stat_Grid_Freq_4G()
	{
		tStat = new Stat_Sample_4G();
		Clear();
	}
	
	public void Clear()
	{		
		tStat.Clear();
	};
	
	public static Stat_Grid_Freq_4G FillData(String[] values, int startPos)
	{
		int i = startPos;
		
		Stat_Grid_Freq_4G item = new Stat_Grid_Freq_4G();
		item.icityid = Integer.parseInt(values[i++]);   
		item.freq = Integer.parseInt(values[i++]); 
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
        item.tStat.RSRP_nCount7  = Integer.parseInt(values[i++]);
		return item;
	}
	
}
