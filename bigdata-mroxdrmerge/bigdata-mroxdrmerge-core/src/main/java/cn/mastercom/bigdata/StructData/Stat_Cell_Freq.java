package cn.mastercom.bigdata.StructData;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class Stat_Cell_Freq implements Serializable
{
	public int icityid;
	public int startTime;
	public int endTime;
	public int iLAC;
	public int wRAC;
	public long iCI;
	public int freq;
	public int pci;
	
	public int iduration;
	public int idistance;
	public int isamplenum;
	
	public int freqCount;

	public long RSRP_nTotal;    // 总数
	public long RSRP_nSum;  // 总和
	public long[] RSRP_nCount; // [-141,-110),[-110,-95),[-95,-80),[-80,-65),[-65,-50),[-50,)
	public long RSRP_nCount7; // [-141,-113)
	
	public long RSRQ_nTotal;    // 总数
	public long RSRQ_nSum;  // 总和
	public long[] RSRQ_nCount; // [-40,-20),[-20,-16),[-16,-12),[-12,-8),[-8,0),[0,40]
	
	public Set<String> freqSet = new HashSet<String>();
	
	public Stat_Cell_Freq()
	{		
		Clear();
	}
	
	public void Clear()
	{
		freqCount = 0;
		
		RSRP_nTotal = 0;
		RSRP_nSum = 0;
		RSRP_nCount = new long[6];
		
		RSRQ_nTotal = 0;
		RSRQ_nSum = 0;
		RSRQ_nCount = new long[6];
		
		freqSet = new HashSet<String>();
	};
	
	public static Stat_Cell_Freq FillData(String[] values, int startPos)
	{
		int i = startPos;
		
		Stat_Cell_Freq item = new Stat_Cell_Freq();
		item.icityid = Integer.parseInt(values[i++]);     
		item.iLAC = Integer.parseInt(values[i++]); 
		item.iCI = Long.parseLong(values[i++]); 
		item.freq = Integer.parseInt(values[i++]); 
		item.startTime = Integer.parseInt(values[i++]); 
		item.endTime = Integer.parseInt(values[i++]);   
		item.iduration = Integer.parseInt(values[i++]); 
		item.idistance = Integer.parseInt(values[i++]); 
		item.isamplenum = Integer.parseInt(values[i++]);
		
		item.RSRP_nTotal = Long.parseLong(values[i++]);
		item.RSRP_nSum = Long.parseLong(values[i++]);
		for(int j=0; j<item.RSRP_nCount.length; ++j)
		{
			item.RSRP_nCount[j] = Long.parseLong(values[i++]);;
		}
		
		item.RSRQ_nTotal = Long.parseLong(values[i++]);
		item.RSRQ_nSum = Long.parseLong(values[i++]);
		for(int j=0; j<item.RSRQ_nCount.length; ++j)
		{
			item.RSRQ_nCount[j] = Long.parseLong(values[i++]);;
		}
		
		try
		{
			item.RSRP_nCount7 = Long.parseLong(values[i++]);	
			item.pci = Integer.parseInt(values[i++]);
		}
		catch (Exception e)
		{

		}
		
		return item;
	}
}
