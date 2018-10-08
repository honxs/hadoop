package cn.mastercom.bigdata.xdr.loc;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.Stat_Grid_Freq_4G;
import cn.mastercom.bigdata.StructData.Stat_Sample_4G;

public class GridData_Freq
{
	private int startTime;
	private int endTime;
	private Stat_Grid_Freq_4G lteGrid; 
	
	public GridData_Freq(int startTime, int endTime, int freq)
	{
	   this.startTime = startTime;
	   this.endTime = endTime;
	   lteGrid  = new Stat_Grid_Freq_4G();
	   lteGrid.freq = freq;
	}
	
	public Stat_Grid_Freq_4G getLteGrid()
	{
		return lteGrid;
	}
	
	public int getStartTime()
	{
		return startTime;
	}
	
	public int getEndTime()
	{
		return endTime;
	}
	
	public void dealSample(DT_Sample_4G sample, int rsrp, int rsrq)
	{
		boolean isMroSample = sample.flag.toUpperCase().equals("MRO");
		boolean isMreSample = sample.flag.toUpperCase().equals("MRE");
			
		lteGrid.isamplenum++;
		if (isMroSample || isMreSample)
		{
			statMro(rsrp, rsrq, lteGrid.tStat);
		}
	}
	
	public void statMro(int RSRP, int RSRQ, Stat_Sample_4G statItem)
	{
		if (RSRP <= -30 && RSRP >= -150)
		{
			statItem.RSRP_nTotal++;

			statItem.RSRP_nSum += RSRP;

			// RSRP_nCount[6]; //
			// [-141,-110),[-110,-105),[-105,-100),[-100,-95),[-95,-85),[-85,)
			if (RSRP < -113)
			{
				statItem.RSRP_nCount7++;
			}
			
			if (RSRP < -110)
			{
				statItem.RSRP_nCount[0]++;
			}
			else if (RSRP < -105)
			{
				statItem.RSRP_nCount[1]++;
			}
			else if (RSRP < -100)
			{
				statItem.RSRP_nCount[2]++;
			}
			else if (RSRP < -95)
			{
				statItem.RSRP_nCount[3]++;
			}
			else if (RSRP < -85)
			{
				statItem.RSRP_nCount[4]++;
			}
			else
			{
				statItem.RSRP_nCount[5]++;
			}
			
			if (RSRQ >-100)
			{
				statItem.RSRQ_nTotal++;
				if (RSRQ != -1000000)
				{
					statItem.RSRQ_nSum += RSRQ;
				}
				// [-40 -20) [-20 -16) [-16 -12)[-12 -8) [-8 0)[0,)
				if (RSRQ < -20 && RSRQ >= -40)
				{
					statItem.RSRQ_nCount[0]++;
				} else if (RSRQ < -16)
				{
					statItem.RSRQ_nCount[1]++;
				} else if (RSRQ< -12)
				{
					statItem.RSRQ_nCount[2]++;
				} else if (RSRQ< -8)
				{
					statItem.RSRQ_nCount[3]++;
				} else if (RSRQ < 0)
				{
					statItem.RSRQ_nCount[4]++;
				} else if (RSRQ>= 0)
				{
					statItem.RSRQ_nCount[5]++;
				}
			}
		}

	}
   
	
	public void finalDeal()
	{
		
	}
	

}
