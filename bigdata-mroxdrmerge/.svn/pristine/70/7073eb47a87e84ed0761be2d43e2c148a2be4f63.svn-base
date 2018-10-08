package cn.mastercom.bigdata.util;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.Stat_Sample_4G;

public class LteStatHelper
{

	public static void statEvt(DT_Sample_4G sample, Stat_Sample_4G statItem)
	{
		if (sample.IPDataUL > 0)
		{
			statItem.UpLen += sample.IPDataUL;

			statItem.DurationU += sample.duration;

			statItem.MaxUpSpeed = Math.max(statItem.MaxUpSpeed, (float) sample.IPThroughputUL);

			if (statItem.DurationU > 0)
				statItem.AvgUpSpeed = (float) (statItem.UpLen / (statItem.DurationU / 1000.0) * 8.0) / 1024;

			if (sample.IPDataUL >= 1024 * 1024)
			{
				statItem.UpLen_1M += sample.IPDataUL;

				statItem.MaxUpSpeed_1M = Math.max(statItem.MaxUpSpeed_1M, (float) sample.IPThroughputUL);

				statItem.DurationU_1M += sample.duration;

				if (statItem.DurationU_1M > 0)
					statItem.AvgUpSpeed_1M = (float) (statItem.UpLen_1M / (statItem.DurationU_1M / 1000.0) * 8.0)
							/ 1024;
			}

		}

		if (sample.IPDataDL > 0)
		{
			statItem.DwLen += sample.IPDataDL;

			statItem.DurationD += sample.duration;

			statItem.MaxDwSpeed = Math.max(statItem.MaxDwSpeed, (float) sample.IPThroughputDL);

			if (statItem.DurationD > 0)
				statItem.AvgDwSpeed = (float) (statItem.DwLen / (statItem.DurationD / 1000.0) * 8.0) / 1024;

			if (sample.IPDataDL >= 1024 * 1024)
			{
				statItem.DwLen_1M += sample.IPDataDL;

				statItem.MaxDwSpeed_1M = Math.max(statItem.MaxDwSpeed_1M, (float) sample.IPThroughputDL);

				statItem.DurationD_1M += sample.duration;

				if (statItem.DurationD_1M > 0)
					statItem.AvgDwSpeed_1M = (float) (statItem.DwLen_1M / (statItem.DurationD_1M / 1000.0) * 8.0)
							/ 1024;

			}
		}
		
	}

	public static void statMro(DT_Sample_4G sample, Stat_Sample_4G statItem)
	{
		if (sample.LteScRSRP >= -150 && sample.LteScRSRP <= -30)
		{
			statItem.RSRP_nTotal++;

			statItem.RSRP_nSum += sample.LteScRSRP;
			if (sample.LteScRSRP < -113)
			{
				statItem.RSRP_nCount7++;
			}
			// RSRP_nCount[6]; //
			// [-141,-110),[-110,-105),[-105,-100),[-100,-95),[-95,-85),[-85,)
			if (sample.LteScRSRP < -110)
			{
				statItem.RSRP_nCount[0]++;
			}
			else if (sample.LteScRSRP < -105)
			{
				statItem.RSRP_nCount[1]++;
			}
			else if (sample.LteScRSRP < -100)
			{
				statItem.RSRP_nCount[2]++;
			}
			else if (sample.LteScRSRP < -95)
			{
				statItem.RSRP_nCount[3]++;
			}
			else if (sample.LteScRSRP < -85)
			{
				statItem.RSRP_nCount[4]++;
			}
			else
			{
				statItem.RSRP_nCount[5]++;
			}
		}

		if (sample.LteScSinrUL >= -1000 && sample.LteScSinrUL <= 1000)
		{
			statItem.SINR_nTotal++; // 总数

			statItem.SINR_nSum += sample.LteScSinrUL;

			// int SINR_nCount[8]; //
			// [-20,0),[0,5),[5,10),[10,15),[15,20),[20,25),[25,50),[50,)
			if (sample.LteScSinrUL < 0)
			{
				statItem.SINR_nCount[0]++;
			}
			else if (sample.LteScSinrUL < 5)
			{
				statItem.SINR_nCount[1]++;
			}
			else if (sample.LteScSinrUL < 10)
			{
				statItem.SINR_nCount[2]++;
			}
			else if (sample.LteScSinrUL < 15)
			{
				statItem.SINR_nCount[3]++;
			}
			else if (sample.LteScSinrUL < 20)
			{
				statItem.SINR_nCount[4]++;
			}
			else if (sample.LteScSinrUL < 25)
			{
				statItem.SINR_nCount[5]++;
			}
			else if (sample.LteScSinrUL < 50)
			{
				statItem.SINR_nCount[6]++;
			}
			else
			{
				statItem.SINR_nCount[7]++;
			}

			if (sample.LteScRSRP >= -150 && sample.LteScRSRP <= -30)
			{
				if ((sample.LteScRSRP >= -100) && (sample.LteScSinrUL >= -3))
				{
					statItem.RSRP100_SINR0++;
				}
				if ((sample.LteScRSRP >= -103) && (sample.LteScSinrUL >= -3))
				{
					statItem.RSRP105_SINR0++;
				}
				if ((sample.LteScRSRP >= -110) && (sample.LteScSinrUL >= -3))
				{
					statItem.RSRP110_SINR3++;
				}
				if ((sample.LteScRSRP >= -113) && (sample.LteScSinrUL >= -3))
				{
					statItem.RSRP110_SINR0++;
				}
			}
		}
		
		int RSRQ = sample.LteScRSRQ;
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
