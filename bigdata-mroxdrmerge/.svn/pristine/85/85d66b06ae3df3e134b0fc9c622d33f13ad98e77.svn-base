package cn.mastercom.bigdata.mro.stat.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.NC_LTE;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.base.constant.DataConstant;
import cn.mastercom.bigdata.util.TimeHelper;

public class CSStat_Cell implements IStat_4G,Serializable{
	protected int icityid;
	protected int startTime;
	protected int endTime;
	protected int iduration;
	protected int idistance;
	protected int isamplenum;
	protected int iLAC;
	protected int wRAC;
	protected long iCI;
	protected int xdrCount;
	protected int mroCount;
	protected int mroxdrCount;
	protected int mreCount;
	protected int mrexdrCount;
	protected int origLocXdrCount;//翰信提供有经纬度的点数
	protected int totalLocXdrCount;//所有有经纬度的点数
	protected int validLocXdrCount;//具有有效经纬度的点数
	protected int dtXdrCount;//高速有经纬度的点数
	protected int cqtXdrCount;//室分有经纬度的点数
	protected int dtexXdrCount;//慢速有经纬度的点数
	protected int sfcnJamSamCount;
	protected int sdfcnJamSamCount;
	 
	
	protected long RSRP_nTotal;    // 总数
	protected long RSRP_nSum;  // 总和
	protected long[] RSRP_nCount=new long[7]; // [-141,-113) [-141,-110),[-110,-95),[-95,-80),[-80,-65),[-65,-50),[-50,)
	protected long SINR_nTotal;    // 总数
	protected long SINR_nSum;  // 总和
	protected long[] SINR_nCount=new long[8]; // [-20,0),[0,5),[5,10),[10,15),[15,20),[20,25),[25,50),[50,)
	protected int RSRQ_nTotal;
	protected int RSRQ_nSum;
	protected long[] RSRQ_nCount = new long[6];
	protected long RSRP100_SINR0; // RSRP>-100 and SINR>0
	protected long RSRP105_SINR0; // RSRP>=-105 and SINR>=0
	protected long RSRP110_SINR3; // RSRP>-110 and SINR>3
	protected long RSRP110_SINR0; // RSRP>-110 and SINR>=0
	protected long UpLen;
	protected long DwLen;
	protected float DurationU;
	protected float DurationD;
	protected float AvgUpSpeed;
	protected float MaxUpSpeed;
	protected float AvgDwSpeed;
	protected float MaxDwSpeed;
	protected long UpLen_1M;
	protected long DwLen_1M;
	protected float DurationU_1M;
	protected float DurationD_1M;
	protected float AvgUpSpeed_1M;
	protected float MaxUpSpeed_1M;
	protected float AvgDwSpeed_1M;
	protected float MaxDwSpeed_1M;
	 
	public void doFirstSample(DT_Sample_4G values)
	{
		icityid=values.cityID;
		startTime=TimeHelper.getRoundDayTime(values.itime);
		endTime=startTime+ 86400;
		iLAC=values.iLAC;
		iCI=values.Eci;
	}
	
	@Override
	public void doSample(DT_Sample_4G values) {
		boolean isSampleMro = values.flag.toUpperCase().equals("MRO");
		boolean isSampleMre = values.flag.toUpperCase().equals("MRE");

		iduration += values.duration;
		if (isSampleMro || isSampleMre)
		{
			isamplenum++;
			if (values.LteScRSRP >= -150 && values.LteScRSRP <= -30)
			{
				RSRP_nTotal++;

				RSRP_nSum += values.LteScRSRP;
				if (values.LteScRSRP < -113)
				{
					RSRP_nCount[6]++;
				}
				// RSRP_nCount[6]; //
				// [-141,-110),[-110,-105),[-105,-100),[-100,-95),[-95,-85),[-85,)
				if (values.LteScRSRP < -110)
				{
					RSRP_nCount[0]++;
				}
				else if (values.LteScRSRP < -105)
				{
					RSRP_nCount[1]++;
				}
				else if (values.LteScRSRP < -100)
				{
					RSRP_nCount[2]++;
				}
				else if (values.LteScRSRP < -95)
				{
					RSRP_nCount[3]++;
				}
				else if (values.LteScRSRP < -85)
				{
					RSRP_nCount[4]++;
				}
				else
				{
					RSRP_nCount[5]++;
				}
			}
			if (values.LteScSinrUL >= -1000 && values.LteScSinrUL <= 1000)
			{
				SINR_nTotal++; // 总数

				SINR_nSum += values.LteScSinrUL;

				// int SINR_nCount[8]; //
				// [-20,0),[0,5),[5,10),[10,15),[15,20),[20,25),[25,50),[50,)
				if (values.LteScSinrUL < 0)
				{
					SINR_nCount[0]++;
				}
				else if (values.LteScSinrUL < 5)
				{
					SINR_nCount[1]++;
				}
				else if (values.LteScSinrUL < 10)
				{
					SINR_nCount[2]++;
				}
				else if (values.LteScSinrUL < 15)
				{
					SINR_nCount[3]++;
				}
				else if (values.LteScSinrUL < 20)
				{
					SINR_nCount[4]++;
				}
				else if (values.LteScSinrUL < 25)
				{
					SINR_nCount[5]++;
				}
				else if (values.LteScSinrUL < 50)
				{
					SINR_nCount[6]++;
				}
				else
				{
					SINR_nCount[7]++;
				}
				if (values.LteScRSRP >= -150 && values.LteScRSRP <= -30)
				{
					if ((values.LteScRSRP >= -100) && (values.LteScSinrUL >= -3))
					{
						RSRP100_SINR0++;
					}
					if ((values.LteScRSRP >= -103) && (values.LteScSinrUL >= -3))
					{
						RSRP105_SINR0++;
					}
					if ((values.LteScRSRP >= -110) && (values.LteScSinrUL >= -3))
					{
						RSRP110_SINR3++;
					}
					if ((values.LteScRSRP >= -113) && (values.LteScSinrUL >= -3))
					{
						RSRP110_SINR0++;
					}
				}
			}			
			int RSRQ = values.LteScRSRQ;
			if (RSRQ >-100)
			{
				RSRQ_nTotal++;
				if (RSRQ != -1000000)
				{
					RSRQ_nSum += RSRQ;
				}
				// [-40 -20) [-20 -16) [-16 -12)[-12 -8) [-8 0)[0,)
				if (RSRQ < -20 && RSRQ >= -40)
				{
					RSRQ_nCount[0]++;
				} else if (RSRQ < -16)
				{
					RSRQ_nCount[1]++;
				} else if (RSRQ< -12)
				{
					RSRQ_nCount[2]++;
				} else if (RSRQ< -8)
				{
					RSRQ_nCount[3]++;
				} else if (RSRQ < 0)
				{
					RSRQ_nCount[4]++;
				} else if (RSRQ>= 0)
				{
					RSRQ_nCount[5]++;
				}
			}
			
			 int result = isSampleJam(values);
				if (result == 1 || result == 2)
				{
					sfcnJamSamCount++;
				}

				if (result == 2 || result == 3)
				{
					sdfcnJamSamCount++;
				}

				if (isSampleMro)
				{
					mroCount++;
					if (values.IMSI > 0)
					{
						mroxdrCount++;
					}
				}
				else if (isSampleMre)
				{
					mreCount++;
					if (values.IMSI > 0)
					{
						mrexdrCount++;
					}
				}
			}
			else
			{
				xdrCount++;

				if (values.ilongitude > 0 && values.indoor >= 0)
				{
					totalLocXdrCount++;

					if (values.locType.equals("ll") || values.locType.equals("ll2") || values.locType.equals("wf") && values.radius <= 100 && values.radius >= 0)
					{
						validLocXdrCount++;
					}

					if (values.testType == StaticConfig.TestType_DT)
					{
						dtXdrCount++;
					}
					else if (values.testType == StaticConfig.TestType_CQT)
					{
						cqtXdrCount++;
					}
					else if (values.testType == StaticConfig.TestType_DT_EX)
					{
						dtexXdrCount++;
					}
				}
				if (values.IPDataUL > 0)
				{
					UpLen += values.IPDataUL;

					DurationU += values.duration;

					MaxUpSpeed = Math.max(MaxUpSpeed, (float) values.IPThroughputUL);

					if (DurationU > 0)
						AvgUpSpeed = (float) (UpLen / (DurationU / 1000.0) * 8.0) / 1024;

					if (values.IPDataUL >= 1024 * 1024)
					{
						UpLen_1M += values.IPDataUL;

						MaxUpSpeed_1M = Math.max(MaxUpSpeed_1M, (float) values.IPThroughputUL);

						DurationU_1M += values.duration;

						if (DurationU_1M > 0)
							AvgUpSpeed_1M = (float) (UpLen_1M / (DurationU_1M / 1000.0) * 8.0)
									/ 1024;
					}
				}
				if (values.IPDataDL > 0)
				{
					DwLen += values.IPDataDL;

					DurationD += values.duration;

					MaxDwSpeed = Math.max(MaxDwSpeed, (float) values.IPThroughputDL);

					if (DurationD > 0)
						AvgDwSpeed = (float) (DwLen / (DurationD / 1000.0) * 8.0) / 1024;

					if (values.IPDataDL >= 1024 * 1024)
					{
						DwLen_1M += values.IPDataDL;

						MaxDwSpeed_1M = Math.max(MaxDwSpeed_1M, (float) values.IPThroughputDL);

						DurationD_1M += values.duration;

						if (DurationD_1M > 0)
							AvgDwSpeed_1M = (float) (DwLen_1M / (DurationD_1M / 1000.0) * 8.0)
									/ 1024;
					}
				}
		}	
	}
	public int isSampleJam(DT_Sample_4G tsam)
	{
		int sameFcnJamCellCount = 0;
		int differFcnJamCellCount = 0;
		if ((tsam.LteScRSRP < -50 && tsam.LteScRSRP > -150) && tsam.LteScRSRP > -110)
		{
			for (NC_LTE item : tsam.tlte)
			{
				if ((item.LteNcRSRP < -50 && item.LteNcRSRP > -150) && item.LteNcRSRP - tsam.LteScRSRP > -6)
				{
					if (tsam.Earfcn == item.LteNcEarfcn)
					{
						sameFcnJamCellCount++;
					}
					else
					{
						differFcnJamCellCount++;
					}
				}
			}
		}
		int result = 0;
		if (sameFcnJamCellCount >= 3)
		{
			result = 1;
		}
		if (sameFcnJamCellCount + differFcnJamCellCount >= 3)
		{
			if (result == 1)
			{
				result = 2;
			}
			else
			{
				result = 3;
			}
		}
		return result;
	}

	@Override
	public void doSampleLT(DT_Sample_4G values) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void doSampleDX(DT_Sample_4G values) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String toLine() {
		StringBuffer sb = new StringBuffer();
		sb.append(icityid);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(iLAC);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(iCI);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(startTime);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(endTime);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(iduration);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(idistance);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(isamplenum);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(xdrCount);//9
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(mroCount);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(mroxdrCount);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(mreCount);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(mrexdrCount);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(origLocXdrCount);//
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(totalLocXdrCount);//15
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(validLocXdrCount);//16
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(dtXdrCount);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(cqtXdrCount);//18
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(dtexXdrCount);//19
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(RSRP_nTotal);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(RSRP_nSum);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);

		for (int i = 0; i <RSRP_nCount.length-1; i++)
		{
			sb.append(RSRP_nCount[i]);
			sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		}
		sb.append(SINR_nTotal);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(SINR_nSum);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);

		for (int i = 0; i <SINR_nCount.length; i++)
		{
			sb.append(SINR_nCount[i]);
			sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		}
		sb.append(RSRP100_SINR0);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(RSRP105_SINR0);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(RSRP110_SINR3);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(RSRP110_SINR0);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(UpLen);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(DwLen);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(DurationU);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(DurationD);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(AvgUpSpeed);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(MaxUpSpeed);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(AvgDwSpeed);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(MaxDwSpeed);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(UpLen_1M);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(DwLen_1M);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(DurationU_1M);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(DurationD_1M);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(AvgUpSpeed_1M);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(MaxUpSpeed_1M);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(AvgDwSpeed_1M);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(MaxDwSpeed_1M);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(sfcnJamSamCount);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(sdfcnJamSamCount);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(RSRQ_nTotal);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(RSRQ_nSum);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(RSRQ_nCount[0]);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(RSRQ_nCount[1]);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(RSRQ_nCount[2]);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(RSRQ_nCount[3]);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(RSRQ_nCount[4]);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(RSRQ_nCount[5]);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(RSRP_nCount[6]);
        return sb.toString();
	}

}
