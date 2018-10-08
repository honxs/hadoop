package cn.mastercom.bigdata.mro.stat.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.base.constant.DataConstant;
import cn.mastercom.bigdata.util.TimeHelper;
import java.util.HashSet;
import java.util.Set;

public class CSStat_CellGrid implements IStat_4G,Serializable{
	protected int icityid;
	protected int iLAC;
	protected long iCI;
	protected int startTime;
	protected int endTime;
	protected int iduration;
	protected int idistance;
	protected int isamplenum;
	protected int itllongitude;
	protected int itllatitude;
	protected int ibrlongitude;
	protected int ibrlatitude;
	protected int UserCount_4G;
	protected int UserCount_3G;
	protected int UserCount_2G;
	protected int UserCount_4GFall;
	protected int XdrCount;
	protected int MrCount;
	protected int overlapnum = 0; // 分子
	protected int overlapden = 0; // 分母
	boolean isten;
	boolean isGrid;
	public Set<Long> imsiSet=new HashSet<Long>(); 
	
	

	protected long RSRP_nTotal;    // 总数
	protected long RSRP_nSum;  // 总和
	protected long[] RSRP_nCount=new long [7]; // [-141,-110),[-110,-95),[-95,-80),[-80,-65),[-65,-50),[-50,)[-141,-113)		 
	protected long SINR_nTotal;    // 总数
	protected long SINR_nSum;  // 总和
	protected long[] SINR_nCount=new long [8]; // [-20,0),[0,5),[5,10),[10,15),[15,20),[20,25),[25,50),[50,)
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
	
	public void doFirstSample(DT_Sample_4G values,boolean isten,boolean isGrid)
	{
		icityid=values.cityID;
		this.isten=isten;
		this.isGrid=isGrid;
		if(isten==true)
		{

			itllongitude= (int) ((long) values.ilongitude/1000 * 1000);
			itllatitude = (int) ((long) values.ilatitude/ 900 * 900 + 900);
			ibrlongitude= itllongitude + 1000;
			ibrlatitude = itllatitude - 900;
		}
		else
		{
			itllongitude=(int) ((long) values.ilongitude / 4000 * 4000);
			itllatitude	=(int) ((long) values.ilatitude / 3600 * 3600 + 3600);
			ibrlongitude= itllongitude + 4000;
			ibrlatitude	=itllatitude - 3600;
		}
		startTime=TimeHelper.getRoundDayTime(values.itime);
		endTime=startTime+ 86400;
		iLAC=values.iLAC;
		iCI=values.Eci;
	}
	@Override
	public void doSample(DT_Sample_4G values) {
	 
		boolean isMrovalues = values.flag.toUpperCase().equals("MRO");
		boolean isMrevalues = values.flag.toUpperCase().equals("MRE");

		isamplenum++;
		if (isMrovalues || isMrevalues)
		{
			MrCount++;
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
		} 
		else if (RSRQ < -16)
		{
			RSRQ_nCount[1]++;
		} 
		else if (RSRQ< -12)
		{
			RSRQ_nCount[2]++;
		} 
		else if (RSRQ< -8)
		{
			RSRQ_nCount[3]++;
		} 
		else if (RSRQ < 0)
		{
			RSRQ_nCount[4]++;
		} 
		else if (RSRQ>= 0)
		{
			RSRQ_nCount[5]++;
		}
	}
 }
 else
		{
			XdrCount++;
			iduration += values.duration;
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
				 
			// 只有xdr，才算用户的个数，mr不用算
			if (values.IMSI > 0)
			{
				imsiSet.add(values.IMSI);
			}
			UserCount_4G=imsiSet.size();
		}
		if(isGrid==false)
		{
			overlapden++;
			if (values.sfcnJamCellCount > 3)
			{
				overlapnum++;
			}
		}
	}

	@Override
	public void doSampleLT(DT_Sample_4G sample) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void doSampleDX(DT_Sample_4G sample) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String toLine() {
		StringBuffer sb = new StringBuffer();
		// sb.append(samKey);sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);

		sb.append(icityid);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		if(isGrid==false)
			{
			sb.append(iLAC);			
			sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
			sb.append(iCI);
			sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
			}
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
		sb.append(itllongitude);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(itllatitude);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(ibrlongitude);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(ibrlatitude);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);

		sb.append(RSRP_nTotal);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(RSRP_nSum);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);

		for (int i = 0; i < RSRP_nCount.length-1; i++)
		{
			sb.append(RSRP_nCount[i]);
			sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		}
		sb.append(SINR_nTotal);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(SINR_nSum);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);

		for (int i = 0; i < SINR_nCount.length; i++)
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

		sb.append(UserCount_4G);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(UserCount_3G);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(UserCount_2G);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(UserCount_4GFall);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(XdrCount);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(MrCount);
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
		if(isGrid==false)
		{
			sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
			sb.append(overlapnum);
			sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
			sb.append(overlapden);
		}
		return sb.toString();
	}

}
