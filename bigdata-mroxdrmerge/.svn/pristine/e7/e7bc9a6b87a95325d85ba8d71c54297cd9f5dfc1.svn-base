package cn.mastercom.bigdata.mro.stat.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.NC_LTE;
import cn.mastercom.bigdata.base.constant.DataConstant;
import cn.mastercom.bigdata.util.TimeHelper;

public class CSStat_GridFreq implements IStat_4G,Serializable{
	protected int icityid;
	protected int freq;
	protected int startTime;
	protected int endTime;
	protected int iduration;//没有统计有输出
	protected int idistance;//没有统计有输出
	protected int isamplenum;

	protected int itllongitude;
	protected int itllatitude;
	protected int ibrlongitude;
	protected int ibrlatitude;
	protected long RSRP_nTotal;    // 总数
	protected long RSRP_nSum;  // 总和
	protected long[] RSRP_nCount=new long[6]; // [-141,-110),[-110,-95),[-95,-80),[-80,-65),[-65,-50),[-50,)
	protected long RSRP_nCount7;//[-141,-113)
	protected long SINR_nTotal;    // 总数	  没有统计有输出
	protected long SINR_nSum;  // 总和  	没有统计有输出
	protected long[] SINR_nCount=new long[8]; // [-20,0),[0,5),[5,10),[10,15),[15,20),[20,25),[25,50),[50,)
	//没有统计有输出
	protected int RSRQ_nTotal;
	protected int RSRQ_nSum;//没有输出有统计
	protected long[] RSRQ_nCount = new long[6];
	
	protected long RSRP100_SINR0; // RSRP>-100 and SINR>0
	protected long RSRP105_SINR0; // RSRP>=-105 and SINR>=0
	protected long RSRP110_SINR3; // RSRP>-110 and SINR>3
	protected long RSRP110_SINR0; // RSRP>-110 and SINR>=0
//没有统计有输出
	
	
	protected long UpLen;//没有输出 没有统计下同
	protected long DwLen;//没有输出 没有统计 
	protected float DurationU;
	protected float DurationD;
	protected float AvgUpSpeed;
	protected float MaxUpSpeed;
	protected float AvgDwSpeed;
	protected float MaxDwSpeed;

	protected long UpLen_1M;//没有输出 没有统计下同
	protected long DwLen_1M;//没有输出 没有统计
	protected float DurationU_1M;
	protected float DurationD_1M;
	protected float AvgUpSpeed_1M;
	protected float MaxUpSpeed_1M;
	protected float AvgDwSpeed_1M;
	protected float MaxDwSpeed_1M;
	int RSRP,RSRQ;
	public void doFirstSample(DT_Sample_4G sample ,NC_LTE nc_LTE,int longitude,int latitude,int brlongitude,int brlatitude)
	{
		RSRP=nc_LTE.LteNcRSRP;
		RSRQ=nc_LTE.LteNcRSRQ;
		icityid=sample.cityID;
		freq = nc_LTE.LteNcEarfcn;
		startTime=TimeHelper.getRoundDayTime(sample.itime);
		endTime=startTime + 86400;
	    itllongitude = longitude;
		itllatitude  = latitude;
		ibrlongitude = brlongitude;
		ibrlatitude  = brlatitude;
	}
	@Override
	public void doSample(DT_Sample_4G sample) {
		boolean isMroSample = sample.flag.toUpperCase().equals("MRO");
		boolean isMreSample = sample.flag.toUpperCase().equals("MRE");
			
		isamplenum++;
		if (isMroSample || isMreSample)
		{

			if (RSRP <= -30 && RSRP >= -150)
			{
				RSRP_nTotal++;

				RSRP_nSum += RSRP;

				// RSRP_nCount[6]; //
				// [-141,-110),[-110,-105),[-105,-100),[-100,-95),[-95,-85),[-85,)
				if (RSRP < -113)
				{
					RSRP_nCount7++;
				}
				
				if (RSRP < -110)
				{
					RSRP_nCount[0]++;
				}
				else if (RSRP < -105)
				{
					RSRP_nCount[1]++;
				}
				else if (RSRP < -100)
				{
					RSRP_nCount[2]++;
				}
				else if (RSRP < -95)
				{
					RSRP_nCount[3]++;
				}
				else if (RSRP < -85)
				{
					RSRP_nCount[4]++;
				}
				else
				{
					RSRP_nCount[5]++;
				}
				
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

		sb.append(icityid);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(freq);
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

		for (int i = 0; i < RSRP_nCount.length; i++)
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
		sb.append(RSRP_nCount7);
		return sb.toString();
	}

}
