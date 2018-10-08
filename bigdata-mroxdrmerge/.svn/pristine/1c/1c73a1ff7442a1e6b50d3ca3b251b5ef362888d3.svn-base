package cn.mastercom.bigdata.mro.stat.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.NC_LTE;
import cn.mastercom.bigdata.base.constant.DataConstant;
import cn.mastercom.bigdata.util.TimeHelper;

public class CSStat_CellFreq implements IStat_4G,Serializable{
	protected int icityid;
	protected int startTime;
	protected int endTime;
	protected int iLAC;
	protected int wRAC;//没有赋值和输出
	protected long iCI;
	protected int freq;
	protected int pci;
	
	protected int iduration;
	protected int idistance;//输出了但是没有赋值
	protected int isamplenum;
	
	protected int freqCount;//没有赋值和输出

	protected long RSRP_nTotal;    // 总数
	protected long RSRP_nSum;  // 总和
	protected long[] RSRP_nCount=new long[6]; // [-141,-110),[-110,-95),[-95,-80),[-80,-65),[-65,-50),[-50,)
	protected long RSRP_nCount7; // [-141,-113)
	
	protected long RSRQ_nTotal;    // 总数
	protected long RSRQ_nSum;  // 总和
	protected long[] RSRQ_nCount=new long[6]; // [-40,-20),[-20,-16),[-16,-12),[-12,-8),[-8,0),[0,40]
	int rsrp;
	int rsrq;
	int i;
	public void doFirstSample(DT_Sample_4G sample,NC_LTE item,int i)
	{
		this.i=i;
		icityid=sample.cityID;
		iLAC=sample.iLAC;
		iCI=sample.Eci;
		freq=item.LteNcEarfcn;
		pci=item.LteNcPci;
		
		startTime=TimeHelper.getRoundDayTime(sample.itime);
		endTime=startTime+ 86400;
	}
	@Override
	public void doSample(DT_Sample_4G sample) {
		rsrp=sample.getNclte_Freq().get(i).LteNcRSRP;
		rsrq=sample.getNclte_Freq().get(i).LteNcRSRQ;
		boolean isSampleMro = sample.flag.toUpperCase().equals("MRO");
		boolean isSampleMre = sample.flag.toUpperCase().equals("MRE");
		// 小区统计
		iduration += sample.duration;
		if (isSampleMro || isSampleMre)
		{
			isamplenum++;

			if (rsrp >= -141 && rsrp <= 200)
			{
				RSRP_nTotal++;

				RSRP_nSum += rsrp;
				if (rsrp < -113)
				{
					RSRP_nCount7++;
				}
				// RSRP_nCount[6]; //
				// [-141,-110),[-110,-105),[-105,-100),[-100,-95),[-95,-85),[-85,)
				if (rsrp < -110)
				{
					RSRP_nCount[0]++;
				}
				else if (rsrp < -105)
				{
					RSRP_nCount[1]++;
				}
				else if (rsrp < -100)
				{
					RSRP_nCount[2]++;
				}
				else if (rsrp < -95)
				{
					RSRP_nCount[3]++;
				}
				else if (rsrp < -85)
				{
					RSRP_nCount[4]++;
				}
				else
				{
					RSRP_nCount[5]++;
				}
			}
			if (rsrq >= -40 && rsrq <= 40)
			{
				RSRQ_nTotal++; // 总数
				RSRQ_nSum += rsrq;
				// int SINR_nCount[8]; //
				// [-40,-20),[-20,-16),[-16,-12),[-12,-8),[-8,0),[0,40]
				if (sample.LteScSinrUL < -20)
				{
					RSRQ_nCount[0]++;
				}
				else if (sample.LteScSinrUL < -16)
				{
					RSRQ_nCount[1]++;
				}
				else if (sample.LteScSinrUL < -12)
				{
					RSRQ_nCount[2]++;
				}
				else if (sample.LteScSinrUL < -8)
				{
					RSRQ_nCount[3]++;
				}
				else if (sample.LteScSinrUL < 0)
				{
					RSRQ_nCount[4]++;
				}
				else if (sample.LteScSinrUL <= 40)
				{
					RSRQ_nCount[5]++;
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
		// sb.append(samKey);sb.append(TabMark);
		sb.append(icityid);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(iLAC);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(iCI);
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
		sb.append(RSRP_nTotal);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(RSRP_nSum);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		for (int i = 0; i < RSRP_nCount.length; i++)
		{
			sb.append(RSRP_nCount[i]);
			sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		}
		sb.append(RSRQ_nTotal);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(RSRQ_nSum);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		for (int i = 0; i < RSRQ_nCount.length; i++)
		{
			if (i == RSRQ_nCount.length - 1)
			{
				sb.append(RSRQ_nCount[i]);
			}
			else
			{
				sb.append(RSRQ_nCount[i]);
				sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
			}
		}
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(RSRP_nCount7);
		sb.append(DataConstant.DEFAULT_COLUMN_SEPARATOR);
		sb.append(pci);
		return sb.toString();
	}

}
