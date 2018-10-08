package cn.mastercom.bigdata.mro.stat.struct;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.util.FormatTime;

public class Stat_UserOutGrid extends Stat_OutGrid implements IStat_4G{
	
	public long imsi;
	public long iECI;
	public int iTime;
	public int tllongitude;
	public int tllatitude;
	public int brlongitude;
	public int brlatitude;
	
	public static final String spliter = "\t";
	
	@Override
	public String toLine() {
		StringBuffer bf = new StringBuffer();
		bf.append(imsi);
		bf.append(spliter);
		bf.append(tllongitude);
		bf.append(spliter);
		bf.append(tllatitude);
		bf.append(spliter);
		bf.append(brlongitude);
		bf.append(spliter);
		bf.append(brlatitude);
		bf.append(spliter);
		bf.append(iECI);
		bf.append(spliter);
		bf.append(iTime);
		bf.append(spliter);		
		bf.append(iMRCnt);
		bf.append(spliter);
		bf.append(iMRRSRQCnt);
		bf.append(spliter);
		bf.append(iMRSINRCnt);
		bf.append(spliter);
		bf.append(fRSRPValue);
		bf.append(spliter);
		bf.append(fRSRQValue);
		bf.append(spliter);
		bf.append(fSINRValue);
		bf.append(spliter);
		bf.append(iMRCnt_95);
		bf.append(spliter);
		bf.append(iMRCnt_100);
		bf.append(spliter);
		bf.append(iMRCnt_103);
		bf.append(spliter);
		bf.append(iMRCnt_105);
		bf.append(spliter);
		bf.append(iMRCnt_110);
		bf.append(spliter);
		bf.append(iMRCnt_113);
		bf.append(spliter);
		bf.append(iMRCnt_128);
		bf.append(spliter);
		bf.append(iRSRP100_SINR0);
		bf.append(spliter);
		bf.append(iRSRP105_SINR0);
		bf.append(spliter);
		bf.append(iRSRP110_SINR3);
		bf.append(spliter);
		bf.append(iRSRP110_SINR0);
		bf.append(spliter);
		bf.append(iSINR_0);
		bf.append(spliter);
		bf.append(iRSRQ_14);
		bf.append(spliter);
		bf.append(fOverlapTotal);
		bf.append(spliter);
		bf.append(iOverlapMRCnt);
		bf.append(spliter);
		bf.append(fOverlapTotalAll);
		bf.append(spliter);
		bf.append(iOverlapMRCntAll);
		bf.append(spliter);
		bf.append(fRSRPMax);
		bf.append(spliter);
		bf.append(fRSRPMin);
		bf.append(spliter);
		bf.append(fRSRQMax);
		bf.append(spliter);
		bf.append(fRSRQMin);
		bf.append(spliter);
		bf.append(fSINRMax);
		bf.append(spliter);
		bf.append(fSINRMin);
		return bf.toString();
	}
	
	public void doFirstSample(DT_Sample_4G sample){
		imsi = sample.IMSI;
		iECI = sample.Eci;
		iTime = FormatTime.RoundTimeForHour(sample.itime);
		tllongitude = sample.grid.tllongitude;
		tllatitude = sample.grid.tllatitude;
		brlongitude = sample.grid.brlongitude;
		brlatitude = sample.grid.brlatitude;
	}
	
	@Override
	public void doSample(DT_Sample_4G sample) {
		// TODO Auto-generated method stub
		if (!(sample.LteScRSRP >= -150 && sample.LteScRSRP <= -30))
			return;

		iMRCnt++;
		fRSRPValue += sample.LteScRSRP;

		if (sample.LteScRSRQ != -1000000)
		{
			iMRRSRQCnt++;
			fRSRQValue += sample.LteScRSRQ;
		}
		if (sample.LteScSinrUL >= -1000 && sample.LteScSinrUL <= 1000)
		{
			iMRSINRCnt++;
			fSINRValue += sample.LteScSinrUL;
		}
		if (sample.LteScRSRP >= -95)
		{
			iMRCnt_95++;
		}
		if (sample.LteScRSRP >= -100)
		{
			iMRCnt_100++;
		}
		if (sample.LteScRSRP >= -103)
		{
			iMRCnt_103++;
		}
		if (sample.LteScRSRP >= -105)
		{
			iMRCnt_105++;
		}
		if (sample.LteScRSRP > -110)
		{
			iMRCnt_110++;
		}
		if (sample.LteScRSRP >= -113)
		{
			iMRCnt_113++;
		}
		if (sample.LteScRSRP >= -128)
		{
			iMRCnt_128++;
		}
		// rsrp_sinr
		if ((sample.LteScRSRP >= -100) && (sample.LteScSinrUL >= 0))
		{
			iRSRP100_SINR0++;
		}
		if ((sample.LteScRSRP >= -105) && (sample.LteScSinrUL >= 0))
		{
			iRSRP105_SINR0++;
		}
		if ((sample.LteScRSRP >= -110) && (sample.LteScSinrUL >= 3))
		{
			iRSRP110_SINR3++;
		}
		if ((sample.LteScRSRP >= -110) && (sample.LteScSinrUL >= 0))
		{
			iRSRP110_SINR0++;
		}
		if (sample.LteScSinrUL >= 0)
		{
			iSINR_0++;
		}
		if (sample.LteScRSRQ >= -14)
		{
			iRSRQ_14++;
		}
		fOverlapTotal += sample.overlapSameEarfcn;
		if (sample.overlapSameEarfcn >= 4)
		{
			iOverlapMRCnt++;
		}
		fOverlapTotalAll += sample.OverlapAll;
		if (sample.OverlapAll >= 4)
		{
			iOverlapMRCntAll++;
		}
		fRSRPMax = getMax(fRSRPMax, sample.LteScRSRP);
		fRSRPMin = getMin(fRSRPMin, sample.LteScRSRP);
		fRSRQMax = getMax(fRSRQMax, sample.LteScRSRQ);
		fRSRQMin = getMin(fRSRQMin, sample.LteScRSRQ);
		fSINRMax = getMax(fSINRMax, sample.LteScSinrUL);
		fSINRMin = getMin(fSINRMin, sample.LteScSinrUL);
	}
	
	private float getMax(float valueMax, int value)
	{
		if (valueMax == StaticConfig.Int_Abnormal || valueMax < value)
		{
			return value;
		}
		return valueMax;
	}

	private float getMin(float valueMin, int value)
	{
		if (value == StaticConfig.Int_Abnormal)
			return valueMin;
		if (valueMin == StaticConfig.Int_Abnormal || valueMin > value)
		{
			return value;
		}
		return valueMin;
	}

	@Override
	public void doSampleLT(DT_Sample_4G sample) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void doSampleDX(DT_Sample_4G sample) {
		// TODO Auto-generated method stub
		
	}

}
