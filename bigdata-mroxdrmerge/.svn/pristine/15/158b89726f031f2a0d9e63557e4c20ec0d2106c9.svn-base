package cn.mastercom.bigdata.mro.loc.hsr.stat;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mro.stat.struct.IStat_4G;
import cn.mastercom.bigdata.util.TimeHelper;

public class Stat_TrainSeg implements IStat_4G
{
	public int iCityID;
	public long lTrainKey;
	public int iSegmentId;
	public int iTime;
	public int iMRCnt;
	public int iMRRSRQCnt;
	public int iMRSINRCnt;
	public double fRSRPValue;
	public double fRSRQValue;
	public double fSINRValue;
	public int iMRCnt_95;
	public int iMRCnt_100;
	public int iMRCnt_103;
	public int iMRCnt_105;
	public int iMRCnt_110;
	public int iMRCnt_113;
	public int iMRCnt_128;
	public int iRSRP100_SINR0;
	public int iRSRP105_SINR0;
	public int iRSRP110_SINR3;
	public int iRSRP110_SINR0;
	public int iSINR_0;
	public int iRSRQ_14;
	public float fOverlapTotal;
	public int iOverlapMRCnt;
	public float fOverlapTotalAll;
	public int iOverlapMRCntAll;
	public float fRSRPMax = StaticConfig.Int_Abnormal;
	public float fRSRPMin = StaticConfig.Int_Abnormal;
	public float fRSRQMax = StaticConfig.Int_Abnormal;
	public float fRSRQMin = StaticConfig.Int_Abnormal;
	public float fSINRMax = StaticConfig.Int_Abnormal;
	public float fSINRMin = StaticConfig.Int_Abnormal;
	public static final String spliter = "\t";

	public String toLine()
	{
		StringBuffer bf = new StringBuffer();
		bf.append(iCityID);
		bf.append(spliter);
		bf.append(lTrainKey);
		bf.append(spliter);
		bf.append(iSegmentId);
		bf.append(spliter);
//		bf.append(tllongitude);
//		bf.append(spliter);
//		bf.append(tllatitude);
//		bf.append(spliter);
//		bf.append(brlongitude);
//		bf.append(spliter);
//		bf.append(brlatitude);
//		bf.append(spliter);
//		bf.append(ifreq);
//		bf.append(spliter);
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

	public String roundDayToLine()
	{
		StringBuffer bf = new StringBuffer();
		bf.append(iCityID);
		bf.append(spliter);
		bf.append(lTrainKey);
		bf.append(spliter);
		bf.append(iSegmentId);
		bf.append(spliter);
//		bf.append(tllongitude);
//		bf.append(spliter);
//		bf.append(tllatitude);
//		bf.append(spliter);
//		bf.append(brlongitude);
//		bf.append(spliter);
//		bf.append(brlatitude);
//		bf.append(spliter);
//		bf.append(ifreq);
//		bf.append(spliter);
		bf.append(TimeHelper.getRoundDayTime(iTime));
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

	public void doFirstSample(DT_Sample_4G sample)
	{
		iCityID = sample.cityID;
		
		lTrainKey = sample.trainKey;
		
		iSegmentId = sample.segmentId;
		
		iTime = sample.itime;
	}

	public void doSample(int rsrp, int rsrq, int sinr, int Overlap, int OverlapAll)
	{
		if (!(rsrp >= -150 && rsrp <= -30))
			return;
		iMRCnt++;
		fRSRPValue += rsrp; // 总的MR的RSRP值，按服务小区计算

		if (rsrq != -1000000)
		{
			iMRRSRQCnt++;
			fRSRQValue += rsrq;
		}
		if (sinr >= -1000 && sinr <= 1000) // 主服上行干扰比
		{
			iMRSINRCnt++;
			fSINRValue += sinr; // 总的MR的RSRQ值，按服务小区计算
		}
		if (rsrp >= -95)
		{
			iMRCnt_95++; // 大于等于-95dB的采样点数，按服务小区计算
		}
		if (rsrp >= -100)
		{
			iMRCnt_100++;
		}
		if (rsrp >= -103)
		{
			iMRCnt_103++;
		}
		if (rsrp >= -105)
		{
			iMRCnt_105++;
		}
		if (rsrp >= -110)
		{
			iMRCnt_110++;
		}
		if (rsrp >= -113)
		{
			iMRCnt_113++;
		}
		if (rsrp >= -128)
		{
			iMRCnt_128++;
		}
		// rsrp_sinr
		if ((rsrp >= -100) && (sinr >= 0))
		{
			iRSRP100_SINR0++;

		}
		if ((rsrp >= -105) && (sinr >= 0))
		{
			iRSRP105_SINR0++;

		}
		if ((rsrp >= -110) && (sinr >= 3))
		{
			iRSRP110_SINR3++;

		}
		if ((rsrp >= -110) && (sinr >= 0))
		{
			iRSRP110_SINR0++;
		}
		if (sinr >= 0)
		{
			iSINR_0++;
		}
		if (rsrq >= -14)
		{
			iRSRQ_14++;
		}

		fOverlapTotal += Overlap;
		if (Overlap >= 4)
		{
			iOverlapMRCnt++;
		}
		fOverlapTotalAll += OverlapAll;
		if (OverlapAll >= 4)
		{
			iOverlapMRCntAll++;
		}

		fRSRPMax = getMax(fRSRPMax, rsrp);
		fRSRPMin = getMin(fRSRPMin, rsrp);
		fRSRQMax = getMax(fRSRQMax, rsrq);
		fRSRQMin = getMin(fRSRQMin, rsrq);
		fSINRMax = getMax(fSINRMax, sinr);
		fSINRMin = getMin(fSINRMin, sinr);
	}

	public void doSample(DT_Sample_4G sample)
	{
		iTime = Math.min(iTime, sample.itime);
		doSample(sample.LteScRSRP, sample.LteScRSRQ, sample.LteScSinrUL, sample.overlapSameEarfcn, sample.OverlapAll);
	}

	public void doSampleLT(DT_Sample_4G sample)
	{
		iTime = Math.min(iTime, sample.itime);
		doSample(sample.lt_freq[0].LteNcRSRP, sample.lt_freq[0].LteNcRSRQ, StaticConfig.Int_Abnormal, sample.overlapSameEarfcn, sample.OverlapAll);
	}

	public void doSampleDX(DT_Sample_4G sample)
	{
		iTime = Math.min(iTime, sample.itime);
		doSample(sample.dx_freq[0].LteNcRSRP, sample.dx_freq[0].LteNcRSRQ, StaticConfig.Int_Abnormal, sample.overlapSameEarfcn, sample.OverlapAll);
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

	public static Stat_TrainSeg FillData(String[] vals, int pos)
	{
		int i = pos;
		Stat_TrainSeg trainSeg = new Stat_TrainSeg();
		trainSeg.iCityID = Integer.parseInt(vals[i++]);
		trainSeg.lTrainKey = Long.parseLong(vals[i++]);
		trainSeg.iSegmentId = Integer.parseInt(vals[i++]);
		trainSeg.iTime = Integer.parseInt(vals[i++]);
		trainSeg.iMRCnt = Integer.parseInt(vals[i++]);
		trainSeg.iMRRSRQCnt = Integer.parseInt(vals[i++]);
		trainSeg.iMRSINRCnt = Integer.parseInt(vals[i++]);
		trainSeg.fRSRPValue = Double.parseDouble(vals[i++]);
		trainSeg.fRSRQValue = Double.parseDouble(vals[i++]);
		trainSeg.fSINRValue = Double.parseDouble(vals[i++]);
		trainSeg.iMRCnt_95 = Integer.parseInt(vals[i++]);
		trainSeg.iMRCnt_100 = Integer.parseInt(vals[i++]);
		trainSeg.iMRCnt_103 = Integer.parseInt(vals[i++]);
		trainSeg.iMRCnt_105 = Integer.parseInt(vals[i++]);
		trainSeg.iMRCnt_110 = Integer.parseInt(vals[i++]);
		trainSeg.iMRCnt_113 = Integer.parseInt(vals[i++]);
		trainSeg.iMRCnt_128 = Integer.parseInt(vals[i++]);
		trainSeg.iRSRP100_SINR0 = Integer.parseInt(vals[i++]);
		trainSeg.iRSRP105_SINR0 = Integer.parseInt(vals[i++]);
		trainSeg.iRSRP110_SINR3 = Integer.parseInt(vals[i++]);
		trainSeg.iRSRP110_SINR0 = Integer.parseInt(vals[i++]);
		trainSeg.iSINR_0 = Integer.parseInt(vals[i++]);
		trainSeg.iRSRQ_14 = Integer.parseInt(vals[i++]);
		trainSeg.fOverlapTotal = Float.parseFloat(vals[i++]);
		trainSeg.iOverlapMRCnt = Integer.parseInt(vals[i++]);
		trainSeg.fOverlapTotalAll = Float.parseFloat(vals[i++]);
		trainSeg.iOverlapMRCntAll = Integer.parseInt(vals[i++]);
		trainSeg.fRSRPMax = Float.parseFloat(vals[i++]);
		trainSeg.fRSRPMin = Float.parseFloat(vals[i++]);
		trainSeg.fRSRQMax = Float.parseFloat(vals[i++]);
		trainSeg.fRSRQMin = Float.parseFloat(vals[i++]);
		trainSeg.fSINRMax = Float.parseFloat(vals[i++]);
		trainSeg.fSINRMin = Float.parseFloat(vals[i++]);
		return trainSeg;

	}
}
