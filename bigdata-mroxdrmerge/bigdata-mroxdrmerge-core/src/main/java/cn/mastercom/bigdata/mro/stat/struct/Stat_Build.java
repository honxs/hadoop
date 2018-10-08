package cn.mastercom.bigdata.mro.stat.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.TimeHelper;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;

public class Stat_Build implements IStat_4G,Serializable
{
	public int iCityID;
	public int iBuildingID;
	public int iHeight;
	public int ifreq;
	public int iTime;
	public int iMRCnt;
	public int iMRRSRQCnt;
	public int iMRSINRCnt;
	public float fRSRPValue;
	public float fRSRQValue;
	public float fSINRValue;
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

	public void doFirstSample(DT_Sample_4G sample, int ifreq)
	{
		iCityID = sample.cityID;
		iBuildingID = sample.ibuildingID;
		iHeight = sample.iheight;
		iTime = FormatTime.RoundTimeForHour(sample.itime);
		this.ifreq = ifreq;
	}

	public void doSample(DT_Sample_4G sample)
	{
		doSample(sample.LteScRSRP, sample.LteScRSRQ, sample.LteScSinrUL, sample.locSource, sample.overlapSameEarfcn,
				sample.OverlapAll);
	}

	public void doSampleLT(DT_Sample_4G sample)
	{
		doSample(sample.lt_freq[0].LteNcRSRP, sample.lt_freq[0].LteNcRSRQ, StaticConfig.Int_Abnormal, sample.locSource, sample.overlapSameEarfcn,
				sample.OverlapAll);
	}

	public void doSampleDX(DT_Sample_4G sample)
	{
		doSample(sample.dx_freq[0].LteNcRSRP, sample.dx_freq[0].LteNcRSRQ, StaticConfig.Int_Abnormal, sample.locSource, sample.overlapSameEarfcn,
				sample.OverlapAll);
	}

	public void doSample(int rsrp, int rsrq, int sinrul, int locSource, int Overlap, int OverlapAll)
	{
		if (!(rsrp >= -150 && rsrp <= -30))
		{
			return;
		}
		iMRCnt++;
		fRSRPValue += rsrp;
		if (rsrq > -10000)
		{
			iMRRSRQCnt++;
			fRSRQValue += rsrq;
		}
		if (sinrul >= -1000 && sinrul <= 1000)
		{
			iMRSINRCnt++;
			fSINRValue += sinrul;
		}
		if (rsrp >= -95)
		{
			iMRCnt_95++;
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

		if ((rsrp >= -100) && (sinrul >= 0))
		{
			iRSRP100_SINR0++;
		}
		if ((rsrp >= -105) && (sinrul >= 0))
		{
			iRSRP105_SINR0++;
		}
		if ((rsrp >= -110) && (sinrul >= 3))
		{
			iRSRP110_SINR3++;
		}
		if ((rsrp >= -110) && (sinrul >= 0))
		{
			iRSRP110_SINR0++;
		}
		if (sinrul >= 0)
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
		fSINRMax = getMax(fSINRMax, sinrul);
		fSINRMin = getMin(fSINRMin, sinrul);
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

	public String toLine()
	{
		StringBuffer bf = new StringBuffer();
		bf.append(iCityID);
		bf.append(spliter);
		bf.append(iBuildingID);
		bf.append(spliter);
		bf.append(iHeight);
		bf.append(spliter);
		bf.append(ifreq);
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

	public static Stat_Build FillData(String[] vals, int pos)
	{
		int i = pos;
		Stat_Build build = new Stat_Build();
		try
		{
			build.iCityID = Integer.parseInt(vals[i++]);
			build.iBuildingID = Integer.parseInt(vals[i++]);
			build.iHeight = Integer.parseInt(vals[i++]);
			build.ifreq = Integer.parseInt(vals[i++]);
			build.iTime = TimeHelper.getRoundDayTime(Integer.parseInt(vals[i++]));
			build.iMRCnt = Integer.parseInt(vals[i++]);
			build.iMRRSRQCnt = Integer.parseInt(vals[i++]);
			build.iMRSINRCnt = Integer.parseInt(vals[i++]);
			build.fRSRPValue = Float.parseFloat(vals[i++]);
			build.fRSRQValue = Float.parseFloat(vals[i++]);
			build.fSINRValue = Float.parseFloat(vals[i++]);
			build.iMRCnt_95 = Integer.parseInt(vals[i++]);
			build.iMRCnt_100 = Integer.parseInt(vals[i++]);
			build.iMRCnt_103 = Integer.parseInt(vals[i++]);
			build.iMRCnt_105 = Integer.parseInt(vals[i++]);
			build.iMRCnt_110 = Integer.parseInt(vals[i++]);
			build.iMRCnt_113 = Integer.parseInt(vals[i++]);
			build.iMRCnt_128 = Integer.parseInt(vals[i++]);
			build.iRSRP100_SINR0 = Integer.parseInt(vals[i++]);
			build.iRSRP105_SINR0 = Integer.parseInt(vals[i++]);
			build.iRSRP110_SINR3 = Integer.parseInt(vals[i++]);
			build.iRSRP110_SINR0 = Integer.parseInt(vals[i++]);
			build.iSINR_0 = Integer.parseInt(vals[i++]);
			build.iRSRQ_14 = Integer.parseInt(vals[i++]);
			build.fOverlapTotal = Float.parseFloat(vals[i++]);
			build.iOverlapMRCnt = Integer.parseInt(vals[i++]);
			build.fOverlapTotalAll = Float.parseFloat(vals[i++]);
			build.iOverlapMRCntAll = Integer.parseInt(vals[i++]);
			build.fRSRPMax = Float.parseFloat(vals[i++]);
			build.fRSRPMin = Float.parseFloat(vals[i++]);
			build.fRSRQMax = Float.parseFloat(vals[i++]);
			build.fRSRQMin = Float.parseFloat(vals[i++]);
			build.fSINRMax = Float.parseFloat(vals[i++]);
			build.fSINRMin = Float.parseFloat(vals[i++]);
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error, "Scene_Build filldata error","", e);
		}
		return build;
	}

}
