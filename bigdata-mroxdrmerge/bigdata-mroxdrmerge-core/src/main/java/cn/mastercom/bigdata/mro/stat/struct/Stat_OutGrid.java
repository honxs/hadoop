package cn.mastercom.bigdata.mro.stat.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.TimeHelper;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;

public class Stat_OutGrid implements IStat_4G,Serializable
{
	public int iCityID;
	public int tllongitude;
	public int tllatitude;
	public int brlongitude;
	public int brlatitude;
	public int ifreq;
	public int iTime;
	
//	private Stater stater;
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
		bf.append(tllongitude);
		bf.append(spliter);
		bf.append(tllatitude);
		bf.append(spliter);
		bf.append(brlongitude);
		bf.append(spliter);
		bf.append(brlatitude);
		bf.append(spliter);
		bf.append(ifreq);
		bf.append(spliter);
		bf.append(iTime);
		bf.append(spliter);
//		stater.toString(bf);
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

	public void doFirstSample(DT_Sample_4G sample, int ifreq)
	{
		iCityID = sample.cityID;
		tllongitude = sample.grid.tllongitude;
		tllatitude = sample.grid.tllatitude;
		brlongitude = sample.grid.brlongitude;
		brlatitude = sample.grid.brlatitude;
		iTime = FormatTime.RoundTimeForHour(sample.itime);
		this.ifreq = ifreq;
	}

	public void doSample(int rsrp, int rsrq, int sinr, int Overlap, int OverlapAll)
	{
		if (!(rsrp >= -150 && rsrp <= -30))
			return;
		iMRCnt++;
		fRSRPValue += rsrp; // 总的MR的RSRP值，按服务小区计算

		if (rsrq > -10000)
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
		doSample(sample.LteScRSRP, sample.LteScRSRQ, sample.LteScSinrUL, sample.overlapSameEarfcn, sample.OverlapAll);
//		stater.compute(sample.LteScRSRP, sample.LteScRSRQ, sample.LteScSinrUL, sample.Overlap, sample.OverlapAll);
	}

	public void doSampleLT(DT_Sample_4G sample)
	{
		doSample(sample.lt_freq[0].LteNcRSRP, sample.lt_freq[0].LteNcRSRQ, StaticConfig.Int_Abnormal, sample.overlapSameEarfcn,
				sample.OverlapAll);
//		stater.compute(sample.LteScRSRP_LT, sample.LteScRSRQ_LT, StaticConfig.Int_Abnormal, sample.Overlap,
//				sample.OverlapAll);
	}

	public void doSampleDX(DT_Sample_4G sample)
	{
		doSample(sample.dx_freq[0].LteNcRSRP, sample.dx_freq[0].LteNcRSRQ, StaticConfig.Int_Abnormal, sample.overlapSameEarfcn,
				sample.OverlapAll);
//		stater.compute(sample.LteScRSRP_DX, sample.LteScRSRQ_DX, StaticConfig.Int_Abnormal, sample.Overlap,
//				sample.OverlapAll);
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

	public static Stat_OutGrid FillData(String[] vals, int pos)
	{
		int i = pos;
		Stat_OutGrid outGrid = new Stat_OutGrid();
		try
		{
			outGrid.iCityID = Integer.parseInt(vals[i++]);
			outGrid.tllongitude = Integer.parseInt(vals[i++]);
			outGrid.tllatitude = Integer.parseInt(vals[i++]);
			outGrid.brlongitude = Integer.parseInt(vals[i++]);
			outGrid.brlatitude = Integer.parseInt(vals[i++]);
			outGrid.ifreq = Integer.parseInt(vals[i++]);
			outGrid.iTime = TimeHelper.getRoundDayTime(Integer.parseInt(vals[i++]));
//		outGrid.stater = Stater.FillData(vals, i);
			outGrid.iMRCnt = Integer.parseInt(vals[i++]);
			outGrid.iMRRSRQCnt = Integer.parseInt(vals[i++]);
			outGrid.iMRSINRCnt = Integer.parseInt(vals[i++]);
			outGrid.fRSRPValue = Double.parseDouble(vals[i++]);
			outGrid.fRSRQValue = Double.parseDouble(vals[i++]);
			outGrid.fSINRValue = Double.parseDouble(vals[i++]);
			outGrid.iMRCnt_95 = Integer.parseInt(vals[i++]);
			outGrid.iMRCnt_100 = Integer.parseInt(vals[i++]);
			outGrid.iMRCnt_103 = Integer.parseInt(vals[i++]);
			outGrid.iMRCnt_105 = Integer.parseInt(vals[i++]);
			outGrid.iMRCnt_110 = Integer.parseInt(vals[i++]);
			outGrid.iMRCnt_113 = Integer.parseInt(vals[i++]);
			outGrid.iMRCnt_128 = Integer.parseInt(vals[i++]);
			outGrid.iRSRP100_SINR0 = Integer.parseInt(vals[i++]);
			outGrid.iRSRP105_SINR0 = Integer.parseInt(vals[i++]);
			outGrid.iRSRP110_SINR3 = Integer.parseInt(vals[i++]);
			outGrid.iRSRP110_SINR0 = Integer.parseInt(vals[i++]);
			outGrid.iSINR_0 = Integer.parseInt(vals[i++]);
			outGrid.iRSRQ_14 = Integer.parseInt(vals[i++]);
			outGrid.fOverlapTotal = Float.parseFloat(vals[i++]);
			outGrid.iOverlapMRCnt = Integer.parseInt(vals[i++]);
			outGrid.fOverlapTotalAll = Float.parseFloat(vals[i++]);
			outGrid.iOverlapMRCntAll = Integer.parseInt(vals[i++]);
			outGrid.fRSRPMax = Float.parseFloat(vals[i++]);
			outGrid.fRSRPMin = Float.parseFloat(vals[i++]);
			outGrid.fRSRQMax = Float.parseFloat(vals[i++]);
			outGrid.fRSRQMin = Float.parseFloat(vals[i++]);
			outGrid.fSINRMax = Float.parseFloat(vals[i++]);
			outGrid.fSINRMin = Float.parseFloat(vals[i++]);
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error,"Stat_OutGrid filldata error", "", e);
		}
		return outGrid;

	}
}
