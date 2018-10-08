package cn.mastercom.bigdata.mro.stat.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.TimeHelper;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;

public class Stat_In_CellGrid implements IStat_4G,Serializable
{
	public int iCityID;
	public int iBuildingID;
	public int iHeight;
	public int tllongitude;
	public int tllatitude;
	public int brlongitude;
	public int brlatitude;
	public int iECI;
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

	public String toLine()
	{
		StringBuffer bf = new StringBuffer();
		bf.append(iCityID);
		bf.append(spliter);
		bf.append(iBuildingID);
		bf.append(spliter);
		bf.append(iHeight);
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

	public void doFirstSample(DT_Sample_4G sample)
	{
		iCityID = sample.cityID;
		iBuildingID = sample.ibuildingID;
		iHeight = sample.iheight;
		tllongitude = sample.grid.tllongitude;
		tllatitude = sample.grid.tllatitude;
		brlongitude = sample.grid.brlongitude;
		brlatitude = sample.grid.brlatitude;
		iECI = (int) sample.Eci;
		iTime = FormatTime.RoundTimeForHour(sample.itime);
	}

	public void doSample(DT_Sample_4G sample)
	{
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

	public static void main(String args[])
	{
		int a = -1000000;
		float b = -1000000f;
		System.out.println(b == a);

	}

	public static Stat_In_CellGrid FillData(String[] vals, int pos)
	{
		int i = pos;
		Stat_In_CellGrid incellGrid = new Stat_In_CellGrid();
		try
		{
			incellGrid.iCityID = Integer.parseInt(vals[i++]);
			incellGrid.iBuildingID = Integer.parseInt(vals[i++]);
			incellGrid.iHeight = Integer.parseInt(vals[i++]);
			incellGrid.tllongitude = Integer.parseInt(vals[i++]);
			incellGrid.tllatitude = Integer.parseInt(vals[i++]);
			incellGrid.brlongitude = Integer.parseInt(vals[i++]);
			incellGrid.brlatitude = Integer.parseInt(vals[i++]);
			incellGrid.iECI = Integer.parseInt(vals[i++]);
			incellGrid.iTime = TimeHelper.getRoundDayTime(Integer.parseInt(vals[i++]));
			incellGrid.iMRCnt = Integer.parseInt(vals[i++]);
			incellGrid.iMRRSRQCnt = Integer.parseInt(vals[i++]);
			incellGrid.iMRSINRCnt = Integer.parseInt(vals[i++]);

			incellGrid.fRSRPValue = Float.parseFloat(vals[i++]);
			incellGrid.fRSRQValue = Float.parseFloat(vals[i++]);
			incellGrid.fSINRValue = Float.parseFloat(vals[i++]);
			incellGrid.iMRCnt_95 = Integer.parseInt(vals[i++]);
			incellGrid.iMRCnt_100 = Integer.parseInt(vals[i++]);
			incellGrid.iMRCnt_103 = Integer.parseInt(vals[i++]);
			incellGrid.iMRCnt_105 = Integer.parseInt(vals[i++]);
			incellGrid.iMRCnt_110 = Integer.parseInt(vals[i++]);
			incellGrid.iMRCnt_113 = Integer.parseInt(vals[i++]);
			incellGrid.iMRCnt_128 = Integer.parseInt(vals[i++]);
			incellGrid.iRSRP100_SINR0 = Integer.parseInt(vals[i++]);
			incellGrid.iRSRP105_SINR0 = Integer.parseInt(vals[i++]);
			incellGrid.iRSRP110_SINR3 = Integer.parseInt(vals[i++]);
			incellGrid.iRSRP110_SINR0 = Integer.parseInt(vals[i++]);
			incellGrid.iSINR_0 = Integer.parseInt(vals[i++]);
			incellGrid.iRSRQ_14 = Integer.parseInt(vals[i++]);
			// incellGrid.iASNei_MRCnt = Integer.parseInt(vals[i++]);
			// incellGrid.fASNei_RSRPValue = Float.parseFloat(vals[i++]);
			incellGrid.fOverlapTotal = Float.parseFloat(vals[i++]);
			incellGrid.iOverlapMRCnt = Integer.parseInt(vals[i++]);
			incellGrid.fOverlapTotalAll = Float.parseFloat(vals[i++]);
			incellGrid.iOverlapMRCntAll = Integer.parseInt(vals[i++]);
			incellGrid.fRSRPMax = Float.parseFloat(vals[i++]);
			incellGrid.fRSRPMin = Float.parseFloat(vals[i++]);
			incellGrid.fRSRQMax = Float.parseFloat(vals[i++]);
			incellGrid.fRSRQMin = Float.parseFloat(vals[i++]);
			incellGrid.fSINRMax = Float.parseFloat(vals[i++]);
			incellGrid.fSINRMin = Float.parseFloat(vals[i++]);
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error,"Stat_In_CellGrid filldata error", "", e);
		}
		return incellGrid;
	}

	@Override
	public void doSampleLT(DT_Sample_4G sample)
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void doSampleDX(DT_Sample_4G sample)
	{
		// TODO Auto-generated method stub
		
	}
}