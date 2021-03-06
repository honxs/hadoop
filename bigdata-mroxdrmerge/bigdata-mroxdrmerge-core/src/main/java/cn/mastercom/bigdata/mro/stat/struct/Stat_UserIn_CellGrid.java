package cn.mastercom.bigdata.mro.stat.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.Func;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.TimeHelper;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;

public class Stat_UserIn_CellGrid extends Stat_OutGrid implements IStat_4G,Serializable{
	
	public int cityID;
	public long imsi;
	public int iBuildingID;
	public int iHeight;
	public Long iECI;
	public int iTime;
	public int tllongitude;
	public int tllatitude;
	public int brlongitude;
	public int brlatitude;
	
	public static final String spliter = "\t";
	
	@Override
	public String toLine() {
		StringBuffer bf = new StringBuffer();
		bf.append(cityID);
		bf.append(spliter);
		bf.append(Func.getEncrypt(imsi));
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
	
	public String mergeStatToLine() {
		StringBuffer bf = new StringBuffer();
		bf.append(cityID);
		bf.append(spliter);
		bf.append(imsi);
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
	
	public void doFirstSample(DT_Sample_4G sample){
		imsi = sample.IMSI;
		cityID = sample.cityID;
		iBuildingID = sample.ibuildingID;
		iHeight = sample.iheight;
		iTime = FormatTime.RoundTimeForHour(sample.itime);
		tllongitude = sample.grid.tllongitude;
		tllatitude = sample.grid.tllatitude;
		brlongitude = sample.grid.brlongitude;
		brlatitude = sample.grid.brlatitude;
		iECI = sample.Eci;
	}

	@Override
	public void doSample(DT_Sample_4G sample) {
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

	@Override
	public void doSampleLT(DT_Sample_4G sample) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void doSampleDX(DT_Sample_4G sample) {
		// TODO Auto-generated method stub
		
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
	
	public static Stat_UserIn_CellGrid FillData(String[] vals, int sPos){
		int i = sPos;
		Stat_UserIn_CellGrid userInGrid = new Stat_UserIn_CellGrid();
		try
		{
			userInGrid.imsi = Long.parseLong(vals[i++]);
			userInGrid.cityID = Integer.parseInt(vals[i++]);
			userInGrid.iBuildingID = Integer.parseInt(vals[i++]);
			userInGrid.iHeight = Integer.parseInt(vals[i++]);
			userInGrid.tllongitude = Integer.parseInt(vals[i++]);
			userInGrid.tllatitude = Integer.parseInt(vals[i++]);
			userInGrid.brlongitude = Integer.parseInt(vals[i++]);
			userInGrid.brlatitude = Integer.parseInt(vals[i++]);
			userInGrid.iECI = Long.parseLong(vals[i++]);
			userInGrid.iTime = TimeHelper.getRoundDayTime(Integer.parseInt(vals[i++]));
			userInGrid.iMRCnt = Integer.parseInt(vals[i++]);
			userInGrid.iMRRSRQCnt = Integer.parseInt(vals[i++]);
			userInGrid.iMRSINRCnt = Integer.parseInt(vals[i++]);

			userInGrid.fRSRPValue = Float.parseFloat(vals[i++]);
			userInGrid.fRSRQValue = Float.parseFloat(vals[i++]);
			userInGrid.fSINRValue = Float.parseFloat(vals[i++]);
			userInGrid.iMRCnt_95 = Integer.parseInt(vals[i++]);
			userInGrid.iMRCnt_100 = Integer.parseInt(vals[i++]);
			userInGrid.iMRCnt_103 = Integer.parseInt(vals[i++]);
			userInGrid.iMRCnt_105 = Integer.parseInt(vals[i++]);
			userInGrid.iMRCnt_110 = Integer.parseInt(vals[i++]);
			userInGrid.iMRCnt_113 = Integer.parseInt(vals[i++]);
			userInGrid.iMRCnt_128 = Integer.parseInt(vals[i++]);
			userInGrid.iRSRP100_SINR0 = Integer.parseInt(vals[i++]);
			userInGrid.iRSRP105_SINR0 = Integer.parseInt(vals[i++]);
			userInGrid.iRSRP110_SINR3 = Integer.parseInt(vals[i++]);
			userInGrid.iRSRP110_SINR0 = Integer.parseInt(vals[i++]);
			userInGrid.iSINR_0 = Integer.parseInt(vals[i++]);
			userInGrid.iRSRQ_14 = Integer.parseInt(vals[i++]);
			userInGrid.fOverlapTotal = Float.parseFloat(vals[i++]);
			userInGrid.iOverlapMRCnt = Integer.parseInt(vals[i++]);
			userInGrid.fOverlapTotalAll = Float.parseFloat(vals[i++]);
			userInGrid.iOverlapMRCntAll = Integer.parseInt(vals[i++]);
			userInGrid.fRSRPMax = Float.parseFloat(vals[i++]);
			userInGrid.fRSRPMin = Float.parseFloat(vals[i++]);
			userInGrid.fRSRQMax = Float.parseFloat(vals[i++]);
			userInGrid.fRSRQMin = Float.parseFloat(vals[i++]);
			userInGrid.fSINRMax = Float.parseFloat(vals[i++]);
			userInGrid.fSINRMin = Float.parseFloat(vals[i++]);
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error,"Stat_UserIn_CellGrid filldata error", "", e);
		}
		return userInGrid;
	}
}
