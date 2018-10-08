package cn.mastercom.bigdata.mro.stat.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.TimeHelper;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;

public class Scene_CellGrid extends Stat_OutGrid implements IStat_4G,Serializable
{
	public int iAreaType;
	public int iAreaID;
	public int iECI;

	public static final String spliter = "\t";

	public String toLine()
	{
		StringBuffer bf = new StringBuffer();
		bf.append(iCityID);
		bf.append(spliter);
		bf.append(iAreaType);
		bf.append(spliter);
		bf.append(iAreaID);
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
		iAreaType = sample.iAreaType;
		iAreaID = sample.iAreaID;
		tllongitude = sample.grid.tllongitude;
		tllatitude = sample.grid.tllatitude;
		brlongitude = sample.grid.brlongitude;
		brlatitude = sample.grid.brlatitude;
		iECI = (int) sample.Eci;
		iTime = FormatTime.RoundTimeForHour(sample.itime);

	}

	public static Scene_CellGrid FillData(String[] vals, int pos)
	{
		int i = pos;
		Scene_CellGrid outcellGrid = new Scene_CellGrid();
		try
		{
			outcellGrid.iCityID = Integer.parseInt(vals[i++]);
			outcellGrid.iAreaType = Integer.parseInt(vals[i++]);
			outcellGrid.iAreaID = Integer.parseInt(vals[i++]);
			outcellGrid.tllongitude = Integer.parseInt(vals[i++]);
			outcellGrid.tllatitude = Integer.parseInt(vals[i++]);
			outcellGrid.brlongitude = Integer.parseInt(vals[i++]);
			outcellGrid.brlatitude = Integer.parseInt(vals[i++]);
			outcellGrid.iECI = Integer.parseInt(vals[i++]);
			outcellGrid.iTime = TimeHelper.getRoundDayTime(Integer.parseInt(vals[i++]));
			outcellGrid.iMRCnt = Integer.parseInt(vals[i++]);
			outcellGrid.iMRRSRQCnt = Integer.parseInt(vals[i++]);
			outcellGrid.iMRSINRCnt = Integer.parseInt(vals[i++]);
			outcellGrid.fRSRPValue = Double.parseDouble(vals[i++]);
			outcellGrid.fRSRQValue = Double.parseDouble(vals[i++]);
			outcellGrid.fSINRValue = Double.parseDouble(vals[i++]);
			outcellGrid.iMRCnt_95 = Integer.parseInt(vals[i++]);
			outcellGrid.iMRCnt_100 = Integer.parseInt(vals[i++]);
			outcellGrid.iMRCnt_103 = Integer.parseInt(vals[i++]);
			outcellGrid.iMRCnt_105 = Integer.parseInt(vals[i++]);
			outcellGrid.iMRCnt_110 = Integer.parseInt(vals[i++]);
			outcellGrid.iMRCnt_113 = Integer.parseInt(vals[i++]);
			outcellGrid.iMRCnt_128 = Integer.parseInt(vals[i++]);
			outcellGrid.iRSRP100_SINR0 = Integer.parseInt(vals[i++]);
			outcellGrid.iRSRP105_SINR0 = Integer.parseInt(vals[i++]);
			outcellGrid.iRSRP110_SINR3 = Integer.parseInt(vals[i++]);
			outcellGrid.iRSRP110_SINR0 = Integer.parseInt(vals[i++]);
			outcellGrid.iSINR_0 = Integer.parseInt(vals[i++]);
			outcellGrid.iRSRQ_14 = Integer.parseInt(vals[i++]);
			outcellGrid.fOverlapTotal = Float.parseFloat(vals[i++]);
			outcellGrid.iOverlapMRCnt = Integer.parseInt(vals[i++]);
			outcellGrid.fOverlapTotalAll = Float.parseFloat(vals[i++]);
			outcellGrid.iOverlapMRCntAll = Integer.parseInt(vals[i++]);
			outcellGrid.fRSRPMax = Float.parseFloat(vals[i++]);
			outcellGrid.fRSRPMin = Float.parseFloat(vals[i++]);
			outcellGrid.fRSRQMax = Float.parseFloat(vals[i++]);
			outcellGrid.fRSRQMin = Float.parseFloat(vals[i++]);
			outcellGrid.fSINRMax = Float.parseFloat(vals[i++]);
			outcellGrid.fSINRMin = Float.parseFloat(vals[i++]);
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error,"Scene_CellGrid filldata error", "", e);
		}

		return outcellGrid;
	}
}
