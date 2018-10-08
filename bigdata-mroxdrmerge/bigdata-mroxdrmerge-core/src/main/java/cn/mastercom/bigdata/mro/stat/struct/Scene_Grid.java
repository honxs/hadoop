package cn.mastercom.bigdata.mro.stat.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.TimeHelper;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;

public class Scene_Grid extends Stat_OutGrid implements Serializable
{
	public int iAreaType;
	public int iAreaID;

	public static final String spliter = "\t";

	@Override
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
		iTime = FormatTime.RoundTimeForHour(sample.itime);
	}

	public static Scene_Grid FillData(String[] vals, int pos)
	{
		int i = pos;
		Scene_Grid outGrid = new Scene_Grid();
		try
		{
			outGrid.iCityID = Integer.parseInt(vals[i++]);
			outGrid.iAreaType = Integer.parseInt(vals[i++]);
			outGrid.iAreaID = Integer.parseInt(vals[i++]);
			outGrid.tllongitude = Integer.parseInt(vals[i++]);
			outGrid.tllatitude = Integer.parseInt(vals[i++]);
			outGrid.brlongitude = Integer.parseInt(vals[i++]);
			outGrid.brlatitude = Integer.parseInt(vals[i++]);
			outGrid.iTime = TimeHelper.getRoundDayTime(Integer.parseInt(vals[i++]));
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
			LOGHelper.GetLogger().writeLog(LogType.error,"Scene_Grod filldata error", "", e);
		}
		return outGrid;

	}
}
