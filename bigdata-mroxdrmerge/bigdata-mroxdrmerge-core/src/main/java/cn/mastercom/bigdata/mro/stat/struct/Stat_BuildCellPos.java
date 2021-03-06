package cn.mastercom.bigdata.mro.stat.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.TimeHelper;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;

public class Stat_BuildCellPos extends Stat_BuildPos implements IStat_4G,Serializable
{
	public int iEci;

	public void doFirstSample(DT_Sample_4G sample, int ifreq)
	{
		iCityID = sample.cityID;
		iBuildingID = sample.ibuildingID;
		iHeight = sample.iheight;
		position = sample.position;
		iTime = FormatTime.RoundTimeForHour(sample.itime);
		this.ifreq = ifreq;
		iEci = (int) sample.Eci;
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
		bf.append(position);
		bf.append(spliter);
		bf.append(iEci);
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

	public static Stat_BuildCellPos FillData(String[] vals, int pos)
	{
		int i = pos;
		Stat_BuildCellPos build = new Stat_BuildCellPos();
		try
		{
			build.iCityID = Integer.parseInt(vals[i++]);
			build.iBuildingID = Integer.parseInt(vals[i++]);
			build.iHeight = Integer.parseInt(vals[i++]);
			build.position = Integer.parseInt(vals[i++]);
			build.iEci = Integer.parseInt(vals[i++]);
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
			LOGHelper.GetLogger().writeLog(LogType.error,"Scene_BuildCellPos filldata error", "", e);
		}
		return build;
	}
}
