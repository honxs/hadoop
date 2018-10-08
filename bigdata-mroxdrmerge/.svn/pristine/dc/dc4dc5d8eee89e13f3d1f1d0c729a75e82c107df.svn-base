package cn.mastercom.bigdata.mro.stat.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;
import cn.mastercom.bigdata.util.FormatTime;

public class BuildPosUserMergeDo implements IMergeDataDo,Serializable
{
	public int dataType;
	public Stat_BuildPosUser stat_BuildPosUser = new Stat_BuildPosUser();
	private StringBuffer sbTemp = new StringBuffer();

	public String getMapKey() {
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(stat_BuildPosUser.iCityID);
		sbTemp.append("_");
		sbTemp.append(stat_BuildPosUser.iBuildingID);
		sbTemp.append("_");
		sbTemp.append(stat_BuildPosUser.iHeight);
		sbTemp.append("_");
		sbTemp.append(stat_BuildPosUser.position);
		sbTemp.append("_");
		sbTemp.append(stat_BuildPosUser.IMSI);
		sbTemp.append("_");
		sbTemp.append(stat_BuildPosUser.iTime);
		return sbTemp.toString();
		

	}

	@Override
	public int getDataType() {
		return dataType;
	}

	@Override
	public int setDataType(int dataType) {
		this.dataType = dataType;
		return 0;
	}
	@Override
	public boolean mergeData(Object o)
	{
		BuildPosUserMergeDo temp = (BuildPosUserMergeDo) o;
		stat_BuildPosUser.iMRCnt += temp.stat_BuildPosUser.iMRCnt;
		stat_BuildPosUser.iMRRSRQCnt += temp.stat_BuildPosUser.iMRRSRQCnt;
		stat_BuildPosUser.iMRSINRCnt += temp.stat_BuildPosUser.iMRSINRCnt;
		stat_BuildPosUser.fRSRPValue += temp.stat_BuildPosUser.fRSRPValue;
		stat_BuildPosUser.fRSRQValue += temp.stat_BuildPosUser.fRSRQValue;
		stat_BuildPosUser.fSINRValue += temp.stat_BuildPosUser.fSINRValue;
		stat_BuildPosUser.iMRCnt_95 += temp.stat_BuildPosUser.iMRCnt_95;
		stat_BuildPosUser.iMRCnt_100 += temp.stat_BuildPosUser.iMRCnt_100;
		stat_BuildPosUser.iMRCnt_103 += temp.stat_BuildPosUser.iMRCnt_103;
		stat_BuildPosUser.iMRCnt_105 += temp.stat_BuildPosUser.iMRCnt_105;
		stat_BuildPosUser.iMRCnt_110 += temp.stat_BuildPosUser.iMRCnt_110;
		stat_BuildPosUser.iMRCnt_113 += temp.stat_BuildPosUser.iMRCnt_113;
		stat_BuildPosUser.iMRCnt_128 += temp.stat_BuildPosUser.iMRCnt_128;
		stat_BuildPosUser.iRSRP100_SINR0 += temp.stat_BuildPosUser.iRSRP100_SINR0;
		stat_BuildPosUser.iRSRP105_SINR0 += temp.stat_BuildPosUser.iRSRP105_SINR0;
		stat_BuildPosUser.iRSRP110_SINR3 += temp.stat_BuildPosUser.iRSRP110_SINR3;
		stat_BuildPosUser.iRSRP110_SINR0 += temp.stat_BuildPosUser.iRSRP110_SINR0;
		stat_BuildPosUser.iSINR_0 += temp.stat_BuildPosUser.iSINR_0;
		stat_BuildPosUser.iRSRQ_14 += temp.stat_BuildPosUser.iRSRQ_14;
		stat_BuildPosUser.fOverlapTotal += temp.stat_BuildPosUser.fOverlapTotal;
		stat_BuildPosUser.iOverlapMRCnt += temp.stat_BuildPosUser.iOverlapMRCnt;
		stat_BuildPosUser.fOverlapTotalAll += temp.stat_BuildPosUser.fOverlapTotalAll;
		stat_BuildPosUser.iOverlapMRCntAll += temp.stat_BuildPosUser.iOverlapMRCntAll;
		if (temp.stat_BuildPosUser.fRSRPMax > stat_BuildPosUser.fRSRPMax)
		{
			stat_BuildPosUser.fRSRPMax = temp.stat_BuildPosUser.fRSRPMax;
		}
		if ((stat_BuildPosUser.fRSRPMin == StaticConfig.Int_Abnormal)
				|| (temp.stat_BuildPosUser.fRSRPMin < stat_BuildPosUser.fRSRPMin && temp.stat_BuildPosUser.fRSRPMin != StaticConfig.Int_Abnormal))
		{
			stat_BuildPosUser.fRSRPMin = temp.stat_BuildPosUser.fRSRPMin;
		}
		if (temp.stat_BuildPosUser.fRSRQMax > stat_BuildPosUser.fRSRQMax)
		{
			stat_BuildPosUser.fRSRQMax = temp.stat_BuildPosUser.fRSRQMax;
		}
		if ((stat_BuildPosUser.fRSRQMin == StaticConfig.Int_Abnormal)
				|| (temp.stat_BuildPosUser.fRSRQMin < stat_BuildPosUser.fRSRQMin && temp.stat_BuildPosUser.fRSRQMin != StaticConfig.Int_Abnormal))
		{
			stat_BuildPosUser.fRSRQMin = temp.stat_BuildPosUser.fRSRQMin;
		}
		if (temp.stat_BuildPosUser.fSINRMax > stat_BuildPosUser.fSINRMax)
		{
			stat_BuildPosUser.fSINRMax = temp.stat_BuildPosUser.fSINRMax;
		}
		if ((stat_BuildPosUser.fSINRMin == StaticConfig.Int_Abnormal)
				|| (temp.stat_BuildPosUser.fSINRMin < stat_BuildPosUser.fSINRMin && temp.stat_BuildPosUser.fSINRMin != StaticConfig.Int_Abnormal))
		{
			stat_BuildPosUser.fSINRMin = temp.stat_BuildPosUser.fSINRMin;
		}
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		try
		{
			stat_BuildPosUser = Stat_BuildPosUser.FillData(vals, 0);
		}  
		catch (Exception e)
		{
			return false;
		}
		return true;
	}

	@Override
	public String getData()
	{
		// TODO Auto-generated method stub
		return stat_BuildPosUser.mergeStatToLine();
	}

}
