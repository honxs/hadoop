package cn.mastercom.bigdata.mro.stat.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;

public class BuildPosMergeDo implements IMergeDataDo,Serializable
{
	private int dataType = 0;
	public Stat_BuildPos buildPos = new Stat_BuildPos();
	private StringBuffer sbTemp = new StringBuffer();

	@Override
	public String getMapKey()
	{
		// TODO Auto-generated method stub
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(buildPos.iCityID);
		sbTemp.append("_");
		sbTemp.append(buildPos.iBuildingID);
		sbTemp.append("_");
		sbTemp.append(buildPos.iHeight);
		sbTemp.append("_");
		sbTemp.append(buildPos.position);
		sbTemp.append("_");
		sbTemp.append(buildPos.ifreq);
		return sbTemp.toString();
	}

	@Override
	public int getDataType()
	{
		// TODO Auto-generated method stub
		return dataType;
	}

	@Override
	public int setDataType(int dataType)
	{
		// TODO Auto-generated method stub
		this.dataType = dataType;
		return 0;
	}

	@Override
	public boolean mergeData(Object o)
	{
		// TODO Auto-generated method stub
		BuildPosMergeDo temp = (BuildPosMergeDo) o;
		buildPos.iMRCnt += temp.buildPos.iMRCnt;
		buildPos.iMRRSRQCnt += temp.buildPos.iMRRSRQCnt;
		buildPos.iMRSINRCnt += temp.buildPos.iMRSINRCnt;
		buildPos.fRSRPValue += temp.buildPos.fRSRPValue;
		buildPos.fRSRQValue += temp.buildPos.fRSRQValue;
		buildPos.fSINRValue += temp.buildPos.fSINRValue;
		buildPos.iMRCnt_95 += temp.buildPos.iMRCnt_95;
		buildPos.iMRCnt_100 += temp.buildPos.iMRCnt_100;
		buildPos.iMRCnt_103 += temp.buildPos.iMRCnt_103;
		buildPos.iMRCnt_105 += temp.buildPos.iMRCnt_105;
		buildPos.iMRCnt_110 += temp.buildPos.iMRCnt_110;
		buildPos.iMRCnt_113 += temp.buildPos.iMRCnt_113;
		buildPos.iMRCnt_128 += temp.buildPos.iMRCnt_128;
		buildPos.iRSRP100_SINR0 += temp.buildPos.iRSRP100_SINR0;
		buildPos.iRSRP105_SINR0 += temp.buildPos.iRSRP105_SINR0;
		buildPos.iRSRP110_SINR3 += temp.buildPos.iRSRP110_SINR3;
		buildPos.iRSRP110_SINR0 += temp.buildPos.iRSRP110_SINR0;
		buildPos.iSINR_0 += temp.buildPos.iSINR_0;
		buildPos.iRSRQ_14 += temp.buildPos.iRSRQ_14;
		buildPos.fOverlapTotal += temp.buildPos.fOverlapTotal;
		buildPos.iOverlapMRCnt += temp.buildPos.iOverlapMRCnt;
		buildPos.fOverlapTotalAll += temp.buildPos.fOverlapTotalAll;
		buildPos.iOverlapMRCntAll += temp.buildPos.iOverlapMRCntAll;
		if (temp.buildPos.fRSRPMax > buildPos.fRSRPMax)
		{
			buildPos.fRSRPMax = temp.buildPos.fRSRPMax;
		}
		if ((buildPos.fRSRPMin == StaticConfig.Int_Abnormal) || (temp.buildPos.fRSRPMin < buildPos.fRSRPMin && temp.buildPos.fRSRPMin != StaticConfig.Int_Abnormal))
		{
			buildPos.fRSRPMin = temp.buildPos.fRSRPMin;
		}
		if (temp.buildPos.fRSRQMax > buildPos.fRSRQMax)
		{
			buildPos.fRSRQMax = temp.buildPos.fRSRQMax;
		}
		if ((buildPos.fRSRQMin == StaticConfig.Int_Abnormal) || (temp.buildPos.fRSRQMin < buildPos.fRSRQMin && temp.buildPos.fRSRQMin != StaticConfig.Int_Abnormal))
		{
			buildPos.fRSRQMin = temp.buildPos.fRSRQMin;
		}
		if (temp.buildPos.fSINRMax > buildPos.fSINRMax)
		{
			buildPos.fSINRMax = temp.buildPos.fSINRMax;
		}
		if ((buildPos.fSINRMin == StaticConfig.Int_Abnormal) || (temp.buildPos.fSINRMin < buildPos.fSINRMin && temp.buildPos.fSINRMin != StaticConfig.Int_Abnormal))
		{
			buildPos.fSINRMin = temp.buildPos.fSINRMin;
		}
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		try
		{
			buildPos = Stat_BuildPos.FillData(vals, 0);
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
		return buildPos.toLine();
	}

}
