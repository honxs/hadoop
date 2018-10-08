package cn.mastercom.bigdata.mro.stat.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;

public class BuildCellPosMergeDo implements IMergeDataDo,Serializable
{
	private int dataType = 0;
	public Stat_BuildCellPos buildCellPos = new Stat_BuildCellPos();
	private StringBuffer sbTemp = new StringBuffer();

	@Override
	public String getMapKey()
	{
		// TODO Auto-generated method stub
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(buildCellPos.iCityID);
		sbTemp.append("_");
		sbTemp.append(buildCellPos.iBuildingID);
		sbTemp.append("_");
		sbTemp.append(buildCellPos.ifreq);
		sbTemp.append("_");
		sbTemp.append(buildCellPos.iHeight);
		sbTemp.append("_");
		sbTemp.append(buildCellPos.position);
		sbTemp.append("_");
		sbTemp.append(buildCellPos.iEci);
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
		BuildCellPosMergeDo temp = (BuildCellPosMergeDo) o;
		buildCellPos.iMRCnt += temp.buildCellPos.iMRCnt;
		buildCellPos.iMRRSRQCnt += temp.buildCellPos.iMRRSRQCnt;
		buildCellPos.iMRSINRCnt += temp.buildCellPos.iMRSINRCnt;
		buildCellPos.fRSRPValue += temp.buildCellPos.fRSRPValue;
		buildCellPos.fRSRQValue += temp.buildCellPos.fRSRQValue;
		buildCellPos.fSINRValue += temp.buildCellPos.fSINRValue;
		buildCellPos.iMRCnt_95 += temp.buildCellPos.iMRCnt_95;
		buildCellPos.iMRCnt_100 += temp.buildCellPos.iMRCnt_100;
		buildCellPos.iMRCnt_103 += temp.buildCellPos.iMRCnt_103;
		buildCellPos.iMRCnt_105 += temp.buildCellPos.iMRCnt_105;
		buildCellPos.iMRCnt_110 += temp.buildCellPos.iMRCnt_110;
		buildCellPos.iMRCnt_113 += temp.buildCellPos.iMRCnt_113;
		buildCellPos.iMRCnt_128 += temp.buildCellPos.iMRCnt_128;
		buildCellPos.iRSRP100_SINR0 += temp.buildCellPos.iRSRP100_SINR0;
		buildCellPos.iRSRP105_SINR0 += temp.buildCellPos.iRSRP105_SINR0;
		buildCellPos.iRSRP110_SINR3 += temp.buildCellPos.iRSRP110_SINR3;
		buildCellPos.iRSRP110_SINR0 += temp.buildCellPos.iRSRP110_SINR0;
		buildCellPos.iSINR_0 += temp.buildCellPos.iSINR_0;
		buildCellPos.iRSRQ_14 += temp.buildCellPos.iRSRQ_14;
		buildCellPos.fOverlapTotal += temp.buildCellPos.fOverlapTotal;
		buildCellPos.iOverlapMRCnt += temp.buildCellPos.iOverlapMRCnt;
		buildCellPos.fOverlapTotalAll += temp.buildCellPos.fOverlapTotalAll;
		buildCellPos.iOverlapMRCntAll += temp.buildCellPos.iOverlapMRCntAll;
		if (temp.buildCellPos.fRSRPMax > buildCellPos.fRSRPMax)
		{
			buildCellPos.fRSRPMax = temp.buildCellPos.fRSRPMax;
		}
		if ((buildCellPos.fRSRPMin == StaticConfig.Int_Abnormal) || (temp.buildCellPos.fRSRPMin < buildCellPos.fRSRPMin
				&& temp.buildCellPos.fRSRPMin != StaticConfig.Int_Abnormal))
		{
			buildCellPos.fRSRPMin = temp.buildCellPos.fRSRPMin;
		}
		if (temp.buildCellPos.fRSRQMax > buildCellPos.fRSRQMax)
		{
			buildCellPos.fRSRQMax = temp.buildCellPos.fRSRQMax;
		}
		if ((buildCellPos.fRSRQMin == StaticConfig.Int_Abnormal) || (temp.buildCellPos.fRSRQMin < buildCellPos.fRSRQMin
				&& temp.buildCellPos.fRSRQMin != StaticConfig.Int_Abnormal))
		{
			buildCellPos.fRSRQMin = temp.buildCellPos.fRSRQMin;
		}
		if (temp.buildCellPos.fSINRMax > buildCellPos.fSINRMax)
		{
			buildCellPos.fSINRMax = temp.buildCellPos.fSINRMax;
		}
		if ((buildCellPos.fSINRMin == StaticConfig.Int_Abnormal) || (temp.buildCellPos.fSINRMin < buildCellPos.fSINRMin
				&& temp.buildCellPos.fSINRMin != StaticConfig.Int_Abnormal))
		{
			buildCellPos.fSINRMin = temp.buildCellPos.fSINRMin;
		}
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		try
		{
			buildCellPos = Stat_BuildCellPos.FillData(vals, 0);
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
		return buildCellPos.toLine();
	}

}
