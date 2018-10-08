package cn.mastercom.bigdata.mro.stat.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;

public class BuildCellMergeDo implements IMergeDataDo,Serializable
{
	private int dataType = 0;
	public Stat_BuildCell buildCell = new Stat_BuildCell();
	private StringBuffer sbTemp = new StringBuffer();

	@Override
	public String getMapKey()
	{
		// TODO Auto-generated method stub
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(buildCell.iCityID);
		sbTemp.append("_");
		sbTemp.append(buildCell.iBuildingID);
		sbTemp.append("_");
		sbTemp.append(buildCell.ifreq);
		sbTemp.append("_");
		sbTemp.append(buildCell.iHeight);
		sbTemp.append("_");
		sbTemp.append(buildCell.iEci);
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
		BuildCellMergeDo temp = (BuildCellMergeDo) o;
		buildCell.iMRCnt += temp.buildCell.iMRCnt;
		buildCell.iMRRSRQCnt += temp.buildCell.iMRRSRQCnt;
		buildCell.iMRSINRCnt += temp.buildCell.iMRSINRCnt;
		buildCell.fRSRPValue += temp.buildCell.fRSRPValue;
		buildCell.fRSRQValue += temp.buildCell.fRSRQValue;
		buildCell.fSINRValue += temp.buildCell.fSINRValue;
		buildCell.iMRCnt_95 += temp.buildCell.iMRCnt_95;
		buildCell.iMRCnt_100 += temp.buildCell.iMRCnt_100;
		buildCell.iMRCnt_103 += temp.buildCell.iMRCnt_103;
		buildCell.iMRCnt_105 += temp.buildCell.iMRCnt_105;
		buildCell.iMRCnt_110 += temp.buildCell.iMRCnt_110;
		buildCell.iMRCnt_113 += temp.buildCell.iMRCnt_113;
		buildCell.iMRCnt_128 += temp.buildCell.iMRCnt_128;
		buildCell.iRSRP100_SINR0 += temp.buildCell.iRSRP100_SINR0;
		buildCell.iRSRP105_SINR0 += temp.buildCell.iRSRP105_SINR0;
		buildCell.iRSRP110_SINR3 += temp.buildCell.iRSRP110_SINR3;
		buildCell.iRSRP110_SINR0 += temp.buildCell.iRSRP110_SINR0;
		buildCell.iSINR_0 += temp.buildCell.iSINR_0;
		buildCell.iRSRQ_14 += temp.buildCell.iRSRQ_14;
		buildCell.fOverlapTotal += temp.buildCell.fOverlapTotal;
		buildCell.iOverlapMRCnt += temp.buildCell.iOverlapMRCnt;
		buildCell.fOverlapTotalAll += temp.buildCell.fOverlapTotalAll;
		buildCell.iOverlapMRCntAll += temp.buildCell.iOverlapMRCntAll;
		if (temp.buildCell.fRSRPMax > buildCell.fRSRPMax)
		{
			buildCell.fRSRPMax = temp.buildCell.fRSRPMax;
		}
		if ((buildCell.fRSRPMin == StaticConfig.Int_Abnormal) || (temp.buildCell.fRSRPMin < buildCell.fRSRPMin
				&& temp.buildCell.fRSRPMin != StaticConfig.Int_Abnormal))
		{
			buildCell.fRSRPMin = temp.buildCell.fRSRPMin;
		}
		if (temp.buildCell.fRSRQMax > buildCell.fRSRQMax)
		{
			buildCell.fRSRQMax = temp.buildCell.fRSRQMax;
		}
		if ((buildCell.fRSRQMin == StaticConfig.Int_Abnormal) || (temp.buildCell.fRSRQMin < buildCell.fRSRQMin
				&& temp.buildCell.fRSRQMin != StaticConfig.Int_Abnormal))
		{
			buildCell.fRSRQMin = temp.buildCell.fRSRQMin;
		}
		if (temp.buildCell.fSINRMax > buildCell.fSINRMax)
		{
			buildCell.fSINRMax = temp.buildCell.fSINRMax;
		}
		if ((buildCell.fSINRMin == StaticConfig.Int_Abnormal) || (temp.buildCell.fSINRMin < buildCell.fSINRMin
				&& temp.buildCell.fSINRMin != StaticConfig.Int_Abnormal))
		{
			buildCell.fSINRMin = temp.buildCell.fSINRMin;
		}
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		try
		{
			buildCell = Stat_BuildCell.FillData(vals, 0);
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
		return buildCell.toLine();
	}

}
