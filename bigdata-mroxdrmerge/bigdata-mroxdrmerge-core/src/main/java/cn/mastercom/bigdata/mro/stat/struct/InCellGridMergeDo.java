package cn.mastercom.bigdata.mro.stat.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;

public class InCellGridMergeDo implements IMergeDataDo,Serializable
{
	private int dataType = 0;
	public Stat_In_CellGrid incellGrid = new Stat_In_CellGrid();
	private StringBuffer sbTemp = new StringBuffer();

	@Override
	public String getMapKey()
	{
		// TODO Auto-generated method stub
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(incellGrid.iCityID);
		sbTemp.append("_");
		sbTemp.append(incellGrid.iBuildingID);
		sbTemp.append("_");
		sbTemp.append(incellGrid.iHeight);
		sbTemp.append("_");
		sbTemp.append(incellGrid.iECI);
		sbTemp.append("_");
		sbTemp.append(incellGrid.tllongitude);
		sbTemp.append("_");
		sbTemp.append(incellGrid.tllatitude);
		sbTemp.append("_");
		sbTemp.append(incellGrid.iTime);
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
		InCellGridMergeDo temp = (InCellGridMergeDo) o;
		incellGrid.iMRCnt += temp.incellGrid.iMRCnt;
		// incellGrid.iMRCnt_In_URI += temp.incellGrid.iMRCnt_In_URI;
		// incellGrid.iMRCnt_In_SDK += temp.incellGrid.iMRCnt_In_SDK;
		// incellGrid.iMRCnt_In_WLAN += temp.incellGrid.iMRCnt_In_WLAN;
		// incellGrid.iMRCnt_In_SIMU += temp.incellGrid.iMRCnt_In_SIMU;
		// incellGrid.iMRCnt_In_Other += temp.incellGrid.iMRCnt_In_Other;
		incellGrid.iMRRSRQCnt += temp.incellGrid.iMRRSRQCnt;
		incellGrid.iMRSINRCnt += temp.incellGrid.iMRSINRCnt;
		incellGrid.fRSRPValue += temp.incellGrid.fRSRPValue;
		incellGrid.fRSRQValue += temp.incellGrid.fRSRQValue;
		incellGrid.fSINRValue += temp.incellGrid.fSINRValue;
		incellGrid.iMRCnt_95 += temp.incellGrid.iMRCnt_95;
		incellGrid.iMRCnt_100 += temp.incellGrid.iMRCnt_100;
		incellGrid.iMRCnt_103 += temp.incellGrid.iMRCnt_103;
		incellGrid.iMRCnt_105 += temp.incellGrid.iMRCnt_105;
		incellGrid.iMRCnt_110 += temp.incellGrid.iMRCnt_110;
		incellGrid.iMRCnt_113 += temp.incellGrid.iMRCnt_113;
		incellGrid.iMRCnt_128 += temp.incellGrid.iMRCnt_128;
		incellGrid.iRSRP100_SINR0 += temp.incellGrid.iRSRP100_SINR0;
		incellGrid.iRSRP105_SINR0 += temp.incellGrid.iRSRP105_SINR0;
		incellGrid.iRSRP110_SINR3 += temp.incellGrid.iRSRP110_SINR3;
		incellGrid.iRSRP110_SINR0 += temp.incellGrid.iRSRP110_SINR0;
		incellGrid.iSINR_0 += temp.incellGrid.iSINR_0;
		incellGrid.iRSRQ_14 += temp.incellGrid.iRSRQ_14;
		// incellGrid.iASNei_MRCnt += temp.incellGrid.iASNei_MRCnt;
		// incellGrid.fASNei_RSRPValue += temp.incellGrid.fASNei_RSRPValue;
		incellGrid.fOverlapTotal += temp.incellGrid.fOverlapTotal;
		incellGrid.iOverlapMRCnt += temp.incellGrid.iOverlapMRCnt;
		incellGrid.fOverlapTotalAll += temp.incellGrid.fOverlapTotalAll;
		incellGrid.iOverlapMRCntAll += temp.incellGrid.iOverlapMRCntAll;
		if (temp.incellGrid.fRSRPMax > incellGrid.fRSRPMax)
		{
			incellGrid.fRSRPMax = temp.incellGrid.fRSRPMax;
		}
		if ((incellGrid.fRSRPMin == StaticConfig.Int_Abnormal) || (temp.incellGrid.fRSRPMin < incellGrid.fRSRPMin
				&& temp.incellGrid.fRSRPMin != StaticConfig.Int_Abnormal))
		{
			incellGrid.fRSRPMin = temp.incellGrid.fRSRPMin;
		}
		if (temp.incellGrid.fRSRQMax > incellGrid.fRSRQMax)
		{
			incellGrid.fRSRQMax = temp.incellGrid.fRSRQMax;
		}
		if ((incellGrid.fRSRQMin == StaticConfig.Int_Abnormal) || (temp.incellGrid.fRSRQMin < incellGrid.fRSRQMin
				&& temp.incellGrid.fRSRQMin != StaticConfig.Int_Abnormal))
		{
			incellGrid.fRSRQMin = temp.incellGrid.fRSRQMin;
		}
		if (temp.incellGrid.fSINRMax > incellGrid.fSINRMax)
		{
			incellGrid.fSINRMax = temp.incellGrid.fSINRMax;
		}
		if ((incellGrid.fSINRMin == StaticConfig.Int_Abnormal) || (temp.incellGrid.fSINRMin < incellGrid.fSINRMin
				&& temp.incellGrid.fSINRMin != StaticConfig.Int_Abnormal))
		{
			incellGrid.fSINRMin = temp.incellGrid.fSINRMin;
		}
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		// TODO Auto-generated method stub
		incellGrid = Stat_In_CellGrid.FillData(vals, 0);
		return true;
	}

	@Override
	public String getData()
	{
		// TODO Auto-generated method stub
		return incellGrid.toLine();
	}

}
