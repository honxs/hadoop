package cn.mastercom.bigdata.mro.stat.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;
import cn.mastercom.bigdata.util.TimeHelper;

public class OutCellGridMergeDo implements IMergeDataDo,Serializable
{
	private int dataType = 0;
	public Stat_Out_CellGrid outcellGrid = new Stat_Out_CellGrid();
	private StringBuffer sbTemp = new StringBuffer();

	@Override
	public String getMapKey()
	{
		// TODO Auto-generated method stub
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(outcellGrid.iCityID);
		sbTemp.append("_");
		sbTemp.append(outcellGrid.iECI);
		sbTemp.append("_");
		sbTemp.append(outcellGrid.tllongitude);
		sbTemp.append("_");
		sbTemp.append(outcellGrid.tllatitude);
		sbTemp.append("_");
		sbTemp.append(outcellGrid.iTime);
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
		OutCellGridMergeDo temp = (OutCellGridMergeDo) o;
		outcellGrid.iMRCnt += temp.outcellGrid.iMRCnt;
		// outcellGrid.iMRCnt_Out_URI += temp.outcellGrid.iMRCnt_Out_URI;
		// outcellGrid.iMRCnt_Out_SDK += temp.outcellGrid.iMRCnt_Out_SDK;
		// outcellGrid.iMRCnt_Out_HIGH += temp.outcellGrid.iMRCnt_Out_HIGH;
		// outcellGrid.iMRCnt_Out_SIMU += temp.outcellGrid.iMRCnt_Out_SIMU;
		// outcellGrid.iMRCnt_Out_Other += temp.outcellGrid.iMRCnt_Out_Other;
		outcellGrid.iMRRSRQCnt += temp.outcellGrid.iMRRSRQCnt;
		outcellGrid.iMRSINRCnt += temp.outcellGrid.iMRSINRCnt;
		outcellGrid.fRSRPValue += temp.outcellGrid.fRSRPValue;
		outcellGrid.fRSRQValue += temp.outcellGrid.fRSRQValue;
		outcellGrid.fSINRValue += temp.outcellGrid.fSINRValue;
		outcellGrid.iMRCnt_95 += temp.outcellGrid.iMRCnt_95;
		outcellGrid.iMRCnt_100 += temp.outcellGrid.iMRCnt_100;
		outcellGrid.iMRCnt_103 += temp.outcellGrid.iMRCnt_103;
		outcellGrid.iMRCnt_105 += temp.outcellGrid.iMRCnt_105;
		outcellGrid.iMRCnt_110 += temp.outcellGrid.iMRCnt_110;
		outcellGrid.iMRCnt_113 += temp.outcellGrid.iMRCnt_113;
		outcellGrid.iMRCnt_128 += temp.outcellGrid.iMRCnt_128;
		outcellGrid.iRSRP100_SINR0 += temp.outcellGrid.iRSRP100_SINR0;
		outcellGrid.iRSRP105_SINR0 += temp.outcellGrid.iRSRP105_SINR0;
		outcellGrid.iRSRP110_SINR3 += temp.outcellGrid.iRSRP110_SINR3;
		outcellGrid.iRSRP110_SINR0 += temp.outcellGrid.iRSRP110_SINR0;
		outcellGrid.iSINR_0 += temp.outcellGrid.iSINR_0;
		outcellGrid.iRSRQ_14 += temp.outcellGrid.iRSRQ_14;
		// outcellGrid.iASNei_MRCnt += temp.outcellGrid.iASNei_MRCnt;
		// outcellGrid.fASNei_RSRPValue += temp.outcellGrid.fASNei_RSRPValue;
		outcellGrid.fOverlapTotal += temp.outcellGrid.fOverlapTotal;
		outcellGrid.iOverlapMRCnt += temp.outcellGrid.iOverlapMRCnt;
		outcellGrid.fOverlapTotalAll += temp.outcellGrid.fOverlapTotalAll;
		outcellGrid.iOverlapMRCntAll += temp.outcellGrid.iOverlapMRCntAll;
		if (temp.outcellGrid.fRSRPMax > outcellGrid.fRSRPMax)
		{
			outcellGrid.fRSRPMax = temp.outcellGrid.fRSRPMax;
		}
		if ((outcellGrid.fRSRPMin == StaticConfig.Int_Abnormal) || (temp.outcellGrid.fRSRPMin < outcellGrid.fRSRPMin && temp.outcellGrid.fRSRPMin != StaticConfig.Int_Abnormal))
		{
			outcellGrid.fRSRPMin = temp.outcellGrid.fRSRPMin;
		}
		if (temp.outcellGrid.fRSRQMax > outcellGrid.fRSRQMax)
		{
			outcellGrid.fRSRQMax = temp.outcellGrid.fRSRQMax;
		}
		if ((outcellGrid.fRSRQMin == StaticConfig.Int_Abnormal) || (temp.outcellGrid.fRSRQMin < outcellGrid.fRSRQMin && temp.outcellGrid.fRSRQMin != StaticConfig.Int_Abnormal))
		{
			outcellGrid.fRSRQMin = temp.outcellGrid.fRSRQMin;
		}
		if (temp.outcellGrid.fSINRMax > outcellGrid.fSINRMax)
		{
			outcellGrid.fSINRMax = temp.outcellGrid.fSINRMax;
		}
		if ((outcellGrid.fSINRMin == StaticConfig.Int_Abnormal) || (temp.outcellGrid.fSINRMin < outcellGrid.fSINRMin && temp.outcellGrid.fSINRMin != StaticConfig.Int_Abnormal))
		{
			outcellGrid.fSINRMin = temp.outcellGrid.fSINRMin;
		}

		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		outcellGrid = Stat_Out_CellGrid.FillData(vals, 0);
		return true;
	}

	@Override
	public String getData()
	{
		return outcellGrid.toLine();
	}

}
