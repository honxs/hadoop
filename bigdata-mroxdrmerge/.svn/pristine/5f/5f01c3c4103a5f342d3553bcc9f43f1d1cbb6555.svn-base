package cn.mastercom.bigdata.mro.stat.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;
import cn.mastercom.bigdata.util.TimeHelper;

public class InGridMergeDo implements IMergeDataDo,Serializable
{
	private int dataType = 0;
	public Stat_InGrid ingrid = new Stat_InGrid();
	private StringBuffer sbTemp = new StringBuffer();

	@Override
	public String getMapKey()
	{
		// TODO Auto-generated method stub
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(ingrid.iCityID);
		sbTemp.append("_");
		sbTemp.append(ingrid.iBuildingID);
		sbTemp.append("_");
		sbTemp.append(ingrid.iHeight);
		sbTemp.append("_");
		sbTemp.append(ingrid.tllongitude);
		sbTemp.append("_");
		sbTemp.append(ingrid.tllatitude);
		sbTemp.append("_");
		sbTemp.append(ingrid.ifreq);
		sbTemp.append("_");
		sbTemp.append(ingrid.iTime);
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
		InGridMergeDo temp = (InGridMergeDo) o;
		ingrid.iMRCnt += temp.ingrid.iMRCnt;
		// ingrid.iMRCnt_In_URI += temp.ingrid.iMRCnt_In_URI;
		// ingrid.iMRCnt_In_SDK += temp.ingrid.iMRCnt_In_SDK;
		// ingrid.iMRCnt_In_WLAN += temp.ingrid.iMRCnt_In_WLAN;
		// ingrid.iMRCnt_In_SIMU += temp.ingrid.iMRCnt_In_SIMU;
		// ingrid.iMRCnt_In_Other += temp.ingrid.iMRCnt_In_Other;
		ingrid.iMRRSRQCnt += temp.ingrid.iMRRSRQCnt;
		ingrid.iMRSINRCnt += temp.ingrid.iMRSINRCnt;
		ingrid.fRSRPValue += temp.ingrid.fRSRPValue;
		ingrid.fRSRQValue += temp.ingrid.fRSRQValue;
		ingrid.fSINRValue += temp.ingrid.fSINRValue;
		ingrid.iMRCnt_95 += temp.ingrid.iMRCnt_95;
		ingrid.iMRCnt_100 += temp.ingrid.iMRCnt_100;
		ingrid.iMRCnt_103 += temp.ingrid.iMRCnt_103;
		ingrid.iMRCnt_105 += temp.ingrid.iMRCnt_105;
		ingrid.iMRCnt_110 += temp.ingrid.iMRCnt_110;
		ingrid.iMRCnt_113 += temp.ingrid.iMRCnt_113;
		ingrid.iMRCnt_128 += temp.ingrid.iMRCnt_128;
		ingrid.iRSRP100_SINR0 += temp.ingrid.iRSRP100_SINR0;
		ingrid.iRSRP105_SINR0 += temp.ingrid.iRSRP105_SINR0;
		ingrid.iRSRP110_SINR3 += temp.ingrid.iRSRP110_SINR3;
		ingrid.iRSRP110_SINR0 += temp.ingrid.iRSRP110_SINR0;
		ingrid.iSINR_0 += temp.ingrid.iSINR_0;
		ingrid.iRSRQ_14 += temp.ingrid.iRSRQ_14;
		ingrid.fOverlapTotal += temp.ingrid.fOverlapTotal;
		ingrid.iOverlapMRCnt += temp.ingrid.iOverlapMRCnt;
		ingrid.fOverlapTotalAll += temp.ingrid.fOverlapTotalAll;
		ingrid.iOverlapMRCntAll += temp.ingrid.iOverlapMRCntAll;
		if (temp.ingrid.fRSRPMax > ingrid.fRSRPMax)
		{
			ingrid.fRSRPMax = temp.ingrid.fRSRPMax;
		}
		if ((ingrid.fRSRPMin == StaticConfig.Int_Abnormal)
				|| (temp.ingrid.fRSRPMin < ingrid.fRSRPMin && temp.ingrid.fRSRPMin != StaticConfig.Int_Abnormal))
		{
			ingrid.fRSRPMin = temp.ingrid.fRSRPMin;
		}
		if (temp.ingrid.fRSRQMax > ingrid.fRSRQMax)
		{
			ingrid.fRSRQMax = temp.ingrid.fRSRQMax;
		}
		if ((ingrid.fRSRQMin == StaticConfig.Int_Abnormal)
				|| (temp.ingrid.fRSRQMin < ingrid.fRSRQMin && temp.ingrid.fRSRQMin != StaticConfig.Int_Abnormal))
		{
			ingrid.fRSRQMin = temp.ingrid.fRSRQMin;
		}
		if (temp.ingrid.fSINRMax > ingrid.fSINRMax)
		{
			ingrid.fSINRMax = temp.ingrid.fSINRMax;
		}
		if ((ingrid.fSINRMin == StaticConfig.Int_Abnormal)
				|| (temp.ingrid.fSINRMin < ingrid.fSINRMin && temp.ingrid.fSINRMin != StaticConfig.Int_Abnormal))
		{
			ingrid.fSINRMin = temp.ingrid.fSINRMin;
		}
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		// TODO Auto-generated method stub
		try
		{
			ingrid = Stat_InGrid.FillData(vals, 0);
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
		return ingrid.toLine();
	}

}
