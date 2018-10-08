package cn.mastercom.bigdata.mro.stat.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;
import cn.mastercom.bigdata.util.TimeHelper;

public class OutGridMerge implements IMergeDataDo,Serializable
{
	private int dataType = 0;
	public Stat_OutGrid outgrid = new Stat_OutGrid();
	private StringBuffer sbTemp = new StringBuffer();

	@Override
	public String getMapKey()
	{
		// TODO Auto-generated method stub
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(outgrid.iCityID);
		sbTemp.append("_");
		sbTemp.append(outgrid.tllongitude);
		sbTemp.append("_");
		sbTemp.append(outgrid.tllatitude);
		sbTemp.append("_");
		sbTemp.append(outgrid.ifreq);
		sbTemp.append("_");
		sbTemp.append(outgrid.iTime);
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
		OutGridMerge temp = (OutGridMerge) o;
		outgrid.iMRCnt += temp.outgrid.iMRCnt;
		// outgrid.iMRCnt_Out_URI += temp.outgrid.iMRCnt_Out_URI;
		// outgrid.iMRCnt_Out_SDK += temp.outgrid.iMRCnt_Out_SDK;
		// outgrid.iMRCnt_Out_HIGH += temp.outgrid.iMRCnt_Out_HIGH;
		// outgrid.iMRCnt_Out_SIMU += temp.outgrid.iMRCnt_Out_SIMU;
		// outgrid.iMRCnt_Out_Other += temp.outgrid.iMRCnt_Out_Other;
		outgrid.iMRRSRQCnt += temp.outgrid.iMRRSRQCnt;
		outgrid.iMRSINRCnt += temp.outgrid.iMRSINRCnt;
		outgrid.fRSRPValue += temp.outgrid.fRSRPValue;
		outgrid.fRSRQValue += temp.outgrid.fRSRQValue;
		outgrid.fSINRValue += temp.outgrid.fSINRValue;
		outgrid.iMRCnt_95 += temp.outgrid.iMRCnt_95;
		outgrid.iMRCnt_100 += temp.outgrid.iMRCnt_100;
		outgrid.iMRCnt_103 += temp.outgrid.iMRCnt_103;
		outgrid.iMRCnt_105 += temp.outgrid.iMRCnt_105;
		outgrid.iMRCnt_110 += temp.outgrid.iMRCnt_110;
		outgrid.iMRCnt_113 += temp.outgrid.iMRCnt_113;
		outgrid.iMRCnt_128 += temp.outgrid.iMRCnt_128;
		outgrid.iRSRP100_SINR0 += temp.outgrid.iRSRP100_SINR0;
		outgrid.iRSRP105_SINR0 += temp.outgrid.iRSRP105_SINR0;
		outgrid.iRSRP110_SINR3 += temp.outgrid.iRSRP110_SINR3;
		outgrid.iRSRP110_SINR0 += temp.outgrid.iRSRP110_SINR0;
		outgrid.iSINR_0 += temp.outgrid.iSINR_0;
		outgrid.iRSRQ_14 += temp.outgrid.iRSRQ_14;
		outgrid.fOverlapTotal += temp.outgrid.fOverlapTotal;
		outgrid.iOverlapMRCnt += temp.outgrid.iOverlapMRCnt;
		outgrid.fOverlapTotalAll += temp.outgrid.fOverlapTotalAll;
		outgrid.iOverlapMRCntAll += temp.outgrid.iOverlapMRCntAll;
		if (temp.outgrid.fRSRPMax > outgrid.fRSRPMax)
		{
			outgrid.fRSRPMax = temp.outgrid.fRSRPMax;
		}
		if ((outgrid.fRSRPMin == StaticConfig.Int_Abnormal)
				|| (temp.outgrid.fRSRPMin < outgrid.fRSRPMin && temp.outgrid.fRSRPMin != StaticConfig.Int_Abnormal))
		{
			outgrid.fRSRPMin = temp.outgrid.fRSRPMin;
		}
		if (temp.outgrid.fRSRQMax > outgrid.fRSRQMax)
		{
			outgrid.fRSRQMax = temp.outgrid.fRSRQMax;
		}
		if ((outgrid.fRSRQMin == StaticConfig.Int_Abnormal)
				|| (temp.outgrid.fRSRQMin < outgrid.fRSRQMin && temp.outgrid.fRSRQMin != StaticConfig.Int_Abnormal))
		{
			outgrid.fRSRQMin = temp.outgrid.fRSRQMin;
		}
		if (temp.outgrid.fSINRMax > outgrid.fSINRMax)
		{
			outgrid.fSINRMax = temp.outgrid.fSINRMax;
		}
		if ((outgrid.fSINRMin == StaticConfig.Int_Abnormal)
				|| (temp.outgrid.fSINRMin < outgrid.fSINRMin && temp.outgrid.fSINRMin != StaticConfig.Int_Abnormal))
		{
			outgrid.fSINRMin = temp.outgrid.fSINRMin;
		}
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		// TODO Auto-generated method stub
		try
		{
			outgrid = Stat_OutGrid.FillData(vals, 0);
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
		return outgrid.toLine();
	}

}
