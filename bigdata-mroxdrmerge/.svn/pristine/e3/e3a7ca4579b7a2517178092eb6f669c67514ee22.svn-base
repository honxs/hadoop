package cn.mastercom.bigdata.mro.stat.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;

public class ImeiMergeDo implements IMergeDataDo,Serializable
{
	private int dataType = 0;
	public Stat_Imei imeiStruct = new Stat_Imei();
	private StringBuffer sbTemp = new StringBuffer();

	@Override
	public String getMapKey()
	{
		// TODO Auto-generated method stub
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(imeiStruct.iCityID);
		sbTemp.append("_");
		sbTemp.append(imeiStruct.imeiTac);
		sbTemp.append("_");
		sbTemp.append(imeiStruct.ifreq);
		sbTemp.append("_");
		sbTemp.append(imeiStruct.iTime);
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
		ImeiMergeDo temp = (ImeiMergeDo) o;
		imeiStruct.iMRCnt += temp.imeiStruct.iMRCnt;
		imeiStruct.iMRCnt_Indoor += temp.imeiStruct.iMRCnt_Indoor;
		imeiStruct.iMRCnt_Outdoor += temp.imeiStruct.iMRCnt_Outdoor;
		imeiStruct.iMRRSRQCnt += temp.imeiStruct.iMRRSRQCnt;
		imeiStruct.iMRRSRQCnt_Indoor += temp.imeiStruct.iMRRSRQCnt_Indoor;
		imeiStruct.iMRRSRQCnt_Outdoor += temp.imeiStruct.iMRRSRQCnt_Outdoor;
		imeiStruct.iMRSINRCnt += temp.imeiStruct.iMRSINRCnt;
		imeiStruct.iMRSINRCnt_Indoor += temp.imeiStruct.iMRSINRCnt_Indoor;
		imeiStruct.iMRSINRCnt_Outdoor += temp.imeiStruct.iMRSINRCnt_Outdoor;
		imeiStruct.fRSRPValue += temp.imeiStruct.fRSRPValue;
		imeiStruct.fRSRPValue_Indoor += temp.imeiStruct.fRSRPValue_Indoor;
		imeiStruct.fRSRPValue_Outdoor += temp.imeiStruct.fRSRPValue_Outdoor;
		imeiStruct.fRSRQValue += temp.imeiStruct.fRSRQValue;
		imeiStruct.fRSRQValue_Indoor += temp.imeiStruct.fRSRQValue_Indoor;
		imeiStruct.fRSRQValue_Outdoor += temp.imeiStruct.fRSRQValue_Outdoor;
		imeiStruct.fSINRValue += temp.imeiStruct.fSINRValue;
		imeiStruct.fSINRValue_Indoor += temp.imeiStruct.fSINRValue_Indoor;
		imeiStruct.fSINRValue_Outdoor += temp.imeiStruct.fSINRValue_Outdoor;
		imeiStruct.iMRCnt_Indoor_0_70 += temp.imeiStruct.iMRCnt_Indoor_0_70;
		imeiStruct.iMRCnt_Indoor_70_80 += temp.imeiStruct.iMRCnt_Indoor_70_80;
		imeiStruct.iMRCnt_Indoor_80_90 += temp.imeiStruct.iMRCnt_Indoor_80_90;
		imeiStruct.iMRCnt_Indoor_90_95 += temp.imeiStruct.iMRCnt_Indoor_90_95;
		imeiStruct.iMRCnt_Indoor_100 += temp.imeiStruct.iMRCnt_Indoor_100;
		imeiStruct.iMRCnt_Indoor_103 += temp.imeiStruct.iMRCnt_Indoor_103;
		imeiStruct.iMRCnt_Indoor_105 += temp.imeiStruct.iMRCnt_Indoor_105;
		imeiStruct.iMRCnt_Indoor_110 += temp.imeiStruct.iMRCnt_Indoor_110;
		imeiStruct.iMRCnt_Indoor_113 += temp.imeiStruct.iMRCnt_Indoor_113;
		imeiStruct.iMRCnt_Outdoor_0_70 += temp.imeiStruct.iMRCnt_Outdoor_0_70;
		imeiStruct.iMRCnt_Outdoor_70_80 += temp.imeiStruct.iMRCnt_Outdoor_70_80;
		imeiStruct.iMRCnt_Outdoor_80_90 += temp.imeiStruct.iMRCnt_Outdoor_80_90;
		imeiStruct.iMRCnt_Outdoor_90_95 += temp.imeiStruct.iMRCnt_Outdoor_90_95;
		imeiStruct.iMRCnt_Outdoor_100 += temp.imeiStruct.iMRCnt_Outdoor_100;
		imeiStruct.iMRCnt_Outdoor_103 += temp.imeiStruct.iMRCnt_Outdoor_103;
		imeiStruct.iMRCnt_Outdoor_105 += temp.imeiStruct.iMRCnt_Outdoor_105;
		imeiStruct.iMRCnt_Outdoor_110 += temp.imeiStruct.iMRCnt_Outdoor_110;
		imeiStruct.iMRCnt_Outdoor_113 += temp.imeiStruct.iMRCnt_Outdoor_113;
		imeiStruct.iIndoorRSRP100_SINR0 += temp.imeiStruct.iIndoorRSRP100_SINR0;
		imeiStruct.iIndoorRSRP105_SINR0 += temp.imeiStruct.iIndoorRSRP105_SINR0;
		imeiStruct.iIndoorRSRP110_SINR3 += temp.imeiStruct.iIndoorRSRP110_SINR3;
		imeiStruct.iIndoorRSRP110_SINR0 += temp.imeiStruct.iIndoorRSRP110_SINR0;
		imeiStruct.iOutdoorRSRP100_SINR0 += temp.imeiStruct.iOutdoorRSRP100_SINR0;
		imeiStruct.iOutdoorRSRP105_SINR0 += temp.imeiStruct.iOutdoorRSRP105_SINR0;
		imeiStruct.iOutdoorRSRP110_SINR3 += temp.imeiStruct.iOutdoorRSRP110_SINR3;
		imeiStruct.iOutdoorRSRP110_SINR0 += temp.imeiStruct.iOutdoorRSRP110_SINR0;
		imeiStruct.iSINR_Indoor_0 += temp.imeiStruct.iSINR_Indoor_0;
		imeiStruct.iRSRQ_Indoor_14 += temp.imeiStruct.iRSRQ_Indoor_14;
		imeiStruct.iSINR_Outdoor_0 += temp.imeiStruct.iSINR_Outdoor_0;
		imeiStruct.iRSRQ_Outdoor_14 += temp.imeiStruct.iRSRQ_Outdoor_14;
		imeiStruct.fOverlapTotal += temp.imeiStruct.fOverlapTotal;
		imeiStruct.iOverlapMRCnt += temp.imeiStruct.iOverlapMRCnt;
		imeiStruct.fOverlapTotalAll += temp.imeiStruct.fOverlapTotalAll;
		imeiStruct.iOverlapMRCntAll += temp.imeiStruct.iOverlapMRCntAll;
		//
		imeiStruct.MRCnt_0_70 += temp.imeiStruct.MRCnt_0_70;
		imeiStruct.MRCnt_70_80 += temp.imeiStruct.MRCnt_70_80;
		imeiStruct.MRCnt_80_90 += temp.imeiStruct.MRCnt_80_90;
		imeiStruct.MRCnt_90_95 += temp.imeiStruct.MRCnt_90_95;
		imeiStruct.MRCnt_100 += temp.imeiStruct.MRCnt_100;
		imeiStruct.MRCnt_103 += temp.imeiStruct.MRCnt_103;
		imeiStruct.MRCnt_105 += temp.imeiStruct.MRCnt_105;
		imeiStruct.MRCnt_110 += temp.imeiStruct.MRCnt_110;
		imeiStruct.MRCnt_113 += temp.imeiStruct.MRCnt_113;
		imeiStruct.RSRP100_SINR0 += temp.imeiStruct.RSRP100_SINR0;
		imeiStruct.RSRP105_SINR0 += temp.imeiStruct.RSRP105_SINR0;
		imeiStruct.RSRP110_SINR3 += temp.imeiStruct.RSRP110_SINR3;
		imeiStruct.RSRP110_SINR0 += temp.imeiStruct.RSRP110_SINR0;
		imeiStruct.SINR_0 += temp.imeiStruct.SINR_0;
		imeiStruct.RSRQ_14 += temp.imeiStruct.RSRQ_14;

		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		// TODO Auto-generated method stub
		try
		{
			imeiStruct = Stat_Imei.FillData(vals, 0);
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
		return imeiStruct.toLine();
	}

}
