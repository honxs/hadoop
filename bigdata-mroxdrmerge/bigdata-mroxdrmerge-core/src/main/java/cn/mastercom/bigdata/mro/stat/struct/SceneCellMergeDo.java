package cn.mastercom.bigdata.mro.stat.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;

public class SceneCellMergeDo implements IMergeDataDo,Serializable
{
	private int dataType = 0;
	public Scene_Cell sceneCell = new Scene_Cell();
	private StringBuffer sbTemp = new StringBuffer();

	@Override
	public String getMapKey()
	{
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(sceneCell.iCityID);
		sbTemp.append("_");
		sbTemp.append(sceneCell.iAreaType);
		sbTemp.append("_");
		sbTemp.append(sceneCell.iAreaID);
		sbTemp.append("_");
		sbTemp.append(sceneCell.iECI);
		sbTemp.append("_");
		sbTemp.append(sceneCell.iTime);
		return sbTemp.toString();
	}

	@Override
	public int getDataType()
	{
		return dataType;
	}

	@Override
	public int setDataType(int dataType)
	{
		this.dataType = dataType;
		return 0;
	}

	@Override
	public boolean mergeData(Object o)
	{
		SceneCellMergeDo temp = (SceneCellMergeDo) o;
		sceneCell.iMRCnt += temp.sceneCell.iMRCnt;
		sceneCell.iMRCnt_Indoor += temp.sceneCell.iMRCnt_Indoor;
		sceneCell.iMRCnt_Outdoor += temp.sceneCell.iMRCnt_Outdoor;
		sceneCell.iMRRSRQCnt += temp.sceneCell.iMRRSRQCnt;
		sceneCell.iMRRSRQCnt_Indoor += temp.sceneCell.iMRRSRQCnt_Indoor;
		sceneCell.iMRRSRQCnt_Outdoor += temp.sceneCell.iMRRSRQCnt_Outdoor;
		sceneCell.iMRSINRCnt += temp.sceneCell.iMRSINRCnt;
		sceneCell.iMRSINRCnt_Indoor += temp.sceneCell.iMRSINRCnt_Indoor;
		sceneCell.iMRSINRCnt_Outdoor += temp.sceneCell.iMRSINRCnt_Outdoor;
		sceneCell.fRSRPValue += temp.sceneCell.fRSRPValue;
		sceneCell.fRSRPValue_Indoor += temp.sceneCell.fRSRPValue_Indoor;
		sceneCell.fRSRPValue_Outdoor += temp.sceneCell.fRSRPValue_Outdoor;
		sceneCell.fRSRQValue += temp.sceneCell.fRSRQValue;
		sceneCell.fRSRQValue_Indoor += temp.sceneCell.fRSRQValue_Indoor;
		sceneCell.fRSRQValue_Outdoor += temp.sceneCell.fRSRQValue_Outdoor;
		sceneCell.fSINRValue += temp.sceneCell.fSINRValue;
		sceneCell.fSINRValue_Indoor += temp.sceneCell.fSINRValue_Indoor;
		sceneCell.fSINRValue_Outdoor += temp.sceneCell.fSINRValue_Outdoor;
		sceneCell.iMRCnt_Indoor_0_70 += temp.sceneCell.iMRCnt_Indoor_0_70;
		sceneCell.iMRCnt_Indoor_70_80 += temp.sceneCell.iMRCnt_Indoor_70_80;
		sceneCell.iMRCnt_Indoor_80_90 += temp.sceneCell.iMRCnt_Indoor_80_90;
		sceneCell.iMRCnt_Indoor_90_95 += temp.sceneCell.iMRCnt_Indoor_90_95;
		sceneCell.iMRCnt_Indoor_100 += temp.sceneCell.iMRCnt_Indoor_100;
		sceneCell.iMRCnt_Indoor_103 += temp.sceneCell.iMRCnt_Indoor_103;
		sceneCell.iMRCnt_Indoor_105 += temp.sceneCell.iMRCnt_Indoor_105;
		sceneCell.iMRCnt_Indoor_110 += temp.sceneCell.iMRCnt_Indoor_110;
		sceneCell.iMRCnt_Indoor_113 += temp.sceneCell.iMRCnt_Indoor_113;
		sceneCell.iMRCnt_Outdoor_0_70 += temp.sceneCell.iMRCnt_Outdoor_0_70;
		sceneCell.iMRCnt_Outdoor_70_80 += temp.sceneCell.iMRCnt_Outdoor_70_80;
		sceneCell.iMRCnt_Outdoor_80_90 += temp.sceneCell.iMRCnt_Outdoor_80_90;
		sceneCell.iMRCnt_Outdoor_90_95 += temp.sceneCell.iMRCnt_Outdoor_90_95;
		sceneCell.iMRCnt_Outdoor_100 += temp.sceneCell.iMRCnt_Outdoor_100;
		sceneCell.iMRCnt_Outdoor_103 += temp.sceneCell.iMRCnt_Outdoor_103;
		sceneCell.iMRCnt_Outdoor_105 += temp.sceneCell.iMRCnt_Outdoor_105;
		sceneCell.iMRCnt_Outdoor_110 += temp.sceneCell.iMRCnt_Outdoor_110;
		sceneCell.iMRCnt_Outdoor_113 += temp.sceneCell.iMRCnt_Outdoor_113;
		sceneCell.iIndoorRSRP100_SINR0 += temp.sceneCell.iIndoorRSRP100_SINR0;
		sceneCell.iIndoorRSRP105_SINR0 += temp.sceneCell.iIndoorRSRP105_SINR0;
		sceneCell.iIndoorRSRP110_SINR3 += temp.sceneCell.iIndoorRSRP110_SINR3;
		sceneCell.iIndoorRSRP110_SINR0 += temp.sceneCell.iIndoorRSRP110_SINR0;
		sceneCell.iOutdoorRSRP100_SINR0 += temp.sceneCell.iOutdoorRSRP100_SINR0;
		sceneCell.iOutdoorRSRP105_SINR0 += temp.sceneCell.iOutdoorRSRP105_SINR0;
		sceneCell.iOutdoorRSRP110_SINR3 += temp.sceneCell.iOutdoorRSRP110_SINR3;
		sceneCell.iOutdoorRSRP110_SINR0 += temp.sceneCell.iOutdoorRSRP110_SINR0;
		sceneCell.iSINR_Indoor_0 += temp.sceneCell.iSINR_Indoor_0;
		sceneCell.iRSRQ_Indoor_14 += temp.sceneCell.iRSRQ_Indoor_14;
		sceneCell.iSINR_Outdoor_0 += temp.sceneCell.iSINR_Outdoor_0;
		sceneCell.iRSRQ_Outdoor_14 += temp.sceneCell.iRSRQ_Outdoor_14;
		sceneCell.fOverlapTotal += temp.sceneCell.fOverlapTotal;
		sceneCell.iOverlapMRCnt += temp.sceneCell.iOverlapMRCnt;
		sceneCell.fOverlapTotalAll += temp.sceneCell.fOverlapTotalAll;
		sceneCell.iOverlapMRCntAll += temp.sceneCell.iOverlapMRCntAll;
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		// TODO Auto-generated method stub
		sceneCell = Scene_Cell.FillData(vals, 0);
		return true;
	}

	@Override
	public String getData()
	{
		// TODO Auto-generated method stub
		return sceneCell.toLine();
	}

}
