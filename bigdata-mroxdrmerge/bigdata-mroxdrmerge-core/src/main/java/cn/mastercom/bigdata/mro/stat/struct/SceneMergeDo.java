package cn.mastercom.bigdata.mro.stat.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;

public class SceneMergeDo implements IMergeDataDo,Serializable
{
	private int dataType = 0;
	public Stat_Scene scene = new Stat_Scene();
	private StringBuffer sbTemp = new StringBuffer();

	@Override
	public String getMapKey()
	{
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(scene.iCityID);
		sbTemp.append("_");
		sbTemp.append(scene.iAreaType);
		sbTemp.append("_");
		sbTemp.append(scene.iAreaID);
		sbTemp.append("_");
		sbTemp.append(scene.iTime);
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
		SceneMergeDo temp = (SceneMergeDo) o;
		scene.iMRCnt += temp.scene.iMRCnt;
		scene.iMRCnt_Indoor += temp.scene.iMRCnt_Indoor;
		scene.iMRCnt_Outdoor += temp.scene.iMRCnt_Outdoor;
		scene.iMRRSRQCnt += temp.scene.iMRRSRQCnt;
		scene.iMRRSRQCnt_Indoor += temp.scene.iMRRSRQCnt_Indoor;
		scene.iMRRSRQCnt_Outdoor += temp.scene.iMRRSRQCnt_Outdoor;
		scene.iMRSINRCnt += temp.scene.iMRSINRCnt;
		scene.iMRSINRCnt_Indoor += temp.scene.iMRSINRCnt_Indoor;
		scene.iMRSINRCnt_Outdoor += temp.scene.iMRSINRCnt_Outdoor;
		scene.fRSRPValue += temp.scene.fRSRPValue;
		scene.fRSRPValue_Indoor += temp.scene.fRSRPValue_Indoor;
		scene.fRSRPValue_Outdoor += temp.scene.fRSRPValue_Outdoor;
		scene.fRSRQValue += temp.scene.fRSRQValue;
		scene.fRSRQValue_Indoor += temp.scene.fRSRQValue_Indoor;
		scene.fRSRQValue_Outdoor += temp.scene.fRSRQValue_Outdoor;
		scene.fSINRValue += temp.scene.fSINRValue;
		scene.fSINRValue_Indoor += temp.scene.fSINRValue_Indoor;
		scene.fSINRValue_Outdoor += temp.scene.fSINRValue_Outdoor;
		scene.iMRCnt_Indoor_0_70 += temp.scene.iMRCnt_Indoor_0_70;
		scene.iMRCnt_Indoor_70_80 += temp.scene.iMRCnt_Indoor_70_80;
		scene.iMRCnt_Indoor_80_90 += temp.scene.iMRCnt_Indoor_80_90;
		scene.iMRCnt_Indoor_90_95 += temp.scene.iMRCnt_Indoor_90_95;
		scene.iMRCnt_Indoor_100 += temp.scene.iMRCnt_Indoor_100;
		scene.iMRCnt_Indoor_103 += temp.scene.iMRCnt_Indoor_103;
		scene.iMRCnt_Indoor_105 += temp.scene.iMRCnt_Indoor_105;
		scene.iMRCnt_Indoor_110 += temp.scene.iMRCnt_Indoor_110;
		scene.iMRCnt_Indoor_113 += temp.scene.iMRCnt_Indoor_113;
		scene.iMRCnt_Outdoor_0_70 += temp.scene.iMRCnt_Outdoor_0_70;
		scene.iMRCnt_Outdoor_70_80 += temp.scene.iMRCnt_Outdoor_70_80;
		scene.iMRCnt_Outdoor_80_90 += temp.scene.iMRCnt_Outdoor_80_90;
		scene.iMRCnt_Outdoor_90_95 += temp.scene.iMRCnt_Outdoor_90_95;
		scene.iMRCnt_Outdoor_100 += temp.scene.iMRCnt_Outdoor_100;
		scene.iMRCnt_Outdoor_103 += temp.scene.iMRCnt_Outdoor_103;
		scene.iMRCnt_Outdoor_105 += temp.scene.iMRCnt_Outdoor_105;
		scene.iMRCnt_Outdoor_110 += temp.scene.iMRCnt_Outdoor_110;
		scene.iMRCnt_Outdoor_113 += temp.scene.iMRCnt_Outdoor_113;
		scene.iIndoorRSRP100_SINR0 += temp.scene.iIndoorRSRP100_SINR0;
		scene.iIndoorRSRP105_SINR0 += temp.scene.iIndoorRSRP105_SINR0;
		scene.iIndoorRSRP110_SINR3 += temp.scene.iIndoorRSRP110_SINR3;
		scene.iIndoorRSRP110_SINR0 += temp.scene.iIndoorRSRP110_SINR0;
		scene.iOutdoorRSRP100_SINR0 += temp.scene.iOutdoorRSRP100_SINR0;
		scene.iOutdoorRSRP105_SINR0 += temp.scene.iOutdoorRSRP105_SINR0;
		scene.iOutdoorRSRP110_SINR3 += temp.scene.iOutdoorRSRP110_SINR3;
		scene.iOutdoorRSRP110_SINR0 += temp.scene.iOutdoorRSRP110_SINR0;
		scene.iSINR_Indoor_0 += temp.scene.iSINR_Indoor_0;
		scene.iRSRQ_Indoor_14 += temp.scene.iRSRQ_Indoor_14;
		scene.iSINR_Outdoor_0 += temp.scene.iSINR_Outdoor_0;
		scene.iRSRQ_Outdoor_14 += temp.scene.iRSRQ_Outdoor_14;
		scene.fOverlapTotal += temp.scene.fOverlapTotal;
		scene.iOverlapMRCnt += temp.scene.iOverlapMRCnt;
		scene.fOverlapTotalAll += temp.scene.fOverlapTotalAll;
		scene.iOverlapMRCntAll += temp.scene.iOverlapMRCntAll;

		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		scene = Stat_Scene.FillData(vals, 0);
		return true;
	}

	@Override
	public String getData()
	{
		return scene.toLine();
	}

}
