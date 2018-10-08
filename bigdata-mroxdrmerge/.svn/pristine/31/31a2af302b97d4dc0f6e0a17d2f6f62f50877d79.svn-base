package cn.mastercom.bigdata.mro.stat.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;
import cn.mastercom.bigdata.util.TimeHelper;

public class SceneCellGridMergeDo implements IMergeDataDo,Serializable
{
	private int dataType = 0;
	public Scene_CellGrid sceneCellGrid = new Scene_CellGrid();
	private StringBuffer sbTemp = new StringBuffer();

	@Override
	public String getMapKey()
	{
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(sceneCellGrid.iCityID);
		sbTemp.append("_");
		sbTemp.append(sceneCellGrid.iAreaType);
		sbTemp.append("_");
		sbTemp.append(sceneCellGrid.iAreaID);
		sbTemp.append("_");
		sbTemp.append(sceneCellGrid.tllongitude);
		sbTemp.append("_");
		sbTemp.append(sceneCellGrid.tllatitude);
		sbTemp.append("_");
		sbTemp.append(sceneCellGrid.iECI);
		sbTemp.append("_");
		sbTemp.append(TimeHelper.getRoundDayTime(sceneCellGrid.iTime));
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
		SceneCellGridMergeDo temp = (SceneCellGridMergeDo) o;
		sceneCellGrid.iMRCnt += temp.sceneCellGrid.iMRCnt;
		// sceneCellGrid.iMRCnt_Out_URI += temp.sceneCellGrid.iMRCnt_Out_URI;
		// sceneCellGrid.iMRCnt_Out_SDK += temp.sceneCellGrid.iMRCnt_Out_SDK;
		// sceneCellGrid.iMRCnt_Out_HIGH += temp.sceneCellGrid.iMRCnt_Out_HIGH;
		// sceneCellGrid.iMRCnt_Out_SIMU += temp.sceneCellGrid.iMRCnt_Out_SIMU;
		// sceneCellGrid.iMRCnt_Out_Other +=
		// temp.sceneCellGrid.iMRCnt_Out_Other;
		sceneCellGrid.iMRRSRQCnt += temp.sceneCellGrid.iMRRSRQCnt;
		sceneCellGrid.iMRSINRCnt += temp.sceneCellGrid.iMRSINRCnt;
		sceneCellGrid.fRSRPValue += temp.sceneCellGrid.fRSRPValue;
		sceneCellGrid.fRSRQValue += temp.sceneCellGrid.fRSRQValue;
		sceneCellGrid.fSINRValue += temp.sceneCellGrid.fSINRValue;
		sceneCellGrid.iMRCnt_95 += temp.sceneCellGrid.iMRCnt_95;
		sceneCellGrid.iMRCnt_100 += temp.sceneCellGrid.iMRCnt_100;
		sceneCellGrid.iMRCnt_103 += temp.sceneCellGrid.iMRCnt_103;
		sceneCellGrid.iMRCnt_105 += temp.sceneCellGrid.iMRCnt_105;
		sceneCellGrid.iMRCnt_110 += temp.sceneCellGrid.iMRCnt_110;
		sceneCellGrid.iMRCnt_113 += temp.sceneCellGrid.iMRCnt_113;
		sceneCellGrid.iMRCnt_128 += temp.sceneCellGrid.iMRCnt_128;
		sceneCellGrid.iRSRP100_SINR0 += temp.sceneCellGrid.iRSRP100_SINR0;
		sceneCellGrid.iRSRP105_SINR0 += temp.sceneCellGrid.iRSRP105_SINR0;
		sceneCellGrid.iRSRP110_SINR3 += temp.sceneCellGrid.iRSRP110_SINR3;
		sceneCellGrid.iRSRP110_SINR0 += temp.sceneCellGrid.iRSRP110_SINR0;
		sceneCellGrid.iSINR_0 += temp.sceneCellGrid.iSINR_0;
		sceneCellGrid.iRSRQ_14 += temp.sceneCellGrid.iRSRQ_14;
		// sceneCellGrid.iASNei_MRCnt += temp.sceneCellGrid.iASNei_MRCnt;
		// sceneCellGrid.fASNei_RSRPValue +=
		// temp.sceneCellGrid.fASNei_RSRPValue;
		sceneCellGrid.fOverlapTotal += temp.sceneCellGrid.fOverlapTotal;
		sceneCellGrid.iOverlapMRCnt += temp.sceneCellGrid.iOverlapMRCnt;
		sceneCellGrid.fOverlapTotalAll += temp.sceneCellGrid.fOverlapTotalAll;
		sceneCellGrid.iOverlapMRCntAll += temp.sceneCellGrid.iOverlapMRCntAll;
		if (temp.sceneCellGrid.fRSRPMax > sceneCellGrid.fRSRPMax)
		{
			sceneCellGrid.fRSRPMax = temp.sceneCellGrid.fRSRPMax;
		}
		if ((sceneCellGrid.fRSRPMin == StaticConfig.Int_Abnormal)
				|| (temp.sceneCellGrid.fRSRPMin < sceneCellGrid.fRSRPMin
						&& temp.sceneCellGrid.fRSRPMin != StaticConfig.Int_Abnormal))
		{
			sceneCellGrid.fRSRPMin = temp.sceneCellGrid.fRSRPMin;
		}
		if (temp.sceneCellGrid.fRSRQMax > sceneCellGrid.fRSRQMax)
		{
			sceneCellGrid.fRSRQMax = temp.sceneCellGrid.fRSRQMax;
		}
		if ((sceneCellGrid.fRSRQMin == StaticConfig.Int_Abnormal)
				|| (temp.sceneCellGrid.fRSRQMin < sceneCellGrid.fRSRQMin
						&& temp.sceneCellGrid.fRSRQMin != StaticConfig.Int_Abnormal))
		{
			sceneCellGrid.fRSRQMin = temp.sceneCellGrid.fRSRQMin;
		}
		if (temp.sceneCellGrid.fSINRMax > sceneCellGrid.fSINRMax)
		{
			sceneCellGrid.fSINRMax = temp.sceneCellGrid.fSINRMax;
		}
		if ((sceneCellGrid.fSINRMin == StaticConfig.Int_Abnormal)
				|| (temp.sceneCellGrid.fSINRMin < sceneCellGrid.fSINRMin
						&& temp.sceneCellGrid.fSINRMin != StaticConfig.Int_Abnormal))
		{
			sceneCellGrid.fSINRMin = temp.sceneCellGrid.fSINRMin;
		}

		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		sceneCellGrid = Scene_CellGrid.FillData(vals, 0);
		return true;
	}

	@Override
	public String getData()
	{
		return sceneCellGrid.toLine();
	}

}
