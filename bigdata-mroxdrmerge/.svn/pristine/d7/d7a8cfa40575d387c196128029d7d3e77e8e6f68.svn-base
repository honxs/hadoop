package cn.mastercom.bigdata.mro.stat.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;

public class SceneGridMergeDo implements IMergeDataDo,Serializable
{
	private int dataType = 0;
	public Scene_Grid sceneGrid = new Scene_Grid();
	private StringBuffer sbTemp = new StringBuffer();

	@Override
	public String getMapKey()
	{
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(sceneGrid.iCityID);
		sbTemp.append("_");
		sbTemp.append(sceneGrid.iAreaType);
		sbTemp.append("_");
		sbTemp.append(sceneGrid.iAreaID);
		sbTemp.append("_");
		sbTemp.append(sceneGrid.tllongitude);
		sbTemp.append("_");
		sbTemp.append(sceneGrid.tllatitude);
		sbTemp.append("_");
		sbTemp.append(sceneGrid.iTime);
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
		SceneGridMergeDo temp = (SceneGridMergeDo) o;
		sceneGrid.iMRCnt += temp.sceneGrid.iMRCnt;
		sceneGrid.iMRRSRQCnt += temp.sceneGrid.iMRRSRQCnt;
		sceneGrid.iMRSINRCnt += temp.sceneGrid.iMRSINRCnt;
		sceneGrid.fRSRPValue += temp.sceneGrid.fRSRPValue;
		sceneGrid.fRSRQValue += temp.sceneGrid.fRSRQValue;
		sceneGrid.fSINRValue += temp.sceneGrid.fSINRValue;
		sceneGrid.iMRCnt_95 += temp.sceneGrid.iMRCnt_95;
		sceneGrid.iMRCnt_100 += temp.sceneGrid.iMRCnt_100;
		sceneGrid.iMRCnt_103 += temp.sceneGrid.iMRCnt_103;
		sceneGrid.iMRCnt_105 += temp.sceneGrid.iMRCnt_105;
		sceneGrid.iMRCnt_110 += temp.sceneGrid.iMRCnt_110;
		sceneGrid.iMRCnt_113 += temp.sceneGrid.iMRCnt_113;
		sceneGrid.iMRCnt_128 += temp.sceneGrid.iMRCnt_128;
		sceneGrid.iRSRP100_SINR0 += temp.sceneGrid.iRSRP100_SINR0;
		sceneGrid.iRSRP105_SINR0 += temp.sceneGrid.iRSRP105_SINR0;
		sceneGrid.iRSRP110_SINR3 += temp.sceneGrid.iRSRP110_SINR3;
		sceneGrid.iRSRP110_SINR0 += temp.sceneGrid.iRSRP110_SINR0;
		sceneGrid.iSINR_0 += temp.sceneGrid.iSINR_0;
		sceneGrid.iRSRQ_14 += temp.sceneGrid.iRSRQ_14;
		sceneGrid.fOverlapTotal += temp.sceneGrid.fOverlapTotal;
		sceneGrid.iOverlapMRCnt += temp.sceneGrid.iOverlapMRCnt;
		sceneGrid.fOverlapTotalAll += temp.sceneGrid.fOverlapTotalAll;
		sceneGrid.iOverlapMRCntAll += temp.sceneGrid.iOverlapMRCntAll;
		if (temp.sceneGrid.fRSRPMax > sceneGrid.fRSRPMax)
		{
			sceneGrid.fRSRPMax = temp.sceneGrid.fRSRPMax;
		}
		if ((sceneGrid.fRSRPMin == StaticConfig.Int_Abnormal) || (temp.sceneGrid.fRSRPMin < sceneGrid.fRSRPMin && temp.sceneGrid.fRSRPMin != StaticConfig.Int_Abnormal))
		{
			sceneGrid.fRSRPMin = temp.sceneGrid.fRSRPMin;
		}
		if (temp.sceneGrid.fRSRQMax > sceneGrid.fRSRQMax)
		{
			sceneGrid.fRSRQMax = temp.sceneGrid.fRSRQMax;
		}
		if ((sceneGrid.fRSRQMin == StaticConfig.Int_Abnormal) || (temp.sceneGrid.fRSRQMin < sceneGrid.fRSRQMin && temp.sceneGrid.fRSRQMin != StaticConfig.Int_Abnormal))
		{
			sceneGrid.fRSRQMin = temp.sceneGrid.fRSRQMin;
		}
		if (temp.sceneGrid.fSINRMax > sceneGrid.fSINRMax)
		{
			sceneGrid.fSINRMax = temp.sceneGrid.fSINRMax;
		}
		if ((sceneGrid.fSINRMin == StaticConfig.Int_Abnormal) || (temp.sceneGrid.fSINRMin < sceneGrid.fSINRMin && temp.sceneGrid.fSINRMin != StaticConfig.Int_Abnormal))
		{
			sceneGrid.fSINRMin = temp.sceneGrid.fSINRMin;
		}
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		// TODO Auto-generated method stub
		sceneGrid = Scene_Grid.FillData(vals, 0);
		return true;
	}

	@Override
	public String getData()
	{
		// TODO Auto-generated method stub
		return sceneGrid.toLine();
	}

}
