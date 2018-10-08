package cn.mastercom.bigdata.mro.stat.mergeBySize;

import cn.mastercom.bigdata.mro.stat.struct.SceneGridMergeDo;

public class SceneGridMergeBySize extends SceneGridMergeDo
{
	private StringBuffer sbTemp = new StringBuffer();
	public static final String spliter = "\t";
	
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
	public String getData()
	{
		StringBuffer bf = new StringBuffer();
		bf.append(sceneGrid.iCityID);
		bf.append(spliter);
		bf.append(sceneGrid.iAreaType);
		bf.append(spliter);
		bf.append(sceneGrid.iAreaID);
		bf.append(spliter);
		bf.append(sceneGrid.tllongitude);
		bf.append(spliter);
		bf.append(sceneGrid.tllatitude);
		bf.append(spliter);
		bf.append(sceneGrid.brlongitude);
		bf.append(spliter);
		bf.append(sceneGrid.brlatitude);
		bf.append(spliter);
		bf.append(sceneGrid.iTime);
		bf.append(spliter);
		bf.append(sceneGrid.iMRCnt);
		bf.append(spliter);
		bf.append(sceneGrid.iMRRSRQCnt);
		bf.append(spliter);
		bf.append(sceneGrid.iMRSINRCnt);
		bf.append(spliter);
		bf.append(sceneGrid.fRSRPValue);
		bf.append(spliter);
		bf.append(sceneGrid.fRSRQValue);
		bf.append(spliter);
		bf.append(sceneGrid.fSINRValue);
		bf.append(spliter);
		bf.append(sceneGrid.iMRCnt_95);
		bf.append(spliter);
		bf.append(sceneGrid.iMRCnt_100);
		bf.append(spliter);
		bf.append(sceneGrid.iMRCnt_103);
		bf.append(spliter);
		bf.append(sceneGrid.iMRCnt_105);
		bf.append(spliter);
		bf.append(sceneGrid.iMRCnt_110);
		bf.append(spliter);
		bf.append(sceneGrid.iMRCnt_113);
		bf.append(spliter);
		bf.append(sceneGrid.iMRCnt_128);
		bf.append(spliter);
		bf.append(sceneGrid.iRSRP100_SINR0);
		bf.append(spliter);
		bf.append(sceneGrid.iRSRP105_SINR0);
		bf.append(spliter);
		bf.append(sceneGrid.iRSRP110_SINR3);
		bf.append(spliter);
		bf.append(sceneGrid.iRSRP110_SINR0);
		bf.append(spliter);
		bf.append(sceneGrid.iSINR_0);
		bf.append(spliter);
		bf.append(sceneGrid.iRSRQ_14);
		bf.append(spliter);
		bf.append(sceneGrid.fOverlapTotal);
		bf.append(spliter);
		bf.append(sceneGrid.iOverlapMRCnt);
		bf.append(spliter);
		bf.append(sceneGrid.fOverlapTotalAll);
		bf.append(spliter);
		bf.append(sceneGrid.iOverlapMRCntAll);
		bf.append(spliter);
		bf.append(sceneGrid.fRSRPMax);
		bf.append(spliter);
		bf.append(sceneGrid.fRSRPMin);
		bf.append(spliter);
		bf.append(sceneGrid.fRSRQMax);
		bf.append(spliter);
		bf.append(sceneGrid.fRSRQMin);
		bf.append(spliter);
		bf.append(sceneGrid.fSINRMax);
		bf.append(spliter);
		bf.append(sceneGrid.fSINRMin);
		return bf.toString();
	}
}
