package cn.mastercom.bigdata.mro.stat.mergeBySize;

import cn.mastercom.bigdata.mro.stat.struct.SceneCellGridMergeDo;

public class SceneCellGridMergeBySize extends SceneCellGridMergeDo
{
	private StringBuffer sbTemp = new StringBuffer();
	public static final String spliter = "\t";
	
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
		sbTemp.append(sceneCellGrid.iTime);
		return sbTemp.toString();
	}
	
	@Override
	public String getData()
	{
		StringBuffer bf = new StringBuffer();
		bf.append(sceneCellGrid.iCityID);
		bf.append(spliter);
		bf.append(sceneCellGrid.iAreaType);
		bf.append(spliter);
		bf.append(sceneCellGrid.iAreaID);
		bf.append(spliter);
		bf.append(sceneCellGrid.tllongitude);
		bf.append(spliter);
		bf.append(sceneCellGrid.tllatitude);
		bf.append(spliter);
		bf.append(sceneCellGrid.brlongitude);
		bf.append(spliter);
		bf.append(sceneCellGrid.brlatitude);
		bf.append(spliter);
		bf.append(sceneCellGrid.iECI);
		bf.append(spliter);
		bf.append(sceneCellGrid.iTime);
		bf.append(spliter);
		bf.append(sceneCellGrid.iMRCnt);
		bf.append(spliter);
		bf.append(sceneCellGrid.iMRRSRQCnt);
		bf.append(spliter);
		bf.append(sceneCellGrid.iMRSINRCnt);
		bf.append(spliter);
		bf.append(sceneCellGrid.fRSRPValue);
		bf.append(spliter);
		bf.append(sceneCellGrid.fRSRQValue);
		bf.append(spliter);
		bf.append(sceneCellGrid.fSINRValue);
		bf.append(spliter);
		bf.append(sceneCellGrid.iMRCnt_95);
		bf.append(spliter);
		bf.append(sceneCellGrid.iMRCnt_100);
		bf.append(spliter);
		bf.append(sceneCellGrid.iMRCnt_103);
		bf.append(spliter);
		bf.append(sceneCellGrid.iMRCnt_105);
		bf.append(spliter);
		bf.append(sceneCellGrid.iMRCnt_110);
		bf.append(spliter);
		bf.append(sceneCellGrid.iMRCnt_113);
		bf.append(spliter);
		bf.append(sceneCellGrid.iMRCnt_128);
		bf.append(spliter);
		bf.append(sceneCellGrid.iRSRP100_SINR0);
		bf.append(spliter);
		bf.append(sceneCellGrid.iRSRP105_SINR0);
		bf.append(spliter);
		bf.append(sceneCellGrid.iRSRP110_SINR3);
		bf.append(spliter);
		bf.append(sceneCellGrid.iRSRP110_SINR0);
		bf.append(spliter);
		bf.append(sceneCellGrid.iSINR_0);
		bf.append(spliter);
		bf.append(sceneCellGrid.iRSRQ_14);
		bf.append(spliter);
		bf.append(sceneCellGrid.fOverlapTotal);
		bf.append(spliter);
		bf.append(sceneCellGrid.iOverlapMRCnt);
		bf.append(spliter);
		bf.append(sceneCellGrid.fOverlapTotalAll);
		bf.append(spliter);
		bf.append(sceneCellGrid.iOverlapMRCntAll);
		bf.append(spliter);
		bf.append(sceneCellGrid.fRSRPMax);
		bf.append(spliter);
		bf.append(sceneCellGrid.fRSRPMin);
		bf.append(spliter);
		bf.append(sceneCellGrid.fRSRQMax);
		bf.append(spliter);
		bf.append(sceneCellGrid.fRSRQMin);
		bf.append(spliter);
		bf.append(sceneCellGrid.fSINRMax);
		bf.append(spliter);
		bf.append(sceneCellGrid.fSINRMin);
		return bf.toString();
	}
}
