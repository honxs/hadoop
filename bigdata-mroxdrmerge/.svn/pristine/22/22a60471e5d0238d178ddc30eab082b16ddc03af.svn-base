package cn.mastercom.bigdata.mro.stat.mergeBySize;

import cn.mastercom.bigdata.mro.stat.struct.OutCellGridMergeDo;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.Func;

public class OutCellGridMergeBySize extends OutCellGridMergeDo
{
	private StringBuffer sbTemp = new StringBuffer();
	public static final String spliter = "\t";
	public int roundSizeOut =Integer.parseInt(MainModel.GetInstance().getAppConfig().getRoundSizeOut());
	
	@Override
	public String getMapKey()
	{
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(outcellGrid.iCityID);
		sbTemp.append("_");
		sbTemp.append(outcellGrid.iECI);
		sbTemp.append("_");
		sbTemp.append(Func.getRoundLongtitude(roundSizeOut, outcellGrid.tllongitude));
		sbTemp.append("_");
		sbTemp.append(Func.getRoundLatitude(roundSizeOut, outcellGrid.tllatitude));
		sbTemp.append("_");
		sbTemp.append(outcellGrid.iTime);
		return sbTemp.toString();
	}
	
	@Override
	public String getData()
	{
		StringBuffer bf = new StringBuffer();
		bf.append(outcellGrid.iCityID);
		bf.append(spliter);
		bf.append(Func.getRoundLongtitude(roundSizeOut, outcellGrid.tllongitude));
		bf.append(spliter);
		bf.append(Func.getRoundTLLatitude(roundSizeOut, outcellGrid.tllatitude));
		bf.append(spliter);
		bf.append(Func.getRoundBRLongtitude(roundSizeOut, outcellGrid.tllongitude));
		bf.append(spliter);
		bf.append(Func.getRoundLatitude(roundSizeOut, outcellGrid.tllatitude));
		bf.append(spliter);
		bf.append(outcellGrid.iECI);
		bf.append(spliter);
		bf.append(outcellGrid.iTime);
		bf.append(spliter);
		bf.append(outcellGrid.iMRCnt);
		bf.append(spliter);
		bf.append(outcellGrid.iMRRSRQCnt);
		bf.append(spliter);
		bf.append(outcellGrid.iMRSINRCnt);
		bf.append(spliter);
		bf.append(outcellGrid.fRSRPValue);
		bf.append(spliter);
		bf.append(outcellGrid.fRSRQValue);
		bf.append(spliter);
		bf.append(outcellGrid.fSINRValue);
		bf.append(spliter);
		bf.append(outcellGrid.iMRCnt_95);
		bf.append(spliter);
		bf.append(outcellGrid.iMRCnt_100);
		bf.append(spliter);
		bf.append(outcellGrid.iMRCnt_103);
		bf.append(spliter);
		bf.append(outcellGrid.iMRCnt_105);
		bf.append(spliter);
		bf.append(outcellGrid.iMRCnt_110);
		bf.append(spliter);
		bf.append(outcellGrid.iMRCnt_113);
		bf.append(spliter);
		bf.append(outcellGrid.iMRCnt_128);
		bf.append(spliter);
		bf.append(outcellGrid.iRSRP100_SINR0);
		bf.append(spliter);
		bf.append(outcellGrid.iRSRP105_SINR0);
		bf.append(spliter);
		bf.append(outcellGrid.iRSRP110_SINR3);
		bf.append(spliter);
		bf.append(outcellGrid.iRSRP110_SINR0);
		bf.append(spliter);
		bf.append(outcellGrid.iSINR_0);
		bf.append(spliter);
		bf.append(outcellGrid.iRSRQ_14);
		bf.append(spliter);
		bf.append(outcellGrid.fOverlapTotal);
		bf.append(spliter);
		bf.append(outcellGrid.iOverlapMRCnt);
		bf.append(spliter);
		bf.append(outcellGrid.fOverlapTotalAll);
		bf.append(spliter);
		bf.append(outcellGrid.iOverlapMRCntAll);
		bf.append(spliter);
		bf.append(outcellGrid.fRSRPMax);
		bf.append(spliter);
		bf.append(outcellGrid.fRSRPMin);
		bf.append(spliter);
		bf.append(outcellGrid.fRSRQMax);
		bf.append(spliter);
		bf.append(outcellGrid.fRSRQMin);
		bf.append(spliter);
		bf.append(outcellGrid.fSINRMax);
		bf.append(spliter);
		bf.append(outcellGrid.fSINRMin);
		return bf.toString();
	}
}
