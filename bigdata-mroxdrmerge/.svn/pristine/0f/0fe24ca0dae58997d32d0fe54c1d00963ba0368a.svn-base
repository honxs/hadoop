package cn.mastercom.bigdata.mro.stat.mergeBySize;

import cn.mastercom.bigdata.mro.stat.struct.InCellGridMergeDo;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.Func;

public class InCellGridMergeBySize extends InCellGridMergeDo
{
	private StringBuffer sbTemp = new StringBuffer();
	public static final String spliter = "\t";
	public int roundSizeIn = Integer.parseInt(MainModel.GetInstance().getAppConfig().getRoundSizeIn());

	@Override
	public String getMapKey()
	{
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(incellGrid.iCityID);
		sbTemp.append("_");
		sbTemp.append(incellGrid.iBuildingID);
		sbTemp.append("_");
		sbTemp.append(incellGrid.iHeight);
		sbTemp.append("_");
		sbTemp.append(incellGrid.iECI);
		sbTemp.append("_");
		sbTemp.append(Func.getRoundLongtitude(roundSizeIn, incellGrid.tllongitude));
		sbTemp.append("_");
		sbTemp.append(Func.getRoundLatitude(roundSizeIn, incellGrid.tllatitude));
		sbTemp.append("_");
		sbTemp.append(incellGrid.iTime);
		return sbTemp.toString();		
	}
	
	@Override
	public String getData()
	{
		StringBuffer bf = new StringBuffer();
		bf.append(incellGrid.iCityID);
		bf.append(spliter);
		bf.append(incellGrid.iBuildingID);
		bf.append(spliter);
		bf.append(incellGrid.iHeight);
		bf.append(spliter);
		bf.append(Func.getRoundLongtitude(roundSizeIn, incellGrid.tllongitude));
		bf.append(spliter);
		bf.append(Func.getRoundTLLatitude(roundSizeIn, incellGrid.tllatitude));
		bf.append(spliter);
		bf.append(Func.getRoundBRLongtitude(roundSizeIn, incellGrid.tllongitude));
		bf.append(spliter);
		bf.append(Func.getRoundLatitude(roundSizeIn, incellGrid.tllatitude));
		bf.append(spliter);
		bf.append(incellGrid.iECI);
		bf.append(spliter);
		bf.append(incellGrid.iTime);
		bf.append(spliter);
		bf.append(incellGrid.iMRCnt);
		bf.append(spliter);
		bf.append(incellGrid.iMRRSRQCnt);
		bf.append(spliter);
		bf.append(incellGrid.iMRSINRCnt);
		bf.append(spliter);
		bf.append(incellGrid.fRSRPValue);
		bf.append(spliter);
		bf.append(incellGrid.fRSRQValue);
		bf.append(spliter);
		bf.append(incellGrid.fSINRValue);
		bf.append(spliter);
		bf.append(incellGrid.iMRCnt_95);
		bf.append(spliter);
		bf.append(incellGrid.iMRCnt_100);
		bf.append(spliter);
		bf.append(incellGrid.iMRCnt_103);
		bf.append(spliter);
		bf.append(incellGrid.iMRCnt_105);
		bf.append(spliter);
		bf.append(incellGrid.iMRCnt_110);
		bf.append(spliter);
		bf.append(incellGrid.iMRCnt_113);
		bf.append(spliter);
		bf.append(incellGrid.iMRCnt_128);
		bf.append(spliter);
		bf.append(incellGrid.iRSRP100_SINR0);
		bf.append(spliter);
		bf.append(incellGrid.iRSRP105_SINR0);
		bf.append(spliter);
		bf.append(incellGrid.iRSRP110_SINR3);
		bf.append(spliter);
		bf.append(incellGrid.iRSRP110_SINR0);
		bf.append(spliter);
		bf.append(incellGrid.iSINR_0);
		bf.append(spliter);
		bf.append(incellGrid.iRSRQ_14);
		bf.append(spliter);
		bf.append(incellGrid.fOverlapTotal);
		bf.append(spliter);
		bf.append(incellGrid.iOverlapMRCnt);
		bf.append(spliter);
		bf.append(incellGrid.fOverlapTotalAll);
		bf.append(spliter);
		bf.append(incellGrid.iOverlapMRCntAll);
		bf.append(spliter);
		bf.append(incellGrid.fRSRPMax);
		bf.append(spliter);
		bf.append(incellGrid.fRSRPMin);
		bf.append(spliter);
		bf.append(incellGrid.fRSRQMax);
		bf.append(spliter);
		bf.append(incellGrid.fRSRQMin);
		bf.append(spliter);
		bf.append(incellGrid.fSINRMax);
		bf.append(spliter);
		bf.append(incellGrid.fSINRMin);
		return bf.toString();
	}
	
}
