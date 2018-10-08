package cn.mastercom.bigdata.mro.stat.mergeBySize;

import cn.mastercom.bigdata.mro.stat.struct.InGridMergeDo;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.Func;

public class InGridMergeBySize extends InGridMergeDo
{
	private StringBuffer sbTemp = new StringBuffer();
	public static final String spliter = "\t";
	public int roundSizeIn = Integer.parseInt(MainModel.GetInstance().getAppConfig().getRoundSizeIn());

	@Override
	public String getMapKey()
	{
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(ingrid.iCityID);
		sbTemp.append("_");
		sbTemp.append(ingrid.iBuildingID);
		sbTemp.append("_");
		sbTemp.append(ingrid.iHeight);
		sbTemp.append("_");
		sbTemp.append(Func.getRoundLongtitude(roundSizeIn, ingrid.tllongitude));
		sbTemp.append("_");
		sbTemp.append(Func.getRoundLatitude(roundSizeIn, ingrid.tllatitude));
		sbTemp.append("_");
		sbTemp.append(ingrid.ifreq);
		sbTemp.append("_");
		sbTemp.append(ingrid.iTime);
		return sbTemp.toString();
	}
	
	@Override
	public String getData()
	{
		StringBuffer bf = new StringBuffer();
		bf.append(ingrid.iCityID);
		bf.append(spliter);
		bf.append(ingrid.iBuildingID);
		bf.append(spliter);
		bf.append(ingrid.iHeight);
		bf.append(spliter);
		bf.append(Func.getRoundLongtitude(roundSizeIn, ingrid.tllongitude));
		bf.append(spliter);
		bf.append(Func.getRoundTLLatitude(roundSizeIn, ingrid.tllatitude));
		bf.append(spliter);
		bf.append(Func.getRoundBRLongtitude(roundSizeIn, ingrid.tllongitude));
		bf.append(spliter);
		bf.append(Func.getRoundLatitude(roundSizeIn, ingrid.tllatitude));
		bf.append(spliter);
		bf.append(ingrid.ifreq);
		bf.append(spliter);
		bf.append(ingrid.iTime);
		bf.append(spliter);
		bf.append(ingrid.iMRCnt);
		bf.append(spliter);
		bf.append(ingrid.iMRRSRQCnt);
		bf.append(spliter);
		bf.append(ingrid.iMRSINRCnt);
		bf.append(spliter);
		bf.append(ingrid.fRSRPValue);
		bf.append(spliter);
		bf.append(ingrid.fRSRQValue);
		bf.append(spliter);
		bf.append(ingrid.fSINRValue);
		bf.append(spliter);
		bf.append(ingrid.iMRCnt_95);
		bf.append(spliter);
		bf.append(ingrid.iMRCnt_100);
		bf.append(spliter);
		bf.append(ingrid.iMRCnt_103);
		bf.append(spliter);
		bf.append(ingrid.iMRCnt_105);
		bf.append(spliter);
		bf.append(ingrid.iMRCnt_110);
		bf.append(spliter);
		bf.append(ingrid.iMRCnt_113);
		bf.append(spliter);
		bf.append(ingrid.iMRCnt_128);
		bf.append(spliter);
		bf.append(ingrid.iRSRP100_SINR0);
		bf.append(spliter);
		bf.append(ingrid.iRSRP105_SINR0);
		bf.append(spliter);
		bf.append(ingrid.iRSRP110_SINR3);
		bf.append(spliter);
		bf.append(ingrid.iRSRP110_SINR0);
		bf.append(spliter);
		bf.append(ingrid.iSINR_0);
		bf.append(spliter);
		bf.append(ingrid.iRSRQ_14);
		bf.append(spliter);
		bf.append(ingrid.fOverlapTotal);
		bf.append(spliter);
		bf.append(ingrid.iOverlapMRCnt);
		bf.append(spliter);
		bf.append(ingrid.fOverlapTotalAll);
		bf.append(spliter);
		bf.append(ingrid.iOverlapMRCntAll);
		bf.append(spliter);
		bf.append(ingrid.fRSRPMax);
		bf.append(spliter);
		bf.append(ingrid.fRSRPMin);
		bf.append(spliter);
		bf.append(ingrid.fRSRQMax);
		bf.append(spliter);
		bf.append(ingrid.fRSRQMin);
		bf.append(spliter);
		bf.append(ingrid.fSINRMax);
		bf.append(spliter);
		bf.append(ingrid.fSINRMin);
		return bf.toString();
	}
}
