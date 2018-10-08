package cn.mastercom.bigdata.mro.stat.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;

public class UserOutCellGridMergeDo implements IMergeDataDo,Serializable{
	
	private int dataType = 0;
	public Stat_UserOut_CellGrid userOut_CellGrid = new Stat_UserOut_CellGrid();
	private StringBuffer sbTemp = new StringBuffer();
	
	@Override
	public String getMapKey() {
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(userOut_CellGrid.imsi);
		sbTemp.append("_");
		sbTemp.append(userOut_CellGrid.iECI);
		sbTemp.append("_");
		sbTemp.append(userOut_CellGrid.tllongitude);
		sbTemp.append("_");
		sbTemp.append(userOut_CellGrid.tllatitude);
		sbTemp.append("_");
		sbTemp.append(userOut_CellGrid.iTime);
		return sbTemp.toString();
	}

	@Override
	public int getDataType() {
		return dataType;
	}

	@Override
	public int setDataType(int dataType) {
		this.dataType = dataType;
		return 0;
	}

	@Override
	public boolean mergeData(Object o) {
		UserOutCellGridMergeDo temp = (UserOutCellGridMergeDo) o;
		userOut_CellGrid.iMRCnt += temp.userOut_CellGrid.iMRCnt;
		userOut_CellGrid.iMRRSRQCnt += temp.userOut_CellGrid.iMRRSRQCnt;
		userOut_CellGrid.iMRSINRCnt += temp.userOut_CellGrid.iMRSINRCnt;
		userOut_CellGrid.fRSRPValue += temp.userOut_CellGrid.fRSRPValue;
		userOut_CellGrid.fRSRQValue += temp.userOut_CellGrid.fRSRQValue;
		userOut_CellGrid.fSINRValue += temp.userOut_CellGrid.fSINRValue;
		userOut_CellGrid.iMRCnt_95 += temp.userOut_CellGrid.iMRCnt_95;
		userOut_CellGrid.iMRCnt_100 += temp.userOut_CellGrid.iMRCnt_100;
		userOut_CellGrid.iMRCnt_103 += temp.userOut_CellGrid.iMRCnt_103;
		userOut_CellGrid.iMRCnt_105 += temp.userOut_CellGrid.iMRCnt_105;
		userOut_CellGrid.iMRCnt_110 += temp.userOut_CellGrid.iMRCnt_110;
		userOut_CellGrid.iMRCnt_113 += temp.userOut_CellGrid.iMRCnt_113;
		userOut_CellGrid.iMRCnt_128 += temp.userOut_CellGrid.iMRCnt_128;
		userOut_CellGrid.iRSRP100_SINR0 += temp.userOut_CellGrid.iRSRP100_SINR0;
		userOut_CellGrid.iRSRP105_SINR0 += temp.userOut_CellGrid.iRSRP105_SINR0;
		userOut_CellGrid.iRSRP110_SINR3 += temp.userOut_CellGrid.iRSRP110_SINR3;
		userOut_CellGrid.iRSRP110_SINR0 += temp.userOut_CellGrid.iRSRP110_SINR0;
		userOut_CellGrid.iSINR_0 += temp.userOut_CellGrid.iSINR_0;
		userOut_CellGrid.iRSRQ_14 += temp.userOut_CellGrid.iRSRQ_14;
		userOut_CellGrid.fOverlapTotal += temp.userOut_CellGrid.fOverlapTotal;
		userOut_CellGrid.iOverlapMRCnt += temp.userOut_CellGrid.iOverlapMRCnt;
		userOut_CellGrid.fOverlapTotalAll += temp.userOut_CellGrid.fOverlapTotalAll;
		userOut_CellGrid.iOverlapMRCntAll += temp.userOut_CellGrid.iOverlapMRCntAll;
		if (temp.userOut_CellGrid.fRSRPMax > userOut_CellGrid.fRSRPMax)
		{
			userOut_CellGrid.fRSRPMax = temp.userOut_CellGrid.fRSRPMax;
		}
		if ((userOut_CellGrid.fRSRPMin == StaticConfig.Int_Abnormal)
				|| (temp.userOut_CellGrid.fRSRPMin < userOut_CellGrid.fRSRPMin && temp.userOut_CellGrid.fRSRPMin != StaticConfig.Int_Abnormal))
		{
			userOut_CellGrid.fRSRPMin = temp.userOut_CellGrid.fRSRPMin;
		}
		if (temp.userOut_CellGrid.fRSRQMax > userOut_CellGrid.fRSRQMax)
		{
			userOut_CellGrid.fRSRQMax = temp.userOut_CellGrid.fRSRQMax;
		}
		if ((userOut_CellGrid.fRSRQMin == StaticConfig.Int_Abnormal)
				|| (temp.userOut_CellGrid.fRSRQMin < userOut_CellGrid.fRSRQMin && temp.userOut_CellGrid.fRSRQMin != StaticConfig.Int_Abnormal))
		{
			userOut_CellGrid.fRSRQMin = temp.userOut_CellGrid.fRSRQMin;
		}
		if (temp.userOut_CellGrid.fSINRMax > userOut_CellGrid.fSINRMax)
		{
			userOut_CellGrid.fSINRMax = temp.userOut_CellGrid.fSINRMax;
		}
		if ((userOut_CellGrid.fSINRMin == StaticConfig.Int_Abnormal)
				|| (temp.userOut_CellGrid.fSINRMin < userOut_CellGrid.fSINRMin && temp.userOut_CellGrid.fSINRMin != StaticConfig.Int_Abnormal))
		{
			userOut_CellGrid.fSINRMin = temp.userOut_CellGrid.fSINRMin;
		}
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos) {
		try
		{
			userOut_CellGrid = Stat_UserOut_CellGrid.FillData(vals, 0);
		}
		catch (Exception e)
		{
			return false;
		}
		return true;
	}

	@Override
	public String getData() {
		return userOut_CellGrid.mergeStatToLine() ;
	}

}
