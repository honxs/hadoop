package cn.mastercom.bigdata.mro.stat.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;

public class UserInCellGridMergeDo implements IMergeDataDo,Serializable{
	
	private int dataType = 0;
	public Stat_UserIn_CellGrid userIn_CellGrid = new Stat_UserIn_CellGrid();
	private StringBuffer sbTemp = new StringBuffer();
	
	@Override
	public String getMapKey() {
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(userIn_CellGrid.imsi);
		sbTemp.append("_");
		sbTemp.append(userIn_CellGrid.iBuildingID);
		sbTemp.append("_");
		sbTemp.append(userIn_CellGrid.iHeight);
		sbTemp.append("_");
		sbTemp.append(userIn_CellGrid.iECI);
		sbTemp.append("_");
		sbTemp.append(userIn_CellGrid.tllongitude);
		sbTemp.append("_");
		sbTemp.append(userIn_CellGrid.tllatitude);
		sbTemp.append("_");
		sbTemp.append(userIn_CellGrid.iTime);
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
		UserInCellGridMergeDo temp = (UserInCellGridMergeDo) o;
		userIn_CellGrid.iMRCnt += temp.userIn_CellGrid.iMRCnt;
		userIn_CellGrid.iMRRSRQCnt += temp.userIn_CellGrid.iMRRSRQCnt;
		userIn_CellGrid.iMRSINRCnt += temp.userIn_CellGrid.iMRSINRCnt;
		userIn_CellGrid.fRSRPValue += temp.userIn_CellGrid.fRSRPValue;
		userIn_CellGrid.fRSRQValue += temp.userIn_CellGrid.fRSRQValue;
		userIn_CellGrid.fSINRValue += temp.userIn_CellGrid.fSINRValue;
		userIn_CellGrid.iMRCnt_95 += temp.userIn_CellGrid.iMRCnt_95;
		userIn_CellGrid.iMRCnt_100 += temp.userIn_CellGrid.iMRCnt_100;
		userIn_CellGrid.iMRCnt_103 += temp.userIn_CellGrid.iMRCnt_103;
		userIn_CellGrid.iMRCnt_105 += temp.userIn_CellGrid.iMRCnt_105;
		userIn_CellGrid.iMRCnt_110 += temp.userIn_CellGrid.iMRCnt_110;
		userIn_CellGrid.iMRCnt_113 += temp.userIn_CellGrid.iMRCnt_113;
		userIn_CellGrid.iMRCnt_128 += temp.userIn_CellGrid.iMRCnt_128;
		userIn_CellGrid.iRSRP100_SINR0 += temp.userIn_CellGrid.iRSRP100_SINR0;
		userIn_CellGrid.iRSRP105_SINR0 += temp.userIn_CellGrid.iRSRP105_SINR0;
		userIn_CellGrid.iRSRP110_SINR3 += temp.userIn_CellGrid.iRSRP110_SINR3;
		userIn_CellGrid.iRSRP110_SINR0 += temp.userIn_CellGrid.iRSRP110_SINR0;
		userIn_CellGrid.iSINR_0 += temp.userIn_CellGrid.iSINR_0;
		userIn_CellGrid.iRSRQ_14 += temp.userIn_CellGrid.iRSRQ_14;
		userIn_CellGrid.fOverlapTotal += temp.userIn_CellGrid.fOverlapTotal;
		userIn_CellGrid.iOverlapMRCnt += temp.userIn_CellGrid.iOverlapMRCnt;
		userIn_CellGrid.fOverlapTotalAll += temp.userIn_CellGrid.fOverlapTotalAll;
		userIn_CellGrid.iOverlapMRCntAll += temp.userIn_CellGrid.iOverlapMRCntAll;
		if (temp.userIn_CellGrid.fRSRPMax > userIn_CellGrid.fRSRPMax)
		{
			userIn_CellGrid.fRSRPMax = temp.userIn_CellGrid.fRSRPMax;
		}
		if ((userIn_CellGrid.fRSRPMin == StaticConfig.Int_Abnormal)
				|| (temp.userIn_CellGrid.fRSRPMin < userIn_CellGrid.fRSRPMin && temp.userIn_CellGrid.fRSRPMin != StaticConfig.Int_Abnormal))
		{
			userIn_CellGrid.fRSRPMin = temp.userIn_CellGrid.fRSRPMin;
		}
		if (temp.userIn_CellGrid.fRSRQMax > userIn_CellGrid.fRSRQMax)
		{
			userIn_CellGrid.fRSRQMax = temp.userIn_CellGrid.fRSRQMax;
		}
		if ((userIn_CellGrid.fRSRQMin == StaticConfig.Int_Abnormal)
				|| (temp.userIn_CellGrid.fRSRQMin < userIn_CellGrid.fRSRQMin && temp.userIn_CellGrid.fRSRQMin != StaticConfig.Int_Abnormal))
		{
			userIn_CellGrid.fRSRQMin = temp.userIn_CellGrid.fRSRQMin;
		}
		if (temp.userIn_CellGrid.fSINRMax > userIn_CellGrid.fSINRMax)
		{
			userIn_CellGrid.fSINRMax = temp.userIn_CellGrid.fSINRMax;
		}
		if ((userIn_CellGrid.fSINRMin == StaticConfig.Int_Abnormal)
				|| (temp.userIn_CellGrid.fSINRMin < userIn_CellGrid.fSINRMin && temp.userIn_CellGrid.fSINRMin != StaticConfig.Int_Abnormal))
		{
			userIn_CellGrid.fSINRMin = temp.userIn_CellGrid.fSINRMin;
		}
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos) {
		try
		{
			userIn_CellGrid = Stat_UserIn_CellGrid.FillData(vals, 0);
		}
		catch (Exception e)
		{
			return false;
		}
		return true;
	}

	@Override
	public String getData() {
		return userIn_CellGrid.mergeStatToLine() ;
	}

}
