package cn.mastercom.bigdata.mro.stat.struct;

import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;

import java.io.Serializable;


public class UserCellByMinMergeDo implements IMergeDataDo,Serializable
{
	private int dataType = 0;
	public UserCellByMin userCellByMin = new UserCellByMin();
	private StringBuffer sbTemp = new StringBuffer();

	@Override
	public String getMapKey()
	{
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(userCellByMin.CityID);
		sbTemp.append("_");
		sbTemp.append(userCellByMin.Imsi);
		sbTemp.append("_");
		sbTemp.append(userCellByMin.ECI);
		sbTemp.append("_");
		sbTemp.append(userCellByMin.Time);
		return sbTemp.toString();
	}

	@Override
	public int getDataType()
	{
		// TODO Auto-generated method stub
		return dataType;
	}

	@Override
	public int setDataType(int dataType)
	{
		// TODO Auto-generated method stub
		this.dataType = dataType;
		return 0;
	}

	@Override
	public boolean mergeData(Object o)
	{
		UserCellByMinMergeDo temp = (UserCellByMinMergeDo) o;
		userCellByMin.MRCnt += temp.userCellByMin.MRCnt;
		userCellByMin.MRRSRQCnt += temp.userCellByMin.MRRSRQCnt;
		userCellByMin.MRSINRCnt += temp.userCellByMin.MRSINRCnt;
		userCellByMin.MRPHRCnt += temp.userCellByMin.MRPHRCnt;
		userCellByMin.MRTADVCnt += temp.userCellByMin.MRTADVCnt;
		userCellByMin.RSRPValue += temp.userCellByMin.RSRPValue;
		userCellByMin.RSRQValue += temp.userCellByMin.RSRQValue;
		userCellByMin.SINRValue += temp.userCellByMin.SINRValue;
		userCellByMin.PHRValue += temp.userCellByMin.PHRValue;
		userCellByMin.TaDvValue += temp.userCellByMin.TaDvValue;
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		try
		{
			userCellByMin = userCellByMin.FillData(vals, sPos);
		}
		catch (Exception e)
		{
			return false;
		}
		return true;
	}

	@Override
	public String getData()
	{
		return userCellByMin.mergeStatToLine();
	}

}
