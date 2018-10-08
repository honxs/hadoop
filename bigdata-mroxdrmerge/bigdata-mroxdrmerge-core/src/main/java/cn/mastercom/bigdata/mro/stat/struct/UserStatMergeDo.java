package cn.mastercom.bigdata.mro.stat.struct;

import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;

import java.io.Serializable;


public class UserStatMergeDo implements IMergeDataDo,Serializable
{
	private int dataType = 0;
	public UserStat userStat = new UserStat();
	private StringBuffer sbTemp = new StringBuffer();

	@Override
	public String getMapKey()
	{
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(userStat.CityID);
		sbTemp.append("_");
		sbTemp.append(userStat.Time);
		sbTemp.append("_");
		sbTemp.append(userStat.Imsi);
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
		UserStatMergeDo temp = (UserStatMergeDo) o;
		if (temp.userStat.Msisdn.length()>0)
		{
			userStat.Msisdn = temp.userStat.Msisdn;
		}
		userStat.mmeCount += temp.userStat.mmeCount;
		userStat.httpCount += temp.userStat.httpCount;
		userStat.locCount += temp.userStat.locCount;
		userStat.mroCount += temp.userStat.mroCount;
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		try
		{
			userStat = userStat.FillData(vals, sPos);
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
		return userStat.mergeStatToLine();
	}

}
