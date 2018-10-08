package cn.mastercom.bigdata.mro.loc;

import java.util.HashMap;
import java.util.Map;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;

public class UserActStatMng
{

	private Map<Long, UserActStat> userActStatMap = new HashMap<Long, UserActStat>();

	public Map<Long, UserActStat> getUserActStatMap()
	{
		return userActStatMap;
	}

	public UserActStatMng()
	{
	}

	public void stat(DT_Sample_4G sample)
	{
		if (sample.IMSI <= 0)
		{
			return;
		}

		// 用户行为统计
		UserActStat userActStat = userActStatMap.get(sample.IMSI);
		if (userActStat == null)
		{
			userActStat = new UserActStat(sample.IMSI, sample.MSISDN);
			userActStatMap.put(sample.IMSI, userActStat);
		}
		userActStat.dealSample(sample);

	}

	public void finalStat()
	{
		for (Map.Entry<Long, UserActStat> userActEntry : userActStatMap.entrySet())
		{
			userActEntry.getValue().finalStat();
		}
	}

}
