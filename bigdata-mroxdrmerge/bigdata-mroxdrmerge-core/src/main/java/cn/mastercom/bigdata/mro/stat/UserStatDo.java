package cn.mastercom.bigdata.mro.stat;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mro.stat.struct.UserStat;
import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.ResultOutputer;

public class UserStatDo extends AMapStatDo_4G<UserStat>
{
	public UserStatDo(ResultOutputer typeResult, int sourceType, int dataType)
	{
		this(typeResult, sourceType, StaticConfig.Natural_Abnormal, dataType);
	}

	public UserStatDo(ResultOutputer typeResult, int sourceType, int confidenceType, int dataType)
	{
		super(typeResult, sourceType, confidenceType, dataType);
	}

	@Override
	protected Object[] getPartitionKeys(DT_Sample_4G sample)
	{
		return new Object[] { sample.cityID, FormatTime.RoundTimeForHour(sample.itime), sample.IMSI};
	}

	@Override
	protected UserStat createFirstStatItem(DT_Sample_4G sample, Object[] keys)
	{
		return new UserStat(sample);
	}

	@Override
	protected boolean statOrNot(DT_Sample_4G sample)
	{
		return sample.IMSI > 0;
	}

}
