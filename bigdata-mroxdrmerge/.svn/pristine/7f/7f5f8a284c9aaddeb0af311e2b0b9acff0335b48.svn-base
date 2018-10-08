package cn.mastercom.bigdata.mro.stat;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mro.stat.struct.UserCell;
import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.ResultOutputer;

public class UserCellStatDo extends AMapStatDo_4G<UserCell>
{
	public UserCellStatDo(ResultOutputer typeResult, int sourceType, int dataType)
	{
		this(typeResult, sourceType, StaticConfig.Natural_Abnormal, dataType);
	}

	public UserCellStatDo(ResultOutputer typeResult, int sourceType, int confidenceType, int dataType)
	{
		super(typeResult, sourceType, confidenceType, dataType);
	}

	@Override
	protected Object[] getPartitionKeys(DT_Sample_4G sample)
	{
		return new Object[] { sample.cityID, sample.Eci, FormatTime.RoundTimeForHour(sample.itime), sample.IMSI, sample.MSISDN };
	}

	@Override
	protected UserCell createFirstStatItem(DT_Sample_4G sample, Object[] keys)
	{
		return new UserCell(sample);
	}

	@Override
	protected boolean statOrNot(DT_Sample_4G sample)
	{
		return sample.Eci > 0 && sample.IMSI > 0;
	}

}
