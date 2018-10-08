package cn.mastercom.bigdata.mro.stat;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.mro.stat.struct.Stat_BuildPosUser;
import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.ResultOutputer;

public class BuildPosUserStat extends AMapStatDo_4G<Stat_BuildPosUser>
{

	public BuildPosUserStat(ResultOutputer resultOutputer, int sourceType, int dataType)
	{
		super(resultOutputer, sourceType, dataType);
		// TODO Auto-generated constructor stub
	}

	public BuildPosUserStat(ResultOutputer resultOutputer, int sourceType, int confidenceType, int dataType)
	{
		super(resultOutputer, sourceType, confidenceType, dataType);
	}

	@Override
	protected Object[] getPartitionKeys(DT_Sample_4G sample)
	{
		int ifreq = getIfreq(sourceType, sample);
		return new Object[]
		{ sample.cityID, sample.ibuildingID, sample.iheight, sample.position, sample.IMSI,
				FormatTime.RoundTimeForHour(sample.itime) };

	} 

	@Override
	protected Stat_BuildPosUser createFirstStatItem(DT_Sample_4G sample, Object[] keys)
	{
		Stat_BuildPosUser stat_BuildPosUser = new Stat_BuildPosUser();
		stat_BuildPosUser.doFirstSample(sample, (int) keys[3]);
		return stat_BuildPosUser;
	}

	@Override
	protected boolean statOrNot(DT_Sample_4G sample)
	{
		return sample.ibuildingID > 0 && sample.IMSI > 0;

	}

}
