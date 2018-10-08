package cn.mastercom.bigdata.mro.stat;

import java.util.Calendar;
import java.util.Date;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.mro.stat.struct.Stat_BuildPosUser;
import cn.mastercom.bigdata.mro.stat.struct.Stat_Resident;
import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.ResultOutputer;

public class ResidentStat extends AMapStatDo_4G<Stat_Resident>
{

	public ResidentStat(ResultOutputer resultOutputer, int sourceType, int dataType)
	{
		super(resultOutputer, sourceType, dataType);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected Object[] getPartitionKeys(DT_Sample_4G sample)
	{
		long time = sample.itime * 1000L;

		Calendar instance = Calendar.getInstance();
		instance.setTime(new Date(time));
		int hour = instance.get(Calendar.HOUR_OF_DAY);
		return new Object[]
		{ sample.cityID, sample.IMSI, hour, sample.Eci };
	}

	@Override
	protected Stat_Resident createFirstStatItem(DT_Sample_4G sample, Object[] keys)
	{
		Stat_Resident stat_Resident = new Stat_Resident();
		stat_Resident.doFirstSample(sample);
		return stat_Resident;
	}

	@Override
	protected boolean statOrNot(DT_Sample_4G sample)
	{

		return sample.Eci > 0 && sample.IMSI>0;

	}

}
