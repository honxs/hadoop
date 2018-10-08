package cn.mastercom.bigdata.mro.stat;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.mro.stat.struct.Stat_Build;
import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.ResultOutputer;

public class BuildStatDo_4G extends AMapStatDo_4G<Stat_Build>
{
	public BuildStatDo_4G(ResultOutputer typeResult, int sourceType, int confidenceType, int dataType)
	{
		super(typeResult, sourceType, confidenceType, dataType);
	}

	@Override
	protected Object[] getPartitionKeys(DT_Sample_4G sample)
	{
		int ifreq = getIfreq(sourceType, sample);
		return new Object[]{sample.cityID , sample.ibuildingID , sample.iheight , ifreq , FormatTime.RoundTimeForHour(sample.itime)};
	}

	@Override
	protected Stat_Build createFirstStatItem(DT_Sample_4G sample, Object[] keys)
	{
		Stat_Build buildStat = new Stat_Build();
		buildStat.doFirstSample(sample, (int)keys[3]);
		return buildStat;
	}

	@Override
	protected boolean statOrNot(DT_Sample_4G sample)
	{ 	
		return sample.ibuildingID > 0;
	}
}
