package cn.mastercom.bigdata.mro.loc.hsr.stat;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.mro.stat.AMapStatDo_4G;
import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.ResultOutputer;

public class TrainSegCellStatDo_4G extends AMapStatDo_4G<Stat_TrainSegCell>
{
	
	public TrainSegCellStatDo_4G(ResultOutputer typeResult, int sourceType, int dataType)
	{
		super(typeResult, sourceType, dataType);
	}

	@Override
	protected Object[] getPartitionKeys(DT_Sample_4G sample)
	{
		return new Object[]{sample.cityID , sample.trainKey , sample.segmentId , sample.Eci, FormatTime.RoundTimeForHour(sample.itime)};
	}

	@Override
	protected Stat_TrainSegCell createFirstStatItem(DT_Sample_4G sample, Object[] keys)
	{
		Stat_TrainSegCell trainSegCellStat = new Stat_TrainSegCell();
		trainSegCellStat.doFirstSample(sample);
		return trainSegCellStat;
	}

	@Override
	protected boolean statOrNot(DT_Sample_4G sample)
	{
		return sample.Eci > 0 && sample.trainKey > 0 && sample.segmentId > 0;
	}

	
}


