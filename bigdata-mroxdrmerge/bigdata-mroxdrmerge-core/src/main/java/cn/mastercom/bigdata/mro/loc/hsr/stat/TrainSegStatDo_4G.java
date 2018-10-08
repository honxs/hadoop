package cn.mastercom.bigdata.mro.loc.hsr.stat;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.mro.stat.AMapStatDo_4G;
import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.ResultOutputer;

public class TrainSegStatDo_4G extends AMapStatDo_4G<Stat_TrainSeg>
{
	
	public TrainSegStatDo_4G(ResultOutputer typeResult, int sourceType, int dataType)
	{
		super(typeResult, sourceType, dataType);
		
	}

	@Override
	protected Object[] getPartitionKeys(DT_Sample_4G sample)
	{
		return new Object[]{sample.cityID , sample.trainKey , sample.segmentId};
	}

	@Override
	protected Stat_TrainSeg createFirstStatItem(DT_Sample_4G sample, Object[] keys)
	{
		Stat_TrainSeg trainSegStat = new Stat_TrainSeg();
		trainSegStat.doFirstSample(sample);
		return trainSegStat;
	}

	@Override
	protected boolean statOrNot(DT_Sample_4G sample)
	{
		return sample.Eci > 0 && sample.trainKey > 0 && sample.segmentId > 0;
	}

	
}


