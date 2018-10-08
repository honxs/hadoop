package cn.mastercom.bigdata.mro.stat;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mro.stat.struct.Stat_Scene;
import cn.mastercom.bigdata.util.ResultOutputer;

public class AreaStatDo_4G extends AMapStatDo_4G<Stat_Scene>
{
	public AreaStatDo_4G(ResultOutputer typeResult, int sourceType, int dataType)
	{
		super(typeResult, sourceType, dataType);
	}
	
	@Override
	protected Stat_Scene createFirstStatItem(DT_Sample_4G sample, Object[] keys)
	{
		Stat_Scene sceneStat = new Stat_Scene();
		sceneStat.doFirstSample(sample);
		return sceneStat;
	}

	@Override
	protected Object[] getPartitionKeys(DT_Sample_4G sample)
	{
		return new Object[]{};
	}

	@Override
	protected boolean statOrNot(DT_Sample_4G sample)
	{
		return sample.iAreaType > 0 && sample.iAreaID > 0 && sample.testType == StaticConfig.TestType_HiRail;
	}

}
