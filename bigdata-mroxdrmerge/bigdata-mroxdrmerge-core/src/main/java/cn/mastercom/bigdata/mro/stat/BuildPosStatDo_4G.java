package cn.mastercom.bigdata.mro.stat;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.mro.stat.struct.Stat_BuildPos;
import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.ResultOutputer;

public class BuildPosStatDo_4G extends AMapStatDo_4G<Stat_BuildPos>
{
	public BuildPosStatDo_4G(ResultOutputer typeResult, int sourceType, int confidenceType, int dataType)
	{
		super(typeResult, sourceType, confidenceType, dataType);
	}

	@Override
	protected Object[] getPartitionKeys(DT_Sample_4G sample)
	{
		int ifreq = getIfreq(sourceType, sample);
		return new Object[]{sample.cityID , sample.ibuildingID , sample.iheight , sample.position , ifreq , FormatTime.RoundTimeForHour(sample.itime)};
	}

	@Override
	protected Stat_BuildPos createFirstStatItem(DT_Sample_4G sample, Object[] keys)
	{
		Stat_BuildPos buildPosStat = new Stat_BuildPos();
		buildPosStat.doFirstSample(sample, (int)keys[4]);
		return buildPosStat;
	}

	@Override
	protected boolean statOrNot(DT_Sample_4G sample)
	{
		return sample.ibuildingID > 0;
	}
}
