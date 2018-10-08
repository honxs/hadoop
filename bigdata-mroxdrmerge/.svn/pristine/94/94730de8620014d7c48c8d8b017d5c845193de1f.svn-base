package cn.mastercom.bigdata.mro.stat;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.mro.stat.struct.Stat_BuildCell;
import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.ResultOutputer;

public class BuildCellStatDo_4G extends AMapStatDo_4G<Stat_BuildCell>
{
	public BuildCellStatDo_4G(ResultOutputer typeResult, int sourceType, int confidenceType, int dataType)
	{
		super(typeResult, sourceType, confidenceType, dataType);
	}

	@Override
	protected Stat_BuildCell createFirstStatItem(DT_Sample_4G sample, Object[] keys)
	{
		Stat_BuildCell statBuild = new Stat_BuildCell();
		statBuild.doFirstSample(sample, (int) keys[4]);
		return statBuild;
	}

	@Override
	protected Object[] getPartitionKeys(DT_Sample_4G sample)
	{
		int ifreq = getIfreq(sourceType, sample);
		return new Object[] { sample.cityID, sample.ibuildingID, sample.iheight, sample.Eci, ifreq, FormatTime.RoundTimeForHour(sample.itime) };
	}

	@Override
	protected boolean statOrNot(DT_Sample_4G sample)
	{
		return sample.Eci > 0 && sample.ibuildingID > 0;
	}

}
