package cn.mastercom.bigdata.mro.stat;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.mro.stat.struct.Stat_OutGrid;
import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.ResultOutputer;

public class OutGridStatDo_4G extends AMapStatDo_4G<Stat_OutGrid>
{
	public OutGridStatDo_4G(ResultOutputer typeResult, int sourceType, int dataType)
	{
		super(typeResult, sourceType, dataType);
	}
	
	public OutGridStatDo_4G(ResultOutputer resultOutputer, int sourceType, int confidenceType, int dataType)
	{
		super(resultOutputer, sourceType, confidenceType, dataType);
	}

	@Override
	protected Object[] getPartitionKeys(DT_Sample_4G sample)
	{
		int ifreq = getIfreq(sourceType, sample);
		return new Object[]{sample.cityID , sample.grid.tllongitude , sample.grid.tllatitude ,ifreq , FormatTime.RoundTimeForHour(sample.itime)};
	}

	@Override
	protected Stat_OutGrid createFirstStatItem(DT_Sample_4G sample, Object[] keys)
	{
		Stat_OutGrid outGrid = new Stat_OutGrid();
		outGrid.doFirstSample(sample, (int)keys[3]);
		return outGrid;
	}

	@Override
	protected boolean statOrNot(DT_Sample_4G sample)
	{
		return sample.Eci > 0 && sample.ilongitude > 0 && sample.ilatitude > 0 && sample.grid != null;
	}

}
