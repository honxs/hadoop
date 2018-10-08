package cn.mastercom.bigdata.mro.stat;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.mro.stat.struct.Stat_Out_CellGrid;
import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.ResultOutputer;

public class OutGridCellStatDo_4G extends AMapStatDo_4G<Stat_Out_CellGrid>
{
	public OutGridCellStatDo_4G(ResultOutputer typeResult, int sourceType, int dataType)
	{
		super(typeResult, sourceType, dataType);
	}
	
	public OutGridCellStatDo_4G(ResultOutputer typeResult, int sourceType, int confidenceType, int dataType)
	{
		super(typeResult, sourceType, confidenceType, dataType);
	}

	@Override
	protected Object[] getPartitionKeys(DT_Sample_4G sample)
	{
		return new Object[]{sample.cityID , sample.Eci , sample.grid.tllongitude , sample.grid.tllatitude , FormatTime.RoundTimeForHour(sample.itime)};
	}

	@Override
	protected Stat_Out_CellGrid createFirstStatItem(DT_Sample_4G sample, Object[] keys)
	{
		Stat_Out_CellGrid outCellGridStat = new Stat_Out_CellGrid();
		outCellGridStat.doFirstSample(sample);
		return outCellGridStat;
	}

	@Override
	protected boolean statOrNot(DT_Sample_4G sample)
	{
		return sample.Eci > 0 && sample.ilongitude > 0 && sample.ilatitude > 0 && sample.grid != null;
	}

}
