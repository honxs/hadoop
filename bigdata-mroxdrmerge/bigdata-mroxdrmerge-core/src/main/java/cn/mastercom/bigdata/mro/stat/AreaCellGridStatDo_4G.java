package cn.mastercom.bigdata.mro.stat;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mro.stat.struct.Scene_CellGrid;
import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.ResultOutputer;

public class AreaCellGridStatDo_4G extends AMapStatDo_4G<Scene_CellGrid>
{
	public AreaCellGridStatDo_4G(ResultOutputer typeResult, int sourceType, int dataType)
	{
		super(typeResult, sourceType, dataType);
	}
	
	@Override
	protected Scene_CellGrid createFirstStatItem(DT_Sample_4G sample, Object[] keys)
	{
		Scene_CellGrid cellGridStat = new Scene_CellGrid();
		cellGridStat.doFirstSample(sample);
		return cellGridStat;
	}

	@Override
	protected Object[] getPartitionKeys(DT_Sample_4G sample)
	{
		return new Object[]{sample.cityID , sample.iAreaType , sample.iAreaID , sample.Eci , sample.grid.tllongitude , sample.grid.tllatitude , FormatTime.RoundTimeForHour(sample.itime)};
	}

	@Override
	protected boolean statOrNot(DT_Sample_4G sample)
	{
		return sample.testType == StaticConfig.TestType_HiRail && sample.Eci > 0 && sample.iAreaType > 0 && sample.iAreaID > 0 && sample.ilongitude > 0 && sample.ilatitude > 0 && sample.grid != null;
	}

}
