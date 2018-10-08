package cn.mastercom.bigdata.mro.stat;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mro.stat.struct.Scene_Grid;
import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.ResultOutputer;

public class AreaGridStatDo_4G extends AMapStatDo_4G<Scene_Grid>
{
	public AreaGridStatDo_4G(ResultOutputer typeResult, int sourceType, int dataType)
	{
		super(typeResult, sourceType, dataType);
	}

	@Override
	protected Scene_Grid createFirstStatItem(DT_Sample_4G sample, Object[] keys)
	{
		Scene_Grid sceneGrid = new Scene_Grid();
		sceneGrid.doFirstSample(sample);
		return sceneGrid;
	}

	@Override
	protected Object[] getPartitionKeys(DT_Sample_4G sample)
	{
		return new Object[]{sample.cityID , sample.iAreaType , sample.iAreaID , sample.grid.tllongitude , sample.grid.tllatitude , FormatTime.RoundTimeForHour(sample.itime)};
	}

	@Override
	protected boolean statOrNot(DT_Sample_4G sample)
	{
		return sample.testType == StaticConfig.TestType_HiRail && sample.Eci > 0 && sample.iAreaType > 0 && sample.iAreaID > 0 && sample.ilongitude > 0 && sample.ilatitude > 0 && sample.grid != null;
	}

}
