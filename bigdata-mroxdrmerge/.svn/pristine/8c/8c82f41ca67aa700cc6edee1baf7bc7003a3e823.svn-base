package cn.mastercom.bigdata.mro.stat;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mro.stat.struct.Scene_Cell;
import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.ResultOutputer;

public class AreaCellStatDo_4G extends AMapStatDo_4G<Scene_Cell>
{
	public AreaCellStatDo_4G(ResultOutputer typeResult, int sourceType, int dataType)
	{
		super(typeResult, sourceType, dataType);
	}

	@Override
	protected Scene_Cell createFirstStatItem(DT_Sample_4G sample, Object[] keys)
	{
		Scene_Cell cellSceneStat = new Scene_Cell();
		cellSceneStat.doFirstSample(sample);
		return cellSceneStat;
	}

	@Override
	protected Object[] getPartitionKeys(DT_Sample_4G sample)
	{
		return new Object[]{sample.cityID , sample.iAreaType , sample.iAreaID , sample.Eci , FormatTime.RoundTimeForHour(sample.itime)};
	}

	@Override
	protected boolean statOrNot(DT_Sample_4G sample)
	{
		return sample.Eci > 0 && sample.iAreaType > 0 && sample.iAreaID > 0 && sample.testType == StaticConfig.TestType_HiRail;
	}

}
