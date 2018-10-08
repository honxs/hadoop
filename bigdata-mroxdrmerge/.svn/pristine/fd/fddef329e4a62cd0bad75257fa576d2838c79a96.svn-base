package cn.mastercom.bigdata.mro.stat;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.mro.stat.struct.Stat_Cell;
import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.ResultOutputer;

/**
 * Created by Administrator on 2017/5/8.
 */
public class CellStatDO_4G extends AMapStatDo_4G<Stat_Cell>
{
	public CellStatDO_4G(ResultOutputer typeResult, int sourceType, int dataType)
	{
		super(typeResult, sourceType, dataType);
	}

	@Override
	protected Object[] getPartitionKeys(DT_Sample_4G sample)
	{
		int ifreq = getIfreq(sourceType, sample);
		return new Object[]{sample.cityID , sample.Eci , ifreq , FormatTime.RoundTimeForHour(sample.itime)};
	}

	@Override
	protected Stat_Cell createFirstStatItem(DT_Sample_4G sample, Object[] keys)
	{
		Stat_Cell cellStat = new Stat_Cell();
		cellStat.doFirstSample(sample, (int)keys[2]);
		return cellStat;
	}

	@Override
	protected boolean statOrNot(DT_Sample_4G sample)
	{
		return sample.Eci > 0;
	}
}
