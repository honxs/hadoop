package cn.mastercom.bigdata.mro.loc.hsr.stat;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.mro.stat.AMapStatDo_4G;
import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.ResultOutputer;

public class HsrCellStatDo_4G extends AMapStatDo_4G<Stat_HsrCell>
{

	public HsrCellStatDo_4G(ResultOutputer typeResult, int sourceType, int dataType)
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
	protected Stat_HsrCell createFirstStatItem(DT_Sample_4G sample, Object[] keys)
	{
		Stat_HsrCell cellStat = new Stat_HsrCell();
		cellStat.doFirstSample(sample, (int)keys[2]);
		return cellStat;
	}

	@Override
	protected boolean statOrNot(DT_Sample_4G sample)
	{
		return sample.Eci > 0;
	}

}
