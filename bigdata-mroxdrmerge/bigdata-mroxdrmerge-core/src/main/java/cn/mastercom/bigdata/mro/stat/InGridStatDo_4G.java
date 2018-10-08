package cn.mastercom.bigdata.mro.stat;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.mro.stat.struct.Stat_InGrid;
import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.ResultOutputer;

public class InGridStatDo_4G extends AMapStatDo_4G<Stat_InGrid>
{
	public InGridStatDo_4G(ResultOutputer typeResult, int sourceType, int confidenceType, int dataType)
	{
		super(typeResult, sourceType, confidenceType, dataType);
	}

	@Override
	protected Object[] getPartitionKeys(DT_Sample_4G sample)
	{
		int ifreq = getIfreq(sourceType, sample);
		return new Object[]{sample.cityID , sample.ibuildingID , sample.iheight , sample.grid.tllongitude , sample.grid.tllatitude , ifreq, FormatTime.RoundTimeForHour(sample.itime)};
	}

	@Override
	protected Stat_InGrid createFirstStatItem(DT_Sample_4G sample, Object[] keys)
	{
		Stat_InGrid inGridStat = new Stat_InGrid();
		inGridStat.doFirstSample(sample, (int)keys[5]);
		return inGridStat;
	}

	@Override
	protected boolean statOrNot(DT_Sample_4G sample)
	{
		return sample.Eci > 0 && sample.ibuildingID > 0 && sample.ilongitude > 0 && sample.ilatitude > 0 && sample.grid != null;
	}

}
