package cn.mastercom.bigdata.mro.stat;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.mro.stat.struct.Stat_Imei;
import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.ResultOutputer;

public class ImeiStatDo_4G extends AMapStatDo_4G<Stat_Imei>
{
	public ImeiStatDo_4G(ResultOutputer typeResult, int sourceType, int dataType)
	{
		super(typeResult, sourceType, dataType);
	}

	@Override
	protected Object[] getPartitionKeys(DT_Sample_4G sample)
	{
		int ifreq = getIfreq(sourceType, sample);
		return new Object[] { sample.cityID, sample.imeiTac, ifreq, FormatTime.RoundTimeForHour(sample.itime) };
	}

	@Override
	protected Stat_Imei createFirstStatItem(DT_Sample_4G sample, Object[] keys)
	{
		Stat_Imei statImei = new Stat_Imei();
		statImei.doFirstSample(sample, (int) keys[2]);
		return statImei;
	}

	@Override
	protected boolean statOrNot(DT_Sample_4G sample)
	{
		return sample.imeiTac > 0;
	}

}
