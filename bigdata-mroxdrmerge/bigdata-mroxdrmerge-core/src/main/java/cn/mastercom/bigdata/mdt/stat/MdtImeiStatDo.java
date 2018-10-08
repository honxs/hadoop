package cn.mastercom.bigdata.mdt.stat;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.mro.stat.AMapStatDo_4G;
import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.ResultOutputer;

public class MdtImeiStatDo extends AMapStatDo_4G<Stat_mdt_imei>
{

	public MdtImeiStatDo(ResultOutputer resultOutPut, int sourceType, int dataType)
	{
		super(resultOutPut, sourceType, dataType);
	}

	@Override
	protected Object[] getPartitionKeys(DT_Sample_4G sample)
	{
		return new Object[] { sample.cityID, sample.imeiTac, FormatTime.RoundTimeForHour(sample.itime) };
	}

	@Override
	protected Stat_mdt_imei createFirstStatItem(DT_Sample_4G sample, Object[] keys)
	{
		Stat_mdt_imei mdt_imeiStat = new Stat_mdt_imei();
		mdt_imeiStat.doFirstSample(sample);
		return mdt_imeiStat;
	}

	@Override
	protected boolean statOrNot(DT_Sample_4G sample)
	{
		return sample.imeiTac > 0;
	}

}
