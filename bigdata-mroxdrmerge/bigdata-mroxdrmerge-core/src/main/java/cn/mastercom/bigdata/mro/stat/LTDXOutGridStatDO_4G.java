package cn.mastercom.bigdata.mro.stat;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.Func;

/**
 * Created by Administrator on 2018/4/11.
 */
public class LTDXOutGridStatDO_4G extends OutGridStatDo_4G
{
	private int index = -1;
	
	public LTDXOutGridStatDO_4G(int index, ResultOutputer resultOutputer, int sourceType, int confidenceType, int dataType)
	{
		super(resultOutputer, sourceType, confidenceType, dataType);
		if (index < 0)
		{
			throw new IllegalArgumentException();
		}
		this.index = index;
	}
	
	@Override
	protected Object[] getPartitionKeys(DT_Sample_4G sample)
	{
		int ifreq = getIfreq(sourceType, sample, index);
		return new Object[]{sample.cityID , sample.grid.tllongitude , sample.grid.tllatitude , ifreq , FormatTime.RoundTimeForHour(sample.itime)};
	}


	@Override
	protected boolean statOrNot(DT_Sample_4G sample)
	{
		return super.statOrNot(sample) && getIfreq(sourceType, sample, index) > 0;
	}
	
	private int getIfreq(int sourceType, DT_Sample_4G sample, int index)
	{
		if (sourceType == StaticConfig.SOURCE_DX)
		{
			if (Func.getFreqType(sample.dx_freq[index].LteNcEarfcn) == Func.YYS_DianXin)
			{
				return sample.dx_freq[index].LteNcEarfcn;
			}
		}
		else if (sourceType == StaticConfig.SOURCE_LT)
		{
			if (Func.getFreqType(sample.lt_freq[index].LteNcEarfcn) == Func.YYS_LianTong)
			{
				return sample.lt_freq[index].LteNcEarfcn;
			}
		}
		return 0;
	}
}
