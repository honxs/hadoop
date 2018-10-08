package cn.mastercom.bigdata.mro.stat;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.util.ResultOutputer;

public class MrAreaSampleStatDo_4G extends AListStatDo_4G
{

	public MrAreaSampleStatDo_4G(ResultOutputer typeResult, int sourceType, int dataType)
	{
		super(typeResult, sourceType, dataType);
	}

	
	@Override
	protected String sampleToString(DT_Sample_4G sample)
	{
		return sample.createAreaSampleToLine();
	}

	@Override
	protected boolean statOrNot(DT_Sample_4G sample)
	{
		return sample.Eci > 0 && sample.iAreaType > 0;
	}

	

}
