package cn.mastercom.bigdata.mro.stat;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.util.ResultOutputer;

public class MrSampleStatDo_4G extends AListStatDo_4G
{
	public MrSampleStatDo_4G(ResultOutputer typeResult, int sourceType, int dataType)
	{
		super(typeResult, sourceType, dataType);
	}
	public MrSampleStatDo_4G(ResultOutputer typeResult, int sourceType, int confidenceType, int dataType)
	{
		super(typeResult, sourceType, confidenceType, dataType);
	}

	@Override
	protected String sampleToString(DT_Sample_4G sample)
	{
		return sample.createNewSampleToLine();
	}

	@Override
	protected boolean statOrNot(DT_Sample_4G sample)
	{
		return sample.Eci > 0;
	}
}
