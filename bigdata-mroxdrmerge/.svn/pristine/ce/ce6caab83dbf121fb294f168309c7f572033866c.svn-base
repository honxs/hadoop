package cn.mastercom.bigdata.mro.loc.hsr.stat;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.mro.stat.MrSampleStatDo_4G;
import cn.mastercom.bigdata.util.ResultOutputer;

public class HsrMrSampleStatDo_4G extends MrSampleStatDo_4G
{
	StringBuilder sb = new StringBuilder();

	public HsrMrSampleStatDo_4G(ResultOutputer typeResult, int sourceType, int dataType)
	{
		super(typeResult, sourceType, dataType);
	}

	@Override
	protected String sampleToString(DT_Sample_4G sample)
	{
		sb.delete(0, sb.length());
		return sb.append(sample.createNewSampleToLine())
		.append("\t").append(sample.trainKey)
		.append("\t").append(sample.sectionId)
		.append("\t").append(sample.segmentId)
		.toString();
	}
	
	

}
