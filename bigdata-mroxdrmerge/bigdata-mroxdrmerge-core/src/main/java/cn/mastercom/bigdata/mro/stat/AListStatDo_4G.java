package cn.mastercom.bigdata.mro.stat;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.util.ResultOutputer;

public abstract class AListStatDo_4G implements IStatDo<DT_Sample_4G>
{
//	protected ArrayList<String> sampleList;
	
	protected int sourceType;
	
	protected int confidenceType;
	
	protected int dataType;
	
	protected ResultOutputer typeResult;

	public AListStatDo_4G(ResultOutputer typeResult, int sourceType, int dataType)
	{
		this(typeResult, sourceType, StaticConfig.Natural_Abnormal, dataType);
	}
	
	public AListStatDo_4G(ResultOutputer typeResult, int sourceType, int confidenceType, int dataType)
	{
		this.typeResult = typeResult;
		this.sourceType = sourceType;
		this.confidenceType = confidenceType;
		this.dataType = dataType;
//		sampleList = new ArrayList<String>();
	}

	@Override
	public int stat(DT_Sample_4G sample)
	{
		if (statOrNot(sample) && (sample.ConfidenceType == confidenceType || confidenceType == StaticConfig.Natural_Abnormal))
		{
//			sampleList.add(sampleToString(sample));
			try {
				typeResult.pushData(dataType, sampleToString(sample));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return 0;
	}

	@Override
	public int outDealingResult()
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int outFinalReuslt()
	{
//		for (String item : sampleList)
//		{
//			try {
//				typeResult.pushData(dataType, item);
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
//		sampleList.clear();

		return 0;
	}

	protected abstract String sampleToString(DT_Sample_4G sample);
	
	protected abstract boolean statOrNot(DT_Sample_4G sample);
}
