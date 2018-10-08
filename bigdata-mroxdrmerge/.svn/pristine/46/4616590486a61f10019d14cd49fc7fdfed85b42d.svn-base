package cn.mastercom.bigdata.mro.stat;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.conf.config.SpecialUserConfig;
import cn.mastercom.bigdata.util.ResultOutputer;

public class SpecUserMrSampleStatDo_4G extends MrSampleStatDo_4G
{

	public SpecUserMrSampleStatDo_4G(ResultOutputer typeResult, int sourceType, int dataType)
	{
		super(typeResult, sourceType, dataType);
	}

	@Override
	protected boolean statOrNot(DT_Sample_4G sample)
	{
		return SpecialUserConfig.GetInstance().ifSpeciUser(sample.IMSI, false);
	}
	
	

}
