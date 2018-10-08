package cn.mastercom.bigdata.mro.stat;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;

public class MrStatUtil
{
	public static boolean rsrpRight(DT_Sample_4G sample, int sourceType)
	{
		int rsrp = 0;
		if (sourceType == StaticConfig.SOURCE_YD)
		{
			rsrp = sample.LteScRSRP;
		}
		else if (sourceType == StaticConfig.SOURCE_LT)
		{
			rsrp = sample.lt_freq[0].LteNcRSRP;
		}
		else if (sourceType == StaticConfig.SOURCE_DX)
		{
			rsrp = sample.dx_freq[0].LteNcRSRP;
		}
		else if (sourceType == StaticConfig.SOURCE_YDLT)
		{
			rsrp = sample.LteScRSRP;
		}
		else if (sourceType == StaticConfig.SOURCE_YDDX)
		{
			rsrp = sample.LteScRSRP;
		}
		return rsrp >= -150 && rsrp <= -30;
	}
}
