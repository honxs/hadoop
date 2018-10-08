package cn.mastercom.bigdata.mapr.mro.loc;

import cn.mastercom.bigdata.util.DataGeter;

public class Util
{
	public static long getEci(String tmStr)
	{
		long eci;
		if (tmStr.indexOf(":") > 0)
		{
			eci = DataGeter.GetInt(tmStr.substring(0, tmStr.indexOf(":")));
		}
		else if (tmStr.indexOf("-") > 0)
		{
			eci = DataGeter.GetInt(tmStr.substring(tmStr.indexOf("-") + 1));
		}
		else
		{
			eci = DataGeter.GetInt(tmStr);
		}
		return eci;
	}

}
