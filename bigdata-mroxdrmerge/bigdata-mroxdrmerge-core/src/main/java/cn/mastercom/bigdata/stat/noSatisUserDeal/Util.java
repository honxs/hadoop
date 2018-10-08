package cn.mastercom.bigdata.stat.noSatisUserDeal;

public class Util
{
	public static Long getEci(String val)
	{

		String tmp = val.replace("46000", "");
		if (tmp.length() < 5)
			return -1L;
		String tmp1 = tmp.substring(0, 5);
		String tmp2 = tmp.substring(5, 7);
		long enbid = Long.valueOf(tmp1, 16);
		long cellid = Long.valueOf(tmp2, 16);
		return enbid * 256 + cellid;
	}

}
