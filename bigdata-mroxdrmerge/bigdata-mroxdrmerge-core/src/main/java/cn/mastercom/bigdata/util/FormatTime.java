package cn.mastercom.bigdata.util;

import java.text.SimpleDateFormat;

public class FormatTime
{
	private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
	public static int RoundTimeForHour(int itime)
	{
		return itime / 3600 * 3600;
	}
	//5分钟粒度
	public static int RoundTimeForFiveMins(int itime)
	{
		return itime / 300 * 300;
	}

	public static int getRoundDayTime(int time)
	{
		return (time + 8 * 3600) / 86400 * 86400 - 8 * 3600;
	}

	public static int getHour(int time)
	{
		return (time + 8 * 3600) % 86400 / 3600;// 需要加上8小时，否则会比实际时间少8小时
	}

	public static void main(String args[])
	{
		System.out.println(getDay(1531534521));
	}

	public static int mdtType(String mrtype)
	{
		return mrtype.equals("MDT_IMM") ? 0 : 1;
	}
	
	// 返回天日期如20180714
	public static String getDay(int time)
	{
		return dateFormat.format(getRoundDayTime(time) * 1000L);
	}

}
