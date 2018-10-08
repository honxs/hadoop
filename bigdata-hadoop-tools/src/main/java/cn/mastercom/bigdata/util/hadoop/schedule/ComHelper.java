package cn.mastercom.bigdata.util.hadoop.schedule;

import java.text.SimpleDateFormat;
import java.util.Date;

public class ComHelper
{
	public static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static String GetJobName(String jobType, Date stime, Date etime)
	{
		String strstime = dateFormat.format(stime);
		String stretime = dateFormat.format(etime);
		return jobType + "-" + strstime + "-" + stretime;
	}

}
