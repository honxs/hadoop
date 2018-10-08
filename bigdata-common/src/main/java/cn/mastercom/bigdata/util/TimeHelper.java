package cn.mastercom.bigdata.util;

public class TimeHelper
{
	
     public static int getRoundDayTime(int time)
     {
    	return (time + 8 * 3600) / 86400 * 86400 - 8 * 3600;
     }
	
	
	
}
