// Decompiled by Jad v1.5.8g. Copyright 2001 Pavel Kouznetsov.
// Jad home page: http://www.kpdus.com/jad.html
// Decompiler options: packimports(3) 
// Source File Name:   TimeUtil.java

package com.chinamobile.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;


public class TimeUtil
{

    public TimeUtil()
    {
    }


    //2016-02-25 23:29:05
    public static long getMillsFromTimeString(String timeString, String timeFormat)
    {
        SimpleDateFormat sdf = new SimpleDateFormat(timeFormat);
        try
        {
            long millionSeconds = sdf.parse(timeString).getTime();
            return millionSeconds;
        } catch (ParseException e)
        {
            e.printStackTrace();
            return -1L;
        }
    }

    /**
     * yyyy-MM-dd HH:mm:ss
     */
    public static String getCurrentTime(String formatType)
    {
        Date date = new Date();
        return getFormatTime(date, formatType);
    }

    public static String getFormatTime(Date date, String timeFormat)
    {
        DateFormat dateFormat = new SimpleDateFormat(timeFormat);
        return dateFormat.format(date);
    }

    public static String getTimeStringFromMillis(long timeMillis, String timeFormat)
    {
    	Date date = new Date(timeMillis);
    	DateFormat dateFormat = new SimpleDateFormat(timeFormat);
        return dateFormat.format(date);
    }
    
    public static int beforeAfter(int MaxDelayedDays, String referenceDate)
	{
		int beforeOrAfter;
		GregorianCalendar gc = new GregorianCalendar();
		gc.setTime(new Date()); // Get current date
		gc.add(5, (-1) * MaxDelayedDays); // Put the date
											// MaxDelayedDays back
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
		String MaxDaysbeforeToday = formatter.format(gc.getTime());
		beforeOrAfter = Integer.valueOf(MaxDaysbeforeToday)
				- Integer.valueOf(referenceDate);
		return beforeOrAfter;
	}

    //获得当前时间往前推某个小时所对应的日期
    public static String getDateOfHoursBefore(int hoursBefore)
    {
        long oldDayMills = System.currentTimeMillis()-hoursBefore*60*60*1000;
        Date oldDate = new Date(oldDayMills);
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        return df.format(oldDate);
    }
    //求出现在的秒
    public static long getCurrentSecond()
    {
        long totalMilliSeconds = System.currentTimeMillis();
        long totalSeconds = totalMilliSeconds / 1000;
        long currentSecond = totalSeconds % 60;
        return currentSecond;
    }
    //求出现在的分
    public static long getCurrentMinute()
    {
        long totalMilliSeconds = System.currentTimeMillis();
        long totalSeconds = totalMilliSeconds / 1000;
        long totalMinutes = totalSeconds / 60;
        long currentMinute = totalMinutes % 60;
        return currentMinute;
    }
    //求出现在的小时,返回北京时间所以+8
    public static long getCurrentHour()
    {
        long totalMilliSeconds = System.currentTimeMillis();
        long totalSeconds = totalMilliSeconds / 1000;
        long totalMinutes = totalSeconds / 60;
        long totalHour = totalMinutes / 60;
        long currentHour = (totalHour+8) % 24;
        return currentHour;
    }

    //求出现在的小时,返回北京时间所以+8
    public static long getHourFromMills(long milliSeconds)
    {
        long totalSeconds = milliSeconds / 1000;
        long totalMinutes = totalSeconds / 60;
        long totalHour = totalMinutes / 60;
        long currentHour = (totalHour+8) % 24;
        return currentHour;
    }

    //获得当前时间往前推某个小时所对应的日期
    public static String getTimeOfSomeHoursBefore(int hoursBefore, String dformat)
    {
        long oldDayMills = System.currentTimeMillis()-hoursBefore*60*60*1000;
        Date oldDate = new Date(oldDayMills);
        SimpleDateFormat df = new SimpleDateFormat(dformat);
        return df.format(oldDate);
    }

}
