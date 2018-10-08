// Decompiled by Jad v1.5.8g. Copyright 2001 Pavel Kouznetsov.
// Jad home page: http://www.kpdus.com/jad.html
// Decompiler options: packimports(3) 
// Source File Name:   TimeUtil.java

package cn.mastercom.bigdata.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public final class TimeUtil
{

    private TimeUtil()
    {
    }

	public static void main(String[] args)
	{
	/*	long millionSeconds = getMillsFromTimeString("2016-11-23 19:04:06.043000","yyyy-MM-dd HH:mm:ss.SSSSSS");
		System.out.println(millionSeconds);*/

	    Date date = TimeUtil.parse("01_18052419", "01_yyMMddHH");
        System.out.println(date);
        System.out.println(TimeUtil.format(date, "01_yyMMddHH"));
        System.out.println(TimeUtil.format(date, "/hello/nidaye/yyMMddHH/youshuju"));
        System.out.println(TimeUtil.format(date, "this is a path did not need to format"));
        Date date1 = TimeUtil.addHours(date, -2);
        System.out.println(date1);
    }
	
    //2016-02-25 23:29:05
    public static long getMillsFromTimeString(String timeString, String timeFormat)
    {
        SimpleDateFormat sdf = new SimpleDateFormat(timeFormat);
        try
        {
            long millionSeconds = sdf.parse(timeString).getTime();
            return millionSeconds;
        } 
        catch (ParseException e)
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


    public static String getDateOfHoursBefore(int hoursBefore)
    {
        long oldDayMills = System.currentTimeMillis()-hoursBefore*60*60*1000;
        Date oldDate = new Date(oldDayMills);
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        return df.format(oldDate);
    }

    public static long getCurrentSecond()
    {
        long totalMilliSeconds = System.currentTimeMillis();
        long totalSeconds = totalMilliSeconds / 1000;
        long currentSecond = totalSeconds % 60;
        return currentSecond;
    }

    public static long getCurrentMinute()
    {
        long totalMilliSeconds = System.currentTimeMillis();
        long totalSeconds = totalMilliSeconds / 1000;
        long totalMinutes = totalSeconds / 60;
        long currentMinute = totalMinutes % 60;
        return currentMinute;
    }
    
    public static long getCurrentHour()
    {
        long totalMilliSeconds = System.currentTimeMillis();
        long totalSeconds = totalMilliSeconds / 1000;
        long totalMinutes = totalSeconds / 60;
        long totalHour = totalMinutes / 60;
        long currentHour = (totalHour+8) % 24;
        return currentHour;
    }


    public static long getHourFromMills(long milliSeconds)
    {
        long totalSeconds = milliSeconds / 1000;
        long totalMinutes = totalSeconds / 60;
        long totalHour = totalMinutes / 60;
        long currentHour = (totalHour+8) % 24;
        return currentHour;
    }


    public static String getTimeOfSomeHoursBefore(int hoursBefore, String dformat)
    {
        long oldDayMills = System.currentTimeMillis()-hoursBefore*60*60*1000;
        Date oldDate = new Date(oldDayMills);
        SimpleDateFormat df = new SimpleDateFormat(dformat);
        return df.format(oldDate);
    }

    /**
     * 在输入日期基础上增加多少个小时
     * @param date 日期
     * @param hours 小时数，当为负数时即减操作
     * @return 结果
     */
    public static Date addHours(Date date, int hours){

        calendar.setTime(date);

        calendar.add(Calendar.HOUR, hours);

        return calendar.getTime();
    }

    /**
     * 在输入日期基础上增加多少天
     * @param date 日期
     * @param days 小时数，当为负数时即减操作
     * @return 结果
     */
    public static Date addDays(Date date, int days){

        calendar.setTime(date);

        calendar.add(Calendar.DAY_OF_MONTH, days);

        return calendar.getTime();
    }

    /**
     * 尽最大努力 按要求格式化
     * @param dateStr
     * @param dateFormat
     * @return
     */
    public static Date parse(String dateStr, String dateFormat){
        if(dateFormat == null || dateFormat.length() == 0){
            return null;
        }
        try{
            //去掉一些影响匹配的字符
            df.applyPattern(dateFormat.replaceAll("[^ ymdhsYMDHS/\\-:\\.]+","_"));
            return df.parse(dateStr.replaceAll("[^ 0-9/\\-:\\.]+","_"));
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 尽最大努力 按要求格式化
     * @param date
     * @param dateFormat
     * @return
     */
    public static String format(Date date, String dateFormat){
        if(dateFormat == null || dateFormat.length() == 0){
            return null;
        }

        try{
            //去掉一些影响匹配的字符
//            df.applyPattern(dateFormat.replaceAll("[^ ymdhsYMDHS/\\-:\\.]+","_"));
            df.applyPattern(dateFormat);
            return df.format(date);
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 单例日历，仅供计算用
     * PS. 线程不安全！线程不安全！线程不安全！
     */
    private static final Calendar calendar = Calendar.getInstance();

    /**
     * 单例日期格式化，仅供计算用
     */
    private static final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
}
