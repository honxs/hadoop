package com.chinamobile.util;

import java.util.*;

public class CalendarEx implements Comparable<CalendarEx> {

	public int _second = 0;
	public int _usecond = 0;

	public CalendarEx(final int second, final int usecond) {
		_second = second;
		_usecond = usecond;
	}
	
	public CalendarEx() {
		Date dt = new Date();
		_second = (int)(dt.getTime()/1000);
		_usecond = (int)(dt.getTime()%1000) *1000 ;
	}
	
	public CalendarEx AddDays(int nDay)
	{
		return new CalendarEx(_second+24*3600*nDay,_usecond);
	}
	
	public CalendarEx AddHours(int nHour)
	{
		return new CalendarEx(_second+3600*nHour,_usecond);
	}
	
	public CalendarEx AddMinutes(int nMinute)
	{
		return new CalendarEx(_second+60*nMinute,_usecond);
	}
	
	public CalendarEx(final long second, final long usecond) {
		_second = (int)second;
		_usecond = (int)usecond;
	}
	
	public CalendarEx(final Date dt) {
		_second = (int)(dt.getTime()/1000);
		_usecond = (int)(dt.getTime()%1000) *1000 ;
	}

	public int compareTo(final CalendarEx other) {
		return (_second - other._second) * 1000000
				+ (_usecond - other._usecond);
	}

	public int getYear() {
		final Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis((long) _second * 1000);

		return calendar.get(Calendar.YEAR);
	}

	public String timeDiffString(final CalendarEx other) {
		final int value = compareTo(other);
		final int second = value / 1000000;
		final int vsecond = value % 1000000;
		return String.format("%6d.%06d", second, vsecond);
	}

	public String getMonth() {
		final int m = getMonthInt();
		final String[] months = new String[] { "January", "February", "March",
				"April", "May", "June", "July", "August", "September",
				"October", "November", "December" };
		if (m > 12) {
			return "Unknown to Man";
		}

		return months[m - 1];
	}

	public String getDay() {
		final int x = getDayOfWeek();
		final String[] days = new String[] { "Sunday", "Monday", "Tuesday",
				"Wednesday", "Thursday", "Friday", "Saturday" };

		if (x > 7) {
			return "Unknown to Man";
		}

		return days[x - 1];

	}

	public int getMonthInt() {
		final Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis((long) _second * 1000);

		return 1 + calendar.get(Calendar.MONTH);
	}

	public String getDate() {
		return getMonthInt() + "/" + getDayOfMonth() + "/" + getYear();
	}

	public String getTime() {
		return getHour() + ":" + getMinute() + ":" + getSecond();
	}

	public int getDayOfMonth() {
		final Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis((long) _second * 1000);

		return calendar.get(Calendar.DAY_OF_MONTH);
	}

	public int getDayOfYear() {
		final Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis((long) _second * 1000);
		return calendar.get(Calendar.DAY_OF_YEAR);
	}

	public int getWeekOfYear() {
		final Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis((long) _second * 1000);

		return calendar.get(Calendar.WEEK_OF_YEAR);
	}

	public int getWeekOfMonth() {
		final Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis((long) _second * 1000);

		return calendar.get(Calendar.WEEK_OF_MONTH);
	}

	public int getDayOfWeek() {
		final Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis((long) _second * 1000);

		return calendar.get(Calendar.DAY_OF_WEEK);
	}

	public int getHour() {
		final Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis((long) _second * 1000);

		return calendar.get(Calendar.HOUR_OF_DAY);
	}

	public int getMinute() {
		final Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis((long) _second * 1000);

		return calendar.get(Calendar.MINUTE);
	}

	public int getSecond() {
		final Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis((long) _second * 1000);

		return calendar.get(Calendar.SECOND);
	}

	public int getUSecond() {
		return _usecond;
	}

	public String getDateStr8() {
		String str = getYear() 
		+ String.format("%02d", getMonthInt()) 
		+ String.format("%02d", getDayOfMonth());
		return str;
	}
	
	public String getDateStr12() {
		String str = getYear() 
		+ String.format("%02d", getMonthInt()) 
		+ String.format("%02d", getDayOfMonth())
		+ String.format("%02d", getHour())
		+ String.format("%02d", getMinute());
		return str;
	}
	
	public String toString(int flag) {
		
		if (flag == 3)
		{
			String str = getYear() + "年" + String.format("%02d", getMonthInt()) + "月"
			+ String.format("%02d", getDayOfMonth()) + "日"
			+ String.format("%02d", getHour()) + "时"
			+ String.format("%02d", getMinute()) + "分";
			return str;
		}
		
		String str = getYear() + "-" + String.format("%02d", getMonthInt()) + "-"
				+ String.format("%02d", getDayOfMonth()) + " "
				+ String.format("%02d", getHour()) + ":"
				+ String.format("%02d", getMinute()) + ":"
				+ String.format("%02d", getSecond());
				
		if(flag == 1)
		{
			str = str + "."
				+ String.format("%06d", _usecond);
		}

		return str;
	}

	public static void main(final String args[]) {
		
		final CalendarEx db = new CalendarEx(new Date());
		print(db.getDateStr12());
		print(CalendarEx.getCurrentTimeStr());
		print("second: " + db.getSecond());
		print("date: " + db.getDayOfMonth());
		print("year: " + db.getYear());
		print("month: " + db.getMonth());
		print("date: " + db);
		print("Day: " + db.getDay());
		print("DayOfYear: " + db.getDayOfYear());
		print("WeekOfYear: " + db.getWeekOfYear());
		print("era: " + db.getEra());
		print("ampm: " + db.getAMPM());
		print("DST: " + db.getDSTOffset());
		print("ZONE Offset: " + db.getZoneOffset());
		print("TIMEZONE: " + db.toString(0));
	}

	private static void print(final String x) {
		System.out.println(x);
	}

	public int getEra() {
		final Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis((long) _second * 1000);

		return calendar.get(Calendar.ERA);
	}

	public String getUSTimeZone() {
		final String[] zones = new String[] { "Hawaii", "Alaskan", "Pacific",
				"Mountain", "Central", "Eastern" };

		return zones[10 + getZoneOffset()];
	}

	public int getZoneOffset() {
		final Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis((long) _second * 1000);

		return calendar.get(Calendar.ZONE_OFFSET) / (60 * 60 * 1000);
	}

	public int getDSTOffset() {
		final Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis((long) _second * 1000);

		return calendar.get(Calendar.DST_OFFSET) / (60 * 60 * 1000);
	}

	public static String getCurrentTimeStr() {
		CalendarEx cal = new CalendarEx(new Date());		
		return cal.toString(0);
	}
	
	public int getAMPM() {
		final Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis((long) _second * 1000);

		return calendar.get(Calendar.AM_PM);
	}
}
