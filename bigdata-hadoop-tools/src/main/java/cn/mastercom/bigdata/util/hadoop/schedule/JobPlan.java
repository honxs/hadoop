package cn.mastercom.bigdata.util.hadoop.schedule;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class JobPlan
{
	public int id;
	public String planName;
	public String jobType;
	public EJobPeriodType jobPeriodType;
    public Date planStartTime;
    public Date planEndTime;
    public EJobLevel level;//低级 1，正常 2，高级 3，紧急 4
    //[jobType1|stime1|etime1|fintime1],[jobType2|stime2|etime2|fintime2],...
    //stime1,etime1记录的是相对时间偏移，
    public String jobInput;
    public boolean isWork;
    
    public JobPlan()
    {
    	id = -1;
    	planName = "";
    	jobType = "";
    	jobPeriodType = EJobPeriodType.None;
    	planStartTime = null;
    	planEndTime = null;
    	level = EJobLevel.Normal;
    	jobInput = "";
    	isWork = false;
    }
    
    public Date getStatSTime(Date curTime)
    {
    	Calendar calendar = new GregorianCalendar(); 
    	calendar.setTime(curTime); 
    	
    	if(jobPeriodType == EJobPeriodType.Minute)
    	{
    		calendar.set(Calendar.MILLISECOND, 0);	
    		calendar.set(Calendar.SECOND, 0);	
    		return calendar.getTime();
    	}
    	else if(jobPeriodType == EJobPeriodType.Hour)
    	{
    		calendar.set(Calendar.MILLISECOND, 0);
    		calendar.set(Calendar.SECOND, 0);
    		calendar.set(Calendar.MINUTE, 0);
    		return calendar.getTime();
    	}
    	else if(jobPeriodType == EJobPeriodType.Day)
    	{
    		calendar.set(Calendar.MILLISECOND, 0);
    		calendar.set(Calendar.SECOND, 0);
    		calendar.set(Calendar.MINUTE, 0);
    		calendar.set(Calendar.HOUR, 0);
    		return calendar.getTime();
    	}
    	else if(jobPeriodType == EJobPeriodType.Month)
    	{
    		calendar.set(Calendar.MILLISECOND, 0);
    		calendar.set(Calendar.SECOND, 0);
    		calendar.set(Calendar.MINUTE, 0);
    		calendar.set(Calendar.HOUR, 0);
    		calendar.set(Calendar.DATE, 1);
    		return calendar.getTime();
    	}
    	return null;
    }
    
    public Date getStatETime(Date curTime)
    {
    	Date sTime = getStatSTime(curTime);
    	Calendar calendar = new GregorianCalendar(); 
    	calendar.setTime(sTime); 
    	
    	if(jobPeriodType == EJobPeriodType.Minute)
    	{
    		calendar.add(Calendar.MINUTE, 1);
    		return calendar.getTime();
    	}
    	else if(jobPeriodType == EJobPeriodType.Hour)
    	{
    		calendar.add(Calendar.HOUR, 1);
    		return calendar.getTime();
    	}
    	else if(jobPeriodType == EJobPeriodType.Day)
    	{
    		calendar.add(Calendar.DATE, 1);
    		return calendar.getTime();
    	}
    	else if(jobPeriodType == EJobPeriodType.Month)
    	{
    		calendar.add(Calendar.MONTH, 1);
    		return calendar.getTime();
    	}
    	return null;
    }
    
    public String getCurJobInput(Date curTime)
    {
    	String resultStr = "";
    	
    	Date stime = getStatSTime(curTime);
    	Date etime = getStatSTime(curTime);
    	
		if (jobInput.length() > 0)
		{
			String tmpStr = new String(jobInput);
			tmpStr = tmpStr.substring(0, 0).equals("[") ? tmpStr.substring(1) : tmpStr;
			tmpStr = tmpStr.substring(tmpStr.length() - 1, tmpStr.length() - 1).equals("]")
					? tmpStr.substring(0, tmpStr.length() - 2) : tmpStr;

			String[] strSlipt = tmpStr.split("],[");
			
			resultStr = "[";
			for (String str : strSlipt)
			{
				String[] vals = str.split("|");
				if(vals.length == 4)
				{
					resultStr += vals[0] + "|" 
				                + (int)(stime.getTime()/1000L) + Integer.parseInt(vals[1]) + "|"
				                + (int)(etime.getTime()/1000L) + Integer.parseInt(vals[2]) + "|"
				                + Integer.parseInt(vals[3]) + "],[";
				}		
			}
			
			if(resultStr.length() > 0)
			{
			 	resultStr = resultStr.substring(0, resultStr.length()-"],[".length());
			}
			
			resultStr += "]";
			
		}
		
		return resultStr;
    }
    
    
}
