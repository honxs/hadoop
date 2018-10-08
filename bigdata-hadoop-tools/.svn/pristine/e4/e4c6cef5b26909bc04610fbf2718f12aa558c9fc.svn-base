package cn.mastercom.bigdata.util.hadoop.schedule;

import java.util.Date;
import java.util.Map;

public class JobInfo
{
    public int id;
    public String jobName;
    public String jobType;
    public Date stime;
    public Date etime;
    public Date creTime;
    public Date statTime;
    public Date finTime;
    public EJobLevel level;//低级 1，正常 2，高级 3，紧急 4
    public EJobStatus status;//任务统计状态，1 任务生成，2 正在运算， 3 运算成功，4  运算异常
    public String jobInput;//[jobType1|stime1|etime1|fintime1],[jobType2|stime2|etime2|fintime2],...
    public String jobResult;//[typename1|value1],[typename2|value2]...
    public String comment; 
   
    private JobInputMng jobInputMng;
    
    public JobInfo()
    {
    	id = -1;
    	jobName = "";
    	jobType = "";
    	stime = null;
    	etime = null;
    	creTime = null;
    	finTime = null;
    	jobInput = "";
    	jobResult = "";
    	level = EJobLevel.None;
    	status = EJobStatus.None;
        comment = "";
    }
    
    public boolean init() throws Exception
    {
    	jobInputMng = new JobInputMng(jobInput);
    	
    	return true;
    }
    
    public boolean checkJobOkToStat(Map<String, JobInfo> curJobMap)
    {
    	if(status != EJobStatus.Prepare)
    	{
    		return false;
    	}
    	
    	if(jobInputMng.checkInputInfoToStat(curJobMap))
    	{
    		return true;
    	}
    	else 
    	{
    		return false;
    	}
    }
    
    public boolean checkJobOkReStat(Map<String, JobInfo> jobListMap)
    {
    	if(status != EJobStatus.Success)
    	{
    		return false;
    	}
    	
    	if(jobInputMng.checkInputInfoReStat(jobListMap))
    	{
    		return true;
    	}
    	else 
    	{
    		return false;
    	}
    }
    
    public JobInputMng getJobInputMng()
	{
		return jobInputMng;
	}
    
}
