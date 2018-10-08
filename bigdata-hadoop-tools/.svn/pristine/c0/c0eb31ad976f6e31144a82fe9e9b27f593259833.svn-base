package cn.mastercom.bigdata.util.hadoop.schedule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JobMng
{
    private JobInfoLoader jobInfoLoader;
	private Map<Integer, JobInfo> jobInfoMap;
	private Map<String, List<JobInfo>> jobListMap;
	private Map<String, JobInfo> curJobMap;//同样的任务下，只保留之间最近的一个任务

	public JobMng(JobInfoLoader jobInfoLoader)
	{
         this.jobInfoLoader = jobInfoLoader;
	}
	
	public JobInfoLoader getJobInfoLoader()
	{
		return jobInfoLoader;
	}
	
	public boolean load() 
	{
		jobInfoLoader.loadJobInfo();
		
	    jobInfoMap = jobInfoLoader.getJobInfoMap();
	    jobListMap = jobInfoLoader.getJobListMap();
	    curJobMap = new HashMap<String, JobInfo>();
		
		try
		{
		    for(List<JobInfo> jobList : jobListMap.values())
		    {
				Collections.sort(jobList, new Comparator<JobInfo>()
				{
					@Override
					public int compare(JobInfo o1, JobInfo o2)
					{
						return (int)(o2.finTime.getTime() - o1.finTime.getTime());
					}
				});	
		    }
		    
		    for(List<JobInfo> jobList : jobListMap.values())
		    {
		    	for (JobInfo jobInfo : jobList)
				{
		    		jobInfo.init();
				}
		    	
		    	if(jobList.size() > 0)
		    	{
		    		JobInfo curJobInfo = jobList.get(0);
		    		String jobName = ComHelper.GetJobName(curJobInfo.jobType, curJobInfo.stime, curJobInfo.etime);
		    		curJobMap.put(jobName, curJobInfo);
		    	}
		    }
		    
		}
		catch (Exception e)
		{
		   return false;
		}
	
	    return true;
	}
	
	
	public List<JobInfo> getJobsToStat()
	{
		List<JobInfo> jobList = new ArrayList<JobInfo>();
		
		for(JobInfo item : jobInfoMap.values())
		{
		    if(item.checkJobOkToStat(curJobMap))
		    {
		    	jobList.add(item);
		    }
		}
		
		return jobList;
	}	
	
	public List<JobInfo> getJobsReStat()
	{
		List<JobInfo> jobList = new ArrayList<JobInfo>();
		
		for(JobInfo item : jobInfoMap.values())
		{
		    if(item.checkJobOkReStat(curJobMap))
		    {
		    	jobList.add(item);
		    }
		}
		
		return jobList;	
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
}
