package cn.mastercom.bigdata.util.hadoop.schedule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class JobLauncher
{
	private JobMng jobMng;
	private ScheduledExecutorService heartbeat;
	private AutoStatTask autoStatTask;
	private List<JobActor> jobActorList;
	
    public JobLauncher(JobMng jobMng)
    {
    	this.jobMng = jobMng;
    	
    	jobActorList = new ArrayList<JobActor>();
    }
    
    public void addActor(IJobDo item)
    {
    	JobActor actor = new JobActor(item, jobMng.getJobInfoLoader());
    	jobActorList.add(actor);
    }
    
    public void startWork()
    {	
    	heartbeat = Executors.newScheduledThreadPool(1);  
    	heartbeat.scheduleWithFixedDelay(autoStatTask,60,10*60,TimeUnit.SECONDS);  
    }
	   
    private class AutoStatTask extends java.util.TimerTask
    {
		@Override
		public void run()
		{
			//每次分配完任务以后，都需要重新加载
			jobMng.load();
			
			List<JobInfo> jobList = new ArrayList<JobInfo>();
			jobList.addAll(jobMng.getJobsToStat());
			jobList.addAll(jobMng.getJobsReStat());
			
			//按照紧急程度和创建时间排序
			Collections.sort(jobList, new Comparator<JobInfo>()
			{
				@Override
				public int compare(JobInfo o1, JobInfo o2)
				{
					if(o1.level.getValue() > o2.level.getValue())
					{
						return -1;
					}
					else if(o1.level.getValue() < o2.level.getValue())
					{
						return 1;
					}
					else 
					{
						if(o1.creTime.getTime() > o2.creTime.getTime())
						{
							return -1;
						}
						else if(o1.creTime.getTime() < o2.creTime.getTime())
						{
							return 1;
						} 
						else 
						{
							return 0;
						}
					}
				}
			});
			
			for(JobInfo jobInfo : jobList)
			{
				for(JobActor actor : jobActorList)
				{
					if(!actor.IsWorking() && actor.IsContainJobType(jobInfo.jobType))
					{
						actor.statJob(jobInfo);
					}
				}
			}
			
		}
    	
    }
	
}
