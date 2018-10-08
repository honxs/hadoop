package cn.mastercom.bigdata.util.hadoop.schedule;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class JobPlanMaker
{

	private JobPlanLoader jobPlanLoader;
	private ScheduledExecutorService heartbeat;
	private AutoStatTask autoStatTask;
	
    public JobPlanMaker(JobPlanLoader jobPlanLoader)
    {
    	this.jobPlanLoader = jobPlanLoader;
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
			jobPlanLoader.loadPlan();
			
			Map<String, Integer> jobNameMap = jobPlanLoader.getJobNameMap();
			Map<String, JobPlan> jobPlanMap = jobPlanLoader.getJobPlanMap();
			
			for(JobPlan jobPlan : jobPlanMap.values())
			{
				if(jobPlan.isWork)
				{
					Date now = new Date();
					String jobName = ComHelper.GetJobName(jobPlan.jobType, jobPlan.getStatSTime(now), jobPlan.getStatETime(now));
					if(!jobNameMap.containsKey(jobName))
					{
						jobPlanLoader.insertNewJob(jobPlan, now);
					}
				}
			}
			
		}
    	
		
    }
    

	
	
}
