package cn.mastercom.bigdata.util.hadoop.schedule;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import cn.mastercom.bigdata.util.BackGroundWorker;
import cn.mastercom.bigdata.util.IBackGroundWorkerDo;
import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;

public class JobActor
{
	private IJobDo jobDo;
    private BackGroundWorker bgWorker;
    private BGWorkerDo bgWorkerDo;
    private IWriteLogCallBack writeLogCallBack;
    private JobInfoLoader jobInfoLoader;
    private Map<String, Integer> jobTypeMap = new HashMap<String, Integer>();
    
    private JobInfo curJobInfo;
    private boolean isWorking;
	
	public JobActor(IJobDo jobDo, JobInfoLoader jobInfoLoader)
	{
		this.jobDo = jobDo;
		this.jobInfoLoader = jobInfoLoader;
		
		String[] jobTypes = jobDo.getJobType().split(",");
		for(String jobType : jobTypes)
		{
			jobTypeMap.put(jobType, 0);
		}
		this.writeLogCallBack = jobDo.getLoger();
		
		bgWorkerDo = new BGWorkerDo();
		bgWorker = new BackGroundWorker(bgWorkerDo);
		
		isWorking = false;
	}
	
    public boolean IsContainJobType(String testType)
    {
    	return jobTypeMap.containsKey(testType);
    }
    
    public int statJob(JobInfo jobInfo)
    {
    	curJobInfo = jobInfo;
    	
    	Object[] args = new Object[1];
    	
    	bgWorker.start(args);
    	
    	return 0;
    }
    
    public boolean IsWorking()
    {
    	return isWorking;
    }
    
    
	private class BGWorkerDo implements IBackGroundWorkerDo
	{
		
		@Override
		public void onSuccess()
		{
			curJobInfo.status = EJobStatus.Success;
			curJobInfo.finTime = new Date();
			
			jobInfoLoader.updateJobInfo(curJobInfo);
		}

		@Override
		public void onFailure()
		{
			curJobInfo.status = EJobStatus.Fail;
			curJobInfo.finTime = new Date();
			
			jobInfoLoader.updateJobInfo(curJobInfo);
		}

		@Override
		public void onStart()
		{
			isWorking = true;
			
			writeLogCallBack.writeLog(LogType.info, "开始执行Job：" + curJobInfo.jobName);
		
			curJobInfo.status = EJobStatus.Working;
			curJobInfo.statTime = new Date();
		}

		@Override
		public void onEnd()
		{
			isWorking = false;
			
			writeLogCallBack.writeLog(LogType.info, "执行完毕Job：" + curJobInfo.jobName);		
		}

		@Override
		public void writeLog(String log)
		{
			writeLogCallBack.writeLog(LogType.error, log);
		}

		@Override
		public void doSomeThing(Object[] args)
		{
			jobDo.statJob(curJobInfo);
			
		}
	
		
		
	}
	
	
}
