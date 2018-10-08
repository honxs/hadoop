package cn.mastercom.bigdata.util.hadoop.schedule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.mastercom.bigdata.util.db.DBRow;
import cn.mastercom.bigdata.util.db.SqlDBHelper;

public class JobInfoLoader
{
	private String dbConn = "";
	private Map<Integer, JobInfo> jobInfoMap;	
	private Map<String, List<JobInfo>> jobListMap;
	

	public JobInfoLoader(String dbConn)
	{
	    this.dbConn = dbConn;
	    jobInfoMap = new HashMap<Integer, JobInfo>();
	    jobListMap = new HashMap<String, List<JobInfo>>();
	}
	
	public Map<Integer, JobInfo> getJobInfoMap()
	{
		return jobInfoMap;
	}
	
	public Map<String, List<JobInfo>> getJobListMap()
	{
		return jobListMap;
	}
	
	public void loadJobInfo()
	{
	    jobInfoMap = new HashMap<Integer, JobInfo>();
	    jobListMap = new HashMap<String, List<JobInfo>>();
	    List<DBRow> rowList = SqlDBHelper.ExcuteQuery(dbConn, "select * from tb_cfg_stat_info ");
		
	    for(DBRow row : rowList)
	    {
	    	JobInfo item = new JobInfo();
	    	item.id = row.getInt("id");
	    	item.jobName = row.getString("jobname");
	    	item.jobType = row.getString("jobtype");
	    	item.stime = row.getDate("stime");
	    	item.etime = row.getDate("etime");
	    	item.creTime = row.getDate("cretime");
	    	item.finTime = row.getDate("fintime");
	    	item.level = EJobLevel.GetEnum(row.getInt("level"));
	    	item.status = EJobStatus.GetEnum(row.getInt("status"));
	    	item.jobInput = row.getString("jobinput");
	    	item.jobResult = row.getString("jobresult");
	    	item.comment = row.getString("comment");    	
	    	jobInfoMap.put(item.id, item);
	    	
	    	
	    	String jobName = ComHelper.GetJobName(item.jobType, item.stime, item.etime);
	    	List<JobInfo> jobList = jobListMap.get(jobName);
	    	if(jobList == null)
	    	{
	    		jobList = new ArrayList<JobInfo>();
	    		jobListMap.put(jobName, jobList);
	    	}
	    	jobList.add(item);
	    	
	    }	 
	}
	
	public void updateJobInfo(JobInfo jobInfo)
	{
		String sql = String.format("update tb_cfg_stat_info set creTime = '%s' and finTime = '%s' and status = %d and jobResult = %s and comment = %s where id = %d",
				     ComHelper.dateFormat.format(jobInfo.creTime),
				     ComHelper.dateFormat.format(jobInfo.finTime),
				     ComHelper.dateFormat.format(jobInfo.status.getValue()),
				     ComHelper.dateFormat.format(jobInfo.jobResult),
				     ComHelper.dateFormat.format(jobInfo.comment),
				     jobInfo.id
				     );
		
		SqlDBHelper.ExcuteNoQuery(dbConn, sql);
	}
	
}
