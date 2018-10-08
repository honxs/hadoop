package cn.mastercom.bigdata.util.hadoop.schedule;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.mastercom.bigdata.util.db.DBRow;
import cn.mastercom.bigdata.util.db.SqlDBHelper;

public class JobPlanLoader
{
	private String dbConn = "";
	private Map<String, JobPlan> jobPlanMap;
	private Map<String, Integer> jobNameMap;

	public JobPlanLoader(String dbConn)
	{
		this.dbConn = dbConn;
		jobPlanMap = new HashMap<String, JobPlan>();
		jobNameMap = new HashMap<String, Integer>();
	}

	public Map<String, JobPlan> getJobPlanMap()
	{
		return jobPlanMap;
	}
	
	public Map<String, Integer> getJobNameMap()
	{
		return jobNameMap;
	}

	public void loadPlan()
	{
		{
			jobPlanMap = new HashMap<String, JobPlan>();
			List<DBRow> rowList = SqlDBHelper.ExcuteQuery(dbConn, "select * from tb_cfg_stat_plan ");

			for (DBRow row : rowList)
			{
				JobPlan item = new JobPlan();
				item.planName = row.getString("planName");
				item.jobType = row.getString("jobType");
				item.jobPeriodType = EJobPeriodType.GetEnum(row.getInt("jobPeriodType"));
				item.planStartTime = row.getDate("planStartTime");
				item.planEndTime = row.getDate("planEndTime");
				item.level = EJobLevel.GetEnum(row.getInt("level"));
				item.jobInput = row.getString("jobinput");
				item.isWork = row.getInt("isWork") > 0 ? true : false;

				jobPlanMap.put(item.planName, item);
			}
		}

		{
			jobNameMap = new HashMap<String, Integer>();
			List<DBRow> rowList = SqlDBHelper.ExcuteQuery(dbConn, "select * from tb_cfg_stat_job ");

			for (DBRow row : rowList)
			{
				String jobType = row.getString("jobType");
				Date stime = row.getDate("stime");
				Date etime = row.getDate("etime");
				
				String jobName = ComHelper.GetJobName(jobType, stime, etime);
				jobNameMap.put(jobName, 0);
			}
		}

	}
	
    public void insertNewJob(JobPlan jobPlan, Date dateTime)
    {
    	Date stime = jobPlan.getStatSTime(dateTime);
    	Date etime = jobPlan.getStatETime(dateTime);
    	String jobName = ComHelper.GetJobName(jobPlan.jobType, jobPlan.getStatSTime(dateTime), jobPlan.getStatETime(dateTime));
    	
    	String sql = "insert into tb_cfg_stat_job values("
    			     + jobName + ","
    			     + jobPlan.jobType + ","
    			     + ComHelper.dateFormat.format(stime) + ","
    			     + ComHelper.dateFormat.format(etime) + ","
    			     + "null" + ","
    			     + "null" + ","
    			     + "null" + ","
    			     + "'" + jobPlan.level.toString() + "',"
    			     + "'" + EJobStatus.Prepare.getName() + "',"
    			     + "'" + jobPlan.getCurJobInput(dateTime) + "'," 
    			     + "'',"
    			     + "''";
    	
    	SqlDBHelper.ExcuteNoQuery(dbConn, sql);
    }

}
