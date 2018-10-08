package cn.mastercom.bigdata.sqlhp;

import java.sql.ResultSet;

import cn.mastercom.bigdata.util.CalendarEx;
import org.apache.log4j.Logger;

public class JobHelper
{
    static Logger log = Logger.getLogger(JobHelper.class.getName());
	   public static void  SetJobStatus(DBHelper help, CalendarEx sdate, String jobType, JobStatus status)
       {
           String sql = "exec PROC_JOB_SETSTATUS '" + sdate.getDateStr8()  + "','" 
               + jobType + "','"
               + status.FinishTime + "','"
               + status.Result + "','"
               + status.Info + "'";
           log.info("SetJobStatus sql :" + sql);
           help.UpdateData(sql, null);
       }

	   public static JobStatus GetJobStatus(DBHelper help, CalendarEx sdate, String jobType )
       {
           	String sSQL = "exec PROC_JOB_GETSTATUS '" + sdate.getDateStr8() + "','" + jobType + "'";
            log.info("GetJobStatus sql :" + sSQL);
           	ResultSet rs = help.GetResultSet(sSQL, null);
			try
			{
				while (rs.next())
				{
                   JobStatus stru = new JobStatus();
            	   stru.FinishTime = (rs.getDate(1) == null)?"":(new CalendarEx(rs.getDate(1))).getDateStr8();
            	   stru.Result = rs.getString(2);
            	   stru.Info = rs.getString(3);
                   return stru;
				}
			}
			catch (Exception ex)
			{
                log.error(ex.getMessage());
			}
			finally
			{
				try
				{
					rs.close();
				}
				catch (Exception ex)
				{
                    log.error(ex.getMessage());
				}
			}

           return null;
       }
  
       
	public static void main(String[] args)
	{
		DBHelper help = new DBHelper("dtauser","dtauser","192.168.1.50\\NEWDB","MBD2_CITY_ENTRY");
		JobStatus jb = GetJobStatus(help, new CalendarEx(), "mrbcp" );
		jb.FinishTime = new CalendarEx().toString(2);
		jb.Result = "成功";
		jb.Info = "dt";
		SetJobStatus(help,new CalendarEx(),"mrbcp",jb);
		/*if (help.TestConn())
			System.out.println("连接成功1");
		else
			System.out.println("连接失败1");

		help.UpdateData("UPDATE [mcs4] SET name = 'test' WHERE name IS null",
				null);

		String sSQL = "SELECT TOP 3 * FROM [MTSYSTEM].[dbo].[mcs4]";
		ResultSet rs = help.GetResultSet(sSQL, null);
		try
		{
			while (rs.next())
			{
				System.out.println("id: " + rs.getDate(1) + " brand_name: "
						+ rs.getString(2));
			}
		}
		catch (Exception ex)
		{
			System.out.println(ex.getMessage());
		}
		finally
		{
			try
			{
				rs.close();
			}
			catch (Exception ex)
			{
				System.out.println(ex.getMessage());
			}
		}*/
	}
} 


