package cn.mastercom.bigdata.sqlhp;

import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


import cn.mastercom.bigdata.util.CalendarEx;
import org.apache.log4j.Logger;


public class DBHelper
{
    static Logger log = Logger.getLogger(DBHelper.class.getName());
	Connection _CONN = null;
	public String sUser;
    public String sPwd;
    public String ServerIp;
    public String DbName;

    public DBHelper(String user, String pwd, String ip, String Dbname){
        this.sUser = user;
        this.sPwd = pwd;
        this.ServerIp = ip;
        this.DbName = Dbname;
    }

	public static void main(String[] args)
	{
		CalendarEx curTime = new CalendarEx(new Date());
		DBHelper help = new DBHelper("dtauser","dtauser","192.168.1.50\\NEWDB:14331","MBD2_CITY_ENTRY");
		JobStatus jb = new JobStatus();
		jb.FinishTime = new CalendarEx().toString(2);
		jb.Result = "成功";
		jb.Info = "dt";
		JobHelper.SetJobStatus(help, curTime, "evt2gbcp", jb);
	}

	// 取得连接
	public Connection GetConn()
	{
		if (_CONN != null)
			return _CONN;
		try
		{
			String sDriverName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
			String sDBUrl = "jdbc:sqlserver://" + ServerIp + ";databaseName=" + DbName;
			Class.forName(sDriverName);
			_CONN = DriverManager.getConnection(sDBUrl, sUser, sPwd);
		} catch (Exception ex)
		{
		    log.error("connection error " + ex.getStackTrace());
			return null;
		}
		return _CONN;
	}

	// 关闭连接
	public void CloseConn()
	{
		try
		{
			_CONN.close();
			_CONN = null;
		} catch (Exception ex)
		{
            log.error("CloseConn error " + ex.getStackTrace());
			_CONN = null;
		}
	}

	// 测试连接
	public boolean TestConn()
	{
		if (GetConn()==null)
			return false;

		CloseConn();
		return true;
	}

	public ResultSet GetResultSet(String sSQL, Object[] objParams)
	{
		GetConn();
		ResultSet rs = null;
		try
		{
			PreparedStatement ps = _CONN.prepareStatement(sSQL);
			if (objParams != null)
			{
				for (int i = 0; i < objParams.length; i++)
				{
					ps.setObject(i + 1, objParams[i]);
				}
			}
			rs = ps.executeQuery();
		} catch (Exception ex)
		{
            log.error("GetResultSet error " + ex.getStackTrace());
			CloseConn();
		} finally
		{
			// CloseConn();
		}
		return rs;
	}

	public Object GetSingle(String sSQL, Object... objParams)
	{
		GetConn();
		try
		{
			PreparedStatement ps = _CONN.prepareStatement(sSQL);
			if (objParams != null)
			{
				for (int i = 0; i < objParams.length; i++)
				{
					ps.setObject(i + 1, objParams[i]);
				}
			}
			ResultSet rs = ps.executeQuery();
			if (rs.next())
				return rs.getString(1);// 索引从1开始
		} catch (Exception ex)
		{
            log.error("GetSingle error " + ex.getStackTrace());
		} finally
		{
			CloseConn();
		}
		return null;
	}

	public DataTable GetDataTable(String sSQL, Object... objParams)
	{
		GetConn();
		DataTable dt = null;
		try
		{
			PreparedStatement ps = _CONN.prepareStatement(sSQL);
			if (objParams != null)
			{
				for (int i = 0; i < objParams.length; i++)
				{
					ps.setObject(i + 1, objParams[i]);
				}
			}
			ResultSet rs = ps.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();

			List<DataRow> row = new ArrayList<DataRow>(); // 表所有行集合
			List<DataColumn> col = null; // 行所有列集合
			DataRow r = null;// 单独一行
			DataColumn c = null;// 单独一列

			String columnName;
			Object value;
			int iRowCount = 0;
			while (rs.next())// 开始循环读取，每次往表中插入一行记录
			{
				iRowCount++;
				col = new ArrayList<DataColumn>();// 初始化列集合
				for (int i = 1; i <= rsmd.getColumnCount(); i++)
				{
					columnName = rsmd.getColumnName(i);
					value = rs.getObject(columnName);
					c = new DataColumn(columnName, value);// 初始化单元列
					col.add(c); // 将列信息加入到列集合
				}
				r = new DataRow(col);// 初始化单元行
				row.add(r);// 将行信息加入到行集合
			}
			dt = new DataTable(row);
			dt.RowCount = iRowCount;
			dt.ColumnCount = rsmd.getColumnCount();
		} catch (Exception ex)
		{
            log.error("GetDataTable error " + ex.getStackTrace());
		} finally
		{
			CloseConn();
		}
		return dt;
	}

	public int UpdateData(String sSQL, Object[] objParams)
	{
		GetConn();
		int iResult = 0;

		try
		{
			Statement ps = _CONN.createStatement();
			iResult = ps.executeUpdate(sSQL);
		} catch (Exception ex)
		{
            log.error("UpdateData error " + ex.getStackTrace());
			return -1;
		} 
		finally
		{
			CloseConn();
		}
		return iResult;
	}

}
