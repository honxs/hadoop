package cn.mastercom.bigdata.util.db;


import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DBHelper
{
	Connection _CONN = null;
	static String sUser;
	static String sPwd;
	static String ServerIp;
	public static String DbName;

	// 静态块 启动就加载
	static
	{
		try
		{
			
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	public void setDBValue(String user, String pwd, String ip, String Dbname)
	{
		sUser = user;
		sPwd = pwd;
		ServerIp = ip;
		DbName = Dbname;
	}

	/*
	 * public DBHelper(String sUser, String sPwd, String ServerIp, String
	 * DbName) { this.sUser = sUser; this.sPwd = sPwd; this.ServerIp = ServerIp;
	 * this.DbName = DbName; }
	 */

	// 取得连接
	private boolean GetConn()
	{
		if (_CONN != null)
			return true;
		try
		{
			String sDriverName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
			String sDBUrl = "jdbc:sqlserver://" + ServerIp + ";databaseName=" + DbName;
			System.out.println("sDriverName:"+sDriverName);
			System.out.println("sDBUrl:"+sDBUrl);
			Class.forName(sDriverName);
			System.out.println("user:"+sUser+"  pwd:"+sPwd);
			_CONN = DriverManager.getConnection(sDBUrl, sUser, sPwd);
		} catch (Exception ex)
		{
			System.out.println("连接出错");
			System.out.println(ex.getMessage());
			return false;
		}
		return true;
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
			System.out.println(ex.getMessage());
			_CONN = null;
		}
	}

	// 测试连接
	public boolean TestConn()
	{
		if (!GetConn())
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
			System.out.println(ex.getMessage());
			CloseConn();
		} finally
		{
			// CloseConn();
		}
		return rs;
	}

/*
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
			System.out.println(ex.getMessage());
		} finally
		{
			CloseConn();
		}
		return null;
	}
*/

/*	public DataTable GetDataTable(String sSQL, Object... objParams)
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
			System.out.println(ex.getMessage());
		} finally
		{
			CloseConn();
		}
		return dt;
	}*/


	public void UpdateData(String sSQL, ArrayList<String[]> objParams)
	{
		GetConn();


		try
		{
//			Statement ps = _CONN.createStatement();
			PreparedStatement preparedStatement = _CONN.prepareStatement(sSQL);
			for (int i = 0; i <objParams.size() ; i++) {
				preparedStatement.setString(1,objParams.get(i)[0]);
				preparedStatement.setInt(2,Integer.parseInt(objParams.get(i)[1]));
				preparedStatement.setString(3,objParams.get(i)[2]);
				preparedStatement.addBatch();

			}

			preparedStatement.executeBatch();

		} catch (Exception ex)
		{
			System.out.println(ex.getMessage());

		} finally
		{
			CloseConn();
		}

	}
	public void UpdateFileData(String sSQL, ArrayList<String[]> objParams)
	{

		GetConn();



		try
		{
//			Statement ps = _CONN.createStatement();
			PreparedStatement preparedStatement = _CONN.prepareStatement(sSQL);
			for (int i = 0; i <objParams.size() ; i++) {
				preparedStatement.setString(1,objParams.get(i)[0]);
				preparedStatement.setString(2,objParams.get(i)[1]);
				preparedStatement.setString(3,objParams.get(i)[2]);
				preparedStatement.setString(4,objParams.get(i)[3]);
				preparedStatement.setString(5,objParams.get(i)[4]);
				preparedStatement.setLong(6,Integer.parseInt(objParams.get(i)[5]));
				preparedStatement.setString(7,objParams.get(i)[6]);
				preparedStatement.setString(8,objParams.get(i)[7]);
				preparedStatement.addBatch();
			}

			preparedStatement.executeBatch();
		} catch (Exception ex)
		{
			System.out.println(ex.getMessage());

		} finally
		{
			//这里注释掉
//			CloseConn();
		}

	}

	public void InsertDateHours(String sSQL, ArrayList<String[]> objParams)
	{

		GetConn();
		try
		{
//			Statement ps = _CONN.createStatement();
			PreparedStatement preparedStatement = _CONN.prepareStatement(sSQL);
			for (int i = 0; i <objParams.size() ; i++) {
				preparedStatement.setString(1,objParams.get(i)[0]); //开始时间
				preparedStatement.setString(2,objParams.get(i)[1]); //结束时间
				preparedStatement.setInt(3,Integer.parseInt(objParams.get(i)[2]));// 频段
				preparedStatement.setString(4,objParams.get(i)[3]); //数据类型
				preparedStatement.addBatch();
			}

			preparedStatement.executeBatch();
		} catch (Exception ex)
		{
			System.out.println(ex.getMessage());

		} finally
		{
			//暂时去掉
//			CloseConn();
		}

	}

	public void UpdateDateHours(String sSQL, ArrayList<String[]> objParams)
	{

		GetConn();
		try
		{
//			Statement ps = _CONN.createStatement();
			PreparedStatement preparedStatement = _CONN.prepareStatement(sSQL);
			for (int i = 0; i <objParams.size() ; i++) {
				preparedStatement.setString(1,objParams.get(i)[0]);
				preparedStatement.setString(2,objParams.get(i)[1]);
				preparedStatement.setString(3,objParams.get(i)[2]);
				preparedStatement.setString(4,objParams.get(i)[3]);
				preparedStatement.addBatch();
			}
			preparedStatement.executeBatch();
		} catch (Exception ex)
		{
			System.out.println(ex.getMessage());

		} finally
		{
//暂时去掉
//			CloseConn();
		}

	}



	public void exeSql(String sql){
		GetConn();
		try {
			PreparedStatement ps = _CONN.prepareStatement(sql);
			ps.execute();

		}catch (Exception e){
			e.printStackTrace();
		}finally {
			//这里注释掉
//			CloseConn();
		}
	}


}
