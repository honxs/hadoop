package cn.mastercom.bigdata.util.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * 数据连接类
 *
 * @version 1.0
 * @create by jan
 */
public class SqlDBHelper
{
	/**
	 * 数据库驱动类名称
	 */
	private static final String DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

	static
	{
		try
		{
			// 加载数据库驱动程序
			Class.forName(DRIVER);
		}
		catch (ClassNotFoundException e)
		{
			System.out.println("加载驱动错误");
			System.out.println(e.getMessage());
		}
	}

	/**
	 * 建立数据库连接
	 *
	 *
	 * @return
	 */
	public static Connection GetConnection(String URLSTR, String USERNAME, String USERPASSWORD)
	{
		Connection connnection = null;
		try
		{
			// 获取连接
		    connnection = DriverManager.getConnection(URLSTR, USERNAME, USERPASSWORD);
		}
		catch (SQLException e)
		{
			System.out.println(e.getMessage());
		}
		return connnection;
	}
	
	/**
	 * 建立数据库连接
	 *
	 *jdbc:sqlserver://localhost:1433; databaseName=Northwind; User=dtauser;Password=dtauser;
	 *
	 * @return
	 */
	public static Connection GetConnection(String connStr)
	{
		Connection connnection = null;
		try
		{
			// 获取连接
		    connnection = DriverManager.getConnection(connStr);
		}
		catch (SQLException e)
		{
			System.out.println(e.getMessage());
		}
		return connnection;
	}

	/**
	 * insert update delete 统一的方法
	 *
	 * @param sql
	 *            insert,update,delete SQL 语句
	 * @return 受影响的行数
	 */
	public static int ExcuteNoQuery(String connStr, String sql)
	{
		int affectedLine = 0;
		Connection connnection = null;
		Statement statement = null;
		
		try
		{
		    // 获得连接
		    connnection = GetConnection(connStr);
		    // 创建Statement对象
		    statement = connnection.createStatement();
			// 执行SQL语句
			affectedLine = statement.executeUpdate(sql);
		}
		catch (SQLException e)
		{
			System.out.println(e.getMessage());
		}
		finally
		{
			// 释放资源
			closeAll(null, connnection, statement);
		}
		return affectedLine;
	}

	/**
	 * 获取结果集，并将结果放在List中
	 * 
	 * @param sql
	 *            SQL语句
	 * @return List结果集
	 */
	public static List<DBRow> ExcuteQuery(String connStr, String sql)
	{
		Connection connnection = null;
		Statement statement = null;
		ResultSet resultSet = null;
		ResultSetMetaData rsmd = null;
		int columnCount = 0;
		List<DBRow> list = new ArrayList<DBRow>();
		
		try
		{
			// 获得连接
			connnection = GetConnection(connStr);
			// 创建Statement对象
			statement = connnection.createStatement();
			// 执行SQL 语句
			resultSet = statement.executeQuery(sql);
			
			rsmd = resultSet.getMetaData();
			
			columnCount = rsmd.getColumnCount();
						
			while (resultSet.next())
			{
				DBRow row = new DBRow(columnCount);	
				
				for (int i = 1; i <= columnCount; i++)
				{
					row.add(rsmd.getColumnLabel(i), i-1, resultSet.getObject(i));
				}
				list.add(row);
			}
			
		}
		catch (SQLException e)
		{
			System.out.println(e.getMessage());
		}
		finally
		{
			closeAll(resultSet, connnection, statement);
		}	
		
		return list;
	}
	
	
	/**
	 * 获取结果集，并将结果放在List中
	 * 
	 * @param sql
	 *            SQL语句
	 * @return List结果集
	 */
	public static boolean ExcuteQuery(String connStr, String sql, IFetchData fetchData)
	{
		Connection connnection = null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		int columnCount = 0;
		ResultSetMetaData rsmd = null;
		
		try
		{		
			connnection = DriverManager.getConnection(connStr);  
			  
			preparedStatement = (PreparedStatement) connnection.prepareStatement(sql,ResultSet.TYPE_FORWARD_ONLY,  
			          ResultSet.CONCUR_READ_ONLY);  
			            
			preparedStatement.setFetchSize(1000);  
			  
			preparedStatement.setFetchDirection(ResultSet.FETCH_FORWARD);  

			resultSet = preparedStatement.executeQuery();  			
			
		    rsmd = resultSet.getMetaData() ; 
		    
			columnCount = rsmd.getColumnCount();
			
			while (resultSet.next())
			{
				DBRow row = new DBRow(columnCount);	
					
				for (int i = 1; i <= columnCount; i++)
				{
					row.add(rsmd.getColumnLabel(i), i-1, resultSet.getObject(i));
				}
			
				fetchData.fetchData(row);
			}
			
		}
		catch (SQLException e)
		{
			System.out.println(e.getMessage());
			return false;
		}
		finally
		{
			closeAll(resultSet, connnection, preparedStatement);
		}	
		
		return true;
	}

	/**
	 * 释放所有资源
	 */
	private static void closeAll(ResultSet resultSet, Connection connnection, Statement statement)
	{
		// 释放结果集连接
		if (resultSet != null)
		{
			try
			{
				resultSet.close();
			}
			catch (SQLException e)
			{
				System.out.println(e.getMessage());
			}
		}
		// 释放声明连接
		if (statement != null)
		{
			try
			{
				statement.close();
			}
			catch (SQLException e)
			{
				System.out.println(e.getMessage());
			}
		}
		// 释放数据库连接
		if (connnection != null)
		{
			try
			{
				connnection.close();
			}
			catch (SQLException e)
			{
				System.out.println(e.getMessage());
			}
		}
	}
	

}