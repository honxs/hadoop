package cn.mastercom.bigdata.util.db;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;

import cn.mastercom.bigdata.util.CalendarEx;

public class GreepPlumHelper {
	
	static Logger log = Logger.getLogger(GreepPlumHelper.class.getName());
	public static String url = "jdbc:postgresql://10.139.6.154:5432/HAERBIN";
	static String user = "dtauser";
	static String password = "dtauser";
	static int  TimeOutSec = 3600*5;
	// 静态块 启动就加载
	static {
		try {
            readConfigInfo();
            Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	


	public static void readConfigInfo() {
		try {
			//XMLWriter writer = null;// 声明写XML的对象
			SAXReader reader = new SAXReader();

			OutputFormat format = OutputFormat.createPrettyPrint();
			format.setEncoding("GBK");// 设置XML文件的编码格式

			String filePath = "conf/config_gp.xml";
			File file = new File(filePath);
			if (file.exists()) {
				Document doc = reader.read(file);// 读取XML文件
				
				{
					List<String> list = doc.selectNodes("//comm/url");
					if(list != null){
						Iterator iter = list.iterator();
						while (iter.hasNext()) {
							Element element = (Element) iter.next();
							url = element.getText();
							break;
						}
					}
				}
				
				{
					List<String> list = doc.selectNodes("//comm/user");
					Iterator iter = list.iterator();
					while (iter.hasNext()) {
						Element element = (Element) iter.next();
						user = element.getText();
						break;
					}
				}
				
				{
					List<String> list = doc.selectNodes("//comm/password");
					if(list != null){
						Iterator iter = list.iterator();
						while (iter.hasNext()) {
							Element element = (Element) iter.next();
							password = element.getText();
							break;
						}
					}	
				}				
			} 
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static Connection getConnection(String url, String user, String password) {
		Connection con = null;
		try {
			con = DriverManager.getConnection(url, user, password);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return con;
	}

	public static Statement getStatement(Connection con) {
		Statement sm = null;
		try {
			sm = con.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return sm;
	}

	public static void main(String[] args) {
		//ImportData("dt",new CalendarEx(new Date()));
		//url = "jdbc:postgresql://192.168.1.65:5432/HAERBIN";
		boolean bret = GreepPlumHelper.IsTableExists("tb_cfg_city1");
	}

	public static int ImportSample(String type, CalendarEx cal)
	{
		String createExTable="select sp_import_sample('" + type + "','" + cal.toString(0) +"')";
		return ExecNonQuery(createExTable);
	}
	
	public static int ImportFGSampleorEvent(String type, String cal ,String templateTbName)
	{
		String createExTable = null;
		if(templateTbName.contains("TB_MODEL_SIGNAL_SAMPLE")){
			createExTable="select * into " + "tb_FG"+type+"signal_sample_01_"+ cal + " from tb_model_signal_sample ";
		}else{
			createExTable="select * into " + "tb_"+type+"signal_event_01_"+ cal + " from tb_model_signal_event ";
		}
		return ExecNonQuery(createExTable);
	}
	
	public static boolean IsTableExists(String tableName)
	{
		String createExTable="select oid from pg_class where relname ilike '" + tableName + "' and relkind = 'r'";
		Connection con = getConnection(url, user, password);
		Statement sm = getStatement(con);
		ResultSet rs;
		 try {
			rs=sm.executeQuery(createExTable);
			if(rs.next()){
				int result=rs.getInt(1);
				return true;
			}
		} catch (SQLException e) {
			log.info(e.getMessage());
			//log.info(e.getStackTrace());
		}
		finally
		{
			try
			{
				//sm.close();
				con.close();
			}
			catch (SQLException e)
			{
				e.printStackTrace();
			}
		}
		return false;
	}
	
	//create_daily_index
	public static String CreateDailyIndex(CalendarEx cal)
	{
		String createExTable="select create_daily_index('" + cal.toString(0) +"')";
		log.info("CreateDailyIndex,Sql= " + createExTable);
		Connection con = getConnection(url, user, password);
		Statement sm = getStatement(con);
		ResultSet rs;
		 try {
			sm.setQueryTimeout(TimeOutSec);
			rs=sm.executeQuery(createExTable);
			if(rs.next()){
				String result=rs.getString(1);
				log.info("CreateDailyIndex,Sql= " + createExTable + ", result=" +result);
				return result;
			}
		} catch (SQLException e) {
			log.info(e.getMessage());
		}
		finally
		{
			try
			{
				//sm.close();
				con.close();
			}
			catch (SQLException e)
			{
				e.printStackTrace();
			}
		}
		return "";
	}
	
	public static int ImportVilSample(CalendarEx cal)
	{
		String createExTable="select sp_import_vilsample('" + cal.toString(0) +"')";
		return ExecNonQuery(createExTable);
	}
	
	public static int ImportEvent(String type, CalendarEx cal)
	{
		String createExTable="select sp_import_event('" + type + "','" + cal.toString(0) +"')";
		return ExecNonQuery(createExTable);
	}
	
	public static int Import23GEvent(String type, CalendarEx cal)
	{
		String createExTable="select sp_import_23gevent('" + type + "','" + cal.toString(0) +"')";
		return ExecNonQuery(createExTable);
	}
	
	public static int ImportS1MMEXDR(CalendarEx cal)
	{
		String createExTable="select sp_import_s1mme('" + cal.toString(0) +"')";
		return ExecNonQuery(createExTable);
	}
	
	public static int ImportS1UXDR(CalendarEx cal)
	{
		String createExTable="select sp_import_s1u('" + cal.toString(0) +"')";
		return ExecNonQuery(createExTable);
	}
	
	public static int ImportBsEvtSample(String typeTable ,String hdfsPath)
	{
		String createExTable="select sp_import_sample_evt('" + typeTable +"','" + hdfsPath +"')";
		return ExecNonQuery(createExTable);
	}
	
	public static int ImportBsMrSample(String typeTable ,String hdfsPath)
	{
		String createExTable="select sp_import_sample_cover('" + typeTable +"','" + hdfsPath +"')";
		return ExecNonQuery(createExTable);
	}
	
	public static int importTable(String typeTable ,String hdfsPath,String gpProcedure)
	{
		String createExTable="select " + gpProcedure + "('" + typeTable +"','" + hdfsPath +"')";
		log.info( "gp sql : " + createExTable);
		return ExecNonQuery(createExTable);
	}
	
	public static int ExecNonQuery(String createExTable)
	{
		Connection con = getConnection(url, user, password);
		Statement sm = getStatement(con);
		ResultSet rs;
		 try {
			sm.setQueryTimeout(TimeOutSec);
			rs=sm.executeQuery(createExTable);
			if(rs.next()){
				int result=rs.getInt(1);
				log.info("ImportData,Sql= " + createExTable + ", result=" +result);
				return result;
			}
		} catch (SQLException e) {
			//e.printStackTrace();
			// System.out.println("INFO:" + "没有返回结果");
		}
		finally
		{
			try
			{
				con.close();
			}
			catch (SQLException e)
			{
				e.printStackTrace();
			}
		}
		return -1;
	}
}
