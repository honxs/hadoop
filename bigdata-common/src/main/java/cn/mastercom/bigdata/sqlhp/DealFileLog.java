package cn.mastercom.bigdata.sqlhp;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

public class DealFileLog {

	String fileType="";
	String fileName="";
	long   fileSize=0;
    private static Logger log = Logger.getLogger(DealFileLog.class.getName());
	public static void main(String[] args) {
		test();
	}

	public boolean FillData(String val)
	{
		String[] vct = val.split("\\|");
		if(vct.length<3)
			return false;
		
		fileType =vct[0];		
		fileName =vct[1];
		fileSize = Long.valueOf(vct[2]);
		
		return true;
	}
	
	private static void test()
	{
		DBHelper help = new DBHelper("dtauser","dtauser","192.168.1.50\\NEWDB:14331","MBD2_CITY_ENTRY");
		DealFileLog log = new DealFileLog();
		log.fileName="0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";
		log.fileType ="MR";
		log.fileSize = 110;
		ArrayList<DealFileLog> listLog =new ArrayList<DealFileLog>();
		for(int i=0; i<10000; i++)
		{
			listLog.add(log);
		}
		try {
			DealFileLog.CreateTable(help, "tb_原始数据核查_数据文件明细", "tb_原始数据核查_数据文件明细_dd_171220");
			SaveDealLogs(help.GetConn(),"tb_原始数据核查_数据文件明细_dd_171220",listLog);
		} catch (Exception e) {			
			e.printStackTrace();
		}
		help.CloseConn();
	}
	
	public static void  CreateTable(DBHelper help, String SrcTable, String DstTable)
	{
       String sql = "exec proc_后台_建表 '" + SrcTable + "','" + DstTable + "',null";
       help.UpdateData(sql, null);
	}
	   
	public static void SaveDealLogs(Connection connection, String tablename, List<DealFileLog> logs) throws Exception
	{
        String sql = "insert into " + tablename + " (文件类型 ,文件名称 ,文件大小) values (?,?,?)";       
        PreparedStatement ps = connection.prepareStatement(sql);       
        long start=System.currentTimeMillis();
        log.info("SaveDealLogs Start ... ");
        int i=0;
        for (DealFileLog pay: logs) 
        {
        	i++;
            ps.setString(1, pay.fileType);
            ps.setString(2, pay.fileName);
            ps.setLong(3, pay.fileSize);

            ps.addBatch();
            
            if(i>100)
            {
            	ps.executeBatch();
            	ps.clearBatch();
            	i=0;
            }     
        }
        ps.executeBatch(); // insert remaining records
        ps.close();
                
        long end=System.currentTimeMillis();
        log.info("SaveDealLogs End : "+(end-start));
	}
}
