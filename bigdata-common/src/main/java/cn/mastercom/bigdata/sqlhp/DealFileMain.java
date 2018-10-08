package cn.mastercom.bigdata.sqlhp;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import cn.mastercom.bigdata.util.LocalFile;
import org.apache.log4j.Logger;

public class DealFileMain extends Thread{
	static ExecutorService pool = null;
	static boolean bExit = false;
    public static String sUser;
    public static String sPwd;
    public static String ServerIp;
    public static String DbName;
    private static Logger log = Logger.getLogger(DealFileMain.class.getName());
	private class ExitHandler extends Thread 
    {
        public ExitHandler() {
            super("Exit Handler");
        }
        @Override
		public void run() {
            log.info("Set exit");          
            try {
            	if(pool != null)
            	{
            		pool.shutdown();
            	}
				Thread.sleep(5000);
			} catch (InterruptedException e) 
            {
			}
            System.exit(1);
        }
    }
    
    public DealFileMain(String paths,String user, String pwd, String ip, String Dbname)
    {
		inputPaths = paths;
		this.sUser = user;
		this.sPwd = pwd;
		this.ServerIp = ip;
		this.DbName = Dbname;
		Runtime.getRuntime().addShutdownHook(new ExitHandler());
    }
    
	public static void main(String[] args) {
		if(args.length<1)
		{
			log.info("usage:<inputpaths>");
			return;
		}
		DealFileMain md = new DealFileMain(args[0],"","","","");
        md.start();
	}

	static String inputPaths;
	
    public static String GetDateString(String str) 
	{     
	    String regEx = "201\\d{5}";
	    Pattern pattern = Pattern.compile(regEx);
	    Matcher matcher = pattern.matcher(str);
	    boolean rs = matcher.find();
	    if(rs)
	    {
	    	return matcher.group();
	    }
	    return ""; 
	}
    
	private static void SaveLogsToDb()
	{
		String[] listInpath = inputPaths.split(",");
		for(String path1 : listInpath)
		{
			if(!LocalFile.checkFileExist(path1))
			{
				continue;
			}
			
			try {
				List<String> fileList = LocalFile.getAllFiles(new File(path1), "", 1);
				for(String filename : fileList)
				{ 
					if (filename.toLowerCase().endsWith(".tmp") ||
						filename.toLowerCase().endsWith(".dealing"))
					{
						continue;
					}
					DealOneFile(filename, GetDateString(new File(filename).getParent())) ;
				}
				
			} catch (Exception e) {
                log.error(e.getStackTrace());
				continue;
			}		
		}
	}
	
	private static void DealOneFile(String FileName, String dateStr) {
		BufferedReader reader = null;
		ZipInputStream zip = null;
		String tempFileName = FileName + ".dealing";
		try {
			if(FileName.toLowerCase().endsWith(".zip"))
			{//zip
				zip = new ZipInputStream(new FileInputStream(FileName));
	            ZipEntry entry = zip.getNextEntry();
	            if (entry == null) 
	            	return;
	            reader = new BufferedReader(new InputStreamReader(zip));
			}
			else
			{
				if(!LocalFile.renameFile(FileName, tempFileName))
				{
					return;
				}
				reader = new BufferedReader(new FileReader(tempFileName));
			}

			String tempString = null;
			DBHelper help = new DBHelper(sUser,sPwd,ServerIp,DbName);
			ArrayList<DealFileLog> listLog =new ArrayList<DealFileLog>();
			while (!bExit && (tempString = reader.readLine()) != null) 
			{
				DealFileLog log = new DealFileLog();
				log.FillData(tempString);
				listLog.add(log);								
			}
			
			try {
				String tbName = "tb_原始数据核查_数据文件明细_dd_"+ dateStr.substring(2);
				DealFileLog.CreateTable(help, "tb_原始数据核查_数据文件明细", tbName);
				DealFileLog.SaveDealLogs(help.GetConn(), tbName, listLog);
				help.CloseConn();
				if (reader != null) {
					try {
						reader.close();
						reader=null;
					} catch (IOException e1) {
					}
				}
				LocalFile.renameFile(tempFileName, FileName.replace("deallog", "deallogbak"));
			} catch (Exception e) {			
				e.printStackTrace();
			}
		} catch (Exception e) {
			log.error(e.getStackTrace());
			return;
		} finally {
			if (reader != null) {
				try {
					reader.close();
					reader=null;
				} catch (IOException e1) {
				}
			}
			if (zip != null) {
				try {
					zip.close();
				} catch (IOException e1) {
				}
			}
		}

	}

	@Override
	public void run()
    {
		log.info("DealFileLog thread start! "  + inputPaths);

		while (!bExit)
		{ 
			try
			{				
				SaveLogsToDb();
				Thread.sleep(60000);
			} 
			catch (Exception e)
			{
				log.error("Thread " + " error:" + e.getMessage());
			}
		}

		log.info("DealFileMain thread end");
    }
}
