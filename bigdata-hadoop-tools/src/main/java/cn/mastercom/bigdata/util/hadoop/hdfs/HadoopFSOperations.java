package cn.mastercom.bigdata.util.hadoop.hdfs;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;
import javax.swing.UIManager;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.JTableHeader;
import javax.swing.table.TableCellRenderer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.nullCondition_return;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.webapp.example.MyApp;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.Versioned;
import com.ice.tar.TarEntry;
import com.ice.tar.TarInputStream;

import cn.mastercom.bigdata.util.CalendarEx;
import cn.mastercom.bigdata.util.LocalFile;
import cn.mastercom.bigdata.util.ftp.FTPClientHelper;
import cn.mastercom.bigdata.util.ftp.TaskInfoFtp;
import cn.mastercom.bigdata.util.hadoop.hdfs.DownloadAndUploadTask.TaskType;
import scala.reflect.internal.Trees.This;



@SuppressWarnings("unused")
public class HadoopFSOperations
{
	static Logger log = Logger.getLogger(HadoopFSOperations.class.getName());
	public static final String  FS_DEFAULT_NAME_KEY = "fs.defaultFS";
	//public static long nTotalLength = 0;
	
	private static Configuration conf = new Configuration();
	public String HADOOP_URL = "";
	String[] HADOOP_URL_List = null;
	public static FileSystem fs;
	private String hadoopHost;
	
	private int hadoopPort;
	public static String downFileName;

	private static DistributedFileSystem hdfs;

	public HadoopFSOperations()
	{
		// SetHadoopRoot();
	}

	public class TaskInfo {
		public List<String> fileLst = new ArrayList<String>();// 待处理文件完整路径组成的list
		public String outputLocalName;
		public boolean Compress = true;
		public List<String> fileName = new ArrayList<String>();
	}
	
	public static final String KEYTAB_FILE_KEY = "hdfs.keytab.file";  
    public static final String USER_NAME_KEY   = "hdfs.kerberos.principal";  
    public static void login(Configuration hdfsConfig) throws IOException {  
        //if (UserGroupInformation.isSecurityEnabled()) 
        {  
        	log.info("UserGroupInformation.isSecurityEnabled");
            String keytab = conf.get(KEYTAB_FILE_KEY);  
            if (keytab != null) {  
                hdfsConfig.set(KEYTAB_FILE_KEY, keytab);  
            }  
            String userName = conf.get(USER_NAME_KEY);  
            if (userName != null) {  
                hdfsConfig.set(USER_NAME_KEY, userName);  
            }  
            SecurityUtil.login(hdfsConfig, KEYTAB_FILE_KEY, USER_NAME_KEY);  
        }  
        //log.info("UserGroupInformation.isSecurityEnabled is fause");
    }  
	
    public HadoopFSOperations(String keytabFile, String username) throws Exception
	{
		try
		{
			Configuration conf = new Configuration();
			
			//设置安全验证方式为kerberos
			conf.set("hadoop.security.authentication","kerberos");

			//使用设置的用户登陆
			UserGroupInformation.setConfiguration(conf);
			UserGroupInformation.loginUserFromKeytab(username,keytabFile);
	        
			HadoopFSOperations.conf = conf;

			fs   = FileSystem.get(conf);
			hdfs = (DistributedFileSystem) fs;
		
			String hdfsUrl = conf.get(FS_DEFAULT_NAME_KEY);
			log.info("hdfsUrl:"+hdfsUrl);
			String tmStr = hdfsUrl.replace("hdfs://", "");
			if(tmStr.indexOf(":") > 0)
			{
				this.hadoopHost = tmStr.substring(0, tmStr.indexOf(":"));
				this.hadoopPort = Integer.parseInt(tmStr.substring(tmStr.indexOf(":")+1));
			}
			else 
			{
				this.hadoopHost = tmStr;
				this.hadoopPort = 8020;
			}
			HADOOP_URL = "hdfs://" + hadoopHost + ":"+hadoopPort;
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw e;
		}
	}
    
	public HadoopFSOperations(Configuration conf) throws Exception
	{
		try
		{
			HadoopFSOperations.conf = conf;

			fs = FileSystem.get(conf);
			hdfs = (DistributedFileSystem) fs;
		
			String hdfsUrl = conf.get(FS_DEFAULT_NAME_KEY);
			String tmStr = hdfsUrl.replace("hdfs://", "");
			if(tmStr.indexOf(":") > 0)
			{
				this.hadoopHost = tmStr.substring(0, tmStr.indexOf(":"));
				this.hadoopPort = Integer.parseInt(tmStr.substring(tmStr.indexOf(":")+1));
			}
			else 
			{
				this.hadoopHost = tmStr;
				this.hadoopPort = 8020;
			}
			HADOOP_URL = "hdfs://" + hadoopHost + ":"+hadoopPort;
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw e;
		}
	}
	
	class FileDownloadCallable implements Callable<Object>
	{
		private TaskInfo task;

		FileDownloadCallable(TaskInfo taskInfo)
		{
			this.task = taskInfo;
		}

		@Override
		public Object call() throws Exception
		{
			FileOutputStream os = null;
			GZIPOutputStream gfs = null;
			if (task != null)
			{
				int fileSeq = 1;
				long nTotalLen = 0;
				String fileName = task.outputLocalName;

				if (task.Compress)
				{
					gfs = new GZIPOutputStream(new FileOutputStream(fileName + ".gz"));
					os = null;
				}
				else
				{
					os = new FileOutputStream(fileName);
					gfs = null;
				}

				for (int i = 0; i < task.fileLst.size(); i++)
				{
					String hdfsFilename = task.fileLst.get(i);
					Path f = new Path(hdfsFilename);
					FSDataInputStream dis = null;
					try {
						dis = fs.open(f);
						InputStream is = dis;
						GZIPInputStream gis = null;
						if(hdfsFilename.toLowerCase().endsWith(".gz"))
						{
							gis = new GZIPInputStream(dis);
							is  = gis;
						}
						byte[] buffer = new byte[1024000];
						int length = 0;
						long nTotalLength = 0;
						int nCount = 0;
						while ((length = is.read(buffer)) > 0)
						{
							nCount++;
							if (gfs != null)
							{
								gfs.write(buffer, 0, length);
								gfs.flush();
							}
							else
							{
								os.write(buffer, 0, length);
								os.flush();
							}
							nTotalLength += length;
						}
						
					} catch (Exception e) {
						log.error(hdfsFilename + e.toString());
						continue;
					}finally {
						if(dis != null)
						{
							dis.close();
						}
						
					}				
				}
				if(gfs != null){
					gfs.close();
				}
				if(os != null ){
					os.close();
				}
			}
			return "Task Exec Success:" + task.outputLocalName;
		}
	}

	class FileCallable implements Callable<Object>
	{
		private TaskInfo task;

		FileCallable(TaskInfo taskInfo)
		{
			this.task = taskInfo;
		}

		@Override
		public Object call() throws Exception
		{
			if (task != null)
			{
				try
				{
					FileOutputStream os = null;

					int fileSeq = 1;
					long nTotalLen = 0;
					String fileName = task.outputLocalName;

					os = new FileOutputStream(fileName);

					for (int i = 0; i < task.fileLst.size(); i++)
					{
						String hdfsFilename = task.fileLst.get(i);
						Path f = new Path(hdfsFilename);
						FSDataInputStream dis = fs.open(f);

						byte[] buffer = new byte[10 * 1024000];
						int length = 0;
						long nTotalLength = 0;
						int nCount = 0;
						while ((length = dis.read(buffer)) > 0)
						{
							nCount++;

							os.write(buffer, 0, length);
							
							nTotalLength += length;
						}

						dis.close();
					}
					os.flush();
					os.close();

				}
				catch (Exception e)
				{
					log.error("文件: " + task.outputLocalName + e.toString());
					return "Task Exec Error:" + task.outputLocalName + "\r\n" + e.getMessage();
				}
			}
			return "Task Exec Success:" + task.outputLocalName;
		}
	}

	ReentrantLock m_ReentrantLock = new ReentrantLock(true);
	public int m_ThreadCnt;

	
	public class FileDownloadCallables implements Callable<Object> {
		public TaskInfo task;
		public String SelectDate;
		public String localFile;
		

		public FileDownloadCallables(TaskInfo taskInfo,String SelectDate,String localFile) {
			this.task = taskInfo;
			this.SelectDate = SelectDate;
			this.localFile = localFile;
		}
		@Override
		public Object call() throws Exception {
			FileOutputStream os = null;
			if (task != null) {
				try {
					m_ReentrantLock.lock();
					m_ThreadCnt++;
					m_ReentrantLock.unlock();
					
					for (int i = 0; i < task.fileLst.size(); i++) {												
						workWrite(task.fileName.get(i),task.fileLst.get(i),os);	
						writeLog(task.fileLst.get(i),SelectDate,localFile);
					}
					
				} catch (Exception e) {
					
					Thread.sleep(1000);
					log.info("下载异常,休息一分钟：" + e.getMessage());					
					return "Task Exec Fail....";
				}
			}
			return "Task Exec Success.....";
		}
	}
	
	public void writeLog(String path ,String SelectDate,String localFile) {
		String fileLog = getLogPath(path,SelectDate, localFile);

		if (fileLog == null)
			return;

		OutputStreamWriter out = null;
		BufferedWriter bufferedWriter = null;
		try {
			out = new OutputStreamWriter(new FileOutputStream(fileLog, true));
			try {
				bufferedWriter = new BufferedWriter(out);
				bufferedWriter.write(path + "\r\n");
				bufferedWriter.flush();
			} finally {
				try {
					if (bufferedWriter != null)
						bufferedWriter.close();
				} catch (Exception e) {

				}
			}

		} catch (Exception e) {
			log.info( " 写日志出错！");
		} finally {
			try {
				if (out != null)
					out.close();
			} catch (Exception e) {

			}

		}
	}
	
	private String getLogPath(String path, String SelectDate,String localFile) {
		if (path.indexOf(SelectDate.replace("-", "")) > 0) {
			return localFile  + SelectDate + ".txt";
		}
		return null;
	}
	
	public void workWrite(String fileName,String hdfsName,FileOutputStream os){
		FSDataInputStream dis = null ;
		try {		
			os = new FileOutputStream(fileName);
			String hdfsFilename = hdfsName;
			Path f = new Path(hdfsFilename);
			dis = fs.open(f);

			byte[] buffer = new byte[1024000];
			int length = 0;
			long nTotalLength = 0;
			int nCount = 0;
			while ((length = dis.read(buffer)) > 0) {
				nCount++;
				os.write(buffer, 0, length);							
				nTotalLength += length;
			}	
			log.info("下载成功:"+fileName);
			dis.close();
			os.close();	
			os.flush();	
		} catch (Exception e) {
			try {
				if(os != null ){				
					os.close();					
				}
				if(dis != null){
					dis.close();					
				}
			} catch (IOException e1) {
				
			}
		}finally{
			try {
				if(os != null ){				
					os.close();					
				}
				if(dis != null){
					dis.close();					
				}
			} catch (IOException e1) {
				
			}
		}
		
	}
	
	
	public HadoopFSOperations(String hdfsRoot)
	{
		conf = new Configuration();
		try {
			fs = FileSystem.get(conf);
			if(fs.getClass().equals(DistributedFileSystem.class))
			{
				log.info("Init hdfs from HadoopFSOperations： " + HADOOP_URL);
				hdfs = (DistributedFileSystem) fs;
				return;
			}
		} catch (IOException e) {			
			e.printStackTrace();
		}		
		if (hdfsRoot.trim().length() == 0)
			return;

		HADOOP_URL_List = hdfsRoot.split(";");
		ProcessStandby();
	}

	private void ProcessStandby()
	{
		for (int i = 0; i < HADOOP_URL_List.length; i++)
		{
			HADOOP_URL = HADOOP_URL_List[i];
			SetHadoopRoot();
			if (!checkStandbyException("/"))
				break;
		}
	}

	public boolean ReConnect()
	{
		try
		{
			FileSystem.closeAll();
			SetHadoopRoot();
			if (checkStandbyException("/"))
			{
				ProcessStandby();
			}
			return checkFileExist("/");
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		return false;
	}
	
	public void disConnect()
	{
		try {
			FileSystem.closeAll();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void SetHadoopRoot()
	{
		try
		{
			if (HADOOP_URL.length() < 6)
				return;
			log.info("SetHadoopRoot, URL： " + HADOOP_URL);
			FileSystem.setDefaultUri(conf, HADOOP_URL);
			fs = FileSystem.get(conf);
			hdfs = (DistributedFileSystem) fs;
			log.info("hdfs:" + hdfs.getClass().getName());
		}
		catch (Exception e)
		{
			log.info("SetHadoopRoot error： " + e.getMessage());
			e.printStackTrace();
		}
		log.info("SetHadoopRoot End： " + HADOOP_URL);
	}

	@SuppressWarnings("deprecation")
	public void moveSmallFilesToParent(String parentDir)
	{
		log.info("MoveSmallFilesToParent： " + parentDir);
		FileStatus fileStatus[];
		try
		{
			fileStatus = fs.listStatus(new Path(parentDir));
			int listlength = fileStatus.length;

			for (int i = 0; i < listlength; i++)
			{
				if (fileStatus[i].isDirectory() == true)
				{
					String pathName = fileStatus[i].getPath().getName().toLowerCase();
					/*
					 * if (!pathName.contains("sample") &&
					 * !pathName.contains("event") &&
					 * !pathName.contains("grid")) { continue; }
					 */

					FileStatus childStatus[] = fs.listStatus(new Path(parentDir + "/" + pathName));

					int childListlength = childStatus.length;
					boolean bShouldMove = false;
					try
					{
						for (int j = 0; j < childListlength; j++)
						{
							if (childStatus[j].isDirectory() == false)
							{
								String childName = childStatus[j].getPath().getName().toLowerCase();
								if (!childName.contains("sample") && !childName.contains("event") && !childName.contains("grid"))
								{
									continue;
								}
								movefile(parentDir + "/" + pathName + "/" + childName, parentDir);
								bShouldMove = true;
							}
						}
						if (bShouldMove)
						{
							fs.delete(new Path(parentDir + "/" + pathName));
							log.info("删除文件夹成功: " + parentDir + "/" + pathName);
						}
					}
					catch (Exception e)
					{
						e.printStackTrace();
					}
				}
			}
		}
		catch (FileNotFoundException e)
		{
			e.printStackTrace();
		}
		catch (IllegalArgumentException e)
		{
			e.printStackTrace();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	public FileStatus[] listStatus(String path)
	{
		try
		{
			if (!path.startsWith("hdfs"))
				path = HADOOP_URL + path;
			return fs.listStatus(new Path(path));
		}
		catch (IOException e)
		{
			log.info(e.getMessage());
		}
		return null;

	}

	/**
	 * 得到文件夹下的文件，元素是DatafileInfo包括文件名、文件大小、文件修改时间
	 */
	public ArrayList<String> allfileList = new ArrayList<String>();
	public ArrayList<String> listAllFiles(String path) throws FileNotFoundException, IllegalArgumentException, IOException
	{
		FileStatus fileStatus[] = fs.listStatus(new Path(path));
		if (fileStatus!=null)
		{
			for (int i = 0; i < fileStatus.length; i++)
			{
				if (fileStatus[i].isDirectory())
				{
					listAllFiles(fileStatus[i].getPath().toString());
				}
				else
				{
					allfileList.add(fileStatus[i].getPath().toString());
				}
			}
		}
		return allfileList;
	}
	
	/**
	 * path 下面全是文件时使用
	 * @param path
	 * @return
	 * @throws FileNotFoundException
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	public ArrayList<String> listAllFile(String path) throws FileNotFoundException, IllegalArgumentException, IOException
	{
		ArrayList<String> allfileList = new ArrayList<String>();
		FileStatus fileStatus[] = fs.listStatus(new Path(path));
		if (fileStatus!=null)
		{
			for (int i = 0; i < fileStatus.length; i++)
			{
				allfileList.add(fileStatus[i].getPath().toString());
			}
		}
		return allfileList;
	}
	
	public ArrayList<DatafileInfo> listFiles(String path) throws FileNotFoundException, IllegalArgumentException, IOException
	{
		FileStatus fileStatus[] = fs.listStatus(new Path(path));
		int listlength = fileStatus.length;
		ArrayList<DatafileInfo> fileList = new ArrayList<DatafileInfo>();
		for (int i = 0; i < listlength; i++)
		{
			if (fileStatus[i].isDirectory() == false)
			{
				if (fileStatus[i].getLen() >= 0)
				{
					long modificationTime = fileStatus[i].getModificationTime();
					fileList.add(new DatafileInfo(fileStatus[i].getPath().getName(), fileStatus[i].getLen(), modificationTime ,fileStatus[i].getPath().toString()));
				}
			}
		}
		return fileList;
	}

	/**
	 * 得到文件夹下的文件夹，元素是DatafileInfo包括文件夹名、文件夹大小、文件夹修改时间
	 */
	public ArrayList<DatafileInfo> listSubDirs(String path)
	{
		ArrayList<DatafileInfo> fileList = new ArrayList<DatafileInfo>();
		try
		{
			FileStatus fileStatus[] = fs.listStatus(new Path(path));
			int listlength = fileStatus.length;
			for (int i = 0; i < listlength; i++)
			{
				if (fileStatus[i].isDirectory() == true)
				{
					long modificationTime = fileStatus[i].getModificationTime();
					fileList.add(new DatafileInfo(fileStatus[i].getPath().getName(), fileStatus[i].getLen(), modificationTime, ""));
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return fileList;
	}

	/**
	 * 列出所有DataNode的名字信息
	 */
	public void listDataNodeInfo()
	{
		try
		{
			DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
			String[] names = new String[dataNodeStats.length];
			log.info("List of all the datanode in the HDFS cluster:");

			for (int i = 0; i < names.length; i++)
			{
				names[i] = dataNodeStats[i].getHostName();
				log.info(names[i]);
			}
			log.info(hdfs.getUri().toString());
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public boolean movefile(String src, String dst)
	{
		try
		{
			Path p1 = new Path(src);
			Path p2 = new Path(dst);
			hdfs.rename(p1, p2);

			log.info("mv操作文件夹或文件成功: " + src + " --> " + dst);
			return true;
		}
		catch (IllegalArgumentException e)
		{
			e.printStackTrace();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		return false;
	}

	public boolean delete(String src) throws Exception
	{
		Path p1 = new Path(src);
		if (hdfs.isDirectory(p1))
		{
			return hdfs.delete(p1, true);
		}
		else if (hdfs.isFile(p1))
		{
			return hdfs.delete(p1, false);
		}
		return true;
	}

	/**
	 * 查看文件是否存在
	 * 
	 * @throws Exception
	 */
	public boolean checkFileExist(String filename)
	{
		filename = modifiedHdfsPath(filename);
		try
		{
			Path f = new Path(filename);
			return hdfs.exists(f);
		}
		catch (org.apache.hadoop.ipc.RemoteException e)
		{
			if (e.getClassName().equals("org.apache.hadoop.ipc.StandbyException"))
			{
				ProcessStandby();
			}
		}
		catch (Exception e)
		{
			log.error(getTrace(e));
			//ReConnect();
			ProcessStandby();
		}
		return false;
	}

	/**
	 * 查看文件是否存在
	 * 
	 * @throws Exception
	 */
	public boolean checkStandbyException(String filename)
	{
		try
		{
			Path f = new Path(filename);
			hdfs.exists(f);
			return false;
		}
		catch (org.apache.hadoop.ipc.RemoteException e)
		{
			e.printStackTrace();
			if (e.getClassName().equals("org.apache.hadoop.ipc.StandbyException"))
			{
				return true;
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return true;
	}

	public boolean mkdir(String dirName)
	{
		dirName = modifiedHdfsPath(dirName);
		try
		{
			// FIXME 当要存在一个 文件 和要创建的 文件夹 名相同的时候，怎么处理？
			if (checkFileExist(dirName))
				return true;
			Path f = new Path(dirName);
			return hdfs.mkdirs(f);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

		return false;
	}

	/**
	 * 读取本地文件到HDFS系统<br>
	 * 请保证文件格式一直是UTF-8，从本地->HDFS
	 */
	public boolean getMergeST(String hdfsDir, String localDir, String filter, boolean Compress)
	{
		FileOutputStream os = null;
		GZIPOutputStream gfs = null;
		try
		{
			ArrayList<DatafileInfo> fileLst = listFiles(hdfsDir);
			if (fileLst.isEmpty())
				return false;
			makeDir(localDir);
			// deleteFile(localDir + "/" + destFileName);
			// File file = new File(localDir + "/" + destFileName);
			
			int fileSeq = 1;
			long nTotalLen = 0;
			String fileName = localDir + "/" + filter + "." + fileSeq;
			if (Compress)
			{
				gfs = new GZIPOutputStream(new FileOutputStream(fileName + ".gz"));
				os = null;
			}
			else
			{
				os = new FileOutputStream(fileName);
				gfs = null;
			}

			for (int i = 0; i < fileLst.size(); i++)
			{
				if (!fileLst.get(i).filename.contains(filter))
				{
					continue;
				}
				nTotalLen += fileLst.get(i).filesize;
				if (nTotalLen > 1024 * 1024 * 1024)
				{
					nTotalLen = 0;
					fileSeq++;
					if (gfs != null)
					{
						gfs.flush();
						gfs.close();
					}
					else
					{
						os.flush();
						os.close();
					}
					
					fileName = localDir + "/" + filter + "." + fileSeq;
					if (Compress)
					{
						gfs = new GZIPOutputStream(new FileOutputStream(fileName + ".gz"));
						os = null;
					}
					else
					{
						os = new FileOutputStream(fileName);
						gfs = null;
					}
				}

				String hdfsFilename = hdfsDir + "/" + fileLst.get(i).filename;
				Path f = new Path(hdfsFilename);
				FSDataInputStream dis = fs.open(f);

				byte[] buffer = new byte[1024*1024];
				int length = 0;
				long nTotalLength = 0;
				int nCount = 0;
				while ((length = dis.read(buffer)) > 0)
				{
					nCount++;
					if (gfs != null)
					{
						gfs.write(buffer, 0, length);
					}
					else
					{
						os.write(buffer, 0, length);
					}
					nTotalLength += length;

					/*
					 * if(nCount%100 == 0) { StringBuilder stringBuilder = new
					 * StringBuilder(); stringBuilder.append((new
					 * Date()).toLocaleString()); stringBuilder.append(
					 * ": Have move ");
					 * stringBuilder.append((nTotalLength/1024*1024));
					 * stringBuilder.append(" MB");
					 * log.info(stringBuilder.toString()); }
					 */
				}

				dis.close();
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}finally{
			try {
				if (gfs != null)
				{
					gfs.flush();
					gfs.close();
				}
				else
				{
					os.flush();
					os.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return true;
	}
	
	public ArrayList<DatafileInfo> getPathList(String[] paths) {
		ArrayList<DatafileInfo> fileList = new ArrayList<>();
		for (String strPath : paths) {
			try {
				ArrayList<DatafileInfo> listFiles = listFiles(strPath);
				for (int i = 0; i < listFiles.size(); i++) {
					fileList.add(listFiles.get(i));
				}
			} catch (Exception e) {
				log.error(e.toString());
			}
		}
		return fileList;
	}

	/**
	 * 读取本地文件到HDFS系统<br>
	 * 请保证文件格式一直是UTF-8，从本地->HDFS
	 */
	public boolean getMerge(String hdfsDir, String localDir, String filter, boolean Compress)
	{
		try
		{
			ArrayList<DatafileInfo> fileLst = null;
			String[] split = null;
			if(hdfsDir.contains(",")) {
				split = hdfsDir.substring(0, hdfsDir.length() - 1).split(",",-1);
			}else{
				split = hdfsDir.split(",",-1);
			}
			
			fileLst = getPathList(split);

			if (fileLst.isEmpty())
				return false;
			String localDirTmp = localDir + "_tmp";
			if(LocalFile.checkFileExist(localDir))
			{//如果localDir已经存在，就不要放到tmp目录了，否则后面改名会失败
				localDirTmp = localDir;
			}
			makeDir(localDirTmp);

			FileOutputStream os = null;
			GZIPOutputStream gfs = null;

			int fileSeq = 1;
			long nTotalLen = 0;

			List<TaskInfo> taskList = new ArrayList<TaskInfo>();
			List<String> fileList = new ArrayList<String>();

			for (int i = 0; i < fileLst.size(); i++)
			{
				nTotalLen += fileLst.get(i).filesize;
				if(hdfsDir.contains(",")) {
					fileList.add(fileLst.get(i).filePath);
				}else {
					fileList.add(hdfsDir + "/" + fileLst.get(i).filename);
				}
				if (nTotalLen > 1024*1024*1024)
				{
					TaskInfo ti = new TaskInfo();
					ti.fileLst = fileList;
					// ?
					if("SUCCESS".contains(filter)){
						ti.outputLocalName = localDirTmp + "/" + filter ;
					}else{
						ti.outputLocalName = localDirTmp + "/" + filter + "." + fileSeq;
					}					
					ti.Compress = Compress;
					taskList.add(ti);
					fileList = new ArrayList<String>();
					fileSeq++;
					nTotalLen = 0;
				}
			}

			if (nTotalLen >= 0)
			{
				TaskInfo ti = new TaskInfo();
				ti.fileLst = fileList;
				if(filter.contains("SUCCESS")){
					ti.outputLocalName = localDirTmp + "/" + filter ;
				}else{
					ti.outputLocalName = localDirTmp + "/" + filter + "." + fileSeq;
				}
				ti.Compress = Compress;
				taskList.add(ti);
			}

			// 创建一个线程池
			ExecutorService pool = Executors.newFixedThreadPool(5);
			// 创建多个有返回值的任务
			@SuppressWarnings("rawtypes")
			List<Future> list = new ArrayList<Future>();

			for (TaskInfo taskinfo : taskList)
			{
				Callable<?> fm = new FileDownloadCallable(taskinfo);

				// 执行任务并获取Future对象
				Future<?> f = pool.submit(fm);
				list.add(f);
			}
		
			// 关闭线程池
			pool.shutdown();

			// 获取所有并发任务的运行结果
			try
			{
				for (Future<?> f : list)
				{
					log.info(">>>" + f.get().toString());
				}
				if(!LocalFile.checkFileExists(localDir))
				{
					LocalFile.renameFile(localDirTmp, localDir);
				}
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}

		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

		return true;
	}
	
	@SuppressWarnings("rawtypes")
	public boolean getToLocal(String hdfsDir, String localDir, String filter){
		try
		{
			// BMY-S1U-RXDHxd-
			ArrayList<DatafileInfo> fileLst = listFiles(hdfsDir);
			if (fileLst.isEmpty())
				return false;
			makeDir(localDir);

			FileOutputStream os = null;

			long nTotalLen = 0;

			List<TaskInfo> taskList = new ArrayList<TaskInfo>();
			List<String> fileList = new ArrayList<String>();
			
			for (int i = 0; i < fileLst.size(); i++)
			{
				nTotalLen += fileLst.get(i).filesize;
				fileList.add(hdfsDir + "/" + fileLst.get(i).filename);
				if (nTotalLen > 1024 * 1000 * 1000 * 1000L)
				{
					TaskInfo ti = new TaskInfo();
					ti.fileLst = fileList;
					ti.outputLocalName = localDir + "/" + filter  + ".txt" ;
					taskList.add(ti);
					fileList = new ArrayList<String>();
					nTotalLen = 0;
				}
			}

			if (nTotalLen > 0)
			{
				TaskInfo ti = new TaskInfo();
				ti.fileLst = fileList;
				ti.outputLocalName = localDir + "/" + filter  + ".txt" ;
				taskList.add(ti);
			}

			// 创建一个线程池
			ExecutorService pool = Executors.newFixedThreadPool(5);
			// 创建多个有返回值的任务
			List<Future> list = new ArrayList<Future>();

			for (TaskInfo taskinfo : taskList)
			{
				Callable fm = new FileCallable(taskinfo);

				// 执行任务并获取Future对象
				@SuppressWarnings("unchecked")
				Future f = pool.submit(fm);
				list.add(f);
			}
			// 关闭线程池
			pool.shutdown();

			// 获取所有并发任务的运行结果
			try
			{
				for (Future f : list)
				{
					log.info(">>>>>>>>>>" + f.get().toString());
				}
			}
			catch (Exception e)
			{
				log.error(e.toString());
			}

		}
		catch (Exception e)
		{
			log.error(e.toString());
		}

		return true;
	}

	public FSDataOutputStream getOutputStream(String destFileName)
	{
		try
		{
			if (this.checkFileExist(destFileName))
			{
				delete(destFileName);
			}
			Path f = new Path(destFileName);
			FSDataOutputStream os = fs.create(f, true);
			return os;
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return null;
	}

	public FSDataInputStream getInputStream(String destFileName)
	{
		try
		{
			if (!this.checkFileExist(destFileName))
			{
				return null;
			}
			Path f = new Path(destFileName);

			FSDataInputStream is = fs.open(f);
			return is;
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 读取本地文件到HDFS系统<br>
	 * 请保证文件格式一直是UTF-8，从本地->HDFS
	 * @param localDirname
	 *            源文件所在位置
	 * @param hdfsPath 要放在服务器的位置
	 * @param destFileName 要合并成的文件名称
	 * @param filter
	 * @return
	 */
	public int putMerge(String localDirname, String hdfsPath, String destFileName, String filter, boolean bCompress)
	{
		try
		{						
			File dir = new File(localDirname);
			if (!dir.isDirectory())
			{
				return 0;
			}

			File[] files = dir.listFiles();
			if (files.length == 0)
				return 0;

			List<String> listFile = new ArrayList<String>();
			for (int i = 0; i < files.length; i++) 
			{
				listFile.add(files[i].getAbsolutePath());
			}
			return putMerge(listFile, hdfsPath, destFileName, filter,bCompress) ;
		}
		catch (Exception e)
		{
			log.info("putMerge error:" + "\r\n" + getTrace(e));
		}
		return 0;
	}
	
	
	/**
	 * 	zip 解压包含嵌套压缩文件上传hdfs
	 * @param files:zip源文件目录集合
	 * @param hdfsPath:hadoop远程所在存放目录
	 */
	public boolean putBigZip(List<String> files, String hdfsPath){
		FileInputStream fis = null;
		ZipInputStream zip = null;
		try {
			for (String str : files) {
				fis = new FileInputStream(str);
				zip = new ZipInputStream(fis);
				while(true){
					ZipEntry nextEntry = zip.getNextEntry();
			        if(nextEntry == null){
			        	break;
			        }
		        	byte ch[] = new byte[1024*1024]; 
		        	int i;			     
		        	mkdir(hdfsPath);
		        	
			        if(nextEntry.getName().endsWith(".zip")){
			        	ZipInputStream zip2 = new ZipInputStream(zip);
			        	ZipEntry nextEntry2 = zip2.getNextEntry();
						Path f = new Path(hdfsPath + "/" + nextEntry2.getName() + ".tmp");
						FSDataOutputStream os = fs.create(f, true);
			        	if(nextEntry2 == null){
			        		break;
			        	}
			        	while ((i = zip2.read(ch)) != -1){
			        		os.write(ch, 0, i);
			        		os.flush();
			        	}	     	
			        	os.close();
			        	delete(hdfsPath + "/" + nextEntry2.getName());
			        	if(!rename(hdfsPath + "/" + nextEntry2.getName() + ".tmp", hdfsPath + "/" + nextEntry2.getName())){
			        		log.info("文件重命名失败：" + nextEntry2.getName() + ".tmp");
			        	};
			        	zip2.closeEntry();
			        }
			        else
			        {
			        	delete(hdfsPath + "/" + nextEntry.getName());
						Path f = new Path(hdfsPath + "/" + nextEntry.getName() + ".tmp");
						FSDataOutputStream os = fs.create(f, true);
			        	if(nextEntry == null){
			        		break;
			        	}
			        	while ((i = zip.read(ch)) != -1){
			        		os.write(ch, 0, i);
			        		os.flush();
			        	}	     	
			        	os.close();
			        	if(!rename(hdfsPath + "/" + nextEntry.getName() + ".tmp", hdfsPath + "/" + nextEntry.getName())){
			        		log.info("文件重命名失败：" + nextEntry.getName() + ".tmp");
			        	};
			        }
				}
		        zip.close();
		        return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
			try {
				if(zip != null){
					zip.close();
				}
				if(fis != null){
					fis.close();
				}
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}finally {
			try {
				if(zip != null){
					zip.close();
				}
				if(fis != null){
					fis.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return false;
	}
	/**
	 * 读取本地文件到HDFS系统<br>
	 * 请保证文件格式一直是UTF-8，从本地->HDFS
	 * @param hdfsPath 要放在服务器的位置
	 * @param destFileName 要合并成的文件名称
	 * @param filter
	 * @return
	 */
	public int putMerge(List<String> files, String hdfsPath, String destFileName, String filter, boolean bCompress)
	{
		try
		{
			if(files.get(0).toLowerCase().endsWith(".zip"))
			{
				return putBigZip(files,hdfsPath) == true ? 1 : 0;
			}
			log.info("Begin move files to " + hdfsPath);
			if ((bCompress) && (!destFileName.endsWith(".gz"))) {
		        destFileName = destFileName + ".gz";
		    }
			if ((!bCompress) && (destFileName.endsWith(".gz"))) {
		        destFileName = destFileName + ".dat";
		    }
			int xCount=0;
			while (checkFileExist(hdfsPath + "/" + destFileName)
				  || checkFileExist(hdfsPath + "/" + destFileName + ".tmp"))
			{
				xCount++;
				if(xCount>20)
				{		
					return 2;
				}
				if(bCompress)
				{
					destFileName = destFileName.replace(".gz", "x.gz");
				}
				else
				{
					destFileName = destFileName.replace(".MRO", "x.MRO").replace(".MRE", "x.MRE").replace(".dat", ".datx");
				}
			}

			mkdir(hdfsPath);
			Path f = new Path(modifiedHdfsPath(hdfsPath + "/" + destFileName + ".tmp"));
			FSDataOutputStream os = fs.create(f, true);
			GZIPOutputStream gfs = null;
			if (bCompress) {
		        gfs = new GZIPOutputStream(os);
		      }
			byte[] buffer = new byte[10*1024*1024];

			for (int i = 0; i < files.size(); i++)
			{
                File file = new File(files.get(i));
			    try {
                    if (!file.exists())
                        continue;
                    if (!file.getName().toLowerCase().contains(filter.toLowerCase()))
                        continue;
                    FileInputStream is = new FileInputStream(file);
                    GZIPInputStream gis = null;
                    if (file.getName().toLowerCase().endsWith("gz")) {
                        gis = new GZIPInputStream(is);
                    }

                    while (true) {
                        int bytesRead = 0;
                        if (gis == null)
                            bytesRead = is.read(buffer);
                        else
                            bytesRead = gis.read(buffer, 0, buffer.length);
                        if (bytesRead >= 0) {
                            if (gfs != null) {
                                gfs.write(buffer, 0, bytesRead);
                            } else
                                os.write(buffer, 0, bytesRead);
                        } else {
                            break;
                        }
                    }
                    if (gis != null)
                        gis.close();
                    is.close();
                }catch (Exception e){
			        log.error("put File error ：" + file.getAbsolutePath() + e.toString());
			        continue;
                }
			}
			if(gfs != null)
			{
				gfs.close();
			}
			os.close();
			rename(hdfsPath + "/" + destFileName + ".tmp", hdfsPath + "/" + destFileName);
			return 1;
		}
		catch (Exception e)
		{
			log.info("put merge " + e.toString() + "\r\n" + getTrace(e)  );
		}
		log.warn("putMerge fail:" + destFileName);
		return 0;
	}

	public boolean hdfsMerge(String srcPath, String destPath, String destFileName, String filter, boolean bCompress)
	{
		try
		{
			ArrayList<DatafileInfo> fileLst = listFiles(srcPath);
			if (fileLst.isEmpty())
				return false;
	
			if(destFileName.isEmpty())
			{
				destFileName = new File(srcPath).getName() + ".txt";
			}
			if(checkFileExist(destPath + "/" + destFileName))
			{
				return false;
			}
			
			int fileSeq = 1;
			long nTotalLen = 0;

			List<String> files = new ArrayList<String>();

			for (int i = 0; i < fileLst.size(); i++)
			{
				files.add(srcPath + "/" + fileLst.get(i).filename);				
			}

			mkdir(destPath);
			LocalFile.deleteFile(destPath + "/" + destFileName + ".tmp");
			Path f = new Path(destPath + "/" + destFileName + ".tmp");
			
			FSDataOutputStream os = fs.create(f, true);
			GZIPOutputStream gfs = null;
			if (bCompress) {
		        gfs = new GZIPOutputStream(os);
		      }
			byte[] buffer = new byte[10*1024*1024];

			for (int i = 0; i < files.size(); i++)
			{
				String hdfsFilename = files.get(i);
				Path file = new Path(hdfsFilename);
				FSDataInputStream dis = fs.open(file);
				InputStream is = dis;
				GZIPInputStream gis = null;
				if(hdfsFilename.toLowerCase().endsWith(".gz"))
				{
					gis = new GZIPInputStream(dis);
					is  = gis;
				}

				while (true)
				{
					int bytesRead = 0;
					if (gis == null)
						bytesRead = is.read(buffer);
					else
						bytesRead = gis.read(buffer, 0, buffer.length);
					if (bytesRead >= 0)
					{
						if(gfs != null)
						{
							gfs.write(buffer, 0, bytesRead);
						}
						else
							os.write(buffer, 0, bytesRead);
					}
					else
					{
						break;
					}
				}
				if (gis != null)
					gis.close();
				is.close();
			}
			if(gfs != null)
			{
				gfs.close();
			}
			os.close();
			rename(destPath + "/" + destFileName + ".tmp", destPath + "/" + destFileName);
			return true;
		}
		catch (Exception e)
		{
			e.printStackTrace();
			log.info(e.getStackTrace());
		}
		return false;
	}
	
	private boolean rename(String src, String dst) throws IllegalArgumentException, IOException {
		return hdfs.rename(new Path(src), new Path(dst));		
	}

	public boolean putMerge(List<String> files, String hdfsPath, String destFileName, String filter, String srcCommonPah, String XdrBkPath, int totalNum, int dealNum)
	{
		File file = null;
		FSDataOutputStream os = null;
		FileInputStream is = null;
		GZIPInputStream gis = null;
		try
		{
			log.info("Begin move files to " + hdfsPath);

			while (checkFileExist(hdfsPath + "/" + destFileName))
			{
				if (destFileName.contains(".x"))
					destFileName += "x";
				else
					destFileName += ".x";
			}

			mkdir(hdfsPath);
			Path f = new Path(hdfsPath + "/" + destFileName);
			os = fs.create(f, true);
			byte[] buffer = new byte[10*1024*1024];

			for (int i = 0; i < files.size(); i++)
			{
				try
				{
					file = new File(files.get(i));
					if (!file.exists())
						continue;
					if (!file.getName().toLowerCase().contains(filter.toLowerCase()))
						continue;
					is = new FileInputStream(file);
					gis = null;
					if (file.getName().toLowerCase().endsWith("gz"))
						gis = new GZIPInputStream(is);

					while (true)
					{
						int bytesRead = 0;
						if (gis == null)
							bytesRead = is.read(buffer);
						else
							bytesRead = gis.read(buffer, 0, buffer.length);
						if (bytesRead >= 0)
						{
							os.write(buffer, 0, bytesRead);
						}
						else
						{
							break;
						}
					}
					log.info("上传文件" + file.getAbsolutePath() + "完成！");
					if (gis != null)
						gis.close();
					is.close();
					dealNum++;
					if (dealNum % 100 == 0)
					{
						log.info("MME和HTTP文件个数总共" + totalNum + "个，已上传完成" + dealNum + "个!");
					}
					if (XdrBkPath.length() > 0)
					{
						log.info("备份文件" + file.getAbsolutePath() + (LocalFile.bkFile(file.getAbsolutePath(), file.getAbsolutePath().replace(srcCommonPah, XdrBkPath)) ? "成功" : "失败"));
					}
					else
					{
						log.info("删除文件" + file.getAbsolutePath() + (file.delete() ? "成功" : "失败"));
					}
				}
				catch (Exception e)
				{
					e.printStackTrace();
					if (gis != null)
						gis.close();
					is.close();
					log.info("删除文件" + file.getAbsolutePath() + (file.delete() ? "成功" : "失败"));
					continue;
				}
			}
			os.close();
			return true;
		}
		catch (Exception e)
		{
			log.info(e.getStackTrace());
		}
		finally
		{
			try
			{
				if (gis != null)
					gis.close();
				is.close();
				os.close();
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		}
		return false;
	}

	public boolean copyDirToHdfs(String localPath, String hdfsPath, boolean bOverRrite)
	{
		try
		{
			File root = new File(localPath);
			File[] files = root.listFiles();

			for (File file : files)
			{
				if (file.isFile())
				{
					if (!copyFileToHdfs(file.getPath().toString(), hdfsPath, bOverRrite))
					{
					}
				}
				else if (file.isDirectory())
				{
					copyDirToHdfs(localPath + "/" + file.getName(), hdfsPath + "/" + file.getName(), bOverRrite);
				}
			}
			return true;
		}
		catch (Exception e)
		{
			log.info(e.getStackTrace());
		}
		return false;
	}

	public List<DownloadAndUploadTask> copyDirToHdfsFuture(String localPath, String hdfsPath, boolean needOverride, ExecutorService pool) {
		List<DownloadAndUploadTask> ret = new ArrayList<>();
		try
		{
			File root = new File(localPath);
			File[] files = root.listFiles();
			for (File file : files)
			{
				if (file.isFile())
				{
					ret.add(copyFileToHdfsFuture(file.getPath().toString(), hdfsPath, needOverride, pool));
				}
				else if (file.isDirectory())
				{
					ret.addAll(copyDirToHdfsFuture(localPath + "/" + file.getName(), hdfsPath + "/" + file.getName(), needOverride, pool));
				}
			}
		}
		catch (Exception e)
		{
			log.info(e.getStackTrace());
		}
		return ret;
	}
	
	/**
	 * 创建一个空文件
	 * 
	 * @param filename
	 * @return
	 */
	public boolean createEmptyFile(String filename)
	{
		filename = modifiedHdfsPath(filename);
		try
		{
			Path f = new Path(filename);
			FSDataOutputStream os = fs.create(f, true);
			os.close();
			return true;
		}
		catch (IllegalArgumentException e)
		{
			e.printStackTrace();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 读取本地文件到HDFS系统<br>
	 * 请保证文件格式一直是UTF-8，从本地->HDFS
	 */
	public boolean unzipFileToHDFS(String localFilename, String hdfsPath)
	{
		try
		{
			log.info("Begin move " + localFilename + " to " + hdfsPath);
			int pos = hdfsPath.lastIndexOf("/");
			if (pos < 0)
				return false;
			String path = hdfsPath.substring(0, pos);
			mkdir(path);

			File file = new File(localFilename);
			FileInputStream is = new FileInputStream(file);
			String finalName = file.getName().replace(".gz", "");

			while (checkFileExist(path + "/" + finalName))
			{
				if (finalName.contains(".x"))
					finalName += "x";
				else
					finalName += ".x";
			}

			Path f = new Path(path + "/" + finalName);

			FSDataOutputStream os = fs.create(f, false);
			GZIPInputStream gis = null;
			if (file.getName().toLowerCase().contains(".gz"))
				gis = new GZIPInputStream(is);

			byte[] buffer = new byte[10*1024*1024];
			int nCount = 0;
			while (true)
			{
				int bytesRead = 0;
				try
				{
					if (gis != null)
						bytesRead = gis.read(buffer);
					else
						bytesRead = is.read(buffer);
				}
				catch (Exception e)
				{
					log.info(e.getMessage());
					bytesRead = -1;
				}
				if (bytesRead >= 0)
				{
					os.write(buffer, 0, bytesRead);
					nCount++;
					// if(nCount%(100) == 0)
					// log.info("Have move " + nCount + " blocks");
				}
				else
				{
					break;
				}
			}

			is.close();
			os.close();
			if (gis != null)
				gis.close();
			log.info("Write content of file " + file.getName() + " to hdfs file " + f.getName() + " success");

			return true;
		}
		catch (Exception e)
		{
			log.info(e.getMessage());
		}
		return false;
	}

	/**
	 * 读取本地文件到HDFS系统<br>
	 * 请保证文件格式一直是UTF-8，从本地->HDFS
	 */
	public boolean copyFileToHdfs(String localFilename, String hdfsPath, boolean bOverRrite)
	{
		try
		{
			log.info("Begin move " + localFilename + " to " + hdfsPath);
			mkdir(hdfsPath);

			File file = new File(localFilename);
			FileInputStream is = new FileInputStream(file);

			if (this.checkFileExist(hdfsPath + "/" + file.getName()))
			{
				if (!bOverRrite) {
					is.close();
					return true;
				} else {
					delete(hdfsPath + "/" + file.getName());
				}
			}

			Path f = new Path(hdfsPath + "/" + file.getName());

			FSDataOutputStream os = fs.create(f, false);
			byte[] buffer = new byte[10*1024*1024];
			int nCount = 0;
			while (true)
			{
				int bytesRead = is.read(buffer);
				if (bytesRead >= 0)
				{
					os.write(buffer, 0, bytesRead);
					nCount++;
					// if (nCount % (100) == 0)
					// log.info("Have move " + nCount + " blocks");
				}
				else
				{
					break;
				}
			}

			is.close();
			os.close();
			log.info("Write content of file " + file.getName() + " to hdfs file " + f.getName() + " success");
			return true;
		}
		catch (Exception e)
		{
			log.info(e.getMessage());
			log.info(e.getMessage());
		}
		return false;
	}
	
	public DownloadAndUploadTask copyFileToHdfsFuture(final String localFileName, final String hdfsPath,
			final boolean needOverride, ExecutorService pool) {
		
		DownloadAndUploadTask task = new DownloadAndUploadTask() {
			@Override
			protected void before() {
				File file;
				long fileSize = 0;
				String targetFilePath = null;
				FileInputStream is = null;
				FSDataOutputStream os = null;

				file = new File(localFileName);
				targetFilePath = modifiedHdfsPath(hdfsPath + "/" + file.getName());
				fileSize = file.length();
				mkdir(hdfsPath);
				try {
					is = new FileInputStream(file);
					if (checkFileExist(targetFilePath)) {
						if (!needOverride) {
							is.close();
							log.info(targetFilePath + "已存在！");
							return;
						} else {
							delete(targetFilePath);
						}
					}
				} catch (Exception ignored) {
				}

				Path f = new Path(targetFilePath);
				try {
					os = fs.create(f, false);
				} catch (IOException ignored) {
				}
				setParameter(is, os, targetFilePath.replaceAll(HADOOP_URL, ""), fileSize, TaskType.UploadFile);
			}
		};
		if (pool!=null) {
			pool.submit(task);
		} else
			new Thread(task).start();

		return task;
	}
	
	public boolean copyFileToHdfsTmp(String localFilename, String hdfsPath)
	{
		try
		{
			log.info("Begin move " + localFilename + " to " + hdfsPath);
			boolean mkdir = mkdir(hdfsPath);
			if(!mkdir){
				return false;
			}

			File file = new File(localFilename);
			FileInputStream is = new FileInputStream(file);

			Path f = new Path(hdfsPath + "/" + file.getName()+".tmp");

			FSDataOutputStream os = fs.create(f, false);
			byte[] buffer = new byte[10*1024*1024];
			int nCount = 0;
			while (true)
			{
				int bytesRead = is.read(buffer);
				if (bytesRead >= 0)
				{
					os.write(buffer, 0, bytesRead);
					nCount++;
				}
				else
				{
					break;
				}
			}

			is.close();
			os.close();
			log.info("Write content of file " + file.getName() + " to hdfs file " + f.getName() + " success");
			delete(hdfsPath + "/" + file.getName());
			rename(hdfsPath + "/" + file.getName()+".tmp", hdfsPath + "/" + file.getName());
			return true;
		}
		catch (Exception e)
		{
			log.info(e.getMessage());
			log.info(e.getMessage());
		}
		return false;
	}
	
	public boolean copyFileToHdfsTmps(TarInputStream iStream, String hdfsPath,File file)
	{
		try
		{
			log.info("Begin move " + file.getPath() + " to " + hdfsPath);
			boolean mkdir = mkdir(hdfsPath);
			if(!mkdir){
				return false;
			}
			
			Path f = new Path(hdfsPath + "/" + file.getName()+".tmp");
			if(hdfs.exists(f)){
				hdfs.delete(f,false);
			}
			
			FSDataOutputStream os = fs.create(f, false);
			byte[] buffer = new byte[10*1024*1024];
			int nCount = 0;
			while (true)
			{
				int bytesRead = iStream.read(buffer);
				if (bytesRead >= 0)
				{
					os.write(buffer, 0, bytesRead);
					nCount++;
				}
				else
				{
					break;
				}
			}
			
			os.close();
			log.info("Write content of file " + file.getName() + " to hdfs file " + f.getName() + " success");
			log.info("删除文件和重命名文件："+file.getName());
			delete(hdfsPath + "/" + file.getName());
			rename(hdfsPath + "/" + file.getName()+".tmp", hdfsPath + "/" + file.getName());
			return true;
		}
		catch (Exception e)
		{
			log.info(e.getMessage());
			log.info(e.getMessage());
		}
		return false;
	}
	
	/**
	 * tar包文件解压
	 * @param localPath
	 * @return
	 * @throws Exception
	 */
	public int tarPackageCompress(String localPath) throws Exception{
		FSDataInputStream fsInputStream = fs.open(new Path(localPath));
		TarInputStream in = new TarInputStream(fsInputStream);
		TarEntry entry = null;
		File file = new File(localPath);
		int flagCount = 0;
		String name = null;
		while ((entry = in.getNextEntry()) != null) {
			try
			{
				if (entry.isDirectory()) {
					continue;
				}
				name = new File(entry.getName()).getName();
				String hdfsPath = file.getParent() + "/" + name + ".tmp";
				FSDataOutputStream fsOutputStream = fs.create(new Path(hdfsPath), true);
				byte[] buffer = new byte[10*1024*1024];
				int nCount = 0;
				while (true)
				{
					int bytesRead = in.read(buffer);
					if (bytesRead >= 0)
					{
						fsOutputStream.write(buffer, 0, bytesRead);
						nCount++;
					}
					else
					{
						break;
					}
				}
				fsOutputStream.close();
				hdfs.rename(new Path(hdfsPath), new Path(hdfsPath.replace(".tmp", "")));
			}
			catch (IOException e)
			{	
				flagCount++;
				log.error("File error :" + name);
				log.error("tarPackageCompress error " + HadoopFSOperations.getTrace(e));
				continue;
			}
		}
		in.close();
		fsInputStream.close();
		return flagCount;	
	}
	
	/**
	 * 读取本地文件到HDFS系统<br>
	 * 请保证文件格式一直是UTF-8，从本地->HDFS
	 */
	public boolean uncompressFileToHDFS(String localFilename, String hdfsPath, boolean bOverRrite)
	{
		hdfsPath = modifiedHdfsPath(hdfsPath);
		try
		{
			log.info("Begin move " + localFilename + " to " + hdfsPath);
			mkdir(hdfsPath);

			File file = new File(localFilename);
			FileInputStream is = new FileInputStream(file);
			GZIPInputStream gis = null;
			if(localFilename.toLowerCase().endsWith(".gz"))
			{
				gis = new GZIPInputStream(is);
			}

			String hdfsFilePath = modifiedHdfsPath(hdfsPath + "/" + file.getName());
			if (this.checkFileExist(hdfsFilePath))
			{
				if (!bOverRrite) {
					is.close();
					return true;
				} else {
					delete(hdfsFilePath);
				}
			}

			Path f = null;
			if(localFilename.toLowerCase().endsWith(".gz"))
			{
				f = new Path(hdfsFilePath + ".txt");
			}
			else
			{
				f = new Path(hdfsFilePath);
			}

			FSDataOutputStream os = fs.create(f, false);
			byte[] buffer = new byte[10*1024*1024];
			int nCount = 0;
			while (true)
			{
				int bytesRead = -1;
				if(gis != null)
					bytesRead= gis.read(buffer);
				else
					bytesRead= is.read(buffer);
				if (bytesRead >= 0)
				{
					os.write(buffer, 0, bytesRead);
					nCount++;
					// if (nCount % (100) == 0)
					// log.info("Have move " + nCount + " blocks");
				}
				else
				{
					break;
				}
			}

			is.close();
			if(gis !=null)
			{
				gis.close();
			}
			os.close();
			log.info(" Write content of file " + file.getName() + " to hdfs file " + f.getName() + " success");

			return true;
		}
		catch (Exception e)
		{
			log.info(e.getMessage());
		}
		return false;
	}
	
	public boolean compressFileToHDFS(String localFilename, String hdfsPath, boolean bOverRrite)
	{
		try
		{
			log.info("Begin move " + localFilename + " to " + hdfsPath);
			mkdir(hdfsPath);

			File file = new File(localFilename);
			FileInputStream is = new FileInputStream(file);
			GZIPInputStream gis = null;
			if(localFilename.toLowerCase().endsWith(".gz"))
			{
				gis = new GZIPInputStream(is);
			}

			if (this.checkFileExist(hdfsPath + "/" + file.getName()))
			{
				if (!bOverRrite) {
					is.close();
					return true;
				} else {
					delete(hdfsPath + "/" + file.getName());
				}
			}

			Path f = null;
			if(localFilename.toLowerCase().endsWith(".gz"))
			{
				f = new Path(hdfsPath + "/" + file.getName() + ".txt");
			}
			else
			{
				f = new Path(hdfsPath + "/" + file.getName());
			}

			FSDataOutputStream os = fs.create(f, false);
			byte[] buffer = new byte[10*1024*1024];
			int nCount = 0;
			while (true)
			{
				int bytesRead = -1;
				if(gis != null)
					bytesRead= gis.read(buffer);
				else
					bytesRead= is.read(buffer);
				if (bytesRead >= 0)
				{
					os.write(buffer, 0, bytesRead);
					nCount++;
					// if (nCount % (100) == 0)
					// log.info(" Have move " + nCount + " blocks");
				}
				else
				{
					break;
				}
			}

			is.close();
			if(gis !=null)
			{
				gis.close();
			}
			os.close();
			log.info(" Write content of file " + file.getName() + " to hdfs file " + f.getName() + " success");

			return true;
		}
		catch (Exception e)
		{
			log.info(e.getMessage());
			log.info(e.getMessage());
		}
		return false;
	}

	/**
	 * 取得文件块所在的位置..
	 */
	public void getLocation()
	{
		try
		{
			Path f = new Path("/user/xxx/input02/file01");
			FileStatus fileStatus = fs.getFileStatus(f);

			BlockLocation[] blkLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
			for (BlockLocation currentLocation : blkLocations)
			{
				String[] hosts = currentLocation.getHosts();
				for (String host : hosts)
				{
					log.info(host);
				}
			}

			// 取得最后修改时间
			long modifyTime = fileStatus.getModificationTime();
			Date d = new Date(modifyTime);
			log.info(d);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public static boolean makeDir(String dirName)
	{
		File file = new File(dirName);
		// 如果文件夹不存在则创建
		if (!file.exists() && !file.isDirectory())
		{
			// log.info("//不存在");
			file.mkdirs();
		}
		return true;
	}

	public boolean downloadHdfsDir(String hdfsDir, String ftpDir, String filter, FTPClientHelper ftp)
	{
		if (ftp == null)
		{
			return readHdfsDirToLocal(hdfsDir, ftpDir, filter);
		}
		return readHdfsDirToftp(hdfsDir, ftpDir, filter, ftp);
	}

	/**
	 * 将hdfs上的文件读到本地
	 * 
	 * @param hdfsDir
	 * @param localDir
	 * @param filter
	 * @return
	 */
	public boolean readHdfsDirToLocal(String hdfsDir, String localDir, String filter)
	{
		final String targetHdfsDirPath = modifiedHdfsPath(hdfsDir);
		try
		{
			ArrayList<String> fileLst = listAllFiles(targetHdfsDirPath);
			if (fileLst.size() == 0)
			{
				return false;
			}
			
			Path path = new Path(targetHdfsDirPath);
			downFileName = path.getName();
			long maxlength = hdfs.getContentSummary(path).getLength();
			for (int i = 0; i < fileLst.size(); i++)
			{
				final CalendarEx cal = new CalendarEx(new Date());

				if (!fileLst.get(i).contains(filter))
				{
					continue;
				}

				/*
				 * if(fileLst.get(i).modificationTime/1000 + 360 >cal._second) {
				 * continue; }
				 */
//				makeDir(localDir);
//				String hdfsFilename = hdfsDir + "/" + fileLst.get(i).getName();
//				String localFilename = localDir + "/" + fileLst.get(i).getName();
				String hdfsFilename = fileLst.get(i);
				String[] str = hdfsFilename.split("/");
				String dir = hdfsFilename.replace("/"+str[str.length-1], "").replace(targetHdfsDirPath, "");//得到最底层目录
				String fileName = hdfsFilename.replace(targetHdfsDirPath, "");
				
				String localdir = localDir + dir;
				makeDir(localdir);
				String localFilename = localDir + fileName;
				LocalFile.deleteFile(localFilename);

				// FIXME ?
				if (!readFileFromHdfs(hdfsFilename, localdir, maxlength))// 读文件到本地
				{
				}
			}
		}
		catch (Exception e)
		{
			init_clear();
			e.printStackTrace();
		}

		return true;
	}

	/**
	 * Future模式下载文件夹
	 * @param hdfsDir hdfs文件夹路径
	 * @param localDir 本地文件夹路径
	 * @param filter 过滤器
	 * @param pool 线程池
	 * @return
	 */
	public List<DownloadAndUploadTask> readHdfsDirToLocalFuture(String hdfsDir, String localDir, String filter, ExecutorService pool)
	{
		hdfsDir = modifiedHdfsPath(hdfsDir);
		List<DownloadAndUploadTask> ret = new ArrayList<>();
		try
		{
			ArrayList<String> fileLst = listAllFiles(hdfsDir);
			
			Path path = new Path(hdfsDir);
			downFileName = path.getName();
			long maxlength = hdfs.getContentSummary(path).getLength();
			for (int i = 0; i < fileLst.size(); i++)
			{
				final CalendarEx cal = new CalendarEx(new Date());

				if (!fileLst.get(i).contains(filter))
				{
					continue;
				}
				
				String hdfsFilename = fileLst.get(i);
				String fileName = hdfsFilename.replace(hdfsDir, "");
				
				String localFilePath = localDir + File.separator + downFileName + fileName;
				FileUtils.touch(new File(localFilePath));
				ret.add(readFileFromHdfsFuture(hdfsFilename, localFilePath, maxlength, pool));
			}
		}
		catch (Exception e)
		{
			init_clear();
			e.printStackTrace();
		}
		return ret;
	}
	
	public void init_clear()
	{
		downFileName = "";
		allfileList.clear();
	}

	public boolean readHdfsDirToftp(String hdfsDir, String ftpDir, String filter, FTPClientHelper ftp)
	{
		try
		{
			ArrayList<DatafileInfo> fileLst = listFiles(hdfsDir);
			if (fileLst.size() == 0)
				return false;
			for (int i = 0; i < fileLst.size(); i++)
			{
				final CalendarEx cal = new CalendarEx(new Date());

				if (!fileLst.get(i).filename.contains(filter))
				{
					continue;
				}

				/*
				 * if(fileLst.get(i).modificationTime/1000 + 360 >cal._second) {
				 * continue; }
				 */
				
				ftp.mkdir(ftpDir);
				String hdfsFilename = hdfsDir + "/" + fileLst.get(i).filename;
				String ftpFilename = ftpDir + "/" + fileLst.get(i).filename;
				ftp.delete(ftpFilename);

				if (!ftp.putFromHdfs(ftpFilename, hdfsFilename, this,true))// 读文件到ftp
				{
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

		return true;
	}
	
	public boolean readHdfsDirToftp(String hdfsDir, String ftpDir, String filter ,String ftpStr,int nMaxThreadNum)
	{
		try
		{		
			String[] split = null;
			if(hdfsDir.contains(",")) {
				split = hdfsDir.substring(0, hdfsDir.length() - 1).split(",",-1);
			}else{
				split = hdfsDir.split(",",-1);
			}
			ArrayList<DatafileInfo> fileLst = getPathList(split);

			if (fileLst.size() == 0)
				return false;			
			groupTask(hdfsDir, ftpDir, filter, nMaxThreadNum, ftpStr, fileLst , "");
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

		return true;
	}
	
	public boolean readODSHdfsDirToftp(String ftpDir , String ftpStr,int nMaxThreadNum ,ArrayList<DatafileInfo> fileLst){	
		return groupTask("", ftpDir, "", nMaxThreadNum, ftpStr, fileLst, "ODS");
	}

	public boolean groupTask(String hdfsDir, String ftpDir, String filter, int nMaxThreadNum, String ftpStr,
			ArrayList<DatafileInfo> fileLst ,String ods) {
		String[] split = ftpStr.split(",");
		TaskInfoFtp[] task = new TaskInfoFtp[nMaxThreadNum];
		for(int y =0 ; y < nMaxThreadNum ; y ++){
			try {
				task[y] = new TaskInfoFtp(split[0],Integer.parseInt(split[1]),split[2],split[3]);				
				task[y].ftp.setBinaryTransfer(true);
				task[y].ftp.setPassiveMode(Boolean.parseBoolean(split[4]));
				task[y].ftp.setEncoding("utf-8");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		int nNum = 0;
		for (int i = 0; i < fileLst.size(); i++)
		{
			
			final CalendarEx cal = new CalendarEx(new Date());

			if (!fileLst.get(i).filename.contains(filter))
			{
				continue;
			}
			String hdfsFilename = null ;
			String ftpFilename = null ;
			if("ODS".equals(ods)){
				hdfsFilename = fileLst.get(i).filePath;
			}else if(hdfsDir.contains(",")) {
				hdfsFilename = fileLst.get(i).filePath;
			}else{
				hdfsFilename = hdfsDir + "/" + fileLst.get(i).filename;
			}
			if(ftpDir.contains("_H")){
				File file = new File(fileLst.get(i).filePath);
				File file2 = new File(file.getParent());
				ftpFilename = ftpDir + "/" + file2.getName() +"/"+ fileLst.get(i).filename;
			}else{
				ftpFilename = ftpDir + "/" + fileLst.get(i).filename;
			}
			// ch添加分组				 
			nNum++ ;
			task[nNum % nMaxThreadNum ].hdfsFileList.add(hdfsFilename);
			task[nNum % nMaxThreadNum ].ftpFileList.add(ftpFilename); 				
		}
		return mkdirTheadPool(task,nMaxThreadNum);
	}
	
	@SuppressWarnings("rawtypes")
	public boolean mkdirTheadPool(TaskInfoFtp[] task,int nMaxThreadNum){
		ExecutorService pool = Executors.newFixedThreadPool(nMaxThreadNum);
		TaskInfoFtp tmpTask = null;
		String tmpFilePath = null;
		List<Future> futureList = new ArrayList<Future>();
		for (int i = 0; i < task.length; i++) {	
			tmpTask = task[i];
			if(tmpTask.ftpFileList.size() > 0 && tmpFilePath == null) {
				tmpFilePath = tmpTask.ftpFileList.get(0);
			}
			Callable hdfsToFtpCallable = new HdfsToFtpCallable(task[i]);
			@SuppressWarnings("unchecked")
			Future f = pool.submit(hdfsToFtpCallable);
			futureList.add(f);
		}
		int count = 0;
		try {
			for (Future f : futureList) {
				Object obj = f.get();
				log.info(obj.toString());
				if(!obj.toString().equals("sucessfully")){
					count ++ ;
				}
			}		
			File file = new File(tmpFilePath);
			String name = file.getParent().replace("\\", "/");
			tmpTask.ftp.rename( name +"_tmp", name);
			futureList.clear();
			// 关闭线程池
			pool.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if(count > 0){
			return false;
		}else{
			return true;
		}
		 
	}
	
	class HdfsToFtpCallable implements Callable<Object>{
		TaskInfoFtp taskInfoFtp = null;
		
		public HdfsToFtpCallable(TaskInfoFtp taskInfoFtp){
			this.taskInfoFtp = taskInfoFtp;
		}

		@Override
		public Object call() throws Exception {
			List<String> hdfsFileList = taskInfoFtp.hdfsFileList;
			List<String> ftpFileList = taskInfoFtp.ftpFileList;
			FTPClientHelper ftp = taskInfoFtp.ftp;
			
			String strFtp = null ;
			String strHdfs = null;
			int count = 0;
			for(int i=0 ; i < hdfsFileList.size() ; i++){		
				int retryTimes = 0;
				while(retryTimes<3)
				{
					retryTimes++;
					try {						
						strFtp = ftpFileList.get(i);
						strHdfs = hdfsFileList.get(i);
						File file = new File(strFtp);
						String fileName = file.getName();
						String tmpDir = file.getParent() + "_tmp/";
						String tmpFilePath = tmpDir + fileName;
						ftp.mkdirs(tmpDir,null,false);
						if(strHdfs.contains("SUCCESS.txt")){
							ftp.delete(tmpFilePath,false);
						}else{
							if(!ftp.delete(tmpFilePath + ".gz",false)){
//									log.error("文件删除失败： " + strFtp + ".gz");
							};
						}												
						if(!workOneHdfsToFtp(tmpFilePath,strHdfs,ftp)){
							log.error("文件上传失败：" + tmpFilePath);
							count ++ ;
						}else{
							break;
						};		
					} catch (Exception e) {
						count ++ ;
						Thread.sleep(20000);
					}finally {
						ftp.disconnect();
					}
				}
			}
			if(ftp.getFTPClient().isConnected()){
				ftp.disconnect();
			}			
			if(count > 0){
				return "failure ";
			}else{
				return "sucessfully";
			}			
		}
		
	}
	
	public boolean workOneHdfsToFtp(String ftpFilename,String hdfsFilename,FTPClientHelper ftp){
		try {
			return ftp.putFromHdfs(ftpFilename, hdfsFilename, this,true);// 读文件到ftp
		} catch (Exception e) {
			log.error("文件上传失败" + hdfsFilename + "  " + e.toString());
		}
		return false;
	}
	
	public boolean putHdfsDirFromftp(String hdfsDir, String ftpDir, String filter, FTPClientHelper ftp)
	{
		try
		{
			ArrayList<DatafileInfo> fileLst = listFiles(hdfsDir);
			if (fileLst.size() == 0)
				return false;
			for (int i = 0; i < fileLst.size(); i++)
			{
				final CalendarEx cal = new CalendarEx(new Date());

				if (!fileLst.get(i).filename.contains(filter))
				{
					continue;
				}

				/*
				 * if(fileLst.get(i).modificationTime/1000 + 360 >cal._second) {
				 * continue; }
				 */
				ftp.mkdir(ftpDir);
				String hdfsFilename = hdfsDir + "/" + fileLst.get(i).filename;
				String ftpFilename = ftpDir + "/" + fileLst.get(i).filename;
				ftp.delete(ftpFilename);

				if (!ftp.putFromHdfs(ftpFilename, hdfsFilename, this,true))// 读文件到ftp
				{
				}
			}
		}
		catch (FileNotFoundException e)
		{
			e.printStackTrace();
		}
		catch (IllegalArgumentException e)
		{
			e.printStackTrace();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

		return true;
	}

	/**
	 * 读取hdfs中的文件内容到本地
	 */
	@SuppressWarnings("static-access")
	public boolean readFileFromHdfs(String hdfsFilename, String localPath, long nMaxSize)
	{
		hdfsFilename = modifiedHdfsPath(hdfsFilename);
		try
		{
			HadoopFSOperations fsOperations = new HadoopFSOperations();
			Path f = new Path(hdfsFilename);
			FSDataInputStream dis = fsOperations.fs.open(f);
			File file = new File(localPath + "/" + f.getName());
			FileOutputStream os = new FileOutputStream(file);

			byte[] buffer = new byte[1024*1024];
			int length = 0;
			int lastProcess = -1;
			int nCount =0;
			long nTotalLength = 0;
			while ((length = dis.read(buffer)) > 0)
			{
				os.write(buffer, 0, length);
				nTotalLength += length;
				// 只下载 nMaxSize 大小的文件
				if (nMaxSize > 0 && nTotalLength > nMaxSize)// ?
					break;
				nCount++;
				/*int a = (int) (nTotalLength *100.0 / nMaxSize);
				if (a%5 == 0 && a!=lastProcess)
				{
					log.info(downFileName+" 已下载: "+a+"%");
				}
				lastProcess = a;
				*/

				// 每传 100*length大小的数据就打印一次日志
				if (nCount % 100 == 0) 
				{
					StringBuilder stringBuilder = new StringBuilder();
					stringBuilder.append(": Have move ");
					stringBuilder.append((nTotalLength / 1024*1024));
					stringBuilder.append(" MB, "  + f.getName());
					log.info(stringBuilder.toString());
				}				 
			}

			os.close();
			dis.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * Future 模式下载
	 * @param hdfsFilename hdfs文件路径
	 * @param localFilePath 本地文件路径
	 * @param nMaxSize 要下载这个文件多少字节
	 * @param pool 线程池
	 * @return
	 */
	public DownloadAndUploadTask readFileFromHdfsFuture(final String hdfsFilename, final String localFilePath, final long nMaxSize, ExecutorService pool)
	{
		DownloadAndUploadTask task = new DownloadAndUploadTask() {
			@Override
			protected void before() {

				final String targetHdfsFilePath = modifiedHdfsPath(hdfsFilename);
				Path f = new Path(targetHdfsFilePath);
				FSDataInputStream dis = null;
				FileOutputStream os = null;
				try {
					dis = fs.open(f);
					File file = new File(localFilePath);
					os = new FileOutputStream(file);
				} catch (Exception ignored) {
					log.error(ignored.getMessage());
				}
				setParameter(dis, os, targetHdfsFilePath.replaceAll(HADOOP_URL, ""), nMaxSize, TaskType.DownloadFile);
			}
		};

		if (pool != null) {
			pool.submit(task);
		} else
			new Thread(task).start();
		return task;
	}
	
	/**
	 * Future模式下载5兆文件
	 * @param hdfsFilename hdfs文件地址
	 * @param localFilePath 本地文件地址
	 * @param pool 线程池
	 * @return
	 */
	public DownloadAndUploadTask read5MFileFromHdfsFuture(final String hdfsFilename, final String localFilePath, ExecutorService pool)
	{
		final String targetHdfsFilePath = modifiedHdfsPath(hdfsFilename);
		Path f = new Path(targetHdfsFilePath);
		FSDataInputStream dis = null;
		FileOutputStream os = null;
		try {
			dis = fs.open(f);
			File file = new File(localFilePath);
			os = new FileOutputStream(file);
		} catch (Exception e) {
			e.printStackTrace();
		}

		DownloadAndUploadTask task = new DownloadAndUploadTask(dis, os, targetHdfsFilePath.replaceAll(HADOOP_URL, ""), 5*1024*1024, TaskType.Download5M);

		if (pool != null) {
			pool.submit(task);
		} else
			new Thread(task).start();
		return task;
	}
	
	/**
	 * 得到文件夹下的文件名
	 */
	public ArrayList<String> allfilenameList = new ArrayList<String>();
	public ArrayList<String> listAllFilesName(String path) throws FileNotFoundException, IllegalArgumentException, IOException
	{
		FileStatus fileStatus[] = fs.listStatus(new Path(path));
		if (fileStatus!=null)
		{
			for (int i = 0; i < fileStatus.length; i++)
			{
				if (fileStatus[i].isDirectory())
				{
					listAllFiles(fileStatus[i].getPath().toString());
				}
				else
				{
					allfileList.add(fileStatus[i].getPath().getName().toString());
				}
			}
		}
		return allfileList;
	}
	
	
	
	/**
	 * 读取hdfs中的文件内容到内存
	 */
	public static List<String > readToMemoryFromHdfs(String hdfsFilename, long nMaxSize)
	{
		
		StringBuffer buffer = new StringBuffer();
		FSDataInputStream dis = null;
		BufferedReader bufferedReader = null;
		String lineTxt = null;
		List<String > list =  new ArrayList<String>();
		hdfsFilename = modifiedHdfsPath(hdfsFilename);
		try
		{
			HadoopFSOperations fsOperations = new HadoopFSOperations();
			Path f = new Path(hdfsFilename);
			 dis = fsOperations.fs.open(f);
				InputStream is = dis;
				GZIPInputStream gis = null;
				if(hdfsFilename.toLowerCase().endsWith(".gz"))
				{
					gis = new GZIPInputStream(dis);
					is  = gis;
				}			
			bufferedReader = new BufferedReader(new InputStreamReader(is));
			while ((lineTxt = bufferedReader.readLine()) != null)
			{	
				String t[] = lineTxt.split("\t");
				list.add(lineTxt);	
			}
			java.util.Iterator<String> it = list.iterator();
			dis.close();
			bufferedReader.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			if (bufferedReader != null)
			{
				try
				{
					bufferedReader.close();
				} catch (IOException e)
				{
					e.printStackTrace();
				}
			}
		}
		return list;
	}
	
	/**
	 * 从内存中把数据写到hdfs
	 */
	public static Boolean readToHdfsFromMemory(String hdfsFilename, List<String> list,long nMaxSize)
	{
		
		StringBuffer buffer = new StringBuffer();
		FSDataInputStream dis = null;
		FSDataOutputStream tant = null;
		BufferedReader bufferedReader = null;
		BufferedWriter bufferedWriter = null;
		String lineTxt = null;
		hdfsFilename = modifiedHdfsPath(hdfsFilename);
		try
		{
			HadoopFSOperations fsOperations = new HadoopFSOperations();
			makeDir(hdfsFilename);
			Path fun = new Path(hdfsFilename);
			tant = fsOperations.fs.create(fun);
			FSDataOutputStream art = tant;
			GZIPOutputStream ggg = null;
			if(hdfsFilename.toLowerCase().endsWith(".gz"))
			{
				ggg = new GZIPOutputStream(tant);
				bufferedWriter = new BufferedWriter(new OutputStreamWriter(ggg));
			}	else{		
			bufferedWriter = new BufferedWriter(new OutputStreamWriter(art));	
			}
			for (int i = 0; i < list.size(); i++) {
					bufferedWriter.write(list.get(i));
					bufferedWriter.newLine();
					bufferedWriter.flush();	
				}	
			ggg.close();
			tant.close();
			bufferedWriter.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			if (bufferedReader != null)
			{
				try
				{
					bufferedReader.close();
				} catch (IOException e)
				{
					e.printStackTrace();
				}
			}
		}

		return false;
	}
	
	
	
	public String viewFileFromHdfs(String hdfsFilename, int nMaxSize)
	{
		String str = "";
		if (nMaxSize > 1024*1024)
			nMaxSize = 1024*1024;// 最大1M
		try
		{
			Path f = new Path(hdfsFilename);

			FSDataInputStream dis = fs.open(f);
			FSDataInputStream diss = fs.open(f);
			
			InputStream is = dis;
			InputStream are = diss;
			GZIPInputStream gis = null;
			GZIPInputStream giss = null;
			if(hdfsFilename.toLowerCase().endsWith(".gz"))
			{
				gis = new GZIPInputStream(dis);
				giss = new GZIPInputStream(diss);
				is  = gis;
				are = giss;
			}			
			
	
			String fileEncode = "GBK";
			//判断编码格式是否为UTF-8
			
			
			 byte[] utf=new byte[3];  
			 are.read(utf);  
			  if(utf[0]==-17&&utf[1]==-69&&utf[2]==-65)  {
				  fileEncode = "UTF-8";
			  }
			  
			byte[] buffer = new byte[1024];
			int length = 0;
			long nTotalLength = 0;
			while ((length = is.read(buffer)) > 0)
			{
				str += new String(buffer, 0, length, fileEncode);
				nTotalLength += length;

				if (nMaxSize > 0 && nTotalLength > nMaxSize)
					break;
			}
			dis.close();
			diss.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return str;
	}

	public FileStatus getFileStatus(String path)
	{
		try
		{
			Path curPath = new Path(path);
			if (hdfs.exists(curPath) && (!curPath.isRoot()))
			{
				FileStatus fileStatus[] = fs.listStatus(curPath.getParent());
				int listlength = fileStatus.length;
				for (int i = 0; i < listlength; i++)
				{
					if (fileStatus[i].getPath().toString().contains(curPath.toString()))
					{
						return fileStatus[i];
					}
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * list all file/directory
	 * 
	 * @param path
	 * @throws IOException
	 * @throws IllegalArgumentException
	 * @throws FileNotFoundException
	 */
	public void listFileStatus(String path) throws FileNotFoundException, IllegalArgumentException, IOException
	{
		FileStatus fileStatus[] = fs.listStatus(new Path(path));
		int listlength = fileStatus.length;
		for (int i = 0; i < listlength; i++)
		{
			if (fileStatus[i].isDirectory() == false)
			{
				log.info("filename:" + fileStatus[i].getPath().getName() + "\tsize:" + fileStatus[i].getLen());
			}
			else
			{
				String newpath = fileStatus[i].getPath().toString();
				listFileStatus(newpath);
			}
		}
	}

	public boolean ImportFileToSqlDb(String dbName, String hdfsDirName, String delichar, String dbURL, String userName, String userPwd)
	{
		try
		{
			if (!this.checkFileExist(hdfsDirName))
			{
				return false;
			}

			FileStatus[] fileStatus = fs.listStatus(new Path(hdfsDirName));
			int listlength = fileStatus.length;

			if (listlength == 0)
				return false;

			List<String> columnNames = new ArrayList<String>();
			List<String> columnTypes = new ArrayList<String>();

			String driverName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
			try
			{

				Class.forName(driverName);

				Connection connection = DriverManager.getConnection(dbURL, userName, userPwd);
				log.info("连接数据库成功");
				DatabaseMetaData dbmd = connection.getMetaData();
				ResultSet colRet = dbmd.getColumns(null, "%", dbName, "%");
				String sql = "insert into " + dbName + "(";

				while (colRet.next())
				{
					String columnName = colRet.getString("COLUMN_NAME");
					sql += columnName + ",";
					String columnType = colRet.getString("TYPE_NAME");
					columnNames.add(columnName);
					columnTypes.add(columnType);
				}
				colRet.close();
				sql = sql.substring(0, sql.length() - 1);
				sql += ") values(";
				for (int i = 0; i < columnNames.size(); i++)
				{
					sql += "?,";
				}
				sql = sql.substring(0, sql.length() - 1);
				sql += ")";

				for (int i = 0; i < listlength; i++)
				{
					if (fileStatus[i].isFile() == true)
					{
						log.info("开始入库文件：" + fileStatus[i].getPath().getName());
						FSDataInputStream dis = fs.open(fileStatus[i].getPath());
						InputStreamReader isr = new InputStreamReader(dis, "utf-8");
						BufferedReader br = new BufferedReader(isr);

						String str = "";
						PreparedStatement ps = connection.prepareStatement(sql);
						int nRows = 0;
						while ((str = br.readLine()) != null)
						{
							String[] vct = str.split(delichar);
							if (vct.length != columnNames.size() && (vct.length != columnNames.size() + 1))
							{
								break;
							}

							boolean bHasError = false;
							for (int j = 0; j < columnNames.size(); j++)
							{
								int jj = j;
								if (vct.length == (columnNames.size() + 1))
								{
									jj = j + 1;
								}

								try
								{
									if (columnTypes.get(j).toLowerCase().contains("varchar"))
									{
										// log.info(vct[jj]);
										ps.setString(j + 1, vct[jj]);
									}
									else if (columnTypes.get(j).toLowerCase().equals("int"))
									{
										try
										{
											ps.setInt(j + 1, Integer.parseInt(vct[jj]));
										}
										catch (Exception e)
										{
											ps.setInt(j + 1, 0);
										}
									}
									else if (columnTypes.get(j).toLowerCase().contains("float"))
									{
										try
										{
											ps.setDouble(j + 1, Double.parseDouble(vct[jj]));
										}
										catch (Exception e)
										{
											ps.setDouble(j + 1, 0);
										}
									}
									else if (columnTypes.get(j).toLowerCase().equals("smallint"))
									{
										try
										{
											ps.setShort(j + 1, Short.parseShort(vct[jj]));
										}
										catch (Exception e)
										{
											ps.setShort(j + 1, Short.parseShort("0"));
										}
									}
									else if (columnTypes.get(j).toLowerCase().contains("tinyint"))
									{
										ps.setByte(j + 1, Byte.parseByte("0"));
									}
									else if (columnTypes.get(j).toLowerCase().contains("bigint"))
									{
										try
										{
											ps.setLong(j + 1, Long.parseLong(vct[jj]));
										}
										catch (Exception e)
										{
											ps.setLong(j + 1, 0L);
										}
									}
									else if (columnTypes.get(j).toLowerCase().contains("datetime"))
									{
										try
										{
											ps.setTimestamp(j + 1, java.sql.Timestamp.valueOf(vct[j]));
										}
										catch (Exception e)
										{
											ps.setTimestamp(j + 1, java.sql.Timestamp.valueOf("1970-01-01 08:00：00"));
										}
									}
									else
									{
										ps.clearParameters();
										bHasError = true;
										break;
									}

								}
								catch (Exception e)
								{
									e.printStackTrace();
									ps.clearParameters();
									bHasError = true;
									break;
								}

							}
							if (bHasError != true)
							{
								nRows++;
								ps.addBatch();
								if (nRows > 1000)
								{
									ps.executeBatch();
									nRows = 0;
								}
							}
						}
						if (nRows > 0)
						{
							ps.executeBatch();
							connection.commit();
							nRows = 0;
						}
						br.close();
						isr.close();
						dis.close();
						log.info("完成入库文件：" + fileStatus[i].getPath().getName());
					}
				}
				log.info("导入完成");
			}

			catch (Exception e)
			{
				e.printStackTrace();
				System.out.print("连接失败");
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

		return true;
	}

	/**
	 *
	 * @param srcfile
	 *            复制的起始目录
	 * @param desfile
	 *            复制的最终目录，包括文件名
	 * @return
	 */
	public boolean hdfsCopyUtils(String srcfile, String desfile)
	{

		Path src = new Path(srcfile);
		Path dst = new Path(desfile);
		try
		{
			FileUtil.copy(src.getFileSystem(conf), src, dst.getFileSystem(conf), dst, false, conf);
		}
		catch (IOException e)
		{
			return false;
		}
		return true;
	}

	// 得到整个目录大小
	public long getFileLen(Path path)
	{
		//SetHadoopRoot();
		long i = 0;
		if (fs == null)
		{
			return 0;
		}
		else
		{
			try
			{
				i = fs.getContentSummary(path).getLength();
			}
			catch (Exception e)
			{
				return 0;
			}
		}
		return i;
	}

	public void deleteFiles(String dpath)
	{
		try
		{
			FileStatus fileStatus[] = fs.listStatus(new Path(dpath));
			for (int i = 0; i < fileStatus.length; i++)
			{
				if (!fileStatus[i].getPath().getName().startsWith("part-"))
				{
					hdfs.delete(fileStatus[i].getPath(), true);
				}
			}
		}
		catch (Exception e)
		{}
	}
	
	public static void main(String[] args) throws Exception
	{
		HadoopFSOperations hdfs = new HadoopFSOperations("hdfs://192.168.1.31:9000");
		System.out.println(hdfs.tarPackageCompress("/winter/cellbuild/1.tar"));
	}
	
	/**
	 * 更正hdfs路径，由于历史原因，hdfs的路径有可能传入的是 //dir1/dir2/dir3... 所以，为了兼容性，需要对路径进行更正 
	 */
	public static String modifiedHdfsPath(String hdfsPath) {
		if (hdfsPath.startsWith("//")) {
			return hdfsPath.substring(1);
		}
		return hdfsPath;
	}
	
	/**
	 * 输出完整的堆栈异常
	 * @param t
	 * @return
	 */
	public static String getTrace(Throwable t) {
		log.error("===============系统堆栈异常==================");
        StringWriter stringWriter= new StringWriter();
        PrintWriter writer= new PrintWriter(stringWriter);
        t.printStackTrace(writer);
        StringBuffer buffer= stringWriter.getBuffer();
        return buffer.toString();
    }
}
