package com.chinamobile.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.log4j.Logger;


public class HadoopFSOperations
{
	static Logger log = Logger.getLogger(HadoopFSOperations.class.getName());
	private static Configuration conf = new Configuration();
	String HADOOP_URL = "hdfs://10.139.6.169:9000";
    boolean bExitFlag = false;
	public static FileSystem fs;

	private static DistributedFileSystem hdfs;
	
	public HadoopFSOperations()
	{
		SetHadoopRoot();
	}

	public HadoopFSOperations(String hdfsRoot)
	{
		HADOOP_URL = hdfsRoot;
		SetHadoopRoot();
	}
	
	private void SetHadoopRoot()
	{
		try
		{
			FileSystem.setDefaultUri(conf, HADOOP_URL);
			fs = FileSystem.get(conf);
			hdfs = (DistributedFileSystem) fs;
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	@SuppressWarnings("deprecation")
	public void moveSmallFilesToParent(String parentDir)
	{
		System.out.println("MoveSmallFilesToParent: " + parentDir);
		FileStatus fileStatus[];
		try
		{
			fileStatus = fs.listStatus(new Path(parentDir));
			int listlength = fileStatus.length;

			for (int i = 0; i < listlength; i++)
			{
				if (fileStatus[i].isDirectory() == true)
				{
					String pathName = fileStatus[i].getPath().getName()
							.toLowerCase();
					/*
					 * if (!pathName.contains("sample") &&
					 * !pathName.contains("event") &&
					 * !pathName.contains("grid")) { continue; }
					 */

					FileStatus childStatus[] = fs
							.listStatus(new Path(parentDir + "/" + pathName));

					int childListlength = childStatus.length;
					boolean bShouldMove = false;
					try
					{
						for (int j = 0; j < childListlength; j++)
						{
							if (childStatus[j].isDirectory() == false)
							{
								String childName = childStatus[j].getPath()
										.getName().toLowerCase();
								if (!childName.contains("sample")
										&& !childName.contains("event")
										&& !childName.contains("grid"))
								{
									continue;
								}
								movefile(parentDir + "/" + pathName + "/"
										+ childName, parentDir);
								bShouldMove = true;
							}
						}
						if (bShouldMove)
						{
							fs.delete(new Path(parentDir + "/" + pathName));
							System.out.println(
									" " + parentDir + "/" + pathName);
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
			if(!path.startsWith("hdfs"))
				path = HADOOP_URL + path;
			return fs.listStatus(new Path(path));
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		return null;
	}

	public ArrayList<DatafileInfo> listFiles(String path)
			throws FileNotFoundException, IllegalArgumentException, IOException
	{
		FileStatus fileStatus[] = fs.listStatus(new Path(path));
		int listlength = fileStatus.length;
		ArrayList<DatafileInfo> fileList = new ArrayList<DatafileInfo>();
		for (int i = 0; i < listlength; i++)
		{
			if (fileStatus[i].isDirectory() == false)
			{
				if (fileStatus[i].getLen() > 0)
				{
					long modificationTime = fileStatus[i].getModificationTime();
					fileList.add(
							new DatafileInfo(fileStatus[i].getPath().getName(),
									fileStatus[i].getLen(),modificationTime));
				}
			}
		}
		return fileList;
	}
	
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
					fileList.add(
							new DatafileInfo(fileStatus[i].getPath().getName(),
									fileStatus[i].getLen(),modificationTime));
				}
			}
		}	
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return fileList;
	}

	/**
	 * 锟叫筹拷锟斤拷锟斤拷DataNode锟斤拷锟斤拷锟斤拷锟斤拷息
	 */
	public void listDataNodeInfo()
	{
		try
		{
			DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
			String[] names = new String[dataNodeStats.length];
			System.out.println("List of all the datanode in the HDFS cluster:");

			for (int i = 0; i < names.length; i++)
			{
				names[i] = dataNodeStats[i].getHostName();
				System.out.println(names[i]);
			}
			System.out.println(hdfs.getUri().toString());
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
			System.out.println("锟斤拷锟斤拷锟斤拷锟侥硷拷锟叫伙拷锟侥硷拷锟缴癸拷: " + src + " --> " + dst);
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
			//System.out.println("删锟斤拷锟侥硷拷锟叫成癸拷: " + src);			
			return hdfs.delete(p1, true);
		}
		else if (hdfs.isFile(p1))
		{
			//System.out.println("删锟斤拷锟侥硷拷锟缴癸拷: " + src);
			return hdfs.delete(p1, false);
		}
		return true;
	}

	/**
	 * 锟介看锟侥硷拷锟角凤拷锟斤拷锟�
	 */
	public boolean checkFileExist(String filename)
	{
		try
		{
			Path f = new Path(filename);
			return hdfs.exists(f);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 锟斤拷锟斤拷锟侥硷拷锟斤拷HDFS系统锟斤拷
	 */
	public void createFile()
	{
		try
		{
			Path f = new Path("/user/xxx/input02/file01");
			System.out.println("Create and Write :" + f.getName() + " to hdfs");

			FSDataOutputStream os = fs.create(f, true);
			Writer out = new OutputStreamWriter(os, "utf-8");// 锟斤拷UTF-8锟斤拷式写锟斤拷锟侥硷拷锟斤拷锟斤拷锟斤拷锟斤拷
			out.write("锟斤拷锟� good job");
			out.close();
			os.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public boolean mkdir(String dirName)
	{
		if (checkFileExist(dirName))
			return true;
		try
		{
			Path f = new Path(dirName);
			System.out.println("Create and Write :" + f.getName() + " to hdfs");
			return hdfs.mkdirs(f);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

		return false;
	}

	/**
	 * 锟斤拷取锟斤拷锟斤拷锟侥硷拷锟斤拷HDFS系统<br>
	 * 锟诫保证锟侥硷拷锟斤拷式一直锟斤拷UTF-8锟斤拷锟接憋拷锟斤拷->HDFS
	 */
	@SuppressWarnings("deprecation")
	public boolean getMerge(String localDir, String hdfsDir, String destFileName, String filter )
	{
		try {
			ArrayList<DatafileInfo> fileLst= listFiles(hdfsDir);
			if(fileLst.isEmpty())
				return false;
			makeDir(localDir);
			deleteFile(localDir + "/" + destFileName);
			File file = new File(localDir + "/" + destFileName);
			FileOutputStream os = new FileOutputStream(file);	
			
			for(int i=0; i<fileLst.size();i++)
			{			
				if(!fileLst.get(i).filename.contains(filter))
				{
					continue;
				}
							
				String hdfsFilename = hdfsDir + "/" + fileLst.get(i).filename;
				Path f = new Path(hdfsFilename);
				FSDataInputStream dis = fs.open(f);			
				
				byte[] buffer = new byte[1024000];
				int length = 0;
				long nTotalLength = 0;
				int  nCount = 0;
				while (bExitFlag != true && (length = dis.read(buffer)) >0)
				{
					nCount++;				
					os.write(buffer,0,length);
					nTotalLength += length;

					if(nCount%100 == 0)
					{
						StringBuilder stringBuilder = new StringBuilder();
						stringBuilder.append((new Date()).toLocaleString());
						stringBuilder.append(": Have move ");
						stringBuilder.append((nTotalLength/1024000));
						stringBuilder.append(" MB");
						System.out.println(stringBuilder.toString());
					}
				}

				dis.close();
			}
			os.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
			
		return true;	
	}
	
	public FSDataOutputStream GetOutputStream(String destFileName)
	{	
		try
		{
			if(this.checkFileExist(destFileName))
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
	
	/**
	 * 锟斤拷取锟斤拷锟斤拷锟侥硷拷锟斤拷HDFS系统<br>
	 * 锟诫保证锟侥硷拷锟斤拷式一直锟斤拷UTF-8锟斤拷锟接憋拷锟斤拷->HDFS
	 */
	public boolean putMerge(String localDirname, String hdfsPath,String destFileName, String filter )
	{
		try
		{
			File dir = new File(localDirname);
			if(!dir.isDirectory())
			{
				System.out.println(localDirname+"锟斤拷锟斤拷目录 ");
				return false;
			}

			File[] files = dir.listFiles();
			if(files.length ==0)
				return false;
			
			System.out.println("Begin move " + localDirname + " to " + hdfsPath);
			
			if(this.checkFileExist(hdfsPath+"/"+destFileName))
			{
				delete(hdfsPath+"/"+destFileName);
			}		
			mkdir(hdfsPath);
			Path f = new Path(hdfsPath+"/"+destFileName);
			FSDataOutputStream os = fs.create(f, true);
			byte[] buffer = new byte[10240000];
 			
			for(int i=0; i<files.length; i++)
			{
				if(bExitFlag == true)
					break;	
				 
				File file = files[i];
				if(!file.getName().toLowerCase().contains(filter.toLowerCase()))
					continue;
				FileInputStream is = new FileInputStream(file);					
				GZIPInputStream gis = null;
				if(file.getName().toLowerCase().contains(".gz"))
					gis=new GZIPInputStream(is);
				
				while(bExitFlag != true)
				{
		            int bytesRead =0;
		            if(gis == null)
		            	bytesRead= is.read(buffer);
		            else
		            	bytesRead= gis.read(buffer,0,buffer.length);
		            if (bytesRead >= 0) 
		            {
		                os.write(buffer, 0, bytesRead);
		            }
		            else
		            {
		            	break;
		            }
				}
	            if(gis != null)
	            	gis.close();
				is.close();
			}
			os.close();
			if(bExitFlag)
				return false;
			System.out.println("Success move " + localDirname + " to " + hdfsPath);
			return true;
		}
		catch (Exception e)
		{
			e.printStackTrace();
			log.info(e.getStackTrace());
		}
		return false;
	}
	
	public boolean CopyDirTohdfs(String localPath, String hdfsPath)
	{
		try
		{
			File root = new File(localPath);
			File[] files = root.listFiles();
			
			for (File file : files)
			{
				if (file.isFile())
				{		
					if(!copyFileToHDFS(file.getPath().toString(), hdfsPath))
					{	
					}
				}
				else if(file.isDirectory())
				{
					CopyDirTohdfs(localPath+"/"+file.getName(), hdfsPath+"/"+file.getName());
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
	
	public boolean CreateEmptyFile(String filename)
	{
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
	 * 锟斤拷取锟斤拷锟斤拷锟侥硷拷锟斤拷HDFS系统<br>
	 * 锟诫保证锟侥硷拷锟斤拷式一直锟斤拷UTF-8锟斤拷锟接憋拷锟斤拷->HDFS
	 */
	@SuppressWarnings("deprecation")
	public boolean copyFileToHDFS(String localFilename, String hdfsPath)
	{
		try
		{
			System.out.println("Begin move " + localFilename + " to " + hdfsPath);
			mkdir(hdfsPath);
			
			File file = new File(localFilename);
			FileInputStream is = new FileInputStream(file);	

			if(this.checkFileExist(hdfsPath+"/"+file.getName()))
			{
				delete(hdfsPath+"/"+file.getName());
			}
			
			Path f = new Path(hdfsPath+"/"+file.getName());
			
			FSDataOutputStream os = fs.create(f, false);
			byte[] buffer = new byte[10240000];
            int nCount = 0;
			while(bExitFlag != true)
			{
	            int bytesRead = is.read(buffer);
	            if (bytesRead >= 0) 
	            {
	                os.write(buffer, 0, bytesRead);
	                nCount++;
	                if(nCount%(100) == 0)
	                	System.out.println((new Date()).toLocaleString() + ": Have move " + nCount + " blocks");
	            }
	            else
	            {
	            	break;
	            }
			}
			
			is.close();
			os.close();
			System.out.println((new Date()).toLocaleString() + ": Write content of file " + file.getName()
					+ " to hdfs file " + f.getName() + " success");
			
			if(bExitFlag)
				return false;
			return true;
		}
		catch (Exception e)
		{
			log.info(e.getMessage());
		}
		return false;
	}
	
	 
	/**
	 * 取锟斤拷锟侥硷拷锟斤拷锟斤拷锟节碉拷位锟斤拷..
	 */
	public void getLocation()
	{
		try
		{
			Path f = new Path("/user/xxx/input02/file01");
			FileStatus fileStatus = fs.getFileStatus(f);

			BlockLocation[] blkLocations = fs.getFileBlockLocations(fileStatus,
					0, fileStatus.getLen());
			for (BlockLocation currentLocation : blkLocations)
			{
				String[] hosts = currentLocation.getHosts();
				for (String host : hosts)
				{
					//System.out.println(host);
				}
			}

			// 取锟斤拷锟斤拷锟斤拷薷锟绞憋拷锟�
			long modifyTime = fileStatus.getModificationTime();
			Date d = new Date(modifyTime);
			System.out.println(d);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public boolean deleteFile(String sPath) {  
	    boolean flag = false;  
	    File file = new File(sPath);  
	    // 路锟斤拷为锟侥硷拷锟揭诧拷为锟斤拷锟斤拷锟斤拷锟缴撅拷锟�  
	    if (file.isFile() && file.exists()) {  
	        file.delete();  
	        flag = true;  
	    }  
	    else if(!file.exists())
	    {
	    	flag = true;
	    }
	    return flag;  
	}  
	
	public static boolean makeDir(String dirName)
	{
		File file =new File(dirName);    
		//锟斤拷锟斤拷募锟斤拷胁锟斤拷锟斤拷锟斤拷虼唇锟�    
		if  (!file .exists()  && !file .isDirectory())      
		{       
		    //System.out.println("//锟斤拷锟斤拷锟斤拷");  
		    file.mkdir();    
		}  
		return true;
	}

	public boolean readHdfsDirToLocal(String hdfsDir, String localDir, String filter)
	{
		try {
			ArrayList<DatafileInfo> fileLst= listFiles(hdfsDir);
			if(fileLst.size() ==0)
				return false;
			for(int i=0; i<fileLst.size();i++)
			{
				final CalendarEx cal = new CalendarEx(new Date());
				
				if(!fileLst.get(i).filename.contains(filter))
				{
					continue;
				}
				
				/*if(fileLst.get(i).modificationTime/1000 + 360 >cal._second)
				{
					continue;
				}*/
				makeDir(localDir);
				String hdfsFilename = hdfsDir + "/" + fileLst.get(i).filename;
				String localFilename = localDir + "/" + fileLst.get(i).filename;
				deleteFile(localFilename);
				
				if(!readFileFromHdfs(hdfsFilename, localDir,-1))
				{				
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return true;
	}
	
	/**
	 * 锟斤拷取hdfs锟叫碉拷锟侥硷拷锟斤拷锟斤拷
	 */
	@SuppressWarnings("deprecation")
	public boolean  readFileFromHdfs(String hdfsFilename, String localPath, int nMaxSize)
	{
		try
		{
			Path f = new Path(hdfsFilename);

			FSDataInputStream dis = fs.open(f);
			File file = new File(localPath + "/" + f.getName());
			FileOutputStream os = new FileOutputStream(file);	

			byte[] buffer = new byte[1024000];
			int length = 0;
			long nTotalLength = 0;
			int  nCount = 0;
			while (bExitFlag != true && (length = dis.read(buffer)) >0)
			{
				nCount++;				
				os.write(buffer,0,length);
				nTotalLength += length;
				if(nMaxSize>0 && nTotalLength>nMaxSize)
					break;

				/*if(nCount%100 == 0)
				{
					StringBuilder stringBuilder = new StringBuilder();
					stringBuilder.append((new Date()).toLocaleString());
					stringBuilder.append(": Have move ");
					stringBuilder.append((nTotalLength/1024000));
					stringBuilder.append(" MB");
					System.out.println(stringBuilder.toString());
				}*/
			}

			os.close();
			dis.close();
			if(bExitFlag != true)
				return true;
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * 锟斤拷取hdfs锟叫碉拷锟侥硷拷锟斤拷锟斤拷
	 */
	public String  viewFileFromHdfs(String hdfsFilename, int nMaxSize)
	{
		String str=""; 
		if(nMaxSize>1024000)nMaxSize=1024000;//锟斤拷锟�1M
		try
		{
			Path f = new Path(hdfsFilename);

			FSDataInputStream dis = fs.open(f);
			byte[] buffer = new byte[1024];
			int length = 0;
			long nTotalLength = 0;
			while (bExitFlag != true && (length = dis.read(buffer)) >0)
			{
				str+=new String( buffer ,"GB2312");
				nTotalLength += length;
				if(nMaxSize>0 && nTotalLength>nMaxSize)
					break;
			}
			dis.close();
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
			if(hdfs.exists(curPath) && (!curPath.isRoot()))
			{
				FileStatus fileStatus[] = fs.listStatus(curPath.getParent());
				int listlength = fileStatus.length;
				for (int i = 0; i < listlength; i++)
				{
					if (fileStatus[i].getPath().toString().equals(curPath.toString()))
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
	 * @param args
	 * @throws IOException
	 * @throws IllegalArgumentException
	 * @throws FileNotFoundException
	 */
	public void listFileStatus(String path)
			throws FileNotFoundException, IllegalArgumentException, IOException
	{
		FileStatus fileStatus[] = fs.listStatus(new Path(path));
		int listlength = fileStatus.length;
		for (int i = 0; i < listlength; i++)
		{
			if (fileStatus[i].isDirectory() == false)
			{
				System.out.println("filename:" + fileStatus[i].getPath().getName()
								+ "\tsize:" + fileStatus[i].getLen());
			}
			else
			{
				String newpath = fileStatus[i].getPath().toString();
				listFileStatus(newpath);
			}
		}
	}

	public boolean ImportFileToSqlDb(String dbName, String hdfsDirName,
			String delichar, String dbURL, String userName, String userPwd)
	{
		try
		{
			if (!this.checkFileExist(hdfsDirName)) { return false; }

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

				Connection connection = DriverManager.getConnection(dbURL,
						userName, userPwd);
				System.out.println("锟斤拷锟斤拷锟斤拷锟捷匡拷晒锟�");
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
						System.out.println("锟斤拷始锟斤拷锟斤拷募锟斤拷锟�"+fileStatus[i].getPath().getName());
						FSDataInputStream dis = fs
								.open(fileStatus[i].getPath());
						InputStreamReader isr = new InputStreamReader(dis,
								"utf-8");
						BufferedReader br = new BufferedReader(isr);

						String str = "";
						PreparedStatement ps = connection.prepareStatement(sql);
						int nRows = 0;
						while ((str = br.readLine()) != null)
						{
							String[] vct = str.split(delichar);
							if (vct.length != columnNames.size()
									&& (vct.length != columnNames.size() + 1))
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
										//System.out.println(vct[jj]);
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
											ps.setInt(j + 1,0);
										}
									}
									else if (columnTypes.get(j).toLowerCase().contains("float"))
									{
										try
										{
											ps.setDouble(j + 1,Double.parseDouble(vct[jj]));
										}
										catch (Exception e)
										{
											ps.setDouble(j + 1,0);
										}
									}
									else if (columnTypes.get(j) .toLowerCase().equals("smallint"))
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
										ps.setByte(j + 1,Byte.parseByte("0"));
									}
									else if (columnTypes.get(j) .toLowerCase() .contains( "bigint"))
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
									else if (columnTypes .get(j).toLowerCase().contains("datetime"))
									{
										try
										{
											ps.setTimestamp(j + 1,java.sql.Timestamp.valueOf(vct[j]));
										}
										catch (Exception e)
										{
											ps.setTimestamp(j + 1,java.sql.Timestamp.valueOf("1970-01-01 08:00锟斤拷00"));
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
						System.out.println("锟斤拷锟斤拷锟斤拷锟侥硷拷锟斤拷"+fileStatus[i].getPath().getName());
					}
				}
				System.out.println("锟斤拷锟斤拷锟斤拷锟�");
			}

			catch (Exception e)
			{
				e.printStackTrace();
				System.out.print("锟斤拷锟斤拷失锟斤拷");
			}
		}
		catch (Exception e)  
		{
			e.printStackTrace();
		}

		return true;
	}
	
	public static void main(String[] args)
	{
		System.out.println("usage:HdfsPuter localFilename hdfsPath");
		HadoopFSOperations hdfs = new HadoopFSOperations();
		//hdfs.putMerge("E:/锟斤拷锟斤拷/MRE/ERIC/decode", "hdfs://10.139.6.169:9000/mt_wlyh/input/20151022","hw_1023.mre",".bcp");
		hdfs.getMerge("E:/锟斤拷锟斤拷/MRE/ERIC/decode", "hdfs://10.139.6.169:9000/mt_wlyh/Data/mroxdrmerge/gridstat/cqtgrid_01_151014/TB_SIGNAL_GRID_01_151014","hw_1023.grid","grid");
		//readFileFromHdfs("hdfs://10.139.6.169:9000/mt_wlyh/Data/mro/151014/test.txt","d:/");
	}
}
