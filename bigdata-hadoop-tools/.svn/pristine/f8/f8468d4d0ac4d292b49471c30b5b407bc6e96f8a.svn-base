package cn.mastercom.bigdata.util.ftp;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.log4j.Logger;

import cn.mastercom.bigdata.util.CalendarEx;
import cn.mastercom.bigdata.util.hadoop.hdfs.DatafileInfo;
import cn.mastercom.bigdata.util.hadoop.hdfs.HadoopFSOperations;


/**
 * FTP客户端
 * 
 * @author summersun_ym
 * @version $Id: FTPClientTemplate.java 2010-11-22 上午12:54:47 $
 */
public class FTPClientHelper implements IFtpClientHelper {
    //---------------------------------------------------------------------
    // Instance data
    //---------------------------------------------------------------------
    /** logger */
    protected final Logger         log                  = Logger.getLogger(getClass());
    private ThreadLocal<FTPClient> ftpClientThreadLocal = new ThreadLocal<FTPClient>();

    private String                 host;
    private int                    port;
    private String                 username;
    private String                 password;

    private boolean                binaryTransfer       = true;
    private boolean                passiveMode          = true;
    private String                 encoding             = "UTF-8";
    private int                    clientTimeout        = 1000 * 300 ;
    
    public FTPClientHelper(String host, int port,String username,String password)
    {
    	setHost(host);
        setPort(port);
        setUsername(username);
        setPassword(password);
    }
    
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public boolean isBinaryTransfer() {
        return binaryTransfer;
    }

    public void setBinaryTransfer(boolean binaryTransfer) {
        this.binaryTransfer = binaryTransfer;
    }

    public boolean isPassiveMode() {
        return passiveMode;
    }

    public void setPassiveMode(boolean passiveMode) {
        this.passiveMode = passiveMode;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public int getClientTimeout() {
        return clientTimeout;
    }
    
	@Override
	public void setTimeout(int timeOut)
	{
		this.clientTimeout = timeOut;		
	}
      
    /**
     * 直接切换至工作目录,检测工作目录是否存在.
     * @param destFileName
     * @return
     * @throws Exception
     */
    public boolean checkDirExist(String destFileName) throws Exception
    {
    	return  getFTPClient().changeWorkingDirectory(destFileName);
    }
    
    public boolean checkFileExist(String destFileName) throws Exception
    {
    	InputStream is = getFTPClient().retrieveFileStream(new String(destFileName.getBytes("GBK"),FTP.DEFAULT_CONTROL_ENCODING));
        if(is == null || getFTPClient().getReplyCode() == FTPReply.FILE_UNAVAILABLE){
            return false;
        }
		return true;  	
    }
    
    /**
     * 返回一个FTPClient实例
     * 
     * @throws Exception
     */
    public FTPClient getFTPClient() throws Exception {
    	if (ftpClientThreadLocal.get() != null && ftpClientThreadLocal.get().isConnected()  ) 
        {
        	try {
				if(ftpClientThreadLocal.get().listNames("/") !=null && ftpClientThreadLocal.get().listNames("/").length>0)
				{
					return ftpClientThreadLocal.get();
				}
			} catch (Exception e) {
				log.error("[" + host + "] " + "ftp disconnect " + e.toString());
			}     
        } 
        FTPClient ftpClient = new FTPClient(); //构造一个FtpClient实例
        ftpClient.setControlEncoding(encoding); //设置字符集

        connect(ftpClient); //连接到ftp服务器

        //设置为passive模式
        if (passiveMode) {
            ftpClient.enterLocalPassiveMode();
        }else {
        	ftpClient.enterLocalActiveMode();//主动模式
        }
        setFileType(ftpClient); //设置文件传输类型

        try {
        	// socket超时 
            ftpClient.setSoTimeout(clientTimeout);
            ftpClient.setConnectTimeout(10 * 1000); // 登录十秒超时  
            ftpClient.setDataTimeout(60 * 1000); // 设置数据超时 十分钟  
            ftpClient.setReceiveBufferSize(1024 * 1024);  
            ftpClient.setBufferSize(1024 * 1024);          
        } catch (SocketException e) {
            throw new Exception("Set timeout error." + e.toString());
        }
        ftpClientThreadLocal.set(ftpClient);
        return ftpClient;
        
    }

    /**
     * 设置文件传输类型
     * 
     * @throws Exception
     * @throws IOException
     */
    private void setFileType(FTPClient ftpClient) throws Exception {
        try {
            if (binaryTransfer) {
                ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
            } else {
                ftpClient.setFileType(FTP.ASCII_FILE_TYPE);
            }
        } catch (IOException e) {
            throw new Exception("Could not to set file type.", e);
        }
    }

    /**
     * 连接到ftp服务器
     * 
     * @param ftpClient
     * @return 连接成功返回true，否则返回false
     * @throws Exception
     */
    private boolean connect(FTPClient ftpClient) throws Exception {
        try {
            ftpClient.connect(host, port);

            // 连接后检测返回码来校验连接是否成功
            int reply = ftpClient.getReplyCode();

            if (FTPReply.isPositiveCompletion(reply)) {
                //登陆到ftp服务器
                if (ftpClient.login(username, password)) {
                    setFileType(ftpClient);
                    return true;
                }
            } else {
                ftpClient.disconnect();
                throw new Exception("FTP server refused connection.");
            }
        } catch (IOException e) {
            if (ftpClient.isConnected()) {
                try {
                    ftpClient.disconnect(); //断开连接
                } catch (IOException e1) {
                	e1.printStackTrace();
                    throw new Exception("Could not disconnect from server.", e1);
                }

            }
            throw new Exception("Could not connect to server.", e);
        }
        return false;
    }

    /**
     * 断开ftp连接
     * @throws Exception
     */
    public void disconnect() throws Exception {
    	FTPClient ftpClient = null;
        try {           
        	ftpClient = getFTPClient();
            ftpClient.logout();
            if (ftpClient.isConnected()) {
                ftpClient.disconnect();
                ftpClient = null;                
            }
        } catch (IOException e) {
        	throw new Exception("ftpClient logout() exception .", e);
        }finally {
        	if(ftpClient == null){
        		return ;
        	}
        	if (ftpClient.isConnected() ) {
                 ftpClient.disconnect();
                 ftpClient = null;                
            }
		}
    }
    
    public boolean mkdir(String pathname) throws Exception {
        return mkdir(pathname, null);
    }
    
    /**
     * 在ftp服务器端创建目录（不支持一次创建多级目录）
     * 
     * 该方法执行完后将自动关闭当前连接
     * 
     * @param pathname
     * @return
     * @throws Exception
     */
    public boolean mkdir(String pathname, String workingDirectory) throws Exception {
        return mkdir(pathname, workingDirectory, true);
    }
    
    /**
     * 在ftp服务器端创建目录（不支持一次创建多级目录）
     * 
     * @param pathname
     * @param autoClose 是否自动关闭当前连接
     * @return
     * @throws Exception
     */
    public boolean mkdir(String pathname, String workingDirectory, boolean autoClose) throws Exception {
        try {
            getFTPClient().changeWorkingDirectory(workingDirectory);
            return getFTPClient().makeDirectory(pathname);
        } catch (IOException e) {
            throw new Exception("Could not mkdir.", e);
        } finally {
            if (autoClose) {
                disconnect(); //断开连接
            }
        }
    }
    
    /**
     * ftp创建多级目录
     * @param pathname : 需要创建的目录路径
     * @param workingDirectory : 切换目录
     * @param autoClose : 是否关闭ftp
     * @throws Exception
     */
    public void mkdirs(String pathname, String workingDirectory, boolean autoClose) throws Exception {
    	String[] split = pathname.replace("\\", "/").split("/");
    	String string = "/";
    	for (int i = 1; i < split.length; i++) {
    		string = string + split[i];
    		mkdir(string, null, false);
			string  = string + "/" ;
		}
    }

    /**
     * 上传一个本地文件到远程指定文件
     * 
     * @param remoteAbsoluteFile 远程文件名(包括完整路径)
     * @param localAbsoluteFile 本地文件名(包括完整路径)
     * @return 成功时，返回true，失败返回false
     * @throws Exception
     */
    public boolean put(String remoteAbsoluteFile, String localAbsoluteFile) throws Exception {
        return put(remoteAbsoluteFile, localAbsoluteFile, false, false);
    }
    
    public boolean put(String remoteAbsoluteFile, InputStream is, boolean autoCompress) throws Exception {
    	if(autoCompress)
    	{	
    		if(remoteAbsoluteFile.contains("SUCCESS")){
    			return put(remoteAbsoluteFile, is, false, false);
    		}else{
    			if(remoteAbsoluteFile.contains(".gz")) {
    				GZIPInputStream gis = new GZIPInputStream(is);
    				return put(remoteAbsoluteFile, new GZIPCompressorInputStream(gis), false, false); 
    			}else {
    				return put(remoteAbsoluteFile+".gz", new GZIPCompressorInputStream(is), false, false); 	
    			}
    		}
    	}
    	else
    	{
    		return put(remoteAbsoluteFile, is, false, false);
    	}
    }
    
    public boolean putFromHdfs(String remoteAbsoluteFile, String hdfsFile, HadoopFSOperations hdfs, boolean autoCompress ) 
    		throws Exception 
    {	
     	return put(remoteAbsoluteFile, hdfs.getInputStream(hdfsFile), autoCompress);
    }
    
    
    
    public boolean putFileToHdfs(String remoteAbsoluteFile, String hdfsFile, HadoopFSOperations hdfs ) throws Exception {	
    	return get(remoteAbsoluteFile, hdfs.getOutputStream(hdfsFile), false, false);
    }
    
    public boolean putDirToHdfs(String remoteAbsoluteFile, String hdfsFile, HadoopFSOperations hdfs ) throws Exception {	
    	Object[] files = listFiles(remoteAbsoluteFile, false);
    	try {
			hdfs.mkdir(hdfsFile);
			for(Object file:files)
			{
				FTPFile ftpFile = (FTPFile)file;
				if(ftpFile.isFile())
				{
					boolean ret =get(remoteAbsoluteFile+"/" + ftpFile.getName(), hdfs.getOutputStream(hdfsFile+"/" + ftpFile.getName()), false,true);
					log.info("Upload file " + (ret? "Success":"Fail") + hdfsFile+"/" + ftpFile.getName());
				}
				else
				{
					putDirToHdfs(remoteAbsoluteFile+"/" + ftpFile.getName(),hdfsFile+"/" + ftpFile.getName(),hdfs);
				}
			}
		} catch (Exception e) {			
			e.printStackTrace();
			return false;
		}    	
    	return true;
    }
    
    public boolean putMergeToHdfs(String remoteAbsoluteFile, String hdfsFile, HadoopFSOperations hdfs, String destFileName ) throws Exception
	{
	   	Object[] files = listFiles(remoteAbsoluteFile, false);
	   	while (hdfs.checkFileExist(hdfsFile + "/" + destFileName))
		{
	   		if(destFileName.length()>100)
	   		{
	   			if(hdfs.delete(hdfsFile + "/" + destFileName))
	   				break;
	   		}
			if (destFileName.contains(".x"))
				destFileName += "x";
			else
				destFileName += ".x";		
		}
	   	
	   	OutputStream os = hdfs.getOutputStream(hdfsFile+"/" + destFileName);
	   	boolean ret = false;
    	try {
    		
			hdfs.mkdir(hdfsFile);
			for(Object file:files)
			{
				FTPFile ftpFile = (FTPFile)file;
				if (ftpFile.getName().toLowerCase().contains(".processing"))
					continue;
				if(ftpFile.isFile())
				{
					ret =get(remoteAbsoluteFile+"/" + ftpFile.getName(), os, false,false);	// 天津在用
					log.info("Upload file " + (ret? "Success":"Fail") + hdfsFile+"/" + ftpFile.getName());
				}				
			}
		} catch (Exception e) {			
			e.printStackTrace();
			return false;
		}    	
    	finally
    	{
    		if(os != null)
    		{
    			os.close();
    		}
    		if(!ret)
    		{
    			hdfs.delete(hdfsFile+"/" + destFileName);
    		}
    	}
    	return true;
	}

    /**
     * <p>多个 ftp 文件归并上传到 hdfs 系统上
     * @param remoteAbsoluteFile ftp 目录地址
     * @param hdfsDestFile hdfs 文件地址
     * @param hdfs	hdfs操作对象
     * @param compress 是否压缩
     * @return 上传成功返回<tt>true</tt>，否则返回<tt>false</tt>
     * @throws Exception 
     */
    public boolean putMergeToHdfsFromFtp(String remoteAbsoluteFile, String hdfsDestFile, HadoopFSOperations hdfs, boolean compress) throws Exception
   	{
    	if (remoteAbsoluteFile==null||hdfsDestFile==null) return false;
    	
    	// localDestFile 判断后缀名是否为 gz 
    	if (compress && !hdfsDestFile.endsWith(".gz")) hdfsDestFile+=".gz";
    	
    	FTPClient ftpClient = getFTPClient();
    	FTPFile[] files = ftpClient.listFiles(remoteAbsoluteFile);
    	
    	OutputStream os = null;
    	OutputStream fos = null;
    	
    	try {
			fos = hdfs.getOutputStream(hdfsDestFile);
			if (compress) os = new GZIPOutputStream(fos);
			else os = fos;
		} catch (Exception e) {
			try {os.close();} catch (Exception ignored) {}
			try {fos.close();} catch (Exception ignored) {}
			return false;
		}
		
    	if (remoteAbsoluteFile.lastIndexOf(remoteAbsoluteFile.length()-1)!='/')remoteAbsoluteFile+="/";
    	for (FTPFile file : files) {
    		InputStream is = null;
    		InputStream ftpis = null;
			try {
				ftpis = ftpClient.retrieveFileStream(remoteAbsoluteFile+file.getName());
				if (file.getName().endsWith(".gz")) is = new GZIPInputStream(ftpis);
				else is = ftpis;
				IOUtils.copy(is, os);
				os.write("\n".getBytes());	// 写入回车符
			} catch (Exception e) { return false;}
			finally {
				os.flush();	// 清空缓冲区，防止回车符没写入
				try {is.close(); } catch (Exception ignored) {}
				try {ftpis.close(); } catch (Exception ignored) {}
				ftpClient.completePendingCommand();
			}
    	}
    	os.close();
    	fos.close();
    	return true;
    }
    
    /**
     * <p>多个 hdfs 文件归并上传到 ftp 系统上
     * @param hdfsSrcPath hdfs 路径
     * @param ftpDestFile ftp 文件地址
     * @param hdfs hdfs 操作对象
     * @param compress 是否压缩
     * @return 上传成功返回<tt>true</tt>，否则返回<tt>false</tt>
     * @throws Exception
     */
    public boolean putMergeToFtpFromHdfs(String hdfsSrcPath, String ftpDestFile, HadoopFSOperations hdfs, boolean compress) throws Exception
   	{
    	if (hdfsSrcPath==null||ftpDestFile==null) return false;
    	
    	// localDestFile 判断后缀名是否为 gz 
    	if (compress && !ftpDestFile.endsWith(".gz")) ftpDestFile+=".gz";
    	
    	FTPClient ftpClient = getFTPClient();
    	ArrayList<DatafileInfo> files = hdfs.listFiles(hdfsSrcPath);
    	
    	OutputStream os = null;
    	OutputStream fos = null;
    	
    	try {
			fos = ftpClient.appendFileStream(ftpDestFile);
			if (compress) os = new GZIPOutputStream(fos);
			else os = fos;
		} catch (Exception e) {
			try {os.close();} catch (Exception ignored) {}
			try {fos.close();} catch (Exception ignored) {}
			try {ftpClient.completePendingCommand();} catch (Exception ignored) {}
			return false;
		}
		
    	for (DatafileInfo file : files) {
    		InputStream is = null;
    		InputStream hdfsis = null;
			try {
				hdfsis = hdfs.getInputStream(file.filePath);
				if (file.filename.endsWith(".gz")) is = new GZIPInputStream(hdfsis);
				else is = hdfsis;
				IOUtils.copy(is, os);
				os.write("\n".getBytes());	// 写入回车符
			} catch (Exception e) { return false;}
			finally {
				os.flush();	// 清空缓冲区，防止回车符没写入
				try {is.close(); } catch (Exception ignored) {}
				try {hdfsis.close(); } catch (Exception ignored) {}
			}
    	}
    	os.close();
    	fos.close();
    	try { ftpClient.completePendingCommand();} catch (Exception ignored) {}
    	return true;
    }
   
    /**
     * <p>多个 ftp 文件归并下载到本地
     * @param remoteAbsoluteFile ftp 文件地址
     * @param localDestFile	本地目标文件地址
     * @param compress	是否压缩
     * @return
     * @throws Exception
     */
    public boolean putMergeToLocalFromFtp(String remoteAbsoluteFile, String localDestFile, boolean compress) throws Exception {
    	if (remoteAbsoluteFile==null||localDestFile==null) return false;
    	
    	// localDestFile 判断后缀名是否为 gz 
    	if (compress && !localDestFile.endsWith(".gz")) localDestFile+=".gz";
    	
    	FTPClient ftpClient = getFTPClient();
    	FTPFile[] files = ftpClient.listFiles(remoteAbsoluteFile);
    	
    	OutputStream os = null;
    	FileOutputStream fos = null;
    	
    	try {
			fos = new FileOutputStream(localDestFile);
			if (compress) os = new GZIPOutputStream(fos);
			else os = fos;
		} catch (Exception e) {
			try {os.close();} catch (Exception ignored) {}
			try {fos.close();} catch (Exception ignored) {}
			return false;
		}
		
    	if (remoteAbsoluteFile.lastIndexOf(remoteAbsoluteFile.length()-1)!='/')remoteAbsoluteFile+="/";
    	for (FTPFile file : files) {
    		InputStream is = null;
    		InputStream ftpis = null;
			try {
				ftpis = ftpClient.retrieveFileStream(remoteAbsoluteFile+file.getName());
				if (file.getName().endsWith(".gz")) is = new GZIPInputStream(ftpis);
				else is = ftpis;
				IOUtils.copy(is, os);
				os.write("\n".getBytes());	// 写入回车符
			} catch (Exception e) { return false;}
			finally {
				os.flush();	// 清空缓冲区，防止回车符没写入
				try {is.close(); } catch (Exception ignored) {}
				try {ftpis.close(); } catch (Exception ignored) {}
				ftpClient.completePendingCommand();
			}
    	}
    	os.close();
    	fos.close();
    	return true;
    }
    
    /**
     * <p>多个本地文件上传到 ftp 系统上
     * @param localSrcPath	本地文件夹路径
     * @param ftpDestFile	ftp文件地址
     * @param compress	是否压缩
     * @return
     * @throws Exception
     */
    public boolean putMergeToFtpFromLocal(String localSrcPath, String ftpDestFile, boolean compress) throws Exception {

    	if (localSrcPath==null||ftpDestFile==null) return false;
    	
    	// localDestFile 判断后缀名是否为 gz 
    	if (compress && !ftpDestFile.endsWith(".gz")) ftpDestFile+=".gz";
    	
    	FTPClient ftpClient = getFTPClient();
    	File[] files = new File(localSrcPath).listFiles(new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				return !pathname.isDirectory();
			}
		});
    	
    	OutputStream os = null;
    	OutputStream fos = null;
    	
    	try {
			fos = ftpClient.appendFileStream(ftpDestFile);
			if (compress) os = new GZIPOutputStream(fos);
			else os = fos;
		} catch (Exception e) {
			try {os.close();} catch (Exception ignored) {}
			try {fos.close();} catch (Exception ignored) {}
			// 完成当前 ftp 命令
			try {ftpClient.completePendingCommand(); } catch (Exception ignored) {}
			return false;
		}
		
    	for (File file : files) {
    		InputStream is = null;
    		InputStream fis = null;
			try {
				fis = new FileInputStream(file);
				if (file.getName().endsWith(".gz")) is = new GZIPInputStream(fis);
				else is = fis;
				IOUtils.copy(is, os);
			} catch (Exception e) { return false;}
			finally {
				os.flush();	// 清空缓冲区，防止回车符没写入
				try {is.close(); } catch (Exception ignored) {}
				try {fis.close(); } catch (Exception ignored) {}
			}
    	}
    	os.close();
    	fos.close();
    	// 完成当前 ftp 命令
    	try {ftpClient.completePendingCommand(); } catch (Exception ignored) {}
    	return true;
    }
    
	final static int BLOCK_64K = 64 * 1024;
	final static int BLOCK_128K = 128 * 1024;
	final static int MAX_BLOCK_SIZE = 32 * 1024 * 1024;

	public static void int2Byte(byte[] b, int offset, int intValue) {
       for (int i = 0; i < 4; i++) {
            b[offset+i] = (byte) (intValue >> 8 * (3 - i) & 0xFF);
        }
    }
	
	/**
     * 上传一个本地文件到远程指定文件
     * 
     * @param remoteAbsoluteFile 远程文件名(包括完整路径)
     * @param localAbsoluteFile 本地文件名(包括完整路径)
     * @param autoClose 是否自动关闭当前连接
     * @return 成功时，返回true，失败返回false
     * @throws Exception
     */
    public boolean put(String remoteAbsoluteFile, InputStream input, boolean autoClose,boolean autoCompress) throws Exception {
        try {
            // 处理传输        	
            return getFTPClient().storeFile(remoteAbsoluteFile, input);
        } catch (FileNotFoundException e) {
            throw new Exception("local file not found.", e);
        } catch (IOException e) {
        	log.error("[" + host + "] " + e.getMessage());
            throw new Exception("Could not put file to server.", e);
        } finally {
            try {
                if (input != null) {
                    input.close();
                }
            } catch (Exception e) {
                throw new Exception("Couldn't close FileInputStream.", e);
            }
            if (autoClose) {
                disconnect(); //断开连接
            }
        }
    }
    
    
    /**
     * 上传一个本地文件到远程指定文件
     * @param remoteAbsoluteFile 远程文件名(包括完整路径)
     * @param localAbsoluteFile 本地文件名(包括完整路径)
     * @param autoClose 是否自动关闭当前连接
     * @return 成功时，返回true，失败返回false
     * @throws Exception
     */
    public boolean put(String remoteAbsoluteFile, String localAbsoluteFile, boolean autoClose,boolean autoCompress) throws Exception {
        InputStream input = null;
        try {
            // 处理传输      	
        	input = new FileInputStream(localAbsoluteFile);
        	return getFTPClient().storeFile(remoteAbsoluteFile, input);
        } catch (FileNotFoundException e) {
            throw new Exception("local file not found.", e);
        } catch (IOException e) {
            throw new Exception("Could not put file to server.", e);
        } finally {
            try 
            {
                if (input != null) {
                    input.close();
                }
            } 
            catch (Exception e) {
                throw new Exception("Couldn't close FileInputStream.", e);
            }
            
            if (autoClose) {
                disconnect(); //断开连接
            }
        }
    }
    
    public boolean put(byte[] bts, String remoteAbsoluteFile, boolean autoClose) throws Exception 
    {
    	ByteArrayInputStream input = null;
    	try {
            // 处理传输
        	input = new ByteArrayInputStream(bts);
            return getFTPClient().storeFile(remoteAbsoluteFile, input);
        } catch (FileNotFoundException e) {
            throw new Exception("local file not found.", e);
        } catch (IOException e) {
            throw new Exception("Could not put file to server.", e);
        } finally {
            try {
                if (input != null) {
                    input.close();
                }
            } catch (Exception e) {
                throw new Exception("Couldn't close FileInputStream.", e);
            }
            if (autoClose) {
                disconnect(); //断开连接
            }
        }
    }

    /**
     * 下载一个远程文件到本地的指定文件
     * 
     * @param remoteAbsoluteFile 远程文件名(包括完整路径)
     * @param localAbsoluteFile 本地文件名(包括完整路径)
     * @return 成功时，返回true，失败返回false
     * @throws Exception
     */
    public boolean get(String remoteAbsoluteFile, String localAbsoluteFile) throws Exception {
        return get(remoteAbsoluteFile, localAbsoluteFile, true);
    }
    
    /**
     * 重命名
     * 
     * @param newfile 原文件名称
     * @param newfile 新文件名称
     * @return 成功时，返回true，失败返回false
     * @throws Exception
     */
    public boolean rename(String oldfile, String newfile) throws Exception {
        return getFTPClient().rename(oldfile, newfile);
    }

    /**
     * 下载一个远程文件到本地的指定文件
     * 
     * @param remoteAbsoluteFile 远程文件名(包括完整路径)
     * @param localAbsoluteFile 本地文件名(包括完整路径)
     * @param autoClose 是否自动关闭当前连接
     * 
     * @return 成功时，返回true，失败返回false
     * @throws Exception
     */
    public boolean get(String remoteAbsoluteFile, String localAbsoluteFile, boolean autoClose) throws Exception {
        OutputStream output = null;
        try {
            output = new FileOutputStream(localAbsoluteFile);
            return get(remoteAbsoluteFile, output, autoClose, true);
        } catch (FileNotFoundException e) {
            throw new Exception("local file not found.", e);
        } finally {
            try {
                if (output != null) {
                    output.close();
                }
            } catch (IOException e) {
                throw new Exception("Couldn't close FileOutputStream.", e);
            }
        }
    }

    /**
     * 下载一个远程文件到指定的流 处理完后记得关闭流
     * 
     * @param remoteAbsoluteFile
     * @param output
     * @return
     * @throws Exception
     */
    @Override
    public boolean get(String remoteAbsoluteFile, OutputStream output) throws Exception {
        return get(remoteAbsoluteFile, output, false, false);
    }
    

    /**
     * 下载一个远程文件到指定的流 处理完后记得关闭流
     * 
     * @param remoteAbsoluteFile
     * @param output
     * @param delFile
     * @return
     * @throws Exception
     */
    public boolean get(String remoteAbsoluteFile, OutputStream output, boolean autoClose, boolean autoCloseOutPut) throws Exception {
        try {
            FTPClient ftpClient = getFTPClient();
            // 处理传输
            return ftpClient.retrieveFile(remoteAbsoluteFile, output);
        } catch (IOException e) {
            throw new Exception("Couldn't get file from server.", e);
        } finally {
        	if(autoCloseOutPut)
        	{
        		output.close();
        	}
        	
            if (autoClose) {
                disconnect(); //关闭链接
            }
        }
    }
    
    /**
     * ftp文件转成输入流
     * @param remoteAbsoluteFile
     * @return 输入流
     * @throws Exception
     */
    public InputStream getFtpInputStream(String remoteAbsoluteFile) throws Exception {
    	FTPClient ftpClient = getFTPClient();
		return ftpClient.retrieveFileStream(remoteAbsoluteFile);
    }

    /**
     * 从ftp服务器上删除一个文件
     * 该方法将自动关闭当前连接
     * 
     * @param delFile
     * @return
     * @throws Exception
     */
    public boolean delete(String delFile) throws Exception {
        return delete(delFile, true);
    }
    
    public boolean deleteDir(String delFile, boolean autoClose) throws Exception {
        try {
        	
        	FTPFile[] listNames = getFTPClient().listFiles(delFile);
        	for(FTPFile ffile:listNames)
        	{
        		if(ffile.isDirectory())
        		{
        			deleteDir(delFile+"/"+ffile.getName(),false);
        		}
        		else 
        		{
        			delete(delFile+"/"+ffile.getName(),false);
        		}       			
        	}
            getFTPClient().removeDirectory(delFile);
            return true;
        } catch (IOException e) {
            throw new Exception("Couldn't delete file from server.", e);
        } finally {
            if (autoClose) {
                disconnect(); //关闭链接
            }
        }
    }
    
    /**
     * 从ftp服务器上删除一个文件
     * 
     * @param delFile
     * @param autoClose 是否自动关闭当前连接
     * 
     * @return
     * @throws Exception
     */
    public boolean delete(String delFile, boolean autoClose) throws Exception {
        try {
            getFTPClient().deleteFile(delFile);
            return true;
        } catch (IOException e) {
            throw new Exception("Couldn't delete file from server.", e);
        } finally {
            if (autoClose) {
                disconnect(); //关闭链接
            }
        }
    }
    
    /**
     * 批量删除
     * 该方法将自动关闭当前连接
     * 
     * @param delFiles
     * @return
     * @throws Exception
     */
    public boolean delete(String[] delFiles) throws Exception {
        return delete(delFiles, true);
    }

    /**
     * 批量删除
     * 
     * @param delFiles
     * @param autoClose 是否自动关闭当前连接
     * 
     * @return
     * @throws Exception
     */
    public boolean delete(String[] delFiles, boolean autoClose) throws Exception {
        try {
            FTPClient ftpClient = getFTPClient();
            for (String s : delFiles) {
                ftpClient.deleteFile(s);
            }
            return true;
        } catch (IOException e) {
            throw new Exception("Couldn't delete file from server.", e);
        } finally {
            if (autoClose) {
                disconnect(); //关闭链接
            }
        }
    }

    /**
     * 列出远程目录下所有的文件
     * 
     * @param remotePath 远程目录名
     * @param autoClose 是否自动关闭当前连接
     * 
     * @return 远程目录下所有文件名的列表，目录不存在或者目录下没有文件时返回0长度的数组
     * @throws Exception
     */
    public Object[] listFiles(String remotePath, boolean autoClose) throws Exception  {
        try {
        	FTPClient ftpClient = getFTPClient();
        	FTPFile[] listNames = ftpClient.listFiles(remotePath);
        	FtpFile[] myFtp = new FtpFile[listNames.length];
        	for (int i = 0;i<listNames.length;i++)
			{
        		FTPFile ftpFile = listNames[i];
        		myFtp[i] =  new FtpFile(remotePath + "/" + ftpFile.getName(), ftpFile.getTimestamp().getTimeInMillis(), ftpFile.getSize());
			}
            return myFtp;
        } catch (IOException e) {
        	disconnect(); //关闭链接
            throw new Exception("列出远程目录下所有的文件时出现异常", e);           
        } finally {
            if (autoClose) {
                disconnect(); //关闭链接
            }
        }
    }
    
    public FTPFile[] listFTPFiles(String remotePath, boolean autoClose) throws Exception  {
    	try {
        	FTPClient ftpClient = getFTPClient();
        	FTPFile[] listNames = ftpClient.listFiles(remotePath);
        	return listNames;
        } catch (IOException e) {
        	disconnect(); //关闭链接
            throw new Exception("列出远程目录下所有的文件时出现异常", e);           
        } finally {
            if (autoClose) {
                disconnect(); //关闭链接
            }
        }
    }
    
    /**
     * 列出远程目录下所有的目录
     * @param remotePath 远程目录名
     * @param autoClose 是否自动关闭当前连接
     * 
     * @return 远程目录下所有文件名的列表，目录不存在或者目录下没有文件时返回0长度的数组
     * @throws Exception
     */
    public Object[] listDirs(String remotePath, boolean autoClose) throws Exception {
        try {
            FTPFile[] listNames = getFTPClient().listFiles(remotePath);
            FtpFile[] myFtp = new FtpFile[listNames.length];
        	for (int i = 0;i<listNames.length;i++)
			{
        		FTPFile ftpFile = listNames[i];
        		myFtp[i] =  new FtpFile(remotePath + "/" + ftpFile.getName(), ftpFile.getTimestamp().getTimeInMillis(), ftpFile.getSize());
			}
            return myFtp;
        } catch (IOException e) {
        	disconnect();
            throw new Exception("列出远程目录下所有的文件时出现异常", e);
        } finally {
            if (autoClose) {
                disconnect(); //关闭链接
            }
        }
    }
    
    public FTPFile[] listFTPDirs(String remotePath, boolean autoClose) throws Exception {
    	try {
            FTPFile[] listNames = getFTPClient().listFiles(remotePath);
            return listNames;
        } catch (IOException e) {
        	disconnect();
            throw new Exception("列出远程目录下所有的文件时出现异常", e);
        } finally {
            if (autoClose) {
                disconnect(); //关闭链接
            }
        }
    }
   
    public List<FTPFile> listFilesAll(String remotePath,String keyword) throws Exception {
        try {
        	List<FTPFile> listFiles = new ArrayList<FTPFile>();
        	FTPFile[] listNames = getFTPClient().listFiles(remotePath);
        	for(FTPFile ffile:listNames)
        	{
        		if(ffile.isDirectory())
        		{
        			List<FTPFile> listFilessub = listFilesAll(remotePath + "/" + ffile.getName(), keyword);
        			listFiles.addAll(listFilessub);
        		}
        		else if(keyword.length()==0 || ffile.getName().contains(keyword))
        		{
        			listFiles.add(ffile);
        		}       			
        	}
            return listFiles;
        } catch (IOException e) {
            throw new Exception("列出远程目录下所有的文件时出现异常", e);
        } 
    }
    
    public static String getParentPath(String path)
    {
    	int pos = path.replace("\\", "/").lastIndexOf("/");
    	if(pos>0)
    	{
    		return path.substring(0, pos);
    	}
    	return path;
    }
    
    public static String getName(String path)
    {
    	int pos = path.replace("\\", "/").lastIndexOf("/");
    	if(pos>0)
    	{
    		return path.substring(pos+1);
    	}
    	return path;
    } 
    
    public long getModificationTime(String path)
    {
    	Object ffile = getFileStatus(path);
    	if(ffile != null)
    	{
    		return getModificationTime((FTPFile)ffile);
    	}
    	return 0;
    }

	private long getModificationTime(FTPFile ffile) {
		CalendarEx cal = new CalendarEx(ffile.getTimestamp().getTime());
		return cal.GetTime();
	}
    
    public Object getFileStatus(String path)
	{
		log.info("getFileStatus：" + path);
		try
		{
			//Path curPath = new Path(path);
			if (checkDirExist(path))
			{
				Object[] fileStatus = listFTPFiles(getParentPath(path), false);
				int listlength = fileStatus.length;
				for (int i = 0; i < listlength; i++)
				{
					FTPFile ftpFile = (FTPFile)fileStatus[i];
					if (ftpFile.getName().equals(getName(path)))
					{
						log.info("getFileStatus：Success");
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
    
    public List<String> listFileNamesAll(String remotePath, String keyword, int waitMinute) throws Exception {
        try {
        	List<String> listFiles = new ArrayList<String>();
        	FTPFile[] listNames = getFTPClient().listFiles(remotePath);
        	for(FTPFile ffile:listNames)
        	{
        		if(ffile.isDirectory())
        		{
        			List<String> listFilessub = listFileNamesAll(remotePath + "/" + ffile.getName(), keyword, waitMinute);
        			listFiles.addAll(listFilessub);
        		}
        		else if(keyword.length()==0 || ffile.getName().contains(keyword))
        		{
        			listFiles.add((remotePath + "/" + ffile.getName()).replace("//", "/"));
        		}       			
        	}
            return listFiles;
        } catch (IOException e) {
            throw new Exception("列出远程目录下所有的文件时出现异常", e);
        } 
    }
    
    public long getFilesize(String remotePath){
    	long size = 0;
    	try {
			FTPFile[] FTPFile = getFTPClient().listFiles(remotePath);
			size = FTPFile[0].getSize();			
		} catch (Exception e) {
			log.error("[" + host + "] " + "path : " + remotePath + "size ：" + size);
			log.error(HadoopFSOperations.getTrace(e));
		}
    	return size;
    }
    
    /**
     * 复制文件到另外一个目录
     * @param sourceFileName：文件名
     * @param sourceDir：数据目录
     * @param targetDir：目标目录
     * @return
     * @throws Exception 
     */
    public boolean copyFile(String sourceFileName, String sourceDir, String targetDir) throws Exception {
        ByteArrayInputStream in = null;
        ByteArrayOutputStream fos = new ByteArrayOutputStream();
        int reply = 0;
        FTPClient ftpClient = getFTPClient();
        reply = ftpClient.getReplyCode();
        try {
            if (!FTPReply.isPositiveCompletion(reply)) {
                ftpClient.disconnect();
            } else {
                if (!ftpClient.changeWorkingDirectory(targetDir)) {
                    Boolean flag = this.createMultiDir(ftpClient, targetDir.replaceAll("\\\\", "/"));// 创建多层目录
                    
                    if (!flag) {
                        return false;
                    }
                    ftpClient.changeWorkingDirectory(targetDir);
                }
                ftpClient.setBufferSize(1024 * 2);
                ftpClient.changeWorkingDirectory(sourceDir);// 变更工作路径   
                ftpClient.enterLocalPassiveMode();
                ftpClient.setFileType(FTP.BINARY_FILE_TYPE);// 设置以二进制流的方式传输
                ftpClient.retrieveFile(new String(sourceFileName.getBytes("UTF-8"), "iso-8859-1"), fos);// 将文件读到内存中
                in = new ByteArrayInputStream(fos.toByteArray());
                if (in != null) {
                    ftpClient.changeWorkingDirectory(targetDir);
                    ftpClient.storeFile(new String(sourceFileName.getBytes("UTF-8"), "iso-8859-1"), in);
                }
            }
        } finally {
            // 关闭流  
            if (in != null) {
                in.close();
            }
            if (fos != null) {
                fos.close();
            }
        }
        return true;
    }
    
    /**
     * 创建多层目录
     * @param ftpClient
     * @param multiDir
     * @return
     * @throws IOException
     */
    public boolean createMultiDir(FTPClient ftpClient, String multiDir) throws IOException {
        boolean bool = false;
        String[] dirs = multiDir.split("/");
        ftpClient.changeWorkingDirectory("/");
        for (int i = 1; dirs != null && i < dirs.length; i++) {
            if (!ftpClient.changeWorkingDirectory(dirs[i])) {
                if (ftpClient.makeDirectory(dirs[i])) {
                    if (!ftpClient.changeWorkingDirectory(dirs[i])) {
                        return false;
                    }
                }
            }
            bool = true;
        }
        return bool;
    }

    public static void main(String[] args) throws Exception, InterruptedException {
    	
        FTPClientHelper ftp = new FTPClientHelper("192.168.3.124",21,"dtauser","dtauser");        
        ftp.setBinaryTransfer(true);
        ftp.setPassiveMode(true);
        ftp.setEncoding("utf-8");
   }
} 
