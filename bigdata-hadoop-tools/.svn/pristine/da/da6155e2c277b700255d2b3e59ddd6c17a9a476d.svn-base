package cn.mastercom.bigdata.util.ftp;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import com.jcraft.jsch.*;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import cn.mastercom.bigdata.util.hadoop.hdfs.HadoopFSOperations;

public class SftpClientHelper implements IFtpClientHelper {

	private String host = "192.168.1.172";
	private String username = "ftp";
	private String password = "ftp";
	private int port = 22;
	private int timeout = 1000;
	private ChannelSftp sftp = null;
	protected final Logger  log = Logger.getLogger(getClass());
	private ThreadLocal<ChannelSftp> sftpThreadLocal = new ThreadLocal<ChannelSftp>();
	private String encode = "utf-8";
	private String priKeyFile;
	private String passphrase;

	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public ChannelSftp getSftp() {
		return sftp;
	}

	public void setSftp(ChannelSftp sftp) {
		this.sftp = sftp;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
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

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void setEncoding(String encode) {
		this.encode = encode;
	}
	
	@Override
	public void setPassiveMode(boolean passMode)
	{
				
	}

	@Override
	public void setBinaryTransfer(boolean binaryTransfer)
	{
			
	}
	

	/**
	 * connect server sftp
	 * @return boolean
	 * @throws Exception
	 */
	@SuppressWarnings("rawtypes")
	public boolean connect() throws Exception {
		try {
			JSch jsch = new JSch();
			addPrikey(jsch);
			jsch.getSession(username, host, port);
			Session sshSession = jsch.getSession(username, host, port);
			if (password != null) {
				sshSession.setPassword(password);
			}
			Properties sshConfig = new Properties();
			sshConfig.put("StrictHostKeyChecking", "no");
			sshConfig.put("PreferredAuthentications","password,keyboard-interactive");
			sshSession.setConfig(sshConfig);
			sshSession.connect();
			Channel channel = sshSession.openChannel("sftp");
			channel.connect();
			sftp = (ChannelSftp) channel;
			Class cl = ChannelSftp.class;
			Field f =cl.getDeclaredField("server_version");
			f.setAccessible(true);
			f.set(sftp, 2);
			sftp.setFilenameEncoding(encode);
			if (sftp.isConnected()) {
				log.info("[" + host + "] " + "Connected to " + host + ":" + port );
				sftpThreadLocal.set(sftp);
				return true;
			} else {
				log.info("[" + host + "] " + "SFTP server refused connection.");
				throw new Exception("SFTP server refused connection.");				
			}
		} catch (Exception e) {
			log.error("[" + host + "] " + HadoopFSOperations.getTrace(e));
			e.printStackTrace();
			if (sftp.isConnected()) {
				try {
					sftp.disconnect(); // 断开连接
				} catch (Exception e1) {
					throw new Exception("Could not disconnect from server.", e1);
				}
			}

		}
		return false;
	}
	
	/**
	 * 添加秘钥
	 * @param jsch
	 * @throws JSchException
	 */
	private void addPrikey(JSch jsch) throws JSchException {
		if(priKeyFile != null) {
			if(priKeyFile !=null && !"".equals(priKeyFile)){ 
				if(passphrase !=null && !"".equals(passphrase))
				{ 
					jsch.addIdentity(priKeyFile, passphrase); 
				}else{ 
					jsch.addIdentity(priKeyFile); 
				} 
			} 
		}
	}
	
	/**
	 * Disconnect with server
	 */
	public void disconnect() {
		if (sftp != null) {
			if (sftp.isConnected()) {				
				try {
					sftp.getSession().disconnect();
					sftp.quit();
					sftp.disconnect();
				} catch (JSchException e) {
					e.printStackTrace();
				}				
				log.info("[" + host + "] " + "sftp is closed success");
			} else if (sftp.isClosed()) {
				log.info("[" + host + "] " + "sftp is closed already");
			}
		}
	}

	public SftpClientHelper(String host, String username, String password, int port, int time) {
		this.host = host;
		this.username = username;
		this.password = password;
		this.port = port;
		this.timeout = time;
	}
	
	public SftpClientHelper(String host, String username, String password, int port, int time,String priKeyFile,String passphrase) {
		this.host = host;
		this.username = username;
		this.password = password;
		this.port = port;
		this.timeout = time;
		this.priKeyFile = priKeyFile;
		this.passphrase = passphrase;
	}

	/**
	 * 根据路径创建文件夹.
	 * @param dir 路径 必须是 /xxx/xxx/xxx/ 不能就单独一个/
	 * @param sftp sftp连接
	 * @throws Exception 异常
	 */
	public static boolean mkdir(final String dir, final ChannelSftp sftp) throws Exception {
		try {
			if (StringUtils.isBlank(dir))
				return false;
			String md = dir.replaceAll("\\\\", "/");
			if (md.indexOf("/") != 0 || md.length() == 1)
				return false;
			return mkdirs(md, sftp);
		} catch (Exception e) {
			exit(sftp);
			throw e;
		}
	}

	/**
	 * 递归创建文件夹.
	 * @param dir 路径
	 * @param sftp sftp连接
	 * @return 是否创建成功
	 * @throws SftpException 异常
	 */
	public static boolean mkdirs(final String dir, final ChannelSftp sftp) throws SftpException {
		String dirs = dir.substring(1, dir.length() - 1).replace("\\","/");
		String[] dirArr = dirs.split("/");
		String base = "";
		for (String d : dirArr) {
			base += "/" + d;
			if (dirExist(base + "/", sftp)) {
				continue;
			} else {
				sftp.mkdir(base + "/");
			}
		}
		return true;
	}

    /**
     * 判断文件是否存在
     * @param filePath
     * @param sftp
     * @return
     */
	public static boolean checkDirExist(String filePath, ChannelSftp sftp){
        // 判断子目录文件夹是否存在
        SftpATTRS attrs = null;
        try {
            attrs = sftp.stat(filePath);
            if(attrs != null){
                return true;
            }
        } catch (Exception e) {
            return false;
        }
        return  false;
    }

	/**
	 * 格式化路径.
	 * @param srcPath 原路径. /xxx/xxx/xxx.yyy 或 X:/xxx/xxx/xxx.yy
	 * @return list, 第一个是路径（/xxx/xxx/）,第二个是文件名（xxx.yy）
	 */
	public static List<String> formatPath(final String srcPath) {
		List<String> list = new ArrayList<String>(2);
		String dir = "";
		String fileName = "";
		String repSrc = srcPath.replaceAll("\\\\", "/");
		int firstP = repSrc.indexOf("/");
		int lastP = repSrc.lastIndexOf("/");
		fileName = repSrc.substring(lastP + 1);
		dir = repSrc.substring(firstP, lastP);
		dir = "/" + (dir.length() == 1 ? dir : (dir + "/"));
		list.add(dir);
		list.add(fileName);
		return list;
	}

	/***
	 * upload file to remotePath
	 */

	public void put(String remoteAbsoluteFile, String localAbsoluteFile) throws Exception {
		put(remoteAbsoluteFile, localAbsoluteFile, false, true);
	}

	public void put(String remoteAbsoluteFile, InputStream is) throws Exception {
		put(remoteAbsoluteFile, is, false);
	}
	
	public boolean put(String remoteAbsoluteFile, InputStream is,String fileName) throws Exception {
		return put(remoteAbsoluteFile, is,fileName, false);
	}

	public void putFromHdfs(String remoteAbsoluteFile, String hdfsFile, HadoopFSOperations hdfs) throws Exception {
		put(remoteAbsoluteFile, hdfs.getInputStream(hdfsFile), false);
	}

	public void putFromToHdfs(String remoteAbsoluteFile, String hdfsFile, HadoopFSOperations hdfs) throws Exception {
		get(remoteAbsoluteFile, hdfs.getOutputStream(hdfsFile));
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public Object[] listFiles(String remotePath, boolean autoClose) throws Exception
	{
		try {
			Vector ls = null;
			try {
				ls = getSFTPClient().ls(remotePath);
			} catch (Exception e) {
//				log.error("[" + host + "] " + "sftp 获取列表失败........." + HadoopFSOperations.getTrace(e));
				return null;
			}
			SftpFile[] file = new SftpFile[ls.size() - 2];
			int count = 0;
			for (Object obj : ls) {
				if (obj instanceof com.jcraft.jsch.ChannelSftp.LsEntry) {
					com.jcraft.jsch.ChannelSftp.LsEntry entry = (com.jcraft.jsch.ChannelSftp.LsEntry) obj;
					String fileName = entry.getFilename();
					if (fileName.equals(".") || fileName.equals("..")) {
						continue;
					}					
					long size = entry.getAttrs().getSize();					
					int lastModelTime = entry.getAttrs().getMTime();
					file[count] = new SftpFile(remotePath + fileName,lastModelTime,size);
					count++;
				}
			}
			return file;
		} catch (Exception e) {
			log.error("[" + host + "] " + "sftp "  + HadoopFSOperations.getTrace(e));
			return null;
		} finally {
			if (autoClose) {
				disconnect(); // 关闭链接
			}
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Object[] listDirs(String remotePath, boolean autoClose) throws Exception
	{
		try {
			Vector ls = null;
			try {
				ls = getSFTPClient().ls(remotePath);
			} catch (Exception e) {
//				log.error("[" + host + "] " + "sftp 获取列表失败........." + HadoopFSOperations.getTrace(e));
				return null;
			}
			SftpFile[] file = new SftpFile[ls.size() - 2];
			int count = 0;
			for (Object obj : ls) {
				if (obj instanceof com.jcraft.jsch.ChannelSftp.LsEntry) {
					com.jcraft.jsch.ChannelSftp.LsEntry entry = (com.jcraft.jsch.ChannelSftp.LsEntry) obj;
					String fileName = entry.getFilename();
					if (fileName.equals(".") || fileName.equals("..")) {
						continue;
					}					
					long size = entry.getAttrs().getSize();					
					int lastModelTime = entry.getAttrs().getMTime();
					file[count] = new SftpFile(remotePath + fileName,lastModelTime,size);
					count++;
				}
			}
			return file;
		} catch (Exception e) {
			log.error("[" + host + "] " + "sftp "  + HadoopFSOperations.getTrace(e));
			return null;
		} finally {
			if (autoClose) {
				disconnect(); // 关闭链接
			}
		}
	}
	
	/**
	 * 上传一个本地文件到远程指定文件夹
	 * @param remoteAbsoluteFile 远程文件名(包括完整路径)
	 * @param localAbsoluteFile 本地文件名(包括完整路径)
	 * @param autoClose 是否自动关闭当前连接
	 */
	public void put(String remoteAbsoluteFile, String localAbsoluteFile, boolean autoClose, boolean autoCompress)
			throws Exception {
		File srcFile = new File(localAbsoluteFile);
		InputStream input = null;
		try {
			input = new FileInputStream(localAbsoluteFile);
			remoteAbsoluteFile = remoteAbsoluteFile.replaceAll("\\\\", "/");
			getSFTPClient().cd(remoteAbsoluteFile);
			getSFTPClient().put(input, srcFile.getName());
		} catch (Exception e) {
			throw new Exception("local file not found. or change dir exception", e);
		} finally {
			try {
				if (input != null) {
					input.close();
				}
			} catch (Exception e) {
				throw new Exception("Couldn't close FileInputStream.", e);
			}
			if (autoClose) {
				disconnect();
			}
		}
	}
	
	/**
	 * 多文件合并上传sftp
	 * OVERWRITE 完全覆盖模式，这是JSCH的默认的文件传输模式，即目标文件存在则传输文件完全覆盖掉目标文件；
	 * RESUME 恢复模式，即断点续传，当传输中断，下次传输时从上一次中断处开始；
	 * APPEND 追加模式，若目标文件已经存在，则传输文件将在目标文件后追加；
	 * @param localSrcPath
	 * @param ftpDestFile
	 * @param compress
	 * @return
	 * @throws Exception
	 */
	public boolean putMergeLocalToSftp(String localSrcPath, String ftpDestFile, boolean compress,boolean close) throws Exception {
    	if (localSrcPath==null||ftpDestFile==null) {
    		return false;    	
    	}
    	// localDestFile 判断后缀名是否为 gz 
    	if (compress && !ftpDestFile.endsWith(".gz")) {
    		ftpDestFile+=".gz";
    	}
    	
    	ChannelSftp sftp = getSFTPClient();
    	File[] files = new File(localSrcPath).listFiles(new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				return !pathname.isDirectory();
			}
		});
    	for (File file : files)
		{
    		InputStream fis = null;
    		try
			{
    			if(file.getName().endsWith(".gz")) {
    				fis = new FileInputStream(file);
    				sftp.put(fis, ftpDestFile, new SFTPProgressMonitor(), ChannelSftp.APPEND);
    			}else {
    				sftp.put(file.getPath(), ftpDestFile, new SFTPProgressMonitor(), ChannelSftp.APPEND);
    			}				
			}
			catch (Exception e)
			{
				log.error("file upload error : " + file.getPath());
				sftp.disconnect();
				return false;
			}finally {
				if(fis != null) {
					fis.close();
				}			
			}
		}
    	if(close) {
    		sftp.disconnect();
    	}
    	return true;
    }
	
	/**
	 * 把输入流输出到远程指定文件
	 * @param remoteAbsoluteFile 远程文件名(包括完整路径)
	 * @param input 输入流
	 * @param autoClose 是否自动关闭当前连接
	 */
	public void put(String remoteAbsoluteFile, InputStream input, boolean autoClose) throws Exception {
		try {
			remoteAbsoluteFile = remoteAbsoluteFile.replaceAll("\\\\", "/");
			getSFTPClient().cd(remoteAbsoluteFile);
			getSFTPClient().put(input, "1.tar");
		} catch (Exception e) {
			throw new Exception("local file not found.", e);
		} finally {
			try {
				if (input != null) {
					input.close();
				}
			} catch (Exception e) {
				throw new Exception("Couldn't close FileInputStream.", e);
			}
			if (autoClose) {
				disconnect(); // 断开连接
			}
		}
	}
	
	/**
	 * 把输入流输出到远程指定文件夹下，并且可以指定文件名
	 * @param remoteAbsoluteFile 远程文件名(包括完整路径)
	 * @param input 输入流
	 * @param fileName 文件名
	 * @param autoClose 是否关闭连接
	 * @return boolean 返回值
	 * @throws Exception
	 */
	public boolean put(String remoteAbsoluteFile, InputStream input,String fileName, boolean autoClose) throws Exception {
		try {
			remoteAbsoluteFile = remoteAbsoluteFile.replaceAll("\\\\", "/");
			getSFTPClient().cd(remoteAbsoluteFile);
			getSFTPClient().put(input, fileName);			
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
			try {
				if (input != null) {
					input.close();
				}
			} catch (Exception e) {
				e.printStackTrace();				
				return false;
			}
			if (autoClose) {
				disconnect(); // 断开连接
			}
		}
		return true;
	}

	/**
	 * 下载一个远程文件到指定的流 处理完后记得关闭流
	 * @param remoteAbsoluteFile
	 * @param output
	 * @return
	 * @throws Exception
	 */
	public boolean get(String remoteAbsoluteFile, OutputStream output) throws Exception {
		File file = new File(remoteAbsoluteFile);
		String parent = file.getParent().replace("\\", "/");
		try
		{
			getSFTPClient().cd(parent);
			getSFTPClient().get(file.getName(), output);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	/**
	 * 下载远程文件到本地
	 * @param remoteAbsoluteFile
	 * @param localAbsoluteFile
	 * @param autoClose
	 * @throws Exception
	 */
	public void get(String remoteAbsoluteFile, String localAbsoluteFile, boolean autoClose) throws Exception {
		OutputStream output = null;
		try {
			output = new FileOutputStream(localAbsoluteFile);
			get(remoteAbsoluteFile, output);
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
	 * 重命名
	 * @param newfile 原文件名称
	 * @param newfile 新文件名称
	 * @throws Exception
	 */
	public void rename(String oldfile, String newfile) throws Exception {
		getSFTPClient().rename(oldfile, newfile);
	}

	/**
	 * 删除文件-sftp协议.
	 * @param deleteFile 要删除的文件
	 * @param sftp sftp连接
	 * @throws Exception 异常
	 */
	public static void rmFile(final String deleteFile, final ChannelSftp sftp) throws Exception {
		try {
			sftp.rm(deleteFile);
		} catch (Exception e) {
			exit(sftp);
			throw e;
		}
	}

	/**
	 * 判断文件夹是否存在.
	 * @param dir 文件夹路径， /xxx/xxx/
	 * @param sftp sftp协议
	 * @return 是否存在
	 */
	public static boolean dirExist(final String dir, final ChannelSftp sftp) {
		try {
			Vector<?> vector = sftp.ls(dir);
			if (null == vector) {
				return false;
			}
			else {
				return true;
			}
		} catch (SftpException e) {
			return false;
		}
	}

	/**
	 * 关闭协议-sftp协议.
	 * @param sftp
	 * sftp连接
	 */
	public static void exit(final ChannelSftp sftp) {
		sftp.exit();
	}

	/**
	 * 返回一个SFTPClient实例
	 * @throws Exception
	 */
	public ChannelSftp getSFTPClient() throws Exception {
		if (sftpThreadLocal.get() != null && sftpThreadLocal.get().isConnected()) {
			try {
				if(sftpThreadLocal.get().ls("/") !=null && sftpThreadLocal.get().ls("/").size() >0)
				{
					return sftpThreadLocal.get();
				}
			} catch (Exception e) {
				log.info("[" + host + "] " + "sftp disconnect " + e.toString());
			} 
		} else {
			connect(); // 连接到sftp服务器			
		}
		return sftpThreadLocal.get();
	}

	public static void main(String[] args) throws Exception {
		SftpClientHelper sftphelp = new SftpClientHelper("192.168.1.31", "hmaster", "mastercom168", 22, 1000);
		sftphelp.setEncoding("utf-8");
        System.out.println(checkDirExist("/home/hmaster/mm/2018" , sftphelp.getSFTPClient()));
        if(!checkDirExist("/home/hmaster/mm/2018" , sftphelp.getSFTPClient())){
            System.out.println(mkdirs("/home/hmaster/mm/2018/", sftphelp.getSFTPClient()));
        }

	}

}