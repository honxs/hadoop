package com.chinamobile.util;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;


public class FtpUtil implements FTPLoginDownload
{

	private FTPClient ftpClient;
	private String province;
	private String vendor;
	private String servername;
	private String strIp;
	private int intPort;
	private String user;
	private String password;
	private String transmissionmode;
	private boolean isLogin;

	private static org.slf4j.Logger logger = LoggerFactory.getLogger(FtpUtil.class);

	/* *
	 * Ftp构造函数
	 */
	public FtpUtil(String province, String vendor, String servername,
			String strIp, int intPort, String user, String Password,
			String transmissionmode)
	{
		this.province = province;
		this.vendor = vendor;
		this.servername = servername;
		this.strIp = strIp;
		this.intPort = intPort;
		this.user = user;
		this.password = Password;
		this.transmissionmode = transmissionmode;
		this.isLogin = false;
	}

	/**
	 * @return 判断是否登入成功
	 * */
	public synchronized String[] ftpLogin()
	{
        this.ftpClient = new FTPClient();

		String[] result = new String[2];
		logger.info("Begin to connect " + this.province + " " + this.vendor
				+ " " + this.servername + " " + this.strIp + " " + this.intPort
				+ " " + this.transmissionmode + " ");
		FTPClientConfig ftpClientConfig = new FTPClientConfig();
		ftpClientConfig.setServerTimeZoneId(TimeZone.getDefault().getID());
		this.ftpClient.setControlEncoding("GBK");
		this.ftpClient.configure(ftpClientConfig);
		
		int reply = -1;
		String replyString = null;
		
		try
		{
			if (this.intPort > 0)
			{
				
				//this.ftpClient.addProtocolCommandListener(new PrintCommandListener(new PrintWriter(System.out), true));
				
				this.ftpClient.connect(this.strIp, this.intPort);
			} else
			{
				this.ftpClient.connect(this.strIp);
			}
			// FTP服务器连接回答
			reply = this.ftpClient.getReplyCode();
			replyString = this.ftpClient.getReplyString();
			boolean islogin = this.ftpClient.login(this.user, this.password);

			if (!(FTPReply.isPositiveCompletion(reply) && islogin))
			{
				if (!FTPReply.isPositiveCompletion(reply))
					logger.error(this.province + " " + this.vendor + " "
							+ this.servername + " " + this.strIp + "返回错误代码:"
							+ reply);
				else if (!islogin)
					logger.error(this.province + " " + this.vendor + " "
							+ this.servername + " " + this.strIp + "用户名口令错误");

                ftpLogout();
				this.isLogin = false;
				result[0] = String.valueOf(false);
				result[1] = "ReturnCode: " + String.valueOf(reply) + " ReturnString: " + replyString;
				return result;
			}

			logger.info(this.province + " " + this.vendor + " "
					+ this.servername + " " + this.strIp + ": "
					+ this.ftpClient.getReplyString());
			
			
//			// 设置传输协议
//			if (this.transmissionmode.equalsIgnoreCase("active"))
//				this.ftpClient.enterLocalActiveMode();
//			else
//				this.ftpClient.enterLocalPassiveMode();
			
			this.ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
			this.ftpClient.setBufferSize(10240000*2);
			this.ftpClient.setDefaultTimeout(1800000*2);  //30 minutes
			this.ftpClient.setConnectTimeout(1800000*2);
			this.ftpClient.setDataTimeout(1800000*2);
			this.ftpClient.setSoTimeout(1800000*2);
			this.ftpClient.setControlKeepAliveTimeout(1800*2);
			this.isLogin = true;
			result[0] = String.valueOf(true);
			result[1] = "ReturnCode: " + String.valueOf(reply) + " ReturnString: " + replyString;
			return result;
		} catch (Exception e)
		{
            ftpLogout();
			e.printStackTrace();
			logger.error(this.province + " " + this.vendor + " "
					+ this.servername + " " + this.strIp + "登录FTP服务失败！"
					+ e.getMessage());
			this.isLogin = false;
			result[0] = String.valueOf(false);
			result[1] = "ReturnCode: " + String.valueOf(reply) + " Exception: " + e.getMessage();
			return result;
		} finally
		{

		}
	}

	/**
	 * @退出关闭服务器链接
	 * */
	@SuppressWarnings("finally")
	public synchronized boolean ftpLogout()
	{
		if (null != this.ftpClient)
		{
			try
			{
				boolean result = this.ftpClient.logout();// 退出FTP服务器
				if (result)
				{
					logger.info(this.province + " " + this.vendor + " "
							+ this.servername + " " + this.strIp + "成功退出服务器");
				}
				else
				{
					logger.info(this.province + " " + this.vendor + " "
							+ this.servername + " " + this.strIp + "退出服务器失败");
				}
					
			} catch (IOException e)
			{
				e.printStackTrace();
				logger.warn(this.province + " " + this.vendor + " "
						+ this.servername + " " + this.strIp + "退出FTP服务器异常！"
						+ e.getMessage());
				return false;
			} finally
			{
				try
				{
					if(this.ftpClient!=null && this.ftpClient.isConnected())
					{						
						this.ftpClient.disconnect();// 关闭FTP服务器的连接
					}

                    //待确认效果
                    if(this.ftpClient!=null) {
                        this.ftpClient = null;
                    }

					return true;
				} catch (IOException e)
				{
					e.printStackTrace();
					logger.warn(this.province + " " + this.vendor + " "
							+ this.servername + " " + this.strIp
							+ "关闭FTP服务器的连接异常！");
					return false;
				}
			}
	}
		else
        {
            logger.info(this.province + " " + this.vendor + " "
                    + this.servername + " " + this.strIp + "已经为NULL退出");
            return true;
        }

		
	}

	/***
	 * 下载文件
	 * 
	 * @param remoteFileName
	 *            待下载文件名称
	 * @param localDires
	 *            下载到当地那个路径下
	 * @param remoteDownLoadPath
	 *            remoteFileName所在的路径
	 * */

	public boolean downloadFile(String remoteFileName,String newFileName, String localDires,
			String remoteDownLoadPath)
	{
		if (!remoteDownLoadPath.startsWith("/"))
		{
			remoteDownLoadPath = "/".concat(remoteDownLoadPath);
		}
		if (!remoteDownLoadPath.endsWith("/"))
		{
			remoteDownLoadPath = remoteDownLoadPath.concat("/");
		}
        String strFilePath = localDires + newFileName;
		OutputStream outStream = null;
		boolean isDirectoryExist = false;
		boolean success = false;
		try
		{
			if (this.transmissionmode.equalsIgnoreCase("active"))
				this.ftpClient.enterLocalActiveMode();
			else
				this.ftpClient.enterLocalPassiveMode();
			this.ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
			isDirectoryExist = this.ftpClient.changeWorkingDirectory(remoteDownLoadPath);
			if (!isDirectoryExist)
			{
				logger.error(this.province + "|" + this.vendor + "|" + this.strIp
						+ "|" + remoteDownLoadPath + "目录未找到");
			}
			if (isDirectoryExist)
			{
                FileUtil.checkDirExistsAndMkDir(new File(localDires));
				File localFile = new File(strFilePath);
				outStream = new FileOutputStream(localFile);			
				success = this.ftpClient.retrieveFile(remoteFileName, outStream);
			}
		} catch (Exception e)
		{
			e.printStackTrace();
			logger.error("exception:"+e.getMessage());
			logger.error(this.province + "|" + this.vendor + "|" + this.strIp
					+ "|" + remoteFileName + "下载失败");
		} finally
		{
			try
			{
				if(outStream!=null)
				{
					outStream.flush();
					outStream.close();
				}
			} catch (IOException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if (success == false)
		{
			logger.error(this.province + "|" + this.vendor + "|"
					+ this.strIp + "|" + remoteFileName + "下载失败: success : false");
			return false;
		}
		else
		{
			return true;
		}
	}

	/***
	 * @上传文件夹
	 * @param localDirectory
	 *            当地文件夹
	 * @param remoteDirectoryPath
	 *            Ftp 服务器路径 以目录"/"结束
	 * */
	public boolean uploadDirectory(String localDirectory,
			String remoteDirectoryPath)
	{
		File src = new File(localDirectory);
		try
		{
			remoteDirectoryPath = remoteDirectoryPath + src.getName() + "/";
			this.ftpClient.makeDirectory(remoteDirectoryPath);
			// ftpClient.listDirectories();
		} catch (IOException e)
		{
			e.printStackTrace();
			logger.info(remoteDirectoryPath + "目录创建失败");
		}
		File[] allFile = src.listFiles();
		for (int currentFile = 0; currentFile < allFile.length; currentFile++)
		{
			if (!allFile[currentFile].isDirectory())
			{
				String srcName = allFile[currentFile].getPath().toString();
				uploadFile(new File(srcName), remoteDirectoryPath);
			}
		}
		for (int currentFile = 0; currentFile < allFile.length; currentFile++)
		{
			if (allFile[currentFile].isDirectory())
			{
				// 递归
				uploadDirectory(allFile[currentFile].getPath().toString(),
						remoteDirectoryPath);
			}
		}
		return true;
	}

	/***
	 * @下载文件夹
	 * @param localDirectoryPath
	 * @param remoteDirectory
	 *            远程文件夹
	 * */
	public boolean downLoadDirectory(String localDirectoryPath,
			String remoteDirectory)
	{
		if (!remoteDirectory.startsWith("/"))
		{
			remoteDirectory = "/".concat(remoteDirectory);
		}
		if (!remoteDirectory.endsWith("/"))
		{
			remoteDirectory = remoteDirectory.concat("/");
		}
		try
		{
			String fileName = new File(remoteDirectory).getName();
			localDirectoryPath = localDirectoryPath + fileName + "//";
			new File(localDirectoryPath).mkdirs();
			FTPFile[] allFile = this.ftpClient.listFiles(remoteDirectory);
			for (int currentFile = 0; currentFile < allFile.length; currentFile++)
			{
				if (!allFile[currentFile].isDirectory())
				{
					downloadFile(allFile[currentFile].getName(),allFile[currentFile].getName(),
							localDirectoryPath, remoteDirectory);    //待修改，目前是保持原文件名
				}
			}
			for (int currentFile = 0; currentFile < allFile.length; currentFile++)
			{
				if (allFile[currentFile].isDirectory())
				{
					String strremoteDirectoryPath = remoteDirectory + "/"
							+ allFile[currentFile].getName();
					downLoadDirectory(localDirectoryPath,
							strremoteDirectoryPath);
				}
			}
		} catch (IOException e)
		{
			e.printStackTrace();
			logger.info(this.province + "|" + this.vendor + "|" + this.strIp
					+ "|" + "下载文件夹失败");
			return false;
		}
		return true;
	}

	// FtpClient的Set 和 Get 函数
	public FTPClient getFtpClient()
	{
		return ftpClient;
	}

	public void setFtpClient(FTPClient ftpClient)
	{
		this.ftpClient = ftpClient;
	}

	/**
	 * 取得相对于当前连接目录的某个目录下极其子目录下所有文件列表
	 * 
	 * @param remotePath
	 * @return
	 */
	public void listAllFiles(String remotePath)
	{
		String[] result = this.ftpLogin();
		if (Boolean.valueOf(result[0]))
		{
			try
			{
				if (!remotePath.startsWith("/"))
				{
					remotePath = "/".concat(remotePath);
				}
				if (!remotePath.endsWith("/"))
				{
					remotePath = remotePath.concat("/");
				}
				if (remotePath.startsWith("/") && remotePath.endsWith("/"))
				{
					FTPFile[] files = ftpClient.listFiles(remotePath);
					for (int i = 0; i < files.length; i++)
					{
						if (files[i].isDirectory())
						{
							listAllFiles(remotePath + files[i].getName() + "/");
						}
					}
				}
			} catch (IOException e)
			{
				e.printStackTrace();
			}
		}
	}

	// 仅列出某一目录下的所有文件，忽略子目录
	public HashSet<String> listDirectoryFiles(String remotePath)
	{
		
		if (this.transmissionmode.equalsIgnoreCase("active"))
			this.ftpClient.enterLocalActiveMode();
		else
			this.ftpClient.enterLocalPassiveMode();
		
		HashSet<String> tmpfilelist = new HashSet<String>();
		if (!remotePath.startsWith("/"))
		{
			remotePath = "/".concat(remotePath);
		}
		if (!remotePath.endsWith("/"))
		{
			remotePath = remotePath.concat("/");
		}
		if (this.isLogin)
		{
			try
			{
				FTPFile[] files = ftpClient.listFiles(remotePath);
				if (files == null)
				{
					return tmpfilelist;
				}
				for (FTPFile file : files)
				{
					tmpfilelist.add(file.getName());
				}
				return tmpfilelist;
				
			} catch (IOException e)
			{
                ftpLogout();
				e.printStackTrace();
				return tmpfilelist;
			}
		}
		else
		{
			return tmpfilelist;
		}
	}
	
	
	// 根据文件列表，得到“文件大小”和“最后修改时间”属性
	public HashMap<String,Long[]> getFileProperty(String remotePath,HashSet ftpFileName)
	{
		
		if (this.transmissionmode.equalsIgnoreCase("active"))
			this.ftpClient.enterLocalActiveMode();
		else
			this.ftpClient.enterLocalPassiveMode();
		
		HashMap<String,Long[]> tmpFilePropertyList = new HashMap<String,Long[]>();
		if (!remotePath.startsWith("/"))
		{
			remotePath = "/".concat(remotePath);
		}
		if (!remotePath.endsWith("/"))
		{
			remotePath = remotePath.concat("/");
		}
		
		if (this.isLogin && ftpFileName.size()>0)
		{
			try
			{
				//http://www.java2s.com/Code/JavaAPI/java.io/FilelistFilesFilenameFilterfilter.htm
				FTPFile[] files = ftpClient.listFiles(remotePath);  //filter to be used here
				if (files == null)
				{
					logger.info(remotePath + "is blank");
					return null;
				}
				for (FTPFile file : files)
				{
					String tmpFileName = file.getName();
					if(ftpFileName.contains(tmpFileName))
					{
						Long[] tmpSizeTimestamp = new Long[2];
						tmpSizeTimestamp[0] = file.getSize();
						//System.out.println("Size of "+ tmpFileName + " : "+tmpSizeTimestamp[0]);
						tmpSizeTimestamp[1] = file.getTimestamp().getTimeInMillis();
						//System.out.println("time of "+ tmpFileName + " : "+TimeUtil.getTimeStringFromMillis(tmpSizeTimestamp[1], "yyyy-MM-dd HH:mm:ss"));
						tmpFilePropertyList.put(tmpFileName, tmpSizeTimestamp);
					}
				}

//				@SuppressWarnings("unchecked")
//				Iterator<String> iter = ftpFileName.iterator();
//				while(iter.hasNext())
//				{
//					Long[] tmpSizeTimestamp = new Long[2];
//					String tmpfile = iter.next();
//					//FTPFile file = ftpClient.mlistFile(remotePath+tmpfile);
//					FTPFile file = ftpClient.mlistFile(ftpClient.printWorkingDirectory());
//					System.out.println("mlistFile: "+remotePath+tmpfile);
//					tmpSizeTimestamp[0] = file.getSize();
//					System.out.println("Size of "+ tmpfile + " : "+tmpSizeTimestamp[0]);
//					tmpSizeTimestamp[1] = file.getTimestamp().getTimeInMillis();
//					tmpFilePropertyList.put(tmpfile, tmpSizeTimestamp);
//				}
							
			} catch (IOException e)
			{
                ftpLogout();
				e.printStackTrace();
				return tmpFilePropertyList;
			}
		}
		
		return tmpFilePropertyList;
	}

	/***
	 * 上传Ftp文件
	 * 
	 * @param localFile
	 *            当地文件
	 * @param romotUpLoadePath
	 *            - 应该以/结束
	 * */
	public boolean uploadFile(File localFile, String romotUpLoadePath)
	{
		BufferedInputStream inStream = null;
		boolean success = false;
		try
		{
			this.ftpClient.changeWorkingDirectory(romotUpLoadePath);// 改变工作路径
			inStream = new BufferedInputStream(new FileInputStream(localFile));
			logger.info(localFile.getName() + "开始上传.....");
			success = this.ftpClient.storeFile(localFile.getName(), inStream);
			if (success == true)
			{
				logger.info(localFile.getName() + "上传成功");
				return success;
			}
		} catch (FileNotFoundException e)
		{
			e.printStackTrace();
			logger.error(localFile + "未找到");
		} catch (IOException e)
		{
            ftpLogout();
			e.printStackTrace();
		} finally
		{
			if (inStream != null)
			{
				try
				{
					inStream.close();
				} catch (IOException e)
				{
					e.printStackTrace();
				}
			}
		}
		return success;
	}

    /**
     * 读取文件夹下所有文件夹名字
     * @param remotePath
     * @return
     * @throws java.io.IOException
     */
    public List<String> GetFolderName(String remotePath)
    {

        if (this.transmissionmode.equalsIgnoreCase("active"))
            this.ftpClient.enterLocalActiveMode();
        else
            this.ftpClient.enterLocalPassiveMode();

        List<String> tmpfilelist = new ArrayList<String>();
        if (!remotePath.startsWith("/"))
        {
            remotePath = "/".concat(remotePath);
        }
        if (!remotePath.endsWith("/"))
        {
            remotePath = remotePath.concat("/");
        }
        if (this.isLogin)
        {
            try
            {
                FTPFile[] files = ftpClient.listDirectories(remotePath);
                if (files == null)
                {
                    return tmpfilelist;
                }
                for (FTPFile file : files)
                {
                    tmpfilelist.add(file.getName());
                }
                return tmpfilelist;

            } catch (IOException e)
            {
                ftpLogout();
                e.printStackTrace();
                return tmpfilelist;
            }
        } else
        {
            return tmpfilelist;
        }
    }
}
