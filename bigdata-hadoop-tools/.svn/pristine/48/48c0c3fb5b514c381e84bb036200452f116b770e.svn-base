package cn.mastercom.bigdata.util.ftp;

import java.util.ArrayList;
import java.util.List;



public class TaskInfoFtp {
	public List<String> ftpFileList = new ArrayList<String>();// 待处理文件完整路径组成的list
	public List<String> hdfsFileList = new ArrayList<String>();

	public FTPClientHelper ftp ;
	public SftpClientHelper sftp ;
		
	public TaskInfoFtp() {

	}
	
	public TaskInfoFtp(String host, int port,String username,String password){
		ftp = new FTPClientHelper(host, port, username, password);
	}
	public TaskInfoFtp(String FtpIP, int FtpPort,String FtpName,String FtpPassWd,int time){
		sftp = new SftpClientHelper(FtpIP, FtpName, FtpPassWd, FtpPort, time);
	}
	
}
