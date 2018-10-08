package cn.mastercom.bigdata.util.ftp;

import java.io.OutputStream;

public interface IFtpClientHelper {
	
	void setHost(String host);
	
	void setUsername(String userName);
	
	void setPassword(String passWd);
	
	void setPort(int port);
	
	void setTimeout(int timeOut);
	
	void disconnect() throws Exception;
	
	void setEncoding(String encode);
	
	void setPassiveMode(boolean passMode);
	
	void setBinaryTransfer(boolean binaryTransfer);
	
	Object[] listFiles(String subPath,boolean close) throws Exception;
	
	Object[] listDirs(String subPath,boolean close) throws Exception;
	
	boolean get(String remoteAbsoluteFile ,OutputStream os) throws Exception;
}
