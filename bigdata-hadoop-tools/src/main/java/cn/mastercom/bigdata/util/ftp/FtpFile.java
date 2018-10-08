package cn.mastercom.bigdata.util.ftp;

import java.io.File;


public class FtpFile  extends SuperFtpFile                                                                                                                                                                                                       
{
	private String m_FullPath = null;
	private long m_lastModifiedTime = 0L;
	private long m_fileSize = 0L;
    
    public FtpFile(String fullPath,long lastModifiedTime,long fileSize)
    {
        m_FullPath = fullPath;
        m_lastModifiedTime = lastModifiedTime;
        m_fileSize = fileSize;
    }
    
    public FtpFile(String fullPath)
    {
        m_FullPath = fullPath;
    }
    
    public FtpFile() {
    	
    }
    
    public String getFullPath()
    {
        return m_FullPath;
    }
    
    public long getLastModifiedTime()
    {
    	return m_lastModifiedTime;
    }
    
    public long getFileSize()
    {
    	return m_fileSize;
    }

	public String getName()
	{
		return new File(m_FullPath).getName();
	}
    
}
