package cn.mastercom.bigdata.util.ftp;

import java.io.File;

public class SftpFile extends SuperFtpFile
{
	public String m_FullPath = null;
	public long m_lastModifiedTime = 0L;
	public long m_fileSize = 0L;
	
    public SftpFile(String fullPath,long lastModifiedTime,long fileSize)
    {
        m_FullPath = fullPath;
        m_lastModifiedTime = lastModifiedTime;
        m_fileSize = fileSize;
    }
    
    public SftpFile(String fullPath)
    {
        m_FullPath = fullPath;
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
