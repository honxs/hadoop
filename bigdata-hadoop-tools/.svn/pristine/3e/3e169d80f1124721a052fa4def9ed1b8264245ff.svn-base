package cn.mastercom.bigdata.util.ftp;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;


public class FTPRuleHelper
{
	private String className ;
	public FTPRuleHelper(String className) {
		this.className = className;
	}
    public List<Object> ListFiles(Object fch, List<String> strs, Date date) throws Exception
    {
        List<Object> result = new ArrayList<Object>();
        for (String str : strs)
        {
            List<Object> ls = listFiles(fch, str, date);
            result.addAll(ls);
        }
        return result;
    }  
    
    public List<Object> listFiles(Object fch, String str, Date date) throws Exception
    {
        str = str.replace("\\", "/").trim();
        if (!str.startsWith("/"))
        {
            str = "/" + str;
        }      
        String s = (new FtpRuleTime()).ReplaceTime(str, date);
        return (new FtpRuleList(className)).ListFiles(fch, s, date);
    }
    
    public static void main(String[] args) throws Exception
    {
        @SuppressWarnings("unused")
		FTPClientHelper fch = new FTPClientHelper("192.168.1.10",21,"ftpuser","ftpuser");
    }
}
