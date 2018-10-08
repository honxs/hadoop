package cn.mastercom.bigdata.util.ftp;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class FtpRuleList
{
	public static int i = 0;
    public final String LISTFlag = "$LIST{";
    public String className = "";
    public FtpRuleList(String className) {
    	this.className = className;
    };

    public List<Object> ListFiles(Object fch, String str, Date date) throws Exception
    {
        FtpRuleLimit frl = new FtpRuleLimit();
        frl.getTimeLimit(str, date);
        return listFiles(fch, frl.Path, frl.Min, frl.Max);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
	private List<Object> listFiles(Object fch, String str, Date min, Date max) throws Exception
    {
        List<Object> paths = new ArrayList<Object>();
        int count = str.length() - str.replace(LISTFlag, "12345").length();// LIST次数
        //通过反射获取Class对象
      	Class stuClass = Class.forName(className);
      	Constructor c = stuClass.getConstructor(String.class);
      	SuperFtpFile f = (SuperFtpFile)c.newInstance(str);
        paths.add(f);
        for (int i = 0; i < count; i++)
        {
            paths = listFiles(fch, paths, min, max);
        }
        return paths;
    }

    /**
     * 对多个路径进行一次list
     * 
     * @param IFtpHelper
     * @param ftpFiles
     * @param min
     * @param max
     * @return
     * @throws Exception
     */
    private List<Object> listFiles(Object IFtpHelper, List<Object> ftpFiles, Date min, Date max)
            throws Exception
    {
        List<Object> results = new ArrayList<Object>();
       
        for (Object object : ftpFiles)
        {    	
            List<Object> ls = listFiles(IFtpHelper, object, min, max);
            if (ls.size() > 0)
            {
                results.addAll(ls);
                ls.clear();
                
            }
        }
        return results;
    }

    /**
     * 对一个路径进行一次list
     * @param IFtpHelper
     * @param str
     * @param min
     * @param max
     * @return
     * @throws Exception
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
	private List<Object> listFiles(Object IFtpHelper, Object Object, Date min, Date max) throws Exception 
    {
    	IFtpClientHelper ih = (IFtpClientHelper)IFtpHelper;
    	List<Object> results = new ArrayList<Object>();
    	try{
    		SuperFtpFile f = (SuperFtpFile)Object;
            String[] arrs = splitLine(f.getFullPath());
            String subPath = arrs[0], strPattern = arrs[1], strValue = arrs[2];
            if (strPattern == "") strPattern = "*";
            String[] patterns = strPattern.split("\\|");

            Object[] files = null;
            if (strValue.length() == 0)
            {
                files = listFiles(ih, subPath);
            }
            else
            {
                files = listDirs(ih, subPath);
            }
            for (Object ftpFile : files)
            {
                if (isMatch(ftpFile, patterns))
                {
                    if (strValue.indexOf("/") != strValue.lastIndexOf("/")
                    		|| strValue.lastIndexOf("/") == 0 || strValue.lastIndexOf("/") == -1
                            )
                    {
                    	SuperFtpFile ftp = (SuperFtpFile)ftpFile;
                    	String fullPath = subPath + ftp.getName() + strValue;
                    	if(fullPath.length() != 0){	 
                    		long lastModifiedTime = ftp.getLastModifiedTime(); 
                    		 //通过反射获取Class对象
                          	Class stuClass = Class.forName(className);
                          	Constructor c = stuClass.getConstructor(String.class,long.class,long.class);
                          	SuperFtpFile f1 = (SuperFtpFile)c.newInstance(fullPath,lastModifiedTime,ftp.getFileSize());
    	                    results.add(f1);
                    	}else{
                    		break;
                    	}                   	                    	
                    }
                }
            }
    	}catch(Exception e){
    		ih.disconnect();
    	}
    	return results;
    }

    private String[] splitLine(String str) throws Exception
    {
        String[] result = new String[3];
        int index = str.indexOf(LISTFlag);
        if (index >= 0)
        {
            result[0] = str.substring(0, index);
            str = str.substring(index + LISTFlag.length());
            index = str.indexOf("}");
            if (index >= 0)
            {
                result[1] = str.substring(0, index);
                result[2] = str.substring(index + 1);
            }
        }

        return result;
    }

    /**
     * list file
     * 
     * @param fch
     * @param subPath
     * @return
     * @throws Exception
     */
    private Object[] listFiles(IFtpClientHelper fch, String subPath) throws Exception
    {
        return fch.listFiles(subPath, false);
    }

    /**
     * list dir
     * 
     * @param fch
     * @param subPath
     * @return
     * @throws Exception
     */
    private Object[] listDirs(IFtpClientHelper fch, String subPath) throws Exception
    {
        return fch.listDirs(subPath, false);
    }
    

    /**
     * 过滤
     * 
     * @param file
     * @param patterns
     * @return
     */
    private boolean isMatch(Object file, String[] patterns)
    {
        for (String pattern : patterns)
        {
            if (isMatch(file, pattern)) return true;
        }
        return false;
    }

    /**
     * 过滤
     * 
     * @param file
     * @param pattern
     * @return
     */
    private boolean isMatch(Object file, String pattern)
    {
        while(pattern.contains("**"))
        {
            pattern.replace("**", "*");
        }
        SuperFtpFile f = (SuperFtpFile)file;
        String input = f.getName();
        input = input.trim().toLowerCase();
        pattern = pattern.trim().toLowerCase();

        if (pattern.contains("*"))
        {
            String[] arrs = pattern.split("\\*", -1);

            int arrsEnd = arrs.length - 1;
            if (arrsEnd == 1 && arrs[0].length() == 0 && arrs[arrsEnd].length() == 0) return true;// pattern == "*" / "**" / ......

            if (arrs[0].length() > 0 && (!input.startsWith(arrs[0]))) return false;
            int inputStart = 0;
            int count = 0;
            for (String arr : arrs)
            {
                if (arr.length() == 0) continue;                
                int inputIndex = input.indexOf(arr, inputStart);
                if(inputIndex < 0){
                	count++;
                	continue ;
                }
                inputStart += arr.length();
            }
            if(count > 0 && inputStart > 0){
            	return false;
            }else if(inputStart > 0) {
            	return true;
            }else{
            	return false;
            }           
        }
        else
        {
            return input.contains(pattern);
        }
    }
    
    
    public static void main(String[] args)
	{
//    	FTPClientHelper fch = new FTPClientHelper("192.168.3.124",21,"dtauser","dtauser");   
    	SftpClientHelper sfch = new SftpClientHelper("192.168.1.31", "hmaster", "mastercom168", 22, 1000);
//    	FtpRuleList2 frh = new FtpRuleList2("cn.mastercom.bigdata.util.ftp.FtpFile");        
    	FtpRuleList frh = new FtpRuleList("cn.mastercom.bigdata.util.ftp.SftpFile");        
        try
		{
			List<Object> files = frh.ListFiles(sfch, "/home/hmaster/$LIST{*}", new Date());
			for (Object object : files)
			{
				SuperFtpFile f = (SuperFtpFile)object;
				System.out.println(f.getName());
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		} 
	}
}
