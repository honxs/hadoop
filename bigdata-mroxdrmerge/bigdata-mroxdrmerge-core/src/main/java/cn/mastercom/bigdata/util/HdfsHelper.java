package cn.mastercom.bigdata.util;

import java.text.SimpleDateFormat;
import java.util.Date;

import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;

public class HdfsHelper
{
	public static String reNameExistsPath(HDFSOper hdfsOper, String srcPath, String tarPath) throws Exception
	{
	    //检测输出目录是否存在，存在就改名
	    if(hdfsOper.checkFileExist(srcPath))
	    {
	    	tarPath = srcPath.trim();
	    	while(tarPath.charAt(tarPath.length()-1) == '/')
	    	{
	    		tarPath = tarPath.substring(0, tarPath.length()-1);
	    	}
	    	Date now = new Date(); 
	    	SimpleDateFormat dateFormat = new SimpleDateFormat("yyMMddHHmmss");
	    	String nowStr = dateFormat.format(now); 
	    	tarPath += "_" + nowStr;
	    	hdfsOper.movefile(srcPath, tarPath);
	    }
	    else 
	    {
	    	tarPath = srcPath;
	    }
	    
	    return tarPath;
	}
}
