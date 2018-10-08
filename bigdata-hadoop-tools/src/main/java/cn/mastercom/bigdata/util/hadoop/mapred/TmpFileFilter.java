package cn.mastercom.bigdata.util.hadoop.mapred;

import java.io.File;
import java.util.HashMap;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;

public class TmpFileFilter implements PathFilter
{
	private String tmpStr = "";
	public static HashMap<String, String> mapValidStr = new HashMap<String, String>(); 
	public static HDFSOper hdfsOper = null;

	@Override
    public boolean accept(Path path) 
	{
		if(path.getClass().equals(org.apache.hadoop.hdfs.DistributedFileSystem.class))
		{
			if(hdfsOper!=null)
			{
				FileStatus fs = hdfsOper.getFileStatus(path.getName());
				if(fs.isDirectory())
				{
					return true;
				}				
			}
		}
		else
		{
			File file = new File(path.getName());
			if(file.exists() && file.isDirectory())
			{
				return true;
			}
		}
		
    	tmpStr = path.getName();
        if(tmpStr.indexOf(".tmp") >= 0)
        {
        	return false;
        }
        else if(tmpStr.indexOf("_COPYING_") >= 0)
        {
        	return false;
        }
       
        if(mapValidStr.size()>0)
        {
        	for(String key:mapValidStr.keySet())
        	{
        		if(tmpStr.indexOf(key) <0)
                {
                	continue;
                }
         		String[] validStr = mapValidStr.get(key).split(",");
        		for(String keyword:validStr)
	        	{
	        		if(tmpStr.indexOf(keyword) >= 0)
	    	        {
	    	        	return true;
	    	        }
	        	}
        		return false;
        	}
        }
        return true;
    }

	public static void main(String[] args)
	{
		TmpFileFilter ff = new TmpFileFilter();
		TmpFileFilter.mapValidStr.put("mme", "170610");
		TmpFileFilter.mapValidStr.put("http", "170610");
		System.out.println(ff.accept(new Path("mme17061002")));
		System.out.println(ff.accept(new Path("http17061102")));
		System.out.println(ff.accept(new Path("mro17061002")));
	}
}
