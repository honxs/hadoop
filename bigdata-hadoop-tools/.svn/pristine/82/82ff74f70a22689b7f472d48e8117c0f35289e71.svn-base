package cn.mastercom.bigdata.util.hadoop.mapred;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MultiOutputMng<KEYOUT, VALUEOUT>
{
	private Configuration conf = null;
	private Map<String, MultiTypeItem> multiTypeMap = null;
	private DistributedFileSystem hdfs = null;
	
	private MultipleOutputs<KEYOUT, VALUEOUT> mos = null;
	
    public MultiOutputMng(TaskInputOutputContext<?, ?, KEYOUT, VALUEOUT> context, String fsUri) throws IOException
    {
    	if(context != null)
    	{
    		this.conf = context.getConfiguration();
	    	multiTypeMap = new HashMap<String, MultiTypeItem>();
	    	
	    	mos = new MultipleOutputs<KEYOUT, VALUEOUT>(context);
	    	
			FileSystem.setDefaultUri(conf, fsUri);
			FileSystem fs = FileSystem.get(conf);  
	        hdfs = (DistributedFileSystem)fs; 
	        String mPath = conf.get("mapreduce.multipleoutputs");
			if(mPath != null)
			{
				String[] mPaths = mPath.trim().split(" ");
				for (String tmstr : mPaths)
				{
					String fileName = FileOutputFormat.getUniqueFile(context, tmstr, "");
					
					MultiTypeItem item = new MultiTypeItem();
					item.typeName = tmstr;
					item.fileName = fileName;
					multiTypeMap.put(item.typeName, item);
				}
			} 				   	
		}	
    }
    
    public void init() throws IOException
    {
    	if(conf == null)
    		return;
    	for(MultiTypeItem item : multiTypeMap.values())
    	{
			//需要检查是否已经存在
    		if(item.basePath.length() > 0)
    		{
    			Path path = new Path(item.basePath, item.fileName);
    			if(hdfs.exists(path))
    			{
    				hdfs.delete(path, false);
    			}
    		}
    	}
    }
    
    public void SetOutputPath(String typeName, String path)
    {
    	MultiTypeItem item = multiTypeMap.get(typeName);
        if(item != null)
        {
        	item.basePath = path;
        }
        else
        {
        	item = new MultiTypeItem();
			item.typeName = typeName;
			item.basePath = path.replace("/", "");
			multiTypeMap.put(item.typeName, item);
        }
    }
    
    public void write(String typeName, KEYOUT key, VALUEOUT value) throws IOException, InterruptedException
    {
    	MultiTypeItem item = multiTypeMap.get(typeName);
    	if(item != null)
    	{	
    		if(mos != null)
    			mos.write(typeName, key, value, item.getPathName());
    	}
    }
    
    public void close() throws IOException, InterruptedException
    {
    	if(mos != null)
    	{
    		mos.close();
    	}
    }
    

   
	
	
	
	
	
}
