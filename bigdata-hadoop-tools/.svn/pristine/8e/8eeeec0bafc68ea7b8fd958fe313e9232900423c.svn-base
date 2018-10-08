package cn.mastercom.bigdata.util.hadoop.mapred;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import cn.mastercom.bigdata.util.IDataOutputer;


public class MultiOutputMngV2<KEYOUT, VALUEOUT> implements IDataOutputer
{
	public static final String MyMutilOupts = "mastercom.mapreduce.multipleoutputs";

	private Configuration conf = null;
	private Map<String, String> multiFileNameMap = null;
	private Map<Integer, MultiTypeItem> multiTypeIDMap = null;
	private DistributedFileSystem hdfs = null;

	private MultipleOutputs<KEYOUT, VALUEOUT> mos = null;

	public static void addNamedOutput(Job job, int datatype, String namedOutput, String outPutPath,
			Class<? extends OutputFormat> outputFormatClass, Class<?> keyClass, Class<?> valueClass)
	{
		MultipleOutputs.addNamedOutput(job, namedOutput, outputFormatClass, keyClass, valueClass);

		Configuration conf = job.getConfiguration();
		String outPutMarkString = datatype + "," + namedOutput + "," + outPutPath;
		conf.set(MyMutilOupts, conf.get(MyMutilOupts, "") + "|" + outPutMarkString);
	}
	
	public MultiOutputMngV2(TaskInputOutputContext<?, ?, KEYOUT, VALUEOUT> context) throws IOException
	{
		this(context, "");
	}

	public MultiOutputMngV2(TaskInputOutputContext<?, ?, KEYOUT, VALUEOUT> context, String fsUri) throws IOException
	{
		if (context != null)
		{
			this.conf = context.getConfiguration();
			multiFileNameMap = new HashMap<String, String>();

			mos = new MultipleOutputs<KEYOUT, VALUEOUT>(context);

			String mPath = conf.get("mapreduce.multipleoutputs");  //得到别名
			if (mPath != null)
			{
				String[] mPaths = mPath.trim().split(" ");
				for (String tmstr : mPaths)
				{
					String typeName = tmstr;
					String fileName = FileOutputFormat.getUniqueFile(context, tmstr, "");
					multiFileNameMap.put(typeName, fileName);
				}
			}
		}
	}

	public void init() throws IOException
	{
		if (conf == null || multiFileNameMap == null)
			return;

		multiTypeIDMap = new HashMap<Integer, MultiTypeItem>();

		// 初始化目录
		String myMutilOupts = conf.get(MyMutilOupts, "");
		
		if (!myMutilOupts.contains(":"))
		{
			FileSystem fs = FileSystem.get(conf);
			hdfs = (DistributedFileSystem) fs;				
		}
		
		String[] flags = myMutilOupts.split("\\|", -1);
		for (String flag : flags)
		{
			if (flag.length() == 0)
			{
				continue;
			}

			String[] subflag = flag.split(",", -1);
			if (subflag.length != 3)
			{
				continue;
			}

			int dataType = Integer.parseInt(subflag[0]);
			String typeName = subflag[1];
			String path = subflag[2];

			MultiTypeItem multiTypeItem = new MultiTypeItem();
			multiTypeItem.dataType = dataType;
//			multiTypeItem.basePath = path.replace("/", "");
			multiTypeItem.basePath = path;
			multiTypeItem.typeName = typeName;

			if (!multiFileNameMap.containsKey(typeName))
			{
				continue;
			}

			multiTypeItem.fileName = multiFileNameMap.get(typeName);
			multiTypeIDMap.put(dataType, multiTypeItem);
		}

		// 删除已存在目录
		for (MultiTypeItem item : multiTypeIDMap.values())
		{
			// 需要检查是否已经存在
			if (item.basePath.length() > 0 && !item.basePath.contains(":"))
			{
				Path path = new Path(item.basePath, item.fileName);
				if (hdfs.exists(path))
				{
					hdfs.delete(path, false);
				}
			}
		}
	}

	@Override
	public int pushData(int dataType, String value) 
	{
//		Text curT=new Text();
		MultiTypeItem item = multiTypeIDMap.get(dataType);
		if (item != null)
		{
			try
			{
//				curT.set(value);
				if (mos != null)
					mos.write(item.typeName, NullWritable.get(), value, item.getPathName());
				return 0;
			}
			catch (Exception e)
			{
				//throw new Exception(e.getMessage());
				return -1;
			}	
		}
		return -1;
	}

	@Override
	public int pushData(int dataType, List<String> valueList) 
	{
		MultiTypeItem item = multiTypeIDMap.get(dataType);
		if (item != null)
		{
			try
			{
				if (mos != null)
				{
					for (String item2 : valueList)
					{
						mos.write(item.typeName, NullWritable.get(), item2, item.getPathName());
					}
					return 0;
				}   
			}
			catch (Exception e)
			{
				//throw new Exception(e.getMessage());
				return -1;
			}
     
		}
		return -1;
	}

	@Override
	public void clear() 
	{
		if (mos != null)
		{
			try
			{
				mos.close();
			}
			catch (Exception e)
			{
				//throw new Exception(e.getMessage());
			}			
		}
	}

}
