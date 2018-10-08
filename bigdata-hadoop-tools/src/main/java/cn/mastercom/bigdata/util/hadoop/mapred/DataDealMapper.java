package cn.mastercom.bigdata.util.hadoop.mapred;

import java.io.IOException;
import java.text.SimpleDateFormat;

import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class DataDealMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> implements IWriteLogCallBack
{
	protected org.apache.hadoop.mapreduce.lib.output.MultipleOutputs<KEYOUT, VALUEOUT> mos;
	protected Configuration conf;
	
	protected String path_myLog;
	protected int logLevel;
	protected StringBuilder sbLog;
	protected SimpleDateFormat logTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	protected final String utf8 = "UTF-8";
	protected Context context;
	protected String codetype;
	
    
	/**
	 * Called once at the beginning of the task.
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		this.context = context;
		this.conf = context.getConfiguration();
		
		path_myLog = conf.get("mastercom.datamapper.path_myLog");
		logLevel = conf.get("mastercom.datamapper.myLoglevel") != null? Integer.parseInt(conf.get("mastercom.datamapper.myLoglevel")):LOGHelper.LogLevel_ALL;
		codetype = conf.get("mastercom.datamapper.codetype");
		
		mos = new org.apache.hadoop.mapreduce.lib.output.MultipleOutputs<KEYOUT, VALUEOUT>(context);
		
		LOGHelper.GetLogger().setLogLevel(logLevel);
		LOGHelper.GetLogger().addWriteLogCallBack(this);	
	}

	/**
	 * Called once at the end of the task.
	 */
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		mos.close();
	}
	
	
	@Override
	public void writeLog(LogType logType, String strlog)
	{
		try
		{
			mos.write("myLogMap", NullWritable.get(), LOGHelper.getFormatLog(logType, strlog), makeFileName(path_myLog, "myLogMap"));
		}
		catch (Exception e)
		{
			e.printStackTrace();
			// TODO: handle exception
		}
	}

	protected String makeFileName(String path, String name)
	{
		return path + "/" + name;
	}

}
