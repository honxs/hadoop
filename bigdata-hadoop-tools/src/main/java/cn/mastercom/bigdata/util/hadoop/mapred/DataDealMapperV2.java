package cn.mastercom.bigdata.util.hadoop.mapred;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;

import cn.mastercom.bigdata.util.HostUtil;
import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;

public class DataDealMapperV2<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> implements IWriteLogCallBack
{
	protected MultiOutputMngV2<KEYOUT, VALUEOUT> mos;
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
		init();
	}

	/**
	 * 可选地排除不需要运行任务的节点(即在该节点运行直接报错，hadoop框架默认失败次数到达后便不再给该节点分配任务)
	 * @param context
	 * @param excludeIpList
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected void setup(Context context, List<String> excludeIpList) throws IOException, InterruptedException
	{
		if (HostUtil.containsOne(excludeIpList)){
			throw new InterruptedException();
		}
		this.context = context;
		init();
	}

	private void init() throws IOException{
		if(context != null)
		{
			conf = context.getConfiguration();
			mos = new MultiOutputMngV2<KEYOUT, VALUEOUT>(context);
			mos.init();
		}

		logLevel = conf.get("mastercom.datamapper.myLoglevel") != null? Integer.parseInt(conf.get("mastercom.datamapper.myLoglevel")):LOGHelper.LogLevel_ALL;
		codetype = conf.get("mastercom.datamapper.codetype");

		LOGHelper.GetLogger().setLogLevel(logLevel);
		LOGHelper.GetLogger().addWriteLogCallBack(this);
	}
	/**
	 * Called once at the end of the task.
	 */
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		LOGHelper.GetLogger().finalWriteLogCount();
		mos.clear();
	}
	
	
	@Override
	public void writeLog(LogType logType, String strlog)
	{
		try
		{
			if(mos != null)
				mos.pushData(DataDealConfigurationV2.DATATYPE_LOG_MAP, LOGHelper.getFormatLog(logType, strlog));
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
