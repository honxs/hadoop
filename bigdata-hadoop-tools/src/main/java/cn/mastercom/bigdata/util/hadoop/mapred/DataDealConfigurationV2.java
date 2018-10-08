package cn.mastercom.bigdata.util.hadoop.mapred;

import java.util.Map.Entry;

import cn.mastercom.bigdata.util.LOGHelper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DataDealConfigurationV2 extends Configuration
{
	private static final Log LOG = LogFactory.getLog(DataDealConfigurationV2.class);

	public final static int DATATYPE_LOG_MAP = 1001;
	public final static int DATATYPE_LOG_REDUCE = 1002;
	
	private static final String MULTIPLE_OUTPUTS = "mapreduce.multipleoutputs";
	private static final String MO_PREFIX = "mapreduce.multipleoutputs.namedOutput.";
	private static final String FORMAT = ".format";
	private static final String KEY = ".key";
	private static final String VALUE = ".value";

	//
	protected static String path_myLog;
	protected static int logLevel;
	protected static String codeType;

	public static void create(String appBasePath, final Job job)
	{
		path_myLog = appBasePath + "/MYLOG";
		logLevel = LOGHelper.LogLevel_ALL;

		job.getConfiguration().set("mastercom.datamapper.myLoglevel", "" + logLevel);
		job.getConfiguration().set("mapreduce.output.fileoutputformat.compress", "false");

		// 筛除无效的文件
		job.getConfiguration().set("mapreduce.input.pathFilter.class", "cn.mastercom.bigdata.util.hadoop.mapred.TmpFileFilter");

		try {
			MultiOutputMngV2.addNamedOutput(job, DATATYPE_LOG_MAP, "myLogMap", path_myLog, TextOutputFormat.class, NullWritable.class, Text.class);
			MultiOutputMngV2.addNamedOutput(job, DATATYPE_LOG_REDUCE, "myLogReduce", path_myLog, TextOutputFormat.class, NullWritable.class, Text.class);			
		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	protected static void addNamedOutput(Configuration conf, String namedOutput, Class<? extends OutputFormat> outputFormatClass, Class<?> keyClass, Class<?> valueClass)
	{
		conf.set(MULTIPLE_OUTPUTS, conf.get(MULTIPLE_OUTPUTS, "") + " " + namedOutput);
		conf.setClass(MO_PREFIX + namedOutput + FORMAT, outputFormatClass, OutputFormat.class);
		conf.setClass(MO_PREFIX + namedOutput + KEY, keyClass, Object.class);
		conf.setClass(MO_PREFIX + namedOutput + VALUE, valueClass, Object.class);
	}

	public static void merge(Configuration destConf, Configuration srcConf)
	{
		for (Entry<String, String> e : srcConf)
		{
			destConf.set(e.getKey(), e.getValue());
		}
	}

	public static void main(String[] args) throws Exception
	{
		// DataDealConfiguration.create().writeXml(System.out);
	}

}
