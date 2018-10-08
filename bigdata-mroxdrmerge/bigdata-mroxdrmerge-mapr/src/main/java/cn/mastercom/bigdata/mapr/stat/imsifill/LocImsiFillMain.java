package cn.mastercom.bigdata.mapr.stat.imsifill;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import cn.mastercom.bigdata.mapr.stat.imsifill.LocImsiFillMapper.ImsiIPKeyComparator;
import cn.mastercom.bigdata.mapr.stat.imsifill.LocImsiFillMapper.ImsiIPKeyGroupComparator;
import cn.mastercom.bigdata.mapr.stat.imsifill.LocImsiFillMapper.ImsiIPMapper;
import cn.mastercom.bigdata.mapr.stat.imsifill.LocImsiFillMapper.ImsiIPPartitioner;
import cn.mastercom.bigdata.mapr.stat.imsifill.LocImsiFillMapper.LocationMapper;
import cn.mastercom.bigdata.mapr.stat.imsifill.LocImsiFillReducer.StatReducer;
import cn.mastercom.bigdata.mapr.util.tools.MapReduceMainTools;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.stat.imsifill.deal.ImsiIPKey;
import cn.mastercom.bigdata.util.HdfsHelper;
import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;
import cn.mastercom.bigdata.util.hadoop.mapred.DataDealConfigurationV2;
import cn.mastercom.bigdata.util.hadoop.mapred.MultiOutputMngV2;
import cn.mastercom.bigdata.xdr.prepare.stat.XdrPrepareTablesEnum;

public class LocImsiFillMain
{
	protected static final Log LOG = LogFactory.getLog(LocImsiFillMain.class);

	private static int reduceNum;
	private static String queueName;
	private static String outpath_date;
	private static String inpath_location;
	private static String inpath_http;
	private static String outpath_table;
	private static String outpath;
	
	private static void makeConfig(Job job, String[] args)
	{
		Configuration conf = job.getConfiguration();
		reduceNum = Integer.parseInt(args[0]);
		queueName = args[1];
		outpath_date = args[2];
		inpath_location = args[3];
		inpath_http = args[4];
		outpath_table = XdrPrepareTablesEnum.getBasePath(args[5], outpath_date.substring(3));

		

		for (int i = 0; i < args.length; ++i)
		{
			LOG.info(i + ": " + args[i] + "\n");
		}

		// table output path
		outpath = outpath_table + "/output";

		LOG.info(outpath);


		MapReduceMainTools.CustomMaprParas(conf, queueName);
		// 初始化自己的配置管理
		DataDealConfigurationV2.create(outpath_table, job);
	}

	public static Job CreateJob(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug)) 
		{
			conf.set("fs.defaultFS", "hdfs://192.168.1.31:9000");
		}
		if (args.length < 6)
		{
			System.err.println("Usage: loc imsi fill input error");
			System.err.println("Now error input num is : " + args.length);
			throw (new Exception("LocImsiFillMain args input error!"));
		}
		Job job = Job.getInstance(conf);
		makeConfig(job, args);
		String mmeFilter = "";
		String httpFilter = "";
		String mmeKeyWord = "";
		String httpKeyWord = "";

		if (args.length >= 10)
		{
			if (!args[6].toLowerCase().equals("NULL"))
			{
				mmeFilter = args[6];
			}

			if (!args[7].toLowerCase().equals("NULL"))
			{
				httpFilter = args[7];
			}
			if (!args[8].toLowerCase().equals("NULL"))
			{
				mmeKeyWord = args[8];
			}

			if (!args[9].toLowerCase().equals("NULL"))
			{
				httpKeyWord = args[9];
			}
		}
		HDFSOper hdfsOper = new HDFSOper(conf);

		job.setJobName("MroXdrMerge.loc.imsifill" + ":" + outpath_date);

		job.setJarByClass(LocImsiFillMain.class);
		job.setReducerClass(StatReducer.class);
		job.setSortComparatorClass(ImsiIPKeyComparator.class);
		job.setPartitionerClass(ImsiIPPartitioner.class);
		job.setGroupingComparatorClass(ImsiIPKeyGroupComparator.class);
		job.setMapOutputKeyClass(ImsiIPKey.class);
		job.setMapOutputValueClass(Text.class);

		long inputSize = 0;
		if (!inpath_location.contains(":"))
		{
			inputSize += MapReduceMainTools.getFileSize(inpath_location, hdfsOper);
			inputSize += MapReduceMainTools.getFileSize(inpath_http, hdfsOper, httpFilter, httpKeyWord);
		}
		reduceNum = MapReduceMainTools.getReduceNum(inputSize, conf);

		job.setNumReduceTasks(reduceNum);
		///////////////////////////////////////////////////////

		MapReduceMainTools.addInputPath(job, inpath_location, hdfsOper, LocationMapper.class);
		MapReduceMainTools.addInputPath(job, inpath_http, hdfsOper, ImsiIPMapper.class);

		MultiOutputMngV2.addNamedOutput(job, XdrPrepareTablesEnum.xdrLocation.getIndex(), XdrPrepareTablesEnum.xdrLocation.getFileName(), XdrPrepareTablesEnum.xdrLocation.getPath(args[5], outpath_date.substring(3)), TextOutputFormat.class, NullWritable.class, Text.class);
		FileOutputFormat.setOutputPath(job, new Path(outpath));
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.GZip))
		{
			FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		}
		// 检测输出目录是否存在，存在就改名
		String tarPath = "";
		HdfsHelper.reNameExistsPath(hdfsOper, outpath_table, tarPath);

		return job;
	}

	public static void main(String[] args) throws Exception
	{
		Job job = CreateJob(args);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
