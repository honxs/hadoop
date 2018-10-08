package cn.mastercom.bigdata.mapr.stat.eciFilter;

import java.io.IOException;

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

import cn.mastercom.bigdata.mapr.stat.eciFilter.EciFilterMapper.EciFilterMapper_HTTP;
import cn.mastercom.bigdata.mapr.stat.eciFilter.EciFilterMapper.EciFilterMapper_MME;
import cn.mastercom.bigdata.mapr.stat.eciFilter.EciFilterMapper.EciFilterMapper_MRO;
import cn.mastercom.bigdata.mapr.stat.eciFilter.EciFilterMapper.EciFilterMapper_XDRLOCATION;
import cn.mastercom.bigdata.mapr.stat.eciFilter.EciFilterMapper.EciFilterPartitioner;
import cn.mastercom.bigdata.mapr.stat.eciFilter.EciFilterReducer.EciFilterReduce;
import cn.mastercom.bigdata.mapr.util.tools.MapReduceMainTools;
import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;
import cn.mastercom.bigdata.util.hadoop.mapred.DataDealConfigurationV2;
import cn.mastercom.bigdata.util.hadoop.mapred.MultiOutputMngV2;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.stat.eciFilter.EciFilterKey;
import cn.mastercom.bigdata.stat.tableinfo.enums.SingleProgEnums;
import cn.mastercom.bigdata.util.HdfsHelper;

public class EciFilterMain
{
	protected static final Log LOG = LogFactory.getLog(EciFilterMain.class);
	private static int reduceNum;
	private static String queueName;
	private static String statTime;
	private static String inputPath_mme;
	private static String inputPath_http;
	private static String inputPath_mro;
	private static String inputPath_xdrlocation;
	private static String outpath_table;
	private static String outpath;
	private static String path_mme;
	private static String path_http;
	private static String path_mro;
	private static String path_xdrlocation;

	private static void makeConfig(Job job, String[] args)
	{
		Configuration conf = job.getConfiguration();
		reduceNum = Integer.parseInt(args[0]);
		queueName = args[1];
		statTime = args[2]; // 01_20171110
		inputPath_mme = args[3];
		inputPath_http = args[4];
		inputPath_mro = args[5];
		inputPath_xdrlocation = args[6];

		String date = statTime.replace("01_", "");
		String mroXdrMergePath = MainModel.GetInstance().getAppConfig().getMroXdrMergePath();
		outpath_table = String.format("%s/eci_filter", mroXdrMergePath); // /mt_wlyh/Data/mroxdrmerge/eci_filter

		// table output path
		outpath = outpath_table + "/output1";
		path_mme = outpath_table + "/" + date + "/mme";
		path_http = outpath_table + "/" + date + "/http";
		path_mro = outpath_table + "/" + date + "/mro";
		path_xdrlocation = "/mt_wlyh/Data/mroxdrmerge/xdr_loc/data_01_171115/XDR_LOCATION_01_171115_OUTPUT";

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
		if (args.length != 7)
		{
			System.err.println("input not enough reduceNum/queueName/statTime/inputPath_mme/inputPath_http/inputPath_mro/inputPath_xdrlocation");
			throw (new IOException("EciFilterMain args input error!"));
		}
		Job job = Job.getInstance(conf, "EciFilter");
		makeConfig(job, args);
		HDFSOper hdfsOper = null;
		job.setJarByClass(EciFilterMain.class);
		job.setReducerClass(EciFilterReduce.class);
		job.setMapOutputKeyClass(EciFilterKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setPartitionerClass(EciFilterPartitioner.class);

		long inputSize = 0;
		if (!inputPath_mme.contains(":"))
		{
			hdfsOper = new HDFSOper(conf);
			inputSize += MapReduceMainTools.getFileSize(inputPath_mme, hdfsOper);
			inputSize += MapReduceMainTools.getFileSize(inputPath_http, hdfsOper);
			inputSize += MapReduceMainTools.getFileSize(inputPath_mro, hdfsOper);
			inputSize += MapReduceMainTools.getFileSize(inputPath_xdrlocation, hdfsOper);
		}
		reduceNum = MapReduceMainTools.getReduceNum(inputSize, conf);
		job.setNumReduceTasks(reduceNum);

		MapReduceMainTools.addInputPath(job, inputPath_mme, hdfsOper, EciFilterMapper_MME.class);
		MapReduceMainTools.addInputPath(job, inputPath_http, hdfsOper, EciFilterMapper_HTTP.class);
		MapReduceMainTools.addInputPath(job, inputPath_mro, hdfsOper, EciFilterMapper_MRO.class);
		MapReduceMainTools.addInputPath(job, inputPath_xdrlocation, hdfsOper, EciFilterMapper_XDRLOCATION.class);

		MultiOutputMngV2.addNamedOutput(job, SingleProgEnums.EciFilter_MME.getIndex(), SingleProgEnums.EciFilter_MME.getFileName(), SingleProgEnums.EciFilter_MME.getPath(path_mme),
				TextOutputFormat.class, NullWritable.class, Text.class);
		MultiOutputMngV2.addNamedOutput(job, SingleProgEnums.EciFilter_HTTP.getIndex(), SingleProgEnums.EciFilter_HTTP.getFileName(), SingleProgEnums.EciFilter_HTTP.getPath(path_http),
				TextOutputFormat.class, NullWritable.class, Text.class);
		MultiOutputMngV2.addNamedOutput(job, SingleProgEnums.EciFilter_MRO.getIndex(), SingleProgEnums.EciFilter_MRO.getFileName(), SingleProgEnums.EciFilter_MRO.getPath(path_mro),
				TextOutputFormat.class, NullWritable.class, Text.class);
		MultiOutputMngV2.addNamedOutput(job, SingleProgEnums.EciFilter_XDRLOCATION.getIndex(), SingleProgEnums.EciFilter_XDRLOCATION.getFileName(),
				SingleProgEnums.EciFilter_XDRLOCATION.getPath(path_xdrlocation), TextOutputFormat.class, NullWritable.class, Text.class);

		FileOutputFormat.setOutputPath(job, new Path(outpath));
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.GZip))
		{
			FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		}
		String tarPath = "";
		if (!outpath_table.contains(":"))
			HdfsHelper.reNameExistsPath(hdfsOper, outpath_table, tarPath);
		return job;
	}

	public static void main(String[] args) throws Exception
	{
		Job job = CreateJob(args);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
