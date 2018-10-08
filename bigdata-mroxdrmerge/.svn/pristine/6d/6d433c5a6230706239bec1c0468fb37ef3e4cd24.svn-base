package cn.mastercom.bigdata.mapr.stat.userAna;

import cn.mastercom.bigdata.util.hadoop.mapred.DataDealConfigurationV2;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.stat.userAna.tableEnums.HsrEnums;
import cn.mastercom.bigdata.stat.userAna.tableEnums.SubwayEnums;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.mastercom.bigdata.mapr.stat.userAna.UserAnaMapper.DatatypePartitioner;
import cn.mastercom.bigdata.mapr.stat.userAna.UserAnaMapper.XdrLocationFilterMapper;
import cn.mastercom.bigdata.mapr.stat.userAna.UserAnaMapper.XdrlocationMapper;
import cn.mastercom.bigdata.mapr.stat.userAna.UserAnaReducer.PotentialUserReducer;
import cn.mastercom.bigdata.mapr.util.tools.MapReduceMainTools;

/**
 * TODO
 * 本步骤可以被优化掉：在XDR_LOC是多吐出一张表，记录 每个用户——站点 的最早最晚时间，也就是相当于当前mapper的工作
 * 然后mapper就不需要再遍历XDR_LOCATION了，这个JOB会快很多
 */
public class PotentialUserAnaMain
{
	protected static final Log LOG = LogFactory.getLog(PotentialUserAnaMain.class);
	private static String queueName;
	private static String input_xdrLocation;
	private static String outpath;
	private static String date;

	private static void makeConfig(Job job, String[] args)
	{
		queueName = args[0];
		date = args[1].trim().replace("01_", "");
		input_xdrLocation = args[2];
		outpath = args[3];
		Configuration conf = job.getConfiguration();

		if (!queueName.equals("NULL"))
		{
			conf.set("mapreduce.job.queuename", queueName);
		}
		// hadoop system set
		conf.set("mapreduce.job.reduce.slowstart.completedmaps", "0.9");// default
		conf.set("mapreduce.reduce.speculative", "false");// 停止推测功能
		conf.set("mapreduce.job.jvm.numtasks", "-1");// jvm可以执行多个map
		MapReduceMainTools.CustomMaprParas(conf);
		conf.set("mapreduce.reduce.memory.mb", 16384 + "");
		conf.set("mapreduce.reduce.java.opts", "-XX:-UseGCOverheadLimit -Xmx" + (int) (16384 * 0.8) + "M");

		// 将小文件进行整合
		long splitMinSize = 128 * 1024 * 1024;
		conf.set("mapreduce.input.fileinputformat.split.maxsize", String.valueOf(splitMinSize));
		long minsizePerNode = 10 * 1024 * 1024;
		conf.set("mapreduce.input.fileinputformat.split.minsize.per.node", String.valueOf(minsizePerNode));
		long minsizePerRack = 32 * 1024 * 1024;
		conf.set("mapreduce.input.fileinputformat.split.minsize.per.rack", String.valueOf(minsizePerRack));

		if (MainModel.GetInstance().getCompile().Assert(CompileMark.LZO_Compress))
		{
			// 中间过程压缩
			conf.set("io.compression.codecs",
					"org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.DeflateCodec,org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.Lz4Codec,org.apache.hadoop.io.compress.SnappyCodec,com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec");
			conf.set("mapreduce.map.output.compress", "LD_LIBRARY_PATH=" + MainModel.GetInstance().getAppConfig().getLzoPath());
			conf.set("mapreduce.map.output.compress", "true");
			conf.set("mapreduce.map.output.compress.codec", "com.hadoop.compression.lzo.LzoCodec");
		}
		// 初始化自己的配置管理
		DataDealConfigurationV2.create(HsrEnums.getBasePath(outpath, date) + "/hsr_log_1", job);
	}

	public static Job CreateJob(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		if (args.length != 4)
		{
			System.err.println("input not enough queue/date/input/output");
			throw new IllegalArgumentException("args input error!");
		}
		Job job = Job.getInstance(conf, "HSRUserAnalysis_1");
		makeConfig(job, args);
		job.setJarByClass(PotentialUserAnaMain.class);
		job.setReducerClass(PotentialUserReducer.class);
		job.setMapOutputKeyClass(PotentialUserAnaKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setPartitionerClass(DatatypePartitioner.class);

		// set reduce num
		int reduceNum = 2;

//		MultipleInputs.addInputPath(job, new Path(input_xdrLocation), CombineTextInputFormat.class, XdrlocationMapper.class);
		MapReduceMainTools.addInputs(job, XdrlocationMapper.class, input_xdrLocation.split(","));

		job.setNumReduceTasks(reduceNum);

		MapReduceMainTools.addOutputs(job, HsrEnums.HSR_TMP_POTENTIAL_USER, outpath, date);

		MapReduceMainTools.addOutputs(job, SubwayEnums.SUBWAY_TMP_POTENTIAL_USER, outpath, date);

		if (MainModel.GetInstance().getCompile().Assert(CompileMark.HiRail2))
		{
			FileOutputFormat.setOutputPath(job, new Path(HsrEnums.getBasePath(outpath, date) + "/output"));
		}
		else if (MainModel.GetInstance().getCompile().Assert(CompileMark.Subway))
		{
			FileOutputFormat.setOutputPath(job, new Path(SubwayEnums.getBasePath(outpath, date) + "/output"));
		}
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.GZip))
		{
			FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		}
		return job;
	}

	public static void main(String[] args)
	{
		// queue/date/input/output
		// args = new String[]{"NULL", "01_171127", "E:/文件/上海/xdr_location",
		// "E:/文件/上海/out"};
		try
		{
			Job job = CreateJob(args);

			FileSystem fs = FileSystem.get(job.getConfiguration());

			Path outPath = null;
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.HiRail2))
			{
				outPath = new Path(HsrEnums.getBasePath(args[3], args[1].replace("01_", "")) + "/output");
			}
			else if (MainModel.GetInstance().getCompile().Assert(CompileMark.Subway))
			{
				outPath = new Path(SubwayEnums.getBasePath(args[3], args[1].replace("01_", "")) + "/output");
			}

			job.waitForCompletion(true);

			if (fs.exists(outPath))
				fs.delete(outPath, true);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}
