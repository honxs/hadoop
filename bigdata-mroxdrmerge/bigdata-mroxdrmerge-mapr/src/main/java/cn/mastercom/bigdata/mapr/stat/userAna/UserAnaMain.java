package cn.mastercom.bigdata.mapr.stat.userAna;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.mastercom.bigdata.mapr.stat.userAna.UserAnaMapper.SectionDatatypePartitioner;
import cn.mastercom.bigdata.mapr.stat.userAna.UserAnaMapper.SectionGroupComparator;
import cn.mastercom.bigdata.mapr.stat.userAna.UserAnaMapper.XdrLocationFilterMapper;
import cn.mastercom.bigdata.mapr.stat.userAna.UserAnaReducer.UserConfigReducer;
import cn.mastercom.bigdata.mapr.util.tools.MapReduceMainTools;
import cn.mastercom.bigdata.util.hadoop.hdfs.FileReader;
import cn.mastercom.bigdata.util.hadoop.hdfs.FileReader.LineHandler;
import cn.mastercom.bigdata.util.hadoop.mapred.DataDealConfigurationV2;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.stat.userAna.tableEnums.HsrEnums;
import cn.mastercom.bigdata.stat.userAna.tableEnums.SubwayEnums;

public class UserAnaMain
{
	protected static final Log LOG = LogFactory.getLog(UserAnaMain.class);
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

		conf.set("mapreduce.job.output.path", outpath);
		conf.set("mapreduce.job.date", date);

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
		DataDealConfigurationV2.create(HsrEnums.getBasePath(outpath, date)+ "/hsr_log_2", job);
	}

	public static Job CreateJob(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		if (args.length != 4)
		{
			System.err.println("input not enough queue/date/input/output");
			throw new IllegalArgumentException("args input error!");
		}
		Job job = Job.getInstance(conf, "HSRUserAnalysis_2");
		makeConfig(job, args);
		job.setJarByClass(UserAnaMain.class);
		job.setReducerClass(UserConfigReducer.class);
		job.setMapOutputKeyClass(UserAnaSectionKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setPartitionerClass(SectionDatatypePartitioner.class);
		job.setGroupingComparatorClass(SectionGroupComparator.class);

		// set reduce num
		// 根据区间数来确定
		final AtomicInteger reduceNum = new AtomicInteger(0);

		if (MainModel.GetInstance().getCompile().Assert(CompileMark.HiRail2))
		{
			// 高铁的区间数
			FileReader.readFile(job.getConfiguration(), MainModel.GetInstance().getAppConfig().getHsrSectionPath(), new LineHandler()
			{

				@Override
				public void handle(String line)
				{
					reduceNum.getAndIncrement();
				}

			});
			MapReduceMainTools.addOutputs(job, HsrEnums.HSR_TRAIN_INFO, outpath, date);
			MapReduceMainTools.addOutputs(job, HsrEnums.HSR_LOCATION_POINT, outpath, date);
			MapReduceMainTools.addOutputs(job, HsrEnums.HSR_IMSI, outpath, date);
			MapReduceMainTools.addOutputs(job, HsrEnums.HSR_NOVER_TIME, outpath, date);
			MapReduceMainTools.addOutputs(job, HsrEnums.HSR_SEG_INFO, outpath, date);
			MapReduceMainTools.addOutputs(job, HsrEnums.HSR_SEG_NOCOVER, outpath, date);
			
			//MapReduceMainTools.addOutputs(job, HsrEnums.HSR_USER_AREA, outpath, date);
			//test
			MapReduceMainTools.addOutputs(job, HsrEnums.HSR_XDR_INFO, outpath, date);
		}
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.Subway))
		{
			// 地铁的区间数
			FileReader.readFile(job.getConfiguration(), MainModel.GetInstance().getAppConfig().getHsrSectionPath(), new LineHandler()
			{

				@Override
				public void handle(String line)
				{
					reduceNum.getAndIncrement();
				}

			});
			MapReduceMainTools.addOutputs(job, SubwayEnums.SUBWAY_TRAIN_INFO, outpath, date);
			MapReduceMainTools.addOutputs(job, SubwayEnums.SUBWAY_LOCATION_POINT, outpath, date);
			MapReduceMainTools.addOutputs(job, SubwayEnums.SUBWAY_IMSI, outpath, date);
			MapReduceMainTools.addOutputs(job, SubwayEnums.SUBWAY_NOVER_TIME, outpath, date);
		}
		
//		MultipleInputs.addInputPath(job, new Path(input_xdrLocation), CombineTextInputFormat.class, XdrLocationFilterMapper.class);
		MapReduceMainTools.addInputs(job, XdrLocationFilterMapper.class, input_xdrLocation.split(","));

//		job.setNumReduceTasks(reduceNum.get() * 2);//0115更改为同一区间两个方向在不同的reduce
		job.setNumReduceTasks(reduceNum.get() * 2 * 8);//0117更改为同一区间两个方向在不同的reduce 加上3小时分区
//		job.setNumReduceTasks(1);
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
