package cn.mastercom.bigdata.mapr.stat.villagestat;

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

import cn.mastercom.bigdata.mapr.stat.villagestat.VillageStatMapper.GridPartitioner;
import cn.mastercom.bigdata.mapr.stat.villagestat.VillageStatMapper.GridSortKeyComparator;
import cn.mastercom.bigdata.mapr.stat.villagestat.VillageStatMapper.GridSortKeyGroupComparator;
import cn.mastercom.bigdata.mapr.stat.villagestat.VillageStatMapper.MroDataMapper;
import cn.mastercom.bigdata.mapr.stat.villagestat.VillageStatMapper.VillageGridMapper;
import cn.mastercom.bigdata.mapr.stat.villagestat.VillageStatReducer.MroDataFileReducer;
import cn.mastercom.bigdata.mapr.util.tools.MapReduceMainTools;
import cn.mastercom.bigdata.stat.village.MroVillageDeal;
import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;
import cn.mastercom.bigdata.util.hadoop.mapred.DataDealConfigurationV2;
import cn.mastercom.bigdata.util.hadoop.mapred.MultiOutputMngV2;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.HdfsHelper;

public class VillageStatMain
{
	protected static final Log LOG = LogFactory.getLog(VillageStatMain.class);

	private static int reduceNum;
	public static String queueName;

	public static String inpath1;
	public static String inpath2;
	public static String outpath;
	public static String outpath_table;
	public static String outpath_date;
	public static String path_sample;
	public static String path_grid;

	private static void makeConfig(Job job, String[] args)
	{
		Configuration conf = job.getConfiguration();
		reduceNum = 10;
		queueName = args[1];
		outpath_date = args[2];
		inpath1 = args[3];// village配置
		inpath2 = args[4];// mro数据
		outpath_table = args[5];

		for (int i = 0; i < args.length; ++i)
		{
			LOG.info(i + ": " + args[i] + "\n");
		}

		// table output path
		outpath = outpath_table + "/output";
		path_sample = outpath_table + "/TB_SIGNAL_VILLAGE_SAMPLE_" + outpath_date;
		path_grid = outpath_table + "/TB_SIGNAL_VILLAGE_GRID_" + outpath_date;

		LOG.info(path_sample);
		LOG.info(path_grid);

//		conf.set("mastercom.mroxdrmerge.mro.villagestat.path_sample", path_sample);
//		conf.set("mastercom.mroxdrmerge.mro.villagestat.path_grid", path_grid);

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
		if (args.length <= 6)
		{
			System.err.println("Usage: MroFormat <in-mro> <in-xdr> <sample tbname> <event tbname>");
			throw (new Exception("VillageStatMain args input error!"));
		}

		// 检测输出目录是否存在，存在就改名
		Job job = Job.getInstance(conf);
		makeConfig(job, args);
		HDFSOper hdfsOper = null;
		if (!inpath2.contains(":"))
		{
			hdfsOper = new HDFSOper(conf);
		}
		job.setJobName("MroXdrMerge.mro.villagestat" + ":" + outpath_date);
		job.setJarByClass(VillageStatMain.class);
		job.setReducerClass(MroDataFileReducer.class);
		job.setSortComparatorClass(GridSortKeyComparator.class);
		job.setPartitionerClass(GridPartitioner.class);
		job.setGroupingComparatorClass(GridSortKeyGroupComparator.class);
		job.setMapOutputKeyClass(GridTypeKey.class);
		job.setMapOutputValueClass(Text.class);


		// set reduce num
		long inputSize = 0;
		if (!inpath1.contains(":"))
		{
			inputSize += MapReduceMainTools.getFileSize(inpath1, hdfsOper);
			inputSize += MapReduceMainTools.getFileSize(inpath2, hdfsOper);
		}
		reduceNum = MapReduceMainTools.getReduceNum(inputSize, conf);

		job.setNumReduceTasks(reduceNum);
		///////////////////////////////////////////////////////

		MapReduceMainTools.addInputPath(job, inpath1, hdfsOper, VillageGridMapper.class);
		MapReduceMainTools.addInputPath(job, inpath2, hdfsOper, MroDataMapper.class);

		MultiOutputMngV2.addNamedOutput(job, MroVillageDeal.mrosample, "mrosample", path_sample, TextOutputFormat.class,
				NullWritable.class, Text.class);
		MultiOutputMngV2.addNamedOutput(job, MroVillageDeal.mrogrid, "mrogrid", path_grid, TextOutputFormat.class,
				NullWritable.class, Text.class);
//		MultipleOutputs.addNamedOutput(job, "mrosample", TextOutputFormat.class, NullWritable.class, Text.class);
//		MultipleOutputs.addNamedOutput(job, "mrogrid", TextOutputFormat.class, NullWritable.class, Text.class);

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
