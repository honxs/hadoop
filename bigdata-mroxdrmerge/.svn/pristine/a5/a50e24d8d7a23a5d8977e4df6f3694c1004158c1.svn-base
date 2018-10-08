package cn.mastercom.bigdata.mapr.stat.userResident;

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

import cn.mastercom.bigdata.loc.userResident.UserResidentKey;
import cn.mastercom.bigdata.mapr.stat.userResident.UserResidentMapper.MroLocLibMap;
import cn.mastercom.bigdata.mapr.stat.userResident.UserResidentMapper.UserResidentMap;
import cn.mastercom.bigdata.mapr.stat.userResident.UserResidentMapper.UserResidentPartitioner;
import cn.mastercom.bigdata.mapr.stat.userResident.UserResidentMapper.UserResidentSortKeyComparator;
import cn.mastercom.bigdata.mapr.stat.userResident.UserResidentMapper.UserResidentSortKeyGroupComparator;
import cn.mastercom.bigdata.mapr.stat.userResident.UserResidentMapper.XdrLocLibMap;
import cn.mastercom.bigdata.mapr.stat.userResident.UserResidentReducer.UserResidentReduce;
import cn.mastercom.bigdata.mapr.util.tools.MapReduceMainTools;
import cn.mastercom.bigdata.mro.stat.tableEnum.MroCsOTTTableEnum;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.stat.userResident.enmus.ResidentLocTablesEnum;
import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;
import cn.mastercom.bigdata.util.hadoop.mapred.DataDealConfigurationV2;
import cn.mastercom.bigdata.util.hadoop.mapred.MultiOutputMngV2;

public class UserResidentMain
{
	protected static final Log LOG = LogFactory.getLog(UserResidentMain.class);
	private static int reduceNum;
	private static String queueName;
	private static String statTime;
	private static String userResident_inputPath;
	private static String mroLocLib_inputPath;
	private static String xdrLocLib_inputPath;
	private static String outpath_table;
	private static String outpath;
    private static String date;
    private static String mroXdrMergePath;
	private static void makeConfig(Job job, String[] args)
	{
		Configuration conf = job.getConfiguration();
		reduceNum = Integer.parseInt(args[0]);
		queueName = args[1];
		statTime = args[2];
		date = statTime.replace("01_", "");
		outpath_table =  ResidentLocTablesEnum.getBasePath(args[3], date);
		mroXdrMergePath = MainModel.GetInstance().getAppConfig().getMroXdrMergePath();
		
		userResident_inputPath = ResidentLocTablesEnum.xdrcellhourTime.getPath(mroXdrMergePath, date);
		mroLocLib_inputPath = MroCsOTTTableEnum.mrloclib.getPath(mroXdrMergePath, date);
		xdrLocLib_inputPath = MroCsOTTTableEnum.xdrloclib.getPath(mroXdrMergePath, date);
		// table output path
		outpath = outpath_table + "/output";
	
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
		if (args.length != 4)
		{
			System.err.println("input not enough reduceNum/queueName/statTime/filterTimes/mroLocLibDistance/userResident_inputPath/mroLocLib_inputPath/outpath_table");
			throw (new IOException("UserResidentMain args input error!"));
		}
		Job job = Job.getInstance(conf);
		makeConfig(job, args);
		job.setJobName("ResidentUser" + ":" + statTime);
		HDFSOper hdfsOper = null;
		job.setJarByClass(UserResidentMain.class);
		job.setReducerClass(UserResidentReduce.class);
		job.setSortComparatorClass(UserResidentSortKeyComparator.class);
		job.setPartitionerClass(UserResidentPartitioner.class);
		job.setGroupingComparatorClass(UserResidentSortKeyGroupComparator.class);
		job.setMapOutputKeyClass(UserResidentKey.class);
		job.setMapOutputValueClass(Text.class);

		// set reduce num
		long inputSize = 0;
		if (!userResident_inputPath.contains(":"))
		{
			hdfsOper = new HDFSOper(conf);
			inputSize += MapReduceMainTools.getFileSize(userResident_inputPath, hdfsOper);
			inputSize += MapReduceMainTools.getFileSize(mroLocLib_inputPath, hdfsOper);
			inputSize += MapReduceMainTools.getFileSize(xdrLocLib_inputPath, hdfsOper);
		}
		reduceNum = Math.max(reduceNum, MapReduceMainTools.getReduceNum(inputSize, conf));

		job.setNumReduceTasks(reduceNum);

		MapReduceMainTools.addInputPath(job, userResident_inputPath, hdfsOper, UserResidentMap.class);
		MapReduceMainTools.addInputPath(job, mroLocLib_inputPath, hdfsOper, MroLocLibMap.class);
		MapReduceMainTools.addInputPath(job, xdrLocLib_inputPath, hdfsOper, XdrLocLibMap.class);

		MultiOutputMngV2.addNamedOutput(job, ResidentLocTablesEnum.resident_user.getIndex(), ResidentLocTablesEnum.resident_user.getFileName(), 
				ResidentLocTablesEnum.resident_user.getPath(mroXdrMergePath, date),
				TextOutputFormat.class, NullWritable.class, Text.class);

		FileOutputFormat.setOutputPath(job, new Path(outpath));
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.GZip))
		{
			FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		}
		
		return job;
	}

	public static void main(String[] args) throws Exception
	{
		Job job = CreateJob(args);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
