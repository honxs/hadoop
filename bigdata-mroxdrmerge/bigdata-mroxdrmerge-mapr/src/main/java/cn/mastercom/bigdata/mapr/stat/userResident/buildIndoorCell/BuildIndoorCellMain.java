package cn.mastercom.bigdata.mapr.stat.userResident.buildIndoorCell;

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

import cn.mastercom.bigdata.mapr.stat.userResident.buildIndoorCell.BuildIndoorCellMapper.BuildIndoorCellMap;
import cn.mastercom.bigdata.mapr.stat.userResident.buildIndoorCell.BuildIndoorCellMapper.BuildIndoorCellPartitioner;
import cn.mastercom.bigdata.mapr.stat.userResident.buildIndoorCell.BuildIndoorCellMapper.BuildIndoorCellSortKeyComparator;
import cn.mastercom.bigdata.mapr.stat.userResident.buildIndoorCell.BuildIndoorCellMapper.BuildIndoorCellSortKeyGroupComparator;
import cn.mastercom.bigdata.mapr.stat.userResident.buildIndoorCell.BuildIndoorCellReducer.BuildIndoorCellReduce;
import cn.mastercom.bigdata.mapr.util.tools.MapReduceMainTools;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.stat.userResident.buildIndoorCell.BuildIndoorCellKey;
import cn.mastercom.bigdata.stat.userResident.enmus.BuildIndoorCellTablesEnum;
import cn.mastercom.bigdata.stat.userResident.enmus.ResidentConfigTablesEnum;
import cn.mastercom.bigdata.stat.userResident.enmus.ResidentUserTablesEnum;
import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;
import cn.mastercom.bigdata.util.hadoop.mapred.DataDealConfigurationV2;
import cn.mastercom.bigdata.util.hadoop.mapred.MultiOutputMngV2;

public class BuildIndoorCellMain
{
	protected static final Log LOG = LogFactory.getLog(BuildIndoorCellMain.class);
	private static int reduceNum;
	private static String queueName;
	private static String statTime;
	private static String date;
	private static String residentUser_inputPath;
	private static String outpath_table;
	private static String outpath;
	private static String mroXdrMergePath;

	private static void makeConfig(Job job, String[] args)
	{
		Configuration conf = job.getConfiguration();
		reduceNum = Integer.parseInt(args[0]);
		queueName = args[1];
		statTime = args[2];	//01_180827
		date = statTime.replace("01_", "");
		outpath_table = args[3];
		mroXdrMergePath = MainModel.GetInstance().getAppConfig().getMroXdrMergePath();

		residentUser_inputPath = ResidentConfigTablesEnum.User_Resident_Location.getPath(mroXdrMergePath, "");
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
			System.err.println("input not enough reduceNum/queueName/residentUser_inputPath/outpath_table");
			throw (new IOException("UserResidentMain args input error!"));
		}
		Job job = Job.getInstance(conf);
		makeConfig(job, args);
		job.setJobName("BuildIndoorCell" + ":" + statTime);
		HDFSOper hdfsOper = null;
		job.setJarByClass(BuildIndoorCellMain.class);
		job.setReducerClass(BuildIndoorCellReduce.class);
		job.setSortComparatorClass(BuildIndoorCellSortKeyComparator.class);
		job.setPartitionerClass(BuildIndoorCellPartitioner.class);
		job.setGroupingComparatorClass(BuildIndoorCellSortKeyGroupComparator.class);
		job.setMapOutputKeyClass(BuildIndoorCellKey.class);
		job.setMapOutputValueClass(Text.class);

		// set reduce num
		long inputSize = 0;
		if (!residentUser_inputPath.contains(":"))
		{
			hdfsOper = new HDFSOper(conf);
			inputSize += MapReduceMainTools.getFileSize(residentUser_inputPath, hdfsOper);
		}
		reduceNum = Math.max(reduceNum, MapReduceMainTools.getReduceNum(inputSize, conf));

		job.setNumReduceTasks(reduceNum);

		MapReduceMainTools.addInputPath(job, residentUser_inputPath, hdfsOper, BuildIndoorCellMap.class);

		for(BuildIndoorCellTablesEnum buildIndoorCellTablesEnum : BuildIndoorCellTablesEnum.values())
		{
			//注册resident_config
			MultiOutputMngV2.addNamedOutput(job, buildIndoorCellTablesEnum.getIndex(), buildIndoorCellTablesEnum.getFileName(), buildIndoorCellTablesEnum.getPath(mroXdrMergePath, ""),
					TextOutputFormat.class, NullWritable.class, Text.class);
		}
		
		//注册resident_user
		MultiOutputMngV2.addNamedOutput(job, ResidentUserTablesEnum.Build_Indoor_Cell.getIndex(), ResidentUserTablesEnum.Build_Indoor_Cell.getFileName(), ResidentUserTablesEnum.Build_Indoor_Cell.getPath(mroXdrMergePath, date),
				TextOutputFormat.class, NullWritable.class, Text.class);
		MultiOutputMngV2.addNamedOutput(job, ResidentUserTablesEnum.Build_pos_Indoor_Cell.getIndex(), ResidentUserTablesEnum.Build_pos_Indoor_Cell.getFileName(), ResidentUserTablesEnum.Build_pos_Indoor_Cell.getPath(mroXdrMergePath, date),
				TextOutputFormat.class, NullWritable.class, Text.class);
		MultiOutputMngV2.addNamedOutput(job, ResidentUserTablesEnum.User_Resident_Indoor.getIndex(), ResidentUserTablesEnum.User_Resident_Indoor.getFileName(), ResidentUserTablesEnum.User_Resident_Indoor.getPath(mroXdrMergePath, date),
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
