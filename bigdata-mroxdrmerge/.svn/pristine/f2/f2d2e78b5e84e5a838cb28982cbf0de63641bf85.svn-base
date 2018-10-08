package cn.mastercom.bigdata.mapr.stat.userResident.homeBroadbandLoc;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

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

import cn.mastercom.bigdata.mapr.stat.userResident.homeBroadbandLoc.MergeUserResidentMapper.MergeUserResidentPartitioner;
import cn.mastercom.bigdata.mapr.stat.userResident.homeBroadbandLoc.MergeUserResidentMapper.MergeUserResidentSortKeyGroupComparator;
import cn.mastercom.bigdata.mapr.stat.userResident.homeBroadbandLoc.MergeUserResidentMapper.TempNewUserResidentMap;
import cn.mastercom.bigdata.mapr.stat.userResident.homeBroadbandLoc.MergeUserResidentMapper.TempUserResidentMap;
import cn.mastercom.bigdata.mapr.stat.userResident.homeBroadbandLoc.MergeUserResidentMapper.UserResidentHomePlaceMap;
import cn.mastercom.bigdata.mapr.stat.userResident.homeBroadbandLoc.MergeUserResidentMapper.UserResidentWorkPlaceMap;
import cn.mastercom.bigdata.mapr.stat.userResident.homeBroadbandLoc.MergeUserResidentReducer.MergeUserResidentReduce;
import cn.mastercom.bigdata.mapr.util.tools.MapReduceMainTools;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.stat.userResident.enmus.ResidentConfigTablesEnum;
import cn.mastercom.bigdata.stat.userResident.enmus.ResidentLocTablesEnum;
import cn.mastercom.bigdata.stat.userResident.enmus.ResidentUserTablesEnum;
import cn.mastercom.bigdata.stat.userResident.homeBroadbandLoc.MergeUserResidentKey;
import cn.mastercom.bigdata.util.HdfsHelper;
import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;
import cn.mastercom.bigdata.util.hadoop.mapred.CombineSmallFileInputFormat;
import cn.mastercom.bigdata.util.hadoop.mapred.DataDealConfigurationV2;
import cn.mastercom.bigdata.util.hadoop.mapred.MultiOutputMngV2;

public class MergeUserResidentMain
{
	protected static final Log LOG = LogFactory.getLog(MergeUserResidentMain.class);
	private static int reduceNum;
	private static String queueName;
	private static String statTime;
	private static String date;
	private static String oldUserResident_inputPath;
	private static String UserResident_workInputPath;
	private static String UserResident_WeekendInputPath;
	private static String outpath_table;
	private static String outpath;
	private static String mroXdrMergePath;
	private static int workDayNum;
	private static int homeDayNum;
	private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

	private static void makeConfig(Job job, String[] args) throws Exception
	{
		Configuration conf = job.getConfiguration();
		reduceNum = Integer.parseInt(args[0]);
		queueName = args[1];
		statTime = args[2];	//01_180209
		date = statTime.replace("01_", "");
		outpath_table = args[3];

		mroXdrMergePath = MainModel.GetInstance().getAppConfig().getMroXdrMergePath();
		
		searchResidentUser(conf, mroXdrMergePath);
		oldUserResident_inputPath = ResidentConfigTablesEnum.getBasePath(mroXdrMergePath);

		// table output path
		outpath = outpath_table + "/output";
		conf.set("mastercom.workDayNum", workDayNum + "");
		conf.set("mastercom.homeDayNum", homeDayNum + "");
		conf.set("mastercom.finalTime", 20 + statTime.replace("01_", ""));

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
			System.err.println("input not enough reduceNum/queueName/statTime/outpath_table");
			throw (new IOException("MergeUserResidentMain args input error!"));
		}
		Job job = Job.getInstance(conf);
		makeConfig(job, args);
		job.setJobName("MergeUserResident" + ":" + statTime);
		HDFSOper hdfsOper = null;
		job.setJarByClass(MergeUserResidentMain.class);
		job.setReducerClass(MergeUserResidentReduce.class);
		job.setMapOutputKeyClass(MergeUserResidentKey.class);
		job.setGroupingComparatorClass(MergeUserResidentSortKeyGroupComparator.class);
		job.setMapOutputValueClass(Text.class);
		job.setPartitionerClass(MergeUserResidentPartitioner.class);

		long inputSize = 0;
		if (!UserResident_workInputPath.contains(":") || !UserResident_WeekendInputPath.contains(":"))
		{
			hdfsOper = new HDFSOper(conf);
			String tarPath = "";
			String tarPath1 = "";
			String tarPath2 = "";
			// 旧的常驻用户路径改名
			if (!oldUserResident_inputPath.contains(":"))
			{
				tarPath = HdfsHelper.reNameExistsPath(hdfsOper, oldUserResident_inputPath, tarPath);
			}
			tarPath1 = String.format("%s/tb_mr_user_location", tarPath);
			MapReduceMainTools.addInputPath(job, tarPath1, hdfsOper, TempUserResidentMap.class);
			inputSize += MapReduceMainTools.getFileSize(tarPath1, hdfsOper);
			
			tarPath2 = String.format("%s/tb_user_resident_location", tarPath);
			MapReduceMainTools.addInputPath(job, tarPath2, hdfsOper, TempNewUserResidentMap.class);
			inputSize += MapReduceMainTools.getFileSize(tarPath2, hdfsOper);
			
			if(workDayNum > 0)
			{
				inputSize += MapReduceMainTools.getFileSize(UserResident_workInputPath, hdfsOper);
			}
			if(homeDayNum - workDayNum > 0)
			{
				inputSize += MapReduceMainTools.getFileSize(UserResident_WeekendInputPath, hdfsOper);
			}
		}
		reduceNum = Math.max(reduceNum, MapReduceMainTools.getReduceNum(inputSize, conf));
		job.setNumReduceTasks(reduceNum);

		if(workDayNum > 0)
		{
			MapReduceMainTools.addInputPath(job, UserResident_workInputPath, hdfsOper,CombineSmallFileInputFormat.class, UserResidentWorkPlaceMap.class);
		}
		if(homeDayNum - workDayNum > 0)
		{
			MapReduceMainTools.addInputPath(job, UserResident_WeekendInputPath, hdfsOper,CombineSmallFileInputFormat.class, UserResidentHomePlaceMap.class);
		}
		
		for(ResidentUserTablesEnum residentUserTablesEnum : ResidentUserTablesEnum.values())
		{
			//注册resident_user
			MultiOutputMngV2.addNamedOutput(job, residentUserTablesEnum.getIndex(), residentUserTablesEnum.getFileName(), residentUserTablesEnum.getPath(mroXdrMergePath, date),
					TextOutputFormat.class, NullWritable.class, Text.class);
		}
		
		for(ResidentConfigTablesEnum residentConfigTablesEnum : ResidentConfigTablesEnum.values())
		{
			//注册resident_config
			MultiOutputMngV2.addNamedOutput(job, residentConfigTablesEnum.getIndex(), residentConfigTablesEnum.getFileName(), residentConfigTablesEnum.getPath(mroXdrMergePath, ""),
					TextOutputFormat.class, NullWritable.class, Text.class);
		}
		
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
	
	/**
	 * 搜索之前配置的天数的常驻用户数据
	 */
	public static void searchResidentUser(Configuration conf, String mroXdrMergePath) throws Exception
	{
		HDFSOper hdfsOper = new HDFSOper(conf);
		String date = "20" + statTime.replace("01_", "");//20180209
		Date formatDate = dateFormat.parse(date);
		Calendar calendar = Calendar.getInstance();
		
		String temp_UserResidentWorkPath = "";
		String temp_UserResidentWeekendPath = "";
		int mergeUserResidentDayNum = MainModel.GetInstance().getAppConfig().getMergeUserResidentDayNum();
		//自动搜索之前配置的天数的常驻用户数据
		for(int i = 0; i > mergeUserResidentDayNum; i--)
		{
			calendar.setTime(formatDate);
			calendar.add(Calendar.DATE, i);
			String oldDate = dateFormat.format(calendar.getTime()).substring(2, 8);
			int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK) - 1;
			//周一到周五拼工作地点路径
			if(dayOfWeek >= 1 && dayOfWeek <= 5)
			{
				//String workPath = String.format("%1$s/resident_loc/data_01_%2$s/tb_mr_user_location_dd_%2$s", mroXdrMergePath, oldDate);
				String workPath = ResidentLocTablesEnum.resident_user.getPath(mroXdrMergePath, oldDate);
				if(hdfsOper.checkDirExist(workPath))
				{
					workDayNum ++;
					homeDayNum ++;
					temp_UserResidentWorkPath += workPath + ",";
					
				}
			}
			else	//周六到周日拼家庭地点路径
			{
				//String weekendPath = String.format("%1$s/resident_loc/data_01_%2$s/tb_mr_user_location_dd_%2$s", mroXdrMergePath, oldDate);
				String weekendPath = ResidentLocTablesEnum.resident_user.getPath(mroXdrMergePath, oldDate);
				if(hdfsOper.checkDirExist(weekendPath))
				{
					homeDayNum ++;
					temp_UserResidentWeekendPath += weekendPath + ",";
					
				}
			}
		}
		
		if(temp_UserResidentWorkPath.contains(","))
		{
			UserResident_workInputPath = temp_UserResidentWorkPath.substring(0, temp_UserResidentWorkPath.length() - 1);
		}
		if(temp_UserResidentWeekendPath.contains(","))
		{
			UserResident_WeekendInputPath = temp_UserResidentWeekendPath.substring(0, temp_UserResidentWeekendPath.length() - 1);
		}
		
		if (homeDayNum <= 0)
		{
			System.out.println("dayNum  must  bigger  than  " + homeDayNum);
			return;
		}
	}
}
