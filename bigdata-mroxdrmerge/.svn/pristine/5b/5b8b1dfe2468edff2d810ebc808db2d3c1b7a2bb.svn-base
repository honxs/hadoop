package cn.mastercom.bigdata.mapr.stat.freqloc;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
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

import cn.mastercom.bigdata.mapr.stat.freqloc.FreqLocMapper.FreqLocMap;
import cn.mastercom.bigdata.mapr.stat.freqloc.FreqLocMapper.FreqLocPartitioner;
import cn.mastercom.bigdata.mapr.stat.freqloc.FreqLocReducer.FreqLocReduce;
import cn.mastercom.bigdata.mapr.util.tools.MapReduceMainTools;
import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;
import cn.mastercom.bigdata.util.hadoop.mapred.DataDealConfigurationV2;
import cn.mastercom.bigdata.util.hadoop.mapred.MultiOutputMngV2;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.stat.freqloc.FreqLocKey;
import cn.mastercom.bigdata.stat.tableinfo.enums.SingleProgEnums;
import cn.mastercom.bigdata.util.HdfsHelper;

public class FreqLocMain
{
	protected static final Log LOG = LogFactory.getLog(FreqLocMain.class);
	private static int reduceNum;
	private static String queueName;
	private static String statTime;
	private static String inputPath;
	private static String outpath_table;
	private static String outpath;
	private static String outputPath;

	private static void makeConfig(Job job, String[] args)
	{
		Configuration conf = job.getConfiguration();
		reduceNum = Integer.parseInt(args[0]);
		queueName = args[1];
		statTime = args[2];
		outpath_table = args[3]; // /mt_wlyh/Data/mroxdrmerge/freqloc
		
		String mroXdrMergePath = MainModel.GetInstance().getAppConfig().getMroXdrMergePath();
		String[] str = statTime.split(",", -1);
		for (String time : str)
		{
			String date = time.replace("01_", "");
			// /mt_wlyh/Data/mroxdrmerge/mro_loc/data_01_171110/tb_mr_insample_low_dd_171110,/mt_wlyh/Data/mroxdrmerge/mro_loc/data_01_171110/tb_mr_outsample_low_dd_171110
			inputPath += String.format("%s/mro_loc/data_%s/tb_mr_outsample_high_dd_%s,", mroXdrMergePath, time, date)
					+ String.format("%s/mro_loc/data_%s/tb_mr_outsample_mid_dd_%s,", mroXdrMergePath, time, date)
					+ String.format("%s/mro_loc/data_%s/tb_mr_outsample_low_dd_%s,", mroXdrMergePath, time, date)
					+ String.format("%s/mro_loc/data_%s/tb_mr_insample_high_dd_%s,", mroXdrMergePath, time, date)
					+ String.format("%s/mro_loc/data_%s/tb_mr_insample_mid_dd_%s,", mroXdrMergePath, time, date)
					+ String.format("%s/mro_loc/data_%s/tb_mr_insample_low_dd_%s", mroXdrMergePath, time, date);
		}

		// table output path
		outpath_table = mroXdrMergePath + "/freqloc/data_" + str[0];
		outpath = outpath_table + "/output";
		outputPath = outpath_table + "/tb_mr_cell_othercarrier_dd_" + str[0].replace("01_", "");

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
			throw (new IOException("FreqLocMain args input error!"));
		}
		Job job = Job.getInstance(conf, "FreqLoc");
		makeConfig(job, args);
		HDFSOper hdfsOper = null;
		job.setJarByClass(FreqLocMain.class);
		job.setReducerClass(FreqLocReduce.class);
		job.setMapOutputKeyClass(FreqLocKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setPartitionerClass(FreqLocPartitioner.class);

		long inputSize = 0;
		if (!inputPath.contains(":"))
		{
			hdfsOper = new HDFSOper(conf);
			inputSize += MapReduceMainTools.getFileSize(inputPath, hdfsOper);
		}
		reduceNum = MapReduceMainTools.getReduceNum(inputSize, conf);
		job.setNumReduceTasks(reduceNum);

		MapReduceMainTools.addInputPath(job, inputPath, hdfsOper, FreqLocMap.class);

		MultiOutputMngV2.addNamedOutput(job, SingleProgEnums.FreqLoc.getIndex(), SingleProgEnums.FreqLoc.getFileName(), SingleProgEnums.FreqLoc.getPath(outputPath), TextOutputFormat.class,
				NullWritable.class, Text.class);
		FileOutputFormat.setOutputPath(job, new Path(outpath));
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.GZip))
		{
			FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		}
		String tarPath = "";
		if (!outpath_table.contains(":"))
			HdfsHelper.reNameExistsPath(hdfsOper, outpath_table, tarPath);
		else
			new File(outpath_table).renameTo(new File(outpath_table + (new SimpleDateFormat("yyMMddHHmmss").format(new Date()))));
		return job;
	}

	public static void main(String[] args) throws Exception
	{
		Job job = CreateJob(args);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
