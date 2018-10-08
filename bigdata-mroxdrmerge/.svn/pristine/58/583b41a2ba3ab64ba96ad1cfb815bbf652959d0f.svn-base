package cn.mastercom.bigdata.mapr.mergestat;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import cn.mastercom.bigdata.mapr.mergestat.MergeStatMapper.MergeMapper;
import cn.mastercom.bigdata.mapr.mergestat.MergeStatReducer.StatReducer;
import cn.mastercom.bigdata.mapr.util.tools.MapReduceMainTools;
import cn.mastercom.bigdata.util.StringHelper;
import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;
import cn.mastercom.bigdata.util.hadoop.mapred.CombineSmallFileInputFormat;
import cn.mastercom.bigdata.util.hadoop.mapred.DataDealConfiguration;
import cn.mastercom.bigdata.mergestat.deal.MergeKey;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.HdfsHelper;

public class MergeStatMain
{
	protected static final Log LOG = LogFactory.getLog(MergeStatMain.class);

	private static int reduceNum;
	private static String queueName;
	private static String outpath_date;
	private static String outpath_table;
	private static int inpathCount;
	private static int[] inpathTypes;
	private static String[] inpaths;
	private static int outpathCount;
	private static int[] outpathTypes;
	private static String[] outpathIndexs;
	private static String[] outpaths;
	//////////////
	private static String outpath;

	private static void makeConfig(Job job, Configuration conf, String[] args)
	{
		int index = 0;
		reduceNum = Integer.parseInt(args[index++]);
		queueName = args[index++];
		outpath_date = args[index++];
		outpath_table = args[index++];

		inpathCount = Integer.parseInt(args[index++]);
		inpathTypes = new int[inpathCount];
		inpaths = new String[inpathCount];
		for (int i = 0; i < inpathCount; ++i)
		{
			inpathTypes[i] = Integer.parseInt(args[index++]);
			inpaths[i] = args[index++];
		}

		outpathCount = Integer.parseInt(args[index++]);
		outpathTypes = new int[outpathCount];
		outpathIndexs = new String[outpathCount];
		outpaths = new String[outpathCount];
		for (int i = 0; i < outpathCount; ++i)
		{
			outpathTypes[i] = Integer.parseInt(args[index++]);
			outpathIndexs[i] = args[index++];
			outpaths[i] = args[index++];
		}

		for (int i = 0; i < args.length; ++i)
		{
			LOG.info(i + ": " + args[i] + "\n");
		}

		// table output path
		outpath = outpath_table + "/output";

		String inpathindex = "";
		for (int i = 0; i < inpathCount; ++i)
		{
			inpathindex += inpathTypes[i] + ";" + inpaths[i] + "\\$";
		}
		inpathindex = StringHelper.SideTrim(inpathindex, "\\$");
		conf.set("mastercom.mroxdrmerge.mergestat.inpathindex", inpathindex);

		String outputindex = "";
		for (int i = 0; i < outpathCount; ++i)
		{
			outputindex += outpathTypes[i] + ";" + outpathIndexs[i] + ";" + outpaths[i] + "\\$";
		}
		outputindex = StringHelper.SideTrim(outputindex, "\\$");
		conf.set("mastercom.mroxdrmerge.mergestat.outpathindex", outputindex);

		if (!MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
		{
			MapReduceMainTools.CustomMaprParas(conf, queueName);
			// 初始化自己的配置管理
			DataDealConfiguration.create(outpath_table, conf);
		}

	}

	public static Job CreateJob(Configuration conf, String[] args) throws Exception
	{
		// 检测输出目录是否存在，存在就改名

		Job job = Job.getInstance(conf, "MroXdrMerge.mergestat" + ":" + outpath_date);

		job.setJarByClass(MergeStatMain.class);
		job.setReducerClass(StatReducer.class);
		job.setMapOutputKeyClass(MergeKey.class);
		job.setMapOutputValueClass(Text.class);

		makeConfig(job, conf, args);
		long inputSize = 0;
		HDFSOper hdfsOper = null;
		if (!outpath_table.contains(":"))
		{
			hdfsOper = new HDFSOper(conf);
			for (int i = 0; i < inpathCount; ++i)
			{
				String[] tm_inpaths = inpaths[i].split(",");

				for (int j = 0; j < tm_inpaths.length; ++j)
				{
					if (hdfsOper.checkDirExist(tm_inpaths[j]))
					{
						inputSize += MapReduceMainTools.getFileSize(tm_inpaths[j], hdfsOper);
					}
				}
			}
		}
		reduceNum = MapReduceMainTools.getReduceNum(inputSize, conf);
		if (reduceNum < 20)
		{
			reduceNum = MapReduceMainTools.getReduceNum(inputSize, conf) * 10;
		}
		else
		{
			reduceNum = MapReduceMainTools.getReduceNum(inputSize, conf) * 5;
		}
		LOG.info("the count of mergestatall reduce to go is : " + reduceNum);
		job.setNumReduceTasks(reduceNum);
		///////////////////////////////////////////////////////

		// input
		for (int i = 0; i < inpathCount; ++i)
		{
			String[] tm_inpaths = inpaths[i].split(",");

			for (int j = 0; j < tm_inpaths.length; ++j)
			{
				if (hdfsOper != null && hdfsOper.checkDirExist(tm_inpaths[j]))
				{
					System.out.println("[info]input path is exists : " + tm_inpaths[j]);

					MultipleInputs.addInputPath(job, new Path(tm_inpaths[j]), CombineSmallFileInputFormat.class, MergeMapper.class);
				}
				else if (hdfsOper == null && new File(tm_inpaths[j]).exists())
				{
					System.out.println("[info]input path is exists : " + tm_inpaths[j]);
					MultipleInputs.addInputPath(job, new Path(tm_inpaths[j]), CombineSmallFileInputFormat.class, MergeMapper.class);
					System.err.println("tm_inpaths[j]: " + tm_inpaths[j]);
				}
				else
				{
					System.err.println("[warn]input path is not exists : " + tm_inpaths[j]);
				}
			}

		}

		// output
		for (int i = 0; i < outpathCount; ++i)
		{
			MultipleOutputs.addNamedOutput(job, outpathIndexs[i], TextOutputFormat.class, NullWritable.class, Text.class);
		}

		FileOutputFormat.setOutputPath(job, new Path(outpath));
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.GZip))
		{
			FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		}
		String tarPath = "";
		if (!outpath_table.contains(":"))
		{
			HdfsHelper.reNameExistsPath(hdfsOper, outpath_table, tarPath);
		}

		return job;
	}

	public static void main(String[] args) throws Exception
	{
		Job job = CreateJob(new Configuration(), args);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
