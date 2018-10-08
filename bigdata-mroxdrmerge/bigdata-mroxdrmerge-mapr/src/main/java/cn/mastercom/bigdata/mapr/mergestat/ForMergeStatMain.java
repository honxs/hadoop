package cn.mastercom.bigdata.mapr.mergestat;

import java.io.File;

import cn.mastercom.bigdata.util.StringHelper;
import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;
import cn.mastercom.bigdata.util.hadoop.mapred.CombineSmallFileInputFormat;
import cn.mastercom.bigdata.util.hadoop.mapred.DataDealConfigurationV2;
import cn.mastercom.bigdata.util.hadoop.mapred.MultiOutputMngV2;
import cn.mastercom.bigdata.mergestat.deal.MergeKey;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import cn.mastercom.bigdata.mapr.mergestat.MergeStatMapper.MergeMapper;
import cn.mastercom.bigdata.mapr.mergestat.MergeStatReducer.StatReducer;
import cn.mastercom.bigdata.mapr.util.tools.MapReduceMainTools;

public class ForMergeStatMain
{
	protected static final Log LOG = LogFactory.getLog(ForMergeStatMain.class);

	private static int reduceNum = 1;
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

	private static void makeConfig(Job job, String[] args, int roundId)
	{
		Configuration conf = job.getConfiguration();

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

//		for (int i = 0; i < args.length; ++i)
//		{
//			LOG.info(i + ": " + args[i] + "\n");
//		}

		// table output path
		outpath = outpath_table + "/output" + roundId;

		String inpathindex = "";
		for (int i = 0; i < inpathCount; ++i)
		{
			inpathindex += inpathTypes[i] + ";" + inpaths[i] + "\\$";
		}
		inpathindex = StringHelper.SideTrim(inpathindex, "\\$");
		conf.set("mastercom.mroxdrmerge.mergestat.inpathindex", inpathindex);
		if (!MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
		{
			MapReduceMainTools.CustomMaprParas(conf, queueName);
			// 将小文件进行整合
			long minsize = 32 * 1024 * 1024;
			long splitMinSize = minsize;
			conf.set("mapreduce.input.fileinputformat.split.minsize", String.valueOf(splitMinSize));

			long splitMaxSize = minsize * 2;
			conf.set("mapreduce.input.fileinputformat.split.maxsize", String.valueOf(splitMaxSize));

			long minsizePerNode = minsize / 2;
			conf.set("mapreduce.input.fileinputformat.split.minsize.per.node", String.valueOf(minsizePerNode));
			long minsizePerRack = minsize / 2;
			conf.set("mapreduce.input.fileinputformat.split.minsize.per.rack", String.valueOf(minsizePerRack));

			// 初始化自己的配置管理
			DataDealConfigurationV2.create(outpath_table, job);
		}

	}

	/**
	 * 
	 * @param conf
	 * @param args
	 * @param i
	 *            第几轮汇聚
	 * @return
	 * @throws Exception
	 */
	public static Job CreateJob(Configuration conf, String[] args, int roundId) throws Exception
	{
		// 检测输出目录是否存在，存在就改名
		Job job = Job.getInstance(conf);

		makeConfig(job, args, roundId);

		job.setJobName("MroXdrMerge.mergestat" + ":" + outpath_date);

		job.setJarByClass(ForMergeStatMain.class);
		job.setReducerClass(StatReducer.class);
		job.setMapOutputKeyClass(MergeKey.class);
		job.setMapOutputValueClass(Text.class);

		// set reduce num
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
//		reduceNum = MapReduceMainTools.getReduceNum(inputSize, conf);
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

		// input
		for (int i = 0; i < inpathCount; ++i)
		{
			String[] tm_inpaths = inpaths[i].split(",");

			for (int j = 0; j < tm_inpaths.length; ++j)
			{
				if (hdfsOper != null && hdfsOper.checkDirExist(tm_inpaths[j]))
				{
					System.err.println("addInput path  : " + tm_inpaths[j]);
					MultipleInputs.addInputPath(job, new Path(tm_inpaths[j]), CombineSmallFileInputFormat.class, MergeMapper.class);
				}
				else if (hdfsOper == null && new File(tm_inpaths[j]).exists())
				{
					MultipleInputs.addInputPath(job, new Path(tm_inpaths[j]), CombineSmallFileInputFormat.class, MergeMapper.class);
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
			if (outpaths[i].indexOf("hdfs://") >= 0)
			{
				int tm_sPos = outpaths[i].indexOf("/", ("hdfs://").length());
				outpaths[i] = outpaths[i].substring(tm_sPos);
			}
			outpaths[i] = StringHelper.SideTrim(outpaths[i], " ");
			outpaths[i] = StringHelper.SideTrim(outpaths[i], "/");
			outpaths[i] = StringHelper.SideTrim(outpaths[i], "\\\\");
			if (outpaths[i].indexOf("hdfs://") < 0)
			{
				outpaths[i] = "/" + outpaths[i];
			}
			MultiOutputMngV2.addNamedOutput(job, outpathTypes[i], outpathIndexs[i], outpaths[i], TextOutputFormat.class, NullWritable.class, Text.class);
		}

		FileOutputFormat.setOutputPath(job, new Path(outpath));

		return job;
	}

}
