package cn.mastercom.bigdata.mapr.mdt.loc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import cn.mastercom.bigdata.mapr.mro.loc.MdtDataMapper.MdtImmMapper_HUAWEI;
import cn.mastercom.bigdata.mapr.mro.loc.MdtDataMapper.MdtImmMapper_ZTE;
import cn.mastercom.bigdata.mapr.mro.loc.MdtDataMapper.MdtLogMapper_HUAWEI;
import cn.mastercom.bigdata.mapr.mro.loc.MdtDataMapper.MdtLogMapper_ZTE;
import cn.mastercom.bigdata.mapr.mro.loc.MroLableMappers.CellPartitioner;
import cn.mastercom.bigdata.mapr.mro.loc.MroLableMappers.CellSortKeyComparator;
import cn.mastercom.bigdata.mapr.mro.loc.MroLableMappers.CellSortKeyGroupComparator;
import cn.mastercom.bigdata.mapr.util.tools.MapReduceMainTools;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.HdfsHelper;
import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;
import cn.mastercom.bigdata.util.hadoop.mapred.DataDealConfigurationV2;
import cn.mastercom.bigdata.util.hadoop.mapred.MultiOutputMngV2;
import cn.mastercom.bigdata.mro.loc.CellTimeKey;
import cn.mastercom.bigdata.xdr.prepare.stat.XdrPrepareTablesEnum;

public class MdtLabelFillMain {
	protected static final Log LOG = LogFactory.getLog(MdtLabelFillMain.class);
	
	private static int reduceNum;
	public static String queueName;
	public static String inpath_xdr_mme;
	public static String inpath_ResidentUser = "";
	public static String inpath_huaweiMdtimm;
	public static String inpath_huaweiMdtlog;
	public static String inpath_zteMdtimm;
	public static String inpath_zteMdtlog;
	public static String outpath;
	public static String outpath_table;
	public static String outpath_date;
	public static String mroXdrMergePath = "";
	//public static String inpath_imsicount;

//	public static String path_ImsiCellLocPath = "";
  
	private static void makeConfig_home(Job job, String[] args)
	{
		Configuration conf = job.getConfiguration();
		reduceNum = Integer.parseInt(args[0]);
		queueName = args[1];
		outpath_date = args[2].substring(3);
		inpath_xdr_mme = args[3];
		inpath_huaweiMdtimm = args[4];
		inpath_huaweiMdtlog = args[5];
		inpath_zteMdtimm = args[6];
		inpath_zteMdtlog = args[7];
		mroXdrMergePath = args[8];

		
		outpath_table = XdrPrepareTablesEnum.getBasePath(mroXdrMergePath, outpath_date);
		//path_ImsiCellLocPath = MainModel.GetInstance().getAppConfig().getPath_ImsiCellLocPath();
	
		
		for (int i = 0; i < args.length; ++i)
		{
			LOG.info(i + ": " + args[i] + "\n");
		}
		
		outpath = outpath_table + "/output";

		//conf.set("mastercom.mroxdrmerge.xdr.locfill.inpath_imsicount", inpath_imsicount);
		conf.set("mapreduce.job.date", outpath_date);
	
		if(!MainModel.GetInstance().getCompile().Assert(CompileMark.Debug)){
			
			MapReduceMainTools.CustomMaprParas(conf, queueName);
			// 初始化自己的配置管理
			DataDealConfigurationV2.create(outpath_table, job);
		}
		
		//TODO 调试用
		MultipleOutputs.setCountersEnabled(job, true);   //输出计数
	}
	
	
	public static Job CreateJob(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
		{
			//默认的HDFS端口
			conf.set("fs.defaultFS", "hdfs://192.168.1.31:9000");
		}
		return CreateJob(conf, args);
	}
	public static Job CreateJob(Configuration conf, String[] args) throws Exception
	{
		if (args.length < 5)
		{
			System.err.println("Usage: xdr lable fill <in-mro> <in-xdr> <out path> <out table path> <out path date>");
			throw (new Exception("MdtLabelFillMain args input error!"));
		}
		Job job = Job.getInstance(conf);
		makeConfig_home(job, args);
		String mmeFilter = "";
		String mdtFilter = "";
		String mmeKeyWord = "";
		String mdtKeyWord = "";

		if (args.length >= 10)
		{
			if (!args[10].toUpperCase().equals("NULL"))
			{
				mmeFilter = args[10];
			}

			if (!args[11].toUpperCase().equals("NULL"))
			{
				mdtFilter = args[11];
			}
			if (!args[12].toUpperCase().equals("NULL"))
			{
				mmeKeyWord = args[12];
			}

			if (!args[13].toUpperCase().equals("NULL"))
			{
				mdtKeyWord = args[13];
			}
		}

		// 检测输出目录是否存在，存在就改
		HDFSOper hdfsOper = null;
		job.setJobName("MroXdrMerge.mroxdr.xdr.locfill" + ":" + outpath_date);
		job.setJarByClass(MdtLabelFillMain.class);
		job.setReducerClass(MdtLableFileReduce.MdtDataFileReducers.class);
		job.setSortComparatorClass(CellSortKeyComparator.class);
		job.setPartitionerClass(CellPartitioner.class);
		job.setGroupingComparatorClass(CellSortKeyGroupComparator.class);
		job.setMapOutputKeyClass(CellTimeKey.class);
		job.setMapOutputValueClass(Text.class);

		long inputSize = 0;
		if (!inpath_xdr_mme.contains(":"))
		{
			hdfsOper = new HDFSOper(conf);
			inputSize += MapReduceMainTools.getFileSize(inpath_xdr_mme, hdfsOper, mmeFilter, mmeKeyWord);
			inputSize += MapReduceMainTools.getFileSize(inpath_huaweiMdtlog, hdfsOper, mdtFilter, mdtKeyWord);
			inputSize += MapReduceMainTools.getFileSize(inpath_huaweiMdtimm, hdfsOper, mdtFilter, mdtKeyWord);
			inputSize += MapReduceMainTools.getFileSize(inpath_zteMdtlog, hdfsOper, mdtFilter, mdtKeyWord);
			inputSize += MapReduceMainTools.getFileSize(inpath_zteMdtlog, hdfsOper, mdtFilter, mdtKeyWord);
		}
		reduceNum = MapReduceMainTools.getReduceNum(inputSize, conf);

		job.setNumReduceTasks(reduceNum);

		MapReduceMainTools.addInputPath(job, inpath_xdr_mme, hdfsOper, MmeLableMapper.XdrDataMapper_MME.class);
		//MapReduceMainTools.addInputPath(job, inpath_MDT_IMM, hdfsOper, MdtImmMapper.class);
		//MapReduceMainTools.addInputPath(job, inpath_MDT_LOG, hdfsOper, MdtLogMapper.class);
		MapReduceMainTools.addInputPath(job, inpath_huaweiMdtlog, hdfsOper, MdtLogMapper_HUAWEI.class);
		MapReduceMainTools.addInputPath(job, inpath_huaweiMdtimm, hdfsOper, MdtImmMapper_HUAWEI.class);
		MapReduceMainTools.addInputPath(job, inpath_zteMdtlog, hdfsOper, MdtLogMapper_ZTE.class);
		MapReduceMainTools.addInputPath(job, inpath_zteMdtimm, hdfsOper, MdtImmMapper_ZTE.class);

		MultiOutputMngV2.addNamedOutput(job, XdrPrepareTablesEnum.xdrLocation.getIndex(), 
											XdrPrepareTablesEnum.xdrLocation.getFileName(), 
											XdrPrepareTablesEnum.xdrLocation.getPath(mroXdrMergePath, outpath_date),
											TextOutputFormat.class, NullWritable.class, Text.class);

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
//		args=new String[]{
//			"100",
//			"root.queue1",
//			"01_180612",
//			"/mt_wlyh/Data/HaErBin/MME/20180612",
//			"/mt_wlyh/Data/HaErBin/mdt/imm-hw/20180612",
//			"/mt_wlyh/Data/HaErBin/mdt/log-hw/20180612",
//			"奇怪的东西",
//			"奇怪的东西",
//			"/mt_wlyh/Data/HaErBin/mroxdrmerge"
//			
//			
//		};
		Job job = CreateJob(args);
		//将作业提交到集群并等待它完成。
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}


	
}
