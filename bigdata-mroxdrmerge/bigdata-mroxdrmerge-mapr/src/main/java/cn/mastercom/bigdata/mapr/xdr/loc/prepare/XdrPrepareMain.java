package cn.mastercom.bigdata.mapr.xdr.loc.prepare;

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

import cn.mastercom.bigdata.mapr.util.tools.MapReduceMainTools;
import cn.mastercom.bigdata.mapr.xdr.loc.prepare.XdrPrepareMapper.XdrDataMapper_HTTP;
import cn.mastercom.bigdata.mapr.xdr.loc.prepare.XdrPrepareMapper.XdrDataMapper_MME;
import cn.mastercom.bigdata.mapr.xdr.loc.prepare.XdrPrepareReducer.StatReducer;
import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;
import cn.mastercom.bigdata.util.hadoop.mapred.DataDealConfigurationV2;
import cn.mastercom.bigdata.util.hadoop.mapred.MultiOutputMngV2;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.HdfsHelper;
import cn.mastercom.bigdata.xdr.prepare.deal.XdrPrepareDeal;
import cn.mastercom.bigdata.xdr.prepare.stat.XdrPrepareTablesEnum;

public class XdrPrepareMain {
	protected static final Log LOG = LogFactory.getLog(XdrPrepareMain.class);

	public static int reduceNum;
	public static String queueName;
	public static String outpath_date;
	public static String inpath_mme;
	public static String inpath_http;
	public static String outpath_table;
	public static String outpath;
	public static String path_ImsiCount;
	public static String path_ImsiIP;
	public static String path_Location;
	public static final int ImsiCount = 1;
	public static final int ImsiIP = 2;

	private static void makeConfig(Job job, String[] args) throws Exception {
		Configuration conf = job.getConfiguration();

		reduceNum = Integer.parseInt(args[0]);
		queueName = args[1];
		outpath_date = args[2];
		inpath_mme = args[3];
		inpath_http = args[4];
		outpath_table = XdrPrepareTablesEnum.getBasePath(args[5], outpath_date.substring(3));

		for (int i = 0; i < args.length; ++i) 
		{
			LOG.info(i + ": " + args[i] + "\n");
		}

		// table output path
		outpath = outpath_table + "/output";
//		path_ImsiCount = outpath_table + "/TB_IMSI_COUNT_" + outpath_date;
//		path_ImsiIP = outpath_table + "/TB_IMSI_IP_" + outpath_date;
		path_Location = outpath_table + "/TB_LOCATION_" + outpath_date;

		MapReduceMainTools.CustomMaprParas(conf, queueName);
		// 初始化自己的配置管理
		DataDealConfigurationV2.create(outpath_table, job);
	}

	public static Job CreateJob(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug)) 
		{
			conf.set("fs.defaultFS", "hdfs://192.168.1.31:9000");
		}
		return CreateJob(conf, args);
	}

	public static Job CreateJob(Configuration conf, String[] args) throws Exception {
		if (args.length < 6) {
			System.err.println("Usage: XdrPrepare <in-mro> <in-xdr> <sample tbname> <event tbname>");
			throw (new Exception("XdrPrepare args input error!"));
		}

		Job job = Job.getInstance(conf);

		makeConfig(job, args);
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
		{
			conf.set("fs.defaultFS", "hdfs://192.168.1.31:9000");
		}

		String mmeFilter = "";
		String httpFilter = "";

		if (args.length >= 8) {
			if (!args[6].toUpperCase().equals("NULL")) {
				mmeFilter = args[6];
			}

			if (!args[7].toUpperCase().equals("NULL")) {
				httpFilter = args[7];
			}
		}

		job.setJobName("MroXdrMerge.mroxdr.xdrprepare" + ":" + outpath_date);
		job.setJarByClass(XdrPrepareMain.class);
//		job.setReducerClass(StatReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

//		long inputSize = 0;
		HDFSOper hdfsOper = null;
		if (!inpath_http.contains(":")) {
			hdfsOper = new HDFSOper(conf);
//			inputSize += MapReduceMainTools.getFileSize(inpath_http, hdfsOper);
//			inputSize += MapReduceMainTools.getFileSize(inpath_mme, hdfsOper);
		}
//		reduceNum = MapReduceMainTools.getReduceNum(inputSize, conf);

		job.setNumReduceTasks(0);
		///////////////////////////////////////////////////////

		MapReduceMainTools.addInputPath(job, inpath_http, hdfsOper, XdrDataMapper_HTTP.class);
//		MapReduceMainTools.addInputPath(job, inpath_mme, hdfsOper, XdrDataMapper_MME.class);

//		MultiOutputMngV2.addNamedOutput(job, ImsiCount, "imsicount", path_ImsiCount, TextOutputFormat.class,
//				NullWritable.class, Text.class);
//		MultiOutputMngV2.addNamedOutput(job, ImsiIP, "imsiip", path_ImsiIP, TextOutputFormat.class, NullWritable.class,
//				Text.class);
		MultiOutputMngV2.addNamedOutput(job, XdrPrepareTablesEnum.xdrLocation.getIndex(), XdrPrepareTablesEnum.xdrLocation.getFileName(), 
				XdrPrepareTablesEnum.xdrLocation.getPath(args[5], outpath_date.substring(3)), TextOutputFormat.class,NullWritable.class, Text.class);

		FileOutputFormat.setOutputPath(job, new Path(outpath));
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.GZip)) {
			FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		}
		String tarPath = "";
		if (hdfsOper != null)
			HdfsHelper.reNameExistsPath(hdfsOper, outpath_table, tarPath);

		return job;
	}

	public static void main(String[] args) throws Exception {
		
//		Job job = CreateJob(args);
//		System.exit(job.waitForCompletion(true) ? 0 : 1);
//		args = new String[]{"1","","01_180125","","","mt_wlyh/Data/BeiJing/mroxdrmerge"};
//		Configuration conf = new Configuration();
//		conf.set("fs.defaultFS", "hdfs://192.168.1.31:9000");
//		Job job = Job.getGsmCellInfo(conf);
//		makeConfig(job, args);
		
		System.out.println(outpath_table);
	}

}
