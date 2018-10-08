package cn.mastercom.bigdata.mapr.xdr.loc;

import cn.mastercom.bigdata.xdr.loc.GsmTdDataType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.lib.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import cn.mastercom.bigdata.util.hadoop.mapred.CombineSmallFileInputFormat;
import cn.mastercom.bigdata.mapr.util.tools.MapReduceMainTools;
import cn.mastercom.bigdata.mapr.xdr.loc.XdrLabelMapper.ImsiPartitioner;
import cn.mastercom.bigdata.mapr.xdr.loc.XdrLabelMapper.ImsiSortKeyComparator;
import cn.mastercom.bigdata.mapr.xdr.loc.XdrLabelMapper.ImsiSortKeyGroupComparator;
import cn.mastercom.bigdata.mapr.xdr.loc.XdrLabelMapper.LocationMapper;
import cn.mastercom.bigdata.mapr.xdr.loc.XdrLabelMapper.LocationWFMapper;
import cn.mastercom.bigdata.mapr.xdr.loc.XdrLabelMapper.ResidentUserMap;
import cn.mastercom.bigdata.mapr.xdr.loc.XdrLabelMapper.XdrDataMapper_HTTP;
import cn.mastercom.bigdata.mapr.xdr.loc.XdrLabelMapper.XdrDataMapper_MME;
import cn.mastercom.bigdata.mapr.xdr.loc.XdrLabelMapper.XdrDataMapper_GSM;
import cn.mastercom.bigdata.mapr.xdr.loc.XdrLabelMapper.XdrDataMapper_TD;
import cn.mastercom.bigdata.mro.stat.tableEnum.XdrLocTablesEnum;
import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;
import cn.mastercom.bigdata.util.hadoop.mapred.DataDealConfigurationV2;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.HdfsHelper;
import cn.mastercom.bigdata.xdr.loc.ImsiTimeKey;
import cn.mastercom.bigdata.xdr.loc.register.XdrTableRegister;


public class XdrLabelFillMain
{
	protected static final Log LOG = LogFactory.getLog(XdrLabelFillMain.class);

	private static int reduceNum;
	public static String queueName;
	public static String inpath_xdr_mme;
	public static String inpath_xdr_http;
	public static String inpath_xdr_23g;
	public static String inpath_lable;
	public static String inpath_location;
	public static String inpath_locationWF;
	public static String inpath_imsicount;
	public static String outpath;
	public static String outpath_table;
	public static String outpath_date;
	public static String path_ImsiCellLocPath = "";
//	public static String inpath_xdr_gsm;
//	public static String inpath_xdr_td;
	public static String gsmCallPath;
	public static String gsmLocationPath;
	public static String gsmSmsPath;
	public static String tdCallPath;
	public static String tdLocationPath;
	public static String tdSmsPath;

	private static void makeConfig_home(Job job, String[] args)
	{
		Configuration conf = job.getConfiguration();

		reduceNum = Integer.parseInt(args[0]);
		queueName = args[1];
		outpath_date = args[2].substring(3);
		inpath_xdr_mme = args[3];
		inpath_xdr_http = args[4];
		inpath_xdr_23g = args[5];
		inpath_lable = args[6];
		inpath_location = args[7];
		inpath_locationWF = args[8];
		inpath_imsicount = args[9];
		outpath_table = XdrLocTablesEnum.getBasePath(args[10], outpath_date);
		gsmCallPath = args[11];
		gsmLocationPath = args[12];
		gsmSmsPath = args[13];
		tdCallPath = args[14];
		tdLocationPath = args[15];
		tdSmsPath = args[16];
		path_ImsiCellLocPath = MainModel.GetInstance().getAppConfig().getPath_ImsiCellLocPath();
	
		for (int i = 0; i < args.length; ++i)
		{
			LOG.info(i + ": " + args[i] + "\n");
		}

		outpath = outpath_table + "/output";
		conf.set("mapreduce.job.date", outpath_date);

		StringBuffer gsmDataTypePath = new StringBuffer();
		if (gsmCallPath.length() > 0 || gsmLocationPath.length() > 0 || gsmSmsPath.length() > 0)
		{
		    gsmDataTypePath.append(GsmTdDataType.XDR_23G_CALL.getDataType()).append("#").append(gsmCallPath).append(";")
                    .append(GsmTdDataType.XDR_23G_LOCATION.getDataType()).append("#").append(gsmLocationPath).append(";")
                    .append(GsmTdDataType.XDR_23G_SMS.getDataType()).append("#").append(gsmSmsPath);
		}
		conf.set("mastercom.mroxdrmerge.xdrloc.gsm.inpath", gsmDataTypePath.toString());

		StringBuffer tdDataTypePath = new StringBuffer();
		if (tdCallPath.length() > 0 || tdLocationPath.length() > 0 || tdSmsPath.length() > 0)
		{
		    tdDataTypePath.append(GsmTdDataType.XDR_23G_CALL.getDataType()).append("#").append(tdCallPath).append(";")
                    .append(GsmTdDataType.XDR_23G_LOCATION.getDataType()).append("#").append(tdLocationPath).append(";")
                    .append(GsmTdDataType.XDR_23G_SMS.getDataType()).append("#").append(tdSmsPath);
		}
		conf.set("mastercom.mroxdrmerge.xdrloc.td.inpath", tdDataTypePath.toString());

		if(!MainModel.GetInstance().getCompile().Assert(CompileMark.Debug)) {
			MapReduceMainTools.CustomMaprParas(conf, queueName);
			// 初始化自己的配置管理
			DataDealConfigurationV2.create(outpath_table, job);
		}
	}

	public static Job CreateJob(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
		{
			conf.set("fs.defaultFS", "hdfs://192.168.1.31:9000");
		}
		return CreateJob(conf, args);
	}

	public static Job CreateJob(Configuration conf, String[] args) throws Exception {
		if (args.length < 17) {
			System.err.println("Usage: xdr lable fill <in-mro> <in-xdr> <out path> <out table path> <out path date>");
			throw (new Exception("XdrLableFillMain args input error!"));
		}
		Job job = Job.getInstance(conf);
		makeConfig_home(job, args);
		String mmeFilter = "";
		String httpFilter = "";
		String mmeKeyWord = "";
		String httpKeyWord = "";

		if (args.length >= 20) {
			if (!args[17].toUpperCase().equals("NULL")) {
				mmeFilter = args[17];
			}

			if (!args[18].toUpperCase().equals("NULL")) {
				httpFilter = args[18];
			}
			if (!args[19].toUpperCase().equals("NULL")) {
				mmeKeyWord = args[19];
			}

			if (!args[20].toUpperCase().equals("NULL")) {
				httpKeyWord = args[20];
			}
		}

		// 检测输出目录是否存在，存在就改
		HDFSOper hdfsOper = null;
		job.setJobName("MroXdrMerge.mroxdr.xdr.locfill" + ":" + outpath_date);
		job.setJarByClass(XdrLabelFillMain.class);
		job.setReducerClass(XdrLabelFileSeqReducer.XdrDataFileReducer.class);
		job.setSortComparatorClass(ImsiSortKeyComparator.class);
		job.setPartitionerClass(ImsiPartitioner.class);
		job.setGroupingComparatorClass(ImsiSortKeyGroupComparator.class);
		job.setMapOutputKeyClass(ImsiTimeKey.class);
		job.setMapOutputValueClass(Text.class);

		long inputSize = 0;
		if (!inpath_xdr_mme.contains(":")) {
			hdfsOper = new HDFSOper(conf);
			inputSize += MapReduceMainTools.getFileSize(inpath_xdr_mme, hdfsOper, mmeFilter, mmeKeyWord);
			inputSize += MapReduceMainTools.getFileSize(inpath_xdr_http, hdfsOper, httpFilter, httpKeyWord);
			//23g
			inputSize += MapReduceMainTools.getFileSize(gsmCallPath, hdfsOper);
			inputSize += MapReduceMainTools.getFileSize(gsmLocationPath, hdfsOper);
			inputSize += MapReduceMainTools.getFileSize(gsmSmsPath, hdfsOper);
			inputSize += MapReduceMainTools.getFileSize(tdCallPath, hdfsOper);
			inputSize += MapReduceMainTools.getFileSize(tdLocationPath, hdfsOper);
			inputSize += MapReduceMainTools.getFileSize(tdSmsPath, hdfsOper);
		}
		reduceNum = MapReduceMainTools.getReduceNum(inputSize, conf);

		job.setNumReduceTasks(reduceNum);

		MapReduceMainTools.addInputPath(job, inpath_xdr_mme, hdfsOper, XdrDataMapper_MME.class);
		MapReduceMainTools.addInputPath(job, inpath_xdr_http, hdfsOper, XdrDataMapper_HTTP.class);
		MapReduceMainTools.addInputPath(job, inpath_location, hdfsOper, LocationMapper.class);
		MapReduceMainTools.addInputPath(job, inpath_locationWF, hdfsOper, LocationWFMapper.class);
		// 用户常驻小区配置表
		MapReduceMainTools.addInputPath(job, path_ImsiCellLocPath, hdfsOper, ResidentUserMap.class);
		//23g
		MapReduceMainTools.addInputPath(job, gsmCallPath, hdfsOper, CombineSmallFileInputFormat.class, XdrDataMapper_GSM.class);
		MapReduceMainTools.addInputPath(job, gsmLocationPath, hdfsOper, CombineSmallFileInputFormat.class, XdrDataMapper_GSM.class);
		MapReduceMainTools.addInputPath(job, gsmSmsPath, hdfsOper, CombineSmallFileInputFormat.class, XdrDataMapper_GSM.class);
		MapReduceMainTools.addInputPath(job, tdCallPath, hdfsOper, CombineSmallFileInputFormat.class, XdrDataMapper_TD.class);
		MapReduceMainTools.addInputPath(job, tdLocationPath, hdfsOper, CombineSmallFileInputFormat.class, XdrDataMapper_TD.class);
		MapReduceMainTools.addInputPath(job, tdSmsPath, hdfsOper, CombineSmallFileInputFormat.class, XdrDataMapper_TD.class);

		XdrTableRegister xdrTable = new XdrTableRegister(args[10], outpath_date);
		xdrTable.registerOutFileName(job);

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