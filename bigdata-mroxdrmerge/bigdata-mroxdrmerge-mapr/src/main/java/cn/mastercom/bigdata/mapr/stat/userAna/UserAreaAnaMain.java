package cn.mastercom.bigdata.mapr.stat.userAna;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.mastercom.bigdata.mapr.stat.userAna.UserAnaMapper.HSRAreaMapper;
import cn.mastercom.bigdata.mapr.stat.userAna.UserAnaMapper.XdrLocationFilterMapper;
import cn.mastercom.bigdata.mapr.stat.userAna.UserAnaReducer.UserAreaReducer;
import cn.mastercom.bigdata.mapr.util.tools.MapReduceMainTools;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.stat.userAna.tableEnums.HsrEnums;
import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;
import cn.mastercom.bigdata.util.hadoop.mapred.DataDealConfigurationV2;


public class UserAreaAnaMain {
	protected static final Log LOG = LogFactory.getLog(UserAreaAnaMain.class);
	private static int reduceNum;
	public static String inpath_xdr;
	private static String queueName;
	private static String inputpath;
	private static String outpath;
	private static String date;
	
	private static void makeConfig(Job job, String[] args){
		queueName = args[0];
		date = args[1].trim().replace("01_", "");
		inputpath = args[2];
		outpath = args[3];
		reduceNum = 5;
		Configuration conf = job.getConfiguration();

		conf.set("mapreduce.job.output.path", outpath);
		conf.set("mapreduce.job.date", date);

		if (!queueName.equals("NULL"))
		{
			conf.set("mapreduce.job.queuename", queueName);
		}

		conf.set("mapreduce.reduce.speculative", "false");// 停止推测功能
		conf.set("mapreduce.job.jvm.numtasks", "-1");// jvm可以执行多个map
		MapReduceMainTools.CustomMaprParas(conf);
		conf.set("mapreduce.reduce.memory.mb", 16384 + "");
		conf.set("mapreduce.reduce.java.opts", "-XX:-UseGCOverheadLimit -Xmx" + (int) (16384 * 0.8) + "M");
		conf.set("mapreduce.job.reduce.slowstart.completedmaps", "0.9");// default		
		
		// 将小文件进行整合
		long splitMinSize = 128 * 1024 * 1024;
		conf.set("mapreduce.input.fileinputformat.split.maxsize", String.valueOf(splitMinSize));
		long minsizePerNode = 10 * 1024 * 1024;
		conf.set("mapreduce.input.fileinputformat.split.minsize.per.node", String.valueOf(minsizePerNode));
		long minsizePerRack = 32 * 1024 * 1024;
		conf.set("mapreduce.input.fileinputformat.split.minsize.per.rack", String.valueOf(minsizePerRack));
		
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.LZO_Compress))
		{
			// 中间过程压缩
			conf.set("io.compression.codecs",
					"org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.DeflateCodec,org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.Lz4Codec,org.apache.hadoop.io.compress.SnappyCodec,com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec");
			conf.set("mapreduce.map.output.compress", "LD_LIBRARY_PATH=" + MainModel.GetInstance().getAppConfig().getLzoPath());
			conf.set("mapreduce.map.output.compress", "true");
			conf.set("mapreduce.map.output.compress.codec", "com.hadoop.compression.lzo.LzoCodec");
		}
		// 初始化自己的配置管理
		DataDealConfigurationV2.create(HsrEnums.getBasePath(outpath, date)+ "/hsr_log_area", job);
	}
	
	public static Job CreateJob(String[] args) throws Exception{
		Configuration conf = new Configuration();
		if (args.length != 4) {
			System.err.println("input not enough queue/date/input/output");
			throw new IllegalArgumentException("args input error!");
		}
		Job job = Job.getInstance(conf, "HSRUserAreaAna");
		makeConfig(job, args);
		job.setJarByClass(UserAreaAnaMain.class);
		job.setReducerClass(UserAreaReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		long inputSize = MapReduceMainTools.addInputs(job, HSRAreaMapper.class, inputpath);
		
		reduceNum = MapReduceMainTools.getReduceNum(inputSize, conf);
		job.setNumReduceTasks(reduceNum);
		MapReduceMainTools.addOutputs(job, HsrEnums.HSR_USER_AREA, outpath, date);		
		FileOutputFormat.setOutputPath(job, new Path(HsrEnums.getBasePath(outpath, date) + "/output"));
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.GZip)) {
			FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		}
		return job;
	
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try{
			Job job = CreateJob(args);
			FileSystem fs = FileSystem.get(job.getConfiguration());
			Path outPath = null;
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.HiRail2)){
				outPath = new Path(HsrEnums.getBasePath(args[3], args[1].replace("01_", "")) + "/output");
			}
			job.waitForCompletion(true);

			if (fs.exists(outPath))
				fs.delete(outPath, true);
		}catch (Exception e){
			e.printStackTrace();
		}
			
	}

}
