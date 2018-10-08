package cn.mastercom.bigdata.spark.mroxdrmerge;

import cn.mastercom.bigdata.evt.locall.stat.TypeIoEvtEnum;
import cn.mastercom.bigdata.mergestat.deal.MergeGroupUtil;
import cn.mastercom.bigdata.mergestat.deal.MergeInputStruct;
import cn.mastercom.bigdata.mergestat.deal.MergeOutPutStruct;
import cn.mastercom.bigdata.mergestat.deal.MergeStatTablesEnum;
import cn.mastercom.bigdata.mro.stat.tableEnum.MroBsTablesEnum;
import cn.mastercom.bigdata.mro.stat.tableEnum.MroCsFgTableEnum;
import cn.mastercom.bigdata.mro.stat.tableEnum.MroCsOTTTableEnum;
import cn.mastercom.bigdata.mro.stat.tableEnum.XdrLocTablesEnum;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.mroxdrmerge.MyKryoRegistrator;
import cn.mastercom.bigdata.spark.common.output.format.MultiTextOutputFormat;
import cn.mastercom.bigdata.spark.evtstat.*;
import cn.mastercom.bigdata.spark.mergestat.MapByKeyFunc_StringKey;
import cn.mastercom.bigdata.spark.mergestat.MtFilterFunction;
import cn.mastercom.bigdata.spark.mroxdrmerge.mrloc.MapByTypeFunc_MrLoc2;
import cn.mastercom.bigdata.spark.mroxdrmerge.ottloc.MapByTypeFunc_OTT2;
import cn.mastercom.bigdata.spark.mroxdrmerge.xdrloc.MapByTypeFunc_XdrLoc2;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;
import cn.mastercom.bigdata.util.spark.HiveHelper;
import cn.mastercom.bigdata.util.spark.RDDLog;
import cn.mastercom.bigdata.util.spark.RDDResultOutputer;
import cn.mastercom.bigdata.util.spark.TypeInfo;
import cn.mastercom.bigdata.xdr.prepare.stat.XdrPrepareTablesEnum;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.storage.StorageLevel;
import scala.Serializable;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.text.SimpleDateFormat;
import java.util.*;

public class MroXdrMergeJob implements Serializable
{
	protected final Log LOG = LogFactory.getLog(MroXdrMergeJob.class);

	private String inpath_xdr_mme;
	private String inpath_xdr_http;
	private String inpath_location;
//	private String inpath_locationWF;
	private String inpath_ott;//
	private String inpath_mro;

	private String outpath_date;
	private String outpath_hour;
	
	private String mroXdrMergePath;
	
	private String strTime;
	private String lastStrTime;
	private String hiveDBName;
	private String xdrLocLibPath;
	private String tbLocLibPath;
	private String xdrLocAllPath;
	private static String parallelism;
	
	private RDDLog rddLog = new RDDLog();
	private transient HDFSOper hdfsOper = null;
	private static Map<String, Integer> statTypeMap = new HashMap<String, Integer>();
	private static SimpleDateFormat timeFormat = new SimpleDateFormat("yyyyMMddHHmm");
	private void makeConfig(Configuration conf, String[] args)
	{
		outpath_date = args[0];
		outpath_hour = args[1];
		if ("NULL".equals(outpath_hour.toUpperCase()))
		{
			outpath_hour = null;
		}
		inpath_xdr_mme = args[2];
		inpath_xdr_http = args[3];
		inpath_ott = args[4];
		inpath_location = args[5];
		inpath_mro = args[6];
		System.out.println("mro: " + inpath_mro);
		String statType = args.length >= 8 ? args[7] : "";
		String[] statTypes = statType.split(",");
		statTypeMap.clear();
		if (statType.trim().length() > 0)
		{
			for (int i = 0; i < statTypes.length; ++i)
			{
				statTypes[i] = statTypes[i].toUpperCase();
				statTypeMap.put(statTypes[i], 0);
			}
		}
		System.out.println("statTypes:" + statTypes);
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.ChongQing))
		{	
			strTime = "20" + outpath_date + outpath_hour;
			lastStrTime = "20" + outpath_date + String.format("%02d",Integer.parseInt(outpath_hour) - 1);
		}
		
		hiveDBName = "";
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.ChongQing))
			hiveDBName = MainModel.GetInstance().getSparkConfig().get_HiveDBName();

		for (int i = 0; i < args.length; ++i)
		{
			LOG.info("传入的第 " + i + "个参数为: " + ": " + args[i] + "\n");
		}
	}
	
	private static boolean checkDoStat(String type)
	{
		if (statTypeMap.values().size() == 0)
		{
			return true;
		}
		return statTypeMap.containsKey(type);
	}

	public void DoSparkJob(String[] args) throws Exception
	{
		//初始化日志输出
		LOGHelper.GetLogger().addWriteLogCallBack(rddLog);	

		if (args.length <= 6)  //!=14
		{
			System.err.println("param count is not right");
			System.exit(2);
		}
		
	    mroXdrMergePath = MainModel.GetInstance().getAppConfig().getMroXdrMergePath();
//		mroDataPath = MainModel.GetInstance().getAppConfig().getMroDataPath();
//		inpath_mro = MainModel.GetInstance().getAppConfig().getMTMroDataPath();
//	    mreDataPath = MainModel.GetInstance().getAppConfig().getMreDataPath();
//		locWFDataPath = MainModel.GetInstance().getAppConfig().getLocWFDataPath();
		
		// make config
		Configuration conf = new Configuration();
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
		{
			conf.set("fs.defaultFS", "hdfs://192.168.1.31:9000");
		}
		MainModel.GetInstance().setConf(conf);
		makeConfig(conf, args);

		// do work
		if(!MainModel.GetInstance().getCompile().Assert(CompileMark.SaveToHive))
		{
			try
			{
				if (!mroXdrMergePath.contains(":"))
				{
					hdfsOper = new HDFSOper(conf);
//					hdfsOper.reNameExistsPath(outpath_table, tarPath);
				}
			}
			catch (Exception e)
			{

			}
		}
		DoMroXdrMerge(conf, args);
	}
	
	
	private void OptionConfig(final Configuration conf, SparkConf sparkConf)
	{
		
		//set hadoop conf
		conf.set("mapreduce.output.fileoutputformat.compress", "false");
		conf.set("dfs.blocksize", "32m");

		//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		
		//set spark conf
		//设置driver大小
		if(MainModel.GetInstance().getCompile().Assert(CompileMark.ChongQing) && MainModel.GetInstance().getSparkConfig().get_spark_driver_maxResultSize().trim().length() > 0)
		{
			sparkConf.set("spark.driver.maxResultSize", MainModel.GetInstance().getSparkConfig().get_spark_driver_maxResultSize());
		}
		//TODO:根据各地市yarn-site.xml中配置即可，效果更好
		//设置nodemanager总大小 该节点上YARN可使用的物理内存总量 默认8G
//		if(MainModel.GetInstance().getSparkConfig().get_yarn_nodemanager_resource_memory().trim().length() > 0)
//		{
//			sparkConf.set("yarn.nodemanager.resource.memory-mb", MainModel.GetInstance().getSparkConfig().get_yarn_nodemanager_resource_memory());
//		}
//	
//		//设置container最大内存，注意executor-mem 不能超过container的最大内存，因为executor进程是跑在container中
//		if(MainModel.GetInstance().getSparkConfig().get_yarn_scheduler_maximum().trim().length() > 0)
//		{
//			sparkConf.set("yarn.scheduler.maximum-allocation-mb", MainModel.GetInstance().getSparkConfig().get_yarn_scheduler_maximum());
//		}
	
		//从命令行获取参数 假设这三个参数合理
		String executorNum = null;
		String executorCoreNum = null;
		String executorMemSize = null;
		if (!MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
		{
			executorNum = sparkConf.get("spark.executor.instances", "10");//一个application拥有的executor数量
			executorCoreNum = sparkConf.get("spark.executor.cores", "1");//单个executor核数
			executorMemSize = sparkConf.get("spark.executor.memory", "12g");//单个executor内存
		}
		
		parallelism = calcParallelism(executorNum, executorCoreNum, executorMemSize);
		
		//注意parallelism的大小设置，需要考虑spark.shuffle.file.buffer的大小，parallelism*spark.shuffle.file.buffer 不能超过excutor的内存
		//这个参数仅在使用raw rdd api时有效  每次发生shuffle的时候,都以该并行度为粒度,由parallelism值来决定分区数
		sparkConf.set("spark.default.parallelism", sparkConf.get("spark.default.parallelism", parallelism));

		//使用DataFrame或spark-sql的api时，使用下面这个参数才能生效
		sparkConf.set("spark.sql.shuffle.partitions", sparkConf.get("spark.sql.shuffle.partitions", parallelism));

		//当为true时，特定查询中的表达式求值的代码将会在运行时动态生成。对于一些拥有复杂表达式的查询，此选项可导致显著速度提升。然而，对于简单的查询，这个选项会减慢查询的执行
		sparkConf.set("spark.sql.codegen", "true");
		
		//设置单个分区最大存储
		String maxPartitionBytes = calcMaxParitionBytes(executorMemSize, parallelism);
		sparkConf.set("spark.files.maxPartitionBytes", sparkConf.get("spark.files.maxPartitionBytes", maxPartitionBytes));
		//The maximum number of bytes to pack into a single partition when reading files.
		sparkConf.set("spark.sql.files.maxPartitionBytes", sparkConf.get("spark.sql.files.maxPartitionBytes", maxPartitionBytes));
		
		//executor执行的时候，用的内存可能会超过executor-memoy，所以会为executor额外预留一部分内存
		sparkConf.set("spark.yarn.executor.memoryOverhead", "4096");
		
		//spark shuffle优化
		sparkConf.set("spark.shuffle.file.buffer", "64k");
		sparkConf.set("spark.reducer.maxSizeInFlight", "64m");
		sparkConf.set("spark.shuffle.io.maxRetries", "10");
		sparkConf.set("spark.shuffle.io.retryWait", "60s");
		sparkConf.set("spark.shuffle.manager", "hash");
		sparkConf.set("spark.shuffle.consolidateFiles", "true");
		sparkConf.set("spark.shuffle.io.numConnectionsPerPeer", "3");
		
//		sparkConf.set("spark.sql.shuffle.partitions", "200");
		
		//压缩调整,序列化
		sparkConf.set("spark.rdd.compress", "true");
		sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		sparkConf.set("spark.kryo.registrator", MyKryoRegistrator.class.getName()); //在Kryo序列化库中注册自定义的类集合
		
		sparkConf.set("spark.broadcast.compress", "true");
		sparkConf.set("spark.shuffle.compress", "true");
		sparkConf.set("spark.shuffle.spill", "true");
		sparkConf.set("spark.shuffle.spill.compress", "true");
//		sparkConf.set("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec");//高速度
//		sparkConf.set("spark.io.compression.codec", "org.apache.spark.io.LZFCompressionCodec");//高压缩比
		
		//spark rdd 存储调整
		//由于重庆测试库内存太少，要预留更多的空间给程序运行内存
		//程序运行所需内存4G
		//excutors=8G,程序运行内存=8G*0.9*（1-spark.storage.memoryFraction-spark.shuffle.memoryFraction）
		//程序运行内存=8G*0.9*0.6
		//sparkConf.set("spark.storage.memoryFraction", "0.2");
		
		
		
		//spark内存分配分三大块，系统预留(默认300M,低于450M抛异常)，
		//用于storage(caching RDD,broadcast,缓存results)
		//用于execution(用于 shuffles，如joins、sorts 和 aggregations，避免频繁的 IO 而需要内存 buffer)
		
		//spark.memory.fraction (JVM最大可用内存-系统预留内存)*spark.memory.fraction=execution+storage
		sparkConf.set("spark.memory.fraction", "0.7");//建议使用默认值0.75
		
		//不会被逐出内存的总量，表示一个相对于 spark.memory.fraction的比例。这个越高，那么执行混洗等操作用的内存就越少，从而溢出磁盘就越频繁
		sparkConf.set("spark.memory.storageFraction", "0.4");
		//如果true，Spark会尝试使用堆外内存。启用 后，spark.memory.offHeap.size必须为正数
		sparkConf.set("spark.memory.offHeap.enabled", "true");
		sparkConf.set("spark.memory.offHeap.size", "2048");
		//是否使用老式的内存管理模式（1.5以及之前) 默认false不启用,测试环境1.4，改为启用
		sparkConf.set("spark.memory.useLegacyMode", "false");
		
		
//		sparkConf.set("spark.storage.safetyFraction", "0.9");
		//启用旧版本内存管理模式，以下三个参数会生效
		//用于Shuffle过程中使用的内存达到总内存多少比例的时候开始Spill，默认0.2
//		sparkConf.set("spark.shuffle.memoryFraction","0.4");
//		sparkConf.set("spark.shuffle.safetyFraction", "0.8"); //shuffle内存：8g*0.4*0.8 内存不足时开始spill
		//用于缓存数据的内存比例，默认0.6
//		sparkConf.set("spark.storage.memoryFraction","0.5");
//		sparkConf.set("spark.storage.unrollFraction","0.2");//缓存数据内存 8g*0.9*0.3*0.2
		
		//设置excute gc参数
		//debug
		//sparkConf.set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+PrintGC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintHeapAtGC -XX:+PrintGCApplicationConcurrentTime");
		sparkConf.set("spark.executor.extraJavaOptions", "-XX:+UseG1GC");
		
		sparkConf.set("spark.files.useFetchCache", "true");
		sparkConf.set("spark.speculation", "true");
		sparkConf.set("spark.speculation.interval", "500");
		sparkConf.set("spark.speculation.quantile", "0.8");
		sparkConf.set("spark.speculation.multiplier", "1.5");
		
		// 打印日志
		sparkConf.set("yarn.log-aggregation-enable", "true");		
		sparkConf.set("spark.eventLog.compress", "false");
		sparkConf.set("spark.eventLog.enabled", "true");
		
		//输出运行配置信息
		if (!MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
		{
			System.out.println("========>spark.executor.instances: " + sparkConf.get("spark.executor.instances", "100"));
			System.out.println("========>spark.executor.cores: " + sparkConf.get("spark.executor.cores", "1"));
			System.out.println("========>spark.executor.memory: " + sparkConf.get("spark.executor.memory", "2g"));
			System.out.println("========>spark.default.parallelism: " + sparkConf.get("spark.default.parallelism", parallelism));
			System.out.println("========>spark.files.maxPartitionBytes: " + sparkConf.get("spark.files.maxPartitionBytes", maxPartitionBytes));
			System.out.println("========>spark.yarn.executor.memoryOverhead: " + sparkConf.get("spark.yarn.executor.memoryOverhead", ""));
		}
		
		
//		double excutorMemory = Double.parseDouble(sparkConf.get("spark.executor.memory").replace("g", ""));
//		double memoryFraction = Double.parseDouble(sparkConf.get("spark.memory.fraction"));
//		double memoryStorageFraction = Double.parseDouble(sparkConf.get("spark.memory.storageFraction"));
//		String memoryOffHeapEnabled = sparkConf.get("spark.memory.offHeap.enabled");
//		double memoryOffHeapSize = Double.parseDouble(sparkConf.get("spark.memory.offHeap.size"));
//		String memoryUseLegacyMode = sparkConf.get("spark.memory.useLegacyMode");
//		
//		System.out.println("========>spark.executor.memory: " + excutorMemory);
//		System.out.println("========>spark.memory.fraction: " + memoryFraction);
//		System.out.println("========>spark.memory.storageFraction: " + memoryStorageFraction);
//		System.out.println("========>spark.memory.offHeap.enabled: " + memoryOffHeapEnabled);
//		System.out.println("========>spark.memory.offHeap.size: " + memoryOffHeapSize);
//		System.out.println("========>spark.memory.useLegacyMode: " + memoryUseLegacyMode);
//		
//		
//		System.out.println("========>Excutor memory max(G): " + excutorMemory);
//		System.out.println("========>Excutor RDD Storage memory max(G): " + (excutorMemory - 0.5) * memoryFraction * memoryStorageFraction );
//		System.out.println("========>Excutor RDD Excution memory max(G): " + (excutorMemory - 0.5) * memoryFraction);
//		System.out.println("========>Excutor RDD User Memory max(G): " + (excutorMemory - 0.5) * (1-memoryFraction));
		

		if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
		{
			sparkConf.setMaster("local");
			sparkConf.set("spark.default.parallelism", "1");
		}
		////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		
	    //set hive config
		long minsize = 128 * 1024 * 1024L;

		// 将小文件进行整合 
		//从sparconf 里面设置 hadoop配置需要在前面加上 spark.hadoop.
		long splitMinSize = minsize;
		sparkConf.set("spark.hadoop.mapreduce.input.fileinputformat.split.minsize", String.valueOf(splitMinSize));
		long splitMaxSize = minsize * 2;
		sparkConf.set("spark.hadoop.mapreduce.input.fileinputformat.split.maxsize", String.valueOf(splitMaxSize));

		long minsizePerNode = minsize / 2;
		sparkConf.set("spark.hadoop.mapreduce.input.fileinputformat.split.minsize.per.node", String.valueOf(minsizePerNode));
		long minsizePerRack = minsize / 2;
		sparkConf.set("spark.hadoop.mapreduce.input.fileinputformat.split.minsize.per.rack", String.valueOf(minsizePerRack));
		
		sparkConf.set("spark.hadoop.hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat");
		
		//hive 0.13.1
		sparkConf.set("spark.hadoop.mapred.min.split.size", String.valueOf(splitMinSize));
		sparkConf.set("spark.hadoop.mapred.max.split.size", String.valueOf(splitMaxSize));
		sparkConf.set("spark.hadoop.mapred.min.split.size.per.node", String.valueOf(minsizePerNode));
		sparkConf.set("spark.hadoop.mapred.min.split.size.per.rack", String.valueOf(minsizePerRack));
		sparkConf.set("spark.hadoop.hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat");	
	    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		System.out.println("配置优化完成! ");
	}

	private static String calcParallelism(String executorNum, String executorCoreNum, String executorMemSize){
		//官方原话： In general, we recommend 2-3 tasks per CPU core in your cluster.
		//即 num-partitions = 2(3) * executor-cores * num-executors 
		if(executorCoreNum != null && executorNum != null){
			return String.valueOf(Integer.parseInt(executorCoreNum) * Integer.parseInt(executorNum) * 2);
		}
		return "200";
	}
	
	private static String calcMaxParitionBytes(String executorMemSize, String partitionNum){
		if(executorMemSize != null && partitionNum != null){
			int memSize = sizeFormat(executorMemSize);
			if(memSize > 0)	
				return String.valueOf(memSize / Integer.parseInt(partitionNum));
		}
		return "134217728";//128MB
	}
	
	private static int sizeFormat(String size){
		
		if(size == null || size.length() == 0)
			return 0;
		
		size = size.trim().toLowerCase();
		
		int value = Integer.parseInt(size.replaceAll("\\D+", ""));
		if(size.endsWith("g") || size.endsWith("gb")){
			return value * 1024 * 1024 * 1024;
		}else if(size.endsWith("m") || size.endsWith("mb")){
			return value * 1024 * 1024;
		}else if(size.endsWith("k") || size.endsWith("kb")){
			return value * 1024;
		}else{
			return value;
		}
	}
	
	private void DoMroXdrMerge(final Configuration conf, final String[] args) throws Exception
	{
		//set spark conf
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("Spark.MroXdrMerge" + ":" + outpath_date);
		
		//优化配置
        OptionConfig(conf, sparkConf);
		// do work
        final SparkContext sparkContext = new SparkContext(sparkConf);
	    final JavaSparkContext ctx = new JavaSparkContext(sparkContext);
	    HiveContext hiveContext = null;
	    if (MainModel.GetInstance().getCompile().Assert(CompileMark.SaveToHive))
	    {
	    	hiveContext = new HiveContext(JavaSparkContext.toSparkContext(ctx));
		}
//	    JobConf jobConf = new JobConf(conf);
	    
		try
		{
			//doStat_TestMR(jobConf, ctx, hiveContext);
			
			if(checkDoStat("XDRPREPARE"))
			{	
				Date stime = new Date();
				doStat_XDRPREPARE(ctx);
				Date etime = new Date();
				int mins = (int) (etime.getTime() / 1000L - stime.getTime() / 1000L) / 60;
				String timeFileName = String.format("%s/%dMins_%s_%s", XdrPrepareTablesEnum.getBasePath(mroXdrMergePath, outpath_date), mins, timeFormat.format(stime), timeFormat.format(etime));
				if (hdfsOper != null && !hdfsOper.checkFileExist(timeFileName))
				{
					hdfsOper.mkfile(timeFileName);
				}
			}
			
			if(checkDoStat("OTTLOC"))
			{
				if(MainModel.GetInstance().getCompile().Assert(CompileMark.SaveToHive))
			    {
		            //清除老数据
			    	String deleteSql = "insert overwrite table " + hiveDBName + ".wymr_dw_RESULT_TIME_yyyyMMDD partition(month_id='" + strTime.substring(0, 6) + "',day_id='" + strTime.substring(0,8) + "',hour_id='" + strTime + "') select tablename,data from " + hiveDBName + ".wymr_dw_TB_MODEL_RESULT_TIME_yyyyMMDD limit 0";
			    	System.out.println(deleteSql);
					hiveContext.sql(deleteSql);
			    }
				doStat_OTTLOC(ctx, hiveContext);
			}
			
			if (checkDoStat("XDRLOC"))
			{
				System.out.println("开始执行XDRLOC过程!");
				Date stime = new Date();
				doStat_XDRLOC(ctx, hiveContext);
				Date etime = new Date();
				int mins = (int) (etime.getTime() / 1000L - stime.getTime() / 1000L) / 60;
				String timeFileName = String.format("%s/%dMins_%s_%s", XdrLocTablesEnum.getBasePath(mroXdrMergePath, outpath_date), mins, timeFormat.format(stime), timeFormat.format(etime));
				if(hdfsOper != null && !hdfsOper.checkDirExist(timeFileName))
				{
					hdfsOper.mkfile(timeFileName);
				}
			}
			
			if (checkDoStat("MROLOC_NEW"))
			{
				System.out.println("开始执行MROLOC过程!");
				Date stime = new Date();
				doStat_MROLOC(ctx, hiveContext);
				Date etime = new Date();
				int mins = (int) (etime.getTime() / 1000L - stime.getTime() / 1000L) / 60;
				String timeFileName = String.format("%s/%dMins_%s_%s", MroCsOTTTableEnum.getBasePath(mroXdrMergePath, outpath_date), mins, timeFormat.format(stime), timeFormat.format(etime));
				if (hdfsOper != null && !hdfsOper.checkFileExist(timeFileName))
				{
					hdfsOper.mkfile(timeFileName);
				}
			}
			
			if (checkDoStat("XDRLOCALL"))
			{
				System.out.println("开始执行XDRLOCALL过程! ");
				
				doStat_XDRLOCALL(ctx, hiveContext);
				
			}
			if (checkDoStat("MERGESTATALL"))
			{
				System.out.println("开始执行MERGESTATALL过程! ");
				Date stime = new Date();
				doStat_MERGESTATALL(ctx, hiveContext);
				Date etime = new Date();
				int mins = (int) (etime.getTime() / 1000L - stime.getTime() / 1000L) / 60;
				String timeFileName = String.format("%s/%dMins_%s_%s", MergeStatTablesEnum.getBasePath(mroXdrMergePath, outpath_date), mins, timeFormat.format(stime), timeFormat.format(etime));
				if (hdfsOper != null) {
					hdfsOper.mkfile(timeFileName);
				}
			}			
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeDetailLog(LogType.error,"DoMroXdrMerge error", "DoMroXdrMerge error.", e);
			e.printStackTrace();
		}
		
		ctx.stop();
	}
	




//	private void doStat_TestMR(JobConf jobConf, JavaSparkContext ctx, HiveContext hiveContext) throws Exception
//	{	
//		String sqlstr = inpath_locationWF;
//		JavaRDD<String> rdd = HiveHelper.SearchRddByHive(sqlstr, hiveContext);	
//		rdd.saveAsTextFile("/cqwlbwy/mt_wlyh/Data/mroxdrmerge/sp_loc/data_01_170731/test");	
//	}
	
	

	private JavaPairRDD<Long, String> ottlocRdd = null;

	private void doStat_OTTLOC(JavaSparkContext ctx, HiveContext hiveContext) throws Exception
	{
		//test ott 存储路径 mroxdrmerge/ott_loc/180106/18
		String path_ott = String.format("%s/ott_loc/data_01_%s/%s", mroXdrMergePath, outpath_date, outpath_hour);
		Object[] params = new Object[5];
		if (hdfsOper != null)
		{
			hdfsOper.reNameExistsPath(path_ott, "");
		}
		params[0] = outpath_date;
		params[1] = path_ott;
		params[2] = outpath_hour;
		params[3] = mroXdrMergePath;
		params[4] = false;
		
		JavaRDD<String> ottRdd = null;
		if (inpath_ott.contains("select"))
		{
			try
			{
				params[4] = true;
//				ottRdd = HiveHelper.SearchRddByHive(inpath_ott, hiveContext);
				ottRdd = readRddByHive(ctx, hiveContext, inpath_ott);
			}
			catch (Exception e)
			{
				System.out.println("WARN: ottRdd is null! : " + inpath_ott);
				ottRdd = ctx.parallelize(Arrays.asList(""), 1);
			}		
		}
		else
		{
			//本地或集群
			if(hdfsOper ==null || hdfsOper.checkDirExist(inpath_ott))
			{
				ottRdd = ctx.textFile(inpath_ott);
			}
			else 
			{
				System.out.println("WARN: ottRdd is null! : " + inpath_ott);
				ottRdd = ctx.parallelize(Arrays.asList(""), 1);
			}
		}
		
//		MapByTypeFunc_OTT mapByTypeFunc_OTT = new MapByTypeFunc_OTT(params);
//		JavaPairRDD<TypeInfo, Iterable<String>> ottlocResultRDD = ottRdd.flatMapToPair(mapByTypeFunc_OTT);
//		ottlocResultRDD.persist(StorageLevel.MEMORY_AND_DISK());
		
		MapByImsiFunc_OTT mapByImsiFunc_OTT = new MapByImsiFunc_OTT();
		ottlocRdd = ottRdd.flatMapToPair(mapByImsiFunc_OTT);
		ottlocRdd.persist(StorageLevel.MEMORY_AND_DISK());
		
		MapByTypeFunc_OTT2 mapByTypeFunc_OTT2 = new MapByTypeFunc_OTT2(params);
		JavaPairRDD<TypeInfo, Iterable<String>> ottlocResultRDD = ottlocRdd.flatMapToPair(mapByTypeFunc_OTT2);
		
		RDDResultOutputer rddResultOutputer = new RDDResultOutputer(mapByTypeFunc_OTT2.getTypeInfoMng(), ottlocResultRDD);
		if(MainModel.GetInstance().getCompile().Assert(CompileMark.SaveToHive))
		{
			rddResultOutputer.saveAsHiveTableTime(hiveContext, hiveDBName, Integer.parseInt(parallelism), "wymr_dw_RESULT_TIME_yyyyMMDD", "wymr_dw_TB_MODEL_RESULT_TIME_yyyyMMDD",
					false, strTime, Arrays.asList(RDDLog.DataType_HIVE_LOG, MapByTypeFunc_OTT2.DataType_OTT_LOCATION));
		}
		else
		{
			ottlocResultRDD.count();
		}
	}
	
	private void doStat_XDRPREPARE(JavaSparkContext ctx) throws Exception 
	{
		String _successFile = XdrPrepareTablesEnum.getBasePath(mroXdrMergePath, outpath_date) + "/output/_SUCCESS";
		LOG.info("xdrPrepare执行结束的文件的路径为: " + _successFile);
		String path_xdrPrepare = XdrPrepareTablesEnum.getHourBasePath(mroXdrMergePath, outpath_date, outpath_hour);
		if (hdfsOper == null || !hdfsOper.checkFileExist(_successFile))
		{
			if (hdfsOper != null)
			{
				hdfsOper.reNameExistsPath(path_xdrPrepare, "");
			}
			Object [] params = new Object[5];
			params[0] = outpath_date;
			params[1] = path_xdrPrepare;
			params[2] = outpath_hour;
			params[3] = mroXdrMergePath;
			params[4] = false;
			JavaRDD<String> httpRdd = ctx.textFile(inpath_xdr_http);
			MapByUriFunc_HTTP mapByImsiFunc_HTTP = new MapByUriFunc_HTTP(params);
			httpRdd.mapPartitionsToPair(mapByImsiFunc_HTTP).count();
			if (hdfsOper != null)
			{
				hdfsOper.mkfile(_successFile);
			}
		}
		else
		{
			LOG.info("XDRPREPARE has been dealed succesfully:" + XdrLocTablesEnum.getBasePath(mroXdrMergePath, outpath_date));
		}
	}
	
	private void doStat_XDRLOC(JavaSparkContext ctx, HiveContext hiveContext) throws Exception
	{
//		String path_xdrloc = String.format("%s/xdr_loc/data_01_%s/%02d", mroXdrMergePath, outpath_date, outpath_hour);
		String path_xdrloc = XdrLocTablesEnum.getHourBasePath(mroXdrMergePath, outpath_date, outpath_hour);
		String _successFile = XdrLocTablesEnum.getBasePath(mroXdrMergePath, outpath_date) + "/output/_SUCCESS";
		if (hdfsOper == null || !hdfsOper.checkFileExist(_successFile))
		{
			if (hdfsOper != null)
			{
				hdfsOper.reNameExistsPath(path_xdrloc, "");
			}
			Object[] params = new Object[5];
			params[0] = outpath_date;
			params[1] = path_xdrloc;
			params[2] = outpath_hour;
			params[3] = mroXdrMergePath;
			params[4] = false;
			
			String locPath = "";
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.URI_ANALYSE))
			{
				locPath = XdrPrepareTablesEnum.xdrLocation.getPath(mroXdrMergePath, outpath_date);
			}
			if (locPath.length() > 0)
			{
				if (!checkDoStat("LOCFILL"))
				{
					locPath = locPath + "," + inpath_location;
				}
			}
			else
			{
				locPath = inpath_location;
			}
			
			JavaRDD<String> mmeRdd = null;
			JavaRDD<String> httpRdd = null;
			JavaRDD<String> lastXdrLocationRdd = null;
			JavaRDD<String> locationRdd = null;
			String inpath_lastxdrlocation = "";
			if (inpath_xdr_mme.contains("select"))
			{
				params[4] = true;
				String inpath_lastxdrlocation_table = "TB_XDR_LOCATION_SPAN_01_" + outpath_date;
				
				inpath_lastxdrlocation = "select data from  " + hiveDBName + ".wymr_dw_RESULT_TIME_yyyyMMDD where hour_id ='" + lastStrTime + "' and tablename ='" + inpath_lastxdrlocation_table + "'";
				System.out.println(inpath_lastxdrlocation_table);
				System.out.println(inpath_lastxdrlocation);
						
	//			httpRdd = HiveHelper.SearchRddByHive(inpath_xdr_http, hiveContext);			
	//			mmeRdd = HiveHelper.SearchRddByHive(inpath_xdr_mme, hiveContext);
				
				httpRdd = readRddByHive(ctx, hiveContext, inpath_xdr_http);			
				mmeRdd = readRddByHive(ctx, hiveContext, inpath_xdr_mme);
				lastXdrLocationRdd = readRddByHive(ctx, hiveContext, inpath_lastxdrlocation);
				locationRdd = readRddByHive(ctx, hiveContext, inpath_location);
			}
			else
			{
				// String inpath_lastxdrlocation = String.format("%s/xdr_loc/data_01_%s/%02d/TB_XDR_LOCATION_SPAN_01_%s", mroXdrMergePath, outpath_date,Integer.parseInt(outpath_hour)-1, outpath_date);
				
				mmeRdd = ctx.textFile(inpath_xdr_mme).coalesce(Integer.valueOf(parallelism));
				httpRdd = ctx.textFile(inpath_xdr_http).coalesce(Integer.valueOf(parallelism));
				lastXdrLocationRdd = readRddByHdfs(ctx, inpath_lastxdrlocation).coalesce(Integer.valueOf(parallelism));
				locationRdd = readRddByHdfs(ctx, locPath);
			}
	
			MapByImsiFunc_MME mapByImsiFunc_MME = new MapByImsiFunc_MME();
			JavaPairRDD<Long, String> pairMmeRdd = mmeRdd.flatMapToPair(mapByImsiFunc_MME);
	
			MapByImsiFunc_HTTP mapByImsiFunc_HTTP = new MapByImsiFunc_HTTP();
			JavaPairRDD<Long, String> pairHttpRdd = httpRdd.flatMapToPair(mapByImsiFunc_HTTP);
			
			MapByImsiFunc_XdrLocation mapByImsiFunc_XdrLocation = new MapByImsiFunc_XdrLocation();
			JavaPairRDD<Long, String> xdrLocationRdd = lastXdrLocationRdd.flatMapToPair(mapByImsiFunc_XdrLocation);
	
			MapByImsiFunc_Location mapByImsiFunc_Location = new MapByImsiFunc_Location();
			JavaPairRDD<Long, String> locRdd = locationRdd.flatMapToPair(mapByImsiFunc_Location);
			
			//聚合最终的位置信息
			JavaPairRDD<Long, String> locTotalRdd = null;
			if(ottlocRdd != null)
			{
				System.out.println("ottlocRdd is not null");
				locTotalRdd = locRdd.union(ottlocRdd);
			}
			else
			{
				locTotalRdd = locRdd;
			}
	
			JavaPairRDD<Long, Tuple4<Iterable<String>, Iterable<String>, Iterable<String>, Iterable<String>>> imsiDataRdd = pairMmeRdd.cogroup(pairHttpRdd, xdrLocationRdd, locTotalRdd);
			MapByTypeFunc_XdrLoc2 mapByTypeFunc_XdrDeal = new MapByTypeFunc_XdrLoc2(params);
			JavaPairRDD<TypeInfo, Iterable<String>> xdrLocRDD = imsiDataRdd.mapPartitionsToPair(mapByTypeFunc_XdrDeal);
			
			////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		    //需要缓存的内容
	//		RDDTypeResult xdrDealTypeResult = new RDDTypeResult(mapByTypeFunc_XdrDeal.getTypeInfoMng(), xdrLocRDD);
	//		xdrLocationRDD = xdrDealTypeResult.getResult(XdrLocTablesEnum.xdrLocation.getIndex());
	//		xdrLocationRDD.persist(StorageLevel.DISK_ONLY());
			
	
			///////////////////////////////////////////////////////////////////////////////////////////////////////////////
			//save act
			RDDResultOutputer rddResultOutputer = new RDDResultOutputer(mapByTypeFunc_XdrDeal.getTypeInfoMng(), xdrLocRDD);		
			if(MainModel.GetInstance().getCompile().Assert(CompileMark.SaveToHive))
			{
				ArrayList<Integer> indexList = new ArrayList<>();
				indexList.add(RDDLog.DataType_HIVE_LOG);
				for(XdrLocTablesEnum xdrLocTablesEnum : XdrLocTablesEnum.values())
				{
					indexList.add(xdrLocTablesEnum.getIndex());
				}
				rddResultOutputer.saveAsHiveTableTime(hiveContext, hiveDBName, Integer.parseInt(parallelism), "wymr_dw_RESULT_TIME_yyyyMMDD", "wymr_dw_TB_MODEL_RESULT_TIME_yyyyMMDD",
						false, strTime, indexList);
			}
			else 
			{
				xdrLocRDD.count();//触发操作
			}
			
			if (hdfsOper != null)
			{
				hdfsOper.mkfile(_successFile);
			}
		}
		else
		{
			LOG.info("XDRLOC has been dealed succesfully:" + XdrLocTablesEnum.getBasePath(mroXdrMergePath, outpath_date));
		}
	}
	
	private void doStat_MROLOC(JavaSparkContext ctx, HiveContext hiveContext) throws Exception
	{
//		String path_mrloc = String.format("%s/mro_loc/data_01_%s/%02d", mroXdrMergePath, outpath_date, outpath_hour);
		String path_mrloc = MroBsTablesEnum.getHourBasePath(mroXdrMergePath, outpath_date, outpath_hour);
		String _successFile = MroCsOTTTableEnum.getBasePath(mroXdrMergePath, outpath_date) + "/output/_SUCCESS";
		if (hdfsOper == null || !hdfsOper.checkFileExist(_successFile))
		{
			if (hdfsOper != null)
			{
				System.out.println(hdfsOper.checkFileExist(path_mrloc));
				hdfsOper.reNameExistsPath(path_mrloc, "");
			}
			Object[] params = new Object[5];
			params[0] = outpath_date;
			params[1] = path_mrloc;
			params[2] = outpath_hour;
			params[3] = mroXdrMergePath;
			params[4] = false;

			JavaRDD<String> mroRdd = null;
			JavaRDD<String> xdrLocationRDD = null;
			JavaRDD<String> allXdrLocationRdd = null;
			JavaRDD<String> lastXdrLocationRdd = null;
			JavaRDD<String> cellBuildRdd = null;
			JavaPairRDD<String, String> eci_mroRdd = null;
			JavaPairRDD<String, String> eci_cellBuildRdd = null;
		
			if (inpath_mro.contains("select"))
			{
				params[4] = true;
	//			mroRdd = HiveHelper.SearchRddByHive(inpath_mro, hiveContext);
				mroRdd = readRddByHive(ctx, hiveContext, inpath_mro);
				MapByCellFunc_Mro mapByCellFunc_Mro = new MapByCellFunc_Mro();
				eci_mroRdd = mroRdd.flatMapToPair(mapByCellFunc_Mro);
				
				//调试作用，可单独运行每一个环节
				//TODO:改进，直接使用xdrloc结果，不必再读取
				if (xdrLocationRDD == null)
				{
					String inpath_xdrlocation_table = "XDR_LOCATION_01_" + outpath_date;
					String inpath_xdrlocation = "select data from  " + hiveDBName + ".wymr_dw_RESULT_TIME_yyyyMMDD where hour_id ='" + strTime + "' and tablename ='" + inpath_xdrlocation_table + "'";	
					xdrLocationRDD = readRddByHive(ctx, hiveContext, inpath_xdrlocation).coalesce(Integer.parseInt(parallelism));
				}
						
				//lastxdrlocationspan_table
				String inpath_lastxdrlocationSpan_table = "TB_XDR_LOCATION_SPAN_01_" + outpath_date;
				String inpath_lastxdrlocationSpan = "select data from  " + hiveDBName + ".wymr_dw_RESULT_TIME_yyyyMMDD where hour_id ='" + lastStrTime + "' and tablename ='" + inpath_lastxdrlocationSpan_table + "'";	
				lastXdrLocationRdd = readRddByHive(ctx, hiveContext, inpath_lastxdrlocationSpan);
				
			}
			else
			{
				//为方便调试，将XDRLOC,MROLOC_NEW步骤分开运行
				if (xdrLocationRDD == null)
				{
					System.out.println("WARN: xdrLocationRDD is null,read data from hdfs...");
					String inpathXdrlocation = String.format("%s/xdr_loc/data_01_%s/XDR_LOCATION_01_%s", mroXdrMergePath, outpath_date, outpath_date);
					System.out.println("xdrLocation path:" + inpathXdrlocation);
					xdrLocationRDD = readRddByHdfs(ctx, inpathXdrlocation);
				}
				if (hdfsOper != null)
				{
					System.out.println(hdfsOper.checkFileExist(inpath_mro));
				}
				mroRdd = ctx.textFile(inpath_mro);
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.ChongQing))
				{
					//重庆原始mr数据就是mrall数据,不需要merge采样点
					MapByCellFunc_Mro mapByCellFunc_Mro = new MapByCellFunc_Mro();
					eci_mroRdd = mroRdd.flatMapToPair(mapByCellFunc_Mro);
				}
				else // if(MainModel.GetInstance().getCompile().Assert(CompileMark.HaErBin))
				{
					//mt解码mro数据
					//.coalesce(1000);//TODO:1000这个值需要通过计算
					//TODO:使用mapPartitions算子，注意内存溢出，所以要控制每个partition的数据大小
					MapByCellFunc_MrAll mapByCellFunc_MrAll = new MapByCellFunc_MrAll();
					eci_mroRdd = mroRdd.mapPartitionsToPair(mapByCellFunc_MrAll); //这一行如果不注释掉执行的时候回卡住
				}
				/*else
				{
					//非mt解码mro
					MapByS1apidFunc_MrAll mapByS1apidFunc_MrAll = new MapByS1apidFunc_MrAll();
					//eci_mroRdd = mroRdd.flatMapToPair(mapByS1apidFunc_MrAll).reduceByKey();
				}*/
				//上个小时最后10分钟xdrloc
				String inpath_lastxdrlocationSpan = "";
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.ChongQing))
				{
					inpath_lastxdrlocationSpan = String.format("%s/xdr_loc/data_01_%s/%02d/TB_XDR_LOCATION_SPAN_01_%s", mroXdrMergePath, outpath_date, Integer.parseInt(outpath_hour)-1, outpath_date);
				}
				lastXdrLocationRdd = readRddByHdfs(ctx, inpath_lastxdrlocationSpan);
			}
			
			//xdr数据聚合
			allXdrLocationRdd = xdrLocationRDD.union(lastXdrLocationRdd);
			MapByCellFunc_XdrLocation mapByCellFunc_XdrLocation = new MapByCellFunc_XdrLocation();
			JavaPairRDD<String, String> eci_xdrLocationRdd = allXdrLocationRdd.flatMapToPair(mapByCellFunc_XdrLocation);
	
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.ChongQing))
			{
				//小区楼宇配置
				String cellBuildPath = MainModel.GetInstance().getAppConfig().getCellBuildPath();
				System.out.println("INFO: cellBuildConfig: " + cellBuildPath);
				if(MainModel.GetInstance().getCompile().Assert(CompileMark.SaveToHive))
				{
					cellBuildRdd = readRddByHive(ctx, hiveContext, cellBuildPath);
				}
				else
				{
					cellBuildRdd = readRddByHdfs(ctx, cellBuildPath);
				}
			}
			else
			{
				System.out.println("INFO: cellBuildRdd is null!");
				cellBuildRdd = ctx.parallelize(Collections.singletonList(""), 1);
			}
			MapByCellFunc_CellBuild mapByCellFunc_CellBuild = new MapByCellFunc_CellBuild(params);
			eci_cellBuildRdd = cellBuildRdd.flatMapToPair(mapByCellFunc_CellBuild);
			
			MapByTypeFunc_MrLoc2 mapByTypeFunc_MrLoc = new MapByTypeFunc_MrLoc2(params);
			JavaPairRDD<String, Tuple3<Iterable<String>, Iterable<String>, Iterable<String>>> cogroup = eci_mroRdd.cogroup(eci_xdrLocationRdd,eci_cellBuildRdd);
			JavaPairRDD<TypeInfo, Iterable<String>> mrlocRDD = cogroup.mapPartitionsToPair(mapByTypeFunc_MrLoc);
			////////////////////////////////////////////////////////////////////////////////////////////////////////////////
			
			RDDResultOutputer rddResultOutputer = new RDDResultOutputer(mapByTypeFunc_MrLoc.getTypeInfoMng(), mrlocRDD);
			if(MainModel.GetInstance().getCompile().Assert(CompileMark.SaveToHive))
			{
				ArrayList<Integer> indexList = new ArrayList<>();
				indexList.add(RDDLog.DataType_HIVE_LOG);
				for (MroBsTablesEnum mroBsTablesEnum : MroBsTablesEnum.values())
				{
					indexList.add(mroBsTablesEnum.getIndex());
				}
				for (MroCsFgTableEnum mroCsFgTableEnum : MroCsFgTableEnum.values())
				{
					indexList.add(mroCsFgTableEnum.getIndex());
				}
				for (MroCsOTTTableEnum mroCsOTTTableEnum : MroCsOTTTableEnum.values())
				{
					indexList.add(mroCsOTTTableEnum.getIndex());
				}
				rddResultOutputer.saveAsHiveTableTime(hiveContext, hiveDBName, Integer.parseInt(parallelism), "wymr_dw_RESULT_TIME_yyyyMMDD", "wymr_dw_TB_MODEL_RESULT_TIME_yyyyMMDD",
						false, strTime, indexList);
			}
			else 
			{
				mrlocRDD.count();
			}
			
			if (hdfsOper != null)
			{
				hdfsOper.mkfile(_successFile);
			}
		}
		else
		{
			LOG.info("MROLOC has been dealed succesfully:" + XdrLocTablesEnum.getBasePath(mroXdrMergePath, outpath_date));
		}
	}

	private void doStat_XDRLOCALL(JavaSparkContext ctx, HiveContext hiveContext) throws Exception
	{
		
		// xdr位置库
        xdrLocLibPath = String.format("%s/mro_loc/data_01_%s/XDR_LOC_LIB_01_%s", mroXdrMergePath, outpath_date,
        		outpath_date);
        //tb位置库
        tbLocLibPath = String.format("%s/mro_loc/data_01_%s/TB_LOC_LIB_01_%s", mroXdrMergePath, outpath_date, outpath_date);
        String xdrLocAllDayPath = mroXdrMergePath + "/xdr_locall/data_01_" + outpath_date;
        xdrLocAllPath = xdrLocAllDayPath + "/imsi";
        String _successFile = xdrLocAllPath + "/output/_SUCCESS";
        if (hdfsOper == null || !hdfsOper.checkFileExist(_successFile))
        {
        	if (hdfsOper != null)
			{
				System.out.println(hdfsOper.checkFileExist(mroXdrMergePath + "/xdr_locall/data_01_" + outpath_date));
				hdfsOper.reNameExistsPath(xdrLocAllDayPath, "");
			}
        	Object[] params = new Object[5];
			params[0] = "01_" + outpath_date;
			params[1] = xdrLocAllPath;
			params[2] = outpath_hour;
			params[3] = mroXdrMergePath;
			params[4] = false;
			ArrayList<HashMap<Integer, String>> inputPathLists = EvtStatUtil.getInputPathMaps("01_" + outpath_date);
		    HashMap<Integer, String> imsiMap = inputPathLists.get(0); //imsi关联的map
		    HashMap<Integer, String> s1apidMap = inputPathLists.get(1);//s1apid关联的map
		    
		    // xdrData组成 key为imsi,value为data_type+"####"+Value
	        JavaPairRDD<String, String> allXdrRDD = null;
	        for (final Integer type : imsiMap.keySet()) {
	        	if(!hdfsOper.checkDirExist(imsiMap.get(type))) {
	        		continue;
	        	}
	            JavaRDD<String> xdrRDD = ctx.textFile(imsiMap.get(type));
	            // imsi, type+value
	            MapByXdrFillData mapByXdrFillData = new MapByXdrFillData(type);
	            MtFilterFunction mtFilter = new MtFilterFunction();
	            JavaPairRDD<String, String> xdrPairRDD = xdrRDD.mapToPair(mapByXdrFillData).filter(mtFilter);
	            if(allXdrRDD == null) 
	            {
	            	allXdrRDD = xdrPairRDD;
	            	continue;
	            }
	            if(xdrPairRDD != null) 
	            {
	            	 allXdrRDD = allXdrRDD.union(xdrPairRDD);
	            }
	        }
	       
	        //位置库组成  key为imsi,value为data_type+"####"+Value
	        MapByLocFillData mapByLocFillData = new MapByLocFillData();
	        MtFilterFunction mtFilter1 = new MtFilterFunction();
	        JavaPairRDD<String, String> allLocRDD = ctx.textFile(xdrLocLibPath + "," + tbLocLibPath)
	                .mapToPair(mapByLocFillData).filter(mtFilter1);

	        JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> cogroupRDD =
	                allXdrRDD.cogroup(allLocRDD);

	        //关联统计
	        MapByTypeFuncEvt mapByTypeFuncEvt = new MapByTypeFuncEvt(params);
	        JavaPairRDD<TypeInfo, Iterable<String>> resultRDD = cogroupRDD.mapPartitionsToPair(mapByTypeFuncEvt);
	        RDDResultOutputer rddResultOutputer = new RDDResultOutputer(mapByTypeFuncEvt.getTypeInfoMng(), resultRDD);

	        //保存
	        if(MainModel.GetInstance().getCompile().Assert(CompileMark.SaveToHive)) {
	        	
	            ArrayList<Integer> indexList = new ArrayList<>();
	            indexList.add(RDDLog.DataType_HIVE_LOG);
	            for (TypeIoEvtEnum typeIoEvtEnum : TypeIoEvtEnum.values()) {
	                indexList.add(typeIoEvtEnum.getIndex());
	            }
	            rddResultOutputer.saveAsHiveTableTime(hiveContext, hiveDBName, Integer.parseInt(parallelism), "wymr_dw_RESULT_TIME_yyyyMMDD", "wymr_dw_TB_MODEL_RESULT_TIME_yyyyMMDD",
	                    false, "01_" + outpath_date , indexList);
	        }
	        else
	        {
//		        	ResultFilterFunc resultFilterFunc = new ResultFilterFunc();
//		        	resultRDD.filter(resultFilterFunc);
	        	resultRDD.count();
	        }
	    	if (hdfsOper != null)
			{
				hdfsOper.mkfile(_successFile);
			}
        }
        else
        {
        	LOG.info("XDRLOCALL has been dealed successfully: " + _successFile);
        }

	}
	
	private void doStat_MERGESTATALL(JavaSparkContext ctx, HiveContext hiveContext) 
	{
 		String path_mergeStat = MergeStatTablesEnum.getHourBasePath(mroXdrMergePath, outpath_date, outpath_hour);
		String _successFile = path_mergeStat + "/output/_SUCCESS";
	    if (hdfsOper == null || !hdfsOper.checkFileExist(_successFile))
        {
//			Object[] params = new Object[5];
//			params[0] = outpath_date;
//			params[1] = path_mergeStat;
//			params[2] = outpath_hour;
//			params[3] = mroXdrMergePath;
//			params[4] = false;
			// 得到输入的type和路径
			ArrayList<MergeInputStruct> inputPath = MergeGroupUtil.addInputPath(mroXdrMergePath, outpath_date);
			System.out.println("输入路径的长度为 : " + inputPath.size());
			for (MergeInputStruct mic : inputPath)
			{
				System.out.println("输入的路径为: " + mic.inputPath);
			}
			//得到输出的type和路径，此type和输入的type一致
			ArrayList<MergeOutPutStruct> outputPath = MergeGroupUtil.addOutputPath(mroXdrMergePath, outpath_date);
			
			//HashMap<Integer, ArrayList<String>> typeInputPathsMap = new HashMap<Integer, ArrayList<String>>();
			HashMap<Integer, String> typeInputPathsMap = new HashMap<Integer, String>();
			HashMap<Integer, String> typeOutputPathsMap = new HashMap<Integer,String>();
			
			for (int i = (inputPath.size() - 1); i >= 0; i--)
			{
				long tempFileCount = hdfsOper.getFileCount(inputPath.get(i).inputPath);
				if (tempFileCount <= 0)// 去掉不存在的目录
				{
					inputPath.remove(i);
					outputPath.remove(i);
				}
			}
//			System.out.println("删除之后的size为: " + outputPath.size());
			for(int i = 0; i < inputPath.size(); i++)
			{
				int type =Integer.parseInt(inputPath.get(i).index);
//				if(typeInputPathsMap.containsKey(type))
//				{
//					typeInputPathsMap.get(type).add(inputPath.get(i).inputPath);
//				} 
//				else
//				{
//					ArrayList<String> pathList = new ArrayList<String>();
//					pathList.add(inputPath.get(i).inputPath);
//					typeInputPathsMap.put(type, pathList);
//				}
				typeInputPathsMap.put(type, inputPath.get(i).inputPath);
			}
			
			for(int i = 0; i < outputPath.size(); i++)
			{
				typeOutputPathsMap.put(Integer.parseInt(outputPath.get(i).index), outputPath.get(i).outpath);
			}

			String basePath = MergeStatTablesEnum.getBasePath(mroXdrMergePath, outpath_date);
			List<JavaPairRDD<String, String>> outputRddList = new LinkedList<>();
			for(Integer type : typeInputPathsMap.keySet())
			{
				// ArrayList<String> pathList = typeInputPathsMap.get(type);
				String path = typeInputPathsMap.get(type);
			
//				if(pathList == null)
//				{
//					LOGHelper.GetLogger().writeLog(LogType.error, "该" + type + " 的路径不存在,请检查! ");
//					System.err.println("maybe some err");
//					continue;
//				}
				/* 拼接路径 */
				String allPath = "";
//				for(String path : pathList) 
//				{
					
					//System.out.println("遍历集合: " + path);
					// allPath = allPath + path + ",";
					if (path.contains(","))
					{
						String [] strs = path.split(",", -1);
						for (String str : strs)
						{
							if (hdfsOper.checkDirExist(str))
							{
								allPath = allPath + str + ",";
							}
						}
					}
					else 
					{
						if(hdfsOper.checkDirExist(path))
						{
							allPath = allPath + path + ",";
						}
					}
//				}
			
				
				// 这里注释掉是为了测试
				if(allPath.length() > 0) 
				{
					allPath = allPath.substring(0, allPath.length() - 1);
//					System.out.println("输出的路径为: " + allPath);
					
					
//					JavaRDD<String> strRDD = ctx.textFile(allPath);
//					MapByKeyFunc_MergeKey mapByKeyFunc_MergeStat = null;
//					try
//					{
//						mapByKeyFunc_MergeStat = new MapByKeyFunc_MergeKey(type);
//					}
//					catch (Exception e) {
//						LOGHelper.GetLogger().writeDetailLog(LogType.error, "PairFlatMap RDD失败", e);
//						e.printStackTrace();
//					}
//					JavaPairRDD<MergeTypeItem, String> itemRDD = strRDD.flatMapToPair(mapByKeyFunc_MergeStat);
//
//					final int type1 = type;
//					ReduceByKeyFunc reduceFunc = new ReduceByKeyFunc(type1);
//					JavaPairRDD<MergeTypeItem,String> reduceByKey = itemRDD.reduceByKey(reduceFunc);
//
//					String savePath = typeOutputPathsMap.get(type);
//					reduceByKey.values().saveAsTextFile(savePath);

					JavaRDD<String> strRDD = ctx.textFile(allPath);
					MapByKeyFunc_StringKey mapByKeyFunc_mergeKey;
					try {
						mapByKeyFunc_mergeKey = new MapByKeyFunc_StringKey(type);
					} catch (IllegalAccessException | InstantiationException e) {
						e.printStackTrace();
						continue;
					}
					JavaPairRDD<String, String> itemRDD = strRDD.mapToPair(mapByKeyFunc_mergeKey);
					ReduceByKeyFunc reduceFunc = new ReduceByKeyFunc(type);
					JavaPairRDD<String, String> reducePairRDD = itemRDD.reduceByKey(reduceFunc);
					String savePath = typeOutputPathsMap.get(type);
					System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + savePath);
					if (savePath.startsWith(basePath)) {
						savePath = savePath.substring(basePath.length() + 1);
					}
					if (savePath.startsWith("/")) {
						savePath = savePath.substring(1);
					}
					final String finalSavePath = savePath;
					JavaPairRDD<String, String> outputRDD = reducePairRDD.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
						@Override
						public Tuple2<String, String> call(Tuple2<String, String> pair) throws Exception {
							return new Tuple2<>(finalSavePath, pair._2);
						}
					});
					outputRddList.add(outputRDD);
				}
			}

			if (outputRddList.size() > 0) {
				JavaPairRDD<String, String> first = outputRddList.remove(0);
				JavaPairRDD<String, String> unionRDD = ctx.union(first, outputRddList);
				unionRDD.saveAsHadoopFile(basePath, String.class, String.class, MultiTextOutputFormat.class);
			}

			if (hdfsOper != null)
			{
				hdfsOper.mkfile(_successFile);
			}
		}
	    else
        {
        	LOG.info("MERGESTATALL has been dealed successfully: " + _successFile);
        }
	}

	private JavaRDD<String> readRddByHdfs(JavaSparkContext ctx, String inpath) {
		JavaRDD<String> hdfsDataRdd;
		if(hdfsOper !=null && hdfsOper.checkDirExist(inpath))
		{
			hdfsDataRdd = ctx.textFile(inpath);
		}
		else 
		{
			System.out.println("WARN: hdfsDataRdd is null! : " + inpath);
			hdfsDataRdd = ctx.parallelize(Collections.singletonList(""), 1);
		}
		return hdfsDataRdd;
	}

	private JavaRDD<String> readRddByHive(JavaSparkContext ctx, HiveContext hiveContext,
			String sql) {
		JavaRDD<String> hiveDataRdd;
		try
		{
			if (!sql.toLowerCase().contains("null"))
			{
				hiveDataRdd = HiveHelper.SearchRddByHive(sql, hiveContext);
			}
			else
			{
				System.out.println("WARN: hiveDataRdd contains null! : " + sql);
				hiveDataRdd = ctx.parallelize(Collections.singletonList(""), 1);
			}
		}
		catch (Exception e)
		{
			System.out.println("WARN: hiveDataRdd is null! : " + sql);
			hiveDataRdd = ctx.parallelize(Collections.singletonList(""), 1);
		}
		return hiveDataRdd;
	}
}
