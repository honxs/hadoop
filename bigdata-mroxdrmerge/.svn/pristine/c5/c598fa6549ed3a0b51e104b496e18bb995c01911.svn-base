package cn.mastercom.bigdata.spark.mroxdrmerge.wfloc;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import jodd.typeconverter.Convert;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;
import cn.mastercom.bigdata.util.spark.RDDLog;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import scala.Tuple2;

public class WflocMain {

	private static String statTime;
	private static String inpath_ydWlan;
	private static String inpath_log;
	private static String inpath_mme;
	private static String wflocOutpath;

	private static RDDLog rddLog = new RDDLog();
	private static HDFSOper hdfsOper;
	private static SimpleDateFormat timeFormat = new SimpleDateFormat("yyyyMMddHHmm");

	private static void makeConfig(Configuration conf, String[] args) {
		statTime = args[0]; // 171110
		inpath_ydWlan = args[1];
		inpath_log = args[2];
		inpath_mme = args[3];
		wflocOutpath = String.format(args[4] + "/20%s", statTime);

		String fsurl = "hdfs://" + MainModel.GetInstance().getAppConfig().getHadoopHost() + ":"
				+ MainModel.GetInstance().getAppConfig().getHadoopHdfsPort();
		conf.set("fs.defaultFS", fsurl);

		MainModel.GetInstance().setConf(conf);
	}

	private static void DoSparkJob(String[] args) throws Exception {

		// make config
		Configuration conf = new Configuration();
		makeConfig(conf, args);

		// do work
		Date stime = new Date();

		LOGHelper.GetLogger().addWriteLogCallBack(rddLog);

		hdfsOper = new HDFSOper(conf);

		CreateJobSpark(conf, args);

		if (!wflocOutpath.contains(":")) {
			Date etime = new Date();
			int mins = (int) (etime.getTime() / 1000L - stime.getTime() / 1000L) / 60;
			String timeFileName = String.format("%s/%dMins_%s_%s", wflocOutpath, mins, timeFormat.format(stime),
					timeFormat.format(etime));
			hdfsOper.mkfile(timeFileName);
		}
	}

	private static void OptionConfig(final Configuration conf, SparkConf sparkConf) {
		// set hadoop conf
		conf.set("mapreduce.output.fileoutputformat.compress", "false");
		conf.set("dfs.blocksize", "32m");
		//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

		// set spark conf
		// 设置driver大小
		if (MainModel.GetInstance().getSparkConfig().get_spark_driver_maxResultSize().trim().length() > 0) {
			sparkConf.set("spark.driver.maxResultSize",
					MainModel.GetInstance().getSparkConfig().get_spark_driver_maxResultSize());
		}

		// 设置nodemanager总大小
		if (MainModel.GetInstance().getSparkConfig().get_yarn_nodemanager_resource_memory().trim().length() > 0) {
			sparkConf.set("yarn.nodemanager.resource.memory-mb",
					MainModel.GetInstance().getSparkConfig().get_yarn_nodemanager_resource_memory());
		}

		// 设置scheduler总大小
		if (MainModel.GetInstance().getSparkConfig().get_yarn_scheduler_maximum().trim().length() > 0) {
			sparkConf.set("yarn.scheduler.maximum-allocation-mb",
					MainModel.GetInstance().getSparkConfig().get_yarn_scheduler_maximum());
		}

		// spark shuffle优化
		sparkConf.set("spark.shuffle.file.buffer", "64k");
		sparkConf.set("spark.reducer.maxSizeInFlight", "64m");
		sparkConf.set("spark.shuffle.io.maxRetries", "10");
		sparkConf.set("spark.shuffle.io.retryWait", "60s");
		sparkConf.set("spark.shuffle.manager", "hash");
		sparkConf.set("spark.shuffle.consolidateFiles", "true");
		sparkConf.set("spark.shuffle.io.numConnectionsPerPeer", "3");

		sparkConf.set("spark.sql.shuffle.partitions", "200");

		// 压缩调整,序列化
		// sparkConf.set("spark.rdd.compress", "true");
		// sparkConf.set("spark.serializer",
		// "org.apache.spark.serializer.KryoSerializer");
		// sparkConf.set("spark.kryo.registrator",
		// MyKryoRegistrator.class.getName()); //在Kryo序列化库中注册自定义的类集合

		// sparkConf.set("spark.broadcast.compress", "true");
		// sparkConf.set("spark.shuffle.compress", "true");
		// sparkConf.set("spark.shuffle.spill.compress", "true");

		// spark rdd 存储调整
		// 由于重庆测试库内存太少，要预留更多的空间给程序运行内存
		// 程序运行所需内存4G
		// excutors=8G,程序运行内存=8G*0.9*（1-spark.storage.memoryFraction-spark.shuffle.memoryFraction）
		// 程序运行内存=8G*0.9*0.6
		// sparkConf.set("spark.storage.memoryFraction", "0.2");

		sparkConf.set("spark.memory.fraction", "0.3");
		sparkConf.set("spark.memory.storageFraction", "0.5");
		sparkConf.set("spark.memory.offHeap.enabled", "false");
		sparkConf.set("spark.memory.offHeap.size", "0");
		sparkConf.set("spark.memory.useLegacyMode", "false");

		// 设置excute gc参数
		// debug
		// sparkConf.set("spark.executor.extraJavaOptions", "-XX:+UseG1GC
		// -XX:+PrintGC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps
		// -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime
		// -XX:+PrintHeapAtGC -XX:+PrintGCApplicationConcurrentTime");
		sparkConf.set("spark.executor.extraJavaOptions", "-XX:+UseG1GC ");

		// 注意parallelism的大小设置，需要考虑spark.shuffle.file.buffer的大小，parallelism*spark.shuffle.file.buffer
		// 不能超过excutor的内存
		sparkConf.set("spark.default.parallelism", "" + 300);

		sparkConf.set("spark.yarn.executor.memoryOverhead", "4096");

		// 打印日志
		sparkConf.set("yarn.log-aggregation-enable", "true");
		sparkConf.set("spark.eventLog.compress", "false");
		sparkConf.set("spark.eventLog.enabled", "true");

		// 输出运行配置信息
		double excutorMemory = Convert.toDoubleValue(sparkConf.get("spark.executor.memory").replace("g", ""));
		double memoryFraction = Convert.toDoubleValue(sparkConf.get("spark.memory.fraction"));
		double memoryStorageFraction = Convert.toDoubleValue(sparkConf.get("spark.memory.storageFraction"));
		String memoryOffHeapEnabled = sparkConf.get("spark.memory.offHeap.enabled");
		double memoryOffHeapSize = Convert.toDoubleValue(sparkConf.get("spark.memory.offHeap.size"));
		String memoryUseLegacyMode = sparkConf.get("spark.memory.useLegacyMode");

		System.out.println("========>spark.executor.memory: " + excutorMemory);
		System.out.println("========>spark.memory.fraction: " + memoryFraction);
		System.out.println("========>spark.memory.storageFraction: " + memoryStorageFraction);
		System.out.println("========>spark.memory.offHeap.enabled: " + memoryOffHeapEnabled);
		System.out.println("========>spark.memory.offHeap.size: " + memoryOffHeapSize);
		System.out.println("========>spark.memory.useLegacyMode: " + memoryUseLegacyMode);

		System.out.println("========>Excutor memory max(G): " + excutorMemory);
		System.out.println("========>Excutor RDD Storage memory max(G): "
				+ (excutorMemory - 0.5) * memoryFraction * memoryStorageFraction);
		System.out.println("========>Excutor RDD Excution memory max(G): " + (excutorMemory - 0.5) * memoryFraction);
		System.out.println("========>Excutor RDD User Memory max(G): " + (excutorMemory - 0.5) * (1 - memoryFraction));

		// if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
		// {
		// sparkConf.setMaster("local");
		// }
		////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

		// set hive config
		long minsize = 128 * 1024 * 1024L;

		// 将小文件进行整合
		// 从sparconf 里面设置 hadoop配置需要在前面加上 spark.hadoop.
		long splitMinSize = minsize;
		sparkConf.set("spark.hadoop.mapreduce.input.fileinputformat.split.minsize", String.valueOf(splitMinSize));
		long splitMaxSize = minsize * 2;
		sparkConf.set("spark.hadoop.mapreduce.input.fileinputformat.split.maxsize", String.valueOf(splitMaxSize));

		long minsizePerNode = minsize / 2;
		sparkConf.set("spark.hadoop.mapreduce.input.fileinputformat.split.minsize.per.node",
				String.valueOf(minsizePerNode));
		long minsizePerRack = minsize / 2;
		sparkConf.set("spark.hadoop.mapreduce.input.fileinputformat.split.minsize.per.rack",
				String.valueOf(minsizePerRack));

		sparkConf.set("spark.hadoop.hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat");

		// hive 0.13.1
		sparkConf.set("spark.hadoop.mapred.min.split.size", String.valueOf(splitMinSize));
		sparkConf.set("spark.hadoop.mapred.max.split.size", String.valueOf(splitMaxSize));
		sparkConf.set("spark.hadoop.mapred.min.split.size.per.node", String.valueOf(minsizePerNode));
		sparkConf.set("spark.hadoop.mapred.min.split.size.per.rack", String.valueOf(minsizePerRack));
		sparkConf.set("spark.hadoop.hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat");
		////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	}

	public static void CreateJobSpark(final Configuration conf, final String[] args) throws Exception {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("Spark-Wfloc");

		// 打印日志
		sparkConf.set("yarn.log-aggregation-enable", "true");

		if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug)) {
			sparkConf.setMaster("local");
		} else {
			//OptionConfig(conf, sparkConf);
		}

		// do work
		final JavaSparkContext ctx = new JavaSparkContext(sparkConf);

		if (inpath_ydWlan.contains(":") || hdfsOper.checkFileExist(inpath_ydWlan)) {
			JavaRDD<String> ydWlanInfo = ctx.textFile(inpath_ydWlan);
			// 将ydWlanInfo转成<mac, appName mac buildingId level longitude
			// latitude>格式的ydWlanInfoRDD
			JavaPairRDD<String, String> ydWlanInfoRDD = ydWlanInfo.distinct()
					.flatMapToPair(new PairFlatMapFunction<String, String, String>() {
						@Override
						public Iterable<Tuple2<String, String>> call(String s) throws Exception {
							String[] split = s.split("\t|\1|,|\\|");
							if (split.length < 6) {
								return new ArrayList<>();
							}
							YdWlanInfo ydWlanInfo = new YdWlanInfo(split);
							String mac = ydWlanInfo.getMac().replace("-", "").toLowerCase();

							ArrayList<Tuple2<String, String>> tuple2s = new ArrayList<>();
							Tuple2<String, String> tp = new Tuple2<>(mac, ydWlanInfo.toString());
							tuple2s.add(tp);
							return tuple2s;
						}

					});

			JavaRDD<String> logInfo = ctx.textFile(inpath_log);
			// 将logInfo转成<mac, imsi msisdn sTime eTime>格式的logInfoRDD
			JavaPairRDD<String, String> logInfoRDD = logInfo
					.flatMapToPair(new PairFlatMapFunction<String, String, String>() {
						@Override
						public Iterable<Tuple2<String, String>> call(String s) throws Exception {
							String[] split = s.split("\t|\1|,|\\|");
							if (split.length < 5) {
								return new ArrayList<>();
							}
							LogInfo logInfo = new LogInfo(split);
							String mac = logInfo.getMac().replace("-", "").toLowerCase();

							ArrayList<Tuple2<String, String>> tuple2s = new ArrayList<>();
							Tuple2<String, String> tp = new Tuple2<>(mac, logInfo.toString());
							tuple2s.add(tp);
							return tuple2s;
						}

					});

			JavaRDD<String> mme = ctx.textFile(inpath_mme);
			// 将mme转成<msisdn, imsi>格式的mmeRDD
//			JavaRDD<String> mmeRDD = mme.flatMap(new FlatMapFunction<String, String>() {
//				@Override
//				public Iterable<String> call(String s) throws Exception {
//					String[] split = s.split("\t|\1|,|\\|");
//					if (split.length < 6) {
//						return new ArrayList<>();
//					}
//					String imsi = split[2].trim();
//					String msisdn = split[4].trim();
//					// 格式化手机号，取后11位
//					if (msisdn.startsWith("86") || msisdn.startsWith("+86")) {
//						msisdn = msisdn.substring(msisdn.length() - 11, msisdn.length());
//					}
//					ArrayList<String> tuple2s = new ArrayList<>();
//					if (imsi.length() == 15 && msisdn.length() == 11) {
//						tuple2s.add(imsi + "|" + msisdn);
//					}
//
//					return tuple2s;
//				}
//
//			});
			
			//mme.distinct().saveAsTextFile(wflocOutpath);
			
			//将mmeRDD去重后组成<msisdn,imsi>的userInfoRDD
			JavaPairRDD<String, String> userInfoRDD = mme
					.flatMapToPair(new PairFlatMapFunction<String, String, String>() {
						@Override
						public Iterable<Tuple2<String, String>> call(String s) throws Exception {
							String[] split = s.split("\t|\1|,|\\|");

							ArrayList<Tuple2<String, String>> tuple2s = new ArrayList<>();
							Tuple2<String, String> tp = new Tuple2<>(split[1], split[0]);
							tuple2s.add(tp);
							return tuple2s;
						}

					});
			/*
			 * 将ydWlanRDD和logRDD cogroup后的中间结果<mac, <appName mac buildingId
			 * level longitude latitude>, <imsi msisdn sTime eTime>>
			 * 转成临时表WflocTmp<appName mac buildingId level longitude latitude
			 * imsi msisdn sTime eTime>格式的tempCogroupRDD
			 */
			JavaRDD<WflocTmp> tempCogroupRDD = ydWlanInfoRDD.cogroup(logInfoRDD).flatMap(
					new FlatMapFunction<Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>>, WflocTmp>() {
						@Override
						public Iterable<WflocTmp> call(Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>> kv)
								throws Exception {
							ArrayList<WflocTmp> result = new ArrayList<>();
							WflocTmp wflocTmp = null;
							Tuple2<Iterable<String>, Iterable<String>> tmp = kv._2;

							String str = null;
							// cogroup会加入null值，所以要去掉。
							if (tmp._1 != null && tmp._2 != null) {
								for (String t1 : tmp._2) {
									for (String t2 : tmp._1) {
										str = t2 + "\t" + t1;
										String[] split = str.split("\t");
										wflocTmp = new WflocTmp();
										wflocTmp.filldata(split);
										result.add(wflocTmp);
										break;
									}
								}
							}
							return result;
						}
					});

			// 将临时类WflocTmp转成<msisdn,appName mac buildingId
			// level longitude latitude imsi sTime eTime>格式的tempRDD
			JavaPairRDD<String, String> tempRDD = tempCogroupRDD
					.flatMapToPair(new PairFlatMapFunction<WflocTmp, String, String>() {
						@Override
						public Iterable<Tuple2<String, String>> call(WflocTmp wflocTmp) throws Exception {
							String msisdn = wflocTmp.getMsisdn();
							ArrayList<Tuple2<String, String>> tuple2s = new ArrayList<>();
							Tuple2<String, String> tp = new Tuple2<>(msisdn, wflocTmp.toString());
							tuple2s.add(tp);
							return tuple2s;
						}

					});

			/*
			 * 将userInfoRDD和tempRDD cogroup后的中间结果<msisdn, <imsi>, <appName mac
			 * buildingId level longitude latitude imsi sTime eTime>> 转成
			 * <imsi appName sTime eTime mac buildingId level longitude latitude>格式的ResultRDD吐出。
			 */
//			userInfoRDD = userInfoRDD.persist(StorageLevel.MEMORY_ONLY_SER());
//			tempRDD = tempRDD.persist(StorageLevel.MEMORY_ONLY_SER());
			
			JavaRDD<String> ResultRDD = userInfoRDD.cogroup(tempRDD)
					.flatMap(new FlatMapFunction<Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>>, String>() {
						@Override
						public Iterable<String> call(Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>> kv)
								throws Exception {
							ArrayList<String> result = new ArrayList<>();
							WflocTmp wflocTmp = null;
							Tuple2<Iterable<String>, Iterable<String>> tmp = kv._2;
							if (tmp._1 != null && tmp._2 != null) {
								for (String t2 : tmp._2) {
									String[] s = t2.split("\t");
									wflocTmp = new WflocTmp();
									wflocTmp.filldata1(s);
									for (String t1 : tmp._1) {
										if (wflocTmp.imsi.length() < 10) {
											wflocTmp.imsi = t1;
										}
										result.add(wflocTmp.toLine());
										break;
									}
								}
							}
							return result;
						}
					});
			ResultRDD.repartition(1).saveAsTextFile(wflocOutpath);
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 5)
			System.err.println(
					"WflocMain input <statTime> <inpath_ydWlan> <inpath_log> <inpath_userInfo> <wflocOutpath>");
		else
			DoSparkJob(args);
	}

}
