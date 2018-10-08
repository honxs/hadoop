package cn.mastercom.bigdata.mapr.util.tools;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;
import cn.mastercom.bigdata.util.hadoop.mapred.MultiOutputMngV2;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.project.enums.IOutPutPathEnum;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;

public class MapReduceMainTools
{
	protected static final Log LOG = LogFactory.getLog(MapReduceMainTools.class);

	public static long getFileSize(String paths, HDFSOper hdfsOper, String Filter, String keyWord)
	{
		long inputSize = 0;
		int compression_ratio = MainModel.GetInstance().getAppConfig().getCompression_ratio();
		for (String path : paths.split(",", -1))
		{
			inputSize += hdfsOper.getSizeOfPath(path, false, keyWord, Filter, compression_ratio);
		}
		return inputSize;
	}

	public static long getFileSize(String paths, HDFSOper hdfsOper)
	{
		long inputSize = 0;
		int compression_ratio = MainModel.GetInstance().getAppConfig().getCompression_ratio();
		for (String path : paths.split(",", -1))
		{
			inputSize += hdfsOper.getSizeOfPath(path, false, compression_ratio);
		}
		return inputSize;
	}

	public static int getReduceNum(long inputSize, Configuration conf)
	{
		int reduceNum = 1;
		if (inputSize > 0)
		{
			int dealReduceSize = Integer.parseInt(MainModel.GetInstance().getAppConfig().getDealSizeReduce());
			double sizeG = inputSize * 1.0 / (1024 * 1024 * 1024);
			double sizePerReduce = dealReduceSize / 1024.0;
			reduceNum = Math.max((int) (sizeG / sizePerReduce), reduceNum);
			LOG.info("total input size of data is : " + sizeG + " G ");
			LOG.info("the count of reduce to go is : " + reduceNum);
		}
		else
		{
			reduceNum = 1;
		}
		return reduceNum;
	}
	
	/**
	 * 增加单个输入，并计算大小
	 * @param job hadoopJob
	 * @param fs 文件系统
	 * @param mapperClass mapper类
	 * @param path 
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static long addInput(Job job, FileSystem fs, Class<? extends Mapper<?, ?, ?, ?>> mapperClass, Path path) throws FileNotFoundException, IOException{
		long size = 0;
		if (fs.exists(path))
		{
			LOG.info("path exits : " + path);
			MultipleInputs.addInputPath(job, path, CombineTextInputFormat.class, mapperClass);
			FileStatus[] files = fs.listStatus(path);
			
			for (FileStatus file : files)
			{

				if (file.getPath().toString().toLowerCase().endsWith(".gz")) {
					size += file.getLen() * 10;
				} else if (file.getPath().toString().toLowerCase().endsWith(".deflate")) {
					size += file.getLen() * 5;
				} else {
					size += file.getLen();
				}

			}
		}
		else{
			LOG.info("path no exits : " + path);
		}
		return size;
	}
	
	/**
	 * 增加多个输入，并计算大小
	 * @param job hadoopJob
	 * @param mapperClass 处理这些路径的Mapper
	 * @param inputDirs 多个路径
	 * @return size 这些目录的总大小
	 * @throws Exception
	 */
	public static long addInputs(Job job, Class<? extends Mapper<?, ?, ?, ?>> mapperClass, String... inputDirs) throws Exception
	{
		Configuration conf = job.getConfiguration();
		FileSystem fs = FileSystem.get(conf);

		long size = 0;
		for(String inputsDir : inputDirs){
			size += addInput(job, fs, mapperClass, new Path(inputsDir));
		}	
		
		return size;
	}

	public static void addInputPath(Job job, String paths, HDFSOper hdfsOper, Class<? extends Mapper> mapperClass)
	{
		addInputPath(job, paths, hdfsOper, CombineTextInputFormat.class, mapperClass);
	}
	
	public static void addInputPath(Job job, String paths, HDFSOper hdfsOper, Class<? extends CombineFileInputFormat<LongWritable, Text>> combineInputClass, Class<? extends Mapper> mapperClass)
	{
		String inpaths[] = paths.split(",", -1);
		for (String inpath : inpaths)
		{
			if (inpath.contains("NULL") || inpath.length() == 0)
			{
				LOG.info("path no exits : " + inpath);
			}
			else if (inpath.contains(":") || hdfsOper.checkDirExist(inpath))
			{
				LOG.info("path exits : " + inpath);
				MultipleInputs.addInputPath(job, new Path(inpath), combineInputClass, mapperClass);
			}
			else
			{
				LOG.info("path no exits : " + inpath);
			}
		}
	}

	public static void CustomMaprParas(Configuration conf)
	{
		int mapMemory = Integer.parseInt(MainModel.GetInstance().getAppConfig().getMapMemory());
		int reduceMemory = Integer.parseInt(MainModel.GetInstance().getAppConfig().getReduceMemory());
		String mapfailRate = MainModel.GetInstance().getAppConfig().getFailPrecentMap();
		String reducefailRate = MainModel.GetInstance().getAppConfig().getFailPrecentReduce();
		String slowReduce = MainModel.GetInstance().getAppConfig().getSlowReduce();
		long minsize = Long.parseLong(MainModel.GetInstance().getAppConfig().getDealSizeMap()) * 1024 * 1024;
		int AmSizeM = MainModel.GetInstance().getAppConfig().getAmSizeG() * 1024;

		int sortMemory = reduceMemory / 5;
		if (sortMemory > 2046)
			sortMemory = 2046;
		conf.set("mapreduce.task.io.sort.mb", sortMemory + "");
		conf.set("mapreduce.map.memory.mb", mapMemory + "");
		conf.set("mapreduce.reduce.memory.mb", reduceMemory + "");
		conf.set("mapreduce.map.java.opts", "-Xmx" + (int) (mapMemory * 0.8) + "M");
		conf.set("mapreduce.reduce.java.opts", "-Xmx" + (int) (reduceMemory * 0.8) + "M");
		conf.set("mapreduce.reduce.cpu.vcores", MainModel.GetInstance().getAppConfig().getReduceVcore());
		conf.set("mapreduce.map.cpu.vcores", MainModel.GetInstance().getAppConfig().getMapVcore());
		conf.set("yarn.app.mapreduce.am.resource.mb", AmSizeM + "");
		conf.set("mapreduce.reduce.shuffle.memory.limit.percent", "0.1");
		conf.set("mapreduce.task.timeout", "3600000");// 默认600000 ， 600s
		conf.set("mapred.max.map.failures.percent", mapfailRate);// 默认0 ，允许失败10%
		conf.set("mapred.max.reduce.failures.percent", reducefailRate);// 允许失败10%
		conf.set("mapreduce.job.reduce.slowstart.completedmaps", slowReduce);
		conf.setInt("Mapred.skip.attempts.to.start.skipping", 1); // 开始“跳过”模式，读取失败超过则开启“skip
		conf.setInt("Mapred.skip.Map.max.skip.reords", 1000);
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.SiChuan))
		{
			conf.set("mapreduce.map.output.compress", "true");
		}

		if (MainModel.GetInstance().getCompile().Assert(CompileMark.SuYanPlat))
		{
			String id = MainModel.GetInstance().getAppConfig().getSuYanId();
			String key = MainModel.GetInstance().getAppConfig().getSunYanKey();
			String queue = MainModel.GetInstance().getAppConfig().getSuYanQueue();
			conf.set("hadoop.security.bdoc.access.id", id);
			conf.set("hadoop.security.bdoc.access.key", key);
			conf.set("mapreduce.job.queuename", queue);
		}

		// 将小文件进行整合
		long splitMinSize = minsize;
		conf.set("mapreduce.input.fileinputformat.split.minsize", String.valueOf(splitMinSize));

		long splitMaxSize = minsize * 2;
		conf.set("mapreduce.input.fileinputformat.split.maxsize", String.valueOf(splitMaxSize));

		long minsizePerNode = minsize / 2;
		conf.set("mapreduce.input.fileinputformat.split.minsize.per.node", String.valueOf(minsizePerNode));
		long minsizePerRack = minsize / 2;
		conf.set("mapreduce.input.fileinputformat.split.minsize.per.rack", String.valueOf(minsizePerRack));

		conf.set("mapreduce.reduce.speculative", "false");// 停止推测功能
		conf.set("mapreduce.job.jvm.numtasks", "-1");// jvm可以执行多个map
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.LZO_Compress))
		{
			// 中间过程压缩
			conf.set("io.compression.codecs",
					"org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.DeflateCodec,org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.Lz4Codec,org.apache.hadoop.io.compress.SnappyCodec,com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec");
			conf.set("mapreduce.map.output.compress", "LD_LIBRARY_PATH=/usr/local/hadoop/lzo/lib");
			conf.set("mapreduce.map.output.compress", "true");
			conf.set("mapreduce.map.output.compress.codec", "com.hadoop.compression.lzo.LzoCodec");
		}

	}

	public static void CustomMaprParas(Configuration conf, String queueName)
	{
		if (!queueName.equals("NULL"))
		{
			conf.set("mapreduce.job.queuename", queueName);
		}
		CustomMaprParas(conf);
	}

	/**
	 * 为Job绑定Enum的输出路径
	 * 
	 * @param job
	 * @param outputEnum
	 *            输出枚举
	 * @param outpath
	 *            基础路径
	 * @param date
	 *            日期 ，例：171231
	 * @throws IOException
	 */
	public static final void addOutputs(Job job, IOutPutPathEnum outputEnum, String outpath, String date) throws IOException
	{
		String path = outputEnum.getPath(outpath, date);
		renameExistOutput(job, path);
		MultiOutputMngV2.addNamedOutput(job, outputEnum.getIndex(), outputEnum.getFileName(), path, TextOutputFormat.class, NullWritable.class, Text.class);
	}

	/**
	 * 重命名输出路径
	 * 
	 * @param job
	 * @param output
	 * @throws IOException
	 */
	public static void renameExistOutput(Job job, String output) throws IOException
	{
		Configuration conf = job.getConfiguration();
		FileSystem fs = FileSystem.get(conf);

		Path outPath = new Path(output);
		if (fs.exists(outPath))
			fs.rename(outPath, new Path(output + (new SimpleDateFormat("yyMMddHHmmss").format(new Date()))));
	}

}
