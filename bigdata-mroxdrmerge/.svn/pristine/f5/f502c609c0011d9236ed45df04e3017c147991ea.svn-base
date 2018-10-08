package cn.mastercom.bigdata.util;

import org.apache.hadoop.conf.Configuration;

import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;

public class MaprConfHelper
{
	public static void CustomMaprParas(Configuration conf)
	{
		int mapMemory = Integer.parseInt(MainModel.GetInstance().getAppConfig().getMapMemory());
		int reduceMemory = Integer.parseInt(MainModel.GetInstance().getAppConfig().getReduceMemory());
		String mapfailRate = MainModel.GetInstance().getAppConfig().getFailPrecentMap();
		String reducefailRate = MainModel.GetInstance().getAppConfig().getFailPrecentReduce();
		String slowReduce = MainModel.GetInstance().getAppConfig().getSlowReduce();

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

		conf.set("mapreduce.reduce.shuffle.memory.limit.percent", "0.1");
		conf.set("mapreduce.task.timeout", "3600000");// 默认600000 ， 600s
		// conf.set("mapreduce.framework.name", "yarn");
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
	}
}
