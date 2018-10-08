package cn.mastercom.bigdata.mapr.evt.locall;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import cn.mastercom.bigdata.evt.locall.stat.ImsiKey;
import cn.mastercom.bigdata.evt.locall.stat.S1apidEciKey;
import cn.mastercom.bigdata.mapr.evt.locall.LocAllReducer_Imsi.StatReducer_Imsi;
import cn.mastercom.bigdata.mapr.evt.locall.LocAllReducer_S1apidEci.StatReducer_S1apidEci;
import cn.mastercom.bigdata.mapr.util.tools.MapReduceMainTools;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.hadoop.mapred.DataDealConfigurationV2;


/**
 * 
 * @author Zhaikaishun
 * <p>
 * 启动maoredece的main方法，本类既可以作为s1apid+ECI+time来关联的job的启动
 * 也可以当作imsi+time来关联的job的启动
 * <p>
 *
 */
public class LocAllEXMain_ImsiOrS1apid
{
	protected static final Log LOG = LogFactory.getLog(LocAllEXMain_ImsiOrS1apid.class);

	// 事件等数据吐出的目录，应类似于/mt_wlyh/Data/mroxdrmerge/xdr_locall/data_01_201025/imsi
	public static String outpath_table;		
	// 日志的吐出目录
	public static String outpath;

	/**
	 * CreateJob即可以创建s1apid关联的job，也可以创建imsi关联的job
	 * @param imsiOrS1apidMap 要么是imsiMap,要么是s1apidMap
	 * @param imsiOrS1apid 要么是"imsi", 要么是"s1apid"
	 * @return 返回一个s1apid关联的job，或者一个imsi关联的job
	 * @throws Exception
	 */
	public static Job CreateJob(HashMap<Integer, String> imsiOrS1apidMap, String imsiOrS1apid) throws Exception
	{

		Configuration conf = new Configuration();

		if (!LocAllEXMain.queueName.equals("NULL"))
		{
			conf.set("mapreduce.job.queuename", LocAllEXMain.queueName);
		}

		if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
		{
			conf.set("fs.defaultFS", "hdfs://192.168.1.31:9000");
		}

		Job job = Job.getInstance(conf);
		conf = job.getConfiguration();

		makeConfig(job, imsiOrS1apid);

		if ("IMSI".equals(imsiOrS1apid.toUpperCase()))
		{
			job.setJobName("XDRLocAllImsi");
			job.setJarByClass(LocAllEXMain_ImsiOrS1apid.class);
			job.setReducerClass(StatReducer_Imsi.class);
			job.setMapOutputKeyClass(ImsiKey.class);
			job.setMapOutputValueClass(Text.class);
			job.setSortComparatorClass(ShuffleUtils.ImsiSortKeyComparator.class);
			job.setPartitionerClass(ShuffleUtils.ImsiPartitioner.class);
			job.setGroupingComparatorClass(ShuffleUtils.ImsiSortKeyGroupComparator.class);
		}
		else
		{
			job.setJobName("XDRLocAllS1apid");
			job.setJarByClass(LocAllEXMain_ImsiOrS1apid.class);
			job.setReducerClass(StatReducer_S1apidEci.class);
			job.setMapOutputKeyClass(S1apidEciKey.class);
			job.setMapOutputValueClass(Text.class);
			job.setSortComparatorClass(ShuffleUtils.S1apidEciSortKeyComparator.class);
			job.setPartitionerClass(ShuffleUtils.s1apidImsiPartitioner.class);
			job.setGroupingComparatorClass(ShuffleUtils.S1apidEciSortKeyGroupComparator.class);
		}

		int reduceNum = LocAllExUtil.getReduceNum(imsiOrS1apidMap);
		System.err.println("reduceNum: "+reduceNum);
		job.setNumReduceTasks(reduceNum);

		// 把 type和 路径 的对应关系发送到mapper中去
		StringBuffer sBuffer = LocAllExUtil.getTypeToPath(imsiOrS1apidMap);
		if (sBuffer.length() > 0)
		{
			conf.set("mastercom.mroxdrmerge.locall.inpathindex", sBuffer.toString().substring(0, sBuffer.length() - 1));
		}

		LocAllExUtil.addInputPath(job, imsiOrS1apidMap, imsiOrS1apid);
		LocAllExUtil.addNamedOutput(job, outpath, "/xdr_locall", imsiOrS1apid);
		//如果数据存在则停止
		if(checkSuccess(imsiOrS1apid)){
			System.exit(0);
		}

		return job;
	}

	private static boolean checkSuccess(String imsiOrS1apid) {
		// 检测目录存在的路径
		String desPath = "";

		// 因为内蒙需要输出的路径和其他地市有去别
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng))
		{
			desPath = LocAllEXMain.mroXdrMergePath + "/xdr_locall/data_01_20"
					+ LocAllEXMain.startTime.substring(3)+"/"+imsiOrS1apid+"/output";
		}else if(MainModel.GetInstance().getCompile().Assert(CompileMark.SiChuan)){
			String[] hours = new String[] { "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11",
					"12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23" };
			desPath = LocAllEXMain.mroXdrMergePath + "/xdr_locall/data_"
					+ LocAllEXMain.startTime+hours[LocAllEXMain.step_up]+"/"+imsiOrS1apid;
		}
		else
		{
			desPath = LocAllEXMain.mroXdrMergePath + "/xdr_locall/data_"
					+ LocAllEXMain.startTime+"/"+imsiOrS1apid;
		}
		if (LocAllEXMain.hdfsOper.checkDirExist(desPath))
		{
			System.out.println("路径已经存在: " + desPath);
			return true;
		}else{
			return false;
		}
	}

	/**
	 * 设置输出的事件统计路径，设置输出的log吐出路径，初始化集群运行程序的配置
	 * @param job
	 * @param imsiOrS1apid
	 */
	private static void makeConfig(Job job, String imsiOrS1apid)
	{
		Configuration conf = job.getConfiguration();
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng))
		{
			outpath_table = LocAllEXMain.mroXdrMergePath + "/xdr_locall/data_01_20" + LocAllEXMain.startTime.substring(3)
					+ "/" + imsiOrS1apid;

		}
		else if(MainModel.GetInstance().getCompile().Assert(CompileMark.SiChuan)){
			String[] hours = new String[] { "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11",
					"12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23" };
			outpath_table = LocAllEXMain.mroXdrMergePath + "/xdr_locall/data_" + LocAllEXMain.startTime
					+"/"+hours[LocAllEXMain.step_up]+ "/"
					+ imsiOrS1apid;
		}
		else
		{
			outpath_table = LocAllEXMain.mroXdrMergePath + "/xdr_locall/data_" + LocAllEXMain.startTime + "/"
					+ imsiOrS1apid;
		}
		// table output path
		outpath = outpath_table + "/output";

		// 初始化集群运行程序的配置
		if (!MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
		{
			MapReduceMainTools.CustomMaprParas(conf);
			DataDealConfigurationV2.create(outpath_table, job);
			if(MainModel.GetInstance().getCompile().Assert(CompileMark.NingXia)){
				conf.set("mapreduce.job.reduce.slowstart.completedmaps", "1");//
			}else{
				conf.set("mapreduce.job.reduce.slowstart.completedmaps", "0.95");//
			}	
		}
		
		conf.set("mapreduce.job.date", LocAllEXMain.startTime);
	}
}
