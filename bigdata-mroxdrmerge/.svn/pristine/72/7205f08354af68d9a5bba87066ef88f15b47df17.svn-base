package cn.mastercom.bigdata.mapr.evt.locall;

import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author zhaikaishun
 *
 */
public class LocAllEXMain
{
	protected static final Log LOG = LogFactory.getLog(LocAllEXMain.class);
	// xdr_loc_lib位置库路径
	public static String xdr_locPath = "";
	// tb_loc_lib位置库路径
	public static String mr_locPath = "";
	// 常驻用户位置库路径
	public static String resident_userPath = "";
	public static String queueName;
	//开始时间: 01_180125
	public static String startTime;
	public static String mroXdrMergePath;
	public static HDFSOper hdfsOper;
	//是否有imsi数据需要跑mapreduce任务标识
	public static boolean haveImsiData=false;
	//是否有s1apid数据需要跑mapreduce任务标识
	public static boolean haveS1apidData=false;
	/* 按照小时跑是有用*/
	public static int step_up;
	public static int step_down;
	public static void main(String[] args)
	{
		try
		{
			if(MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng)){
				int stepLength = 1;
				int i =0;
				while(i<24){
					step_down = i;
					step_up = i+stepLength;
					i=i+stepLength;
					localMain(args);
				}
			}
			else{
				localMain(args);
			}

		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * 事件统计入口方法
	 * @param args args[0]: 队列名, args[1]: 日期，例如01_180125
	 * @throws Exception
	 */
	public static void localMain(String[] args) throws Exception
	{
		haveImsiData=false;
		haveS1apidData=false;
		Configuration conf = null;
		conf = new Configuration();
		MainModel.GetInstance().setConf(conf);
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
		{			
			conf.set("fs.defaultFS", "hdfs://192.168.1.31:9000");
		}
		else
		{
			conf = MainModel.GetInstance().getConf();
		}
		queueName = args[0];// network
		startTime = args[1];// 01_151013

		mroXdrMergePath = MainModel.GetInstance().getAppConfig().getMroXdrMergePath();

		xdr_locPath = String.format("%s/mro_loc/data_%s/XDR_LOC_LIB_%s", mroXdrMergePath, startTime, startTime);
		mr_locPath = String.format("%s/mro_loc/data_%s/TB_LOC_LIB_%s", mroXdrMergePath, startTime, startTime);
		resident_userPath = String.format("%s/resident_user/tb_mr_user_location", mroXdrMergePath);
		hdfsOper = new HDFSOper(conf);
		if (hdfsOper == null)
		{
			System.err.println("hdfsoper is null，check the conf");
			System.exit(1);
		}

		
		ArrayList<HashMap<Integer, String>> inputPathLists = LocAllExUtil.getInputPathMaps(conf, startTime);
		// imsi关联的数据的路径集合
		HashMap<Integer, String> imsiMap = inputPathLists.get(0);
		// s1apid关联的数据的路径集合
		HashMap<Integer, String> s1apidMap = inputPathLists.get(1);

		if(imsiMap.size()>0)
		{
			Job imsiJob = LocAllEXMain_ImsiOrS1apid.CreateJob(imsiMap, "imsi");
			if(haveImsiData){
				if (!imsiJob.waitForCompletion(true))
				{
					System.out.println("xdr locall1 Job error! stop run.");
					throw (new Exception("system.exit1"));
				}
			}

		}

		if(s1apidMap.size()>0)
		{
			Job s1apidJob = LocAllEXMain_ImsiOrS1apid.CreateJob(s1apidMap, "s1apid");
			if(haveS1apidData){
				if (!s1apidJob.waitForCompletion(true))
				{
					System.out.println("xdr locall2 Job error! stop run.");
					throw (new Exception("system.exit1"));
				}
			}

		}


	}

}
