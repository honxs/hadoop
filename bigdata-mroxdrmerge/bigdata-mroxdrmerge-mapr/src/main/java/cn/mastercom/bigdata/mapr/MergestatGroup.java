package cn.mastercom.bigdata.mapr;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import cn.mastercom.bigdata.mapr.mergestat.ForMergeStatMain;
import cn.mastercom.bigdata.mergestat.deal.MergeGroupUtil;
import cn.mastercom.bigdata.mergestat.deal.MergeInputStruct;
import cn.mastercom.bigdata.mergestat.deal.MergeOutPutStruct;
import cn.mastercom.bigdata.mergestat.deal.MergeStatTablesEnum;
import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;
import cn.mastercom.bigdata.util.hadoop.mapred.DataDealJob;

public class MergestatGroup
{

	public static String srcPath = "";
	public static String _successFile = "";

	public static void doMergestatGroup(String queueName, String statTime, String mroXdrMergePath, Configuration conf, HDFSOper hdfsOper)
	{

		srcPath = MergeStatTablesEnum.getBasePath(mroXdrMergePath, statTime.substring(3));;
		_successFile = srcPath + "/output1/_SUCCESS";

		ArrayList<MergeInputStruct> inputPath = MergeGroupUtil.addInputPath(mroXdrMergePath, statTime.substring(3));
		// ........................ output................................
		ArrayList<MergeOutPutStruct> outputList = MergeGroupUtil.addOutputPath(mroXdrMergePath, statTime.substring(3));

		long fileCount = 0L;
		int j = 1;
		ArrayList<MergeInputStruct> tempInputPath = new ArrayList<MergeInputStruct>();
		ArrayList<MergeOutPutStruct> tempOutputPath = new ArrayList<MergeOutPutStruct>();
		ArrayList<String> paramsList = new ArrayList<String>();
		for (int i = (inputPath.size() - 1); i >= 0; i--)
		{
			long tempFileCount = hdfsOper.getFileCount(inputPath.get(i).inputPath);
			if (tempFileCount <= 0)// 去掉不存在的目录
			{
				inputPath.remove(i);
				outputList.remove(i);
			}
			else
			{
				fileCount += tempFileCount;
				tempInputPath.add(inputPath.get(i));
				tempOutputPath.add(outputList.get(i));
				if(fileCount >= 50000)
				{
					prepareMerge(paramsList, tempInputPath, tempOutputPath, hdfsOper, conf, j++, queueName, statTime);
					fileCount = 0L;
				}
			}
			if(i == 0)
			{
				if(fileCount < 50000 && fileCount > 0)
				{
					prepareMerge(paramsList, tempInputPath, tempOutputPath, hdfsOper, conf, j++, queueName, statTime);
					fileCount = 0L;
				}
				hdfsOper.mkfile(String.format("%1$s/mergestat/data_%2$s/Finished", mroXdrMergePath, statTime));
				hdfsOper.mkdir(String.format("%1$s/mergestat/data_%2$s/finishFlag", mroXdrMergePath, statTime));
				hdfsOper.mkfile(String.format("%1$s/mergestat/data_%2$s/finishFlag/Finished", mroXdrMergePath, statTime));
			}
		}
	}

	

	

	public static void prepareMerge(ArrayList<String> paramsList, ArrayList<MergeInputStruct> tempInputPath, ArrayList<MergeOutPutStruct> tempOutputPath, HDFSOper hdfsOper, Configuration conf, int j, String queueName, String statTime)
	{
		paramsList.add("100");
		paramsList.add(queueName);
		paramsList.add(statTime);
		paramsList.add(srcPath);
		paramsList.add(tempInputPath.size() + "");
		fillInPutArray(tempInputPath, paramsList); //TODO 感觉没有必要，或者tostringjike
		paramsList.add(tempOutputPath.size() + "");
		fillOutPutArray(tempOutputPath, paramsList);
		String[] params = listToArray(paramsList);
		mergeDo(hdfsOper, conf, params, j);
		tempInputPath.clear();
		tempOutputPath.clear();
		paramsList.clear();
	}
	
	public static void mergeDo(HDFSOper hdfsOper, Configuration conf, String[] params, int roundId)
	{
		try
		{
			System.out.println("the [" + roundId + "] times merge");
			Job curJob;
			curJob = ForMergeStatMain.CreateJob(conf, params, roundId);
			DataDealJob dataJob = new DataDealJob(curJob, hdfsOper);
			if (!dataJob.Work())
			{
				System.out.println("mergestat job error! stop run.");
				throw (new Exception("system.exit1"));
			}
			else
			{
				System.out.println("the[" + roundId + "] times has been dealed succesfully once");
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public static void fillInPutArray(ArrayList<MergeInputStruct> srcArray, ArrayList<String> paramsList)
	{
		for (int i = 0; i < srcArray.size(); i++)
		{
			paramsList.add(srcArray.get(i).index);
			paramsList.add(srcArray.get(i).inputPath);
		}
	}

	public static void fillOutPutArray(ArrayList<MergeOutPutStruct> srcArray, ArrayList<String> paramsList)
	{
		for (int i = 0; i < srcArray.size(); i++)
		{
			paramsList.add(srcArray.get(i).index);
			paramsList.add(srcArray.get(i).fileName);
			paramsList.add(srcArray.get(i).outpath);
		}
	}

	public static String[] listToArray(ArrayList<String> list)
	{
		String[] params = new String[list.size()];
		for (int i = 0; i < list.size(); i++)
		{
			params[i] = list.get(i);
		}
		return params;
	}
}
