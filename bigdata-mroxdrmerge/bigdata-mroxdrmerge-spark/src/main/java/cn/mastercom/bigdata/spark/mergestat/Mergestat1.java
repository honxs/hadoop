package cn.mastercom.bigdata.spark.mergestat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;
import cn.mastercom.bigdata.mergestat.deal.MergeDataFactory;
import cn.mastercom.bigdata.mergestat.deal.MergeGroupUtil;
import cn.mastercom.bigdata.mergestat.deal.MergeInputStruct;
import cn.mastercom.bigdata.mergestat.deal.MergeOutPutStruct;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;


public class Mergestat1 {

	public static void main(String[] args) throws Exception {
		
		String mroXdrMergePath = MainModel.GetInstance().getAppConfig().getMroXdrMergePath();
		String statTime = args[0];
		String date = "";
		String hPath_mergestat = String.format("%1$s/mergestat/data_%2$s", mroXdrMergePath, statTime);
		// 得到输入的type和路径
		ArrayList<MergeInputStruct> inputPath = MergeGroupUtil.addInputPath(mroXdrMergePath, date);
		//得到输出的type和路径，此type和输入的type一致
		ArrayList<MergeOutPutStruct> outputPath = MergeGroupUtil.addOutputPath(mroXdrMergePath, date);
		
		HashMap<Integer, ArrayList<String>> typePaths = new HashMap<Integer,ArrayList<String>>();
		HashMap<Integer, String> typeOutputPathS = new HashMap<Integer,String>();
		
		for(int i=0;i<inputPath.size();i++){
			int type =Integer.parseInt(inputPath.get(i).index);
			if(typePaths.containsKey(type)){
				typePaths.get(type).add(inputPath.get(i).inputPath);
			}else{
				ArrayList<String> pathList = new ArrayList<String>();
				pathList.add(inputPath.get(i).inputPath);
				typePaths.put(type, pathList);
			}
		}
		for(int i=0;i<outputPath.size();i++){
			typeOutputPathS.put(Integer.parseInt(outputPath.get(i).index), outputPath.get(i).outpath);
		}
		
		
		
		
		
		Mergestat1 mergestat1 = new Mergestat1();
		mergestat1.mergeData(typePaths,typeOutputPathS);

	}

	public void mergeData(HashMap<Integer, ArrayList<String>> typePaths, 
			HashMap<Integer, String> typeOutputPathS) throws Exception
	{
		Configuration conf = new Configuration();
		MainModel.GetInstance().setConf(conf);
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
		{			
			conf.set("fs.defaultFS", "hdfs://192.168.1.31:9000");
		}
		
		HDFSOper hdfsOper = new HDFSOper(conf);

		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("Spark.MroXdrMerge" + ":");
		sparkConf.set("yarn.nodemanager.resource.memory-mb", "8192");
		sparkConf.set("yarn.scheduler.maximum-allocation-mb", "8192");

		if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
		{
			sparkConf.setMaster("local");
		}
		
		// do work
		final JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	    //merge stat
		
		for(Integer type : typePaths.keySet())
		{
			ArrayList<String> pathList = typePaths.get(type);
			List<JavaPairRDD<MergeTypeItem, String>> mergeTypeRddList = new ArrayList<JavaPairRDD<MergeTypeItem, String>>();
			if(pathList==null){
				System.err.println("maybe some err");
				continue;
			}
			
			for(String path : pathList)
			{
				if(!hdfsOper.checkDirExist(path)){
					continue;
				}
				
				
				JavaRDD<String> strRDD = ctx.textFile(path);
				MapByKeyFunc_MergeKey mapByKeyFunc_MergeStat = new MapByKeyFunc_MergeKey(type);
				JavaPairRDD<MergeTypeItem, String> itemRDD = strRDD.mapToPair(mapByKeyFunc_MergeStat);
				mergeTypeRddList.add(itemRDD);
			}
			JavaPairRDD<MergeTypeItem, String> unionMergeRDD = null;
			if(mergeTypeRddList.size()>1){
				for(JavaPairRDD<MergeTypeItem, String> mergeRDD : mergeTypeRddList)
		        {
		        	if(unionMergeRDD == null)
		        	{
		        		unionMergeRDD = mergeRDD;
		        	}
		        	else 
		        	{
		        		unionMergeRDD = unionMergeRDD.union(mergeRDD);
		        	}
		        }
			}else{
				unionMergeRDD = mergeTypeRddList.get(0);
			}
			final int type1 = type;
			JavaPairRDD<MergeTypeItem,String> reduceByKey = unionMergeRDD.
	        		reduceByKey(new Function2<String, String, String>() {

				private static final long serialVersionUID = 1L;

				@Override
				public String call(String values1, String values2) throws Exception {
					// TODO Auto-generated method stub
					//这里要知道用哪个来解析，然后++
					IMergeDataDo mergeDataObject1 = MergeDataFactory.GetInstance().getMergeDataObject(type1);
					mergeDataObject1.fillData(values1.split("\t",-1), 0);
					
					IMergeDataDo mergeDataObject2 = MergeDataFactory.GetInstance().getMergeDataObject(type1);
					mergeDataObject2.fillData(values2.split("\t",-1), 0);
					mergeDataObject1.mergeData(mergeDataObject2);
					return mergeDataObject1.getData();
				}
			});
			
			//直接把reduceByKey吐出即可
			JobConf jobConf = new JobConf(conf);
			String savePath = typeOutputPathS.get(type)+"TODO";
			reduceByKey.saveAsHadoopFile(savePath, NullWritable.class,
					String.class,TextOutputFormat.class, jobConf);;
		}
	}
}
