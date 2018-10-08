package cn.mastercom.bigdata.mapr;
//package model.hadoop;

import cn.mastercom.bigdata.mroxdrmerge.AppConfig;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.spark.MapByTypeFuncBaseV2;
import cn.mastercom.bigdata.util.spark.TypeInfo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author 邝晓林
 * @Description
 * @date 2017/11/21
 */
public class MultipleOutput4Spark_DEMO implements Serializable{

	private static final long serialVersionUID = 1L;

	public static void main(String[] args) throws Exception{
        
		new MultipleOutput4Spark_DEMO().test();
    }

    public void test() throws Exception{
    	 SparkConf sparkConf = new SparkConf();
         sparkConf.setMaster("local");
         sparkConf.setAppName("test");
         JavaSparkContext sc = new JavaSparkContext(sparkConf);

         List<Tuple2<String, String>> tmp = new ArrayList<>();
         tmp.add(new Tuple2<String, String>("KEY1", "VALUE1"));
         tmp.add(new Tuple2<String, String>("KEY2", "VALUE2"));
         tmp.add(new Tuple2<String, String>("KEY3", "VALUE3"));
         tmp.add(new Tuple2<String, String>("KEY4", "VALUE4"));
         tmp.add( new Tuple2<String, String>("KEY5", "VALUE5"));
         JavaPairRDD<String, String> rdd =  sc.parallelizePairs(tmp);
         
         String dateStr = "20171123";
         String outPath = "/tmp/out4";
         
         rdd.repartition(2).map(new TestFunc(new Object[]{dateStr, outPath})).count();

         sc.stop();	 
    }
    
	private class TestFunc extends MapByTypeFuncBaseV2 implements Function<Tuple2<String, String>,Object>{

		public TestFunc(Object[] args) throws Exception
		{
			super(args);
			//step 1: 获取hdfs路径 
			AppConfig appconfig = MainModel.GetInstance().getAppConfig();
			fsUri = "hdfs://" + appconfig.getHadoopHost() + ":" + appconfig.getHadoopHdfsPort();
			//step 2：注册类型-路径
			String filename = "tbTest";
			String dirName = "TB_TEST_" + dateStr;
			TypeInfo typeInfo = new TypeInfo(1, filename, outPath + "/" + dirName);
			typeInfoMng_HDFS.registTypeInfo(typeInfo);
			
			filename = "tbTest2";
			dirName = "TB_TEST2_" + dateStr;
			typeInfo = new TypeInfo(2, filename, outPath + "/" + dirName);
			typeInfoMng_HDFS.registTypeInfo(typeInfo);
		}

		@Override
		public Object call(Tuple2<String, String> kv) throws Exception
		{
			init_once();
			dataOutputMng.pushData(1, kv._1);
			dataOutputMng.pushData(2, kv._2);
			return null;
		}

		@Override
		protected int init_once_sub() throws Exception
		{
			// TODO Auto-generated method stub
			return 0;
		}
		
	}
}
