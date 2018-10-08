package cn.mastercom.bigdata.spark.mergestat;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;
import cn.mastercom.bigdata.mergestat.deal.MergeDataFactory;
import cn.mastercom.bigdata.mergestat.deal.MergeGroupUtil;
import cn.mastercom.bigdata.mergestat.deal.MergeInputStruct;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;
import cn.mastercom.bigdata.util.spark.HiveHelper;
import scala.Tuple2;

public class MergeMain implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static String statTime = "";	//开始时间01_180125
	private static String hiveDBName = "";		//读取的hive库
	private static String sourceTbName = "wymr_dw_RESULT_TIME_yyyyMMDD";			//读取的hive表
	private static String resultTbName = "wymr_dw_RESULT_MERGESTAT_TIME_yyyyMMDD";   //结果表名
	private static String modelTbName = "wymr_dw_TB_MODEL_RESULT_TIME_yyyyMMDD"; //模板表名
	private static  String strMonth=""; //分区月份
	private static  String strDay="";   //分区天
	private static  String strHour="";  //分区小时
	 
	private static void main(String[] args) throws Exception {

		String mroXdrMergePath = MainModel.GetInstance().getAppConfig().getMroXdrMergePath();
		statTime = args[0];	// "01_190426";
		String date = statTime.replace("01_", ""); // 171227
		
		 // 01_18012513
	    strMonth = statTime.substring(5,7);
		strDay = statTime.substring(7,9);
		strHour = statTime.substring(9,11);
		hiveDBName = MainModel.GetInstance().getSparkConfig().get_HiveDBName();
		
		// 得到输入的type和路径
		ArrayList<MergeInputStruct> inputPath = MergeGroupUtil.addInputPath(mroXdrMergePath, date);
	
		// key为tableName， value为type，用来实例化类
		HashMap<String, Integer> tableNameToType = new HashMap<String,Integer>();
		
		for(int i=0;i<inputPath.size();i++){
			int type =Integer.parseInt(inputPath.get(i).index);
			String path = inputPath.get(i).inputPath;
			String tableName = "";
			if(path.contains("tb_")){
				tableName = path.substring(path.lastIndexOf("tb_"), -1);
			}else{
				tableName = path.substring(path.lastIndexOf("TB_"), -1);
			}
			tableNameToType.put(tableName, type);

		}

		MergeMain mergeMain = new MergeMain();
		boolean ifishive = MainModel.GetInstance().getCompile().Assert(CompileMark.ChongQing);
		mergeMain.mergeData(tableNameToType,ifishive);

	}

	public void mergeData(final HashMap<String, Integer> tableNameToType, 
			boolean isHive) throws Exception
	{
		SparkConf sparkConf = makeConf();
		
		// 定义两个context，一个是读取hdfs，一个读取hive
		final JavaSparkContext ctx = new JavaSparkContext(sparkConf);
	    final HiveContext hiveContext = new HiveContext(JavaSparkContext.toSparkContext(ctx));
	    JavaRDD<String> strRDD = HiveHelper.SearchRddByHive("select * from XXX表 where 时间等于",hiveContext);
	   
	    final Broadcast<HashMap<String, Integer>> broadcast = ctx.broadcast(tableNameToType);
	    
	    //strRDD 变成pairRDD， key为tableName+##+key维度，value为value（tablename+value）
	    JavaPairRDD<String, String> pairRdd = strRDD.flatMapToPair(new PairFlatMapFunction<String, String, String>() {
			@Override
			public Iterable<Tuple2<String, String>> call(String arg0) throws Exception {
				
				//使用了广播
				HashMap<String,Integer> tableNameType = broadcast.value();
				String[] split = arg0.split("###",2);
				String tableName = split[0];
				int type =tableNameType.get(tableName);
				IMergeDataDo iMergeData = MergeDataFactory.GetInstance().getMergeDataObject(type);
				iMergeData.fillData(split[1].split("\t",-1),0);
				String key = iMergeData.getMapKey();
				
				String keyWeidu = tableName+"###"+key;
				ArrayList<Tuple2<String, String>> result = new ArrayList<Tuple2<String,String>>();
				Tuple2<String, String> tuple2 = new Tuple2<String,String>(keyWeidu,split[1]);
				result.add(tuple2);
				return result;
			}
		});
	    
	    // key为tableName+##+key维度  value为tablename+,+value
	    JavaPairRDD<String,String> nameWeiDuRdd = pairRdd.reduceByKey(new Function2<String, String, String>() {
			@Override
			public String call(String value0, String value1) throws Exception {
				String[] split0 = value0.split("###",2);
				String[] split1 = value1.split("###",2);
				String tableName = split0[0];
				int type =tableNameToType.get(tableName);
				IMergeDataDo iMergeData0 = MergeDataFactory.GetInstance().getMergeDataObject(type);
				iMergeData0.fillData(split0[1].split("\t",-1),0);
				IMergeDataDo iMergeData1 = MergeDataFactory.GetInstance().getMergeDataObject(type);
				iMergeData1.fillData(split1[1].split("\t",-1),0);
				iMergeData1.mergeData(iMergeData0);
				return tableName +"##"+ iMergeData1.getData();
			}
		});
	    //pairRDD转StringRDD TODO应该可以直接values
	    JavaRDD<String> results = nameWeiDuRdd.values();
	    
	    JavaRDD<Row> rowsRelust = results.flatMap(new FlatMapFunction<String, Row>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Row> call(String arg0) throws Exception {
				//tableName, data
				List<Row> rowList = new ArrayList<Row>();
				String[] split = arg0.split("##");
				Row row = RowFactory.create(split[0],split[1]);
				rowList.add(row);
				return rowList;
			}
		});
	    
	
		hiveContext.sql("use " + hiveDBName);
		//使用like在创建表的时候，拷贝表模式(而无需拷贝数据)
		hiveContext.sql("create table IF NOT EXISTS " + resultTbName + " like " + modelTbName);
		
		List<StructField> structFields = new ArrayList<StructField>();
		// 列名称 列的具体类型（Integer Or String） 是否为空一般为true，实际在开发环境是通过for循环，而不是手动添加
		structFields.add(DataTypes.createStructField("tableName", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("data", DataTypes.StringType, true));

		// 构建StructType,用于最后DataFrame元数据的描述
		StructType structType = DataTypes.createStructType(structFields);

		DataFrame df = hiveContext.createDataFrame(rowsRelust, structType);
		df.registerTempTable("tmpTable");
		hiveContext.sql("insert into table " + resultTbName + " partition(month_id='" + strMonth + "',day_id='" + strDay + "',hour_id='" + strHour + "') select tableName,data from tmpTable" );
		
	}
	
	private SparkConf makeConf() throws Exception {
		Configuration conf = new Configuration();
		MainModel.GetInstance().setConf(conf);
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
		{			
			conf.set("fs.defaultFS", "hdfs://192.168.1.31:9000");
		}
		
		HDFSOper hdfsOper = new HDFSOper(conf);

		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("Spark.MroXdrMerge." + "MergeStat:");


		if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
		{
			sparkConf.setMaster("local");
		}else{
//			sparkConf.set("yarn.nodemanager.resource.memory-mb", "8192");
//			sparkConf.set("yarn.scheduler.maximum-allocation-mb", "8192");
		}
		return sparkConf;
	}
}
