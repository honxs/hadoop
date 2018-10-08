package cn.mastercom.bigdata.util.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;

public class RDDResultOutputer implements Serializable
{
	private static final long serialVersionUID = 1L; 
	
	public static final String OutPutSaveList = "mastercom.spark.output.savelist";
	
	private JavaPairRDD<TypeInfo, Iterable<String>> rddResult;
	private TypeInfoMng typeInfoMng;
	
	public RDDResultOutputer(TypeInfoMng typeInfoMng, JavaPairRDD<TypeInfo, Iterable<String>> rddResult)
	{
		this.typeInfoMng = typeInfoMng;
		this.rddResult = rddResult;
	}
	
	public int saveAsHadoopFile(String basePath, JobConf conf) throws Exception
	{
		List<Integer> saveDataList = new ArrayList<Integer>();
		for(TypeInfo item : typeInfoMng.getTypeInfoMap().values())
		{
			if(item.getTypeName().length() > 0 && item.getTypePath().length() > 0)
			{
				saveDataList.add(item.getType());
			}	
		}
		return saveAsHadoopFile(basePath, conf, saveDataList);
	}
	
	public int saveAsHadoopFile(String basePath, JobConf conf, List<Integer> saveDataList) throws Exception
    {
		
		if(!basePath.contains(":"))
		{//目录存在就需要挪了
			HDFSOper hdfsOper = new HDFSOper(conf);	
			String tarPath = "";
		    hdfsOper.reNameExistsPath(basePath, tarPath);
		}
				
		String saveList = "";
		for(int i : saveDataList)
		{
			if(typeInfoMng.getTypeInfo(i) != null)
			{
				if(saveList.length() > 0)
				{
					saveList += "," + i;
				}
				else 
				{
					saveList = Integer.toString(i);
				}
			}
		}
		
	    conf.set(OutPutSaveList, saveList);
		
	    try
		{
		    rddResult.saveAsHadoopFile(basePath, NullWritable.class, String.class, RDDResultMultipleTextOutputFormat.class, conf);
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error,"rdd save error", "rdd save error: ", e);
			return -1;
		}

		return 0;
	}
	
	
	public int saveAsHiveTable(HiveContext hiveContext, String dataBase, String tbName, String modelTbName,
			boolean toDropTable, List<Integer> saveDataList) throws Exception
    {
		try
		{	
			//设定动态分区
			hiveContext.setConf("hive.exec.dynamic.partition", "true");
			hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict");
					
			//save data
			hiveContext.sql("use " + dataBase);

			if (toDropTable)
			{
				hiveContext.sql("drop table IF EXISTS " + tbName);

				hiveContext.sql("create table " + tbName + " like " + modelTbName);
			}
			else
			{
				hiveContext.sql("create table IF NOT EXISTS " + tbName + " like " + modelTbName);
			}

			List<StructField> structFields = new ArrayList<StructField>();
			// 列名称 列的具体类型（Integer Or String） 是否为空一般为true，实际在开发环境是通过for循环，而不是手动添加
			structFields.add(DataTypes.createStructField("tableName", DataTypes.StringType, true));
			structFields.add(DataTypes.createStructField("data", DataTypes.StringType, true));

			// 构建StructType,用于最后DataFrame元数据的描述
			StructType structType = DataTypes.createStructType(structFields);
			RowRDDFunc rowRDDFunc = new RowRDDFunc(saveDataList);
			JavaRDD<Row> Rows = rddResult.flatMap(rowRDDFunc);

			DataFrame df = hiveContext.createDataFrame(Rows, structType);
			df.registerTempTable("tmpTable");
			hiveContext.sql("insert into table " + tbName + " partition(tableName) select data,tableName from tmpTable" );
		}
		catch (Exception e)
		{
			return -1;
		}
		return 0;
    }
	
	//strTime:2017082813
	public int saveAsHiveTableTime(HiveContext hiveContext, String dataBase, int partition, String tbName, String modelTbName,
			boolean toDeleteOldData, String strTime, List<Integer> saveDataList) throws Exception
    {
		try
		{						
			String strMonth = strTime.substring(0, 6);
			String strDay = strTime.substring(0, 8);
			String strHour = strTime;
			int repartition = 1;

			if (partition > 15){
				repartition  = partition / 15;
			}else {
				repartition = partition;
			}
			
			//save data
			hiveContext.sql("use " + dataBase);
			//使用like在创建表的时候，拷贝表模式(而无需拷贝数据)
			hiveContext.sql("create table IF NOT EXISTS " + tbName + " like " + modelTbName);
			
			if (toDeleteOldData)
			{
				hiveContext.sql("delete from " + tbName + " where hour_id='" + strHour + "'");
			}

			List<StructField> structFields = new ArrayList<StructField>();
			// 列名称 列的具体类型（Integer Or String） 是否为空一般为true，实际在开发环境是通过for循环，而不是手动添加
			structFields.add(DataTypes.createStructField("tableName", DataTypes.StringType, true));
			structFields.add(DataTypes.createStructField("data", DataTypes.StringType, true));

			// 构建StructType,用于最后DataFrame元数据的描述
			StructType structType = DataTypes.createStructType(structFields);
			RowRDDFunc rowRDDFunc = new RowRDDFunc(saveDataList);
			JavaRDD<Row> Rows = rddResult.flatMap(rowRDDFunc).repartition(repartition);

			DataFrame df = hiveContext.createDataFrame(Rows, structType);
			df.registerTempTable("tmpTable");
			hiveContext.sql("insert into table " + tbName + " partition(month_id='" + strMonth + "',day_id='" + strDay + "',hour_id='" + strHour + "') select tableName,data from tmpTable" );
		}
		catch (Exception e)
		{
			return -1;
		}
		return 0;
    }


	
}
