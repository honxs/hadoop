package cn.mastercom.bigdata.util.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import cn.mastercom.bigdata.util.IDataOutputer;
import cn.mastercom.bigdata.util.LOGHelper;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * Difference between V2 and V1 is 
 *  the multipleOutput implement uses the {@link MultipleOutputs} instead of {@link TypeResult}  
 *  which solves the problem of swelling-up memory when it outputs the records. 
 */
public abstract class MapByTypeFuncBaseV2 implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	//用于记录保存到HDFS的数据信息
	protected TypeInfoMng typeInfoMng_RDD;
	protected TypeResult typeResult;
	
	//用于记录保存到RDD的数据信息
	protected TypeInfoMng typeInfoMng_HDFS;
	private MultiOutputMngSpark<NullWritable, Text> mosMng;
	
	//输出数据管理器
	private List<IDataOutputer> outputerList = new ArrayList<IDataOutputer>();
	protected DataOutputMng dataOutputMng;
	
	protected String dateStr;
	protected String outPath;
	protected String hourStr;
	protected String mroXdrMergePath;
	protected boolean hiveFlag;
	protected String fsUri;
	
	public MapByTypeFuncBaseV2(Object[] args) throws Exception
	{
		dateStr = (String) args[0];
		outPath = (String) args[1];
		hourStr = (String) args[2];
		mroXdrMergePath = (String) args[3];
		hiveFlag = (boolean) args[4];
		
		typeInfoMng_HDFS = new TypeInfoMng();
		typeInfoMng_RDD = new TypeInfoMng();

		// 初始化日志
		TypeInfo hdfsLogTypeInfo = RDDLog.getHdfsLogTypeInfo(outPath);
		typeInfoMng_HDFS.registTypeInfo(hdfsLogTypeInfo);
		TypeInfo hiveLogTypeInfo = RDDLog.getHiveLogTypeInfo(outPath);
		typeInfoMng_RDD.registTypeInfo(hiveLogTypeInfo);
	}
	
	public TypeInfoMng getTypeInfoMng()
	{
		return typeInfoMng_RDD;
	}

	protected boolean bOnce = true;
	protected int init_once() throws Exception
	{
		if (!bOnce)
		{
			return 0;
		}
		bOnce = false;

		// 初始化结果存储类
		typeResult = new TypeResult(typeInfoMng_RDD);
		outputerList.add(typeResult);
		
		if(!hiveFlag)
		{
			mosMng = new MultiOutputMngSpark<NullWritable, Text>(typeInfoMng_HDFS);
			mosMng.init(fsUri, outPath);
			outputerList.add(mosMng);
		}

		dataOutputMng = new DataOutputMng(outputerList);
		
		// 初始化日志处理
		RDDLog rddLog = new RDDLog(dataOutputMng);
		LOGHelper.GetLogger().addWriteLogCallBack(rddLog);

//		LOGHelper.GetLogger().writeLog(LogType.info, "Added rdd log! ");
			
		return init_once_sub();
	}
	
	/**
	 * 子类
	 * @return
	 * @throws Exception
	 */
	protected abstract int init_once_sub() throws Exception;
	
	private void clear(boolean cachable){
		if(cachable){			
			mosMng.clear();
		}else{
			dataOutputMng.clear();	
		}
	}
	
}
