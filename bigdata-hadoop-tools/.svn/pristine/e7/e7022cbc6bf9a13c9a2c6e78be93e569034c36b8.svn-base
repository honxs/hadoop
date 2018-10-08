package cn.mastercom.bigdata.util.spark;

import java.io.Serializable;

import cn.mastercom.bigdata.util.IDataOutputer;
import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;

import org.apache.log4j.Logger;

public class RDDLog implements IWriteLogCallBack, Serializable
{
	private static final long serialVersionUID = 1L;
	public static Logger logger = Logger.getLogger(cn.mastercom.bigdata.util.spark.RDDLog.class);
	
	public static final int DataType_HDFS_LOG = 900;
	public static final int DataType_HIVE_LOG = 901;
	
    private IDataOutputer typeResult = null;
	
	public RDDLog()
	{
		
	}
    
	public RDDLog(IDataOutputer typeResult)
	{
       this.typeResult = typeResult;
	}

	@Override
	public void writeLog(LogType type, String strlog)
	{
		try
		{
			if(typeResult != null)
			{
				typeResult.pushData(DataType_HDFS_LOG, LOGHelper.getFormatLog(type, strlog));
				typeResult.pushData(DataType_HIVE_LOG, LOGHelper.getFormatLog(type, strlog));
			}
			else 
			{
				//System.err.println(LOGHelper.getFormatLog(type, strlog));
			}
		}
		catch (Exception e)
		{
			// TODO: handle exception
		}
	}
	
	public static TypeInfo getHdfsLogTypeInfo(String path)
	{
		String filename = "MYLOG";
		TypeInfo typeInfo = new TypeInfo(DataType_HDFS_LOG, filename, path + "/" + filename);
		return typeInfo;
	}

	public static TypeInfo getHiveLogTypeInfo(String path)
	{
		String filename = "MYLOG";
		TypeInfo typeInfo = new TypeInfo(DataType_HIVE_LOG, filename, path + "/" + filename);
		return typeInfo;
	}

}
