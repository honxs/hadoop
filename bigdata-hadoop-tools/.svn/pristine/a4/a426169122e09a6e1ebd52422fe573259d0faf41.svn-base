package cn.mastercom.bigdata.util.spark;

import java.io.Serializable;

import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;

public abstract class MapByTypeFuncBase implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	protected TypeInfoMng typeInfoMng;
	protected TypeResult typeResult;
	protected String dateStr;
	protected String outPath;
	
	public MapByTypeFuncBase(Object[] args) throws Exception
	{
		dateStr = (String) args[0];
		outPath = (String) args[1];

		typeInfoMng = new TypeInfoMng();

		// 初始化日志
		TypeInfo logTypeInfo = RDDLog.getHdfsLogTypeInfo(outPath);
		typeInfoMng.registTypeInfo(logTypeInfo);
	}
	
	public TypeInfoMng getTypeInfoMng()
	{
		return typeInfoMng;
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
		typeResult = new TypeResult(typeInfoMng);

		// 初始化日志处理
		RDDLog rddLog = new RDDLog(typeResult);
		LOGHelper.GetLogger().addWriteLogCallBack(rddLog);

		LOGHelper.GetLogger().writeLog(LogType.info, "Added rdd log! ");
		
		return init_once_sub();
	}
	
	protected abstract int init_once_sub() throws Exception;
	
}
