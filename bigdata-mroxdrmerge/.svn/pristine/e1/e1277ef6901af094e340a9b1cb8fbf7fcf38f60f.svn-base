package cn.mastercom.bigdata.spark.mroxdrmerge.ottloc;

import java.util.List;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.util.Func;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.util.spark.MapByTypeFuncBaseV2;
import cn.mastercom.bigdata.util.spark.TypeInfo;
import cn.mastercom.bigdata.xdr.loc.LocationItem;
import scala.Tuple2;

public class MapByTypeFunc_OTT2 extends MapByTypeFuncBaseV2 implements org.apache.spark.api.java.function.PairFlatMapFunction<Tuple2<Long,String>, TypeInfo, Iterable<String>>
{
	private static final long serialVersionUID = 1L;

	public static final int DataType_OTT_LOCATION = 1;
	
	private ResultOutputer resultOutputer;
	
    public MapByTypeFunc_OTT2(Object[] args) throws Exception
    {
    	super(args);	  
	    registerTableInfo();
    }
    
    public void registerTableInfo()
    {
    	String filename = "TB_OTT_01_" + dateStr;
		TypeInfo typeInfo = new TypeInfo(DataType_OTT_LOCATION, "ott", outPath + "/" + filename);
		registTypeInfo(typeInfo);
    }

    private void registTypeInfo(TypeInfo typeInfo)
	{
		if (!MainModel.GetInstance().getCompile().Assert(CompileMark.SaveToHive))
		{
			typeInfoMng_HDFS.registTypeInfo(typeInfo);
		}
		else
		{
			typeInfoMng_RDD.registTypeInfo(typeInfo);
		}
	}

	@Override
	protected int init_once_sub() throws Exception
	{
		try
		{
			resultOutputer = new ResultOutputer(dataOutputMng);
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error,"MapByTypeFunc_OTT2 error", "MapByTypeFunc_OTT2 error!:", e);
		}
		
		return 0;
	}
	
	@Override
	public Iterable<Tuple2<TypeInfo, Iterable<String>>> call(Tuple2<Long, String> t) throws Exception
	{
		//初始化日志
		init_once();
		try
		{
			String[] strs = t._2().split("\\|" + "|" + "\t", -1);
			LocationItem item = new LocationItem();
			if (strs[0].equals("URI") || strs[0].equals(""))
			{
				item.FillData(strs, 1);
			}
			else
			{
				item.FillData(strs, 0);
			}
			item.imsi = Func.getEncrypt(item.imsi);
			resultOutputer.pushData(DataType_OTT_LOCATION, item.toString());
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.info,"ottloc dealing error23", "ottloc dealing error:", e);
		}
		
		List<Tuple2<TypeInfo, Iterable<String>>> resultRdd = typeResult.GetRdds();
		dataOutputMng.clear();
		return resultRdd;
	}

}
