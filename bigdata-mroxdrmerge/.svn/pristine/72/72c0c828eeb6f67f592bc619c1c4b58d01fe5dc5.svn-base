package cn.mastercom.bigdata.spark.mroxdrmerge.ottloc;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.util.DataAdapterReader;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.util.DataAdapterConf.ParseItem;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.util.spark.MapByTypeFuncBaseV2;
import cn.mastercom.bigdata.util.spark.TypeInfo;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.LocModel;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import scala.Tuple2;

public class MapByTypeFunc_OTT extends MapByTypeFuncBaseV2 implements org.apache.spark.api.java.function.PairFlatMapFunction<String, TypeInfo, Iterable<String>>
{
	private static final long serialVersionUID = 1L;

	public static final int DataType_OTT_LOCATION = 1;
	
	private ResultOutputer resultOutputer;
	private String xmString = "";
	private String[] valstrs;
	private ParseItem parseItem;
	private DataAdapterReader dataAdapterReader;
	private Date d_beginTime;
	
	private long imsi_long;
	private long ECI;
	private long stime;
	private String host;
	private String URI;
	private String ul_content;
	private String dl_content;
	
    public MapByTypeFunc_OTT(Object[] args) throws Exception
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
			parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("OTT-LOC");
			if(parseItem == null)
			{
				throw new IOException("parse item do not get.");
			}
		    dataAdapterReader = new DataAdapterReader(parseItem);
			resultOutputer = new ResultOutputer(dataOutputMng);
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error,"LocModel GetInstance error", "LocModel GetInstance error! : " + e.getMessage(), e);
		}
		
		return 0;
	}

	private boolean bOnce = true;
	protected int init_once() throws Exception
	{
		if (!bOnce)
		{
			return 0;
		}
		bOnce = false;
		
		LocModel.GetInstance("123456789mastercom");
		
		return 0;
	}
	
	@Override
	public Iterable<Tuple2<TypeInfo, Iterable<String>>> call(String value) throws Exception
	{
		// 初始化配置
		init_once();
		
		try
		{
		    xmString = new String(value.toString().getBytes(StaticConfig.UTFCode));
			valstrs = xmString.toString().split(parseItem.getSplitMark(), -1);

			dataAdapterReader.readData(valstrs);
			
			imsi_long = dataAdapterReader.GetLongValue("IMSI", -1);
			ECI = dataAdapterReader.GetLongValue("ECI", -1);
			
			d_beginTime = dataAdapterReader.GetDateValue("Procedure_Start_Time", new Date(1970,1,1)); 		
			stime = (int) (d_beginTime.getTime() / 1000L);
			
			host = dataAdapterReader.GetStrValue("host", "");
			URI = dataAdapterReader.GetStrValue("URI", "");	
			ul_content = dataAdapterReader.GetStrValue("ul_content", "");		
			dl_content = dataAdapterReader.GetStrValue("dl_content", "");
					
			List<String> filledLocationInfoList = null;
			if(MainModel.GetInstance().getCompile().Assert(CompileMark.ChongQing))
			{
				filledLocationInfoList = LocModel.DecryptLocString("POST", host, URI, dl_content, 
						ul_content, true, false, stime+"", ECI+"", imsi_long+""); 
			}
			else 
			{
				filledLocationInfoList = LocModel.DecryptLocString("POST", host, URI, dl_content, 
						ul_content, true, true, stime+"", ECI+"", imsi_long+""); 
			}
			
			if(filledLocationInfoList != null)
			{
				for(String locStr : filledLocationInfoList)
				{
					resultOutputer.pushData(DataType_OTT_LOCATION, locStr);
				}
			}
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.info,"ott dealing error", "ott dealing error.", e);
		}
		
		List<Tuple2<TypeInfo, Iterable<String>>> resultRdd = typeResult.GetRdds();
		dataOutputMng.clear();
		return resultRdd;
	}

}
