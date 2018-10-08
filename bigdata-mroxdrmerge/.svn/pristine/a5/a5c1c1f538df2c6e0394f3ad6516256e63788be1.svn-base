package cn.mastercom.bigdata.spark.mroxdrmerge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import cn.mastercom.bigdata.util.DataAdapterReader;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.DataAdapterConf.ParseItem;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.LocModel;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import scala.Tuple2;
import cn.mastercom.bigdata.StructData.StaticConfig;

public class MapByImsiFunc_OTT implements org.apache.spark.api.java.function.PairFlatMapFunction<String, Long, String>
{
	private static final long serialVersionUID = 1L;
	
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
	
	public MapByImsiFunc_OTT() throws Exception
	{
		parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("OTT-LOC");
		if(parseItem == null)
		{
			throw new IOException("parse item do not get.");
		}
	    dataAdapterReader = new DataAdapterReader(parseItem);
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
	public Iterable<Tuple2<Long, String>> call(String value) throws Exception
	{	
		init_once();
		
		try
		{
			if(value.length() == 0)
			{
				new ArrayList<Tuple2<Long, String>>();
			}
			
			List<Tuple2<Long, String>> resList = new ArrayList<Tuple2<Long, String>>();
			
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
					Tuple2<Long, String> tp = new Tuple2<Long, String>(imsi_long, locStr);
					resList.add(tp);
				}
			}
			return resList;
			
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error,"MapByCellFunc_OTT get data error", "MapByCellFunc_OTT get data error: ", e);
			return new ArrayList<Tuple2<Long, String>>();
		}
	}

}
