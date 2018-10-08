package cn.mastercom.bigdata.spark.mroxdrmerge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import cn.mastercom.bigdata.util.DataAdapterReader;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.DataAdapterConf.ParseItem;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import scala.Tuple2;
import cn.mastercom.bigdata.StructData.StaticConfig;

public class MapByImsiFunc_MME implements org.apache.spark.api.java.function.PairFlatMapFunction<String, Long, String>
{
	private static final long serialVersionUID = 1L;
	
//	String beginTime = "";
	String xmString = "";
	String[] valstrs;
	long imsi_long;
	ParseItem parseItem;
	DataAdapterReader dataAdapterReader;
	int splitMax = -1;
	private List<String> columnNameList;
//	String imsi;

	public MapByImsiFunc_MME() throws Exception
	{
		parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("S1-MME");
		if (parseItem == null)
		{
			throw new IOException("parse item do not get.");
		}
		columnNameList = parseItem.getColumNameList();
		dataAdapterReader = new DataAdapterReader(parseItem);

		splitMax = parseItem.getSplitMax(columnNameList);
		if (splitMax < 0)
		{
			throw new IOException("time or imsi pos not right.");
		}
	}

	@Override
	public Iterable<Tuple2<Long, String>> call(String value) throws Exception
	{
		xmString = new String(value.toString().getBytes(StaticConfig.UTFCode));
		valstrs = xmString.toString().split(parseItem.getSplitMark(), splitMax + 2);
		try
		{
			dataAdapterReader.readData(valstrs);
			imsi_long = dataAdapterReader.GetLongValue("IMSI", 0);
//			beginTime = dataAdapterReader.GetStrValue("Procedure_Start_Time", "");
//			imsi = dataAdapterReader.GetStrValue("IMSI", "");
//			if (beginTime.length() == 0 || imsi.trim().length() != 15)
//			{
//				if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
//				{
//					//LOGHelper.GetLogger().writeLog(LogType.error, "get data error :" + xmString);
//				}
//				return new ArrayList<Tuple2<Long, String>>();
//			}
//			imsi_long = Long.parseLong(imsi);
			if (imsi_long <= 0 || (imsi_long+"").length()<15 || imsi_long == 6061155539545534980L)
			{
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
				{
					LOGHelper.GetLogger().writeLog(LogType.error, "get data error :" + xmString);
				}
				return new ArrayList<Tuple2<Long, String>>();
			}
			Tuple2<Long, String> tp = new Tuple2<Long, String>(imsi_long, dataAdapterReader.getAppendString(columnNameList));
			return Arrays.asList(tp);
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error,"MapByCellFunc_MME get data error", "MapByCellFunc_MME get data error : " + xmString, e);
			return new ArrayList<Tuple2<Long, String>>();
		}
	}

}
