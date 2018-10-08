package cn.mastercom.bigdata.spark.mroxdrmerge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import cn.mastercom.bigdata.util.DataAdapterReader;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.DataAdapterConf.ParseItem;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import scala.Tuple2;
import cn.mastercom.bigdata.StructData.StaticConfig;

public class MapByCellFunc_Mro implements
		org.apache.spark.api.java.function.PairFlatMapFunction<String, String, String>
{
	private static final long serialVersionUID = 1L;

	private String tmStr = "";
	private long eci = -1;
	private int CellId = -1;
	private int ENBId = -1;
	private String xmString = "";
	private String[] valstrs;
	private Date d_beginTime;
	private long ltime;

	private ParseItem parseItem;
	private DataAdapterReader dataAdapterReader;
	private int splitMax = -1;
	private List<String> columnNameList;

	public MapByCellFunc_Mro() throws Exception
	{
		parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MRO-SRC");
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
		LOGHelper.GetLogger().writeLog(LogType.error, "get splitMax : " + splitMax);
	}

	@Override
	public Iterable<Tuple2<String, String>> call(String value) throws Exception
	{
		xmString = new String(value.toString().getBytes(StaticConfig.UTFCode));
		valstrs = xmString.toString().split(parseItem.getSplitMark(), splitMax + 2);

		dataAdapterReader.readData(valstrs);

		try
		{
			d_beginTime = dataAdapterReader.GetDateValue("beginTime", new Date(1970, 1, 1));
			ltime = d_beginTime.getTime();
			ENBId = dataAdapterReader.GetIntValue("ENBId", -1);
			CellId = dataAdapterReader.GetIntValue("CellId", -1);
			eci = dataAdapterReader.GetIntValue("Eci", -1);

			if (CellId <= 255 && CellId >= 0 && eci <= 0)
			{
				eci = ENBId * 256 + CellId;
			}

			if (ltime <= 100 || eci <= 0)
			{
				return new ArrayList<Tuple2<String, String>>();
			}

			tmStr = eci + "_"
					+ (ltime / 1000L / MapByCellFunc_XdrLocation.TimeSpan * MapByCellFunc_XdrLocation.TimeSpan);
			Tuple2<String, String> tp = new Tuple2<String, String>(tmStr, dataAdapterReader.getAppendString(columnNameList));
			return Arrays.asList(tp);
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error,"MapByCellFunc_Mro.get data error", "MapByCellFunc_Mro.get data error : " + xmString, e);
			return new ArrayList<Tuple2<String, String>>();
		}
	}

}
