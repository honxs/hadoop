package cn.mastercom.bigdata.spark.mroxdrmerge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

import cn.mastercom.bigdata.util.DataAdapterReader;
import cn.mastercom.bigdata.util.DataAdapterConf.ParseItem;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import scala.Tuple2;
import cn.mastercom.bigdata.util.DataGeter;
import cn.mastercom.bigdata.StructData.*;

public class MapByS1apidFunc_MrAll implements org.apache.spark.api.java.function.PairFlatMapFunction<String, String, MroOrigDataMT>
{
	private static final long serialVersionUID = 1L;
	private ParseItem parseItem;
	private DataAdapterReader dataAdapterReader;
	
	public MapByS1apidFunc_MrAll() throws Exception
	{
		parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MRO-SRC");
		if (parseItem == null)
		{
			throw new IOException("parse item do not get.");
		}
		dataAdapterReader = new DataAdapterReader(parseItem);
	}
	

	@Override
	public Iterable<Tuple2<String, MroOrigDataMT>> call(String value) throws Exception {
		// TODO Auto-generated method stub
		ArrayList<Tuple2<String, MroOrigDataMT>> list = new ArrayList<>();
		String[] valstrs = value.split(parseItem.getSplitMark(), -1);
		dataAdapterReader.readData(valstrs);
		MroOrigDataMT item = new MroOrigDataMT();
		if (!item.FillData(dataAdapterReader))
		{
			return list;
		}
		String tmStr = dataAdapterReader.GetStrValue("CellId", "0");
		int enbid = dataAdapterReader.GetIntValue("ENBId", -1);
		long eci = 0;
		long cellid = 0L;
		if (tmStr.indexOf(":") > 0)
		{
			cellid = DataGeter.GetLong(tmStr.substring(0, tmStr.indexOf(":")));
		}
		else if (tmStr.indexOf("-") > 0)
		{
			cellid = DataGeter.GetLong(tmStr.substring(tmStr.indexOf("-") + 1));
		}
		else
		{
			cellid = DataGeter.GetLong(tmStr);
		}
		if (cellid < 256)
		{
			eci = enbid * 256 + cellid;
		}
		else
		{
			eci = cellid;
		}
		
		String s1apid = dataAdapterReader.GetStrValue("MmeUeS1apId", "");
		Date beginTime = dataAdapterReader.GetDateValue("beginTime", null);
		String eventType = dataAdapterReader.GetStrValue("EventType", "");
		list.add(new Tuple2<String, MroOrigDataMT>(eci+"_"+s1apid+"_"+beginTime+"_"+eventType, item));
		return list;
	}

}
