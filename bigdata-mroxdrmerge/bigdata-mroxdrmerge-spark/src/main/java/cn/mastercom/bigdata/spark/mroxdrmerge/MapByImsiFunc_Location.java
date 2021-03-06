package cn.mastercom.bigdata.spark.mroxdrmerge;

import java.util.ArrayList;
import java.util.Arrays;

import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import scala.Tuple2;
import cn.mastercom.bigdata.xdr.loc.LocationItem;

public class MapByImsiFunc_Location implements org.apache.spark.api.java.function.PairFlatMapFunction<String, Long, String>
{
	private static final long serialVersionUID = 1L;

	@Override
	public Iterable<Tuple2<Long, String>> call(String value) throws Exception
	{
		String[] strs = value.toString().split("\\|" + "|" + "\t", -1);

		LocationItem item = new LocationItem();
		try
		{
			if (strs[0].equals("URI"))
			{
				item.FillData(strs, 1);
			}
			else
			{
			item.FillData(strs, 0);
			}
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error,"MapByCellFunc_Location get data error",
                    "MapByCellFunc_Location get data error : " +
					value, e);
			return new ArrayList<Tuple2<Long, String>>();
		}

		Tuple2<Long, String> tp = new Tuple2<Long, String>(item.imsi, value);
		return Arrays.asList(tp);
	}

}
