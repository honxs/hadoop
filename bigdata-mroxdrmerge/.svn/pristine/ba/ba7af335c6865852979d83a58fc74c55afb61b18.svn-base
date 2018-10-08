package cn.mastercom.bigdata.spark.mroxdrmerge;

import java.util.ArrayList;
import java.util.Arrays;

import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import scala.Tuple2;

public class MapByImsiFunc_XdrLocation implements org.apache.spark.api.java.function.PairFlatMapFunction<String, Long, String>
{
	private static final long serialVersionUID = 1L;
	
	private long imsi;

	@Override
	public Iterable<Tuple2<Long, String>> call(String value) throws Exception
	{
		String[] strs = value.toString().split("\t", 6);

		try
		{
			imsi = Long.parseLong(strs[4]);
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error,"MapByCellFunc_XDRLOCATION get data error", "MapByCellFunc_XDRLOCATION get data error : " + value, e);
			return new ArrayList<Tuple2<Long, String>>();
		}

		Tuple2<Long, String> tp = new Tuple2<Long, String>(imsi, value);
		return Arrays.asList(tp);
	}

}
