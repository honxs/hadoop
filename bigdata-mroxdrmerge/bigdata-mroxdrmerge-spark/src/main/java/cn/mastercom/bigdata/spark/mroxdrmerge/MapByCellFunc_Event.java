package cn.mastercom.bigdata.spark.mroxdrmerge;

import java.util.ArrayList;
import java.util.Arrays;

import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import scala.Tuple2;

public class MapByCellFunc_Event implements org.apache.spark.api.java.function.PairFlatMapFunction<String, Long, String>
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String streci = "";
	private long eci;
	private String xmString = "";
	private String[] valstrs;
	
	public MapByCellFunc_Event() throws Exception
	{

	}
	

	@Override
	public Iterable<Tuple2<Long, String>> call(String value) throws Exception
	{
		valstrs = value.toString().split("\t", 16);

		if (valstrs.length < 16)
		{
			return new ArrayList<Tuple2<Long, String>>();
		}

		streci = valstrs[14];

		if (streci.length() <= 1)
		{
			return new ArrayList<Tuple2<Long, String>>();
		}

		try
		{
			eci = Long.parseLong(streci);
			Tuple2<Long, String> tp1 = new Tuple2<Long, String>(eci, value);
			
			return Arrays.asList(tp1);	
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error,"get XdrLocation error", "get XdrLocation error : " + xmString, e);
		}
		return new ArrayList<Tuple2<Long, String>>();
	}

}