package cn.mastercom.bigdata.spark.mroxdrmerge;

import java.util.ArrayList;
import java.util.Arrays;

import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import scala.Tuple2;

public class MapByCellFunc_XdrLocation implements org.apache.spark.api.java.function.PairFlatMapFunction<String, String, String>
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static final int TimeSpan = 3600;
	
	private String streci = "";
	private String strtime = "";
	private long eci;
	private int itime;
	private String tmStr = "";
	private String xmString = "";
	private String[] valstrs;
	
	public MapByCellFunc_XdrLocation() throws Exception
	{

	}
	

	@Override
	public Iterable<Tuple2<String, String>> call(String value) throws Exception
	{
		valstrs = value.toString().split("\t", -1);

		if (valstrs.length < 11)
		{
			return new ArrayList<Tuple2<String, String>>();
		}

		streci = valstrs[1];
		strtime = valstrs[3];
//
//		if (streci.length() <= 1 || strtime.length() <= 1)
//		{
//			return new ArrayList<Tuple2<String, String>>();
//		}

		try
		{
			eci = Long.parseLong(streci.trim());
			itime = Integer.parseInt(strtime.trim());
//			if (eci <= 0 || itime <= 0)
//			{
//				return new ArrayList<Tuple2<String, String>>();
//			}
			
			tmStr = eci + "_" + (itime/TimeSpan*TimeSpan);
			if((itime+600)/TimeSpan*TimeSpan != (itime/TimeSpan*TimeSpan))//前1小时后10分钟数据
			{
				tmStr = eci + "_" + ((itime+600)/TimeSpan*TimeSpan);
				Tuple2<String, String> tp2 = new Tuple2<String, String>(tmStr, value);
				
				return Arrays.asList(tp2);
			}
			else
			{
				Tuple2<String, String> tp1 = new Tuple2<String, String>(tmStr, value);
				return Arrays.asList(tp1);
			}
			
			
//			Tuple2<String, String> tp1 = new Tuple2<String, String>(tmStr, value);
//			
//			if((itime+600)/TimeSpan*TimeSpan != (itime/TimeSpan*TimeSpan))//前1小时后10分钟数据
//			{
//				tmStr = eci + "_" + ((itime+600)/TimeSpan*TimeSpan);
//				Tuple2<String, String> tp2 = new Tuple2<String, String>(tmStr, value);
//				
//				return Arrays.asList(tp1, tp2);
//			}
//			else 
//			{
//				return Arrays.asList(tp1);
//			}
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error,"MapByCellFunc_XdrLocation get data error",
					"MapByCellFunc_XdrLocation get XdrLocation" +
					" error : "	+ xmString, e);
		}
		return new ArrayList<Tuple2<String, String>>();
	}

}