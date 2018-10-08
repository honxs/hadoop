package cn.mastercom.bigdata.spark.mroxdrmerge;


import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import scala.Tuple2;

public class MapByCellFunc_CellBuild implements org.apache.spark.api.java.function.PairFlatMapFunction<String, String, String>
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public static final int TimeSpan = 3600;
	public String outpath_date;
	public String outpath_hour;
	public String strTime;
	public long timeSpan;
	public String eci_Time;
	public boolean IOnce = true;
	private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHH");

	public MapByCellFunc_CellBuild(Object[] obj)
	{
		outpath_date = (String) obj[0];
		outpath_hour = (String) obj[2];
	}
	
	public void init_once()
	{
		if(IOnce)
		{
			Date date = null;
			try
			{
				strTime = "20" + outpath_date + outpath_hour;
				date = dateFormat.parse(strTime);
				timeSpan =  date.getTime() / 1000L / TimeSpan * TimeSpan;
			} catch (Exception e)
			{
				e.printStackTrace();
			}
		}
		IOnce = false;
	}
	
	@Override
	public Iterable<Tuple2<String, String>> call(String str) throws Exception {
		// TODO Auto-generated method stub
		init_once();
		String[] strs = str.split("\t", -1);
		try 
		{
			String eci = strs[1];
			eci_Time = eci + "_" + timeSpan;
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeDetailLog(LogType.error,"MapByCellFunc_CellBuild get cellBuild eci_Time", "MapByCellFunc_CellBuild get cellBuild eci_Time : " +
					str,	e);
			return new ArrayList<Tuple2<String, String>>();
		}
		
		Tuple2<String, String> tp = new Tuple2<String, String>(eci_Time, str);
		return Arrays.asList(tp);
	}
}
