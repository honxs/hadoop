package cn.mastercom.bigdata.mapr.mro.loc;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.util.DataAdapterReader;
import cn.mastercom.bigdata.util.DataAdapterConf.ParseItem;
import cn.mastercom.bigdata.mro.loc.CellTimeKey;
import cn.mastercom.bigdata.mro.loc.MroXdrDeal;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.FilterByEci;

public class MroFormatMapperSiChuan
{
	public static class MroMapper_ERICSSON extends Mapper<Object, Text, CellTimeKey, Text>
	{
		private long eci = 0;
		private int time = 0;
		private final int TimeSpan = 600;// 10分钟间隔
		private final int subTimeSpan = 5;// 5秒钟间隔
		private Date TimeStamp;// 2015-11-01 00:02:43.000
		private String tmCellID;

		private CellTimeKey mrokey;
		private Text Mt_value = new Text();

		private ParseItem parseItem_fdd;
		private List<String> columnNameList_fdd;
		private DataAdapterReader dataAdapterReader_fdd;
		private int splitMax_fdd = -1;

		private ParseItem parseItem_td;
		private List<String> columnNameList_td;
		private DataAdapterReader dataAdapterReader_td;
		private int splitMax_td = -1;
		
		private ParseItem parseItem;
		private List<String> columnNameList;
//		private DataAdapterReader dataAdapterReader;
//		private int splitMax = -1;
		
		public StringBuffer sb = new StringBuffer();
		public static String spliter = "\t";

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			Configuration conf = context.getConfiguration();

			parseItem_fdd = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MRO-SRC-ERICSSON-FDD");
			if (parseItem_fdd == null)
			{
				throw new IOException("parse item do not get.");
			}
			columnNameList_fdd = parseItem_fdd.getColumNameList();
			dataAdapterReader_fdd = new DataAdapterReader(parseItem_fdd);

			splitMax_fdd = parseItem_fdd.getSplitMax(columnNameList_fdd);
			if (splitMax_fdd < 0)
			{
				throw new IOException("time or imsi pos not right.");
			}

			parseItem_td = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MRO-SRC-ERICSSON-TD");
			if (parseItem_td == null)
			{
				throw new IOException("parse item do not get.");
			}
			columnNameList_td = parseItem_td.getColumNameList();
			dataAdapterReader_td = new DataAdapterReader(parseItem_td);

			splitMax_td = parseItem_td.getSplitMax(columnNameList_td);
			if (splitMax_td < 0)
			{
				throw new IOException("time or imsi pos not right.");
			}
			//按照统一格式拼接
			parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MRO-SRC");
			if (parseItem == null)
			{
				throw new IOException("parse item do not get.");
			}
			columnNameList = parseItem.getColumNameList();
			
			String date = conf.get("mapreduce.job.date");
			FilterByEci.readEciList(conf, date);
			
		}

		/**
		 * Called once at the end of the task.
		 */
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			if (value.toString().length() == 0)
			{
				return;
			}

			if (value.toString().length() >= 100000)
			{
				return;
			}

			String xmString = new String(value.toString().getBytes(StaticConfig.UTFCode));
			String[] valstrs = xmString.toString().split(",|\t", -1);

			try
			{
				if (valstrs.length > 50)
				{
					dataAdapterReader_fdd.readData(valstrs);
					if (!dataAdapterReader_fdd.GetStrValue("measurementtype", "").equals("M1"))
					{
						return;
					}
					String timeTemp = dataAdapterReader_fdd.GetStrValue("beginTime", null);
					TimeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(timeTemp.replace("T", " "));
					time = (int) (TimeStamp.getTime() / 1000);
					tmCellID = dataAdapterReader_fdd.GetStrValue("CellId", "0");
					eci = Util.getEci(tmCellID);
					if (!FilterByEci.ifMap(eci))
					{
						return;
					}
//					String MT_mro = formatERICSSON_FDD(dataAdapterReader_fdd);
//					String MT_mro = dataAdapterReader_fdd.getAppendString(columnNameList_fdd);
					String MT_mro = formatERICSSON(columnNameList, dataAdapterReader_fdd);
					if (MT_mro.length() == 0)
					{
						return;
					}
					Mt_value.set(MT_mro);
				}
				else
				{
					dataAdapterReader_td.readData(valstrs);
					if (!dataAdapterReader_td.GetStrValue("measurementtype", "").equals("M1"))
					{
						return;
					}
					String timeTemp = dataAdapterReader_td.GetStrValue("beginTime", null);
					TimeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(timeTemp.replace("T", " "));
					time = (int) (TimeStamp.getTime() / 1000);
					tmCellID = dataAdapterReader_td.GetStrValue("CellId", "0");
					eci = Util.getEci(tmCellID);
//					String MT_mro = formatERICSSON_TD(dataAdapterReader_td);
//					String MT_mro = dataAdapterReader_td.getAppendString(columnNameList_td);
					String MT_mro = formatERICSSON(columnNameList, dataAdapterReader_td);
					if (MT_mro.length() == 0)
					{
						return;
					}
					Mt_value.set(MT_mro);
				}
				mrokey = new CellTimeKey(eci, time / TimeSpan * TimeSpan, MroXdrDeal.DataType_MRO, time / subTimeSpan * subTimeSpan);
				context.write(mrokey, Mt_value);
			}
			catch (Exception e)
			{
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
				{
					return;
				}
			}
		}
		
		public String formatERICSSON(List<String> columnNameList, DataAdapterReader dataAdapterReader)
		{
			sb.delete(0, sb.length());
			try {
				for (String columnName : columnNameList){
					if (columnName.indexOf("ENBId") >= 0) {
						 sb.append(eci/256);
					}else {
						sb.append(dataAdapterReader.GetStrValue(columnName, ""));
					}
					sb.append(spliter);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return "";
			}
			return sb.toString().substring(0, sb.length()-1);
		}
	}

	public static class MroMapper_HUAWEI_TD extends Mapper<Object, Text, CellTimeKey, Text>
	{
		private long eci = 0;
		private int time = 0;
		private final int TimeSpan = 600;// 10分钟间隔
		private final int subTimeSpan = 5;// 5秒钟间隔
		private CellTimeKey mrokey;
		private Date TimeStamp;// 2015-11-01 00:02:43.000
		private String tmCellID;

		private Text Mt_value = new Text();

		private ParseItem parseItem_hw;
		private DataAdapterReader dataAdapterReader_hw;
		private int splitMax_hw = -1;
		private List<String> columnNameList_hw;
		
		private ParseItem parseItem;
		private List<String> columnNameList;
		
		public StringBuffer sb = new StringBuffer();
		public static String spliter = "\t";

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			Configuration conf = context.getConfiguration();
			
			parseItem_hw = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MRO-SRC-HUAWEI-TD");
			if (parseItem_hw == null)
			{
				throw new IOException("parse item do not get.");
			}
			columnNameList_hw = parseItem_hw.getColumNameList();
			dataAdapterReader_hw = new DataAdapterReader(parseItem_hw);

			splitMax_hw = parseItem_hw.getSplitMax(columnNameList_hw);
			if (splitMax_hw < 0)
			{
				throw new IOException("time or imsi pos not right.");
			}
			
			parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MRO-SRC");
			if (parseItem == null)
			{
				throw new IOException("parse item do not get.");
			}
			columnNameList = parseItem.getColumNameList();
			String date = conf.get("mapreduce.job.date");
			FilterByEci.readEciList(conf, date);
		}

		/**
		 * Called once at the end of the task.
		 */
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			if (value.toString().length() == 0)
			{
				return;
			}

			if (value.toString().length() >= 100000)
			{
				return;
			}

			String xmString = new String(value.toString().getBytes(StaticConfig.UTFCode));
			String[] valstrs = xmString.toString().split(parseItem_hw.getSplitMark(), -1);

			try
			{
				dataAdapterReader_hw.readData(valstrs);
				if (!dataAdapterReader_hw.GetStrValue("measurementtype", "").equals("M1"))
				{
					return;
				}
				String timeTemp = dataAdapterReader_hw.GetStrValue("beginTime", null);
				TimeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(timeTemp.replace("T", " "));
				time = (int) (TimeStamp.getTime() / 1000);
				tmCellID = dataAdapterReader_hw.GetStrValue("CellId", "0");
				eci = Util.getEci(tmCellID);
				if (!FilterByEci.ifMap(eci))
				{
					return;
				}
//				String MT_mro = formatHUAWEI_TD(dataAdapterReader);
//				String MT_mro = dataAdapterReader_hw.getAppendString(columnNameList_hw);
				String MT_mro = formatHUAWEI(columnNameList, dataAdapterReader_hw);
				if (MT_mro.length() == 0)
				{
					return;
				}
				mrokey = new CellTimeKey(eci, time / TimeSpan * TimeSpan, MroXdrDeal.DataType_MRO, time / subTimeSpan * subTimeSpan);
				Mt_value.set(MT_mro);
				context.write(mrokey, Mt_value);
			}
			catch (Exception e)
			{
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
				{
					return;
				}
			}
		}

		public String formatHUAWEI(List<String> columnNameList, DataAdapterReader dataAdapterReader)
		{
			sb.delete(0, sb.length());
			try {
				for (String columnName : columnNameList) {
					if (columnName.indexOf("ENBId") >= 0) {
						 sb.append(eci/256);
					}else {
						sb.append(dataAdapterReader.GetStrValue(columnName, ""));
					}
					sb.append(spliter);
				}
			} catch (Exception e) {
				return "";
				// TODO: handle exception
			}
			return sb.toString().substring(0, sb.length()-1);
		}
	}

	public static class MroMapper_NSN_TD extends Mapper<Object, Text, CellTimeKey, Text>
	{
		private long eci = 0;
		private int time = 0;
		private final int TimeSpan = 600;// 10分钟间隔
		private final int subTimeSpan = 5;// 5秒钟间隔
		private CellTimeKey mrokey;
		private Date TimeStamp;// 2015-11-01 00:02:43.000
		private String tmCellID;
		private Text Mt_value = new Text();

		private ParseItem parseItem_nsn;
		private List<String> columnNameList_nsn;
		private DataAdapterReader dataAdapterReader_nsn;
		private int splitMax_nsn = -1;
		
		private ParseItem parseItem;
		private List<String> columnNameList;
		
		public StringBuffer sb = new StringBuffer();
		public static String spliter = "\t";
		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			Configuration conf = context.getConfiguration();
			parseItem_nsn = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MRO-SRC-NSN-TD");
			if (parseItem_nsn == null)
			{
				throw new IOException("parse item do not get.");
			}
			columnNameList_nsn = parseItem_nsn.getColumNameList();
			dataAdapterReader_nsn = new DataAdapterReader(parseItem_nsn);

			splitMax_nsn = parseItem_nsn.getSplitMax(columnNameList_nsn);
			if (splitMax_nsn < 0)
			{
				throw new IOException("time or imsi pos not right.");
			}
			
			parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MRO-SRC");
			if (parseItem == null)
			{
				throw new IOException("parse item do not get.");
			}
			columnNameList = parseItem.getColumNameList();
			String date = conf.get("mapreduce.job.date");
			FilterByEci.readEciList(conf, date);
		}

		/**
		 * Called once at the end of the task.
		 */
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			if (value.toString().length() == 0)
			{
				return;
			}

			if (value.toString().length() >= 100000)
			{
				return;
			}

			String xmString = new String(value.toString().getBytes(StaticConfig.UTFCode));
			String[] valstrs = xmString.toString().split(parseItem_nsn.getSplitMark(), -1);

			try
			{
				dataAdapterReader_nsn.readData(valstrs);
				if (!dataAdapterReader_nsn.GetStrValue("measurementtype", "").equals("M1"))
				{
					return;
				}
				String timeTemp = dataAdapterReader_nsn.GetStrValue("beginTime", null);
				TimeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(timeTemp.replace("T", " "));
				time = (int) (TimeStamp.getTime() / 1000);
				tmCellID = dataAdapterReader_nsn.GetStrValue("CellId", "0");
				eci = Util.getEci(tmCellID);
				if (!FilterByEci.ifMap(eci))
				{
					return;
				}
//				String MT_mro = formatNSN_TD(dataAdapterReader);
//				String MT_mro = dataAdapterReader_nsn.getAppendString(columnNameList_nsn);
				String MT_mro = formatNSN(columnNameList, dataAdapterReader_nsn);
				if (MT_mro.length() == 0)
				{
					return;
				}
				Mt_value.set(MT_mro);
				mrokey = new CellTimeKey(eci, time / TimeSpan * TimeSpan, MroXdrDeal.DataType_MRO, time / subTimeSpan * subTimeSpan);
				context.write(mrokey, Mt_value);
			}
			catch (Exception e)
			{
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
				{
					return;
				}
			}
		}
		
		public String formatNSN(List<String> columnNameList, DataAdapterReader dataAdapterReader){
			sb.delete(0, sb.length());
			try {
				for (String columnName : columnNameList) {
					if (columnName.indexOf("ENBId") >= 0) {
						 sb.append(eci/256);
					}else {
						sb.append(dataAdapterReader.GetStrValue(columnName, ""));
					}
					sb.append(spliter);
				}
			} catch (Exception e) {
				return "";
				// TODO: handle exception
			}
			return sb.toString().substring(0, sb.length()-1);
		}
	}

	public static class MroMapper_ZTE_TD extends Mapper<Object, Text, CellTimeKey, Text>
	{
		private long eci = 0;
		private int time = 0;
		private final int TimeSpan = 600;// 10分钟间隔
		private final int subTimeSpan = 5;// 5秒钟间隔
		private CellTimeKey mrokey;
		private Date TimeStamp;// 2015-11-01 00:02:43.000
		private String tmCellID;

		private Text Mt_value = new Text();

		private ParseItem parseItem_zte;
		private List<String> columnNameList_zte;
		private DataAdapterReader dataAdapterReader_zte;
		private int splitMax_zte = -1;
		
		private ParseItem parseItem;
		private List<String> columnNameList;
		
		public StringBuffer sb = new StringBuffer();
		public static String spliter = "\t";

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			Configuration conf = context.getConfiguration();
			parseItem_zte = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MRO-SRC-ZTE-TD");
			if (parseItem_zte == null)
			{
				throw new IOException("parse item do not get.");
			}
			columnNameList_zte = parseItem_zte.getColumNameList();
			dataAdapterReader_zte = new DataAdapterReader(parseItem_zte);

			splitMax_zte = parseItem_zte.getSplitMax(columnNameList_zte);
			if (splitMax_zte < 0)
			{
				throw new IOException("time or imsi pos not right.");
			}
			
			parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MRO-SRC");
			if (parseItem == null)
			{
				throw new IOException("parse item do not get.");
			}
			columnNameList = parseItem.getColumNameList();
			String date = conf.get("mapreduce.job.date");
			FilterByEci.readEciList(conf, date);
		}

		/**
		 * Called once at the end of the task.
		 */
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			if (value.toString().length() == 0)
			{
				return;
			}

			if (value.toString().length() >= 100000)
			{
				return;
			}

			String xmString = new String(value.toString().getBytes(StaticConfig.UTFCode));
			String[] valstrs = xmString.toString().split(parseItem_zte.getSplitMark(), -1);

			try
			{
				dataAdapterReader_zte.readData(valstrs);
				if (!dataAdapterReader_zte.GetStrValue("measurementtype", "").equals("M1"))
				{
					return;
				}
				String timeTemp = dataAdapterReader_zte.GetStrValue("beginTime", null);
				TimeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(timeTemp.replace("T", " "));
				time = (int) (TimeStamp.getTime() / 1000);

				tmCellID = dataAdapterReader_zte.GetStrValue("CellId", "0");
				eci = Util.getEci(tmCellID);
				if (!FilterByEci.ifMap(eci))
				{
					return;
				}
//				String MT_mro = formatZTE_TD(dataAdapterReader);
//				String MT_mro = dataAdapterReader_zte.getAppendString(columnNameList_zte);
				String MT_mro = formatZTE(columnNameList, dataAdapterReader_zte);
				if (MT_mro.length() == 0)
				{
					return;
				}
				Mt_value.set(MT_mro);
				mrokey = new CellTimeKey(eci, time / TimeSpan * TimeSpan, MroXdrDeal.DataType_MRO, time / subTimeSpan * subTimeSpan);
				context.write(mrokey, Mt_value);
			}
			catch (Exception e)
			{
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
				{
					return;
				}
			}
		}
		
		public String formatZTE(List<String> columnNameList, DataAdapterReader dataAdapterReader){
			sb.delete(0, sb.length());
			try {
				for (String columnName : columnNameList) {
					if (columnName.indexOf("ENBId") >= 0) {
						 sb.append(eci/256);
					}else {
						sb.append(dataAdapterReader.GetStrValue(columnName, ""));
					}
					sb.append(spliter);
				}
			} catch (Exception e) {
				return "";
				// TODO: handle exception
			}
			return sb.toString().substring(0, sb.length()-1);
		}
	}
}
