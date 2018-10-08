package cn.mastercom.bigdata.mapr.xdr.loc;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import cn.mastercom.bigdata.util.*;
import cn.mastercom.bigdata.util.hadoop.mapred.CombineSmallFileRecordReader;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.DataAdapterConf.ParseItem;
import cn.mastercom.bigdata.util.hadoop.mapred.DataDealMapperV2;
import cn.mastercom.bigdata.xdr.loc.ImsiTimeKey;
import cn.mastercom.bigdata.xdr.loc.XdrLocDeal;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class XdrLabelMapper
{
	public static class XdrDataMapper_GSM extends DataDealMapperV2<Object, Text, ImsiTimeKey, Text>
	{
		private final int TimeSpan = 600;// 10分钟间隔
		private int stime = 0;
		private Date d_beginTime;
		private ImsiTimeKey imsiKey;
		private String xmString = "";
		private String[] valstrs;
		private long imsi_long;
		private ParseItem parseItem;
		private DataAdapterReader dataAdapterReader;
		private Text curText = new Text();
		private String curFileSplitPath = "";
		private Map<String, String> dataPathMap = null;
		private StringBuffer sb = new StringBuffer();
		private String splitMark = "\t";

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context, MainModel.GetInstance().getAppConfig().getExcludeIpList());
			initData(context);
		}

		public void initData(Context context)
		{
			String allDataInpath = conf.get("mastercom.mroxdrmerge.xdrloc.gsm.inpath");
			dataPathMap = new HashMap<>();
			for(String dataInpath : allDataInpath.split(";", -1))
			{
				String[] items = dataInpath.split("#", -1);
				if (items.length == 2)
				{
					String dataType = items[0];
					String input = items[1];
					dataPathMap.put(input, dataType);
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			sb.delete(0, sb.length());
			String tmpInPath = context.getConfiguration().get(CombineSmallFileRecordReader.CombineSmallFilePath);
			int sPos = tmpInPath.indexOf("/", ("hdfs://").length());
			String formatPath = tmpInPath.substring(sPos);
			if (tmpInPath.startsWith("file:"))
			{
				formatPath = tmpInPath.replace("file:", "");
			}
			if(!formatPath.equals(curFileSplitPath) || curFileSplitPath.length() != formatPath.length())
			{
				parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem(dataPathMap.get(formatPath));
				if (parseItem == null)
				{
					throw new IOException("parse item do not get.");
				}
				dataAdapterReader = new DataAdapterReader(parseItem);
				curFileSplitPath = formatPath;
			}

			xmString = new String(value.toString().getBytes(StaticConfig.UTFCode));
			valstrs = xmString.split(parseItem.getSplitMark(), -1);
			try{
				dataAdapterReader.readData(valstrs);
				d_beginTime = dataAdapterReader.GetDateValue("Procedure_Start_Time", new Date(1970, 1, 1));
				imsi_long = dataAdapterReader.GetLongValue("IMSI", 0);
				if (String.valueOf(imsi_long).length() < 15 || imsi_long == 6061155539545534980L
						|| imsi_long == 5750288053043553775L)
				{
					return;
				}
				int lac = dataAdapterReader.GetIntValue("LAC", 0);
				int ci = dataAdapterReader.GetIntValue("Cell_ID", 0);
				String msisdn = dataAdapterReader.GetStrValue("MSISDN", "");
				if(msisdn.startsWith("86") || msisdn.startsWith("+86"))
				{
					msisdn = msisdn.substring(msisdn.length() - 11, msisdn.length());
				}
				String imei = dataAdapterReader.GetStrValue("IMEI", "");
				stime = (int) (d_beginTime.getTime() / 1000L);

				sb.append(stime).append(splitMark)
						.append(imsi_long).append(splitMark)
						.append(msisdn).append(splitMark)
						.append(imei).append(splitMark)
						.append(lac).append(splitMark)
						.append(ci);
				imsiKey = new ImsiTimeKey(imsi_long, stime, stime / TimeSpan * TimeSpan, XdrLocDeal.DataType_XDR_GSM);
				curText.set(sb.toString());
				context.write(imsiKey, curText);
			}catch (Exception e){
				LOGHelper.GetLogger().writeLog(LogType.error,"XdrDataFileReducer.pushDataError", "XdrDataMapper_GSM.map error : " + xmString, e);
			}
		}
	}

	public static class XdrDataMapper_TD extends DataDealMapperV2<Object, Text, ImsiTimeKey, Text>
	{
		private final int TimeSpan = 600;// 10分钟间隔
		private int stime = 0;
		private Date d_beginTime;
		private ImsiTimeKey imsiKey;
		private String xmString = "";
		private String[] valstrs;
		private long imsi_long;
		private ParseItem parseItem;
		private DataAdapterReader dataAdapterReader;
		private Text curText = new Text();
		private String curFileSplitPath = "";
		private Map<String, String> dataPathMap = null;
		private String splitMark = "\t";
		private StringBuffer sb = new StringBuffer();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			initData(context);
		}

		public void initData(Context context)
		{
			String allDataInpath = conf.get("mastercom.mroxdrmerge.xdrloc.td.inpath");
			dataPathMap = new HashMap<>();
			for(String dataInpath : allDataInpath.split(";", -1))
			{
				String[] items = dataInpath.split("#", -1);
				if (items.length == 2)
				{
					String dataType = items[0];
					String input = items[1];
					dataPathMap.put(input, dataType);
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			sb.delete(0, sb.length());
			String tmpInPath = context.getConfiguration().get(CombineSmallFileRecordReader.CombineSmallFilePath);
			int sPos = tmpInPath.indexOf("/", ("hdfs://").length());
			String formatPath = tmpInPath.substring(sPos);
			if (tmpInPath.startsWith("file:"))
			{
				formatPath = tmpInPath.replace("file:", "");
			}
			if(!formatPath.equals(curFileSplitPath) || curFileSplitPath.length() != formatPath.length())
			{
				parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem(dataPathMap.get(formatPath));
				if (parseItem == null)
				{
					throw new IOException("parse item do not get.");
				}
				dataAdapterReader = new DataAdapterReader(parseItem);
				curFileSplitPath = formatPath;
			}

			xmString = new String(value.toString().getBytes(StaticConfig.UTFCode));
			valstrs = xmString.toString().split(parseItem.getSplitMark());
			try{
				dataAdapterReader.readData(valstrs);
				d_beginTime = dataAdapterReader.GetDateValue("Procedure_Start_Time", new Date(1970, 1, 1));
				imsi_long = dataAdapterReader.GetLongValue("IMSI", 0);
				if (String.valueOf(imsi_long).length() < 15 || imsi_long == 6061155539545534980L
						|| imsi_long == 5750288053043553775L)
				{
					return;
				}
				int lac = dataAdapterReader.GetIntValue("LAC", 0);
				int ci = dataAdapterReader.GetIntValue("Cell_ID", 0);
				String msisdn = dataAdapterReader.GetStrValue("MSISDN", "");
				if(msisdn.startsWith("86") || msisdn.startsWith("+86"))
				{
					msisdn = msisdn.substring(msisdn.length() - 11, msisdn.length());
				}
				String imei = dataAdapterReader.GetStrValue("IMEI", "");
				stime = (int) (d_beginTime.getTime() / 1000L);

				sb.append(stime).append(splitMark)
						.append(imsi_long).append(splitMark)
						.append(msisdn).append(splitMark)
						.append(imei).append(splitMark)
						.append(lac).append(splitMark)
						.append(ci);
				imsiKey = new ImsiTimeKey(imsi_long, stime, stime / TimeSpan * TimeSpan, XdrLocDeal.DataType_XDR_TD);
				curText.set(sb.toString());
				context.write(imsiKey, curText);
			}catch (Exception e){
				LOGHelper.GetLogger().writeLog(LogType.error,"XdrDataMapper_TD.map error", "XdrDataMapper_TD.map error : " + xmString, e);
			}
		}
	}

	public static class ResidentUserMap extends Mapper<Object, Text, ImsiTimeKey, Text>
	{
		private String imsi = "";
		private String[] valstrs;
		private ImsiTimeKey imsiKey;

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			valstrs = value.toString().split("\t", -1);
			try
			{
				if (valstrs.length < 6 || Long.parseLong(valstrs[8]) <= 0)
				{
					return;
				}
				imsi = valstrs[1];
				if (imsi.trim().length() < 5)
				{
					return;
				}
			}
			catch (NumberFormatException e)
			{
				return;
			}
			imsiKey = new ImsiTimeKey(Long.parseLong(imsi), 0, 0, XdrLocDeal.DataType_RESIDENT_USER);
			context.write(imsiKey, value);
		}
	}

	public static class LocationMapper extends Mapper<Object, Text, ImsiTimeKey, Text>
	{
		private long imsi;
		private String[] valstrs;
		private ImsiTimeKey imsiKey;

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			valstrs = value.toString().split("\\|" + "|" + "\t", -1);

			if (valstrs.length < 11)
			{
				return;
			}
			
			String imsiStr = "";
			if (valstrs[0].equals("URI"))
			{
				imsiStr = valstrs[1];
			}
			else
			{
				imsiStr = valstrs[0];
			}

			if (imsiStr.trim().length() < 15 || imsiStr.equals("6061155539545534980"))
			{// 6061155539545534980是空字符加密后的结果
				return;
			}
			
			if (StringUtil.isNum(imsiStr)) {
				imsi = Long.parseLong(imsiStr);
			}else {
				imsi = StringUtil.EncryptStringToLong(imsiStr);
			}

			try
			{
				imsiKey = new ImsiTimeKey(imsi, 0, 0, XdrLocDeal.DataType_LOCATION);
				context.write(imsiKey, value);
			}
			catch (NumberFormatException e)
			{

			}
		}
	}

	public static class LableMapper extends Mapper<Object, Text, ImsiTimeKey, Text>
	{
		private String beginTime = "";
		private String endTime = "";
		private long imsi;
		private final int TimeSpan = 600;// 10分钟间隔
		private int stime = 0;
		private int etime = 0;
		private Date d_beginTime;
		private String[] valstrs;
		private ImsiTimeKey imsiKey;
		private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		private String xmString;

		@Override
		protected void setup(Mapper<Object, Text, ImsiTimeKey, Text>.Context context) throws IOException, InterruptedException
		{
			format.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
			super.setup(context);
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			xmString = new String(value.toString().getBytes(StaticConfig.UTFCode));
			valstrs = xmString.toString().split("\t" + "|" + "\\|", -1);

			for (int i = 0; i < valstrs.length; i++)
			{
				valstrs[i] = valstrs[i].trim();
			}

			if (valstrs.length < 4)
			{
				return;
			}

			beginTime = valstrs[1];
			endTime = valstrs[2];
			String imsiStr = valstrs[0].trim();

			if (beginTime.length() == 0 || imsiStr.trim().length()< 15)
			{
				return;
			}
			
			if (StringUtil.isNum(imsiStr)) {
				imsi = Long.parseLong(imsiStr);
			}else {
				imsi = StringUtil.EncryptStringToLong(imsiStr);
			}

			stime = 0;
			try
			{
				beginTime = beginTime.substring(0, beginTime.length() - 7);
				d_beginTime = format.parse(beginTime);
				stime = (int) (d_beginTime.getTime() / 1000L);
			}
			catch (Exception e)
			{
				return;
			}

			etime = 0;
			try
			{
				endTime = endTime.substring(0, endTime.length() - 7);
				d_beginTime = format.parse(endTime);
				etime = (int) (d_beginTime.getTime() / 1000L);
			}
			catch (Exception e)
			{
				return;
			}

			int tmSTime = stime / TimeSpan * TimeSpan;
			int tmETime = etime / TimeSpan * TimeSpan;
			if (tmSTime == tmETime)
			{
				imsiKey = new ImsiTimeKey(imsi, stime, stime / TimeSpan * TimeSpan, XdrLocDeal.DataType_LABEL);
				context.write(imsiKey, value);
			}
			else
			{
				for (int tTime = tmSTime; tTime <= tmETime; tTime += TimeSpan)
				{
					imsiKey = new ImsiTimeKey(imsi, tTime, tTime, XdrLocDeal.DataType_LABEL);
					context.write(imsiKey, value);
				}
			}
		}
	}

	public static class LocationWFMapper extends Mapper<Object, Text, ImsiTimeKey, Text>
	{
		private long imsi;
		private String[] valstrs;
		private ImsiTimeKey imsiKey;

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			valstrs = value.toString().split("\\|" + "|" + "\t", 3);

			if (valstrs.length < 2)
			{
				return;
			}

			String imsiStr = valstrs[0];
			if (imsiStr.length() < 5 || imsiStr.equals("6061155539545534980"))
			{
				return;
			}
			
			if (StringUtil.isNum(imsiStr)) {
				imsi = Long.parseLong(imsiStr);
			}else {
				imsi = StringUtil.EncryptStringToLong(imsiStr);
			}

			imsiKey = new ImsiTimeKey(imsi, 0, 0, XdrLocDeal.DataType_WIFI);
			context.write(imsiKey, value);
		}
	}

	public static class XdrDataMapper_MME extends DataDealMapperV2<Object, Text, ImsiTimeKey, Text>
	{
		private final int TimeSpan = 600;// 10分钟间隔
		private int stime = 0;
		private Date d_beginTime;
		private ImsiTimeKey imsiKey;
		private String xmString = "";
		private String[] valstrs;
		private long imsi_long;
		private long eci;
		private ParseItem parseItem;
		private DataAdapterReader dataAdapterReader;
		private int splitMax = -1;
		private Text curText = new Text();
		private List<String> columnNameList;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);

			String date = conf.get("mapreduce.job.date");
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
			FilterByEci.readEciList(conf, date);
		}
		
		/**
		 * Called once at the end of the task.
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}

		// 华为seq数据接入
        @Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{

			xmString = new String(value.toString().getBytes(StaticConfig.UTFCode));
			valstrs = xmString.toString().split(parseItem.getSplitMark(), splitMax + 2);
			try
			{
				dataAdapterReader.readData(valstrs);
				d_beginTime = dataAdapterReader.GetDateValue("Procedure_Start_Time", new Date(1970, 1, 1));
				imsi_long = dataAdapterReader.GetLongValue("IMSI", 0);
				eci = dataAdapterReader.GetLongValue("Cell_ID", 0);
				if (eci > Integer.MAX_VALUE || !FilterByEci.ifMap(eci))
				{
					return;
				}
				if (String.valueOf(imsi_long).length() < 15 || imsi_long == 6061155539545534980L 
						|| imsi_long == 5750288053043553775L)
				{
                    LOGHelper.GetLogger().writeLog(LogType.error, "get data error :" + xmString);
					return;
				}
				stime = (int) (d_beginTime.getTime() / 1000L);
				imsiKey = new ImsiTimeKey(imsi_long, stime, stime / TimeSpan * TimeSpan, XdrLocDeal.DataType_XDR_MME);
				curText.set(dataAdapterReader.getAppendString(columnNameList));
				context.write(imsiKey, curText);
			}
			catch (Exception e)
			{
                LOGHelper.GetLogger().writeLog(LogType.error, "XdrLabelMapper get data error","XdrLabelMapper get data error : " + xmString, e);
				return;
			}
		}
	}

	public static class XdrDataMapper_HTTP extends DataDealMapperV2<Object, Text, ImsiTimeKey, Text>
	{
		private final int TimeSpan = 600;// 10分钟间隔
		private int stime = 0;
		private Date d_beginTime;
		private ImsiTimeKey imsiKey;
		private String xmString = "";
		private String[] valstrs;
		private long imsi_long;
		private long eci;
		private ParseItem parseItem;
		private DataAdapterReader dataAdapterReader;
		private int splitMax = -1;
		private Text curText = new Text();
		private List<String> columnNameList;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);

			String date = conf.get("mapreduce.job.date");
			parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("S1-HTTP");
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

			FilterByEci.readEciList(conf, date);
		}

		/**
		 * Called once at the end of the task.
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}

		// 华为seq数据接入
        @Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			xmString = new String(value.toString().getBytes(StaticConfig.UTFCode));
			valstrs = xmString.toString().split(parseItem.getSplitMark(), splitMax + 2);

			try
			{
				dataAdapterReader.readData(valstrs);
				d_beginTime = dataAdapterReader.GetDateValue("Procedure_Start_Time", new Date(1970, 1, 1));
				imsi_long = dataAdapterReader.GetLongValue("IMSI", 0);
				eci = dataAdapterReader.GetLongValue("Cell_ID", 0);
				if (eci > Integer.MAX_VALUE || !FilterByEci.ifMap(eci))
				{
					return;
				}
				double longitude = dataAdapterReader.GetDoubleValue("longitude", 0);
				if (longitude <= 0)
				{
					return;
				}
				//6061155539545534980L=""  5750288053043553775L=0
				if (String.valueOf(imsi_long).length() < 15 || imsi_long == 6061155539545534980L 
						|| imsi_long == 5750288053043553775L)
				{
                    LOGHelper.GetLogger().writeLog(LogType.error, "get data error :" + xmString);
					return;
				}
				stime = (int) (d_beginTime.getTime() / 1000L);
				imsiKey = new ImsiTimeKey(imsi_long, stime, stime / TimeSpan * TimeSpan, XdrLocDeal.DataType_XDR_HTTP);
				curText.set(dataAdapterReader.getAppendString(columnNameList));
				context.write(imsiKey, curText);
			}
			catch (Exception e)
			{
                LOGHelper.GetLogger().writeLog(LogType.error, "XdrDataMapper_HTTP.map error","XdrDataMapper_HTTP.map error" +
                        " : " + xmString, e);
				return;
			}
		}
	}

	public static class ImsiPartitioner extends Partitioner<ImsiTimeKey, Text> implements Configurable
	{
		private Configuration conf = null;

		@Override
		public Configuration getConf()
		{
			return conf;
		}

		@Override
		public void setConf(Configuration conf)
		{
			this.conf = conf;
		}

		@Override
		public int getPartition(ImsiTimeKey key, Text value, int numOfReducer)
		{
			// return Math.abs((int)(key.getImsi() & 0x00000000ffffffffL)) %
			// numOfReducer;

			if (key.getDataType() == 4)// cpe用户
			{
				return Math.abs((key.getImsi() + "_" + key.getTime() / 3600 * 3600).hashCode()) % numOfReducer;
			}
			else
			{
				return Math.abs(("" + key.getImsi()).hashCode()) % numOfReducer;
			}
		}

	}

	public static class ImsiSortKeyComparator extends WritableComparator
	{
		public ImsiSortKeyComparator()
		{
			super(ImsiTimeKey.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b)
		{
			ImsiTimeKey s1 = (ImsiTimeKey) a;
			ImsiTimeKey s2 = (ImsiTimeKey) b;
			return s1.compareTo(s2);
		}

	}

	public static class ImsiSortKeyGroupComparator extends WritableComparator
	{

		public ImsiSortKeyGroupComparator()
		{
			super(ImsiTimeKey.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b)// 同一个imsi
																		// 10分钟，放一组内
		{
			ImsiTimeKey s1 = (ImsiTimeKey) a;
			ImsiTimeKey s2 = (ImsiTimeKey) b;

			if (s1.getImsi() > s2.getImsi())
			{
				return 1;
			}
			else if (s1.getImsi() < s2.getImsi())
			{
				return -1;
			}
			else
			{
				if (s1.getTimeSpan() > s2.getTimeSpan())
				{
					return 1;
				}
				else if (s1.getTimeSpan() < s2.getTimeSpan())
				{
					return -1;
				}
				return 0;
			}

		}
	}
}
