package cn.mastercom.bigdata.mapr.mdt.loc;

import cn.mastercom.bigdata.mro.loc.CellTimeKey;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.DataAdapterConf.ParseItem;
import cn.mastercom.bigdata.util.DataAdapterReader;
import cn.mastercom.bigdata.util.FilterByEci;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.hadoop.mapred.DataDealMapperV2;

import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Processor.list_privileges;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class MmeLableMapper {
	public static class XdrDataMapper_MME extends DataDealMapperV2<Object, Text, CellTimeKey, Text>
	{
		private final int TimeSpan = 600;// 10分钟间隔
		private int stime = 0;
		private long eci;
		private int splitMax = -1;
		private DataAdapterReader dataAdapterReader_MME;
		private ParseItem parseItem;
		private Text curText = new Text();
		private List<String> columnNameList;
		private long errorCount; // 统计有问题的数据条数
		private List<String> errorDataList = new ArrayList<>();
		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			errorCount  = 0L;
			errorDataList.clear();
			super.setup(context, MainModel.GetInstance().getAppConfig().getExcludeIpList());
			//获得JOB的时间
			String date = conf.get("mapreduce.job.date");
			parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("S1-MME");

			if (parseItem == null)
			{
				throw new IOException("parse item do not get.");
			}
			columnNameList = parseItem.getColumNameList();
			dataAdapterReader_MME = new DataAdapterReader(parseItem);
			//切分字段的时候 最多切分多少字段
			splitMax = parseItem.getSplitMax(columnNameList);
			if (splitMax < 0)
			{
				throw new IOException("time or imsi pos not right.");
			}
			FilterByEci.readEciList(conf, date);

		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
			for (String errorData : errorDataList)
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "该mme数据解析出现问题! " + errorData);
			}
			LOGHelper.GetLogger().writeLog(LogType.error, "该map有问题的记录条数为! " + errorCount);
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] strForMME = value.toString().split(parseItem.getSplitMark(), -1);
			dataAdapterReader_MME.readData(strForMME);
			try
			{
				Date tmpTime = dataAdapterReader_MME.GetDateValue("Procedure_Start_Time",null);
				if(tmpTime == null){
					return;
				}
				stime = (int) (tmpTime.getTime() / 1000);
//				int enbID = dataAdapterReader_MME.GetIntValue("ENBId", -1);
                long imsi = dataAdapterReader_MME.GetLongValue("IMSI", -1);
                int s1apid = dataAdapterReader_MME.GetIntValue("MME_UE_S1AP_ID", -1);
//				long cellID = 0;
				eci = dataAdapterReader_MME.GetLongValue("Cell_ID", 0);
				int tmTime = stime / TimeSpan * TimeSpan;
				String str = imsi+"_"+stime+"_"+eci+"_"+s1apid;
				curText.set(str); 
				if(eci <= 0){
					return;
				}
				CellTimeKey keyItem = new CellTimeKey(eci, tmTime - TimeSpan, MdtMmeDeal.DataType_MME);
				context.write(keyItem, curText);

				keyItem = new CellTimeKey(eci, tmTime, MdtMmeDeal.DataType_MME);
				context.write(keyItem, curText);

				keyItem = new CellTimeKey(eci, tmTime + TimeSpan, MdtMmeDeal.DataType_MME);
				context.write(keyItem, curText);

			}
			catch (Exception e)
			{
				errorCount++;
				if (errorCount <= 10)
				{
					errorDataList.add(value.toString());
				}
			}
		}
	}
}



