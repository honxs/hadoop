package cn.mastercom.bigdata.mapr.xdr.loc.prepare;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;

import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.util.DataAdapterReader;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.util.DataAdapterConf.ParseItem;
import cn.mastercom.bigdata.util.hadoop.mapred.DataDealMapperV2;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.xdr.prepare.deal.CPEUserItem;
import cn.mastercom.bigdata.xdr.prepare.deal.XdrPrepareDeal;

public class XdrPrepareMapper
{
	public static class XdrDataMapper_MME extends DataDealMapperV2<Object, Text, Text, Text>
	{
		private String xmString = "";
		private StringBuffer sb = new StringBuffer();
		private String[] valstrs;
		private long imsi;
		private Map<Long, CPEUserItem> cpeMap = new HashMap<Long, CPEUserItem>();
		private Text curText = new Text();
		private Text curTextKey = new Text();
		
		private ParseItem parseItem;
		private DataAdapterReader dataAdapterReader;
		private int splitMax = -1;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context, MainModel.GetInstance().getAppConfig().getExcludeIpList());
						
			parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("S1-MME");
			if (parseItem == null)
			{
				throw new IOException("parse item do not get.");
			}
			dataAdapterReader = new DataAdapterReader(parseItem);

			splitMax = parseItem.getSplitMax("IMSI");
			if (splitMax < 0)
			{
				throw new IOException("time or imsi pos not right.");
			}    
		}
		
		/**
		 * Called once at the end of the task.
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			for (Map.Entry<Long, CPEUserItem> imsiMapEntry : cpeMap.entrySet())
			{
				sb.delete(0, sb.length());
				sb.append(imsiMapEntry.getValue().imsi);
				sb.append("\t");
				sb.append(imsiMapEntry.getValue().count);
				sb.append("\t");
				sb.append(imsiMapEntry.getValue().isCPE);				
				
				curTextKey.set(imsiMapEntry.getKey().toString());
				curText.set(sb.toString());
				context.write(curTextKey, curText);
			}
			
			super.cleanup(context);
		
		}
		
		//华为seq数据
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			xmString = new String(value.toString().getBytes(StaticConfig.UTFCode));
			valstrs = xmString.toString().split(parseItem.getSplitMark(), splitMax + 2);
			
			try
			{
				dataAdapterReader.readData(valstrs);
				imsi = dataAdapterReader.GetLongValue("IMSI", -1);

				if (imsi <= 0)
				{
					return;
				}

				CPEUserItem cpeItem = cpeMap.get(imsi);
				if (cpeItem == null)
				{
					cpeItem = new CPEUserItem();
					cpeItem.imsi = imsi;
					cpeItem.count = 0;
					cpeItem.isCPE = 0;
					cpeMap.put(imsi, cpeItem);
				}

				cpeItem.count++;
			}
			catch (Exception e)
			{
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
				{
					LOGHelper.GetLogger().writeLog(LogType.error,"XdrPrepareMapper format error", "XdrPrepareMapper format error：" + xmString, e);
				}	
			}
			
		}
	}

	public static class XdrDataMapper_HTTP extends DataDealMapperV2<Object, Text, Text, Text>
	{
//		private StringBuffer sb = new StringBuffer();
//		private Map<Long, CPEUserItem> cpeMap = new HashMap<Long, CPEUserItem>();
//		private Text curText = new Text();
//		private Text curTextKey = new Text();
		private ResultOutputer resultOutputer;
		public XdrPrepareDeal xdrPrepareDeal;
		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context, MainModel.GetInstance().getAppConfig().getExcludeIpList());
			
			resultOutputer = new ResultOutputer(mos);
			xdrPrepareDeal = new XdrPrepareDeal(resultOutputer);
		}
		
		/**
		 * Called once at the end of the task.
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
//			for (Map.Entry<Long, CPEUserItem> imsiMapEntry : cpeMap.entrySet())
//			{
//				xdrPrepareDeal.sb.delete(0, sb.length());
//				xdrPrepareDeal.sb.append(imsiMapEntry.getValue().imsi);
//				xdrPrepareDeal.sb.append("\t");
//				xdrPrepareDeal.sb.append(imsiMapEntry.getValue().count);
//				xdrPrepareDeal.sb.append("\t");
//				xdrPrepareDeal.sb.append(imsiMapEntry.getValue().isCPE);				
//				
//				curTextKey.set(imsiMapEntry.getKey().toString());
//				curText.set(xdrPrepareDeal.sb.toString());
//				context.write(curTextKey, curText);
//			}
			super.cleanup(context);		
		}
		
		//华为seq数据
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			
			int pushDataFlag = 0;
			pushDataFlag = xdrPrepareDeal.pushData(XdrPrepareDeal.DataType_MAP, value.toString());
			if (pushDataFlag == StaticConfig.FAILURE)
			{
				return;
			}
		}
	}
}