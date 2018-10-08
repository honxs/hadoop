package cn.mastercom.bigdata.mapr.mro.loc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import cn.mastercom.bigdata.util.hadoop.mapred.DataDealMapperV2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cn.mastercom.bigdata.util.FilterByEci;
import cn.mastercom.bigdata.StructData.NC_LTE;
import cn.mastercom.bigdata.StructData.SIGNAL_MR_All;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.util.DataAdapterConf.ParseItem;
import cn.mastercom.bigdata.util.DataAdapterReader;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.data.Tuple2;
import cn.mastercom.bigdata.mro.loc.CellTimeKey;
import cn.mastercom.bigdata.mro.loc.MroXdrDeal;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;

/**
 *  PS. Hadoop中Mapper类是由反射创建的，即调用无参构造器。
 *	故不同厂商的Mdt需要多个Mapper类。
 *	否则需根据输入路径来实例化parseItem(弃用)
 *  已达到复用的目的。
 */
public class MdtDataMapper
{
	public static long GetEci(String tmCellID)
	{
		long eci = 0;
		if (tmCellID.indexOf("-") > 0)
		{
			String enbid = tmCellID.substring(0, tmCellID.indexOf("-"));
			String cellid = tmCellID.substring(tmCellID.indexOf("-") + 1);
			eci = Long.parseLong(enbid) * 256 + Long.parseLong(cellid);
		}
		else
		{
			eci = Long.parseLong(tmCellID);
		}
		return eci;
	}
	
	public static String mdtParseFormat(String spliter, StringBuffer sb, List<String> columnNameList, DataAdapterReader dataAdapterReader)
	{
		int enbId = StaticConfig.Int_Abnormal;
		try
		{
			if (dataAdapterReader.GetIntValue("ENBId", StaticConfig.Int_Abnormal) > 0)
			{
				enbId = dataAdapterReader.GetIntValue("ENBId", StaticConfig.Int_Abnormal);
			}
			if (dataAdapterReader.GetIntValue("CellId", StaticConfig.Int_Abnormal) > 255)
			{
				enbId = dataAdapterReader.GetIntValue("CellId", StaticConfig.Int_Abnormal) / 256;
			}
			
			for (String columnName : columnNameList)
			{
				if ("beginTime".equals(columnName))
				{
					sb.append(dataAdapterReader.getStrValue(dataAdapterReader.getColumnInfo("beginTime").pos, "")).append(spliter);
					
				}
				else if ("ENBId".equals(columnName))
				{
					sb.append(enbId).append(spliter);
				}
				else
				{
					sb.append(dataAdapterReader.GetStrValue(columnName, "")).append(spliter);
				}
			}

		}
		catch(Exception e)
		{
			return "";
		}
		return sb.toString().substring(0, sb.length() - 1);
	}

	public static class MdtImmMapper extends DataDealMapperV2<Object, Text, CellTimeKey, Text>
	{
		private Date TimeStamp;// 2015-11-01 00:02:43.000
		private String tmCellID;
		private int TimeSpan = 600;

		protected ParseItem parseItem;
		protected DataAdapterReader dataAdapterReader;
		private Text outputValue;
		protected ParseItem mdtImmParseItem;
		protected List<String> mdtImmColumnNameList;
		protected StringBuffer mdtImmSb = new StringBuffer();
	
	
		
		/**
		 * 子类覆盖此方法以注入不同厂商的adapter
		 * @return
		 */
		protected String getParseItemKey()
		{
			return "MDT-SRC-IMM";
		}
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context, MainModel.GetInstance().getAppConfig().getExcludeIpList());
			outputValue = new Text();
			parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem(getParseItemKey());
			if (parseItem == null)
			{
				throw new IOException("parse item do not get.");
			}
			dataAdapterReader = new DataAdapterReader(parseItem);
			mdtImmParseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MDT-SRC-IMM");
			if (mdtImmParseItem == null)
			{
				throw new IOException("mdtImm parseItem do not get, please check! ");
			}
			mdtImmColumnNameList = mdtImmParseItem.sortColumNameByPos();
		}

		/**
		 * Called once at the end of the task.
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			if (value.toString().length() == 0)
			{
				return;
			}

			if (value.toString().length() >= 100000)
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "map data too long : " + value.toString());
				return;
			}

			String xmString = new String(value.toString().getBytes(StaticConfig.UTFCode));
			String[] valstrs = xmString.toString().split(parseItem.getSplitMark(), -1);

			try
			{
				dataAdapterReader.readData(valstrs);
				TimeStamp = dataAdapterReader.GetDateValue("beginTime", null);
				tmCellID = dataAdapterReader.GetStrValue("CellId", "0");
				if(TimeStamp == null || tmCellID.length() <= 1 || tmCellID.length() > 10) return;

				long eci = GetEci(tmCellID);
				CellTimeKey keyItem = new CellTimeKey(eci, (int) (TimeStamp.getTime() / 1000) / TimeSpan * TimeSpan, MroXdrDeal.DataType_MDT_IMM);
				String resultStr = format(dataAdapterReader);
                if(resultStr != null  && resultStr.length() > 0 ){
                    outputValue.set(resultStr);
                    context.write(keyItem, outputValue);
                }
			}
			catch (Exception e)
			{
//				if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
//				{
					LOGHelper.GetLogger().writeLog(LogType.error,"MdrDataMapper.map data error", "MdrDataMapper.map data error : " + value.toString(),
							e);
					return;
//				}
			}

		}
		
		protected String format(DataAdapterReader dataAdapterReader){
			return dataAdapterReader.getTmStrs().toString();
		}
	}

	public static class MdtLogMapper extends DataDealMapperV2<Object, Text, CellTimeKey, Text>
	{
		private Date TimeStamp;// 2015-11-01 00:02:43.000
		private String tmCellID;
		private int TimeSpan = 600;

		protected ParseItem parseItem;
		protected DataAdapterReader dataAdapterReader;
		protected ParseItem mdtLogParseItem;
		protected List<String> mdtLogColumnNameList;
		
		protected StringBuffer mdtLogSb = new StringBuffer();
		private Text outputValue;
		
		
		/**
		 * 子类覆盖此方法以注入不同厂商的adapter
		 * @return
		 */
		protected String getParseItemKey() {
			return "MDT-SRC-LOG";
		}
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context, MainModel.GetInstance().getAppConfig().getExcludeIpList());
			outputValue = new Text();
			parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem(getParseItemKey());
			if (parseItem == null)
			{
				throw new IOException("parse item do not get.");
			}
			mdtLogParseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MDT-SRC-LOG");
			if (mdtLogParseItem == null)
			{
				throw new IOException("mdtLog parseItem do no get . please check!");
			}
			mdtLogColumnNameList = mdtLogParseItem.sortColumNameByPos();
			dataAdapterReader = new DataAdapterReader(parseItem);
		}

		/**
		 * Called once at the end of the task.
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			if (value.toString().length() == 0)
			{
				return;
			}

			String xmString = new String(value.toString().getBytes(StaticConfig.UTFCode));
			String[] valstrs = xmString.toString().split(parseItem.getSplitMark(), -1);

			try
			{
				dataAdapterReader.readData(valstrs);
				TimeStamp = dataAdapterReader.GetDateValue("beginTime", null);
				tmCellID = dataAdapterReader.GetStrValue("CellId", "0");
				if(TimeStamp == null || tmCellID.length() <= 1 || tmCellID.length() > 10) return;

				long eci = GetEci(tmCellID);
				CellTimeKey keyItem = new CellTimeKey(eci, (int) (TimeStamp.getTime() / 1000) / TimeSpan * TimeSpan, MroXdrDeal.DataType_MDT_LOG);
                String resultStr = format(dataAdapterReader);
                if(resultStr != null  && resultStr.length() > 0 ){
                    outputValue.set(resultStr);
                    context.write(keyItem, outputValue);
                }
			}
			catch (Exception e)
			{
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
				{
					LOGHelper.GetLogger().writeLog(LogType.error,"MdrDataMapper.map data error", "MdrDataMapper.map " +
                            "data error : " + value.toString(), e);
					return;
				}
			}
		}
		
		protected String format(DataAdapterReader dataAdapterReader){
			return dataAdapterReader.getTmStrs().toString();
		}
	}

	private static String getValidString(Number o){
		if(o == null) return "";
		else if(o.intValue() == StaticConfig.Int_Abnormal) return "";
		else return o.toString();
	}
	
	/************************************* 不同厂商 *************************************/
	public static class MdtImmMapper_HUAWEI extends MdtImmMapper
	{
		@Override
		protected String getParseItemKey() {
			return "MDT-SRC-IMM-HUAWEI";
		}
		
		@Override
		protected String format(DataAdapterReader dataAdapterReader) {
			mdtImmSb.setLength(0);
			String mdtImmSpliter = mdtImmParseItem.getSplitMark();
			return mdtParseFormat(mdtImmSpliter, mdtImmSb, mdtImmColumnNameList, dataAdapterReader);
			
			
		
			
		}
	}
	public static class MdtLogMapper_HUAWEI extends MdtLogMapper
	{
		@Override
		protected String getParseItemKey() {
			return "MDT-SRC-LOG-HUAWEI";
		}
		@Override
		protected String format(DataAdapterReader dataAdapterReader) {
			String mdtLogSpliter = parseItem.getSplitMark();
			mdtLogSb.setLength(0);
			return mdtParseFormat(mdtLogSpliter, mdtLogSb, mdtLogColumnNameList, dataAdapterReader);
		}
		
	}
	public static class MdtImmMapper_ZTE extends MdtImmMapper
	{
		@Override
		protected String getParseItemKey() {
			return "MDT-SRC-IMM-ZTE";
		}
		
		@Override
		protected String format(DataAdapterReader dataAdapterReader){
			mdtImmSb.setLength(0);
			String mdtImmSpliter = parseItem.getSplitMark();
			return mdtParseFormat(mdtImmSpliter, mdtImmSb, mdtImmColumnNameList, dataAdapterReader);
		}
	}
	
	public static class MdtLogMapper_ZTE extends MdtLogMapper
	{
		private final int TimeSpan = 600;// 10分钟间隔
		private ParseItem parseItem;
		private Text resultValue = new Text();
		private SIGNAL_MR_All mdtResult;
		private List<SIGNAL_MR_All> mdtList;
		String beginTime = null;//记录时间字符串
		
		@Override
		protected String getParseItemKey() {
			return "MDT-SRC-LOG-ZTE";
		}

		@Override
		protected void setup(
				Mapper<Object, Text, CellTimeKey, Text>.Context context)
				throws IOException, InterruptedException {
			super.setup(context, MainModel.GetInstance().getAppConfig().getExcludeIpList());
			parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MDT-SRC-LOG-ZTE");
			dataAdapterReader = new DataAdapterReader(parseItem);
			if (parseItem == null)
			{
				throw new IOException("parse item do not get.");
			}

			mdtList = new ArrayList<>();
			mdtResult = new SIGNAL_MR_All();
			// 按小区列表过滤
			Configuration conf = context.getConfiguration();
			String date = conf.get("mapreduce.job.date");
			FilterByEci.readEciList(conf, date);
		}

		@Override
		protected void cleanup(
				Mapper<Object, Text, CellTimeKey, Text>.Context context)
				throws IOException, InterruptedException {
			if (mdtResult.tsc.MmeUeS1apId > 0 )
			{
				mdtResult = merge(mdtList);
				resultValue.set(transform(mdtResult));
				CellTimeKey keyItem = new CellTimeKey(mdtResult.tsc.Eci,  mdtResult.tsc.beginTime / TimeSpan * TimeSpan, MroXdrDeal.DataType_MDT_LOG);
				context.write(keyItem, resultValue);
			}
			super.cleanup(context);
		}

		@Override
		public void map(Object key, Text value,
				Mapper<Object, Text, CellTimeKey, Text>.Context context)
				throws IOException, InterruptedException {
			String xmString = new String(value.toString().getBytes(StaticConfig.UTFCode));
			String[] valstrs = xmString.toString().split(parseItem.getSplitMark(), -1);
			SIGNAL_MR_All item = new SIGNAL_MR_All();

			try {
				dataAdapterReader.readData(valstrs);
				item.FillLOGData(dataAdapterReader);
			} catch (Exception e) {
				LOGHelper.GetLogger().writeLog(LogType.error,"MdtDataMapper.FillLOGData error", "MdtDataMapper.FillLOGData error ", e);
				return;
			}
			
			if (mdtList.isEmpty() || isSameOne(item, mdtList.get(0))) {
				mdtList.add(item);
			}else{				
				mdtResult = merge(mdtList);
                String resultStr = transform(mdtResult);
                if(resultStr.length() > 0 ){
                    resultValue.set(resultStr);
                    CellTimeKey keyItem = new CellTimeKey(mdtResult.tsc.Eci,  mdtResult.tsc.beginTime / TimeSpan * TimeSpan, MroXdrDeal.DataType_MDT_LOG);
                    context.write(keyItem, resultValue);
                }
				mdtList.add(item);
			}
		}
		
		/**
		 * 是否要合并为一条记录
		 * @param one
		 * @param other
		 * @return
		 */
		private boolean isSameOne(SIGNAL_MR_All one, SIGNAL_MR_All other){
			
			String oneKey = one.tsc.beginTime + "_" + one.tsc.MmeUeS1apId;
			String otherKey = other.tsc.beginTime + "_" + other.tsc.MmeUeS1apId;
			return oneKey.equals(otherKey);
		}
		
		/**
		 * 合并成同一条记录
		 * @param mrList
		 * @return
		 */
		private SIGNAL_MR_All merge(List<SIGNAL_MR_All> mrList){
			
			class NCStat{
				int sumRsrp;
				int sumRsrq;
				int count;
				
				int avgRsrp;
				int avgRsrq;
				
				public NCStat(int sumRsrp, int sumRsrq, int count) {
					this.sumRsrp = sumRsrp;
					this.sumRsrq = sumRsrq;
					this.count = count;
					stat();
				}
				
				public void add(NC_LTE nc){
					if (nc.LteNcRSRP != StaticConfig.Int_Abnormal && nc.LteNcEarfcn > 0 && nc.LteNcPci > 0) {
						this.sumRsrp += nc.LteNcRSRP;
						this.sumRsrq += nc.LteNcRSRQ;
						this.count ++;
						stat();
					}
				}
				
				public void stat(){
					if(count > 0){
						avgRsrp = sumRsrp / count;
						avgRsrq = sumRsrq / count;
					}
				}
			}
			//频点+PCI -> <sum(RSRP), count(RSRP)>
			Map<String, NCStat> earfcnPciMap = new HashMap<>();
			mdtResult = mrList.get(0);
			ArrayList<Tuple2<Integer, Integer>> latLngList = new ArrayList<>();
			int sumSCRsrp = 0;
			int sumSCRsrq = 0;
			int countSC = 0;
			for (int i = 0; i < mrList.size(); i++) {
				SIGNAL_MR_All item = mrList.get(i);
				
				if (item.tsc.longitude > 0) {
					latLngList.add(new Tuple2<>(item.tsc.longitude, item.tsc.latitude));
				}
				
				if (item.tsc.LteScRSRP != StaticConfig.Int_Abnormal && item.tsc.LteScRSRQ != StaticConfig.Int_Abnormal) {
					sumSCRsrp += item.tsc.LteScRSRP;
					sumSCRsrq += item.tsc.LteScRSRQ;
					countSC ++;
				}
				
				//合并邻区, 移动联通电信的都需要合并
				for (NC_LTE[] ncArr : new NC_LTE[][]{item.tlte, item.lt_freq, item.dx_freq}){
					for (NC_LTE nc : ncArr) {
						String earfcnPciKey = nc.LteNcEarfcn + "_" + nc.LteNcPci;
						NCStat ncStat = earfcnPciMap.get(earfcnPciKey);
						if (ncStat != null) {
							ncStat.add(nc);
						}else{
							earfcnPciMap.put(earfcnPciKey, new NCStat(nc.LteNcRSRP, nc.LteNcRSRQ, 1));
						}
					}
				}

				if (i > 0) {
					mergeTwo(mdtResult, item);
				}
			}
			
			//取中位数所在的经纬度 作为结果经纬度
			int latLngListSize = latLngList.size();
			if (latLngListSize > 0) {
				mdtResult.tsc.longitude = (int) latLngList.get(latLngListSize / 2).first;
				mdtResult.tsc.latitude = (int) latLngList.get(latLngListSize / 2).second;
			}
			
			//驻留小区rsrp取均值
			if (countSC > 0) {
				mdtResult.tsc.LteScRSRP = sumSCRsrp / countSC;
				mdtResult.tsc.LteScRSRQ = sumSCRsrq / countSC;
			}
			
			List<Map.Entry<String, NCStat>> earfcnPciList = new ArrayList<Map.Entry<String, NCStat>>(earfcnPciMap.entrySet());
	        //根据RSRP降序排序
	        Collections.sort(earfcnPciList,new Comparator<Map.Entry<String, NCStat>>() {
	            public int compare(Entry<String, NCStat> o1, Entry<String, NCStat> o2) {
	                return o2.getValue().avgRsrp - o1.getValue().avgRsrp;
	            }
	        });
	        
	        try {
                NC_LTE[] NC = mdtResult.tlte;
                for (int i = 0; i < earfcnPciList.size() && i < 6; i++) {
                    String earfcnPci = earfcnPciList.get(i).getKey();
                    NC[i].LteNcEarfcn = Integer.parseInt(earfcnPci.substring(0, earfcnPci.lastIndexOf("_")));
                    NC[i].LteNcPci = Integer.parseInt(earfcnPci.substring(earfcnPci.lastIndexOf("_") + 1));
                    NC[i].LteNcRSRP = earfcnPciList.get(i).getValue().avgRsrp;
                    NC[i].LteNcRSRQ = earfcnPciList.get(i).getValue().avgRsrq;
                }
            }catch (Exception e){}

	        mrList.clear();
			return mdtResult;
		}
		
		/**
		 * 合并两条数据
		 * @param mdtResult
		 * @param item
		 */
		private void mergeTwo(SIGNAL_MR_All mdtResult, SIGNAL_MR_All item) {
			mdtResult.tsc.MmeGroupId = getValidValueInt(mdtResult.tsc.MmeGroupId, item.tsc.MmeGroupId);
			mdtResult.tsc.MmeCode = getValidValueInt(mdtResult.tsc.MmeCode, item.tsc.MmeCode);
			mdtResult.tsc.IMSI = getValidValueLong(mdtResult.tsc.IMSI, item.tsc.IMSI);
			mdtResult.tsc.MmeUeS1apId = getValidValueLong(mdtResult.tsc.MmeUeS1apId, item.tsc.MmeUeS1apId);
			mdtResult.tsc.CellId = getValidValueLong(mdtResult.tsc.CellId, item.tsc.CellId);
			mdtResult.Confidence = getValidValueInt(mdtResult.Confidence, item.Confidence);
			mdtResult.tsc.LteScEarfcn = getValidValueInt(mdtResult.tsc.LteScEarfcn, item.tsc.LteScEarfcn);
			mdtResult.tsc.LteScPci = getValidValueInt(mdtResult.tsc.LteScPci, item.tsc.LteScPci);
		}

		/**
		 * 适配格式
		 * @param mdtResult
		 * @return
		 */
		private String transform(SIGNAL_MR_All mdtResult){
			
			String mdtLogSpliter = parseItem.getSplitMark();
			mdtLogSb.setLength(0);
			long tmp = 0;
			try
			{
				mdtLogSb.append(dataAdapterReader.getStrValue(dataAdapterReader.getColumnInfo("beginTime").pos, "")).append(mdtLogSpliter)
				.append(getValidString((tmp = mdtResult.tsc.Eci) > 0 ? tmp / 256 : tmp)).append(mdtLogSpliter)
				.append(getValidString(mdtResult.tsc.MmeGroupId)).append(mdtLogSpliter)
				.append(getValidString(mdtResult.tsc.MmeCode)).append(mdtLogSpliter)
				.append(mdtResult.tsc.IMSI).append(mdtLogSpliter)
				.append(mdtResult.tsc.MmeUeS1apId).append(mdtLogSpliter)
				.append(mdtResult.tsc.Eci).append(mdtLogSpliter)
				.append(mdtResult.tsc.LteScRSRP).append(mdtLogSpliter)
				.append(mdtResult.tsc.LteScRSRQ).append(mdtLogSpliter)
				.append(mdtResult.tsc.longitude / 10000000D).append(mdtLogSpliter)
				.append(mdtResult.tsc.latitude / 10000000D).append(mdtLogSpliter)
				.append(mdtResult.Confidence).append(mdtLogSpliter)
				 .append(getValidString(mdtResult.tlte[0].LteNcPci)).append(mdtLogSpliter)
			    .append(getValidString(mdtResult.tlte[0].LteNcEarfcn)).append(mdtLogSpliter)
			    .append(getValidString(mdtResult.tlte[0].LteNcRSRP)).append(mdtLogSpliter)
			    .append(getValidString(mdtResult.tlte[0].LteNcRSRQ)).append(mdtLogSpliter)
			    .append(getValidString(mdtResult.tlte[1].LteNcPci)).append(mdtLogSpliter)
			    .append(getValidString(mdtResult.tlte[1].LteNcEarfcn)).append(mdtLogSpliter)
			    .append(getValidString(mdtResult.tlte[1].LteNcRSRP)).append(mdtLogSpliter)
			    .append(getValidString(mdtResult.tlte[1].LteNcRSRQ)).append(mdtLogSpliter)
			    .append(getValidString(mdtResult.tlte[2].LteNcPci)).append(mdtLogSpliter)
			    .append(getValidString(mdtResult.tlte[2].LteNcEarfcn)).append(mdtLogSpliter)
			    .append(getValidString(mdtResult.tlte[2].LteNcRSRP)).append(mdtLogSpliter)
			    .append(getValidString(mdtResult.tlte[2].LteNcRSRQ)).append(mdtLogSpliter)
			    .append(getValidString(mdtResult.tlte[3].LteNcPci)).append(mdtLogSpliter)
			    .append(getValidString(mdtResult.tlte[3].LteNcEarfcn)).append(mdtLogSpliter)
			    .append(getValidString(mdtResult.tlte[3].LteNcRSRP)).append(mdtLogSpliter)
			    .append(getValidString(mdtResult.tlte[3].LteNcRSRQ)).append(mdtLogSpliter)
			    .append(getValidString(mdtResult.tlte[4].LteNcPci)).append(mdtLogSpliter)
			    .append(getValidString(mdtResult.tlte[4].LteNcEarfcn)).append(mdtLogSpliter)
			    .append(getValidString(mdtResult.tlte[4].LteNcRSRP)).append(mdtLogSpliter)
			    .append(getValidString(mdtResult.tlte[4].LteNcRSRQ)).append(mdtLogSpliter)
			    .append(getValidString(mdtResult.tlte[5].LteNcPci)).append(mdtLogSpliter)
			    .append(getValidString(mdtResult.tlte[5].LteNcEarfcn)).append(mdtLogSpliter)
			    .append(getValidString(mdtResult.tlte[5].LteNcRSRP)).append(mdtLogSpliter)
			    .append(getValidString(mdtResult.tlte[5].LteNcRSRQ)).append(mdtLogSpliter);
				return mdtLogSb.toString();
			}catch (Exception e) {
				LOGHelper.GetLogger().writeLog(LogType.error, "zte mdt log 数据转换异常! " + e.getStackTrace());
			}
			return mdtParseFormat(mdtLogSpliter, mdtLogSb, mdtLogColumnNameList, dataAdapterReader);
		}
		
		public static int getValidValueInt(int srcValue, int targValue)
		{
			if (targValue != StaticConfig.Int_Abnormal)
			{
				return targValue;
			}
			return srcValue;
		}
		
		public static double getValidValueDouble(double srcValue, double targValue)
		{
			if (targValue != StaticConfig.Double_Abnormal)
			{
				return targValue;
			}
			return srcValue;
		}

		public static long getValidValueLong(long srcValue, long targValue)
		{
			if (targValue != StaticConfig.Long_Abnormal)
			{
				return targValue;
			}
			return srcValue;
		}

		public static String getValidValueString(String srcValue, String targValue)
		{
			if (!targValue.equals(""))
			{
				return targValue;
			}
			return srcValue;
		}
		
	}
}
