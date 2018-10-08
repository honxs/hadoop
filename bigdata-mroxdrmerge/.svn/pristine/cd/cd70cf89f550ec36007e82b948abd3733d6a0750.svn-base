package cn.mastercom.bigdata.spark.mroxdrmerge.xdrloc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import cn.mastercom.bigdata.util.DataAdapterReader;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.util.DataAdapterConf.ParseItem;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.util.spark.MapByTypeFuncBaseV2;
import cn.mastercom.bigdata.util.spark.TypeInfo;
import cn.mastercom.bigdata.mro.stat.tableEnum.XdrLocTablesEnum;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import org.apache.hadoop.conf.Configuration;
import cn.mastercom.bigdata.conf.cellconfig.CellConfig;
import scala.Tuple2;
import scala.Tuple4;
import cn.mastercom.bigdata.xdr.loc.XdrLocDeal;

public class MapByTypeFunc_XdrLoc extends MapByTypeFuncBaseV2 implements org.apache.spark.api.java.function.PairFlatMapFunction<Tuple2<Long,Tuple4<Iterable<String>,Iterable<String>,Iterable<String>,Iterable<String>>>,TypeInfo, Iterable<String>>
{
	private static final long serialVersionUID = 1L;
	
	//数据集合下标位置
	public static final int DataMark_Location = 0;
	public static final int DataMark_XdrLocSpan = 1;
	public static final int DataMark_MME = 2;
	public static final int DataMark_HTTP = 3;
	
	public ResultOutputer resultOutputer;
	public XdrLocDeal xdrLocDeal;
	public boolean iOnce = true;
	private final static int TimeSpan = 600;// 10分钟间隔
	ParseItem mmeParseItem;
	ParseItem httpParseItem;
	private DataAdapterReader mmeAdapterReader;
	private DataAdapterReader httpAdapterReader;
	int mmeSplitMax = -1;
	int httpSplitMax = -1;
	
	private HashMap<Integer, List<List<String>>> allDataMap; //顺序location xdrlocspan mme http 
	public StringBuilder sb = new StringBuilder();
	
	public MapByTypeFunc_XdrLoc(Object[] args) throws Exception
	{
	    super(args);
	    
		String filename = "TB_DTSIGNAL_EVENT_" + dateStr;
		TypeInfo typeInfo = new TypeInfo(XdrLocTablesEnum.xdreventdt.getIndex(), "eventdt", outPath + "/" + filename);
		typeInfoMng_HDFS.registTypeInfo(typeInfo);

		filename = "TB_CQTSIGNAL_EVENT_" + dateStr;
		typeInfo = new TypeInfo(XdrLocTablesEnum.xdreventcqt.getIndex(), "eventcqt", outPath + "/" + filename);
		typeInfoMng_HDFS.registTypeInfo(typeInfo);

		filename = "TB_DTEXSIGNAL_EVENT_" + dateStr;
		typeInfo = new TypeInfo(XdrLocTablesEnum.xdreventdtex.getIndex(), "eventdtex", outPath + "/" + filename);
		typeInfoMng_HDFS.registTypeInfo(typeInfo);

		filename = "TB_SIGNAL_EVENT_01_" + dateStr;
		typeInfo = new TypeInfo(XdrLocTablesEnum.xdrevent.getIndex(), "xdrevent", outPath + "/" + filename);
		typeInfoMng_HDFS.registTypeInfo(typeInfo);

		filename = "TB_SIGNAL_USERINFO_" + dateStr;
		typeInfo = new TypeInfo(XdrLocTablesEnum.xdruserinfo.getIndex(), "userinfo", outPath + "/" + filename);
		typeInfoMng_HDFS.registTypeInfo(typeInfo);

		filename = "TB_XDR_LOCATION_" + dateStr;
		typeInfo = new TypeInfo(XdrLocTablesEnum.xdrLocation.getIndex(), "xdrLocation", outPath + "/" + filename);
		typeInfoMng_HDFS.registTypeInfo(typeInfo);

		filename = "TB_SIG_USER_BEHAVIOR_LOC_CELL_" + dateStr;
		typeInfo = new TypeInfo(XdrLocTablesEnum.xdruseract.getIndex(), "useract", outPath + "/" + filename);
		typeInfoMng_HDFS.registTypeInfo(typeInfo);
		
		filename = "TB_XDR_LOCATION_SPAN_" + dateStr;
		typeInfo = new TypeInfo(XdrLocTablesEnum.xdrLocSpan.getIndex(), "xdrLocSpan", outPath + "/" + filename);
		typeInfoMng_HDFS.registTypeInfo(typeInfo);
		//TODO：很多表需要注册
		filename = "/TB_SIGNAL_GRID_" + dateStr;
		typeInfo = new TypeInfo(XdrLocTablesEnum.xdrgrid.getIndex(), "xdrgrid", outPath + "/" + filename);
		typeInfoMng_HDFS.registTypeInfo(typeInfo);
		
		filename = "/TB_SIGNAL_CELL_" + dateStr;
		typeInfo = new TypeInfo(XdrLocTablesEnum.xdrcell.getIndex(), "xdrcell", outPath + "/" + filename);
		typeInfoMng_HDFS.registTypeInfo(typeInfo);
		
		filename = "/TB_SIGNAL_CELLGRID_" + dateStr;
		typeInfo = new TypeInfo(XdrLocTablesEnum.xdrcellgrid.getIndex(), "xdrcellgrid", outPath + "/" + filename);
		typeInfoMng_HDFS.registTypeInfo(typeInfo);
		
		filename = "/TB_DTSIGNAL_GRID_" + dateStr;
		typeInfo = new TypeInfo(XdrLocTablesEnum.xdrgriddt.getIndex(), "griddt", outPath + "/" + filename);
		typeInfoMng_HDFS.registTypeInfo(typeInfo);
		
		filename = "/TB_CQTSIGNAL_GRID_" + dateStr;
		typeInfo = new TypeInfo(XdrLocTablesEnum.xdrgridcqt.getIndex(), "gridcqt", outPath + "/" + filename);
		typeInfoMng_HDFS.registTypeInfo(typeInfo);
		
		filename = "TB_XDR_LOCATION_" + dateStr;
		typeInfo = new TypeInfo(XdrLocTablesEnum.xdrLocation.getIndex(), "xdrLocation", outPath + "/" + filename);
		typeInfoMng_RDD.registTypeInfo(typeInfo);
	}

	@Override
	protected int init_once_sub() throws Exception
	{
		//初始化基础配置	
		Configuration conf = new Configuration();
		MainModel.GetInstance().setConf(conf);

		// 初始化lte小区的信息
//		if (!CellConfig.GetInstance().loadLteCell(conf))
//		{
//			LOGHelper.GetLogger().writeLog(LogType.error, "ltecell init error 请检查！" + MainModel.GetInstance().getConf().toString());
//			throw (new IOException("ltecell init error 请检查！"));
//		}
		mmeParseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("S1-MME");
		mmeAdapterReader = new DataAdapterReader(mmeParseItem);
		mmeSplitMax = mmeParseItem.getSplitMax("Procedure_Start_Time");
		
		httpParseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("S1-HTTP");
		httpAdapterReader = new DataAdapterReader(httpParseItem);
		httpSplitMax = httpParseItem.getSplitMax("Procedure_Start_Time,longitude");
		
		resultOutputer = new ResultOutputer(dataOutputMng);
		xdrLocDeal = new XdrLocDeal(resultOutputer);
		
		// 打印状态日志
		LOGHelper.GetLogger().writeLog(LogType.info,"ltecell init count is : " + CellConfig.GetInstance().getLteCellInfoMap().size());
		
		return 0;
	}
	
	public int addData(int timeSpan,String value,int dataTypeMark)
	{
		List<List<String>> tenDataList;
		List<String> dataTypeList;
		if (!allDataMap.containsKey(timeSpan)) 
		{
			tenDataList = new ArrayList<>();
			for(int i=0;i<=DataMark_HTTP;i++)//初始化，避免越界
			{
				tenDataList.add(i,null);
			}
			dataTypeList = new ArrayList<>();
			dataTypeList.add(value);
			tenDataList.set(dataTypeMark,dataTypeList);
			allDataMap.put(timeSpan, tenDataList);
		}
		else if(allDataMap.get(timeSpan).get(dataTypeMark)== null)//有当前10分钟的数据，但没有此类型的数据
		{
			dataTypeList = new ArrayList<>();
			dataTypeList.add(value);
			allDataMap.get(timeSpan).set(dataTypeMark,dataTypeList);
		}
		else
		{
			allDataMap.get(timeSpan).get(dataTypeMark).add(value);
		}
		return 0;
	}
	
	@Override
	// 待处理的数据<imsi,<mmeList>,<httpList>,<locList>>
	public Iterable<Tuple2<TypeInfo, Iterable<String>>> call(Tuple2<Long, Tuple4<Iterable<String>, Iterable<String>, Iterable<String>, Iterable<String>>> tp) throws Exception
	{
		// 初始化配置
		init_once();
		xdrLocDeal.initImsi(tp._1);
		try
		{
			allDataMap = new HashMap<>();
			Tuple4<Iterable<String>, Iterable<String>, Iterable<String>, Iterable<String>> tp4 = tp._2;
			//Test
			//location数据
			Iterator<String> it4 = tp4._4().iterator();
			while(it4.hasNext())
			{
				int timeSpan = 0;
				String locData = it4.next();
				String[] strs = locData.split("\\|" + "|" + "\t", 4);
				if (strs[0].equals("URI") || strs[0].equals(""))
				{
					timeSpan = (int)(Long.parseLong(strs[2])/1000L) / TimeSpan * TimeSpan;
				}
				else
				{
					timeSpan = (int)(Long.parseLong(strs[1])/1000L) / TimeSpan * TimeSpan;
				}
				addData(timeSpan, locData, DataMark_Location);
			}
			
			Iterator<String> it3 = tp4._3().iterator();//前1小时最后十分钟xdr
			while(it3.hasNext())
			{
				String lastXdrData = it3.next();
				String[] strs = lastXdrData.split("\t",5);
				int timeSpan = Integer.parseInt(strs[3]) / TimeSpan * TimeSpan;
				addData(timeSpan, lastXdrData, DataMark_XdrLocSpan);
			}
			
			Iterator<String> it1 = tp4._1().iterator();
			while(it1.hasNext())
			{
				String mmeData = it1.next();
				String[] strs = mmeData.split(mmeParseItem.getSplitMark(),mmeSplitMax+2);
				mmeAdapterReader.readData(strs);
				Date d_beginTime = mmeAdapterReader.GetDateValue("Procedure_Start_Time", new Date(1970, 1, 1));
				int timeSpan = (int)(d_beginTime.getTime() / 1000L) / TimeSpan * TimeSpan;
				addData(timeSpan, mmeData, DataMark_MME);
			}

			Iterator<String> it2 = tp4._2().iterator();
			while(it2.hasNext())
			{
				String httpData = it2.next();
				String[] strs = httpData.split(httpParseItem.getSplitMark(),httpSplitMax+2);
				httpAdapterReader.readData(strs);
				Date d_beginTime = httpAdapterReader.GetDateValue("Procedure_Start_Time", new Date(1970, 1, 1));
				int timeSpan = (int)(d_beginTime.getTime() / 1000L) / TimeSpan * TimeSpan;
				addData(timeSpan, httpData, DataMark_HTTP);
			}
			
			List<Integer> timeList = new ArrayList<>(allDataMap.keySet());
			Collections.sort(timeList,new Comparator<Integer>()
			{
				@Override
				public int compare(Integer o1, Integer o2) {
					return o1 - o2;
				}
			});
			
			for(Integer timeSpan : timeList)
			{
				List<List<String>> tenDataList = allDataMap.get(timeSpan);
				if (tenDataList.get(DataMark_Location) != null)
				{
					for(String locData : tenDataList.get(DataMark_Location))
					{
						xdrLocDeal.pushData(XdrLocDeal.DataType_LOCATION, locData);
					}
				}
				
				if (tenDataList.get(DataMark_XdrLocSpan) != null)
				{
					for (String lastXdrData : tenDataList.get(DataMark_XdrLocSpan))
					{
						xdrLocDeal.pushData(XdrLocDeal.DataType_XDR_LOC_SPAN, lastXdrData);
					}
				}
				
				if (tenDataList.get(DataMark_MME) != null)
				{
					for (String mmeData : tenDataList.get(DataMark_MME))
					{
						xdrLocDeal.pushData(XdrLocDeal.DataType_XDR_MME, mmeData);
					}
				}
				
				if (tenDataList.get(DataMark_HTTP) != null)
				{
					for (String httpData : tenDataList.get(DataMark_HTTP))
					{
						xdrLocDeal.pushData(XdrLocDeal.DataType_XDR_HTTP, httpData);
					}
				}
				allDataMap.get(timeSpan).clear();//当前10分钟数据清空
				xdrLocDeal.statData();
				xdrLocDeal.outData();
			}
			allDataMap.clear();
			xdrLocDeal.outAllData();
		}
		catch (Exception e)
		{
			e.printStackTrace();
			LOGHelper.GetLogger().writeLog(LogType.error, "MapByTypeFunc_XdrLoc Error: " + sb.toString());
		}
		
		List<Tuple2<TypeInfo, Iterable<String>>> resultRdd = typeResult.GetRdds();
		typeResult.clear();
		return resultRdd;
	}

	public void logErr(Exception e)
	{
		sb.delete(0, sb.length());
		StackTraceElement[] trace = e.getStackTrace();
		for (StackTraceElement stackTraceElement : trace)
		{
			sb.append("\tat " + stackTraceElement);
		}
	}
	
	

}
