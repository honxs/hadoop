package cn.mastercom.bigdata.xdr.loc;

import java.io.IOException;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.*;

import cn.mastercom.bigdata.StructData.*;
import cn.mastercom.bigdata.util.*;
import org.apache.hadoop.conf.Configuration;

import cn.mastercom.bigdata.conf.cellconfig.CellConfig;
import cn.mastercom.bigdata.conf.cellconfig.LteCellInfo;
import cn.mastercom.bigdata.conf.config.SpecialUserConfig;
import cn.mastercom.bigdata.loc.userResident.UserResidentCluster;
import cn.mastercom.bigdata.mro.loc.XdrLabel;
import cn.mastercom.bigdata.mro.stat.tableEnum.XdrLocTablesEnum;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.stat.hsr.old.HiRailConfig;
import cn.mastercom.bigdata.stat.hsr.old.RailFillFunc;
import cn.mastercom.bigdata.stat.userResident.enmus.ResidentLocTablesEnum;
import cn.mastercom.bigdata.util.DataAdapterConf.ParseItem;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.util.data.MyInt;
import cn.mastercom.bigdata.xdr.loc.UserActStat.UserAct;
import cn.mastercom.bigdata.xdr.loc.UserActStat.UserActTime;
import cn.mastercom.bigdata.xdr.loc.UserInfoMng.UserInfo;

public class XdrLocDeal implements IDataDeal, Serializable
{
	public static final int DataType_RESIDENT_USER = 0;
	public static final int DataType_LOCATION = 1;
	public static final int DataType_LABEL = 2;
	public static final int DataType_WIFI = 3;
	public static final int DataType_XDR_LOC_SPAN = 9;
	public static final int DataType_XDR_MME = 10;
	public static final int DataType_XDR_HTTP = 11;
	public static final int DataType_XDR_GSM = 12;
	public static final int DataType_XDR_TD= 13;

	private static DecimalFormat doubleFormat = new DecimalFormat("#.00");
	private ResultOutputer resultOutputer;
	private final int TimeSpan = 600;// 10分钟间隔
	public StatDeal statDeal;
	public StatDeal_DT statDeal_DT;
	public StatDeal_CQT statDeal_CQT;
	private LabelItemMng labelItemMng;
	private LocationItemMng locationItemMng;
	private LocationWFItemMng locationWFItemMng;
	private ResidentUserMng residentUserMng;// 用户常住小区
	private LabelDeal labelDeal;
	// private LocationDayDeal locationDayDeal;// 一天的location数据处理
	
	private UserInfoMng userInfoMng;
	public List<SIGNAL_LOC> userXdrList;
	private HashMap<Integer, List<SIGNAL_LOC>> gsmTdXdrListMap;
	public int mmeCount;
	public int httpCount;
	public int locCount;
	public int formatErrCount;
	public int eciErrCount;
	public int locErrCount;
	public int radiusErrCount;
	public int lteXdrLocCount;
	private Map<Long, MyInt> cellLocDic = new HashMap<Long, MyInt>();
	List<SIGNAL_LOC> xdrItemList;
	int curTimeSpan = 0;
	ParseItem parseItem_MME;
	ParseItem parseItem_HTTP;
	DataAdapterReader dataAdapterReader_MME;
	DataAdapterReader dataAdapterReader_HTTP;
	private boolean hasInit = false;
	private long lastImsi = -1;
	private long residentUserCellDistance;

	public XdrLocDeal(ResultOutputer resultOutputer)
	{
		this.resultOutputer = resultOutputer;
		init_once(resultOutputer);
	}

	private void init_once(ResultOutputer resultOutputer)
	{
		eciErrCount = 0;
		locErrCount = 0;
		radiusErrCount = 0;
		mmeCount = 0;
		httpCount = 0;
		locCount = 0;
		formatErrCount = 0;
		lteXdrLocCount = 0;

		userXdrList = new ArrayList<SIGNAL_LOC>();
		gsmTdXdrListMap = new HashMap<>();
		statDeal = new StatDeal(resultOutputer);
		statDeal.cellLocDic = cellLocDic;
		statDeal_DT = new StatDeal_DT(resultOutputer);
		statDeal_DT.cellLocDic = cellLocDic;
		statDeal_CQT = new StatDeal_CQT(resultOutputer);
		statDeal_CQT.cellLocDic = cellLocDic;

		labelItemMng = new LabelItemMng();
		locationItemMng = new LocationItemMng();
		locationWFItemMng = new LocationWFItemMng();
		userInfoMng = new UserInfoMng();
		residentUserMng = new ResidentUserMng();

		// 初始化lte小区的信息
		Configuration conf = MainModel.GetInstance().getConf();

		try{
			if (!CellConfig.GetInstance().loadLteCell(conf)) 
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "ltecell init error 请检查！" + CellConfig.GetInstance().errLog);
			}
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.HiRail))// 高铁需要的配置
			{
				if (!HiRailConfig.loadConfig(conf)) {
					LOGHelper.GetLogger().writeLog(LogType.error, "HiRail  init error 请检查！" + HiRailConfig.errLog.toString());
				}
			}
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.BeiJing)) 
			{	
				
				if (!SpecialUserConfig.GetInstance().loadSpecialuser(conf, true)) 
				{
					LOGHelper.GetLogger().writeLog(LogType.error, "specialUser init error 请检查！");
				}
			}
		}catch(Exception e){
			LOGHelper.GetLogger().writeLog(LogType.error,"XdrLocDeal.init_once error", "XdrLocDeal.init_once error ", e);
		}
		

		parseItem_MME = MainModel.GetInstance().getDataAdapterConfig().getParseItem("S1-MME");
		if (parseItem_MME == null) 
		{
			throw new RuntimeException("parse item do not get.");
		}
		parseItem_MME.resortColumNamePos();
		dataAdapterReader_MME = new DataAdapterReader(parseItem_MME);

		parseItem_HTTP = MainModel.GetInstance().getDataAdapterConfig().getParseItem("S1-HTTP");
		if (parseItem_HTTP == null) 
		{
			throw new RuntimeException("parse item do not get.");
		}
		parseItem_HTTP.resortColumNamePos();
		dataAdapterReader_HTTP = new DataAdapterReader(parseItem_HTTP);

		residentUserCellDistance = MainModel.GetInstance().getAppConfig().getResidentUserCellDistance();
		// 打印状态日志
		LOGHelper.GetLogger().writeLog(LogType.info, "ltecell init count is : " + CellConfig.GetInstance().getLteCellInfoMap().size());
		LOGHelper.GetLogger().writeLog(LogType.info, "gsmcell init count is : " + CellConfig.GetInstance().getGsmCellInfoMap().size());
		LOGHelper.GetLogger().writeLog( LogType.info, "tdcell init count is : " + CellConfig.GetInstance().getTDCellInfoMap().size());

	}

	public void init(long imsi)
	{
		if (lastImsi != imsi)
		{
			// 吐出上个用户的xdr
			outUserData();
			initImsi(imsi);
		}
	}

	public void initImsi(long imsi)
	{
		lastImsi = imsi;
		userXdrList = new ArrayList<SIGNAL_LOC>();
		gsmTdXdrListMap = new HashMap<>();
		labelItemMng = new LabelItemMng();
		locationItemMng = new LocationItemMng();
		locationWFItemMng = new LocationWFItemMng();
		residentUserMng = new ResidentUserMng();
		//++
		// locationDayDeal = new LocationDayDeal(lastImsi, resultOutputer);
		labelDeal = new LabelDeal(lastImsi, resultOutputer);
		residentUserMng.setImsi(lastImsi);
		// timeSpanDataMap = new HashMap<>();
		xdrItemList = new ArrayList<>();
		curTimeSpan = 0;
		// tmpTimeSpan = -1;
		hasInit = false;
		mmeCount = 0;
		httpCount = 0;
		locCount = 0;
	}

	/**
	 * 半小时粒度进行经纬度回填
	 * 
	 * @param xdrItemList
	 *            用户全天的xdr
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public void dealEstiLoc(List<SIGNAL_LOC> xdrItemList)
	{
		int timeSpan = 1800;// 30min
		List<SIGNAL_LOC> xdrItemList_30Min = new ArrayList<SIGNAL_LOC>();
		boolean ifUpLoc = false;// 是否上报过位置
		int stime = 0;
		Collections.sort(xdrItemList);
 
		// 用户全天的数据按时间排序
		for (SIGNAL_LOC tempXdr : xdrItemList)
		{
			if (!(FormatTime.getHour(tempXdr.stime) >= 0 && FormatTime.getHour(tempXdr.stime) <= 6))
			{
				continue;// 只回填0-6点的
			}
			if (tempXdr.stime / timeSpan * timeSpan == stime)
			{
				if (tempXdr.longitude > 0 && ifUpLoc == false)
				{
					ifUpLoc = true;// 上报过位置
					xdrItemList_30Min.clear();// 清空
				}
				else if (ifUpLoc == false)
				{
					xdrItemList_30Min.add(tempXdr);
				}
			}
			else
			{
				if (residentUserMng.getImsiCellLocMap().size() > 0 && xdrItemList_30Min.size() > 0)
				{
					labelDeal.estiLabelDeal(xdrItemList_30Min, residentUserMng);
					xdrItemList_30Min.clear();
				}

				stime = tempXdr.stime / timeSpan * timeSpan;
				if (tempXdr.longitude > 0)
				{
					ifUpLoc = true;
				}
				else
				{
					ifUpLoc = false;
					xdrItemList_30Min.add(tempXdr);
				}
			}
		}
		// 最后半小时回填
		if (residentUserMng.getImsiCellLocMap().size() > 0 && xdrItemList_30Min.size() > 0)
		{
			labelDeal.estiLabelDeal(xdrItemList_30Min, residentUserMng);
			xdrItemList_30Min.clear();
		}
	}

	// 吐出用户过程数据，为了防止内存过多
	private void outDealingData(List<SIGNAL_LOC> xdrList)
	{
		userXdrList.addAll(xdrList);
	}

	private void outUserData()
	{
		dealUserData(userXdrList);
		//处理2，3G数据
		dealSample_23G(gsmTdXdrListMap);
		
		statDeal.outDealingData();
		statDeal_DT.outDealingData();
		statDeal_CQT.outDealingData();

		// 如果用户数据大于10000个，就吐出去先
		if (userInfoMng.getUserInfoMap().size() > 10000)
		{
			userInfoMng.finalStat();

			// 用户统计信息输出
			for (UserInfo userInfo : userInfoMng.getUserInfoMap().values())
			{
				try
				{
					resultOutputer.pushData(XdrLocTablesEnum.xdruserinfo.getIndex(), ResultHelper.getPutUerInfo(userInfo));
				}
				catch (Exception e)
				{
					LOGHelper.GetLogger().writeLog(LogType.error,"XdrLocDeal.outUserData error", "XdrLocDeal.outUserData error ", e);
				}
			}
			userInfoMng.getUserInfoMap().clear();
			// 用户行动信息输出
			for (UserActStat userActStat : userInfoMng.getUserActStatMap().values())
			{
				try
				{
					StringBuffer sb = new StringBuffer();
					String TabMark = "\t";
					for (UserActTime userActTime : userActStat.userActTimeMap.values())
					{
						for (UserAct userAct : userActTime.userActMap.values())
						{
							sb.delete(0, sb.length());

							sb.append(0);
							sb.append(TabMark); // cityid
							sb.append(userActStat.imsi);
							sb.append(TabMark);
							sb.append(userActStat.msisdn);
							sb.append(TabMark);
							sb.append(userActTime.stime);
							sb.append(TabMark);
							sb.append(userActTime.etime);
							sb.append(TabMark);
							sb.append(userAct.eci);
							sb.append(TabMark);
							sb.append(userAct.sn);
							sb.append(TabMark);
							sb.append(userActTime.cellcount);
							sb.append(TabMark);
							sb.append(userActTime.xdrcountTotal);
							sb.append(TabMark);
							sb.append(userActTime.duration);
							sb.append(TabMark);
							sb.append(userAct.xdrcount);
							sb.append(TabMark);
							sb.append(userAct.cellduration);
							sb.append(TabMark);
							sb.append(doubleFormat.format(userAct.durationrate));

							resultOutputer.pushData(XdrLocTablesEnum.xdruseract.getIndex(), sb.toString());
						}
					}
				}
				catch (Exception e)
				{
					LOGHelper.GetLogger().writeLog(LogType.error,"XdrLocDeal user action error", "XdrLocDeal user action error", e);
				}
			}
			userInfoMng.getUserActStatMap().clear();
			userInfoMng = new UserInfoMng();
		}

	}

	public void dealUserData(List<SIGNAL_LOC> xdrItemList)
	{
		if (xdrItemList.size() == 0)
		{
			return;
		}

		 //labelDeal.finalDeal();
		// 用户常驻小区经纬度回填
		dealEstiLoc(xdrItemList);

		TestTypeDeal testTypeDeal = new TestTypeDeal(lastImsi, labelDeal.IsDDDriver());

		testTypeDeal.deal(xdrItemList);
		
		//++
		/*List<SIGNAL_XDR_4G> addEciList = locationDayDeal.deal();// 处理一天的location数据
		if (addEciList != null) {
			xdrItemList.addAll(addEciList);
		}*/

		dealSample(xdrItemList);
	}

	// 吐出数据
	public void outAllData()
	{
		outUserData();

		statDeal.outAllData();
		statDeal_DT.outAllData();
		statDeal_CQT.outAllData();

		// 用户统计信息输出
		userInfoMng.finalStat();

		for (UserInfo userInfo : userInfoMng.getUserInfoMap().values())
		{
			try
			{
				resultOutputer.pushData(XdrLocTablesEnum.xdruserinfo.getIndex(), ResultHelper.getPutUerInfo(userInfo));
			}
			catch (Exception e)
			{
                LOGHelper.GetLogger().writeLog(LogType.error, "XdrLocDeal.pushDataError index is","XdrLocDeal.pushDataError index is: "+
                        XdrLocTablesEnum.xdruserinfo.getIndex() , e);
			}
		}
		userInfoMng.getUserInfoMap().clear();
		for (UserActStat userActStat : userInfoMng.getUserActStatMap().values())
		{
			try
			{
				StringBuffer sb = new StringBuffer();
				String TabMark = "\t";
				for (UserActTime userActTime : userActStat.userActTimeMap.values())
				{
					for (UserAct userAct : userActTime.userActMap.values())
					{
						sb.delete(0, sb.length());

						sb.append(0);
						sb.append(TabMark); // cityid
						sb.append(userActStat.imsi);
						sb.append(TabMark);
						sb.append(userActStat.msisdn);
						sb.append(TabMark);
						sb.append(userActTime.stime);
						sb.append(TabMark);
						sb.append(userActTime.etime);
						sb.append(TabMark);
						sb.append(userAct.eci);
						sb.append(TabMark);
						sb.append(userAct.sn);
						sb.append(TabMark);
						sb.append(userActTime.cellcount);
						sb.append(TabMark);
						sb.append(userActTime.xdrcountTotal);
						sb.append(TabMark);
						sb.append(userActTime.duration);
						sb.append(TabMark);
						sb.append(userAct.xdrcount);
						sb.append(TabMark);
						sb.append(userAct.cellduration);
						sb.append(TabMark);
						sb.append(doubleFormat.format(userAct.durationrate));
						resultOutputer.pushData(XdrLocTablesEnum.xdruseract.getIndex(), sb.toString());
					}
				}
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error,"user actionMap pushData error", "user actionMap pushData error index is: "+
                        XdrLocTablesEnum.xdruseract.getIndex(), e);
			}
		}
		userInfoMng.getUserActStatMap().clear();
		userInfoMng = new UserInfoMng();
	}

	/**
	 * 用户小区切换列表
	 * 
	 * @param xdrList
	 *            用户全天xdr数据
	 * @return
	 */
	public ArrayList<CellSwitInfo> getCellSwitList(List<SIGNAL_LOC> xdrList)
	{
		ArrayList<CellSwitInfo> cellSwitList = new ArrayList<CellSwitInfo>();// 这个用户的小区切换列表
		CellSwitInfo cellInfo = null;
		CellSwitInfo tempCellInfo = new CellSwitInfo();
		for (SIGNAL_LOC data : xdrList)
		{
			//SIGNAL_XDR_4G xdrItem = (SIGNAL_XDR_4G) data;
			if (data.stime <= 0)
			{
				continue;
			}
			cellInfo = new CellSwitInfo(data);
			if (cellSwitList.size() > 0)
			{
				tempCellInfo = cellSwitList.get(cellSwitList.size() - 1);// list中最后一个cellInfo;
			}
			if (cellInfo.eci / 256 == tempCellInfo.eci / 256)
			{
				tempCellInfo.etime = cellInfo.stime;
			}
			else
			{
				cellSwitList.add(cellInfo);
			}
		}
		return cellSwitList;
	}

	/**
	 * 用户在小区每个小时停留时长
	 * 
	 * @param cellSwitList
	 *            小区切换列表
	 */
	public void userCellTimePerHour(ArrayList<CellSwitInfo> cellSwitList, int dataType)
	{
		// 小时小区时间统计
		HashMap<CellTimeKey, ArrayList<ResidentUser>> userCellHourMap = new HashMap<CellTimeKey, ArrayList<ResidentUser>>();
		ArrayList<ResidentUser> tempList = null;
		ResidentUser tempResidentUser = null;
		for (CellSwitInfo cellInfo : cellSwitList)
		{
			String day = FormatTime.getDay(cellInfo.stime);
			int sdayhour = FormatTime.getHour(cellInfo.stime);
			int edayhour = FormatTime.getHour(cellInfo.etime);
			CellTimeKey key = null;
			long durTimes = 0;

			int durHour = edayhour - sdayhour;// 间隔n个小时
			for (int i = 0; i <= durHour; i++)
			{
				if (i == 0)
				{
					if (sdayhour == edayhour)
					{
						durTimes = cellInfo.etime - cellInfo.stime;// 停留时间
					}
					else
					{
						durTimes = 3600 - cellInfo.stime % 3600;
					}
				}
				else if (i == durHour)
				{
					durTimes = cellInfo.etime % 3600;
				}
				else
				{
					durTimes = 3600;
				}
				key = new CellTimeKey(cellInfo.eci, sdayhour + i);
				tempResidentUser = new ResidentUser(cellInfo, day, sdayhour + i);
				tempResidentUser.duration = durTimes;
				tempList = userCellHourMap.get(key);
				if (tempList == null)
				{
					tempList = new ArrayList<ResidentUser>();
					userCellHourMap.put(key, tempList);
				}
				tempList.add(tempResidentUser);
			}
		}
		
		// 写数据
		for (ArrayList<ResidentUser> residentUserList : userCellHourMap.values())
		{
			ResidentUser cellTime = UserResidentCluster.residentUserCluster(residentUserCellDistance, residentUserList);
			if(cellTime != null)
			{
				cellTime.getPmOrAm();
				try
				{
					resultOutputer.pushData(dataType, cellTime.ToDayLine());
				}
				catch (Exception e)
				{
					LOGHelper.GetLogger().writeLog(LogType.error,"UserResidentCellDailyWrite Err", "UserResidentCellDailyWrite Err", e);
				}
			}
		}
	}


	/**
	 * 高铁定位
	 * 
	 * @param xdrList
	 *            用户全天xdr 包括mme和http
	 */
	public void doHiRailFix(List<SIGNAL_LOC> xdrList)
	{
		ArrayList<SIGNAL_XDR_4G> allList = new ArrayList<SIGNAL_XDR_4G>();
		for (SIGNAL_LOC temp : xdrList)
		{
			SIGNAL_XDR_4G xdrItem = (SIGNAL_XDR_4G) temp;
			allList.add(xdrItem);
		}
		RailFillFunc railFillFunc = new RailFillFunc();
		railFillFunc.deal(allList);
	}

	private void dealSample_23G(HashMap<Integer,List<SIGNAL_LOC>> xdrMap)
	{
		for (int dataType : xdrMap.keySet())
		{
			List<SIGNAL_LOC> xdrList = xdrMap.get(dataType);
			Collections.sort(xdrList);
			ArrayList<CellSwitInfo> cellSwitList = getCellSwitList(xdrList);// 得到小区切换列表
			userCellTimePerHour(cellSwitList, dataType); // 得到用户每个小时在小区停留时间
		}
	}


	private void dealSample(List<SIGNAL_LOC> xdrList)
	{
		DT_Sample_4G sample = new DT_Sample_4G();
		DT_Sample_23G sample_23g = new DT_Sample_23G();
		ArrayList<CellSwitInfo> cellSwitList = getCellSwitList(xdrList);// 得到小区切换列表
		userCellTimePerHour(cellSwitList, ResidentLocTablesEnum.xdrcellhourTime.getIndex()); // 得到用户每个小时在小区停留时间
			// 前后五分钟小区切换列表
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.BA5MinCell))
		{
			BA5MinCellHelper.getBA5MinEciList(cellSwitList, xdrList);
		}
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.HiRail))// 识别高铁用户
		{
			LOGHelper.GetLogger().writeLog(LogType.info, "begin fix  HiRail ... ...");
			doHiRailFix(xdrList);
		}
		for (SIGNAL_LOC data : xdrList)
		{
			if (data.testType == StaticConfig.TestType_ERROR)
			{
				continue;
			}
			// 如果是4g数据，就按照4g的数据处理
			if (data instanceof SIGNAL_XDR_4G)
			{
				sample.Clear();
				statXdr_4G(sample, (SIGNAL_XDR_4G) data);
				statKpi_4G(sample);
				userInfoMng.stat(sample);
			}
		}
	}

	private void statKpi_4G(DT_Sample_4G sample)
	{
		statDeal.dealSample(sample);
		statDeal_DT.dealSample(sample);
		statDeal_CQT.dealSample(sample);
	}

	private void statXdr_4G(DT_Sample_4G tsam, SIGNAL_XDR_4G tTemp)
	{
		// sample
		tsam.ilongitude = tTemp.longitude;
		tsam.ilatitude = tTemp.latitude;

		tsam.ibuildingID = (tTemp.stime_ms >> 16); // 标识经纬度来源
		tsam.cityID = tTemp.cityID;
		tsam.itime = tTemp.stime;
		tsam.wtimems = (short) (tTemp.stime_ms);
		tsam.iLAC = (int) getValidData(tsam.iLAC, tTemp.TAC);
		tsam.iCI = (long) getValidData(tsam.iCI, tTemp.CI);
		tsam.Eci = (long) getValidData(tsam.Eci, tTemp.Eci);

		if (tTemp.MSISDN.length() != 0)
		{
			tsam.MSISDN = tTemp.MSISDN;
		}
		if (tTemp.IMEI.length() > 10)
		{
			tsam.UETac = tTemp.IMEI.substring(0, 7) + "0";
		}
		if (tTemp.Brand.length() != 0)
		{
			tsam.UEBrand = tTemp.Brand;
		}
		if (tTemp.Type.length() != 0)
		{
			tsam.UEType = tTemp.Type;
		}

		tsam.serviceType = tTemp.Service_Type;
		tsam.serviceSubType = tTemp.Sub_Service_Type;
		tsam.urlDomain = tTemp.Referer;
		tsam.IPDataUL = tTemp.IP_Data_Len_UL;
		tsam.IPDataDL = tTemp.IP_Data_Len_DL;
		tsam.IPPacketUL = tTemp.Count_Packet_UL;
		tsam.IPPacketDL = tTemp.Count_Packet_DL;
		// tsam.duration = tTemp.Duration;

		// result == 1 才是正常合成的事件
		tsam.IPThroughputUL = 0;
		tsam.IPThroughputDL = 0;
		tsam.duration = 0;
		if (tTemp.Result == 1)
		{
			tsam.duration = tTemp.Result_DelayFirst;
			if (tTemp.Result_DelayFirst > 0)
			{
				tsam.IPThroughputUL = (double) (tTemp.IP_Data_Len_UL * 8.0 / (tTemp.Result_DelayFirst / 1000.0)) / 1024;
				tsam.IPThroughputDL = (double) (tTemp.IP_Data_Len_DL * 8.0 / (tTemp.Result_DelayFirst / 1000.0)) / 1024;
			}
		}

		tsam.TCPReTranPacketUL = tTemp.Retran_Packet_UL;
		tsam.TCPReTranPacketDL = tTemp.Retran_Packet_DL;
		tsam.sessionRequest = 1;
		tsam.sessionResult = tTemp.Result;
		tsam.eventType = tTemp.Event_Type;

		tsam.eNBName = tTemp.ENB;
		// tsam.eNBLongitude = tTemp.longitude;
		// tsam.eNBLatitude = tTemp.latitude;
		tsam.flag = "EVT";
		if (tTemp.CI != 0 && tTemp.CI != -1000000)
		{
			tsam.ENBId = (int) (tTemp.CI / 256);
		}
		tsam.CellId = (long) getValidData(tsam.CellId, tTemp.CI);
		tsam.IMSI = (long) getValidData(tsam.IMSI, tTemp.IMSI);
		tsam.MmeCode = (int) getValidData(tsam.MmeCode, tTemp.MME_CODE);
		tsam.MmeGroupId = (int) getValidData(tsam.MmeGroupId, tTemp.MME_GROUP_ID);
		tsam.MmeUeS1apId = (Long) getValidData(tsam.MmeUeS1apId, tTemp.MME_UE_S1AP_ID);
		tsam.LocFillType = tTemp.LocFillType;

		tsam.location = tTemp.location;
		tsam.dist = tTemp.dist;
		tsam.radius = tTemp.radius;
		tsam.locType = tTemp.loctp;
		// tsam.indoor = tTemp.indoor;
		tsam.testType = tTemp.testType;

		tsam.networktype = tTemp.networktype;
		// tsam.lable = tTemp.lable;
		tsam.indoor = (int) tTemp.mt_speed;
		tsam.label = tTemp.mt_label;
		// 吐出xdr location
		try
		{
			XdrLabel xdrlocation = new XdrLabel();
			xdrlocation.cityID = tsam.cityID;
			xdrlocation.eci = tsam.Eci;
			xdrlocation.s1apid = tsam.MmeUeS1apId;
			xdrlocation.itime = tsam.itime;
			xdrlocation.imsi = tsam.IMSI;
			xdrlocation.ilongtude = tsam.ilongitude;
			xdrlocation.ilatitude = tsam.ilatitude;
			xdrlocation.testType = tsam.testType;
			xdrlocation.location = tsam.location;
			xdrlocation.dist = tsam.dist;//radius
			xdrlocation.radius = tsam.radius;
			xdrlocation.loctp = tsam.locType;
			xdrlocation.indoor = tsam.indoor;
			xdrlocation.networktype = tsam.networktype;
			xdrlocation.lable = tsam.label;
			xdrlocation.longitudeGL = tTemp.longitudeGL;
			xdrlocation.latitudeGL = tTemp.latitudeGL;
			xdrlocation.testTypeGL = tTemp.testTypeGL;
			xdrlocation.locationGL = tTemp.locationGL;
			xdrlocation.distGL = tTemp.distGL;
			xdrlocation.radiusGL = tTemp.radiusGL;
			xdrlocation.loctpGL = tTemp.loctpGL;
			xdrlocation.indoorGL = tTemp.indoorGL;
			xdrlocation.lableGL = tTemp.lableGL;
			xdrlocation.serviceType = tTemp.Service_Type;
			xdrlocation.subServiceType = tTemp.Sub_Service_Type;
			xdrlocation.moveDirect = tTemp.moveDirect;
			xdrlocation.loctimeGL = tTemp.loctimeGL;
			xdrlocation.host = tTemp.host;
			xdrlocation.wifiName = tTemp.wifiName;
		//	xdrlocation.moveDirect = tTemp.moveDirect;
			try
			{
				xdrlocation.imeiTac = Integer.parseInt(tTemp.IMEI.substring(0, 8));
			}
			catch (Exception e)
			{
                LOGHelper.GetLogger().writeLog(LogType.error, "XdrLocDeal imeiTal parse error","XdrLocDeal imeiTal parse error", e);
				xdrlocation.imeiTac = 0;
			}
			xdrlocation.eciSwitchList = tTemp.eciSwitchList.toString();
			xdrlocation.areaType = tTemp.areaType;
			xdrlocation.areaId = tTemp.areaId;
			xdrlocation.msisdn = tTemp.MSISDN;
			xdrlocation.buildingid = tTemp.buildingid;
			xdrlocation.level = tTemp.level;
			xdrlocation.imei = tTemp.IMEI;

			if (tTemp.testType == StaticConfig.TestType_HiRail)
			{
				resultOutputer.pushData(XdrLocTablesEnum.xdrhirail.getIndex(), xdrlocation.toString());
			}
			resultOutputer.pushData(XdrLocTablesEnum.xdrLocation.getIndex(), xdrlocation.toString());
			//统计用户mme,http,loc条数
			/*if (MainModel.GetInstance().getCompile().Assert(CompileMark.HaErBin))
			{
				toXdrCountStat(xdrlocation);
			}*/
			//四川指标
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.SiChuan))
			{
				try
				{
					StringBuffer tmSb = new StringBuffer();
					String spliter = ",";
					tmSb.append(tTemp.stime);
					tmSb.append(spliter);
					tmSb.append(tsam.MmeCode);
					tmSb.append(spliter);
					tmSb.append(tsam.MmeGroupId );
					tmSb.append(spliter);
					tmSb.append(tsam.MmeUeS1apId);
					tmSb.append(spliter);
					tmSb.append(tsam.ilongitude);
					tmSb.append(spliter);
					tmSb.append(tsam.ilatitude);
					tmSb.append(spliter);
					resultOutputer.pushData(XdrLocTablesEnum.xdrSiChuan.getIndex(), tmSb.toString());



				}
				catch (Exception e)
				{
                    LOGHelper.GetLogger().writeLog(LogType.error,"SiChuan.pushData error", "SiChuan.pushData error, " +
                            "xdrSiChuan.getIndex is: " +XdrLocTablesEnum.xdrSiChuan.getIndex(), e);
				}
			}

			// 最后10分钟xdrlocation
			int tmpInt = tTemp.stime / 3600 * 3600;
			if (tTemp.stime >= tmpInt + 3600 - TimeSpan)
			{
				resultOutputer.pushData(XdrLocTablesEnum.xdrLocSpan.getIndex(), xdrlocation.toString());
			}
		}
		catch (Exception e)
		{
            LOGHelper.GetLogger().writeLog(LogType.error,"iChuan.pushData error1", "SiChuan.pushData error, " +
                    "xdrLocSpan.getIndex is: "
                    +XdrLocTablesEnum.xdrLocSpan.getIndex(), e);
		}

		// Event Output
		DT_Event tEvt = new DT_Event();
		tEvt.Clear();
		tEvt.fileID = tTemp.Procedure_Status;
		tEvt.imsi = tsam.IMSI;
		tEvt.SampleID = tsam.imeiTac + 1;
		tEvt.ilongitude = tsam.ilongitude;
		tEvt.ilatitude = tsam.ilatitude;

		tEvt.cityID = tTemp.cityID;
		tEvt.projectID = 99999999;
		tEvt.itime = tTemp.stime;
		tEvt.wtimems = (short) (tTemp.stime_ms);
		tEvt.eventID = tTemp.Event_Type < 100 ? 29000 + tTemp.Event_Type : tTemp.Event_Type + 20000;
		tEvt.iLAC = tTemp.TAC;
		// tEvt.iCI = tTemp.CI;
		// 存放eci，原始ici不是标准格式
		tEvt.iCI = (long) tTemp.Eci;
		tEvt.ivalue1 = tTemp.Service_Type;
		tEvt.ivalue2 = tTemp.Sub_Service_Type;
		// tEvt.ivalue3 = tTemp.IMSI;

		tEvt.ivalue4 = 0;
		tEvt.ivalue5 = 0;
		tEvt.ivalue6 = 0;
		if (tTemp.Result == 1)
		{
			if (tTemp.Result_DelayFirst > 0)
			{
				tEvt.ivalue4 = Math.round(tTemp.IP_Data_Len_UL * 8.0 / (tTemp.Result_DelayFirst / 1000.0));
				tEvt.ivalue5 = Math.round(tTemp.IP_Data_Len_DL * 8.0 / (tTemp.Result_DelayFirst / 1000.0));
			}
			tEvt.ivalue6 = tTemp.Result_DelayFirst;
		}

		tEvt.ivalue7 = tTemp.MME_UE_S1AP_ID;
		tEvt.cqtposid = (tTemp.stime_ms >> 16); // 标识经纬度来源
		tEvt.LocFillType = tTemp.LocFillType;

		// 用带采样点的赋值
		tEvt.testType = tTemp.testType;
		tEvt.location = tTemp.location;
		tEvt.dist = tTemp.dist;
		tEvt.radius = tTemp.radius;
		tEvt.loctp = tTemp.loctp;
		tEvt.networktype = tTemp.networktype;
		if(tEvt.testType == StaticConfig.TestType_CQT)
		{
			tEvt.indoor = 1;
		}
		tEvt.label = tTemp.mt_label;
		tEvt.moveDirect = tTemp.moveDirect;

		// 低速率事件
		if ((tEvt.ivalue5 < 1024 * 1024) && ((tEvt.ivalue5 / 8.0 * (tEvt.ivalue6 / 1000.0)) > 1 * 1024 * 1024))
		{
			tEvt.SampleID += 1;
			tEvt.eventID = 28001;

			outEvent(tEvt);
		}
		else
		{
			outEvent(tEvt);
		}

	}

	private void toXdrCountStat(XdrLabel xdrlocation) throws Exception {
		//xdr原始数据分析
		StringBuffer userInfo = new StringBuffer();
		userInfo.append(xdrlocation.cityID);
		userInfo.append(StaticConfig.DataSliper3);
		userInfo.append(FormatTime.getRoundDayTime(xdrlocation.itime));
		userInfo.append(StaticConfig.DataSliper3);
		userInfo.append(xdrlocation.imsi);
		userInfo.append(StaticConfig.DataSliper3);
		userInfo.append(xdrlocation.msisdn);
		userInfo.append(StaticConfig.DataSliper3);
		userInfo.append(mmeCount);
		userInfo.append(StaticConfig.DataSliper3);
		userInfo.append(httpCount);
		userInfo.append(StaticConfig.DataSliper3);
		userInfo.append(locCount);
		userInfo.append(StaticConfig.DataSliper3);
		userInfo.append(0);
		resultOutputer.pushData(XdrLocTablesEnum.xdrCountStat.getIndex(), userInfo.toString());
	}

	private void outEvent(DT_Event tEvt)
	{
		try
		{
			// 特殊用户吐出全量的event
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.BeiJing) && SpecialUserConfig.GetInstance().ifSpeciUser(tEvt.imsi, false))
			{
				resultOutputer.pushData(XdrLocTablesEnum.xdrevtVap.getIndex(), ResultHelper.getPutLteEvent(tEvt));
			}

			if (tEvt.testType == StaticConfig.TestType_DT)
			{
				resultOutputer.pushData(XdrLocTablesEnum.xdreventdt.getIndex(), ResultHelper.getPutLteEvent(tEvt));
			}
			else if (tEvt.testType == StaticConfig.TestType_DT_EX || tEvt.testType == StaticConfig.TestType_CPE)
			{
				resultOutputer.pushData(XdrLocTablesEnum.xdreventdtex.getIndex(), ResultHelper.getPutLteEvent(tEvt));
			}
			else if (tEvt.testType == StaticConfig.TestType_CQT)
			{
				resultOutputer.pushData(XdrLocTablesEnum.xdreventcqt.getIndex(), ResultHelper.getPutLteEvent(tEvt));
			}
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error,"outEvent error", "outEvent error ", e);
		}

	}

	private Object getValidData(Object srcData, Object tarData)
	{
		if (tarData instanceof Integer)
		{
			if ((Integer) tarData != 0 && (Integer) tarData != StaticConfig.Int_Abnormal)
			{
				return tarData;
			}
			return srcData;
		}
		else if (tarData instanceof Long)
		{
			if ((Long) tarData != 0 && (Long) tarData != StaticConfig.Long_Abnormal)
			{
				return tarData;
			}
			return srcData;
		}
		return srcData;
	}

	private void statXdr_23G(DT_Sample_23G tsam, SIGNAL_XDR_23G tTemp)
	{
		// sample
		tsam.ilongitude = tTemp.longitude;
		tsam.ilatitude = tTemp.latitude;
		tsam.ibuildingID = (tTemp.stime_ms >> 16); // 标识经纬度来源
		tsam.cityID = tTemp.cityID;
		tsam.itime = tTemp.stime;
		tsam.wtimems = (short) (tTemp.stime_ms);
		tsam.iLAC = (int) getValidData(tsam.iLAC, tTemp.lac);
		tsam.iCI = (int) getValidData(tsam.iCI, tTemp.ci);
		tsam.eventType = tTemp.eventType;
		tsam.flag = "EVT";
		tsam.CellId = (int) getValidData(tsam.CellId, tTemp.ci);
		tsam.IMSI = (Long) getValidData(tsam.IMSI, tTemp.imsi);
		tsam.location = tTemp.location;
		tsam.dist = tTemp.dist;
		tsam.radius = tTemp.radius;
		tsam.loctp = tTemp.loctp;
		tsam.testType = tTemp.testType;
		tsam.networktype = tTemp.networktype;
		tsam.indoor = (int) tTemp.mt_speed;
		tsam.lable = tTemp.mt_label;
		tsam.nettypeFB = tTemp.nettype;

		// Event Output
		DT_Event tEvt = new DT_Event();
		tEvt.Clear();
		tEvt.fileID = 0;
		tEvt.imsi = tsam.IMSI;
		tEvt.SampleID = tsam.SampleID + 1;
		tEvt.ilongitude = tsam.ilongitude;
		tEvt.ilatitude = tsam.ilatitude;

		tEvt.cityID = tTemp.cityID;
		tEvt.projectID = 99999999;
		tEvt.itime = tTemp.stime;
		tEvt.wtimems = (short) (tTemp.stime_ms);
		tEvt.eventID = tTemp.eventType < 100 ? 29000 + tTemp.eventType : tTemp.eventType + 20000;
		tEvt.iLAC = tTemp.lac;
		tEvt.iCI = tTemp.ci;
		tEvt.ivalue1 = tTemp.nettype;
		tEvt.ivalue2 = tTemp.lockNetMark;

		// 用带采样点的赋值
		tEvt.testType = tTemp.testType;
		tEvt.location = tTemp.location;
		tEvt.dist = tTemp.dist;
		tEvt.radius = tTemp.radius;
		tEvt.loctp = tTemp.loctp;
		tEvt.networktype = tTemp.uenettype;
		tEvt.indoor = (int) tTemp.mt_speed;
		tEvt.label = tTemp.mt_label;
		tEvt.moveDirect = tTemp.moveDirect;

		outEvent_23G(tEvt);
	}

	private void outEvent_23G(DT_Event tEvt)
	{
		try
		{
		
			if (tEvt.testType == StaticConfig.TestType_DT)
			{
				resultOutputer.pushData(XdrLocTablesEnum.xdreventdt23g.getIndex(), ResultHelper.getPutLteEvent(tEvt));
			}
			else if (tEvt.testType == StaticConfig.TestType_DT_EX)
			{
				resultOutputer.pushData(XdrLocTablesEnum.xdreventdtex23g.getIndex(), ResultHelper.getPutLteEvent(tEvt));
			}
			else if (tEvt.testType == StaticConfig.TestType_CQT)
			{
				resultOutputer.pushData(XdrLocTablesEnum.xdreventcqt23g.getIndex(), ResultHelper.getPutLteEvent(tEvt));
			}
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error,"outEvent error1", "outEvent error ", e);
		}

	}

	private int checkDataSource(String value, SIGNAL_XDR_4G xdrItem)
	{
		// 保留原始数据
		xdrItem.valStr = new String(value.toString());

		// 统计算是有经纬度的点个数
		if (xdrItem.Eci > 0 && xdrItem.longitude > 0)
		{
			lteXdrLocCount++;

			MyInt myTemp = cellLocDic.get(xdrItem.Eci);
			if (myTemp == null)
			{
				myTemp = new MyInt(0);
				cellLocDic.put(xdrItem.Eci, myTemp);
			}
			myTemp.data++;
		}

		// dd司机判断
		if (xdrItem.location == 3)
		{
			labelDeal.setDDDriver(true);
		}

		// =============================采样点有效性判断=============================================

		if (xdrItem.IMSI <= 0 || xdrItem.Eci <= 0 || xdrItem.stime <= 0)
		{
			return StaticConfig.EXCEPTION;
		}

		LteCellInfo lteCellInfo = CellConfig.GetInstance().getLteCell(xdrItem.Eci);
		int maxRadius = 3000;
		if (lteCellInfo != null)
		{
			if (xdrItem.longitude > 0 && xdrItem.latitude > 0 && lteCellInfo.ilongitude > 0 && lteCellInfo.ilatitude > 0)
			{
				xdrItem.dist = (long) GisFunction.GetDistance(xdrItem.longitude, xdrItem.latitude, lteCellInfo.ilongitude, lteCellInfo.ilatitude);
			}
			if (3 * lteCellInfo.radius <= 1000)// 没有理想覆盖的数据
			{
				maxRadius = 1000;
			}
			else if (3 * lteCellInfo.radius >= 3000)
			{
				maxRadius = 3000;
			}
			else
			{
				maxRadius = 3 * lteCellInfo.radius;
			}
			xdrItem.cityID = lteCellInfo.cityid;
		}

		if (xdrItem.cityID < 0)
		{
			eciErrCount++;
		}

		if (xdrItem.dist >= maxRadius && xdrItem.longitude > 0)
		{
			locErrCount++;
			xdrItem.cleanLoc();
		}

		if (xdrItem.dist < 0 && xdrItem.longitude > 0)
		{
			locErrCount++;
		}

		// 如果是location > 7 属于不可预知的情况,去掉
		if (xdrItem.location > 7)
		{
			return StaticConfig.EXCEPTION;
		}

		if (xdrItem.longitude > 0)
		{
			if (xdrItem.location == ELocationMark.BaiDuSDK.getValue())
			{
				if (xdrItem.radius > 100)
				{
					radiusErrCount++;
				}
			}
			else if (xdrItem.location == ELocationMark.GaoDeSDK.getValue())
			{
				if (xdrItem.radius > 100)
				{
					radiusErrCount++;
				}
			}
		}
		// =============================采样点有效性判断=============================================

		if (curTimeSpan == 0)
		{
			curTimeSpan = xdrItem.stime / TimeSpan * TimeSpan;
		}
		xdrItemList.add(xdrItem);
		return StaticConfig.SUCCESS;
	}

	@Override
	public int pushData(int dataType, String value)
	{// 10分钟
		// TODO Auto-generated method stub
		if ((dataType == DataType_XDR_MME || dataType == DataType_XDR_HTTP) && hasInit == false)
		{
			locationItemMng.finInit();
			labelItemMng.init();
			locationWFItemMng.finInit();
			hasInit = true;
		}
		if (userXdrList.size() >= 100000) // 同一个imsi mme和http数据大于100000条，则是异常用户
		{
			return StaticConfig.FAILURE;
		}
		if (dataType == DataType_RESIDENT_USER)
		{
			String[] strs = value.toString().split("\t", -1);
			ResidentUser temp = new ResidentUser();
			temp.fillMergeData(strs);
			if (temp.longitude > 0)
			{
				residentUserMng.putItem(temp);
			}
		}
		else if (dataType == DataType_LOCATION)
		{
			String[] strs = value.toString().split("\\|" + "|" + "\t", -1);
			try
			{
				LocationItem item = new LocationItem();
				if (strs[0].equals("URI") || strs[0].equals(""))
				{
					item.FillData(strs, 1);
				}
				else
				{
					item.FillData(strs, 0);
				}
				locationItemMng.AddItem(item);
				locCount++;
			}
			catch (Exception e)
			{
                LOGHelper.GetLogger().writeLog(LogType.error, "DataType_LOCATION LocationItem FillDataError","DataType_LOCATION LocationItem FillDataError value is"+
                        value.toString(), e);
			}
		}
		else if (dataType == DataType_LABEL)
		{
			String[] strs = value.toString().split(StaticConfig.DataSliper2 + "|" + "\t", -1);

			LabelItem xdrLocItem = new LabelItem();
			if (xdrLocItem.FillData(strs, 0))
			{
				labelItemMng.AddItem(xdrLocItem);
			}
		}
		else if (dataType == DataType_WIFI)
		{
			String[] strs = value.toString().split(StaticConfig.DataSliper2 + "|" + "\t", -1);
			LocationWFItem item = new LocationWFItem();
			if (item.FillData(strs, 0))
			{
				locationWFItemMng.AddItem(item);
			}
		}
		else if (dataType == DataType_XDR_MME)
		{
			mmeCount++;
			SIGNAL_XDR_4G xdrItem = new SIGNAL_XDR_4G();
			try
			{
				dataAdapterReader_MME.readData(value);
				if (!xdrItem.FillData_SEQ_MME(dataAdapterReader_MME))
				{
					return StaticConfig.EXCEPTION;
				}

				/*LocationItem locItem = locationItemMng.getLableItem(xdrItem.stime);// 找间隔时间最近的location
				if (locItem != null)
				{
					xdrItem.longitude = locItem.longitude;
					xdrItem.latitude = locItem.latitude;
					xdrItem.location = locItem.location;
					xdrItem.loctp = locItem.loctp;
					xdrItem.radius = locItem.radius;
					xdrItem.latlng_time = locItem.locTime;
					xdrItem.wifiName = locItem.wifiName;
				}
				else
				{*/
					LocationWFItem locWFItem = locationWFItemMng.getLableItem(xdrItem.stime);
					if (locWFItem != null)
					{
						xdrItem.longitude = locWFItem.longitude;
						xdrItem.latitude = locWFItem.latitude;
						xdrItem.location = ELocationMark.CMCCWLAN.getValue();
						xdrItem.loctp = ELocationType.Wifi.getName();
						xdrItem.radius = 20;
						xdrItem.latlng_time = xdrItem.stime;
						xdrItem.buildingid = locWFItem.buildid;
						xdrItem.level = locWFItem.level;
					}
//				}
				int rtncode = checkDataSource(value, xdrItem);
				if (rtncode == StaticConfig.SUCCESS) {
					//20180730 add MME的统计 由于部分地方不做事件 又需要统计MME数据
					//TODO 当所有地市都部署事件时，MME统计转移到事件模块
				}
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error,"xdrItem.FillData error", "xdrItem.FillData error : " + value.toString(), e);
				formatErrCount++;
				return StaticConfig.EXCEPTION;
			}
		}
		else if (dataType == DataType_XDR_HTTP)
		{
			httpCount++;
			SIGNAL_XDR_4G xdrItem = new SIGNAL_XDR_4G();
			try
			{
				dataAdapterReader_HTTP.readData(value);
				if (!xdrItem.FillData_SEQ_HTTP(dataAdapterReader_HTTP))
				{
					return StaticConfig.EXCEPTION;
				}
				//
				// LocationWFItem locWFItem =
				// locationWFItemMng.getLableItem(xdrItem.stime);
				// if (locWFItem != null && xdrItem.longitude <= 0)
				// {
				// xdrItem.longitude = locWFItem.longitude;
				// xdrItem.latitude = locWFItem.latitude;
				// xdrItem.location = ELocationMark.BaiDuSDK.getValue();//
				// xdrItem.loctp = ELocationType.Wifi.getName();
				// xdrItem.radius = 0;
				// xdrItem.latlng_time = xdrItem.stime;
				// }
				
				//++
				int rtncode = checkDataSource(value, xdrItem);
				if (rtncode == StaticConfig.SUCCESS && xdrItem.longitude > 0 && xdrItem.latitude > 0) {
//					locationDayDeal.add(xdrItem);
					//统计有经纬度的点
				}
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error,"xdrItem.FillData error", "xdrItem.FillData error : " + value.toString(), e);
				formatErrCount++;
				return StaticConfig.EXCEPTION;
			}
		}
		// 上批次最后的xdr location
		else if (dataType == DataType_XDR_LOC_SPAN)
		{
			String[] strs = value.toString().split("\t", -1);
			for (int i = 0; i < strs.length; ++i)
			{
				strs[i] = strs[i].trim();
			}
			XdrLocation xdrLable;
			try
			{
				xdrLable = XdrLocation.FillData(strs, 0);
				// 添加到xdr列表里面
				SIGNAL_XDR_4G xdrItem = new SIGNAL_XDR_4G();
				xdrItem.cityID = xdrLable.cityID;
				xdrItem.Eci = xdrLable.eci;
				xdrItem.MME_UE_S1AP_ID = xdrLable.s1apid;
				xdrItem.stime = xdrLable.itime;
				xdrItem.stime_ms = 0;
				xdrItem.etime = xdrLable.itime;
				xdrItem.IMSI = xdrLable.imsi;
				xdrItem.longitude = xdrLable.ilongtude;
				xdrItem.latitude = xdrLable.ilatitude;
				xdrItem.testType = xdrLable.testType;
				xdrItem.location = xdrLable.location;
				xdrItem.dist = xdrLable.dist;
				xdrItem.radius = xdrLable.radius;
				xdrItem.loctp = xdrLable.loctp;
				xdrItem.indoor = xdrLable.indoor;
				xdrItem.networktype = xdrLable.networktype;
				xdrItem.lable = xdrLable.lable;
				xdrItem.Service_Type = xdrLable.serviceType;
				xdrItem.Sub_Service_Type = xdrLable.subServiceType;
				xdrItem.moveDirect = xdrLable.moveDirect;
				xdrItem.host = xdrLable.host;
				xdrItem.wifiName = xdrLable.wifiName;

				xdrItemList.add(xdrItem);
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error,"XdrLable.FillData error", "XdrLable.FillData error :" + value, e);
				return StaticConfig.EXCEPTION;
			}
		}
		else if (dataType == DataType_XDR_GSM)
		{
			fillGsmTdXdrData(value, ResidentLocTablesEnum.xdrcellhourTime_gsm.getIndex());
		}
		else if (dataType == DataType_XDR_TD)
		{
			fillGsmTdXdrData(value, ResidentLocTablesEnum.xdrcellhourTime_td.getIndex());
		}
		return StaticConfig.SUCCESS;
	}

	private void fillGsmTdXdrData(String value, int dataType_xdr) {
		SIGNAL_XDR_23G xdrItem = new SIGNAL_XDR_23G();
		String[] values = value.split(StaticConfig.DataSliper3);
		if (xdrItem.fillData(values)) {
			ArrayList<SIGNAL_LOC> gsmXdrList = null;
			if (gsmTdXdrListMap.get(dataType_xdr) == null) {
				gsmXdrList = new ArrayList<>();
				gsmTdXdrListMap.put(dataType_xdr, gsmXdrList);
			}
			gsmXdrList.add(xdrItem);
		}
	}

	@Override
	public void statData()
	{// 10分钟

		// TODO Auto-generated method stub
		// 基于gps信息自增相关的xdr
		List<LocationItem> locItemList = locationItemMng.getLocSpan(curTimeSpan);
		//++
//		List<LocationItem> locItemList = locationItemMng.getLocList();
		if (locItemList != null)
		{
			for (LocationItem locItem : locItemList)// 把location中的位置信息保存
			{
				SIGNAL_XDR_4G xdrItem = new SIGNAL_XDR_4G();
				xdrItem.IMSI = locItem.imsi;
				xdrItem.stime = locItem.locTime;
				xdrItem.Eci = locItem.eci;
				xdrItem.Event_Type = 100;

				xdrItem.longitude = locItem.longitude;
				xdrItem.latitude = locItem.latitude;
				xdrItem.location = locItem.location;
				xdrItem.loctp = locItem.loctp;
				xdrItem.radius = locItem.radius;
				xdrItem.latlng_time = locItem.locTime;
				xdrItem.wifiName = locItem.wifiName;
				xdrItem.location2 = locItem.location2;

				xdrItemList.add(xdrItem);
				
				//++
			//	locationDayDeal.add(xdrItem);
			}
		}
		labelDeal.deal(xdrItemList);
		outDealingData(xdrItemList);
		xdrItemList.clear();
		// 10分钟的初始化
		hasInit = false;
		curTimeSpan = 0;
		labelItemMng = new LabelItemMng();
	}

	@Override
	public void outData()
	{
		// TODO Auto-generated method stub
		// LOGHelper.GetLogger().writeLog(LogType.info, "user xdr count info : "
		// + key.getImsi() + " " + userXdrList.size());
		// outUserData();
	}

}
