package cn.mastercom.bigdata.mro.loc;

import cn.mastercom.MrLocationPredict;
import cn.mastercom.bigdata.StructData.*;
import cn.mastercom.bigdata.conf.cellconfig.CellConfig;
import cn.mastercom.bigdata.conf.cellconfig.LteCellInfo;
import cn.mastercom.bigdata.conf.config.*;
import cn.mastercom.bigdata.evt.locall.stat.*;
import cn.mastercom.bigdata.loc.hsr.HSRConfig;
import cn.mastercom.bigdata.loc.hsr.LocFunc;
import cn.mastercom.bigdata.mro.loc.UserActStat.UserActTime;
import cn.mastercom.bigdata.mro.loc.UserActStat.UserCell;
import cn.mastercom.bigdata.mro.loc.UserActStat.UserCellAll;
import cn.mastercom.bigdata.mro.stat.UserMrStat;
import cn.mastercom.bigdata.mro.stat.tableEnum.MroCsOTTTableEnum;
import cn.mastercom.bigdata.mroxdrmerge.AppConfig;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.stat.userResident.homeBroadbandLoc.NewResidentUser;
import cn.mastercom.bigdata.util.DataAdapterConf.ParseItem;
import cn.mastercom.bigdata.util.*;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.xdr.loc.ResultHelper;
import cn.mastercom.exception.ModelNotTrainedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class MroXdrDeal implements IDataDeal
{
	public static final int DataType_XDRLOCATION = 0;
	public static final int DataType_MRO_MT = 1;
	public static final int DataType_MRO = 2;
	public static final int DataType_MDT_IMM = 3;
	public static final int DataType_MDT_LOG = 4;
	public static final int DataType_CELLBUILD = 5;
	public static final int DataType_RESIDENTUSER = 6;
	private ResultOutputer resultOutputer;
	public UserMrStat userStat;
	public DataStater dataStater = null;
	private CellBuildInfo cellBuild;
	private CellBuildWifi cellBuildWifi;
	protected static final Log LOG = LogFactory.getLog(MroXdrDeal.class);
	private final int TimeSpan = 600;// 10分钟间隔
	private final int TimeSpanHour = 3600;// 1小时间隔

	private StatDeal statDeal;
	private StatDeal_DT statDeal_DT;
	private StatDeal_CQT statDeal_CQT;

	private XdrLabelMng xdrLabelMng;
	private UserActStatMng userActStatMng;
	private cn.mastercom.bigdata.locuser_v3.UserLocer userLocer3;
	public FigureFixedOutput figureMroFix;// 指纹库定位结果输出

	private int IndoorGridSize = 0;
	private int OutdoorGridSize = 0;
	LteCellInfo cellInfo;
	private long currEci = 0;
	private long tempEci = -1;
	private int curTimeSpan = -1;
//	private CellTimeKey tempKey;

	private ParseItem parseItem;
	private DataAdapterReader dataAdapterReader;
	// private CellTimeKey key;
	List<SIGNAL_MR_All> allMroItemList = null;
	HashMap<String, ArrayList<MroOrigDataMT>> map = new HashMap<String, ArrayList<MroOrigDataMT>>();
	ParseItem parseItem_IMM;
	DataAdapterReader dataAdapterReader_IMM;
	DataAdapterReader dataAdapterReader_LOG;
	ParseItem parseItem_LOG;
	Configuration conf;
	//是否被初始化过
	boolean hasInit;

	List<SIGNAL_MR_All> allMdtItemList = null;

	int packet = 0;
	int current5Sec = -1;
	int previous5Sec = -1;
	boolean[] dataTypeSwitch = null;

	HSRConfig hsrConfig;
	private HashMap<Long, ArrayList<NewResidentUser>> residentUserMap = new HashMap<>();

	private MrLocationPredict mrLocationPredict = null;

	/**
	 * @param resultOutputer
	 */
	public MroXdrDeal(ResultOutputer resultOutputer)
	{
		this.resultOutputer = resultOutputer;
		statDeal = new StatDeal(resultOutputer);
		statDeal_DT = new StatDeal_DT(resultOutputer);
		statDeal_CQT = new StatDeal_CQT(resultOutputer);
		userActStatMng = new UserActStatMng();
		dataStater = new DataStater(resultOutputer);
		// key = new CellTimeKey();
		try
		{
			this.IndoorGridSize = Integer.parseInt(MainModel.GetInstance().getAppConfig().getInDoorSize());
			this.OutdoorGridSize = Integer.parseInt(MainModel.GetInstance().getAppConfig().getOutDoorSize());
			this.parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MRO-SRC");
			if (parseItem == null)
			{
				throw new IOException("parse item do not get.");
			}
			this.parseItem.resortColumNamePos();
			this.dataAdapterReader = new DataAdapterReader(parseItem);

			conf = MainModel.GetInstance().getConf();
			figureMroFix = new FigureFixedOutput(conf, resultOutputer);
			figureMroFix.setup();

			String date = conf.get("mapreduce.job.date").replace("01_", "");
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyMMdd");
			Date thestrTime = null;
			try
			{
				thestrTime = simpleDateFormat.parse(date.substring(0,6));
				XdrLocallexDeal2.ROUND_dAY_TIME = (int)(thestrTime.getTime()/1000L);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			// 初始化小区的信息
			if (!CellConfig.GetInstance().loadLteCell(conf))
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "cellconfig init error 请检查！");
			}
			// 初始化imei表
			if (!ImeiConfig.GetInstance().loadImeiCapbility(conf))
			{
				LOGHelper.GetLogger().writeLog(LogType.info, "imeiconfig  init error 请检查！");
			}

			// 加载特例用户
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.BeiJing))
			{
				if (!FilterCellConfig.GetInstance().loadSceneCell(conf))
				{
					LOGHelper.GetLogger().writeLog(LogType.error, "subwayUser init error 请检查！");
				}
				
				if (!SpecialUserConfig.GetInstance().loadSpecialuser(conf, true))
				{
					LOGHelper.GetLogger().writeLog(LogType.error, "specialUser init error 请检查！");
				}
			}

			// 20171129 add 加载高铁配置
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.HiRail2))
			{
				AppConfig appConfig = MainModel.GetInstance().getAppConfig();
				String baseOutpath = appConfig.getMroXdrMergePath();
				hsrConfig = HSRConfig.GetInstance();

				String dateTime = date;
				/*if(MainModel.GetInstance().getCompile().Assert(CompileMark.GanSu)){
					//甘肃每三个小时计算一次高铁配置，只能用第三个小时的配置：18052416 -> 180524  16
					Date curHour = TimeUtil.parse(dateTime, "yyMMddHH");

					dateTime = TimeUtil.format(HSRUserAnaDeal.dateToDeal(curHour), "yyMMddHH");
				}*/

				if (!hsrConfig.initDailyCfg(conf, baseOutpath, dateTime)
						||
					!hsrConfig.initGerneralCfg(conf, appConfig.getHsrSegmentPath(), appConfig.getHsrSectionPath(), appConfig.getHsrSectionCellPath(), appConfig.getHsrStationCellPath(), appConfig.getHsrStationPath())
					)
				{
					LOGHelper.GetLogger().writeLog(LogType.error, "高铁车次配置init error 请检查！");
					hsrConfig = null;
				}
			}
			// 加载指定小区配置
			String filePath = "";
		 	filePath = MainModel.GetInstance().getAppConfig().getSpecifiedCellPath();
			FilterCellConfig.GetInstance().loadSpecifiedCell(conf, filePath);
			// fgottstat
			// typeResult = new ResultOutputer(typeInfoMng);
			// userStat = new UserMrStat(typeResult);
			userStat = new UserMrStat(resultOutputer);
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeDetailLog(LogType.error,"MroXdrDeal init error", "MroXdrDeal init error: ", e);
			e.printStackTrace();
		}
		// 打印状态日志
		LOGHelper.GetLogger().writeLog(LogType.info, "cellconfig init count is : " + CellConfig.GetInstance()
                .getLteCellInfoMap().size());
	}

	public void init(CellTimeKey key)
	{
		try
		{
			// 初始化小区楼宇表
			if (key.getEci() != tempEci)
			{
				residentUserMap.clear();
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.BuildingPredict)){
					try{
						mrLocationPredict = new MrLocationPredict(key.getEci(), MainModel.GetInstance().getAppConfig().getMrLocationPredictPath(), conf);

					} catch (IllegalArgumentException e) {
						mrLocationPredict = null;
						LOGHelper.GetLogger().writeLog(LogType.error,"load mrLocationPredict error", "load mrLocationPredict error :" + key.getEci
								(), e);
					}
				}

				cellInfo = CellConfig.GetInstance().getLteCell(key.getEci());
				if (cellInfo == null)// cell统计需要全量，不能抛弃
				{
					cellInfo = new LteCellInfo();
					LOGHelper.GetLogger().writeLog(LogType.info,
							"gongcansize:" + CellConfig.GetInstance().getlteCellInfoMapSize() + "  gongcan no eci:" + key.getEci() + "  enbid:" + key.getEci() / 256 + " cellid:" + key.getEci() % 256);
				}
				//加载邻区配置
				CellConfig.GetInstance().loadNcCell(key.getEci());
				// 初始化小区楼宇表
				cellBuild = new CellBuildInfo();
                cellBuild.setCellInfo(cellInfo);
				if (!MainModel.GetInstance().getCompile().Assert(CompileMark.ChongQing) && !cellBuild.loadCellBuild(conf, key.getEci(), cellInfo.cityid)) 
				{
					LOGHelper.GetLogger().writeLog(LogType.error, "cellbuild  init error 请检查！eci:" + key.getEci());
				}
	
				// 初始化小区楼宇wifi
				cellBuildWifi = new CellBuildWifi();
				if (!cellBuildWifi.loadBuildWifi(conf, key.getEci(), cellInfo.cityid))
				{
					LOGHelper.GetLogger().writeLog(LogType.error, "cellbuildwifi  init error 请检查！eci:" + key.getEci() + " map.size:" + cellBuildWifi.getCellBuildWifiMap().size());
				}

				
			}
			
		
		}catch (Exception e)
		{
			LOGHelper.GetLogger().writeDetailLog(LogType.error, "load config error","load config error :" + key.getEci(), e);
			e.printStackTrace();
		}
		tempEci = key.getEci();
		curTimeSpan = key.getTimeSpan();

		packet = 0;
		dataTypeSwitch = new boolean[10];
		xdrLabelMng = new XdrLabelMng();
		allMdtItemList = new ArrayList<SIGNAL_MR_All>();
		allMroItemList = new ArrayList<>();
		hasInit = false;
		current5Sec = -1;
		previous5Sec = -1;
	}

	/**
	 * 给每个SIGNAL_MR_ALL的samState赋值 以区分室内外采样点
	 * @param mroItemList
	 */
	public void getInOrOut(List<SIGNAL_MR_All> mroItemList)
	{
		for (SIGNAL_MR_All mrAll : mroItemList)
		{
			if (mrAll.tsc.longitude <= 0)// 没有关联上
			{
				continue;
			}
			if (mrAll.ibuildingID > 0)// 已有楼宇id和楼层
			{
				mrAll.samState = StaticConfig.ACTTYPE_IN;
				continue;
			}
			if (mrAll.testType != StaticConfig.TestType_CQT)
			{
				mrAll.samState = StaticConfig.ACTTYPE_OUT;
				continue;
			}
			try
			{
				List<Integer> buildIdList = cellBuild.getBuildIds(mrAll.tsc.longitude, mrAll.tsc.latitude);
				if (buildIdList != null && !buildIdList.isEmpty())
				{
					mrAll.samState = StaticConfig.ACTTYPE_IN;
					if (cellBuildWifi.getCellBuildWifiMap() != null && !cellBuildWifi.getCellBuildWifiMap().isEmpty())
					{
						short iheight = -1;
						for(int i = 0; i < buildIdList.size(); i++)
						{
							iheight = (short) WifiFixed.returnLevel(cellBuildWifi, mrAll.tsc.UserLabel, buildIdList.get(i));
							if (iheight >= 0) 
							{ // 群楼情况,一个采样点无法判断在那栋楼,此时build包含多个,结合楼宇高度可以确定唯一的楼宇id
								mrAll.ibuildingID = buildIdList.get(i);
								mrAll.iheight = iheight;
								break;
							}
						}
						if (mrAll.ibuildingID <= 0)
						{
							int pos = Math.abs((mrAll.tsc.longitude+"_"+mrAll.tsc.latitude).hashCode()) % buildIdList.size();
							mrAll.ibuildingID = buildIdList.get(pos);
							mrAll.iheight = -1;
						}
					}
					else
					{
						int pos = Math.abs((mrAll.tsc.longitude+"_"+mrAll.tsc.latitude).hashCode()) % buildIdList.size();
						mrAll.ibuildingID = buildIdList.get(pos);
						mrAll.iheight = -1;
					}
				}
				else
				{
					mrAll.samState = StaticConfig.ACTTYPE_OUT;
				}
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
	}
	
	public void getPosition(SIGNAL_MR_All mrAll)
	{
		ArrayList<LteCellInfo> NcCellList = new ArrayList<>();
		for (NC_LTE item : mrAll.tlte)
		{
			if (item.LteNcRSRP==StaticConfig.Int_Abnormal || NcCellList.size()>= 2)
			{
				break;//邻区为空，后续都为空
			}
			LteCellInfo lteCellInfo = CellConfig.GetInstance().getNearestCell(cellInfo.cityid, item.LteNcEarfcn, item.LteNcPci);
			if(lteCellInfo != null)
			{
				if (lteCellInfo.indoor == StaticConfig.INCOVER)
				{
					continue;
				}
				else
				{
					if (mrAll.tsc.LteScRSRP - item.LteNcRSRP > 10)//差值10DB
					{
						break;
					}
					NcCellList.add(lteCellInfo);
				}
			}
		}
		dealNcCellInfo(NcCellList, mrAll);
	}
	
	public void dealNcCellInfo(ArrayList<LteCellInfo> NcCellList, SIGNAL_MR_All mrAll)
	{
		int position = 0;
		String centerLngLat = cellBuild.getCenterLngLat(mrAll.ibuildingID);
		if (centerLngLat != null && NcCellList.size() > 0)
		{
			String[] value = centerLngLat.split("_");
			int centerLongitude = Integer.parseInt(value[0]);
			int centerLatitude = Integer.parseInt(value[1]);
			for (int i = 0; i < NcCellList.size(); i++)
			{
				int tempPos = GisFunction.GetDirection(centerLongitude, centerLatitude, NcCellList.get(i).ilongitude, NcCellList.get(i).ilatitude);
				position = Func.getDirection(tempPos, position);
				if (i == 1)
				{
					position = Func.getDirection(position, tempPos);
				}
			}
			mrAll.position = position;
		}
	}
	
	public void outPutLocLib(List<SIGNAL_MR_All> MrAll_List)
	{
		for (SIGNAL_MR_All item : MrAll_List)
		{
			if (item.tsc.longitude <= 0)
			{
				continue;
			}
			LocItem loclibItem = new LocItem();
			loclibItem.cityID = item.tsc.cityID;
			loclibItem.itime = item.tsc.beginTime;
			loclibItem.wtimems = (short) item.tsc.beginTimems;
			loclibItem.imsi = item.tsc.IMSI;
			loclibItem.ilongitude = item.tsc.longitude;
			loclibItem.ilatitude = item.tsc.latitude;
			loclibItem.ibuildid = item.ibuildingID;
			loclibItem.iheight = item.iheight;
			loclibItem.testType = item.testType;
			loclibItem.doorType = item.samState;
			loclibItem.radius = item.radius;
			loclibItem.loctp = item.locType;
			loclibItem.label = item.label;
			loclibItem.iAreaType = item.areaType;
			loclibItem.iAreaID = item.areaId;
			loclibItem.locSource = item.locSource;
			loclibItem.lteScRSRP = item.tsc.LteScRSRP;
			loclibItem.lteScSinrUL = item.tsc.LteScSinrUL;
			loclibItem.eci = item.tsc.Eci;
			loclibItem.confidentType = item.ConfidenceType;
			loclibItem.msisdn = item.tsc.Msisdn;
			// yzx add 2017.10.24
			loclibItem.s1apid = item.tsc.MmeUeS1apId;
			// direction
			loclibItem.position = item.position;
			try
			{
				resultOutputer.pushData(MroCsOTTTableEnum.mrloclib.getIndex(), loclibItem.toString());
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error,"create mr loclib", "create mr loclib", e);
			}
		}
	}

	public String outPutLocLib(XdrLabel xdrlocation)
	{
		LocItem loclibItem = new LocItem();
		loclibItem.cityID = xdrlocation.cityID;
		loclibItem.itime = xdrlocation.itime;
		loclibItem.wtimems = 0;
		loclibItem.imsi = xdrlocation.imsi;
		loclibItem.ilongitude = xdrlocation.longitudeGL;
		loclibItem.ilatitude = xdrlocation.latitudeGL;
		loclibItem.testType = xdrlocation.testType;
		if (loclibItem.testType != StaticConfig.TestType_CQT)
		{
			loclibItem.doorType = StaticConfig.ACTTYPE_OUT;
		}
		else
		{
			List<Integer> buildIdList = cellBuild.getBuildIds(xdrlocation.longitudeGL, xdrlocation.latitudeGL);
			if (buildIdList != null && !buildIdList.isEmpty())
			{
				loclibItem.doorType = StaticConfig.ACTTYPE_IN;
				if (cellBuildWifi.getCellBuildWifiMap() != null && !cellBuildWifi.getCellBuildWifiMap().isEmpty())
				{
					short iheight = -1;
					for(int i = 0; i < buildIdList.size(); i++)
					{
						iheight = (short) WifiFixed.returnLevel(cellBuildWifi, xdrlocation.wifiName, buildIdList.get(i));
						if (iheight >= 0) 
						{ // 群楼情况,一个采样点无法判断在那栋楼,此时build包含多个,结合楼宇高度可以确定唯一的楼宇id
							loclibItem.ibuildid = buildIdList.get(i);
							loclibItem.iheight = iheight;
							break;
						}
					}
					if (loclibItem.ibuildid <= 0)
					{
						int pos = Math.abs((xdrlocation.longitudeGL+"_"+xdrlocation.latitudeGL).hashCode()) % buildIdList.size();
						loclibItem.ibuildid = buildIdList.get(pos);
						loclibItem.iheight = -1;
					}
				}
				else
				{
					int pos = Math.abs((xdrlocation.longitudeGL+"_"+xdrlocation.latitudeGL).hashCode()) % buildIdList.size();
					loclibItem.ibuildid = buildIdList.get(pos);
					loclibItem.iheight = -1;
				}
			}
			else
			{
				loclibItem.doorType = StaticConfig.ACTTYPE_OUT;
			}
		}
		loclibItem.radius = xdrlocation.radius;
		loclibItem.loctp = xdrlocation.loctp;
		loclibItem.label = xdrlocation.lable;
		loclibItem.iAreaType = xdrlocation.areaType;
		loclibItem.iAreaID = xdrlocation.areaId;
		loclibItem.locSource = Func.getLocSource(xdrlocation.loctp);
		loclibItem.lteScRSRP = -1000000;
		loclibItem.lteScSinrUL = -1000000;
		loclibItem.eci = xdrlocation.eci;
		loclibItem.confidentType = Func.getSampleConfidentType(xdrlocation.loctp, loclibItem.doorType, xdrlocation.testType);
		// yzx add 2017.10.24
		loclibItem.s1apid = xdrlocation.s1apid;
		loclibItem.msisdn = xdrlocation.msisdn;
		return loclibItem.toString();
	}

	// 吐出用户过程数据，为了防止内存过多
	private void outDealingData()
	{
		// 天数据吐出/////////////////////////////////////////////////////////////////////////////////////
		statDeal.outDealingData();
		statDeal_DT.outDealingData();
		statDeal_CQT.outDealingData();

		// 如果用户数据大于10000个，就吐出去先
		if (userActStatMng.getUserActStatMap().size() > 10000)
		{
			userActStatMng.finalStat();
			// 用户行动信息输出
			StringBuffer sb = new StringBuffer();
			for (UserActStat userActStat : userActStatMng.getUserActStatMap().values())
			{
				try
				{
					sb.delete(0, sb.length());
					String TabMark = "\t";
					for (UserActTime userActTime : userActStat.userActTimeMap.values())
					{
						for (UserCellAll userActAll : userActTime.userCellAllMap.values())
						{
							sb.delete(0, sb.length());
							sb.append(0);// cityid
							sb.append(TabMark);
							sb.append(userActStat.imsi);
							sb.append(TabMark);
							sb.append(userActStat.msisdn);
							sb.append(TabMark);
							sb.append(userActTime.stime);
							sb.append(TabMark);
							sb.append(userActTime.etime);
							sb.append(TabMark);
							// 主服小区
							UserCell mainUserCell = userActAll.getMainUserCell();
							sb.append(userActAll.eci);
							sb.append(TabMark);
							sb.append(0);
							sb.append(TabMark);
							sb.append(userActAll.eci);
							sb.append(TabMark);
							sb.append(mainUserCell.rsrpSum);
							sb.append(TabMark);
							sb.append(mainUserCell.rsrpTotal);
							sb.append(TabMark);
							sb.append(mainUserCell.rsrpMaxMark);
							sb.append(TabMark);
							sb.append(mainUserCell.rsrpMinMark);
							resultOutputer.pushData(MroCsOTTTableEnum.useractcell.getIndex(), sb.toString());
							// 邻区
							List<UserCell> userCellList = userActAll.getUserCellList();
							int sn = 1;
							for (UserCell userCell : userCellList)
							{
								if (userCell.eci == userActAll.eci)
								{
									continue;
								}
								sb.delete(0, sb.length());
								sb.append(0);// cityid
								sb.append(TabMark);
								sb.append(userActStat.imsi);
								sb.append(TabMark);
								sb.append(userActStat.msisdn);
								sb.append(TabMark);
								sb.append(userActTime.stime);
								sb.append(TabMark);
								sb.append(userActTime.etime);
								sb.append(TabMark);

								sb.append(userActAll.eci);
								sb.append(TabMark);
								sb.append(sn);
								sb.append(TabMark);
								sb.append(userCell.eci);
								sb.append(TabMark);
								sb.append(userCell.rsrpSum);
								sb.append(TabMark);
								sb.append(userCell.rsrpTotal);
								sb.append(TabMark);
								sb.append(userCell.rsrpMaxMark);
								sb.append(TabMark);
								sb.append(userCell.rsrpMinMark);
								resultOutputer.pushData(MroCsOTTTableEnum.useractcell.getIndex(), sb.toString());
								sn++;
							}

						}
					}
				}
				catch (Exception e)
				{
					LOGHelper.GetLogger().writeLog(LogType.error,"user action error", "user action error", e);
				}
			}

			userActStatMng = new UserActStatMng();
		}
	}

	public void outAllData()
	{
		try
		{
			userStat.outResult();
		}
		catch(Exception e)
		{
			LOGHelper.GetLogger().writeDetailLog(LogType.error,"MroXdrDeal outAllData Error", "MroXdrDeal outAllData Error: " , e);
		}

		statDeal.outAllData();
		statDeal_DT.outAllData();
		statDeal_CQT.outAllData();

		userActStatMng.finalStat();
		// 用户行动信息输出
		for (UserActStat userActStat : userActStatMng.getUserActStatMap().values())
		{
			try
			{
				StringBuffer sb = new StringBuffer();
				String TabMark = "\t";
				for (UserActTime userActTime : userActStat.userActTimeMap.values())
				{
					for (UserCellAll userActAll : userActTime.userCellAllMap.values())
					{
						sb.delete(0, sb.length());

						sb.append(0);// cityid
						sb.append(TabMark);
						sb.append(userActStat.imsi);
						sb.append(TabMark);
						sb.append(userActStat.msisdn);
						sb.append(TabMark);
						sb.append(userActTime.stime);
						sb.append(TabMark);
						sb.append(userActTime.etime);
						sb.append(TabMark);

						// 主服小区
						UserCell mainUserCell = userActAll.getMainUserCell();
						sb.append(userActAll.eci);
						sb.append(TabMark);
						sb.append(0);
						sb.append(TabMark);
						sb.append(userActAll.eci);
						sb.append(TabMark);
						sb.append(mainUserCell.rsrpSum);
						sb.append(TabMark);
						sb.append(mainUserCell.rsrpTotal);
						sb.append(TabMark);
						sb.append(mainUserCell.rsrpMaxMark);
						sb.append(TabMark);
						sb.append(mainUserCell.rsrpMinMark);

						resultOutputer.pushData(MroCsOTTTableEnum.useractcell.getIndex(), sb.toString());

						// 邻区
						List<UserCell> userCellList = userActAll.getUserCellList();
						int sn = 1;
						for (UserCell userCell : userCellList)
						{
							if (userCell.eci == userActAll.eci)
							{
								continue;
							}

							sb.delete(0, sb.length());
							sb.append(0);// cityid
							sb.append(TabMark);
							sb.append(userActStat.imsi);
							sb.append(TabMark);
							sb.append(userActStat.msisdn);
							sb.append(TabMark);
							sb.append(userActTime.stime);
							sb.append(TabMark);
							sb.append(userActTime.etime);
							sb.append(TabMark);

							sb.append(userActAll.eci);
							sb.append(TabMark);
							sb.append(sn);
							sb.append(TabMark);
							sb.append(userCell.eci);
							sb.append(TabMark);
							sb.append(userCell.rsrpSum);
							sb.append(TabMark);
							sb.append(userCell.rsrpTotal);
							sb.append(TabMark);
							sb.append(userCell.rsrpMaxMark);
							sb.append(TabMark);
							sb.append(userCell.rsrpMinMark);

							resultOutputer.pushData(MroCsOTTTableEnum.useractcell.getIndex(), sb.toString());
							sn++;
						}
					}
				}
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error,"user action error", "user action error", e);
			}
		}
		userActStatMng.getUserActStatMap().clear();
		userActStatMng = new UserActStatMng();
		try
		{
			figureMroFix.cleanup();
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeDetailLog(LogType.error,"figureMroFix.cleanup error", "figureMroFix.cleanup error", e);
		}
	}

	private void dealSample(List<SIGNAL_MR_All> mroList)
	{
		DT_Sample_4G sample = new DT_Sample_4G();
		int dist;
		int maxRadius = 3000;

		for (SIGNAL_MR_All data : mroList)
		{
			sample.Clear();
			// 如果采样点过远就需要筛除
			LteCellInfo lteCellInfo = CellConfig.GetInstance().getLteCell(data.tsc.Eci);
			dist = -1;
			if (lteCellInfo != null)
			{
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
				if (data.tsc.longitude > 0 && data.tsc.latitude > 0 && lteCellInfo.ilongitude > 0 && lteCellInfo.ilatitude > 0)
				{
					dist = (int) GisFunction.GetDistance(data.tsc.longitude, data.tsc.latitude, lteCellInfo.ilongitude, lteCellInfo.ilatitude);
				}
			}
			data.dist = dist;
			if (dist > maxRadius)
			{
				data.dist = -1000000;
				data.tsc.longitude = 0;
				data.tsc.latitude = 0;
				data.testType = StaticConfig.TestType_OTHER;
				data.samState = 0;
				data.locSource = 0;
			}

			// 基于Ta进行筛
			if (data.tsc.LteScTadv >= 15 && data.tsc.LteScTadv < 1282)
			{
				double taDist = MrLocation.calcDist(data.tsc.LteScTadv, data.tsc.LteScRTTD);
				if (dist > taDist * 1.2)
				{
					data.dist = -1;
					data.tsc.longitude = 0;
					data.tsc.latitude = 0;
					data.testType = StaticConfig.TestType_OTHER;
				}
			}
			//目前过滤北京地铁小区
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.BeiJing))
			{
				if(FilterCellConfig.GetInstance().getLteCell(data.tsc.Eci, true))
				{
					data.tsc.longitude = 0;
					data.tsc.latitude = 0;
					data.testType = StaticConfig.TestType_OTHER;
				}
			}

			//计算给经度类型赋值
			data.ConfidenceType = Func.getSampleConfidentType(data.locSource, data.samState, data.testType);
			
			try
			{
				statMro(sample, data);
				if (!sample.isQciData()) {
					outCsSample(sample);
                    if (!sample.mrType.contains("MDT"))// mdt不在参与后面的统计
                    {
                        statKpi(sample);
                    }
				}
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "MroXdrDeal.dealSample","MroXdrDeal.dealSample: ", e);
			}
		}
	}

	private void statKpi(DT_Sample_4G sample)
	{
		// cpe不参与kpi运算
		if (sample.testType == StaticConfig.TestType_CPE)
		{
			return;
		}
		statDeal.dealSample(sample);
		userActStatMng.stat(sample);


		// StaticConfig.TestType_DT_EX 不参与运算
		if (sample.testType == StaticConfig.TestType_DT)
		{
			statDeal_DT.dealSample(sample);
		}

		if (sample.testType == StaticConfig.TestType_CQT)
		{
			statDeal_CQT.dealSample(sample);
		}
	}

	private void statMro(DT_Sample_4G tsam, SIGNAL_MR_All tTemp)
	{
		if (tTemp.ibuildingID > 0)
		{
			tsam.ibuildingID = tTemp.ibuildingID;
		}
		else
		{
			tsam.ibuildingID = -1;
		}
		if (tTemp.iheight < 0)
		{
			tsam.iheight = -1;
		}
		else
		{
			tsam.iheight = tTemp.iheight;
		}
		tsam.isResidentUser = tTemp.isResidentUser;
		tsam.position = tTemp.position;
		tsam.simuLatitude = tTemp.simuLatitude;
		tsam.simuLongitude = tTemp.simuLongitude;
		tsam.ilongitude = tTemp.tsc.longitude;
		tsam.ilatitude = tTemp.tsc.latitude;
		tsam.testType = tTemp.testType;
		tsam.samState = tTemp.samState;
		tsam.locSource = tTemp.locSource;
		tsam.cityID = tTemp.tsc.cityID;
		tsam.itime = tTemp.tsc.beginTime;
		tsam.wtimems = (short) (tTemp.tsc.beginTimems);
		tsam.IMSI = tTemp.tsc.IMSI;
		tsam.UETac = tTemp.UETac;
		tsam.iLAC = (int) MroLocStat.getValidData(tsam.iLAC, tTemp.tsc.TAC);
		tsam.iCI = (long) MroLocStat.getValidData(tsam.iCI, tTemp.tsc.CellId);
		tsam.Eci = (long) MroLocStat.getValidData(tsam.Eci, tTemp.tsc.Eci);
		tsam.eventType = 0;
		tsam.ENBId = (int) MroLocStat.getValidData(tsam.ENBId, tTemp.tsc.ENBId);
		tsam.UserLabel = tTemp.tsc.UserLabel;
		tsam.wifilist = tTemp.tsc.UserLabel;// mrall 中userlabel中装的wifi信息
		tsam.CellId = (long) MroLocStat.getValidData(tsam.CellId, tTemp.tsc.CellId);
		tsam.Earfcn = tTemp.tsc.Earfcn;
		tsam.SubFrameNbr = tTemp.tsc.SubFrameNbr;
		tsam.MmeCode = (int) MroLocStat.getValidData(tsam.MmeCode, tTemp.tsc.MmeCode);
		tsam.MmeGroupId = (int) MroLocStat.getValidData(tsam.MmeGroupId, tTemp.tsc.MmeGroupId);
		tsam.MmeUeS1apId = (long) MroLocStat.getValidData(tsam.MmeUeS1apId, tTemp.tsc.MmeUeS1apId);
		tsam.Weight = tTemp.tsc.Weight;
		tsam.LteScRSRP = tTemp.tsc.LteScRSRP;
		tsam.LteScRSRQ = tTemp.tsc.LteScRSRQ;
		tsam.LteScEarfcn = tTemp.tsc.LteScEarfcn;
		tsam.LteScPci = tTemp.tsc.LteScPci;
		tsam.LteScBSR = tTemp.tsc.LteScBSR;
		tsam.LteScRTTD = tTemp.tsc.LteScRTTD;
		tsam.LteScTadv = tTemp.tsc.LteScTadv;
		tsam.LteScAOA = tTemp.tsc.LteScAOA;
		tsam.LteScPHR = tTemp.tsc.LteScPHR;
		tsam.LteScRIP = tTemp.tsc.LteScRIP;
		tsam.LteScSinrUL = tTemp.tsc.LteScSinrUL;
		tsam.LocFillType = 1;

		tsam.testType = tTemp.testType;
		tsam.location = tTemp.location;
		tsam.dist = tTemp.dist;
		tsam.radius = tTemp.radius;
		tsam.locType = tTemp.locType;
		tsam.indoor = tTemp.indoor;
		tsam.networktype = tTemp.networktype;
		tsam.label = tTemp.label;

		tsam.serviceType = tTemp.serviceType;
		tsam.serviceSubType = tTemp.subServiceType;

		tsam.moveDirect = tTemp.moveDirect;

		tsam.LteScPUSCHPRBNum = tTemp.tsc.LteScPUSCHPRBNum;
		tsam.LteScPDSCHPRBNum = tTemp.tsc.LteScPDSCHPRBNum;
		tsam.imeiTac = tTemp.tsc.imeiTac;
		tsam.fullNetType = ImeiConfig.GetInstance().getValue(tTemp);
		tsam.eciSwitchList = tTemp.eciSwitchList;
		tsam.ConfidenceType = tTemp.ConfidenceType;
		if (tsam.samState == StaticConfig.ACTTYPE_OUT || tsam.iAreaType > 0)
		{
			tsam.grid = new GridItemOfSize(tsam.cityID, tsam.ilongitude, tsam.ilatitude, OutdoorGridSize);
		}
		else if (tsam.samState == StaticConfig.ACTTYPE_IN)
		{
			tsam.grid = new GridItemOfSize(tsam.cityID, tsam.ilongitude, tsam.ilatitude, IndoorGridSize);
		}
		tsam.MSISDN = tTemp.tsc.Msisdn;

		if (tTemp.tsc.EventType.equals("MRO"))
		{
			tsam.flag = "MRO";
		}
		else if (tTemp.tsc.EventType.equals("MDT_IMM"))
		{
			tsam.flag = "MDT_IMM";
		}
		else if (tTemp.tsc.EventType.equals("MDT_LOG"))
		{
			tsam.flag = "MDT_LOG";
		}
		else
		{
			tsam.flag = "MRO";
		}
		tsam.mrType = tTemp.tsc.EventType;

		for (int i = 0; i < tsam.nccount.length; i++)
		{
			tsam.nccount[i] = tTemp.nccount[i];
		}
		
		if (tsam.LteScRSRP >= -110) {
			for (int i = 0; i < tsam.tlte.length; i++)
			{
				tsam.tlte[i] = tTemp.tlte[i];
				if (tTemp.tlte[i].LteNcRSRP >= -150 && tTemp.tlte[i].LteNcRSRP <= -30)
				{
					if (tTemp.tlte[i].LteNcRSRP >= tTemp.tsc.LteScRSRP - 6)
					{
						if (tTemp.tlte[i].LteNcEarfcn == tTemp.tsc.LteScEarfcn)
						{
							tsam.overlapSameEarfcn++;
						}
						tsam.OverlapAll++;
					}
				}
			}
		}

		for (int i = 0; i < tsam.tgsm.length; i++)
		{
			tsam.tgsm[i] = tTemp.tgsm[i];
		}

		for (int i = 0; i < tsam.tgsm.length; i++)
		{
			tsam.tgsm[i] = tTemp.tgsm[i];
		}
		for (int i = 0; i < tsam.dx_freq.length; i++)
		{
			tsam.dx_freq[i] = tTemp.dx_freq[i];
		}
		
		for (int i = 0; i < tsam.lt_freq.length; i++)
		{
			tsam.lt_freq[i] = tTemp.lt_freq[i];
		}

//		for (int i = 0; i < tsam.trip.length; i++)
//		{
//			tsam.trip[i] = tTemp.trip[i];
//		}


		tsam.iAreaID = tTemp.areaId;
		tsam.iAreaType = tTemp.areaType;
		// mdt 置信度
		tsam.Confidence = tTemp.Confidence;
		// 高铁信息
		tsam.trainKey = tTemp.trainKey;
		tsam.sectionId = tTemp.sectionId;
		tsam.segmentId = tTemp.segmentId;

		// 20171030 add QCI stat
		tsam.qciData = new LteScPlrQciData(tTemp.tsc.LteScPlrULQci, tTemp.tsc.LteScPlrDLQci);

		calJamType(tsam);
		// output to hbase
		try
		{
			/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
			// 新表计算
			if (!tsam.isQciData()) {
                userStat.dealSample(tsam);
			}
			//如果是mdt的数据就不要吐出采样点
			if (tsam.mrType.equals("MDT_IMM") || tsam.mrType.equals("MDT_LOG"))
			{
				return;
			}
			
			// 吐出 单通/断续 故障事件
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.MroDetail) && tsam.isQciData())
			{
				// 这里内部相当于已经判断了是不是QCI数据 不用另做处理
				List<EventData> evtDatas = tsam.toEventData_Detail();
				List<EventData> mroEvtData = tsam.toEventData();
				StringBuffer bf = new StringBuffer();
				statEventAndOutDetail(evtDatas, bf);
				bf.delete(0, bf.length());
				statEventAndOutDetail(mroEvtData,bf);
			}
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeDetailLog(LogType.error,"MroXdrDeal.statmro", "MroXdrDeal.statmro: ", e);
		}
	}

	/**
	 * 输出详单和统计事件
	 * @param evtDatas xdr数据List
	 * @param bf
	 * @throws Exception
	 */
	private void statEventAndOutDetail(List<EventData> evtDatas, StringBuffer bf) throws Exception {
		/* 输出详单 */
		for (EventData evtData : evtDatas)
        {
        	if(evtData.eventDetial==null){
        		continue;
			}

			evtData.toString(bf);
			if (evtData.confidentType == StaticConfig.IH)
			{
				if (evtData.eventDetial.sampleType == StaticConfig.FAILURE_SAMPLE)
				{
					resultOutputer.pushData(TypeIoEvtEnum.IHSAMPLE.getIndex(), bf.toString());
				}

				resultOutputer.pushData(TypeIoEvtEnum.IHSAMPLEALL.getIndex(), bf.toString());

			} else if (evtData.confidentType == StaticConfig.IM)
			{
				if (evtData.eventDetial.sampleType == StaticConfig.FAILURE_SAMPLE)
				{
					resultOutputer.pushData(TypeIoEvtEnum.IMSAMPLE.getIndex(), bf.toString());
				}

				resultOutputer.pushData(TypeIoEvtEnum.IMSAMPLEALL.getIndex(), bf.toString());

			} else if (evtData.confidentType == StaticConfig.IL)
			{
				if (evtData.eventDetial.sampleType == StaticConfig.FAILURE_SAMPLE)
				{
					resultOutputer.pushData(TypeIoEvtEnum.ILSAMPLE.getIndex(), bf.toString());
				}

				resultOutputer.pushData(TypeIoEvtEnum.ILSAMPLEALL.getIndex(), bf.toString());

			} else if (evtData.confidentType == StaticConfig.OH)
			{
				if (evtData.eventDetial.sampleType == StaticConfig.FAILURE_SAMPLE)
				{
					resultOutputer.pushData(TypeIoEvtEnum.OHSAMPLE.getIndex(), bf.toString());
				}

				resultOutputer.pushData(TypeIoEvtEnum.OHSAMPLEALL.getIndex(), bf.toString());

			} else if (evtData.confidentType == StaticConfig.OM)
			{
				if (evtData.eventDetial.sampleType == StaticConfig.FAILURE_SAMPLE)
				{
					resultOutputer.pushData(TypeIoEvtEnum.OMSAMPLE.getIndex(), bf.toString());
				}

				resultOutputer.pushData(TypeIoEvtEnum.OMSAMPLEALL.getIndex(), bf.toString());

			} else
			{
				if (evtData.eventDetial.sampleType == StaticConfig.FAILURE_SAMPLE)
				{
					resultOutputer.pushData(TypeIoEvtEnum.OLSAMPLE.getIndex(), bf.toString());
				}

				resultOutputer.pushData(TypeIoEvtEnum.OLSAMPLEALL.getIndex(), bf.toString());

			}
			bf.delete(0, bf.length());

        }
		/* 事件统计 */
		for (EventData evtData : evtDatas)
        {
            if (evtData.eventStat != null)
            {
                dataStater.stat(evtData);
            }
        }
	}

	private void outCsSample(DT_Sample_4G tsam) throws Exception {
		// 特殊用户吐出全量的sample
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.BeiJing) && SpecialUserConfig.GetInstance().ifSpeciUser(tsam.IMSI, false))
		{
			resultOutputer.pushData(MroCsOTTTableEnum.mrvap.getIndex(), ResultHelper.getPutLteSample(tsam));
		}
				
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.OutAllSample))
		{
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.NingXia)) 
			{
				if(FilterCellConfig.GetInstance().getLeaderLteCell(tsam.Eci))
				{
					resultOutputer.pushData(MroCsOTTTableEnum.mrosample.getIndex(), ResultHelper.getPutLteSample(tsam));
				}
			}
			else
			{
				resultOutputer.pushData(MroCsOTTTableEnum.mrosample.getIndex(), ResultHelper.getPutLteSample(tsam));
			}
		}
		

		if (tsam.locType.contains("fp") || tsam.locType.contains("fg"))// 过滤掉指纹库定位,常住小区回填的loctp=fp
		{
			tsam.ilongitude = 0;
			tsam.ilatitude = 0;
			return;
		}

		if (tsam.testType == StaticConfig.TestType_DT)
		{
			if (tsam.ilongitude > 0)
			{
				resultOutputer.pushData(MroCsOTTTableEnum.sampledt.getIndex(), ResultHelper.getPutLteSample(tsam));
			}
		}
		else if (tsam.testType == StaticConfig.TestType_DT_EX || tsam.testType == StaticConfig.TestType_CPE)
		{
			if (tsam.ilongitude > 0)
			{
				resultOutputer.pushData(MroCsOTTTableEnum.sampledtex.getIndex(), ResultHelper.getPutLteSample(tsam));
			}
		}
		else if (tsam.testType == StaticConfig.TestType_CQT)
		{
			if (tsam.ilongitude > 0)
			{
				resultOutputer.pushData(MroCsOTTTableEnum.samplecqt.getIndex(), ResultHelper.getPutLteSample(tsam));
			}
		}
	}

	public void calJamType(DT_Sample_4G tsam)
	{
		if ((tsam.LteScRSRP < -50 && tsam.LteScRSRP > -150) && tsam.LteScRSRP > -110)
		{
			for (NC_LTE item : tsam.tlte)
			{
				if ((item.LteNcRSRP < -50 && item.LteNcRSRP > -150) && item.LteNcRSRP - tsam.LteScRSRP > -6)
				{
					if (tsam.Earfcn == item.LteNcEarfcn)
					{
						tsam.sfcnJamCellCount++;
					}
					else
					{
						tsam.dfcnJamCellCount++;
					}
				}
			}
		}
	}

	/**
	 * 三种指纹库定位
	 */
	public void dealSimuLoc(List<SIGNAL_MR_All> allMroItemList)
	{
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.UserLoc3))
		{
			LOGHelper.GetLogger().writeLog(LogType.info, "userloc  begin ... ...");
			if (currEci == 0 || currEci != tempEci)
			{
				currEci = tempEci;
				userLocer3 = new cn.mastercom.bigdata.locuser_v3.UserLocer();
			}
			MroLocStat.UserLoc3Fixed(userLocer3, allMroItemList, resultOutputer);
			if ((dataTypeSwitch[DataType_MRO_MT] || dataTypeSwitch[DataType_MRO]) && 
					!MainModel.GetInstance().getCompile().Assert(CompileMark.NoCsTable))
			{
				figureMroFix.FigureMroItemList = allMroItemList;
				figureMroFix.outDealingData();
			}
		}
	}
	
	public void collectAndFixed()
	{
		List<ArrayList<MroOrigDataMT>> listMr = new ArrayList<ArrayList<MroOrigDataMT>>();
		listMr.addAll(map.values());
		map.clear();
		for (ArrayList<MroOrigDataMT> mroList : listMr)
		{
			SIGNAL_MR_All mroItem = MroLocStat.collectData(mroList);
			if (mroItem == null)
			{
				continue;
			}
			MroLocStat.srcMroOttFixed(allMroItemList, mroItem, xdrLabelMng, cellInfo);
		}
	}

	public void dealLast5SecMro()
	{
		if (map.size() > 0)
		{
			collectAndFixed();
		}
	}
	
	public void dealPosition(List<SIGNAL_MR_All> mrList)
	{
		for (SIGNAL_MR_All mrAll : mrList)
		{
			if(mrAll.ibuildingID > 0)
			{
				getPosition(mrAll);
			}
		}
	}
	
	public void dealResidentUser(List<SIGNAL_MR_All> mrList)
	{
		if(residentUserMap.size() > 0)
		{
			HashMap<Long,Integer> eciSwitchMap = new HashMap<Long,Integer>();
			for (SIGNAL_MR_All mrAll : mrList)
			{
				try
				{
					eciSwitchMap.clear();
					ArrayList<NewResidentUser> newResidentUserList = residentUserMap.get(mrAll.tsc.IMSI);
					if(newResidentUserList != null && !newResidentUserList.isEmpty())
					{
						mrAll.isResidentUser = true;  //常驻用户标签
						if(mrAll.tsc.longitude > 0) //即使ott定位回填位置，也要打上常驻用户标签，但不用再回填位置
						{
							continue;
						}
						if(mrAll.eciSwitchList.length() > 0)
						{
							String[] strs = mrAll.eciSwitchList.split(";", -1);
							for(String str : strs)
							{
								if(str.length() > 0)
								{
									eciSwitchMap.put(Long.parseLong(str) / 256, 1);	//前后五分钟切换小区数改成按照基站来计算
								}
							}
						}
						if(eciSwitchMap.size() > 2 || eciSwitchMap.size() == 0)	//前后5分钟小区切换信息超过2个则不回填
						{
							continue;
						}
						int mroDateType = Func.getDateType(mrAll.tsc.beginTime);
						for (NewResidentUser residentUser : newResidentUserList)
						{
							if(residentUser != null && residentUser.locSource.contains("ru") && residentUser.rCellType > StaticConfig.RU_RC0)	//只回填ru部分位置，只回填rcelltype>0的常驻用户
							{
								//对mr回填工作常驻地经纬度
								if(mroDateType == StaticConfig.WORKTIME && residentUser.locType == StaticConfig.WORKTIME)
								{
									fillResidentData(mrAll, residentUser);
								}
								//对mr回填家庭常驻地经纬度
								else if(mroDateType == StaticConfig.HOMETIME && residentUser.locType == StaticConfig.HOMETIME)
								{
									fillResidentData(mrAll, residentUser);
								}
							}
							//由于小区太少，暂时隔绝bfp
//								else if (null != residentUser && MainModel.GetInstance().getCompile().Assert(CompileMark.BuildingPredict))
//								{
//									LOGHelper.GetLogger().writeLog(LogType.error, "bfp locsource");
//									MroLocStat.fillSpecialResidentData(mrAll, cellBuild, mrLocationPredict);
//								}
						}
					}
				}catch (Exception e)
				{
					LOGHelper.GetLogger().writeLog(LogType.error,"ResidentUser fill mr error", "ResidentUser fill mr error ", e);
				}
			}
		}
	}

	private void fillResidentData(SIGNAL_MR_All mrAll, NewResidentUser residentUser) {
		mrAll.tsc.longitude = (int) residentUser.longitude;
		mrAll.tsc.latitude = (int) residentUser.latitude;
		mrAll.ibuildingID = residentUser.buildId;
		mrAll.iheight = (short) residentUser.height;
		mrAll.locType = residentUser.locSource;
		mrAll.locSource = Func.getLocSource(mrAll.locType);
		mrAll.position = residentUser.position;
		mrAll.testType = StaticConfig.TestType_CQT;
		mrAll.label = "static";
	}

	public int pushData(CellTimeKey key, String value)
	{
		int dataType = key.getDataType();
		current5Sec = key.getSuTime();
		return pushData(dataType, value);
	}

	@Override
	public int pushData(int dataType, String value)
	{
		if (dataType != DataType_XDRLOCATION && !hasInit)
		{
			xdrLabelMng.init();
			hasInit = true;
		}
		
		if (dataType == DataType_XDRLOCATION)
		{
			String[] strs = value.toString().split("\t", -1);
			for (int i = 0; i < strs.length; ++i)
			{
				strs[i] = strs[i].trim();
			}
			XdrLabel xdrLabel;
			try
			{
				xdrLabel = XdrLabel.FillData(strs, 0);
				/* 吐出位置库 */
				if ((xdrLabel.longitudeGL > 0 && xdrLabel.latitudeGL > 0) && (xdrLabel.itime / TimeSpan * TimeSpan == curTimeSpan || xdrLabel.itime / TimeSpanHour * TimeSpanHour == curTimeSpan))
				{
					resultOutputer.pushData(MroCsOTTTableEnum.xdrloclib.getIndex(), outPutLocLib(xdrLabel));
				}
				
				xdrLabelMng.addXdrLocItem(xdrLabel);
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeDetailLog(LogType.error,"XdrLable.FillData error", "XdrLable.FillData error ", e);
				return StaticConfig.EXCEPTION;
			}
		}
		else if (dataType == DataType_CELLBUILD)
		{
			cellBuild.loadCellBuildValue(value);
		}
		else if (dataType == DataType_MRO_MT)
		{
			String[] strs;
			Integer i = 0;
			SIGNAL_MR_All mroItem = new SIGNAL_MR_All();
			try
			{
				if(MainModel.GetInstance().getCompile().Assert(CompileMark.ChongQing))
				{
					strs = value.split(parseItem.getSplitMark(), -1);
					dataAdapterReader.readData(strs);
					mroItem.FillData(dataAdapterReader);
				}
				else
				{
					strs = value.split(StaticConfig.DataSliper2, -1);
					mroItem.FillData(new Object[] { strs, i });
				}
				MroLocStat.ottFix(mroItem, xdrLabelMng, cellInfo);
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeDetailLog(LogType.error,"SIGNAL_MR_All.FillData error", "SIGNAL_MR_All.FillData error ", e);
				return StaticConfig.EXCEPTION;
			}
			allMroItemList.add(mroItem);
			/*if ("MRO".equals(mroItem.tsc.EventType) || mroItem.tsc.longitude > 0)//mre数据不做指纹库回填
			{
				allMroItemList.add(mroItem);
			}*/
		}
		else if (dataType == DataType_MRO)
		{
			if (current5Sec != previous5Sec)
			{	
				previous5Sec = current5Sec;
				if (map.size() > 0)
				{
					collectAndFixed();
				}
				packet = 0;
				packet++;
				MroLocStat.groupMro(map, value, dataAdapterReader, parseItem);
			}
			else
			{
				packet++;
				if (packet % 1000 == 0){
                    LOGHelper.GetLogger().writeLog(LogType.info, " mergeMro:" + tempEci + ", packet num=" + packet);
                }

				if (packet > 50000)
				{
					LOG.info("mro num biger than 50000 !");
					LOGHelper.GetLogger().writeLog(LogType.error, "mro num biger than 50000 !");
					return StaticConfig.FAILURE;
				}
				MroLocStat.groupMro(map, value, dataAdapterReader, parseItem);
			}
		}
		else if (dataType == DataType_MDT_IMM)
		{
			SIGNAL_MR_All mdtItem = null;
			String[] strs = null;
			try
			{
				mdtItem = new SIGNAL_MR_All();
				if (parseItem_IMM == null)
				{
					parseItem_IMM = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MDT-SRC-IMM");
					if (parseItem_IMM == null)
					{
						throw new IOException("parse item do not get.");
					}
					dataAdapterReader_IMM = new DataAdapterReader(parseItem_IMM);
				}
				strs = value.toString().split(parseItem_IMM.getSplitMark(), -1);
				dataAdapterReader_IMM.readData(strs);
				mdtItem.FillIMMData(dataAdapterReader_IMM);
				if (mdtItem == null || mdtItem.tsc == null || mdtItem.tsc.MmeUeS1apId <= 0 || mdtItem.tsc.Eci <= 0 || mdtItem.tsc.beginTime <= 0)
					return StaticConfig.EXCEPTION;
				MroLocStat.ottFix(mdtItem, xdrLabelMng, cellInfo);
				allMdtItemList.add(mdtItem);
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error,"MDT_IMM.FillData error", "MDT_IMM.FillData error ", e);
				return StaticConfig.EXCEPTION;
			}
		}
		else if (dataType == DataType_MDT_LOG)
		{
			SIGNAL_MR_All mdtItem = null;
			String[] strs = null;
			try
			{
				if (parseItem_LOG == null)
				{
					parseItem_LOG = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MDT-SRC-LOG");
					if (parseItem_LOG == null)
					{
						throw new IOException("parse item do not get.");
					}
					dataAdapterReader_LOG = new DataAdapterReader(parseItem_LOG);
				}
				mdtItem = new SIGNAL_MR_All();
				strs = value.split(parseItem_LOG.getSplitMark(), -1);
				dataAdapterReader_LOG.readData(strs);
				mdtItem.FillLOGData(dataAdapterReader_LOG);
				if (mdtItem == null || mdtItem.tsc == null || mdtItem.tsc.MmeUeS1apId <= 0 || mdtItem.tsc.Eci <= 0 || mdtItem.tsc.beginTime <= 0)
					return StaticConfig.EXCEPTION;
				MroLocStat.ottFix(mdtItem, xdrLabelMng, cellInfo);
				allMdtItemList.add(mdtItem);
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error,"MDT_LOG.FillData error1", "MDT_LOG.FillData error1 ", e);
				return StaticConfig.EXCEPTION;
			}
		}
		else if(dataType == DataType_RESIDENTUSER)
		{
			String[] strs = value.split("\t", -1);
			NewResidentUser residentUser = new NewResidentUser();
			residentUser.fillMergeData(strs);
			ArrayList<NewResidentUser> newResidentUserList = residentUserMap.get(residentUser.imsi);
			if(newResidentUserList == null)
			{
				newResidentUserList = new ArrayList<>();
				residentUserMap.put(residentUser.imsi, newResidentUserList);
			}
			newResidentUserList.add(residentUser);
		}		
		dataTypeSwitch[dataType] = true;
		return StaticConfig.SUCCESS;
	}
	
	@Override
	public void statData()
	{
		dealLast5SecMro();// 处理mro的最后5s数据

		List<SIGNAL_MR_All> mrList = new ArrayList<SIGNAL_MR_All>();	
		if (dataTypeSwitch[DataType_MRO_MT] || dataTypeSwitch[DataType_MRO])
		{
			mrList = allMroItemList;
		}	
		if (dataTypeSwitch[DataType_MDT_IMM] || dataTypeSwitch[DataType_MDT_LOG])
		{
			mrList.addAll(allMdtItemList);
		}
		if(mrList.size() == 0)
		{
			return;
		}
		
		// 高铁定位
		//20180329 定位需要置于getInOrOut的前面，以获取室内外的samState, 供后续统计
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.HiRail2) && hsrConfig != null)
		{
			LocFunc hsrLocFunc = new LocFunc(hsrConfig);
			MroLocStat.HSRLocFixed(hsrLocFunc, mrList);

			//添加场景代码
//			AreaPointFunc areaLocFunc = new AreaPointFunc(hsrConfig);
//			MroLocStat.HSRAreaLocFixed(areaLocFunc, mrList);
		}

		//常驻用户定位 2018/04/17 定位需要置于getInOrOut的前面，以回填没有位置无法区分室内外的采样点
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.ResidentUser))
		{
			dealResidentUser(mrList);
		}

		//北京室分站小区位置可直接回填采样点（ott，resident之后）
        if(MainModel.GetInstance().getCompile().Assert(CompileMark.BeiJing)){
            FillBackLocByCellBuild.fillBackByCellBuild(mrList,cellInfo,cellBuild.getBuildID());
        }
		dealSimuLoc(mrList);	//指纹库定位  mre数据不参与指纹库定位
		getInOrOut(mrList);
		dealPosition(mrList);	//定方位
		dealSample(mrList);
	}

	@Deprecated
	private void justForTest(List<SIGNAL_MR_All> mrList) throws ModelNotTrainedException {
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.BuildingPredict)) {
			for (SIGNAL_MR_All mrAll : mrList) {
				MroLocStat.fillSpecialResidentData(mrAll, cellBuild, mrLocationPredict);
			}
		}
	}

	@Override
	public void outData()
	{
		// outFgOttStat(); // bs表和mdt表
		outDealingData();// 吐出10000条
		outPutLocLib(allMroItemList);
			//mdt也吐出到位置库
		if(allMdtItemList != null && allMdtItemList.size() > 0 )
		{
			outPutLocLib(allMdtItemList);			
		}
	}
		// figureMroFix.outDealingData();
}
