package cn.mastercom.bigdata.stat.userResident.homeBroadbandLoc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.conf.cellconfig.CellConfig;
import cn.mastercom.bigdata.conf.cellconfig.LteCellInfo;
import cn.mastercom.bigdata.conf.config.PropertyConfig;
import cn.mastercom.bigdata.conf.config.PropertyInfo;
import cn.mastercom.bigdata.loc.userResident.UserResidentCluster;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.stat.userResident.enmus.ResidentConfigTablesEnum;
import cn.mastercom.bigdata.stat.userResident.enmus.ResidentUserTablesEnum;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.xdr.loc.ResidentUser;

public class MergeUserResidentDeal
{
	public static final int DATATYPE_RESIDENT_USER_WORKPLACE = 1;
	public static final int DATATYPE_RESIDENT_USER_HOMEPLACE = 2;
	public static final int DATATYPE_RESIDENT_USER_TEMPORARYPLACE = 3;
	public static final int DATATYPE_RESIDENT_USER_OLD = 4;
	public static final int DATATYPE_NEW_RESIDENT_USER_OLD = 5;
	
	private ResultOutputer resultOutputer;
	public int dayFilterTimes;
	public int hourFilterTimes;
	private long tempEci;// 记录上一个eci
	private int workDayNum;
	private int mergeUserResidentDayNum;
	private String finalTime;

	public MergeUserResidentDeal(ResultOutputer resultOutputer, Configuration conf) throws IOException
	{
		this.resultOutputer = resultOutputer;
		workDayNum = Integer.parseInt(conf.get("mastercom.workDayNum"));
		finalTime = conf.get("mastercom.finalTime");
		mergeUserResidentDayNum = MainModel.GetInstance().getAppConfig().getMergeUserResidentDayNum();
		dayFilterTimes = MainModel.GetInstance().getAppConfig().getMergeUserResidentTime();
		hourFilterTimes = Integer.parseInt(MainModel.GetInstance().getAppConfig().residentTime());
		// 初始化小区的信息
		if (!CellConfig.GetInstance().loadLteCell(conf))
		{
			LOGHelper.GetLogger().writeLog(LogType.error, "cellconfig init error 请检查！");
			throw (new IOException("cellconfig init error 请检查！" + CellConfig.GetInstance().errLog));
		}
		// 物业点
		if (!PropertyConfig.GetInstance().loadProperty(conf))
		{
			LOGHelper.GetLogger().writeLog(LogType.error, "PropertyConfig init error 请检查！");
		}
	}

	public void deal(MergeUserResidentKey key, Iterable<Text> values) throws IOException
	{
		HashMap<Integer, BaseStation> newResidentUserWorkPlaceMap = new HashMap<>();
		HashMap<Integer, BaseStation> newResidentUserHomePlaceMap = new HashMap<>();
		HashMap<Long, Long> eciWorkPlaceMap = new HashMap<>();
		HashMap<Long, Long> eciHomePlaceMap = new HashMap<>();
		HashMap<Integer, ArrayList<ResidentUser>> newResidentUserTemporaryPlaceMap = new HashMap<>();
		HashMap<String, ResidentUser> dayResidentUserMap = new HashMap<>();
		HashMap<String, ArrayList<ResidentUserLoc>> allDaysResidentUserLocMap = new HashMap<>();
		HashMap<String, ResidentUserLoc> dayResidentUserLocMap = new HashMap<>();
		HashMap<String, ResidentUser> oldResidentUserMap = new HashMap<>();
		HashMap<String, NewResidentUser> NewResidentUserMap = new HashMap<>();
		HashMap<String, NewResidentUser> oldNewResidentUserMap = new HashMap<>();

		if (key.getEci() != tempEci)
		{
			LteCellInfo cellInfo = CellConfig.GetInstance().getLteCell(key.getEci());
			if (cellInfo == null)
			{
				cellInfo = new LteCellInfo();
				LOGHelper.GetLogger().writeLog(LogType.info,
						"gongcansize:" + CellConfig.GetInstance().getlteCellInfoMapSize() + "  gongcan no eci:" + key.getEci() + "  enbid:" + key.getEci() / 256 + " cellid:" + key.getEci() % 256);
			}
			tempEci = key.getEci();
		}

		for (Text value : values)
		{
			if (key.getDataType() == DATATYPE_RESIDENT_USER_WORKPLACE)
			{
				//计算工作常驻小区特性
				dealEveryData(value, eciWorkPlaceMap, newResidentUserWorkPlaceMap);
			}
			else if (key.getDataType() == DATATYPE_RESIDENT_USER_HOMEPLACE)
			{
				//计算晚上常驻小区特性
				dealEveryData(value, eciHomePlaceMap, newResidentUserHomePlaceMap);
			}
			else if (key.getDataType() == DATATYPE_RESIDENT_USER_TEMPORARYPLACE)
			{
				//计算临时常驻小区特性
				dealEveryData2(value, newResidentUserTemporaryPlaceMap);
				//计算全量位置
				dealResidentUserLoc(value, allDaysResidentUserLocMap);
			}
			else if (key.getDataType() == DATATYPE_RESIDENT_USER_OLD)
			{
				//组织旧的常驻用户
				String[] strs = value.toString().split(",|\t", -1);
				ResidentUser oldResidentUser = new ResidentUser();
				oldResidentUser.fillMergeData(strs); //旧常驻用户
				String tempKey = oldResidentUser.eci + "_" + oldResidentUser.hour;
				oldResidentUserMap.put(tempKey, oldResidentUser);
			}
			else if (key.getDataType() == DATATYPE_NEW_RESIDENT_USER_OLD)
			{
				// 组织旧的常驻用户
				String[] strs = value.toString().split(",|\t", -1);
				NewResidentUser oldResidentUser = new NewResidentUser();
				oldResidentUser.fillMergeData(strs); //旧常驻用户
				String tempKey = oldResidentUser.eci + "_" + oldResidentUser.locType;
				oldNewResidentUserMap.put(tempKey, oldResidentUser);
			}
		}
		
		//统计多天数据，计算用户在每个地市驻留的天数，求出驻留天数最多的天，如果天数>=10天，则说明该用户属于该城市驻留人口；否则去掉，不参与运算。
		if(!IsResidentPeople(newResidentUserTemporaryPlaceMap))
		{
			return;
		}
		
		//将多天常驻数据筛出常驻特性小区
		//计算工作地点常驻小区
		getResidentUserCell(eciWorkPlaceMap, newResidentUserWorkPlaceMap, dayResidentUserMap);
		
		//计算家庭地点常驻小区
		getResidentUserCell(eciHomePlaceMap, newResidentUserHomePlaceMap, dayResidentUserMap);
		
		//计算临时常驻小区
		getTemporaryResidentCell(newResidentUserTemporaryPlaceMap, dayResidentUserMap);
		
		//将多天常驻位置数据筛出天小区各位置源位置
		getResidentUserCellLoc(allDaysResidentUserLocMap,dayResidentUserLocMap);
		
		//更新位置
		updateLoc(dayResidentUserLocMap, dayResidentUserMap);
		
		//将常驻用户给新常驻用户表
		getNewResidentUser(dayResidentUserMap, NewResidentUserMap);
		
		save1(dayResidentUserMap, oldResidentUserMap);
		save2(NewResidentUserMap, oldNewResidentUserMap);
	}
	
	/**
	 * @param 按工作时间段和休息时间段（多小时数据放一起）处理数据，装进map
	 */
	public void dealEveryData(Text value, HashMap<Long, Long> eciMap, HashMap<Integer, BaseStation> newDaysResidentUserMap)
	{
		String[] vals = value.toString().split(",|\t", -1);
		ResidentUser dailyResidentUser = new ResidentUser();
		dailyResidentUser.fillData3(vals);
		dailyResidentUser.getPmOrAm();
		LteCellInfo cellInfo = CellConfig.GetInstance().getLteCell(dailyResidentUser.eci);
		if(cellInfo != null)
		{
			//计算用户工作/家庭的每个小区下的驻留总时长
			long eciKey = dailyResidentUser.eci;
			if(!eciMap.containsKey(eciKey))
			{
				eciMap.put(eciKey, 0L);
			}
			eciMap.put(eciKey, eciMap.get(eciKey) + dailyResidentUser.duration);
			
			//默认给小区经纬度cl
//			dailyResidentUser.longitude = cellInfo.ilongitude;
//			dailyResidentUser.latitude = cellInfo.ilatitude;
//			dailyResidentUser.locSource = StaticConfig.RULOC_CL;
			//计算常驻都以同一基站为准
			int enbidKey = (int) (dailyResidentUser.eci / 256);
			BaseStation tempBaseStation = newDaysResidentUserMap.get(enbidKey);
			if (tempBaseStation == null)
			{
				tempBaseStation = new BaseStation();
				tempBaseStation.fillData(cellInfo);
				newDaysResidentUserMap.put(enbidKey, tempBaseStation);
			}
			tempBaseStation.updateTotalTimes(dailyResidentUser);
			tempBaseStation.getResidentUserList().add(dailyResidentUser);
		}
	}

	/**
	 * @param 按所有时间的小时处理每种数据，装进map
	 */
	public void dealEveryData2(Text value, HashMap<Integer, ArrayList<ResidentUser>> newDaysResidentUserMap)
	{
		String[] vals = value.toString().split(",|\t", -1);
		ResidentUser dailyResidentUser = new ResidentUser();
		dailyResidentUser.fillData3(vals);
		LteCellInfo cellInfo = CellConfig.GetInstance().getLteCell(dailyResidentUser.eci);
		if(cellInfo != null)
		{
			dailyResidentUser.longitude = cellInfo.ilongitude;
			dailyResidentUser.latitude = cellInfo.ilatitude;
			dailyResidentUser.locSource = StaticConfig.RULOC_CL;
			dailyResidentUser.locType = 3;	//临时驻留特性小区地点
			int tempKey = (int) dailyResidentUser.eci;
			
			ArrayList<ResidentUser> tempResidentUserList = newDaysResidentUserMap.get(tempKey);
			if (tempResidentUserList == null)
			{
				tempResidentUserList = new ArrayList<ResidentUser>();
				newDaysResidentUserMap.put(tempKey, tempResidentUserList);
			}
			tempResidentUserList.add(dailyResidentUser);
		}
	}
	
	//统计多天数据，计算用户在每个地市驻留的天数，求出驻留天数最多的天，如果天数>=10天，则说明该用户属于该城市驻留人口。
	public boolean IsResidentPeople(HashMap<Integer, ArrayList<ResidentUser>> newDaysResidentUserMap)
	{
		HashMap<Integer, Set<String>> cityIdResidentMap = new HashMap<>();
		for (ArrayList<ResidentUser> list: newDaysResidentUserMap.values())
		{
			for (ResidentUser temp : list)
			{
				Set<String> daySet = cityIdResidentMap.get(temp.cityID);
				if(daySet == null)
				{
					daySet = new HashSet<>();
					cityIdResidentMap.put(temp.cityID, daySet);
				}
				daySet.add(temp.day);
			}
		}
		
		ArrayList<Entry<Integer,Set<String>>> list = new ArrayList<Map.Entry<Integer, Set<String>>>(cityIdResidentMap.entrySet());
		Collections.sort(list, new Comparator<Entry<Integer,Set<String>>>() {

			@Override
			public int compare(Entry<Integer, Set<String>> o1, Entry<Integer, Set<String>> o2) {
				return o2.getValue().size() - o1.getValue().size();
			}
			
		});
		
		if(!list.isEmpty() && list.get(0).getValue().size() >= 10)
		{
			return true;
		}
		
		return false;
	}
	
	/**
	 * @param 将该常驻用户多天的位置信息(每个小区每种位置源对应一个key)抽取出来，装进map
	 */
	public void dealResidentUserLoc(Text value, HashMap<String, ArrayList<ResidentUserLoc>> allDaysResidentUserLocMap)
	{
		String[] vals = value.toString().split(",|\t", -1);
		ResidentUserLoc dailyResidentUserLoc = new ResidentUserLoc();
		dailyResidentUserLoc.fillDayData(vals);
		if(dailyResidentUserLoc.buildId > 0)	//位置回填只要带经纬度的
		{
			String tempKey = "";
			if((StaticConfig.RULOC_WF1).equals(dailyResidentUserLoc.locSource))
			{
				tempKey = dailyResidentUserLoc.eci + "_" + StaticConfig.RULOC_RU1;
				putMap(tempKey , dailyResidentUserLoc, allDaysResidentUserLocMap);
			}
			else if((StaticConfig.RULOC_WF2).equals(dailyResidentUserLoc.locSource))
			{
				tempKey = dailyResidentUserLoc.eci + "_" + StaticConfig.RULOC_RU2;
				putMap(tempKey , dailyResidentUserLoc, allDaysResidentUserLocMap);
				
				tempKey = dailyResidentUserLoc.eci / 256 + "_" + StaticConfig.RULOC_RU5;
				putMap(tempKey , dailyResidentUserLoc, allDaysResidentUserLocMap);
			}
			else if((StaticConfig.RULOC_WF3).equals(dailyResidentUserLoc.locSource))
			{
				tempKey = dailyResidentUserLoc.eci + "_" + StaticConfig.RULOC_RU3;
				putMap(tempKey , dailyResidentUserLoc, allDaysResidentUserLocMap);
				
				tempKey = dailyResidentUserLoc.eci / 256 + "_" + StaticConfig.RULOC_RU6;
				putMap(tempKey , dailyResidentUserLoc, allDaysResidentUserLocMap);
			}
			else if((StaticConfig.RULOC_WF4).equals(dailyResidentUserLoc.locSource))
			{
				tempKey = dailyResidentUserLoc.eci + "_" + StaticConfig.RULOC_RU4;
				putMap(tempKey , dailyResidentUserLoc, allDaysResidentUserLocMap);
				
				tempKey = dailyResidentUserLoc.eci / 256 + "_" + StaticConfig.RULOC_RU7;
				putMap(tempKey , dailyResidentUserLoc, allDaysResidentUserLocMap);
			}
			else if((StaticConfig.RULOC_WF5).equals(dailyResidentUserLoc.locSource))
			{
				tempKey = dailyResidentUserLoc.eci + "_" + StaticConfig.RULOC_RU8;
				putMap(tempKey , dailyResidentUserLoc, allDaysResidentUserLocMap);
			}
			//暂时隔绝ru9
//			else if((StaticConfig.RULOC_WF6).equals(dailyResidentUserLoc.locSource))
//			{
//				tempKey = dailyResidentUserLoc.eci + "_" + StaticConfig.RULOC_RU9;
//				putMap(tempKey , dailyResidentUserLoc, allDaysResidentUserLocMap);
//			}
			else if((StaticConfig.RULOC_WL).equals(dailyResidentUserLoc.locSource))
			{
				tempKey = dailyResidentUserLoc.eci + "_" + StaticConfig.RULOC_WL;
				putMap(tempKey , dailyResidentUserLoc, allDaysResidentUserLocMap);
			}
			else if((StaticConfig.RULOC_FP).equals(dailyResidentUserLoc.locSource))
			{
				tempKey = dailyResidentUserLoc.eci + "_" + StaticConfig.RULOC_FP;
				putMap(tempKey , dailyResidentUserLoc, allDaysResidentUserLocMap);
			}
			else if((StaticConfig.RULOC_CL).equals(dailyResidentUserLoc.locSource))
			{
				tempKey = dailyResidentUserLoc.eci + "_" + StaticConfig.RULOC_CL;
				putMap(tempKey , dailyResidentUserLoc, allDaysResidentUserLocMap);
			}
		}
	}
	
	//装map
	public void putMap(String tempKey, ResidentUserLoc dailyResidentUser, HashMap<String, ArrayList<ResidentUserLoc>> newDaysResidentUserMap)
	{
		ArrayList<ResidentUserLoc> tempResidentUserList = newDaysResidentUserMap.get(tempKey);
		if (tempResidentUserList == null)
		{
			tempResidentUserList = new ArrayList<ResidentUserLoc>();
			newDaysResidentUserMap.put(tempKey, tempResidentUserList);
		}
		tempResidentUserList.add(dailyResidentUser);
	}
	
	//获取用户常驻小区列表,特殊场景常驻用户按工作时间段和休息时间段总时长（超过2小时为常驻用户）过滤数据
	public void getResidentUserCell(HashMap<Long, Long> eciMap, HashMap<Integer, BaseStation> allDaysBaseStationMap, HashMap<String, ResidentUser> dayResidentUserMap)
	{
		ResidentArea bestResidentArea= null;
		ArrayList<ResidentUser> maxList = null;
		//求出驻留时长最长驻留区域，判别为用户疑似驻留区域。时长最长的基站为疑似驻留基站。（确保每个用户都有常驻小区）
		ArrayList<ResidentArea> residentAreaList = UserResidentCluster.baseStationCluster(1000L, eciMap, allDaysBaseStationMap);
		if(residentAreaList != null && !residentAreaList.isEmpty())
		{
			ResidentArea maxResidentArea = residentAreaList.get(0);
			if(maxResidentArea != null && !maxResidentArea.getBaseStationList().isEmpty())
			{
				maxList = maxResidentArea.getBaseStationList().get(0).getResidentUserList();
				if(maxList != null && !maxList.isEmpty())
				{
					for(ResidentUser item : maxList)
					{
						item.rCellType = StaticConfig.RU_RC0;
						item.getThreeBaseStation(maxResidentArea);
					}
				}
			}
			
			for (ResidentArea residentArea : residentAreaList)
			{
				residentArea.dealResidentAreaDayRatio(workDayNum);
				//求出用户天出现（天出现3个小时以上）超过>=5天 且 驻留时长最长驻留区域，判别为用户疑似驻留区域。时长最长的基站为疑似驻留基站。
				if(residentArea.getDays() >= 5)
				{
					maxList = residentArea.getBaseStationList().get(0).getResidentUserList();
					if(maxList != null && !maxList.isEmpty())
					{
						for(ResidentUser item : maxList)
						{
							item.rCellType = StaticConfig.RU_RC1;
							item.getThreeBaseStation(residentArea);
						}
					}
					break;	//找出用户天出现（天出现3个小时以上）超过>=5天且驻留时长最长驻留区域即跳出
				}
			}
			
			for (ResidentArea residentArea : residentAreaList)
			{
				//计算该驻留区域天出现占比
				residentArea.dealResidentAreaDayRatio(workDayNum);
				//求出用户天出现（天出现3个小时以上）占比60%以上且驻留时长最长驻留区域，判别为用户疑似驻留区域，时长最长的基站为疑似驻留基站。
				if(residentArea.getDayRatio() >= 0.6f)
				{
					bestResidentArea = residentArea;
					maxList = residentArea.getBaseStationList().get(0).getResidentUserList();
					if(maxList != null && !maxList.isEmpty())
					{
						for(ResidentUser item : maxList)
						{
							item.rCellType = StaticConfig.RU_RC2;
							item.getThreeBaseStation(residentArea);
						}
					}
					break;	//找出用户天出现（天出现3个小时以上）占比60%以上且驻留时长最长驻留区域即跳出
				}
			}
		}
		
		//判断用户精准常驻区域内，找出用户驻留基站天驻留时长均在2小时以上的时长最长基站，如有，则判断为精准驻留基站。
		if(bestResidentArea != null)
		{
			for(BaseStation tempBaseStation : bestResidentArea.getBaseStationList())
			{
				// 判断每天基站平均时长如果小于filterTimes就过滤掉
				if(tempBaseStation.getTotalTimes() / workDayNum >= dayFilterTimes)
				{
					maxList = tempBaseStation.getResidentUserList();
					if(maxList != null && !maxList.isEmpty())
					{
						for(ResidentUser item : maxList)
						{
							item.rCellType = StaticConfig.RU_RC3;
							item.getThreeBaseStation(bestResidentArea);
						}
					}
					break;
				}
			}
		}
		
		//找出精准驻留基站赋给该用户
		if(maxList != null && !maxList.isEmpty())
		{
			for(ResidentUser item : maxList)
			{
				String eciHourKey = item.eci + "_" + item.hour;
				if(!dayResidentUserMap.containsKey(eciHourKey))
				{
					dayResidentUserMap.put(eciHourKey, item);
				}
			}
		}
	}
		
	//获取临时常驻小区&将最近出现在驻留地（基站）的时间赋给工作和家庭常驻地用户
	public void getTemporaryResidentCell(HashMap<Integer, ArrayList<ResidentUser>> allDaysResidentUserMap, HashMap<String, ResidentUser> dayResidentUserMap)
	{
		for (ArrayList<ResidentUser> residentUserList : allDaysResidentUserMap.values())
		{
			String eciHourKey = "";
			if(residentUserList != null && residentUserList.size() > 0)
			{
				getMaxRecentlyTime(residentUserList);
				for(ResidentUser item : residentUserList)
				{
					eciHourKey = item.eci + "_" + item.hour;
					if(dayResidentUserMap.containsKey(eciHourKey))
					{
						//将最近出现在驻留地（基站）的时间赋给工作和家庭常驻地用户
						dayResidentUserMap.get(eciHourKey).maxRecentlyTime = item.maxRecentlyTime;
					}
					else
					{
						if(item.duration > hourFilterTimes)	//用户某小时在该小区待半小时就算临时常驻
						{
							dayResidentUserMap.put(eciHourKey, item);
						}
					}
				}
			}
		}
	}
		
	//获取天小区各位置源的用户位置库
	public void getResidentUserCellLoc(HashMap<String, ArrayList<ResidentUserLoc>> allDaysResidentUserLocMap, HashMap<String, ResidentUserLoc> dayResidentUserLocMap)
	{
		for(String eKey : allDaysResidentUserLocMap.keySet())
		{
			ArrayList<ResidentUserLoc> ResidentUserCellLocList = allDaysResidentUserLocMap.get(eKey);
			if(ResidentUserCellLocList != null && ResidentUserCellLocList.size() > 0)
			{
				ResidentUserLoc bestResidentUserLoc = getBestLoc(ResidentUserCellLocList);
				if(bestResidentUserLoc != null && bestResidentUserLoc.longitude > 0)
				{
					if(!dayResidentUserLocMap.containsKey(eKey))
					{
						dayResidentUserLocMap.put(eKey, bestResidentUserLoc);
					}
				}
			}
		}
	}
	
	//取累计时长最长的楼宇和楼层其中的一个经纬度，累计时长最长的方位，作为最合适的位置。
	public ResidentUserLoc getBestLoc(ArrayList<ResidentUserLoc> residentUserLocList)
	{
		long MaxTotalTimes = 0L;
		long MaxPositionTimes = 0L;
		HashMap<String, ResidentUserLoc> mergeResidentUserMap = new HashMap<>();
		HashMap<Integer, ResidentUserLoc> positionMap = new HashMap<>();
		ResidentUserLoc maxResidentUserLoc = null;
			
		for (ResidentUserLoc residentUserLoc : residentUserLocList)
		{
			String tempKey = residentUserLoc.buildId + "_" + residentUserLoc.height;
			ResidentUserLoc tempResidentUserLoc = mergeResidentUserMap.get(tempKey);
			if (tempResidentUserLoc == null)
			{
				mergeResidentUserMap.put(tempKey, residentUserLoc);
			}
			else
			{
				tempResidentUserLoc.duration += residentUserLoc.duration;
			}
			
			//找最累计时长最长的方位
			if(residentUserLoc.position > 0)
			{
				ResidentUserLoc item = positionMap.get(residentUserLoc.position);
				if(item == null)
				{
					item = new ResidentUserLoc();
					item.duration = 0L;
					positionMap.put(residentUserLoc.position, item);
				}
				item.duration += residentUserLoc.duration;
			}
		}
		for(ResidentUserLoc residentUserLoc : mergeResidentUserMap.values())
		{
			if(residentUserLoc.duration > MaxTotalTimes)
			{
				MaxTotalTimes = residentUserLoc.duration;
				maxResidentUserLoc = residentUserLoc;
			}
		}
				
		//找最长时长的用户方位
		for(ResidentUserLoc residentUserLoc : positionMap.values())
		{
			if(residentUserLoc.duration > MaxPositionTimes)
			{
				MaxPositionTimes = residentUserLoc.duration;
				maxResidentUserLoc.position = residentUserLoc.position;
			}
		}
		return maxResidentUserLoc;
	}
	
	//更新位置
	public void updateLoc(HashMap<String, ResidentUserLoc> dayResidentUserLocMap, HashMap<String, ResidentUser> dayResidentUserMap)
	{
		//分别获取时长最长工作和家庭小区
		long workEci = getMaxTimeEci(StaticConfig.WORKTIME, dayResidentUserMap);
		long homeEci = getMaxTimeEci(StaticConfig.HOMETIME, dayResidentUserMap);
		
		for(ResidentUser temp : dayResidentUserMap.values())
		{
			if(temp.locType == StaticConfig.WORKTIME)//工作地点位置回填
			{
				if(!findSameEciLoc(temp, StaticConfig.RULOC_RU1, dayResidentUserLocMap))
				{
					if(!findMaxEciLoc(workEci, temp, StaticConfig.RULOC_RU3, dayResidentUserLocMap))
					{
						if(!findMaxEciLoc(workEci, temp, StaticConfig.RULOC_RU4, dayResidentUserLocMap))
						{
							if(!findSameStationLoc(temp, StaticConfig.RULOC_RU6, dayResidentUserLocMap))
							{
								if(!findSameStationLoc(temp, StaticConfig.RULOC_RU7, dayResidentUserLocMap))
								{
									if(!findSameEciLoc(temp, StaticConfig.RULOC_RU8, dayResidentUserLocMap))
									{
										if(!findMaxEciLoc(workEci, temp, StaticConfig.RULOC_RU9, dayResidentUserLocMap))
										{
											if(!findSameEciLoc(temp, StaticConfig.RULOC_WL, dayResidentUserLocMap))
											{
												findMaxEciLoc(workEci, temp, StaticConfig.RULOC_FP, dayResidentUserLocMap);
											}
										}
									}
								}
							}
						}
					}
				}
			}
			else if(temp.locType == StaticConfig.HOMETIME)//家庭地点位置回填
			{
				if(!findSameEciLoc(temp, StaticConfig.RULOC_RU1, dayResidentUserLocMap))
				{
					if(!findMaxEciLoc(homeEci, temp, StaticConfig.RULOC_RU2, dayResidentUserLocMap))
					{
						if(!findMaxEciLoc(homeEci, temp, StaticConfig.RULOC_RU4, dayResidentUserLocMap))
						{
							if(!findSameStationLoc(temp, StaticConfig.RULOC_RU5, dayResidentUserLocMap))
							{
								if(!findSameStationLoc(temp, StaticConfig.RULOC_RU7, dayResidentUserLocMap))
								{
									if(!findSameEciLoc(temp, StaticConfig.RULOC_RU8, dayResidentUserLocMap))
									{
										if(!findMaxEciLoc(homeEci, temp, StaticConfig.RULOC_RU9, dayResidentUserLocMap))
										{
											if(!findSameEciLoc(temp, StaticConfig.RULOC_WL, dayResidentUserLocMap))
											{
												findMaxEciLoc(homeEci, temp, StaticConfig.RULOC_FP, dayResidentUserLocMap);
											}
										}
									}
								}
							}
						}
					}
				}
			}
			else//其他时间位置回填
			{
				if(!findSameEciLoc(temp, StaticConfig.RULOC_RU1, dayResidentUserLocMap))
				{
					if(!findSameEciLoc(temp, StaticConfig.RULOC_RU4, dayResidentUserLocMap))
					{
						if(!findSameStationLoc(temp, StaticConfig.RULOC_RU7, dayResidentUserLocMap))
						{
							if(!findSameEciLoc(temp, StaticConfig.RULOC_RU8, dayResidentUserLocMap))
							{
								if(!findSameEciLoc(temp, StaticConfig.RULOC_RU9, dayResidentUserLocMap))
								{
									if(!findSameEciLoc(temp, StaticConfig.RULOC_WL, dayResidentUserLocMap))
									{
										if(!findSameEciLoc(temp, StaticConfig.RULOC_FP, dayResidentUserLocMap))
										{
											findSameEciLoc(temp, StaticConfig.RULOC_CL, dayResidentUserLocMap);
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}
	
	//找最长时长小区eci
	public long getMaxTimeEci(int locType, HashMap<String, ResidentUser> dayResidentUserMap)
	{
		long maxTotalTimes = 0;
		long eci = StaticConfig.Long_Abnormal;
		for(ResidentUser temp : dayResidentUserMap.values())
		{
			if(temp.locType == locType)//工作时间位置回填
			{
				if(temp.totalTimes > maxTotalTimes)
				{
					maxTotalTimes = temp.totalTimes;
					eci = temp.eci;
				}
			}
		}
		return eci;
	}
	
	//找时长最长小区下的位置
	public boolean findMaxEciLoc(long eci, ResidentUser temp, String locSource, HashMap<String, ResidentUserLoc> dayResidentUserLocMap)
	{
		String locKey = eci + "_" + locSource;
		ResidentUserLoc bestResidentUserLoc = dayResidentUserLocMap.get(locKey);
		if(bestResidentUserLoc != null)
		{
			fillLoc(bestResidentUserLoc, temp);
			temp.locSource = locSource;
			return true;
		}
		return false;
	}
	
	//找同小区下的位置
	public boolean findSameEciLoc(ResidentUser temp, String locSource, HashMap<String, ResidentUserLoc> dayResidentUserLocMap)
	{
		String locKey = temp.eci + "_" + locSource;
		ResidentUserLoc bestResidentUserLoc = dayResidentUserLocMap.get(locKey);
		if(bestResidentUserLoc != null)
		{
			fillLoc(bestResidentUserLoc, temp);
			temp.locSource = locSource;
			return true;
		}
		return false;
	}
	
	//找同基站下的位置
	public boolean findSameStationLoc(ResidentUser temp, String locSource, HashMap<String, ResidentUserLoc> dayResidentUserLocMap)
	{
		String locKey = temp.eci / 256 + "_" + locSource;
		ResidentUserLoc bestResidentUserLoc = dayResidentUserLocMap.get(locKey);
		if(bestResidentUserLoc != null)
		{
			fillLoc(bestResidentUserLoc, temp);
			temp.locSource = locSource;
			return true;
		}
		return false;
	}
	
	//填位置
	public void fillLoc(ResidentUserLoc bestResidentUserLoc, ResidentUser residentUser)
	{
		residentUser.longitude = bestResidentUserLoc.longitude;
		residentUser.latitude = bestResidentUserLoc.latitude;
		residentUser.buildId = bestResidentUserLoc.buildId;
		residentUser.height = bestResidentUserLoc.height;
		residentUser.position = bestResidentUserLoc.position;
		residentUser.locSource = bestResidentUserLoc.locSource;
	}
	
	//获取最近出现在驻留地（基站）的时间
	public void getMaxRecentlyTime(ArrayList<ResidentUser> tempList)
	{
		String maxRecentlyTime = "0";
		for(ResidentUser temp : tempList)
		{
			try {
					if(Integer.parseInt(temp.day) > Integer.parseInt(maxRecentlyTime))
					{
						maxRecentlyTime = temp.day;
					}
				} 
				catch (NumberFormatException e)
				{
					LOGHelper.GetLogger().writeLog(LogType.error,"get maxRecentlyTime error", "get maxRecentlyTime error", e);
				}
		}
		
		for(ResidentUser temp : tempList)
		{
			temp.maxRecentlyTime = maxRecentlyTime;
		}
	}
	
	public void getNewResidentUser(HashMap<String, ResidentUser> dayResidentUserMap, HashMap<String, NewResidentUser> NewResidentUserMap)
	{
		for (ResidentUser temp : dayResidentUserMap.values())
		{
			//过滤掉临时常驻
			if(temp.locType == StaticConfig.OTHERTIME)
			{
				continue;
			}
			
			temp.enSureInfo();
			if(!(StaticConfig.RULOC_CL).equals(temp.locSource))
			{
				//假如重心点落在物业点内，则直接回填物业点。假如重心点不在物业点内，则计算附近1公里的物业点，并且物业点的小区列表内包含用户驻留小区3强，求出满足条件的最近的一个物业点，进行回填。
				PropertyInfo propertyInfo = PropertyConfig.GetInstance().getAreaID((int) temp.longitude, (int) temp.latitude, temp.getEciSet());
				if(propertyInfo != null)
				{
					temp.areaID1 = propertyInfo.areaID;
					temp.areaID2 = propertyInfo.business_community;
				}
			}
			
			NewResidentUser item = new NewResidentUser();
			item.fillData(temp);
			item.networkType = "lte";
			item.recentlyTime = temp.maxRecentlyTime;
			item.operationCycle = -mergeUserResidentDayNum;
			item.effectiveDays = workDayNum;
			item.finalTime = finalTime;
			item.residentState = 1;
			
			String tempKey = item.eci + "_" + item.locType;
			if(!NewResidentUserMap.containsKey(tempKey))
			{
				NewResidentUserMap.put(tempKey, item);
			}
		}
	}
	
	//吐出旧版常驻用户数据
	public void save1(HashMap<String, ResidentUser> dayResidentUserMap, HashMap<String, ResidentUser> oldResidentUserMap)
	{
		if (dayResidentUserMap.size() > 0 && oldResidentUserMap.size() > 0)
		{
			for (String mapkey : dayResidentUserMap.keySet())
			{
				ResidentUser dailyResidentUser = dayResidentUserMap.get(mapkey);
				ResidentUser oldResidentUser = oldResidentUserMap.get(mapkey);
				// 旧常驻用户沒有，新常驻用户有；取置信度高(值越大精度越高)的，相同置信度取新的
				if (oldResidentUser == null || dailyResidentUser.confidenceLevel >= oldResidentUser.confidenceLevel)
				{
					// 用新的去更新旧的，都存在旧的里面。
					oldResidentUserMap.put(mapkey, dailyResidentUser);
				}
			}
			saveFile(oldResidentUserMap);
		}
		else if (dayResidentUserMap.size() > 0)
		{
			saveFile(dayResidentUserMap);
		}
		else if (oldResidentUserMap.size() > 0)
		{
			saveFile(oldResidentUserMap);
		}
	}
	
	//吐出新版常驻用户数据
	public void save2(HashMap<String, NewResidentUser> NewResidentUserMap, HashMap<String, NewResidentUser> oldNewResidentUserMap)
	{
		if (NewResidentUserMap.size() > 0 && oldNewResidentUserMap.size() > 0)
		{
			for (String mapkey : NewResidentUserMap.keySet())
			{
				NewResidentUser dailyResidentUser = NewResidentUserMap.get(mapkey);
				NewResidentUser oldResidentUser = oldNewResidentUserMap.get(mapkey);
				// 旧常驻用户沒有，新常驻用户有；取置信度高(值越大精度越高)的，相同置信度取新的
				if (oldResidentUser == null || dailyResidentUser.confidenceLevel >= oldResidentUser.confidenceLevel)
				{
					// 用新的去更新旧的，都存在旧的里面。
					oldNewResidentUserMap.put(mapkey, dailyResidentUser);
				}
				else
				{
					oldResidentUser.residentState = 2;
				}
			}
			saveFile2(oldNewResidentUserMap);
		}
		else if (NewResidentUserMap.size() > 0)
		{
			saveFile2(NewResidentUserMap);
		}
		else if (oldNewResidentUserMap.size() > 0)
		{
			for (NewResidentUser oldResidentUser : oldNewResidentUserMap.values())
			{
				oldResidentUser.residentState = 2;
			}
			saveFile2(oldNewResidentUserMap);
		}
	}
	
	/**
	 * @param 吐出旧版常驻用户数据
	 */
	public void saveFile(HashMap<String, ResidentUser> residentMap)
	{
		for (ResidentUser temp : residentMap.values())
		{
			try
			{
				resultOutputer.pushData(ResidentUserTablesEnum.Merge_Resident_User.getIndex(), temp.toEncryptLine());
				resultOutputer.pushData(ResidentUserTablesEnum.Merge_Resident_User_UnEncrypt.getIndex(), temp.toUnEncryptLine());
				resultOutputer.pushData(ResidentConfigTablesEnum.Merge_Resident_User.getIndex(), temp.toUnEncryptLine());
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error,"save error", "save error", e);
			}
		}
	}
	
	/**
	 */
	public void saveFile2(HashMap<String, NewResidentUser> residentMap)
	{
		for (NewResidentUser temp : residentMap.values())
		{
			try
			{
				resultOutputer.pushData(ResidentUserTablesEnum.User_Resident_Location.getIndex(), temp.toEncryptLine());
				resultOutputer.pushData(ResidentUserTablesEnum.User_Resident_Location_UnEncrypt.getIndex(), temp.toUnEncryptLine());
				resultOutputer.pushData(ResidentConfigTablesEnum.User_Resident_Location.getIndex(), temp.toUnEncryptLine());
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error,"save2 error", "save2 error", e);
			}
		}
	}

}
