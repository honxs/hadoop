package cn.mastercom.bigdata.xdr.loc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.conf.config.PropertyConfig;
import cn.mastercom.bigdata.conf.config.PropertyInfo;
import cn.mastercom.bigdata.stat.userResident.homeBroadbandLoc.BaseStation;
import cn.mastercom.bigdata.stat.userResident.homeBroadbandLoc.ResidentArea;
import cn.mastercom.bigdata.util.Func;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.util.LOGHelper;

public class ResidentUser
{
	public int cityID;
	public String day; // 天常驻用户日期
	public long imsi;
	public String msisdn;
	public String imei; // 设备号
	public int isPm; // 1：白天2：夜晚
	public int hour;
	public long eci;
	public long duration;
	public long longitude;
	public long latitude;
	public int buildId; // 楼宇id
	public int height; // 楼层
	public int isIndoor; // 室内室外
	public int rCellType;	//常驻小区类型
	public int locType; // 驻点类型
	public String locSource; // 位置来源
	public String dataStatus; // 数据状态
	public long updateTime; // 数据更新时间
	public int confidenceLevel; // 置信度
	public int position;
	public int areaID1;	//物业点1
	public String areaID2;	//物业点2
	
	//yzx add 2018/09/15
	public long testLongitude;	//测试经度
	public long testLatitude;	//测试纬度
	public int testBuildId;	//测试楼宇
	public int testAreaID1;	//测试物业点1
	public String testAreaID2;	//测试物业点2
	
	public long totalLongitude; // 一小时某用户在某小区上报总的经度
	public long totalLatitude;
	public int totalNum; // 聚类个数
	public long totalTimes;// 该小区总停留时长
	public int num; // 一小时某用户在某小区上报的次数

	public int testType; //运动状态
	
	public String maxRecentlyTime;	//最近出现在驻留地时间
	
	private ArrayList<BaseStation> threeBaseStationList;	//该用户所驻留的时长前三的基站
	private Set<Long> eciSet;	//该用户所驻留的时长前三的基站里面驻留时长最长的三个小区列表
	
	public static final String spliter = "\t";

	public ResidentUser()
	{
		clean();
	}

	public void clean()
	{
		cityID = -1;
		day = "";
		imsi = 0;
		msisdn = "";
		imei = "";
		isPm = 0;
		hour = -1;
		eci = -1000000;
		duration = 0;
		longitude = 0;
		latitude = 0;
		buildId = -1;
		height = -1;
		isIndoor = 0;
		rCellType = -1;
		locType = 0;
		locSource = "";
		dataStatus = "";
		updateTime = 0;
		confidenceLevel = 0;
		position = 0;
		areaID1 = -1;
		areaID2 = "";
		
		testLongitude = 0;
		testLatitude = 0;
		testBuildId = -1;
		testAreaID1 = -1;
		testAreaID2 = "";
		
		totalLongitude = 0;
		totalLatitude = 0;
		totalNum = 0;
		totalTimes = 0;
		num = 0;
		testType = 0;
		maxRecentlyTime = "";
		threeBaseStationList =  new ArrayList<>();
		eciSet = new HashSet<Long>();
	}

	//使用天数据（带day字段）
	public ResidentUser(CellSwitInfo cellInfo, String day, int dayhour)
	{
		try {
				this.cityID = cellInfo.cityID;
				this.day = day;
				this.imsi = cellInfo.imsi;
				this.msisdn = cellInfo.msisdn;
				this.imei = cellInfo.imei;
				this.isPm = 0;
				this.hour = dayhour;
				this.eci = cellInfo.eci;
				this.duration = 0;
				this.longitude = cellInfo.longtitude;
				this.latitude = cellInfo.latitude;
				this.buildId = -1;
				this.height = -1;
				this.isIndoor = -1;
				this.rCellType = -1;
				this.locType = 0;
				this.locSource = cellInfo.locSource;
				this.dataStatus = "";
				this.updateTime = 0;
				this.confidenceLevel = 0;
				this.position = 0;
				this.areaID1 = -1;
				this.areaID2 = "";
				
				this.testType = cellInfo.testType;
				this.totalLongitude = 0;
				this.totalLatitude = 0;
				this.totalNum = 0;
				this.totalTimes = 0;
				this.num = 0;
		} 
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error, "ResidentUser init error","ResidentUser init error", e);
		}
	}

	public void getPmOrAm()
	{
		if ((hour >= 9 && hour <= 11) || (hour >= 14 && hour <= 17))
		{
			isPm = 1;
			locType = 1;
		}
		else if((hour >= 0 && hour <= 5) || (hour >= 20 && hour <= 23))
		{
			isPm = 2;
			locType = 2;
		}
		else
		{
			isPm = 3;
			locType = 3;
		}
	}

	//使用天数据（带day字段）
	public void fillDayData(String[] vals)
	{
		try {
				int i = 0;
				cityID = Integer.parseInt(vals[i++]);
				day = vals[i++];
				imsi = Long.parseLong(vals[i++]);
				msisdn = vals[i++];
				imei = vals[i++];
				isPm = Integer.parseInt(vals[i++]);
				hour = Integer.parseInt(vals[i++]);
				eci = Long.parseLong(vals[i++]);
				duration = Long.parseLong(vals[i++]);
				longitude = Long.parseLong(vals[i++]);
				latitude = Long.parseLong(vals[i++]);
				buildId = Integer.parseInt(vals[i++]);
				height = Integer.parseInt(vals[i++]);
				isIndoor = Integer.parseInt(vals[i++]);
				locType = Integer.parseInt(vals[i++]);
				locSource = vals[i++];
				dataStatus = vals[i++];
				updateTime = Long.parseLong(vals[i++]);
				confidenceLevel = Integer.parseInt(vals[i++]);
				position = Integer.parseInt(vals[i++]);
			} 
			catch (NumberFormatException e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error,"fillDayData error", "fillDayData error", e);
			}
	}
	
	//使用汇聚后的数据（不带day字段）
	public void fillMergeData(String[] vals)
	{
		try {
				int i = 0;
				cityID = Integer.parseInt(vals[i++]);
				imsi = Long.parseLong(vals[i++]);
				msisdn = vals[i++];
				imei = vals[i++];
				isPm = Integer.parseInt(vals[i++]);
				hour = Integer.parseInt(vals[i++]);
				eci = Long.parseLong(vals[i++]);
				duration = Long.parseLong(vals[i++]);
				longitude = Long.parseLong(vals[i++]);
				latitude = Long.parseLong(vals[i++]);
				buildId = Integer.parseInt(vals[i++]);
				height = Integer.parseInt(vals[i++]);
				isIndoor = Integer.parseInt(vals[i++]);
				rCellType = Integer.parseInt(vals[i++]);
				locType = Integer.parseInt(vals[i++]);
				locSource = vals[i++];
				dataStatus = vals[i++];
				updateTime = Long.parseLong(vals[i++]);
				confidenceLevel = Integer.parseInt(vals[i++]);
				position = Integer.parseInt(vals[i++]);
				areaID1 = Integer.parseInt(vals[i++]);
				areaID2 = vals[i++];
			} 
			catch (NumberFormatException e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error,"fillMergeData error", "fillMergeData error", e);
			}
	}
	
	//使用天数据（带day字段）
	public void fillData2(ResidentUser residentUser)
	{
		try {
				cityID = residentUser.cityID;
				day = residentUser.day;
				imsi = residentUser.imsi;
				msisdn = residentUser.msisdn;
				imei = residentUser.imei;
				isPm = residentUser.isPm;
				hour = residentUser.hour;
				eci = residentUser.eci;
				duration = residentUser.duration;
				longitude = residentUser.longitude;
				latitude = residentUser.latitude;
				buildId = residentUser.buildId;
				height = residentUser.height;
				isIndoor = residentUser.isIndoor;
				locType = residentUser.locType;
				locSource = residentUser.locSource;
				dataStatus = residentUser.dataStatus;
				updateTime = residentUser.updateTime;
				confidenceLevel = residentUser.confidenceLevel;
				position = residentUser.position;
			} 
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error,"fillData2 error", "fillData2 error", e);
			}
	}
	
	//使用天数据（带day字段）
	public void fillData3(String[] vals)
	{
		try {
				int i = 0;
				cityID = Integer.parseInt(vals[i++]);
				day = vals[i++];
				imsi = Long.parseLong(vals[i++]);
				msisdn = vals[i++];
				imei = vals[i++];
				isPm = Integer.parseInt(vals[i++]);
				hour = Integer.parseInt(vals[i++]);
				eci = Long.parseLong(vals[i++]);
				duration = Long.parseLong(vals[i++]);
				i++;
				i++;
				i++;
				i++;
				isIndoor = Integer.parseInt(vals[i++]);
				locType = Integer.parseInt(vals[i++]);
				i++;
				i++;
				i++;
				i++;
				i++;
		} 
		catch (NumberFormatException e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error,"fillData3 error", "fillData3 error", e);
		}
	}

	/**
	 * 不进行用户信息加密，吐出天数据，带day字段
	 * 
	 * @return
	 */
	public String ToDayLine()
	{
		StringBuffer sb = new StringBuffer();
		sb.delete(0, sb.length());
		sb.append(cityID);
		sb.append(spliter);
		sb.append(day);
		sb.append(spliter);
		sb.append(imsi);
		sb.append(spliter);
		sb.append(msisdn);
		sb.append(spliter);
		sb.append(imei);
		sb.append(spliter);
		sb.append(isPm);
		sb.append(spliter);
		sb.append(hour);
		sb.append(spliter);
		sb.append(eci);
		sb.append(spliter);
		sb.append(duration);
		sb.append(spliter);
		sb.append(longitude);
		sb.append(spliter);
		sb.append(latitude);
		sb.append(spliter);
		sb.append(buildId);
		sb.append(spliter);
		sb.append(height);
		sb.append(spliter);
		sb.append(isIndoor);
		sb.append(spliter);
		sb.append(locType);
		sb.append(spliter);
		sb.append(locSource);
		sb.append(spliter);
		sb.append(dataStatus);
		sb.append(spliter);
		sb.append(updateTime);
		sb.append(spliter);
		sb.append(confidenceLevel);
		sb.append(spliter);
		sb.append(position);

		return sb.toString();
	}
	
	/**
	 * MERGE_RESIDENTUSER汇聚吐出时不进行用户信息加密，不带day字段
	 */
	public String toUnEncryptLine()
	{
		StringBuffer sb = new StringBuffer();
		sb.delete(0, sb.length());
		sb.append(cityID);
		sb.append(spliter);
		sb.append(imsi);
		sb.append(spliter);
		sb.append(msisdn);
		sb.append(spliter);
		sb.append(imei);
		sb.append(spliter);
		sb.append(isPm);
		sb.append(spliter);
		sb.append(hour);
		sb.append(spliter);
		sb.append(eci);
		sb.append(spliter);
		sb.append(duration);
		sb.append(spliter);
		sb.append(longitude);
		sb.append(spliter);
		sb.append(latitude);
		sb.append(spliter);
		sb.append(buildId);
		sb.append(spliter);
		sb.append(height);
		sb.append(spliter);
		sb.append(isIndoor);
		sb.append(spliter);
		sb.append(rCellType);
		sb.append(spliter);
		sb.append(locType);
		sb.append(spliter);
		sb.append(locSource);
		sb.append(spliter);
		sb.append(dataStatus);
		sb.append(spliter);
		sb.append(updateTime);
		sb.append(spliter);
		sb.append(confidenceLevel);
		sb.append(spliter);
		sb.append(position);
		sb.append(spliter);
		sb.append(areaID1);
		sb.append(spliter);
		sb.append(areaID2);

		return sb.toString();
	}

	/**
	 *MERGE_RESIDENTUSER汇聚吐出时进行用户信息加密，不带day字段
	 */
	public String toEncryptLine()
	{
		StringBuffer sb = new StringBuffer();
		sb.delete(0, sb.length());
		sb.append(cityID);
		sb.append(spliter);
		sb.append(Func.getEncryptResident(imsi));
		sb.append(spliter);
		sb.append(Func.getEncryptResident(msisdn));
		sb.append(spliter);
		sb.append(imei);
		sb.append(spliter);
		sb.append(isPm);
		sb.append(spliter);
		sb.append(hour);
		sb.append(spliter);
		sb.append(eci);
		sb.append(spliter);
		sb.append(duration);
		sb.append(spliter);
		sb.append(longitude);
		sb.append(spliter);
		sb.append(latitude);
		sb.append(spliter);
		sb.append(buildId);
		sb.append(spliter);
		sb.append(height);
		sb.append(spliter);
		sb.append(isIndoor);
		sb.append(spliter);
		sb.append(rCellType);
		sb.append(spliter);
		sb.append(locType);
		sb.append(spliter);
		sb.append(locSource);
		sb.append(spliter);
		sb.append(dataStatus);
		sb.append(spliter);
		sb.append(updateTime);
		sb.append(spliter);
		sb.append(confidenceLevel);
		sb.append(spliter);
		sb.append(position);
		sb.append(spliter);
		sb.append(areaID1);
		sb.append(spliter);
		sb.append(areaID2);
		
		return sb.toString();
	}

	public void enSureInfo()
	{
		if (buildId > 0)
		{
			isIndoor = 1;
		}
		else
		{
			isIndoor = 2;
		}
		confidenceLevel = getConfidenceLevel(locSource);
	}
	
	public int getConfidenceLevel(String locSource)
	{
		if (locSource.contains(StaticConfig.RULOC_RU1))
		{
			return StaticConfig.RU_RU1;
		}
		else if (locSource.contains(StaticConfig.RULOC_RU2))
		{
			return StaticConfig.RU_RU2;
		}
		else if (locSource.contains(StaticConfig.RULOC_RU3))
		{
			return StaticConfig.RU_RU3;
		}
		else if (locSource.contains(StaticConfig.RULOC_RU4))
		{
			return StaticConfig.RU_RU4;
		}
		else if (locSource.contains(StaticConfig.RULOC_RU5))
		{
			return StaticConfig.RU_RU5;
		}
		else if (locSource.contains(StaticConfig.RULOC_RU6))
		{
			return StaticConfig.RU_RU6;
		}
		else if (locSource.contains(StaticConfig.RULOC_RU7))
		{
			return StaticConfig.RU_RU7;
		}
		else if (locSource.contains(StaticConfig.RULOC_RU8))
		{
			return StaticConfig.RU_RU8;
		}
		else if (locSource.contains(StaticConfig.RULOC_RU9))
		{
			return StaticConfig.RU_RU9;
		}
		else if (locSource.contains(StaticConfig.RULOC_WL))
		{
			return StaticConfig.RU_WL;
		}
		else if (locSource.contains(StaticConfig.RULOC_FP))
		{
			return StaticConfig.RU_FP;
		}
		else if (locSource.contains(StaticConfig.RULOC_CL))
		{
			return StaticConfig.RU_CL;
		}
		return 0;
	}
	
	public void updateData(ResidentUser residentUser)
	{
		totalNum++;
	}
	
	public ArrayList<BaseStation> getThreeBaseStationList() {
		return threeBaseStationList;
	}

	public void setThreeBaseStationList(ArrayList<BaseStation> threeBaseStationList) {
		this.threeBaseStationList = threeBaseStationList;
	}
	
	public Set<Long> getEciSet() {
		return eciSet;
	}

	public void setEciSet(Set<Long> eciSet) {
		this.eciSet = eciSet;
	}

	//获取该用户所驻留的时长前三的基站
	public void getThreeBaseStation(ResidentArea residentArea)
	{
		for (BaseStation baseStation : residentArea.getBaseStationList())
		{
			threeBaseStationList.add(baseStation);
			if(threeBaseStationList.size() >= 3)
			{
				break;
			}
		}
		
		//基于每个基站的占用时长，求出用户的重心位置
		long allTimes = 0L;
		for (BaseStation baseStation : threeBaseStationList)
		{
			allTimes += baseStation.getTotalTimes();
		}
		
		testLongitude = 0;
		testLatitude = 0;
		for (int i = 0; i < threeBaseStationList.size(); i++)
		{
			testLongitude += threeBaseStationList.get(i).getLongitude() * (threeBaseStationList.get(i).getTotalTimes() * 1.0 / allTimes);
			testLatitude += threeBaseStationList.get(i).getLatitude() * (threeBaseStationList.get(i).getTotalTimes() * 1.0 / allTimes);
		}
		longitude = testLongitude;
		latitude = testLatitude;
		locSource = StaticConfig.RULOC_CL;
		
		getEciList(threeBaseStationList);
		//假如重心点落在物业点内，则直接回填物业点。假如重心点不在物业点内，则计算附近1公里的物业点，并且物业点的小区列表内包含用户驻留小区3强，求出满足条件的最近的一个物业点，进行回填。
		PropertyInfo propertyInfo = PropertyConfig.GetInstance().getAreaID((int)testLongitude, (int)testLatitude, eciSet);
		if(propertyInfo != null)
		{
			testAreaID1 = propertyInfo.areaID;
			testAreaID2 = propertyInfo.business_community;
		}
		
		areaID1 = testAreaID1;
		areaID2 = testAreaID2;
	}
	
	//求出用户的驻留小区列表3强。通过获取常驻区域内的三个基站，获取驻留时长最长的三个小区，作为用户的驻留小区列表。
	public void getEciList(ArrayList<BaseStation> threeBaseStationList)
	{
		HashMap<Long, Long> eciMap = new HashMap<>();
		for (BaseStation baseStation : threeBaseStationList)
		{
			for (ResidentUser residentUser : baseStation.getResidentUserList())
			{
				if(!eciMap.containsKey(residentUser.eci))
				{
					eciMap.put(residentUser.eci, residentUser.totalTimes);
				}
			}
		}
		
		ArrayList<Entry<Long, Long>> list = new ArrayList<Map.Entry<Long, Long>>(eciMap.entrySet());
		Collections.sort(list, new Comparator<Entry<Long, Long>>() {
			@Override
			public int compare(Entry<Long, Long> o1, Entry<Long, Long> o2) {
				return (int) (o2.getValue() - o1.getValue());
			}
		});
		
		for (Entry<Long, Long> entry : list)
		{
			eciSet.add(entry.getKey());
			if(eciSet.size() >= 3)
			{
				break;
			}
		}
	}
}
