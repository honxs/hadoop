package cn.mastercom.bigdata.stat.userResident.homeBroadbandLoc;

import cn.mastercom.bigdata.util.Func;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.xdr.loc.ResidentUser;

public class NewResidentUser
{
	public int cityID;
	public long imsi;
	public String msisdn;
	public String imei; // 设备号
	public String networkType; // 网络类型
	public int locType; // 驻点类型
	public long eci;
	public int rCellType;	//常驻小区类型
	public long longitude;
	public long latitude;
	public int buildId; // 楼宇id
	public int height; // 楼层
	public int position;
	public String locSource; // 位置来源
	public int locAccuracy;	//定位精度
	public int confidenceLevel; // 置信度
	public String recentlyTime;	//最近出现在驻留地时间
	public int operationCycle ;	//运算周期
	public int effectiveDays;	//有效运算天数
	public long duration;	//驻留总时长
	public String finalTime;	//最近判别为常驻的时间
	public int residentState;	//驻留状态
	public int locationInfo1;	//位置信息1
	public String locationInfo2;	//位置信息2
	public String locationInfo3;	//位置信息3
	
	//yzx add 2018/09/15
	public long testLongitude;	//测试经度
	public long testLatitude;	//测试纬度
	public int testBuildId;	//测试楼宇
	public int testLocationInfo1;	//测试物业点1
	public String testLocationInfo2;	//测试物业点2
	
	public static final String spliter = "\t";

	public NewResidentUser()
	{
		clean();
	}

	public void clean()
	{
		cityID = -1;
		imsi = 0;
		msisdn = "";
		imei = "";
		networkType = "";
		locType = 0;
		eci = -1000000;
		rCellType = -1;
		longitude = 0;
		latitude = 0;
		buildId = -1;
		height = -1;
		position = 0;
		locSource = "";
		locAccuracy = 0;
		confidenceLevel = 0;
		recentlyTime = "";
		operationCycle = 0;
		effectiveDays = 0;
		duration = 0;
		finalTime = "";
		residentState = 0;
		locationInfo1 = -1;
		locationInfo2 = "";
		locationInfo3 = "";
		
		testLongitude = 0;
		testLatitude = 0;
		testBuildId = -1;
		testLocationInfo1 = -1;
		testLocationInfo2 = "";
	}
	
	//使用旧版常驻用户数据
	public void fillData(ResidentUser residentUser)
	{
		try {
				cityID = residentUser.cityID;
				imsi = residentUser.imsi;
				msisdn = residentUser.msisdn;
				imei = residentUser.imei;
				locType = residentUser.locType;
				eci = residentUser.eci;
				rCellType = residentUser.rCellType;
				longitude = residentUser.longitude;
				latitude = residentUser.latitude;
				buildId = residentUser.buildId;
				height = residentUser.height;
				position = residentUser.position;
				locSource = residentUser.locSource;
				confidenceLevel = residentUser.confidenceLevel;
				duration = residentUser.totalTimes;
				locationInfo1 = residentUser.areaID1;
				locationInfo2 = residentUser.areaID2;
				
				testLongitude = residentUser.testLongitude;
				testLatitude = residentUser.testLatitude;
				testBuildId = residentUser.testBuildId;
				testLocationInfo1 = residentUser.testAreaID1;
				testLocationInfo2 = residentUser.testAreaID2;
			} 
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "fillData error","fillData error", e);
			}
	}
	
	//使用汇聚后的常驻用户数据
	public void fillMergeData(String[] vals)
	{
		try {
				int i = 0;
				cityID = Integer.parseInt(vals[i++]);
				imsi = Long.parseLong(vals[i++]);
				msisdn = vals[i++];
				imei = vals[i++];
				networkType = vals[i++];
				locType = Integer.parseInt(vals[i++]);
				eci = Long.parseLong(vals[i++]);
				rCellType = Integer.parseInt(vals[i++]);
				longitude = Long.parseLong(vals[i++]);
				latitude = Long.parseLong(vals[i++]);
				buildId = Integer.parseInt(vals[i++]);
				height = Integer.parseInt(vals[i++]);
				position = Integer.parseInt(vals[i++]);
				locSource = vals[i++];
				locAccuracy = Integer.parseInt(vals[i++]);
				confidenceLevel = Integer.parseInt(vals[i++]);
				recentlyTime = vals[i++];
				operationCycle = Integer.parseInt(vals[i++]);
				effectiveDays = Integer.parseInt(vals[i++]);
				duration = Long.parseLong(vals[i++]);
				finalTime = vals[i++];
				residentState = Integer.parseInt(vals[i++]);
				locationInfo1 = Integer.parseInt(vals[i++]);
				locationInfo2 = vals[i++];
				locationInfo3 = vals[i++];
				testLongitude = Long.parseLong(vals[i++]);
				testLatitude = Long.parseLong(vals[i++]);
				testBuildId = Integer.parseInt(vals[i++]);
				testLocationInfo1 = Integer.parseInt(vals[i++]);
				testLocationInfo2 = vals[i++];
			} 
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error,"fillMergeData error", "fillMergeData error", e);
			}
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
		sb.append(networkType);
		sb.append(spliter);
		sb.append(locType);
		sb.append(spliter);
		sb.append(eci);
		sb.append(spliter);
		sb.append(rCellType);
		sb.append(spliter);
		sb.append(longitude);
		sb.append(spliter);
		sb.append(latitude);
		sb.append(spliter);
		sb.append(buildId);
		sb.append(spliter);
		sb.append(height);
		sb.append(spliter);
		sb.append(position);
		sb.append(spliter);
		sb.append(locSource);
		sb.append(spliter);
		sb.append(locAccuracy);
		sb.append(spliter);
		sb.append(confidenceLevel);
		sb.append(spliter);
		sb.append(recentlyTime);
		sb.append(spliter);
		sb.append(operationCycle);
		sb.append(spliter);
		sb.append(effectiveDays);
		sb.append(spliter);
		sb.append(duration);
		sb.append(spliter);
		sb.append(finalTime);
		sb.append(spliter);
		sb.append(residentState);
		sb.append(spliter);
		sb.append(locationInfo1);
		sb.append(spliter);
		sb.append(locationInfo2);
		sb.append(spliter);
		sb.append(locationInfo3);
		sb.append(spliter);
		sb.append(testLongitude);
		sb.append(spliter);
		sb.append(testLatitude);
		sb.append(spliter);
		sb.append(testBuildId);
		sb.append(spliter);
		sb.append(testLocationInfo1);
		sb.append(spliter);
		sb.append(testLocationInfo2);

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
		sb.append(networkType);
		sb.append(spliter);
		sb.append(locType);
		sb.append(spliter);
		sb.append(eci);
		sb.append(spliter);
		sb.append(rCellType);
		sb.append(spliter);
		sb.append(longitude);
		sb.append(spliter);
		sb.append(latitude);
		sb.append(spliter);
		sb.append(buildId);
		sb.append(spliter);
		sb.append(height);
		sb.append(spliter);
		sb.append(position);
		sb.append(spliter);
		sb.append(locSource);
		sb.append(spliter);
		sb.append(locAccuracy);
		sb.append(spliter);
		sb.append(confidenceLevel);
		sb.append(spliter);
		sb.append(recentlyTime);
		sb.append(spliter);
		sb.append(operationCycle);
		sb.append(spliter);
		sb.append(effectiveDays);
		sb.append(spliter);
		sb.append(duration);
		sb.append(spliter);
		sb.append(finalTime);
		sb.append(spliter);
		sb.append(residentState);
		sb.append(spliter);
		sb.append(locationInfo1);
		sb.append(spliter);
		sb.append(locationInfo2);
		sb.append(spliter);
		sb.append(locationInfo3);
		sb.append(spliter);
		sb.append(testLongitude);
		sb.append(spliter);
		sb.append(testLatitude);
		sb.append(spliter);
		sb.append(testBuildId);
		sb.append(spliter);
		sb.append(testLocationInfo1);
		sb.append(spliter);
		sb.append(testLocationInfo2);

		return sb.toString();
	}	
}
