package cn.mastercom.bigdata.loc.userResident;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.util.GisFunction;
import cn.mastercom.bigdata.xdr.loc.ResidentUser;
import cn.mastercom.bigdata.evt.locall.stat.LocItem;
import cn.mastercom.bigdata.stat.userResident.homeBroadbandLoc.BaseStation;
import cn.mastercom.bigdata.stat.userResident.homeBroadbandLoc.ResidentArea;

public class UserResidentCluster
{
	//RESIDENT_LOC使用，对位置库聚类
	public static LocItem cluster(Long mroLocLibDistance, ArrayList<LocItem> mroList)
	{
		if(mroList.size() == 0)
		{
			return null;
		}
		ArrayList<ArrayList<LocItem>> allList = new ArrayList<>();
		long minDistance = 0; // 最小距离
		long tempDistance = 0; // 缓存距离

		for (LocItem tempMro : mroList)
		{
			minDistance = mroLocLibDistance; // 最小距离
			
			if(allList.size() == 0)
			{
				ArrayList<LocItem> locItemlist = new ArrayList<>();
				locItemlist.add(tempMro);
				allList.add(locItemlist);
			}
			else
			{
				// 找最小距离对应的mro位置点在list中的索引
				ArrayList<LocItem> minDistanceCluster = null;
				for (ArrayList<LocItem> locItemList : allList)
				{
					LocItem firstLocItem = locItemList.get(0);
					tempDistance = (long) GisFunction.GetDistance(firstLocItem.ilongitude, firstLocItem.ilatitude,
							tempMro.ilongitude, tempMro.ilatitude);
					if(tempDistance < minDistance)
					{
						minDistance = tempDistance;
						minDistanceCluster = locItemList;
					}
				}
				
				if(minDistanceCluster != null)
				{
					minDistanceCluster.add(tempMro);
				}
				else 
				{
					minDistanceCluster = new ArrayList<LocItem>();
					minDistanceCluster.add(tempMro);
					allList.add(minDistanceCluster);	
				}
			}
		}
		
		int maxNum = -1; // 最多个数
		ArrayList<LocItem> maxLocCluster = null;
		for(ArrayList<LocItem> itemList : allList)
		{
			if(itemList.size() > maxNum)
			{
				maxNum = itemList.size();
				maxLocCluster = itemList;
			}
		}
		return getBuildIdPosition(maxLocCluster);
	}
	
	public static LocItem getBuildIdPosition(ArrayList<LocItem> list)
	{
		HashMap<String, ArrayList<LocItem>> buildIdHeightMap = new HashMap<>();
		HashMap<String, ArrayList<LocItem>> positionMap = new HashMap<>();
		
		for(LocItem locItem : list)
		{
			if(locItem.ibuildid > 0)	//只取有楼宇的那些点装进map
			{
				String buildIdHeightKey = locItem.ibuildid + "_" + locItem.iheight;
				ArrayList<LocItem> buildIdHeightList = buildIdHeightMap.get(buildIdHeightKey);
				if(buildIdHeightList == null)
				{
					buildIdHeightList = new ArrayList<>();
					buildIdHeightMap.put(buildIdHeightKey, buildIdHeightList);
				}
				buildIdHeightList.add(locItem);
			}
			
			if(locItem.position > 0)	//只取有方位的那些点装进map
			{
				String positionKey = locItem.position + "";
				ArrayList<LocItem> positionList = positionMap.get(positionKey);
				if(positionList == null)
				{
					positionList = new ArrayList<>();
					positionMap.put(positionKey, positionList);
				}
				positionList.add(locItem);
			}
		}
		
		//TODO 2018/05/29
		LocItem buildIdHeightLocItem = getMax(buildIdHeightMap);	//以楼宇+高度取到的第一个的点为基准
		if(buildIdHeightLocItem != null)
		{
			LocItem positionLocItem = getMax(positionMap);
			if(positionLocItem != null)
			{
				buildIdHeightLocItem.position = positionLocItem.position;
			}
		}
		return buildIdHeightLocItem;
	}
	
	public static LocItem getMax(HashMap<String, ArrayList<LocItem>> map)
	{
		int maxNum = -1; // 最多个数
		String strKey = ""; //最多个数对应的key
		LocItem locItem = null;
		for(String str : map.keySet())
		{
			ArrayList<LocItem> list = map.get(str);
			if (list.size() > maxNum)	
			{
				maxNum = list.size();
				strKey = str;
			}
		}
		if(maxNum > 0)
		{
			locItem = map.get(strKey).get(0);	//取第一个点
		}
		return locItem;
	}
	
	//XDRLOC使用，对同一用户同小区同一小时下的点聚类
	public static ResidentUser residentUserCluster(Long residentUserCellDistance, ArrayList<ResidentUser> residentUserList)
	{
		ArrayList<ResidentUser> list = new ArrayList<>();
		ResidentUser firstResidentUser; // 第一个位置点
		long tempDistance = 0; // 缓存距离
		long minDistance = -1; // 最小距离
		int minIndex = 0; // 最小距离对应索引
		boolean flag = false;

		for (ResidentUser tempResidentUser : residentUserList)
		{
			if(tempResidentUser.testType == StaticConfig.TestType_CQT)
			{
				flag = true;
				break;
			}
		}
		
		for (ResidentUser temp : residentUserList)
		{
			if(flag && temp.testType == StaticConfig.TestType_CQT)
			{
				// 找最小距离对应的位置点在list中的索引
				for (int i = 0; i < list.size(); i++)
				{
					tempDistance = (long) GisFunction.GetDistance(list.get(i).longitude, list.get(i).latitude,
							temp.longitude, temp.latitude);
					if (minDistance == -1)
					{
						minIndex = 0;
						minDistance = tempDistance;
					}
					else if (minDistance > tempDistance)
					{
						minIndex = i;
						minDistance = tempDistance;
					}
				}
				// 第一个位置点做第一个，超过阈值的也新建一个，并更新经纬度。
				if (minDistance == -1 || minDistance > residentUserCellDistance)
				{
					firstResidentUser = new ResidentUser();
					firstResidentUser.fillData2(temp);
					firstResidentUser.updateData(temp);
					list.add(firstResidentUser);
				}
				// 并入list已存储的位置点中，并更新经纬度。
				else
				{
					list.get(minIndex).updateData(temp);
				}
			}
			else if (flag)
			{
				continue;
			}
			else
			{
				temp.longitude = 0;
				temp.latitude = 0;
				temp.locSource = "";
				return temp;
			}
		}
		
		int maxNum = -1; // 最多个数
		int maxIndex = -1; // 最多个数位置对应的索引
		ResidentUser tempResidentUser = null;
		for (int i = 0; i < list.size(); i++)
		{
			tempResidentUser = list.get(i);
			if (tempResidentUser.totalNum > maxNum)
			{
				maxNum = tempResidentUser.totalNum;
				maxIndex = i;
			}
		}
		if (maxIndex >= 0)
		{
			tempResidentUser = list.get(maxIndex);
		}
		return tempResidentUser;
	}
	
	//MERGE_RESIDENTUSER使用，对基站聚类，返回所有驻留区域
	public static ArrayList<ResidentArea> baseStationCluster(Long baseStationDistance, HashMap<Long, Long> eciMap, HashMap<Integer, BaseStation> baseStationMap)
	{
		if(baseStationMap.size() == 0)
		{
			return null;
		}
		
		ArrayList<ResidentArea> list = new ArrayList<>();
		ResidentArea firstResidentArea; // 第一个驻留区域
		long tempDistance = 0; // 缓存距离
		long minDistance = -1; // 最小距离
		int minIndex = 0; // 最小距离对应索引

		for (BaseStation temp : baseStationMap.values())
		{
			//获取每个小区下的驻留总时长
			for (ResidentUser residentUser : temp.getResidentUserList())
			{
				residentUser.totalTimes = eciMap.get(residentUser.eci);
			}
			
			// 找最小距离对应的驻留区域在list中的索引
			for (int i = 0; i < list.size(); i++)
			{
				tempDistance = (long) GisFunction.GetDistance(list.get(i).getBaseStationList().get(0).getLongitude(), list.get(i).getBaseStationList().get(0).getLatitude(),
						temp.getLongitude(), temp.getLatitude());
				if (minDistance == -1)
				{
					minIndex = 0;
					minDistance = tempDistance;
				}
				else if (minDistance > tempDistance)
				{
					minIndex = i;
					minDistance = tempDistance;
				}
			}
			// 第一个驻留区域做第一个，超过阈值的也新建一个，并更新总时长。
			if (minDistance == -1 || minDistance > baseStationDistance)
			{
				firstResidentArea = new ResidentArea();
				firstResidentArea.fillData(temp);
				list.add(firstResidentArea);
			}
			// 并入list已存储的基站中，并更新总时长。
			else
			{
				list.get(minIndex).getBaseStationList().add(temp);
				list.get(minIndex).updateTotalTimes(temp);
			}
		}
		
		//将用户的驻留区域按照驻留时长倒序排序
		Collections.sort(list, new Comparator<ResidentArea>() {
			@Override
			public int compare(ResidentArea b1, ResidentArea b2) {
				// TODO Auto-generated method stub
				return (int) (b2.getTotalTimes() - b1.getTotalTimes());
			}
		});
		
		for(ResidentArea temp : list)
		{
			//将用户的每个驻留区域中的基站按照驻留时长倒序排序
			Collections.sort(temp.getBaseStationList(), new Comparator<BaseStation>() {
				@Override
				public int compare(BaseStation b1, BaseStation b2) {
					// TODO Auto-generated method stub
					return (int) (b2.getTotalTimes() - b1.getTotalTimes());
				}
			});
		}
		
		return list;
	}
}
