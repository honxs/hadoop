package cn.mastercom.bigdata.loc.userResident;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.conf.cellconfig.CellConfig;
import cn.mastercom.bigdata.conf.cellconfig.LteCellInfo;
import cn.mastercom.bigdata.conf.config.BuildIndoorCellUserConfig;
import cn.mastercom.bigdata.conf.config.CellBuildInfo;
import cn.mastercom.bigdata.conf.config.HomeBroadbandConfig;
import cn.mastercom.bigdata.conf.config.HomeBroadbandItem;
import cn.mastercom.bigdata.evt.locall.stat.LocItem;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.stat.userResident.enmus.ResidentLocTablesEnum;
import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.GisFunction;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.xdr.loc.ResidentUser;

public class ResidentUserLocDeal
{
	public static final int DATATYPE_RESIDENT_USER = 1;
	public static final int DATATYPE_MRO_LOC_LIB = 2;
	public static final int DATATYPE_XDR_LOC_LIB = 3;
	
	public static final int WORKTIME = 1;
	public static final int HOMETIME = 2;
	public static final int OTHERTIME = 3;
	
	private ResultOutputer resultOutputer;
	private long mroLocLibDistance;
	private long tempEci;// 记录上一个eci
	private CellBuildInfo cellBuild;
	private Configuration conf;
	
	private int buildId;
	private int buildLongitude;
	private int buildLatitude;

	public ResidentUserLocDeal(ResultOutputer resultOutputer, Configuration conf) throws Exception
	{
		this.resultOutputer = resultOutputer;
		this.conf = conf;
		mroLocLibDistance = MainModel.GetInstance().getAppConfig().getResidentUserCellDistance();
		// 初始化lte小区的信息
		if (!CellConfig.GetInstance().loadLteCell(conf))
		{
			LOGHelper.GetLogger().writeLog(LogType.error, "ltecell init error 请检查！" + CellConfig.GetInstance().errLog);
			throw (new IOException("ltecell init error 请检查！"));
		}
		// 楼宇楼层室分用户数配置
		if (!BuildIndoorCellUserConfig.GetInstance().loadBuildIndoorCellUser(conf))
		{
			LOGHelper.GetLogger().writeLog(LogType.error, "BuildIndoorCellUserConfig init error 请检查！");
		}
		// 家宽数据
		if (!HomeBroadbandConfig.GetInstance().loadHomeBroadband(conf))
		{
			LOGHelper.GetLogger().writeLog(LogType.error, "homebroadbandconfig init error 请检查！");
		}
	}

	public void deal(UserResidentKey key, Iterable<Text> values)
	{
		try
		{
			if (key.getEci() != tempEci)
			{
				LteCellInfo cellInfo = CellConfig.GetInstance().getLteCell(key.getEci());
				if (cellInfo == null)
				{
					cellInfo = new LteCellInfo();
					LOGHelper.GetLogger().writeLog(LogType.info,
							"gongcansize:" + CellConfig.GetInstance().getlteCellInfoMapSize() + "  gongcan no eci:" + key.getEci() + "  enbid:" + key.getEci() / 256 + " cellid:" + key.getEci() % 256);
				}
				cellBuild = new CellBuildInfo();
				if (!cellBuild.loadCellBuild(conf, (int) key.getEci(), cellInfo.cityid))
				{
					LOGHelper.GetLogger().writeLog(LogType.error, "cellbuild init error 请检查！eci:" + key.getEci());
				}
				
				//优化：同一个小区只找一次小区最近楼宇经纬度、buildId
				buildId = -1;
				buildLongitude = 0;
				buildLatitude = 0;
				if(cellInfo != null)
				{
					buildId = cellBuild.getBuildId(cellInfo);
					String centerLngLat = cellBuild.getCenterLngLat(buildId);
					if(centerLngLat != null)
					{
						String[] str = centerLngLat.split("_", -1);
						buildLongitude = Integer.parseInt(str[0]);
						buildLatitude = Integer.parseInt(str[1]);
					}
				}
				tempEci = key.getEci();
			}

			HashMap<String, ArrayList<ResidentUser>> residentUserMap = new HashMap<>();
			HashMap<String, ArrayList<LocItem>> mroOttLocLibMap = new HashMap<>();
			HashMap<String, ArrayList<LocItem>> mroBfpLocLibMap = new HashMap<>();
			HashMap<String, ArrayList<LocItem>> mroFpLocLibMap = new HashMap<>();

			for (Text value : values)
			{
				String[] strs = value.toString().split("\t", -1);
				if (key.getDataType() == DATATYPE_RESIDENT_USER) // 1.UserResident
				{
					ResidentUser residentUser = new ResidentUser();
					residentUser.fillDayData(strs);
					int time = getPmOrAm(residentUser.hour);
					String tempKey = residentUser.imsi + "_" + time;
					ArrayList<ResidentUser> tempList = residentUserMap.get(tempKey);
					if(tempList == null)
					{
						tempList = new ArrayList<ResidentUser>();
						residentUserMap.put(tempKey, tempList);
					}
					tempList.add(residentUser);
				}
				else if (key.getDataType() == DATATYPE_MRO_LOC_LIB || key.getDataType() == DATATYPE_XDR_LOC_LIB) // 2.LocLib
				{
					LocItem locLibItem = new LocItem();
					locLibItem.fillData(strs);
					if(key.getDataType() == DATATYPE_XDR_LOC_LIB)
					{
						if(locLibItem.doorType != StaticConfig.ACTTYPE_IN)
						{
							//找离该位置最近的楼宇(80米内)
							locLibItem.ibuildid = cellBuild.getBuildId(locLibItem.ilongitude, locLibItem.ilatitude, 80);
							String centerLngLat = cellBuild.getCenterLngLat(locLibItem.ibuildid);
							if(centerLngLat != null)
							{
								String[] str = centerLngLat.split("_", -1);
								locLibItem.ilongitude = Integer.parseInt(str[0]);
								locLibItem.ilatitude = Integer.parseInt(str[1]);
							}
						}
					}
					int time = getPmOrAm(FormatTime.getHour(locLibItem.itime));
					if(locLibItem.loctp.contains(StaticConfig.RULOC_WF))
					{
						if (time == WORKTIME)	//工作时间OTT
						{
							String eKey = locLibItem.imsi + "_" + WORKTIME;
							LocItem tempMroLocLibItem = new LocItem();
							tempMroLocLibItem.fillData2(locLibItem);
							tempMroLocLibItem.loctp = StaticConfig.RULOC_WF3;
							putMap(tempMroLocLibItem, eKey, mroOttLocLibMap);
						}
						else if(time == HOMETIME)	//晚上时间OTT
						{
							String eKey = locLibItem.imsi + "_" + HOMETIME;
							LocItem tempMroLocLibItem = new LocItem();
							tempMroLocLibItem.fillData2(locLibItem);
							tempMroLocLibItem.loctp = StaticConfig.RULOC_WF2;
							putMap(tempMroLocLibItem, eKey, mroOttLocLibMap);
						}	
						
						//所有时间OTT
						String oKey = locLibItem.imsi + "_" + OTHERTIME;
						locLibItem.loctp = StaticConfig.RULOC_WF4;
						putMap(locLibItem, oKey, mroOttLocLibMap);
					}
					else if(locLibItem.loctp.contains(StaticConfig.RULOC_BFP))
					{
						dealFpMap(locLibItem, time, StaticConfig.RULOC_BFP, mroBfpLocLibMap);
					}
					else if(locLibItem.loctp.contains(StaticConfig.RULOC_FP))
					{
						dealFpMap(locLibItem, time, StaticConfig.RULOC_FP, mroFpLocLibMap);
					}
				}
			}

			for (String mapKey : residentUserMap.keySet())
			{
				ArrayList<ResidentUser> residentUserList = residentUserMap.get(mapKey);
				int time = Integer.parseInt(mapKey.substring(mapKey.indexOf("_") + 1, mapKey.length()));
				String strImsi = mapKey.substring(0, mapKey.indexOf("_"));
				
				if(time == WORKTIME)
				{
					ArrayList<LocItem> ottLocLibList = mroOttLocLibMap.get(strImsi + "_" + WORKTIME);
					ArrayList<LocItem> otherOttLocLibList = mroOttLocLibMap.get(strImsi + "_" + OTHERTIME);
					ArrayList<LocItem> bfpLocLibList = mroBfpLocLibMap.get(strImsi + "_" + WORKTIME);
					ArrayList<LocItem> fpLocLibList = mroFpLocLibMap.get(strImsi + "_" + WORKTIME);
					dealMap(residentUserList, ottLocLibList, otherOttLocLibList, bfpLocLibList, fpLocLibList);
				}
				else if(time == HOMETIME)
				{
					ArrayList<LocItem> ottLocLibList = mroOttLocLibMap.get(strImsi + "_" + HOMETIME);
					ArrayList<LocItem> otherOttLocLibList = mroOttLocLibMap.get(strImsi + "_" + OTHERTIME);
					ArrayList<LocItem> bfpLocLibList = mroBfpLocLibMap.get(strImsi + "_" + HOMETIME);
					ArrayList<LocItem> fpLocLibList = mroFpLocLibMap.get(strImsi + "_" + HOMETIME);
					dealMap(residentUserList, ottLocLibList, otherOttLocLibList, bfpLocLibList, fpLocLibList);
				}
				else
				{
					ArrayList<LocItem> otherOttLocLibList = mroOttLocLibMap.get(strImsi + "_" + OTHERTIME);
					ArrayList<LocItem> bfpLocLibList = mroBfpLocLibMap.get(strImsi + "_" + OTHERTIME);
					ArrayList<LocItem> fpLocLibList = mroFpLocLibMap.get(strImsi + "_" + OTHERTIME);
					dealMap(residentUserList, otherOttLocLibList, null, bfpLocLibList, fpLocLibList);
				}
			}
			
			for (ArrayList<ResidentUser> tempList : residentUserMap.values())
			{
				for(ResidentUser temp : tempList)
				{
					if (temp.buildId < 0)
					{
						List<Integer> buildIdList = cellBuild.getBuildIds((int) temp.longitude, (int) temp.latitude);
						if (buildIdList != null && !buildIdList.isEmpty())
						{
							int pos = Math.abs((temp.longitude +"_"+ temp.latitude).hashCode()) % buildIdList.size();
							temp.buildId = buildIdList.get(pos);
						}
					}
					saveFile(temp);
				}
			}
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error,"residentUserLoc err", "residentUserLoc err ", e);
		}
	}
	
	//吐出
	public void saveFile(ResidentUser residentUser)
	{
		try {
				resultOutputer.pushData(ResidentLocTablesEnum.resident_user.getIndex(), residentUser.ToDayLine());
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error,"residentUser save err1", "residentUser save err ", e);
			}
	}
	
	//区分时间段
	public int getPmOrAm(int hour)
	{
		if ((hour >= 9 && hour <= 11) || (hour >= 14 && hour <= 16))
		{
			return WORKTIME;
		}
		else if((hour >= 0 && hour <= 5) || (hour >= 19 && hour <= 23))
		{
			return HOMETIME;
		}
		else
		{
			return OTHERTIME;
		}
	}
	
	//装进map
	public void dealFpMap(LocItem locLibItem, int time, String loctp, HashMap<String, ArrayList<LocItem>> fpLocLibMap)
	{
		if (time == WORKTIME)	//工作时间
		{
			String eKey = locLibItem.imsi + "_" + WORKTIME;
			LocItem tempMroLocLibItem = new LocItem();
			tempMroLocLibItem.fillData2(locLibItem);
			tempMroLocLibItem.loctp = loctp;
			putMap(tempMroLocLibItem, eKey, fpLocLibMap);
		}
		else if(time == HOMETIME)	//晚上时间
		{
			String eKey = locLibItem.imsi + "_" + HOMETIME;
			LocItem tempMroLocLibItem = new LocItem();
			tempMroLocLibItem.fillData2(locLibItem);
			tempMroLocLibItem.loctp = loctp;
			putMap(tempMroLocLibItem, eKey, fpLocLibMap);
		}	
		
		//所有时间
		String oKey = locLibItem.imsi + "_" + OTHERTIME;
		locLibItem.loctp = loctp;
		putMap(locLibItem, oKey, fpLocLibMap);
	}
	
	//装进map
	public void putMap(LocItem mroLocLibItem, String eKey, HashMap<String, ArrayList<LocItem>> locLibMap)
	{
		ArrayList<LocItem> mroLocLibList = locLibMap.get(eKey);
		if (mroLocLibList == null)
		{
			mroLocLibList = new ArrayList<LocItem>();
			locLibMap.put(eKey, mroLocLibList);
		}
		mroLocLibList.add(mroLocLibItem);
	}
	
	public boolean dealMap(ArrayList<ResidentUser> residentUserList, ArrayList<LocItem> ottLocLibList, ArrayList<LocItem> otherOttLocLibList, ArrayList<LocItem> bfpLocLibList, ArrayList<LocItem> fpLocLibList)
	{	
		if(residentUserList.size() == 0)
		{
			return true;
		}
		if(!fillIndoorCellAtBuildPos(residentUserList))
		{
			if(!fillPos(ottLocLibList, residentUserList))
			{
				if(!fillPos(otherOttLocLibList, residentUserList))
				{
					if(!fillBuildIndoorCellPos(residentUserList))
					{
						// 楼宇高精度指纹库定位
						if(!fillPos(bfpLocLibList, residentUserList))
						{
							if(!fillHomeBroadbandPos(residentUserList))
							{
								if(!fillPos(fpLocLibList, residentUserList))
								{
									//回填小区最近楼宇经纬度、buildId，定位类型cl
									fillClPos(residentUserList);
								}
							}
						}
					}
				}
			}
		}
		return true;
	}
	
	public boolean fillPos(ArrayList<LocItem> locLibList, ArrayList<ResidentUser> residentUserList)
	{
		if(locLibList == null || locLibList.size() == 0)
		{
			return false;
		}
		
		ArrayList<LocItem> inLocLibList = new ArrayList<>();
		ArrayList<LocItem> allLocLibList = new ArrayList<>();
		LocItem userLocItem = null;
		
		for(LocItem locItem : locLibList)
		{
			if(locItem.doorType == StaticConfig.ACTTYPE_IN)
			{
				inLocLibList.add(locItem);
			}
			allLocLibList.add(locItem);
		}
		
		//位置库聚类
		userLocItem = UserResidentCluster.cluster(mroLocLibDistance, inLocLibList);
		if (userLocItem != null)
		{
			fillDetailPos(userLocItem, residentUserList);
			return true;
		}
		else
		{
			userLocItem = UserResidentCluster.cluster(mroLocLibDistance, allLocLibList);
			if (userLocItem != null)
			{
				fillDetailPos(userLocItem, residentUserList);
				return true;
			}
		}
		return false;
	}
	
	//回填该室分小区共址楼宇buildId,经纬度,位置来源为(wf1)
	public boolean fillIndoorCellAtBuildPos(ArrayList<ResidentUser> residentUserList)
	{
		for(ResidentUser item : residentUserList)
		{
			LteCellInfo cellInfo = CellConfig.GetInstance().getLteCell(item.eci);
			if(cellInfo != null)
			{
				item.buildId = BuildIndoorCellUserConfig.GetInstance().getIndoorCellAtBuild(cellInfo, cellBuild);
				String centerLngLat = cellBuild.getCenterLngLat(item.buildId);
				if(centerLngLat != null)
				{
					String[] str = centerLngLat.split("_", -1);
					item.longitude = Integer.parseInt(str[0]);
					item.latitude = Integer.parseInt(str[1]);
					item.locSource = StaticConfig.RULOC_WF1;
					return true;
				}
			}
		}
		return false;
	}
	
	//回填该室分小区覆盖楼宇的buildId,经纬度,位置来源为wf5
	public boolean fillBuildIndoorCellPos(ArrayList<ResidentUser> residentUserList)
	{
		for(ResidentUser item : residentUserList)
		{
			LteCellInfo cellInfo = CellConfig.GetInstance().getLteCell(item.eci);
			if(cellInfo != null)
			{
				item.buildId = BuildIndoorCellUserConfig.GetInstance().getIndoorCellBuild(cellInfo, cellBuild);
				String centerLngLat = cellBuild.getCenterLngLat(item.buildId);
				if(centerLngLat != null)
				{
					String[] str = centerLngLat.split("_", -1);
					item.longitude = Integer.parseInt(str[0]);
					item.latitude = Integer.parseInt(str[1]);
					item.locSource = StaticConfig.RULOC_WF5;
					return true;
				}
			}
		}
		return false;
	}
	
	//回填家宽配置经纬度buildId,经纬度,位置来源为(wl)
	public boolean fillHomeBroadbandPos(ArrayList<ResidentUser> residentUserList)
	{
		if (HomeBroadbandConfig.GetInstance().getHomeBroadbandItemMap().size() == 0)
		{
			return false;
		}
		for(ResidentUser temp : residentUserList)
		{
			// 去家宽配置表中找该常驻用户的家宽信息list(一条或多条)
			List<HomeBroadbandItem> homeBroadbandItemList = HomeBroadbandConfig.GetInstance().getHomeBroadbandItemMap().get(temp.msisdn);
			if (homeBroadbandItemList != null && homeBroadbandItemList.size() > 0)
			{
				for (HomeBroadbandItem item : homeBroadbandItemList)
				{
					LteCellInfo cellInfo = CellConfig.GetInstance().getLteCell(temp.eci);
					if(cellInfo != null)
					{
						long tempDistance = (long) GisFunction.GetDistance(item.longitude, item.latitude,
								cellInfo.ilongitude, cellInfo.ilatitude);
						//家宽经纬度与常驻小区经纬度距离需要小于300米
						if(tempDistance > 300L)
						{
							return false;
						}
						//经纬度跟用户常驻小区下的所有楼宇计算距离，求出最近的楼宇及距离，假如距离小于200，则用户常驻在楼宇下。
						int buildId = cellBuild.getBuildId(item.longitude, item.latitude, 200L);
						String centerLngLat = cellBuild.getCenterLngLat(buildId);
						if(centerLngLat != null)
						{
							String[] str = centerLngLat.split("_", -1);
							temp.longitude = Integer.parseInt(str[0]);
							temp.latitude = Integer.parseInt(str[1]);
							temp.buildId = buildId;
							temp.locSource = StaticConfig.RULOC_WL;
							return true;
						}
					}
				}
			}
		}
		return false;
	}
	
	public void fillDetailPos(LocItem userLocItem, ArrayList<ResidentUser> residentUserList)
	{
		for(ResidentUser item : residentUserList)
		{
			if (userLocItem.ilongitude > 0)
			{
				item.longitude = userLocItem.ilongitude;
				item.latitude = userLocItem.ilatitude;
				item.locSource = userLocItem.loctp;
				if(item.locSource.contains(StaticConfig.RULOC_BFP))
				{
					// 楼宇高精度指纹库定位
					item.locSource = StaticConfig.RULOC_WF6;
				}
				item.buildId = userLocItem.ibuildid;
			}
			item.height = userLocItem.iheight;
			item.position = userLocItem.position;
		}
	}
	
	//TODO 默认回填小区最近楼宇经纬度、buildId，位置来源为cl
	public void fillClPos(ArrayList<ResidentUser> residentUserList)
	{
		for(ResidentUser item : residentUserList)
		{
			if(buildLongitude > 0)
			{
				item.buildId = buildId;
				item.longitude = buildLongitude;
				item.latitude = buildLatitude;
			}
			else
			{
				LteCellInfo lteCellInfo = CellConfig.GetInstance().getLteCell(item.eci);
				if(lteCellInfo != null)
				{
					item.longitude = lteCellInfo.ilongitude;
					item.latitude = lteCellInfo.ilatitude;
				}
			}
			item.locSource = StaticConfig.RULOC_CL;
		}
	}
}
