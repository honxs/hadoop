package cn.mastercom.bigdata.mro.loc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cn.mastercom.bigdata.StructData.SIGNAL_MR_All;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.stat.noSatisUserDeal.GL_BaseInfo;
import cn.mastercom.bigdata.util.Func;

public class XdrLabelMng
{
	private long eci;
	// 基于s1apid的xdr列表
	private Map<Long, S1apIDMngExtend> s1apIDMap = new HashMap<Long, S1apIDMngExtend>();
	// 基于imsi的xdr列表
	private Map<Long, ImsiS1apIDMngExtend> imsiS1apIDMap = new HashMap<Long, ImsiS1apIDMngExtend>();

	public XdrLabelMng()
	{

	}

	public String getSize()
	{
		return "s1apIDMap.size=" + s1apIDMap.size() + " imsiS1apIDMap.size=" + imsiS1apIDMap.size();
	}

	/**
	 * 将位置库放入s1apIDMap, key为s1apID, value为S1apIDMngExtend  
	 * @see S1apIDMngExtend
	 * @param xdrLabel
	 */
	public void addXdrLocItem(XdrLabel xdrLabel)
	{
		{
			if (xdrLabel.s1apid > 0)
			{
				S1apIDMngExtend mng = s1apIDMap.get(xdrLabel.s1apid);
				if (mng == null)
				{
					mng = new S1apIDMngExtend();
					s1apIDMap.put(xdrLabel.s1apid, mng);
				}
				mng.addXdrLocItem(xdrLabel);
			}

		}

		{
			ImsiS1apIDMngExtend mng = imsiS1apIDMap.get(xdrLabel.imsi);
			if (mng == null)
			{
				mng = new ImsiS1apIDMngExtend();
				imsiS1apIDMap.put(xdrLabel.imsi, mng);
			}
			mng.addXdrLocItem(xdrLabel);
		}
	}

	public void init()
	{
		for (S1apIDMngExtend item : s1apIDMap.values())
		{
			item.init();
		}

		for (ImsiS1apIDMngExtend item : imsiS1apIDMap.values())
		{
			item.init();
		}
	}

	public long getEci()
	{
		return eci;
	}

	/**
	 * 关联imsi
	 * 
	 * @param baseInfo
	 * @return
	 */
	public boolean dealGLBaseInfo(GL_BaseInfo baseInfo)
	{
//		S1apIDMngExtend s1apIDMng = s1apIDMap.get(baseInfo.mmeUes1apid);
//		if (s1apIDMng == null)
//		{
//			return false;
//		}
//		XdrLabel locItem = s1apIDMng.getXdrLoc(baseInfo.times);
		
		XdrLabel locItem = findXdrByS1apidTime(baseInfo.mmeUes1apid, baseInfo.times);

		if (locItem != null)
		{
			baseInfo.imsi = locItem.imsi;
		}
		else
		{
			return false;
		}
		return true;
	}

	public boolean dealMroData(SIGNAL_MR_All mroItem)
	{
		{//通过s1apid找MME回填IMSI
			
//			S1apIDMngExtend s1apIDMng = s1apIDMap.get(mroItem.tsc.MmeUeS1apId);
//			if (s1apIDMng == null)
//			{
////				LOGHelper.GetLogger().writeLog(LogType.error, "s1apid【"+mroItem.tsc.MmeUeS1apId+"】没有关联上, " + getSize());
//				return false;
//			}
//
//			XdrLabel locItem = s1apIDMng.getXdrLoc(mroItem.tsc.beginTime);
			XdrLabel locItem = findXdrByS1apidTime(mroItem.tsc.MmeUeS1apId, mroItem.tsc.beginTime);

			
			if (locItem != null)
			{
				mroItem.tsc.IMSI = locItem.imsi;
				mroItem.tsc.Msisdn = locItem.msisdn;
				mroItem.tsc.imeiTac = locItem.imeiTac;
				mroItem.eciSwitchList = locItem.eciSwitchList;
			}
			else
			{
//				LOGHelper.GetLogger().writeLog(LogType.error, "时间【"+mroItem.tsc.beginTime+"】没有关联上： " + "最早时间【"+ s1apIDMng.xdrLableList.get(0).itime + "】, 最晚时间【" + s1apIDMng.xdrLableList.get(s1apIDMng.xdrLableList.size()-1).itime+"】");
				return false;
			}
			
		}
		
		/* 180510 修改 by kwong : 对于mdt或者 有经纬度的mr数据，还是需要回填状态
		if (mroItem.loctp.equals("mdt") && mroItem.tsc.longitude > 0 && mroItem.tsc.latitude > 0)// 有经纬度的mdt数据只回填imsi和经纬度
		{
			return true;
		}*/
		

		{//通过回填的IMSI找http/location来回填经纬度
			
//			ImsiS1apIDMngExtend imsiMng = imsiS1apIDMap.get(mroItem.tsc.IMSI);
//			if (imsiMng == null)
//			{
//				return false;
//			}
//
//			XdrLabel lableItem = imsiMng.getXdrLocEx(mroItem.tsc.beginTime);
			
			XdrLabel lableItem = findXdrByImsiTime(mroItem.tsc.IMSI, mroItem.tsc.beginTime);

			if (lableItem != null)
			{
				mroItem.location = lableItem.locationGL;
				mroItem.dist = lableItem.distGL;
				mroItem.radius = lableItem.radiusGL;
				mroItem.locType = lableItem.loctpGL;
				mroItem.indoor = lableItem.indoorGL;
				mroItem.label = lableItem.lableGL;
							
				mroItem.ibuildingID = lableItem.buildingid;
				mroItem.iheight = (short) lableItem.level;
				
				mroItem.serviceType = lableItem.serviceType;
				mroItem.subServiceType = lableItem.subServiceType;
				mroItem.moveDirect = lableItem.moveDirect;
				mroItem.tsc.UserLabel = lableItem.wifiName;
				
				mroItem.testType = lableItem.testTypeGL;
				mroItem.locSource = Func.getLocSource(mroItem.locType);
				if (mroItem.locSource == 0)
				{
					mroItem.locSource = Func.getLocSource(mroItem.location);
				}
				//++
				if (mroItem.tsc.longitude > 0 && mroItem.tsc.latitude > 0)// 有经纬度的mdt/mr数据只回填状态
				{
					return true;
				}
				
				if (lableItem.testTypeGL == StaticConfig.TestType_DT || lableItem.testTypeGL == StaticConfig.TestType_DT_EX)
				{
					if (Math.abs(mroItem.tsc.beginTime - lableItem.itime) <= 10)
					{
						mroItem.tsc.longitude = lableItem.longitudeGL;
						mroItem.tsc.latitude = lableItem.latitudeGL;
					}
				}
				else if (lableItem.testTypeGL == StaticConfig.TestType_CQT)
				{
					mroItem.tsc.longitude = lableItem.longitudeGL;
					mroItem.tsc.latitude = lableItem.latitudeGL;
				}
				else if (lableItem.testTypeGL == StaticConfig.TestType_HiRail)
				{
					mroItem.areaId = lableItem.areaId;
					mroItem.areaType = lableItem.areaType;
					mroItem.tsc.latitude = lableItem.latitudeGL;
					mroItem.tsc.longitude = lableItem.longitudeGL;
				}
			}
		}
		
		
		return true;
	}
	
	/**
	 * 通过s1apid和时间找最近的xdr
	 * @param s1apid
	 * @param time
	 * @return
	 */
	public XdrLabel findXdrByS1apidTime(long s1apid, int time){
		S1apIDMngExtend s1apIDMng = s1apIDMap.get(s1apid);
		if (s1apIDMng == null)
		{
			return null;
		}
		XdrLabel locItem = s1apIDMng.getXdrLoc(time);
		return locItem;
	}
	
	/**
	 * 通过imsi和找时间最近的xdr
	 * @param imsi
	 * @param time
	 * @return
	 */
	public XdrLabel findXdrByImsiTime(long imsi, int time){
		ImsiS1apIDMngExtend imsiMng = imsiS1apIDMap.get(imsi);
		if (imsiMng == null)
		{
			return null;
		}

		XdrLabel lableItem = imsiMng.getXdrLocEx(time);
		return lableItem;
	}

	/**
	 * 主要里面有个类List<XdrLable> xdrLableList;
	 */
	private class S1apIDMngExtend
	{
		private List<XdrLabel> xdrLabelList;
		private int timeExpend = 10 * 60;

		public S1apIDMngExtend()
		{
			xdrLabelList = new ArrayList<XdrLabel>();
		}

		public void addXdrLocItem(XdrLabel labelItem)
		{
			xdrLabelList.add(labelItem);
		}

		/**
		 * 将这个类里面的xdrLableList 进行排序
		 */
		public void init()
		{
			Collections.sort(xdrLabelList, new Comparator<XdrLabel>()
			{
				public int compare(XdrLabel a, XdrLabel b)
				{
					return a.itime - b.itime;
				}
			});
		}

		/**
		 * 对给定时间，找其前后十分钟内时间最接近的xdr
		 * @param tmTime
		 * @return
		 */
		public XdrLabel getXdrLoc(int tmTime)
		{
			int curTimeSpan = timeExpend;
			XdrLabel curItem = null;
			Set<Long> imsiSet = new HashSet<Long>();
			for (XdrLabel item : xdrLabelList)
			{
				// 过早的loc,跳过
				if (item.itime + timeExpend < tmTime)
					continue;

				// 过晚的loc，跳过
				if (tmTime + timeExpend < item.itime)
					break;

				if (item.imsi > 0)
				{
					imsiSet.add(item.imsi);
				}
				// 前后5分钟如果同一个usapid有两个及以上的imsi 丢弃
				if (imsiSet.size() > 1)
				{
					return null;
				}
				int tmTimeSpan = Math.abs(tmTime - item.itime);

				if (tmTime >= item.itime)
				{// 找到比mrO时间早的loc
					if (curTimeSpan > tmTimeSpan)
					{
						curTimeSpan = tmTimeSpan;
						curItem = item;
						continue;
					}
				}

				if (item.itime > tmTime && curItem != null)
				{
					if ((item.itime - tmTime) < (tmTime - curItem.itime))
					{
						return item;
					}
					else
					{
						return curItem;
					}
				}
				return item;
			}

			return curItem;
		}

	}

	public class ImsiS1apIDMngExtend
	{
		private List<XdrLabel> xdrLabelList;
		private int TimeSpan = 600;

		public ImsiS1apIDMngExtend()
		{
			xdrLabelList = new ArrayList<XdrLabel>();
		}

		public void addXdrLocItem(XdrLabel lableItem)
		{
			if (lableItem.longitudeGL > 0 && lableItem.testTypeGL <= StaticConfig.TestType_HiRail)
			{
				xdrLabelList.add(lableItem);
			}
		}

		public void init()
		{
			Collections.sort(xdrLabelList, new Comparator<XdrLabel>()
			{
				public int compare(XdrLabel a, XdrLabel b)
				{
					return a.itime - b.itime;
				}
			});
		}

		public XdrLabel getXdrLocEx(int tmTime)
		{
			XdrLabel[] lables = new XdrLabel[2];
			int[] spTimes = new int[2];
			int spTime;

			for (XdrLabel item : xdrLabelList)
			{
				spTime = tmTime - item.loctimeGL;
				if (spTime > TimeSpan)
				{// 时间比当前xdr时间早10分钟以上
					continue;
				}
				else if (-spTime > TimeSpan)
				{// 时间比当前xdr时间晚10分钟以上
					break;
				}

				if (spTime >= 0)
				{// 时间比当前mr时间早，肯定比上一个点更接近当前时间
					lables[0] = item;
					spTimes[0] = Math.abs(spTime);
				}
				if (spTime < 0)
				{// 时间比当前xdr时间晚，退出搜索
					lables[1] = item;
					spTimes[1] = Math.abs(spTime);
					break;
				}
			}

			if (lables[0] != null && lables[1] == null)
			{// 只有一个XDR,CQT必须1分钟之内有效
				if (lables[0].testTypeGL != StaticConfig.TestType_CQT || spTimes[0] < 60)
					return lables[0];
				return null;
			}
			else if (lables[0] == null && lables[1] != null)
			{// 只有一个XDR,CQT必须1分钟之内有效
				if (lables[1].testTypeGL != StaticConfig.TestType_CQT || spTimes[1] < 60)
					return lables[1];
				return null;
			}
			else if (lables[0] != null && lables[1] != null)
			{
				// testtype是100内的都是正常的测试类型
				if (lables[0].testTypeGL == lables[1].testTypeGL && lables[0].testTypeGL < 100)
				{// 前后类型相同，找时间最近的
					return spTimes[0] <= spTimes[1] ? lables[0] : lables[1];
				}

				// 判断高铁用户
				if (lables[0].testTypeGL == StaticConfig.TestType_HiRail)
				{
					return lables[0];
				}

				if (lables[1].testTypeGL == StaticConfig.TestType_HiRail)
				{
					return lables[1];
				}

				// 判断DT
				if (lables[0].testTypeGL == StaticConfig.TestType_DT)
				{
					return lables[0];
				}

				if (lables[1].testTypeGL == StaticConfig.TestType_DT)
				{
					return lables[1];
				}
				// 判断DTEX
				if (lables[0].testTypeGL == StaticConfig.TestType_DT_EX)
				{
					return lables[0];
				}

				if (lables[1].testTypeGL == StaticConfig.TestType_DT_EX)
				{
					return lables[1];
				}
				// 判断CQT
				if (lables[0].testTypeGL == StaticConfig.TestType_CQT && spTimes[0] < 120)
				{// 只有一个XDR,CQT必须1分钟之内有效
					return lables[0];
				}

				if (lables[1].testTypeGL == StaticConfig.TestType_CQT && spTimes[1] < 120)
				{// 只有一个XDR,CQT必须1分钟之内有效
					return lables[1];
				}
			}
			return null;
		}

	}
}
