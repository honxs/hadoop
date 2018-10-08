package cn.mastercom.bigdata.mro.loc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import cn.mastercom.bigdata.StructData.SIGNAL_MR_All;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.StructData.XdrLocation;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;

public class XdrLocationMng
{
	private long eci;
	// 基于s1apid的xdr列表
	private Map<Long, S1apIDMngExtend> s1apIDMap = new HashMap<Long, S1apIDMngExtend>();
	// 基于imsi的xdr列表
	private Map<Long, ImsiS1apIDMngExtend> imsiS1apIDMap = new HashMap<Long, ImsiS1apIDMngExtend>();

	public XdrLocationMng()
	{

	}

	public void addXdrLocItem(XdrLocation xdrLable)
	{
		{
			if (xdrLable.s1apid > 0)
			{
				S1apIDMngExtend mng = s1apIDMap.get(xdrLable.s1apid);
				if (mng == null)
				{
					mng = new S1apIDMngExtend();
					s1apIDMap.put(xdrLable.s1apid, mng);
				}
				mng.addXdrLocItem(xdrLable);
			}

		}

		{
			ImsiS1apIDMngExtend mng = imsiS1apIDMap.get(xdrLable.imsi);
			if (mng == null)
			{
				mng = new ImsiS1apIDMngExtend();
				imsiS1apIDMap.put(xdrLable.imsi, mng);
			}
			mng.addXdrLocItem(xdrLable);
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

	public void dealMroData(SIGNAL_MR_All mroItem)
	{
		{
			// 关联imsi
			S1apIDMngExtend s1apIDMng = s1apIDMap.get(mroItem.tsc.MmeUeS1apId);
			if (s1apIDMng == null)
			{
				return;
			}
			XdrLocation locItem = s1apIDMng.getXdrLoc(mroItem.tsc.beginTime);
			if (locItem != null)
			{
				mroItem.tsc.IMSI = locItem.imsi;
				mroItem.tsc.cityID = locItem.cityID;
				mroItem.tsc.imeiTac = locItem.imei;
				mroItem.locSource = locItem.locSource;
			}
			else
			{
				return;
			}
		}

		{
			// 关联lable
			ImsiS1apIDMngExtend imsiMng = imsiS1apIDMap.get(mroItem.tsc.IMSI);
			if (imsiMng == null)
			{
				return;
			}

			XdrLocation lableItem = imsiMng.getXdrLoc(mroItem.tsc.beginTime);
			if (lableItem != null)
			{
				mroItem.testType = lableItem.testTypeGL;
				mroItem.location = lableItem.locationGL;
				mroItem.dist = lableItem.distGL;
				mroItem.radius = lableItem.radiusGL;
				mroItem.locType = lableItem.loctpGL;
				mroItem.indoor = lableItem.indoorGL;
				mroItem.label = lableItem.lableGL;

				mroItem.serviceType = lableItem.serviceType;
				mroItem.subServiceType = lableItem.subServiceType;

				mroItem.moveDirect = lableItem.moveDirect;

				mroItem.tsc.UserLabel = lableItem.host;

				mroItem.tsc.UserLabel = lableItem.wifiName;

				if (lableItem.testTypeGL == StaticConfig.TestType_DT || lableItem.testTypeGL == StaticConfig.TestType_DT_EX)
				{
					// 贵州数据要求60秒都可以回填
					if (MainModel.GetInstance().getCompile().Assert(CompileMark.GuiZhou))
					{
						if (Math.abs(mroItem.tsc.beginTime - lableItem.itime) <= 60)
						{
							mroItem.tsc.longitude = lableItem.longitudeGL;
							mroItem.tsc.latitude = lableItem.latitudeGL;
						}
					}
					else
					{
						if (Math.abs(mroItem.tsc.beginTime - lableItem.itime) <= 10)
						{
							mroItem.tsc.longitude = lableItem.longitudeGL;
							mroItem.tsc.latitude = lableItem.latitudeGL;
							
							//LOGHelper.GetLogger().writeLog(LogType.debug, "fill the location: " + mroItem.tsc.IMSI + " " + mroItem.tsc.longitude + " " + mroItem.tsc.latitude);
						}
					}

				}
				else if (lableItem.testTypeGL == StaticConfig.TestType_CQT)
				{
					mroItem.tsc.longitude = lableItem.longitudeGL;
					mroItem.tsc.latitude = lableItem.latitudeGL;
				}

			}

		}

	}

	private class S1apIDMngExtend
	{
		private List<XdrLocation> xdrLableList;
		private int timeExpend = 5 * 60;
//		private int TimeSpan = 600;

		public S1apIDMngExtend()
		{
			xdrLableList = new ArrayList<XdrLocation>();
		}

		public void addXdrLocItem(XdrLocation lableItem)
		{
			xdrLableList.add(lableItem);
		}

		public void init()
		{
			Collections.sort(xdrLableList, new Comparator<XdrLocation>()
			{
				public int compare(XdrLocation a, XdrLocation b)
				{
					return a.itime - b.itime;
				}
			});
		}

		public XdrLocation getXdrLoc(int tmTime)
		{
			int curTimeSpan = timeExpend;
			XdrLocation curItem = null;
			for (XdrLocation item : xdrLableList)
			{
				// 过早的loc,跳过
				if (item.itime + timeExpend < tmTime)
					continue;

				// 过晚的loc，跳过
				if (tmTime + timeExpend < item.itime)
					continue;

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
					return curItem;
				}
				return item;
			}

			return curItem;
		}

	}

	public class ImsiS1apIDMngExtend
	{
		private List<XdrLocation> xdrLableList;
		private int timeExpend = 5 * 60;
		private int TimeSpan = 600;

		public ImsiS1apIDMngExtend()
		{
			xdrLableList = new ArrayList<XdrLocation>();
		}

		public void addXdrLocItem(XdrLocation lableItem)
		{
			xdrLableList.add(lableItem);
		}

		public void init()
		{
			Collections.sort(xdrLableList, new Comparator<XdrLocation>()
			{
				public int compare(XdrLocation a, XdrLocation b)
				{
					return a.itime - b.itime;
				}
			});
		}

		public XdrLocation getXdrLoc(int tmTime)
		{
			int curDTTime = timeExpend;
			int curCQTTime = timeExpend;
			int curDTEXTime = timeExpend;
			XdrLocation curDTItem = null;
			XdrLocation curCQTItem = null;
			XdrLocation curDTEXItem = null;
			int tmTimeSpan = tmTime / TimeSpan * TimeSpan;
			int spTime;

			for (XdrLocation item : xdrLableList)
			{
				if (item.itime / TimeSpan * TimeSpan != tmTimeSpan)
				{
					continue;
				}

				if (item.testTypeGL == StaticConfig.TestType_DT && item.longitudeGL > 0)
				{
					spTime = Math.abs(tmTime - item.loctimeGL);
					if (spTime < curDTTime)
					{
						curDTTime = spTime;
						curDTItem = item;
					}
				}

				if (item.testTypeGL == StaticConfig.TestType_CQT && item.longitudeGL > 0)
				{
					spTime = Math.abs(tmTime - item.loctimeGL);
					if (spTime < curCQTTime)
					{
						curCQTTime = spTime;
						curCQTItem = item;
					}
				}

				if (item.testTypeGL == StaticConfig.TestType_DT_EX && item.longitudeGL > 0)
				{
					spTime = Math.abs(tmTime - item.loctimeGL);
					if (spTime < curDTEXTime)
					{
						curDTEXTime = spTime;
						curDTEXItem = item;
					}
				}
			}

			if (curDTItem != null)
			{
				return curDTItem;
			}
			else if (curCQTItem != null)
			{
				return curCQTItem;
			}
			else if (curDTEXItem != null)
			{
				return curDTEXItem;
			}
			return null;
		}

	}

	public boolean dealMroDataForXdr(SIGNAL_MR_All mroItem)
	{
		{
			// 关联imsi
			S1apIDMngExtend s1apIDMng = s1apIDMap.get(mroItem.tsc.MmeUeS1apId);
			if (s1apIDMng == null)
			{
				return false;
			}

			XdrLocation locItem = s1apIDMng.getXdrLoc(mroItem.tsc.beginTime);
			if (locItem != null)
			{
				mroItem.tsc.IMSI = locItem.imsi;
				mroItem.tsc.cityID = locItem.cityID;
			}
			else
			{
				return false;
			}

		}

		{
			// 关联lable
			ImsiS1apIDMngExtend imsiMng = imsiS1apIDMap.get(mroItem.tsc.IMSI);
			if (imsiMng == null)
			{
				return false;
			}

			XdrLocation lableItem = imsiMng.getXdrLoc(mroItem.tsc.beginTime);
			if (lableItem != null)
			{
				mroItem.testType = lableItem.testTypeGL;
				mroItem.location = lableItem.locationGL;
				mroItem.dist = lableItem.distGL;
				mroItem.radius = lableItem.radiusGL;
				mroItem.locType = lableItem.loctpGL;
				mroItem.indoor = lableItem.indoorGL;
				mroItem.label = lableItem.lableGL;

				mroItem.serviceType = lableItem.serviceType;
				mroItem.subServiceType = lableItem.subServiceType;

				mroItem.moveDirect = lableItem.moveDirect;

				mroItem.tsc.UserLabel = lableItem.host;

				if (lableItem.testTypeGL == StaticConfig.TestType_DT || lableItem.testTypeGL == StaticConfig.TestType_DT_EX)
				{
					// 贵州数据要求60秒都可以回填
					if (MainModel.GetInstance().getCompile().Assert(CompileMark.GuiZhou))
					{
						if (Math.abs(mroItem.tsc.beginTime - lableItem.itime) <= 60)
						{
							mroItem.tsc.longitude = lableItem.longitudeGL;
							mroItem.tsc.latitude = lableItem.latitudeGL;
						}
					}
					else
					{
						if (Math.abs(mroItem.tsc.beginTime - lableItem.itime) <= 10)
						{
							mroItem.tsc.longitude = lableItem.longitudeGL;
							mroItem.tsc.latitude = lableItem.latitudeGL;
						}
					}
				}
				else if (lableItem.testTypeGL == StaticConfig.TestType_CQT)
				{
					mroItem.tsc.longitude = lableItem.longitudeGL;
					mroItem.tsc.latitude = lableItem.latitudeGL;
				}

			}

		}
		return true;

	}

}
