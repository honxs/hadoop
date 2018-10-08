package cn.mastercom.bigdata.stat.userAna.hsr;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import cn.mastercom.bigdata.stat.userAna.hsr.HSRSecTrainData.LocPoint;
import cn.mastercom.bigdata.stat.userAna.hsr.HSRSecTrainData.TrainSeg;
import cn.mastercom.bigdata.stat.userAna.model.ImsiEciTime;
import cn.mastercom.bigdata.stat.userAna.model.ImsiFixEndTimeHelper;
import cn.mastercom.bigdata.stat.userAna.model.ImsiInfo;
import cn.mastercom.bigdata.stat.userAna.model.ImsiInfoSimple;
import cn.mastercom.bigdata.stat.userAna.model.RruInfo;
import cn.mastercom.bigdata.stat.userAna.model.StationImsi;
import cn.mastercom.bigdata.stat.userAna.model.Xdr_ImsiEciTime;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.util.LOGHelper;

/*
 * 线路信息
 */
public class RailSec
{
	public int cityID;
	public int id;
	public RailStation stationID1;
	public RailStation stationID2;
	public double length;

	// 区间中段,[开始时间+ 此值 : 调整结束时间+此值]
	private final int MidSpan = 120000;

	/*
	 * 配置 小区集与stationID1的距离
	 */
	public Map<Long, DistancePair> eci_dist_map;

	/**
	 * 相邻小区的RRU切换点
	 */
	public Map<Double, RruInfo> rruInfoMap;

	public class DistancePair
	{
		public double distance1;
		public double distance2;

		public DistancePair(double dist1, double dist2)
		{
			distance1 = dist1;
			distance2 = dist2;
		}
	}

	/*
	 * 添加配置
	 */
	public void addEci(long eci, double dist1, double dist2)
	{
		eci_dist_map.put(eci, new DistancePair(dist1, dist2));
	}

	public void addRru(long eci1, long eci2, double dist1, double dist2)
	{
		RruInfo rruInfo = null;
		if (rruInfoMap.containsKey(dist1))
		{
			rruInfo = rruInfoMap.get(dist1);
		}
		else
		{
			rruInfo = new RruInfo(dist1, dist2);
			rruInfoMap.put(dist1, rruInfo);
		}
		rruInfo.Add(eci1, eci2);
	}

	// CityID 线路ID（去掉） 区间ID 起始站ID 终点站ID 区间长度 区间点阵 Comment
	public RailSec(String line, Map<Integer, RailStation> idStationMap)
	{
		eci_dist_map = new HashMap<Long, DistancePair>();
		rruInfoMap = new HashMap<Double, RruInfo>();

		String[] arrs = line.split("\t");

		cityID = Integer.parseInt(arrs[0]);
		id = Integer.parseInt(arrs[1]);
		int id1 = Integer.parseInt(arrs[2]);
		int id2 = Integer.parseInt(arrs[3]);
		length = Double.parseDouble(arrs[4]);

		if (idStationMap.containsKey(id1))
		{
			stationID1 = idStationMap.get(id1);
		}
		if (idStationMap.containsKey(id1))
		{
			stationID2 = idStationMap.get(id2);
		}
	}

	public RailSecImsi work()
	{
		LOGHelper.GetLogger().writeLog(LogType.info, "================ 区间"+id+" ===============");
		LOGHelper.GetLogger().writeLog(LogType.info, "---------------- 站点1用户数："+stationID1.imsi_StationImsi_map.size()+" ----------------");
		LOGHelper.GetLogger().writeLog(LogType.info, "---------------- 站点2用户数："+stationID2.imsi_StationImsi_map.size()+" ----------------");
		
		// one - two
		Map<String, ImsiInfo> imsiInfoMap1 = new HashMap<String, ImsiInfo>();

		// two - one
		Map<String, ImsiInfo> imsiInfoMap2 = new HashMap<String, ImsiInfo>();
		
		/*
		 * 取两站点的交集用户,生成用户信息
		 */
		getImsiInfo(imsiInfoMap1, imsiInfoMap2);
		
		LOGHelper.GetLogger().writeLog(LogType.info, "---------------- 方向：1 ----------------");
		LOGHelper.GetLogger().writeLog(LogType.info, "---------------- 交集用户数：" + allUserCounter1 + " ----------------");
		LOGHelper.GetLogger().writeLog(LogType.info, "---------------- 速度过滤后用户数：" + filterUserCounter1 + " ----------------");
		LOGHelper.GetLogger().writeLog(LogType.info, "---------------- 结果用户数：" + imsiInfoMap1.size() + " ----------------");
		LOGHelper.GetLogger().writeLog(LogType.info, "---------------- 方向：2 ----------------");
		LOGHelper.GetLogger().writeLog(LogType.info, "---------------- 交集用户数：" + allUserCounter2 + " ----------------");
		LOGHelper.GetLogger().writeLog(LogType.info, "---------------- 速度过滤后用户数：" + filterUserCounter2 + " ----------------");
		LOGHelper.GetLogger().writeLog(LogType.info, "---------------- 结果用户数：" + imsiInfoMap2.size() + " ----------------");

		if (imsiInfoMap1.size() == 0 && imsiInfoMap2.size() == 0) return null;

		/*
		 * 计算结束调整时间
		 */
		calcImsiFixedEndTime(imsiInfoMap1);

		/*
		 * 计算结束调整时间
		 */
		calcImsiFixedEndTime(imsiInfoMap2);

		Map<String, ImsiInfoSimple> imsiInfoSimpleMap1 = imsiInfoMap2imsiInfoSimpleMap(imsiInfoMap1);
		Map<String, ImsiInfoSimple> imsiInfoSimpleMap2 = imsiInfoMap2imsiInfoSimpleMap(imsiInfoMap2);

		return new RailSecImsi(id, imsiInfoSimpleMap1, imsiInfoSimpleMap2);
	}

	/*
	 * 计算结束调整时间
	 */
	private void calcImsiFixedEndTime(Map<String, ImsiInfo> imsiInfoMap)
	{
		ImsiFixEndTimeHelper ifeth = new ImsiFixEndTimeHelper();
		for (ImsiInfo imsiInfo : imsiInfoMap.values())
		{
			ifeth.add(imsiInfo);
		}
		boolean noNeed = stationID1.outProvince || stationID2.outProvince;// 有省外站,则无需修正结束时间
		ifeth.fix(noNeed);
	}

	private Map<String, ImsiInfoSimple> imsiInfoMap2imsiInfoSimpleMap(Map<String, ImsiInfo> imsiInfoMap)
	{
		Map<String, ImsiInfoSimple> imsiInfoSimpleMap = new HashMap<String, ImsiInfoSimple>();
		for (ImsiInfo imsiInfo : imsiInfoMap.values())
		{
			imsiInfoSimpleMap.put(imsiInfo.imsi, new ImsiInfoSimple(imsiInfo));
		}
		return imsiInfoSimpleMap;
	}

	int allUserCounter1 = 0;
	int filterUserCounter1 = 0;
	int allUserCounter2 = 0;
	int filterUserCounter2 = 0;
	
	private void log(ImsiInfo info){
		if(info.one2Two){
			allUserCounter1++;
			if (info.enabled) 
				filterUserCounter1++;
		}else{
			allUserCounter2++;
			if (info.enabled) 
				filterUserCounter2++;
		}
		
	}
	
	/*
	 * 取两站点的交集用户,生成用户信息
	 */
	private void getImsiInfo(Map<String, ImsiInfo> imsiInfoMap1, Map<String, ImsiInfo> imsiInfoMap2)
	{
		for (Map.Entry<String, List<StationImsi>> kv : stationID1.imsi_StationImsi_map.entrySet())
		{
			String key = kv.getKey();

			if (stationID2.imsi_StationImsi_map.containsKey(key))
			{
				List<StationImsi> recs1 = kv.getValue();
				List<StationImsi> recs2 = stationID2.imsi_StationImsi_map.get(key);

				List<ImsiInfo> ls = new ArrayList<ImsiInfo>();
				for (StationImsi rec1 : recs1)
				{
					for (StationImsi rec2 : recs2)
					{
						ImsiInfo info = new ImsiInfo(key, this, rec1, rec2);
						info.init();

						if (info.enabled)
						{
							ls.add(info);
						}
						/* 测试用 */
						log(info);
					}
				}
				for (int i = 0; i < ls.size(); i++)
				{
					ImsiInfo info = ls.get(i);
					info.setMark(i);
					if (info.one2Two)
					{
						imsiInfoMap1.put(info.imsi, info);
					}
					else
					{
						imsiInfoMap2.put(info.imsi, info);
					}
				}
			}
		}
	}

	public void work(RailSecImsi railSegImsi, HSRData hsrData, Map<Long, Set<RailStation>> eci_station_map)
	{

		// one - two
		Map<Long, Map<String, ImsiEciTime>> xdrMap1 = new HashMap<Long, Map<String, ImsiEciTime>>();

		// two - one
		Map<Long, Map<String, ImsiEciTime>> xdrMap2 = new HashMap<Long, Map<String, ImsiEciTime>>();

		/*
		 * 两站点的交集用户,再和线路(中间段)中的用户取交集,先按ECI分组,再按imsi分组
		 */
		System.out.println(new Date() + "--------开始 求同车用户---------");
		getValidXdr(hsrData, xdrMap1, xdrMap2, railSegImsi.imsiInfoSimpleMap1, railSegImsi.imsiInfoSimpleMap2,
				eci_station_map);
		System.out.println(new Date() + "--------结束 求同车用户---------");
		if (xdrMap1.size() == 0 && xdrMap2.size() == 0) return;

		RailTrain railTrain1 = new RailTrain(this, true);
		RailTrain railTrain2 = new RailTrain(this, false);

		int trainid = 0;
		System.out.println(new Date() + "--------开始 求车次结果集---------");
		trainid = railTrain2.work(hsrData, railSegImsi.imsiInfoSimpleMap2, xdrMap2, trainid);
		railSegImsi.imsiInfoSimpleMap2.clear();
		trainid = railTrain1.work(hsrData, railSegImsi.imsiInfoSimpleMap1, xdrMap1, trainid);
		railSegImsi.imsiInfoSimpleMap1.clear();
		System.out.println(new Date() + "--------结束 求车次结果集---------");
		System.gc();
	}

	/*
	 * 两站点的交集用户,再和线路(中间段)中的用户取交集,先按ECI分组,再按imsi分组
	 */
	private void getValidXdr(HSRData hsrData, final Map<Long, Map<String, ImsiEciTime>> xdrMap1,
			final Map<Long, Map<String, ImsiEciTime>> xdrMap2, final Map<String, ImsiInfoSimple> imsiInfoMap1,
			final Map<String, ImsiInfoSimple> imsiInfoMap2, final Map<Long, Set<RailStation>> eci_station_map)
	{
		//省内站的话要在对时间进行约束
		int fixStime1, fixEtime1, fixStime2, fixEtime2;
		fixStime1 = stationID1.outProvince ? 0 : MidSpan;
		fixEtime1 = stationID2.outProvince ? 0 : MidSpan;
		fixStime2 = stationID2.outProvince ? 0 : MidSpan;
		fixEtime2 = stationID1.outProvince ? 0 : MidSpan;
		
		for (Xdr_ImsiEciTime xdrRecord : hsrData.xdrLocationList)
		{
			//20180208 不排除站点小区，以增加定位点
			/*if (eci_station_map.containsKey(xdrRecord.eci))
			{
				RailStation station = eci_station_map.get(xdrRecord.eci);
				if (!station.outProvince) continue;// 如果是省外站,可以加到线路中
			}*/

			dealXdr(xdrMap1, imsiInfoMap1, xdrRecord, fixStime1, fixEtime1);
			dealXdr(xdrMap2, imsiInfoMap2, xdrRecord, fixStime2, fixEtime2);
		}
		return;
	}

	private void dealXdr(Map<Long, Map<String, ImsiEciTime>> xdrMap, Map<String, ImsiInfoSimple> imsiInfoMap,
			Xdr_ImsiEciTime xdrRecord, int stimeToFix, int etimeToFix)
	{
		if (imsiInfoMap.containsKey(xdrRecord.imsi))
		{
			ImsiInfoSimple info = imsiInfoMap.get(xdrRecord.imsi);

			long time = xdrRecord.time;
			
//			if (time >= info.beginTime && time <= info.endTime) 
			//20180201 修改为使用 minBeginTime和maxEndTime来过滤 xdr，目的是生成更多的定位点
			if (time >= info.minBeginTime && time <= info.maxEndTime)
			{
				info.add(xdrRecord);
			}
			
			if (time > info.beginTime + stimeToFix && time <= info.endTimeFixed - etimeToFix)
			{
				Map<String, ImsiEciTime> map = null;
				if (xdrMap.containsKey(xdrRecord.eci))
				{
					map = xdrMap.get(xdrRecord.eci);
				}
				else
				{
					map = new HashMap<String, ImsiEciTime>();
					xdrMap.put(xdrRecord.eci, map);
				}

				if (map.containsKey(xdrRecord.imsi))
				{
					map.get(xdrRecord.imsi).stat(xdrRecord);//记录最小时间以及 timeSet.add(time);
				}
				else
				{
					map.put(xdrRecord.imsi, new ImsiEciTime(xdrRecord));
				}
			}

		}
	}

	public void fillSegInfo(List<HSRSecTrainData> secTrainData, List<RailSeg> railSegList)
	{
		for (HSRSecTrainData secData : secTrainData)
		{
			// 判断方向
			boolean one2Two = false;
			if (secData.endStatiionid == this.stationID2.id && secData.startStationid == this.stationID1.id)
			{
				one2Two = true;
			}
			try
			{
				// new SecSegHelper(secData, railSegList, one2Two).compute();
				new SimpleSecSegHelper(secData, railSegList, one2Two).compute();
			}
			catch (Throwable e)
			{
				String msg = e.getMessage();
				if (msg != null && msg.startsWith("车次【")) continue;
				e.printStackTrace();
			}
		}
	}
//
//	private class SecSegHelper
//	{
//		/**
//		 * 路段
//		 */
//		private List<RailSeg> railSegList;
//		/**
//		 * 车次
//		 */
//		private HSRSecTrainData secTrainData;
//		/**
//		 * 定位点时间-加速度
//		 */
//		private TreeMap<Long, Double> accelerationMap;
//		/**
//		 * 定位点时间-速度
//		 */
//		private TreeMap<Long, Double> speedMap;
//		/**
//		 * 定位点时间-位移
//		 */
//		private TreeMap<Long, Double> distanceMap;
//		/**
//		 * 1到2 : true; 2到1：false
//		 */
//		private boolean one2Two;
//
//		public SecSegHelper(HSRSecTrainData secTrainData, List<RailSeg> railSegList, boolean one2Two)
//		{
//
//			this.secTrainData = secTrainData;
//			this.railSegList = railSegList;
//			this.one2Two = one2Two;
//			init();
//
//		}
//
//		private void init()
//		{
//
//			System.out.println("================" + secTrainData.trainkey);
//			// sortLocPoints();
//			accelerationMap = new TreeMap<>();
//			speedMap = new TreeMap<>();
//			distanceMap = new TreeMap<>();
//
//			// 起始站 - 第一个定位点
//			int i = 0;
//			double vLastPoint = 0;// 上一个定位点的速度
//			LocPoint lastPoint = null;
//			LocPoint thisPoint = null;
//			while (i < secTrainData.locPointList.size()
//					&& !isValidLocPoint(thisPoint = secTrainData.locPointList.get(i++)))
//				;// 找出第一个 有效定位点
//
//			if (thisPoint == null || !isValidLocPoint(thisPoint))
//				throw new RuntimeException("车次【" + secTrainData.trainkey + "】没有找到有效定位点");
//
//			// 通过加速度来判断是否停站，而不通过给定的.假设是停站
//			boolean stopStart = true, stopEnd = true;
//			double accStartIfStop = thisPoint.startStationDistance * 2
//					/ Math.pow(mSecToSec((thisPoint.ltime - secTrainData.lstime)), 2);
//			if (accStartIfStop > 1 || accStartIfStop < 0)
//			{
//				stopStart = false;
//			}
//
//			if (/* !secTrainData.stopStart */!stopStart)
//			{// 不停的话，视为匀速
//
//				vLastPoint = thisPoint.startStationDistance / mSecToSec(thisPoint.ltime - secTrainData.lstime);
//				speedMap.put(secTrainData.lstime, vLastPoint);
//				accelerationMap.put(secTrainData.lstime, 0D);
//
//			}
//			else
//			{// 停，从0开始匀加速， s = 1/2 * a * t^2
//
//				vLastPoint = 0D;
//				speedMap.put(secTrainData.lstime, vLastPoint);
//				accelerationMap.put(secTrainData.lstime, thisPoint.startStationDistance * 2
//						/ Math.pow(mSecToSec((thisPoint.ltime - secTrainData.lstime)), 2));
//			}
//			distanceMap.put(secTrainData.lstime, 0D);
//
//			vLastPoint = vLastPoint
//					+ accelerationMap.get(secTrainData.lstime) * mSecToSec(thisPoint.ltime - secTrainData.lstime);
//			lastPoint = thisPoint;
//
//			// 相邻定位点之间
//			for (; i < secTrainData.locPointList.size(); i++)
//			{
//				thisPoint = secTrainData.locPointList.get(i);
//				if (isValidLocPoint(thisPoint))
//				{
//					distanceMap.put(thisPoint.ltime, thisPoint.startStationDistance);
//					if (lastPoint != null)
//					{
//						speedMap.put(lastPoint.ltime, vLastPoint);
//
//						double accLastToThis = computeAcc(vLastPoint, lastPoint, thisPoint);
//						assert accLastToThis > -3 && accLastToThis < 1;
//						accelerationMap.put(lastPoint.ltime, accLastToThis);
//
//						vLastPoint = nextV0(vLastPoint, accLastToThis, mSecToSec(thisPoint.ltime - lastPoint.ltime));
//						assert vLastPoint > 0;
//					}
//					lastPoint = thisPoint;
//				}
//			}
//			// 最后一个定位点 - 终点站
//			double accEndIfStop = -Math.pow(vLastPoint, 2)
//					/ (2 * (RailSec.this.length - thisPoint.startStationDistance));
//			if (accEndIfStop < -3 || accEndIfStop > 0)
//			{
//				stopEnd = false;
//			}
//
//			if (/* !secTrainData.stopEnd */!stopEnd)
//			{// 不停，匀速
//				speedMap.put(thisPoint.ltime, vLastPoint);
//				accelerationMap.put(thisPoint.ltime, 0D);
//			}
//			else
//			{// 停，匀减速，v^2 - v0^2 = 2 * a *s --> v==0 --> a = - v0^2 / 2s
//				speedMap.put(thisPoint.ltime, vLastPoint);
//				accelerationMap.put(thisPoint.ltime,
//						-Math.pow(vLastPoint, 2) / (2 * (RailSec.this.length - thisPoint.startStationDistance)));
//			}
//		}
//
//		private boolean isValidLocPoint(LocPoint locPoint)
//		{
//			return locPoint.startStationDistance > 0 && (distanceMap.floorKey(locPoint.ltime) == null
//					|| distanceMap.floorEntry(locPoint.ltime).getValue() < locPoint.startStationDistance);
//		}
//
//
//		public void compute()
//		{
//
//			if (railSegList.isEmpty())
//			{
//				return;
//			}
//			if (railSegList.size() == 1)
//			{// 只有1个路段
//				secTrainData.segList.add(secTrainData.new TrainSeg(railSegList.get(0).id, secTrainData.lstime,
//						secTrainData.letime, length / (secTrainData.letime - secTrainData.lstime)));
//				return;
//			}
//
//			// sortSegs();
//			if (true)
//			{// 打印定位点
//				System.out.println("----------------------------");
//				System.out.println("加速度map：" + accelerationMap);
//				System.out.println("----------------------------");
//				System.out.println("速度map：" + speedMap);
//				System.out.println("----------------------------");
//			}
//
//			double v0 = speedMap.get(secTrainData.lstime); // 初速度
//
//			if (one2Two)
//			{
//				for (RailSeg railSeg : railSegList)
//				{
//					secTrainData.segList
//							.add(secTrainData.new TrainSeg(railSeg.id, railSeg.distToStart, railSeg.distToEnd));
//				}
//			}
//			else
//			{
//				for (int i = railSegList.size() - 1; i >= 0; i--)
//				{
//					RailSeg railSeg = railSegList.get(i);
//					secTrainData.segList
//							.add(secTrainData.new TrainSeg(railSeg.id, railSeg.distToStart, railSeg.distToEnd));
//				}
//			}
//
//			// 所有路段时长求和
//			long sumDeltaT = compute(secTrainData.segList.iterator(), v0, secTrainData.lstime,
//					nextA(secTrainData.lstime));
//
//		}
//
//		/**
//		 * 计算两个定位点之间的加速度
//		 * 
//		 * @param vA
//		 *            A点的速度
//		 * @param pointA
//		 * @param pointB
//		 * @return 从A到B的加速度
//		 */
//		private double computeAcc(double vA, LocPoint pointA, LocPoint pointB)
//		{
//			double _s = pointB.startStationDistance - pointA.startStationDistance;
//
//			long _t = pointB.ltime - pointA.ltime;
//
//			return computeAcc(vA, mSecToSec(_t), _s);
//		}
//
//		/**
//		 * @param v0
//		 *            单位：m/s
//		 * @param _t
//		 *            单位：s
//		 * @param _s
//		 *            单位：m
//		 * @return a 单位：m/s^2
//		 */
//		private double computeAcc(double v0, double _t, double _s)
//		{
//			// 根据 v0 * _t + 1/2 * a * _t^2 = _s
//			// 得：a = (_s - v0 * t) * 2 / _t^2
//			return (_s - v0 * _t) * 2 / Math.pow(_t, 2);
//		}
//
//		private long compute(Iterator<TrainSeg> segs, double v0, long lstime, double a)
//		{
//			assert v0 >= 0 && lstime >= 0;
//			TrainSeg seg = segs.next();
//			try
//			{
//				double _s = seg.disToEnd - seg.disToStart; // 位移差 △s
//				long _t = secToMSec(computeDeltaT(v0, a, _s));// 时间差 △t
//
//				seg.stime = lstime;
//				seg._t = _t;
//				seg.acc = a;
//				seg.v0 = v0;
//
//				long etime = seg.stime + _t;
//				if (!segs.hasNext())
//				{
//					return _t;
//				}
//				else
//				{
//					return _t + compute(segs, fixNextV0(seg.stime, etime, nextV0(v0, a, mSecToSec(_t))), etime,
//							nextA(etime));
//				}
//			}
//			catch (Exception e)
//			{
//				e.printStackTrace();
//				// throw new
//				// IllegalArgumentException(String.format("车次【%s】路段【%s】初速度：%s，开始时间：%s，加速度：%s",secTrainData.trainkey,
//				// seg.segId, v0, lstime, a));
//				return -1;
//			}
//		}
//
//		/**
//		 * v0 * _t + 1/2 (_t ^2) * acc = _s
//		 * 
//		 * @param v0
//		 *            单位：m/s
//		 * @param acc
//		 *            单位：m/s^2
//		 * @param _s
//		 *            单位：m
//		 * @return _t 单位：s
//		 */
//		private double computeDeltaT(double v0, double acc, double _s)
//		{
//			if (v0 < 0 || _s < 0)
//			{
//				throw new IllegalArgumentException();
//			}
//
//			if (acc == 0)
//			{// 匀速
//				return _s / v0;
//			}
//
//			double a = acc / 2;
//
//			double b = v0;
//
//			double c = -_s;
//
//			Tuple2<Double, Double> result = computeZeroPoint(a, b, c);
//
//			// 转换成毫秒
//			if (result.first >= 0)
//				return result.first;
//
//			else if (result.second >= 0)
//				return result.second;
//			// 没有正解
//			else throw new IllegalArgumentException(String.format("车次【%s】初速度：%s，加速度：%s，位移：%s，计算结果:%s,%s",
//					secTrainData.trainkey, v0, acc, _s, result.first, result.second));
//		}
//
//		/**
//		 * 求解 方程y = a * x^2 + b * x + c，在y = 0时的解
//		 * 
//		 * @param a
//		 * @param b
//		 * @param c
//		 * @return 两个解
//		 */
//		private Tuple2<Double, Double> computeZeroPoint(double a, double b, double c)
//		{
//			// 判别式△ = b^2-4ac
//			double judge = Math.pow(b, 2) - 4 * a * c;
//
//			if (judge < 0)
//			{// 方程无解
//				throw new IllegalArgumentException(
//						String.format("车次【%s】方程无解a：%s，b：%s，c：%s", secTrainData.trainkey, a, b, c));
//			}
//
//			double tmp = Math.sqrt(judge);
//			return new Tuple2((tmp - b) / (2 * a), (-tmp - b) / (2 * a));
//		}
//
//		/**
//		 * 下一路段的初速度
//		 * 
//		 * @param v0
//		 *            单位：m
//		 * @param a
//		 *            单位：m/s^2
//		 * @param _t
//		 *            单位: s
//		 * @return
//		 */
//		private double nextV0(double v0, double a, double _t)
//		{
//			assert v0 >= 0 && _t >= 0;
//			return v0 + a * _t;
//		}
//
//		/**
//		 * 下一路段的加速度
//		 * 
//		 * @param t
//		 * @return
//		 */
//		private double nextA(long t)
//		{
//			return accelerationMap.floorEntry(t).getValue();
//		}
//
//		private double mSecToSec(long ms)
//		{
//			return ms / 1000D;
//		}
//
//		private long secToMSec(double s)
//		{
//			return (long) (s * 1000L);
//		}
//
//		/**
//		 * 对于跨节点的路段，需要对下一路段的初速度进行调整
//		 * 
//		 * @param t0
//		 *            当前路段的开始时间
//		 * @param t1
//		 *            下一路段的开始时间
//		 * @return 调整后的 下一路段的初速度
//		 */
//		private double fixNextV0(long t0, long t1, double v1)
//		{
//			Map.Entry<Long, Double> e0 = accelerationMap.floorEntry(t0), e1 = accelerationMap.floorEntry(t1);
//
//			double acc0 = e0.getValue(), acc1 = e1.getValue();
//
//			if (acc0 != acc1)
//			{// 加速度不一样说明不在同一直线上，即路段内含有转折点
//
//				// 取路段内最后一个定位点 计算调整后的速度
//				long tPoint = e1.getKey();
//
//				assert speedMap.containsKey(tPoint);
//
//				double vPoint = speedMap.get(tPoint);
//
//				return nextV0(vPoint, acc1, mSecToSec(t1 - tPoint));
//
//			}
//			return v1;
//		}
//
//	}

	private class SimpleSecSegHelper
	{

		private final double MAX_SPEED = 400 / 3.6D;

		private final double MIN_SPEED = 100 / 3.6D;

		/**
		 * 区间的平均速度
		 */
		private double secAvgSpeed;
		/**
		 * 路段
		 */
		private List<RailSeg> railSegList;
		/**
		 * 车次
		 */
		private HSRSecTrainData secTrainData;
		/**
		 * 定位点位移-速度
		 */
		private TreeMap<Double, Double> speedMap;
		/**
		 * 定位点时间-位移
		 */
		// private TreeMap<Long,Double> distanceMap;
		/**
		 * 1到2 : true; 2到1：false
		 */
		private boolean one2Two;

		private double validMaxSpeed;

		private double validMinSpeed;

		public SimpleSecSegHelper(HSRSecTrainData secTrainData, List<RailSeg> railSegList, boolean one2Two)
		{

			this.secTrainData = secTrainData;
			this.railSegList = railSegList;
			this.one2Two = one2Two;

			this.secAvgSpeed = RailSec.this.length / mSecToSec(secTrainData.letime - secTrainData.lstime);
			this.validMaxSpeed = secAvgSpeed + 100 / 3.6D;
			this.validMinSpeed = secAvgSpeed - 100 / 3.6D;
			validMaxSpeed = validMaxSpeed > MAX_SPEED ? MAX_SPEED : validMaxSpeed;
			validMinSpeed = validMinSpeed < MIN_SPEED ? MIN_SPEED : validMinSpeed;

			init();

		}

		private void init()
		{

			// System.out.println("================" + secTrainData.trainkey);
			speedMap = new TreeMap<>();

			// 起始站 - 第一个定位点
			double vLastPoint = 0;// 上一个定位点的速度
			LocPoint lastPoint = null;
			LocPoint thisPoint = null;
			Iterator<LocPoint> iter = secTrainData.locPointList.iterator();

			// 找出第一个 有效定位点
			boolean findFirstValidPoint = false;
			while (iter.hasNext())
			{
				thisPoint = iter.next();
				if (isValidLocPoint(thisPoint)) {
					findFirstValidPoint = true;
					break;
				}
				iter.remove();
			}

			if (!findFirstValidPoint)
				throw new RuntimeException("车次【" + secTrainData.trainkey + "】没有找到有效定位点");

			speedMap.put(0D, avgV(null, thisPoint));

			lastPoint = thisPoint;

			// 相邻定位点之间
			while (iter.hasNext())
			{
				thisPoint = iter.next();
				if (isValidLocPoint(lastPoint, thisPoint))
				{
					vLastPoint = avgV(lastPoint, thisPoint);

					assert vLastPoint > 0;
					speedMap.put(lastPoint.startStationDistance, vLastPoint);

					lastPoint = thisPoint;
				}
				else iter.remove();
			}
			// 最后一个定位点 - 终点站
			speedMap.put(lastPoint.startStationDistance, avgV(lastPoint, null));
		}

		private boolean isValidLocPoint(LocPoint locPoint)
		{
//			double tmpSpeed = -1;
			return locPoint.startStationDistance > 0 /*&& (tmpSpeed = avgV(null, locPoint)) <= validMaxSpeed
					&& tmpSpeed >= validMinSpeed*/;
		}

		private boolean isValidLocPoint(LocPoint lastPoint, LocPoint thisPoint)
		{
//			double tmpSpeed = -1;
			return thisPoint.startStationDistance > 0 && lastPoint.startStationDistance < thisPoint.startStationDistance
					/*&& (tmpSpeed = avgV(lastPoint, thisPoint)) <= validMaxSpeed && tmpSpeed >= validMinSpeed
					&& (tmpSpeed = (RailSec.this.length - thisPoint.startStationDistance)
							/ mSecToSec(secTrainData.letime - thisPoint.ltime)) <= validMaxSpeed
					&& tmpSpeed >= validMinSpeed*/;
		}

		private double mSecToSec(long ms)
		{
			return ms / 1000D;
		}

		/*private long secToMSec(double s)
		{
			return (long) (s * 1000L);
		}*/

		private double avgV(LocPoint lastPoint, LocPoint thisPoint)
		{
			double v = -1;
			if (lastPoint == null)
				v = (thisPoint.startStationDistance / mSecToSec(thisPoint.ltime - secTrainData.lstime));
			else if (thisPoint == null)
				v = (RailSec.this.length - lastPoint.startStationDistance)
						/ mSecToSec(secTrainData.letime - lastPoint.ltime);
			else v = (thisPoint.startStationDistance - lastPoint.startStationDistance)
					/ mSecToSec(thisPoint.ltime - lastPoint.ltime);
			// assert v > 0 && v < MAX_SPEED;
			if (v < 0 || v > MAX_SPEED)
			{
				System.out.println("_____");
			}
			return v;
		}

		public void compute()
		{

			if (railSegList.isEmpty())
			{
				return;
			}
			if (railSegList.size() == 1)
			{// 只有1个路段
				secTrainData.segList.add(secTrainData.new TrainSeg(railSegList.get(0).id, secTrainData.lstime,
						secTrainData.letime, length / (secTrainData.letime - secTrainData.lstime)));
				return;
			}

/*			{// 打印定位点
				System.out.println("----------------------------");
				System.out.println("速度map：" + speedMap);
				LOGHelper.GetLogger().writeLog(LogType.info, secTrainData.trainkey + speedMap.toString());
				System.out.println("----------------------------");
			}*/
			
			if (one2Two)
			{
				for (RailSeg railSeg : railSegList)
				{
					secTrainData.segList
							.add(secTrainData.new TrainSeg(railSeg.id, railSeg.distToStart, railSeg.distToEnd));
				}
			}
			else
			{
				for (int i = railSegList.size() - 1; i >= 0; i--)
				{
					RailSeg railSeg = railSegList.get(i);
					secTrainData.segList
							.add(secTrainData.new TrainSeg(railSeg.id, railSeg.distToStart, railSeg.distToEnd));
				}
			}
			double lastSegEndTime = secTrainData.lstime / 1000D;//(s)
			double segHeadToStartStation = -1, segTailToStartStation = -1;
			for (TrainSeg trainSeg : secTrainData.segList)
			{
				if (one2Two)
				{
					segHeadToStartStation = trainSeg.disToStart;
					segTailToStartStation = trainSeg.disToEnd;
				}
				else
				{
					segHeadToStartStation = (RailSec.this.length - trainSeg.disToEnd);
					segTailToStartStation = (RailSec.this.length - trainSeg.disToStart);
				}
				Map.Entry<Double, Double> eHead = speedMap.floorEntry(segHeadToStartStation);
				Map.Entry<Double, Double> eTail = speedMap.floorEntry(segTailToStartStation);
				double segHeadSpeed = eHead.getValue();
				double segTailSpeed = eTail.getValue();
				if (segHeadSpeed == segTailSpeed)
				{// 不跨定位点

					trainSeg.avgSpeed = segHeadSpeed;
					trainSeg._t = (trainSeg.disToEnd - trainSeg.disToStart) / trainSeg.avgSpeed;

				}
				else
				{// 仅考虑跨一个定位点
					double distPoint = eTail.getKey();
					double segHeadTimeCost = (distPoint - segHeadToStartStation) / segHeadSpeed;
					double segTailTimeCost = (segTailToStartStation - distPoint) / segTailSpeed;
					trainSeg._t = segHeadTimeCost + segTailTimeCost;//行驶时间
					trainSeg.avgSpeed = (trainSeg.disToEnd - trainSeg.disToStart) / (segHeadTimeCost + segTailTimeCost);
				}
				trainSeg.v0 = mpsToKmph(segHeadSpeed);
				trainSeg.avgSpeed = mpsToKmph(trainSeg.avgSpeed);
				trainSeg.acc = 0;
				trainSeg.stime = lastSegEndTime;
				lastSegEndTime = trainSeg.stime + trainSeg._t;
			}
		}

		/**
		 * 1m/s = 3.6 km/h
		 * 
		 * @param mps
		 * @return
		 */
		private double mpsToKmph(double mps)
		{
			return mps * 3.6;
		}
	}

}
