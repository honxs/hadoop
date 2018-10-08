package cn.mastercom.bigdata.stat.userAna.hsr;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.stat.userAna.hsr.HSRSecTrainData.LocPoint;
import cn.mastercom.bigdata.stat.userAna.hsr.RailSec.DistancePair;
import cn.mastercom.bigdata.stat.userAna.model.ImsiEciTime;
import cn.mastercom.bigdata.stat.userAna.model.ImsiGroupHelper;
import cn.mastercom.bigdata.stat.userAna.model.ImsiInfo;
import cn.mastercom.bigdata.stat.userAna.model.ImsiInfoSimple;
import cn.mastercom.bigdata.stat.userAna.model.RruInfo;
import cn.mastercom.bigdata.stat.userAna.model.XdrGroupHelper;
import cn.mastercom.bigdata.stat.userAna.model.XdrGroupHelperRru;
import cn.mastercom.bigdata.stat.userAna.model.Xdr_ImsiEciTime;
import cn.mastercom.bigdata.stat.userAna.model.IXdrGroupHelper;

public class RailTrain
{
	public static String[] decodeKey(String key)
	{
		String[] arrs = key.split("___");

		return arrs;
	}

	public static String encodeKey(String imsi1, String imsi2)
	{
		int result = imsi1.compareTo(imsi2);

		if (result == 0)
		{
			return null;
		}
		else if (result > 0)
		{
			return imsi1 + "___" + imsi2;
		}
		else
		{
			return imsi2 + "___" + imsi1;
		}
	}

	// 任意两个用户的xdr中,xdr间隔大于等于此值,则用户不在一趟车中
	private final int InvalidSpan = 40000;
	// 任意两个用户的xdr中,xdr间隔小于等于此值,则用户可能在一趟车中
	private final int ValidSpan = 10000;
	// 发车时间间隔小于此值则丢弃
	private final int BeginTimeSpan = 60000;

	private final int maxBeginOrEndSpan = 10 * 60 * 1000;
	// 定位点 在 开始与结束的 这个距离将被 收缩清洗
	private final int pointToCleanInDistFromStartEnd = 3000;
	// 与上面参数挂钩，100米内的将被去掉
	private final int locPointCleanDist = 200;

	public int cityID;
	public int id;
	public RailStation stationBegin;
	public RailStation stationEnd;
	public double length;

	private DistanceHelper distanceHelper;

	private boolean m_bRru;

	class DistanceHelper
	{
		/**
		 * 配置 小区集与stationID1的距离
		 */
		private Map<Long, DistancePair> m_eci_dist_map;
		boolean one2two;

		public DistanceHelper(Map<Long, DistancePair> eci_dist_map, boolean one2two)
		{
			m_eci_dist_map = eci_dist_map;
			this.one2two = one2two;
		}

		/**
		 * 通过配置获取距离(与站点1的距离)
		 */
		public double getDistanceByEci(long eci)
		{
			if (m_eci_dist_map.containsKey(eci))
			{
				DistancePair pair = m_eci_dist_map.get(eci);
				if (one2two)
				{
					return pair.distance1;
				}
				else
				{
					return pair.distance2;
				}
			}
			else
			{
				return -1;
			}
		}

		public double getDistanceByRru(RruInfo rruInfo)
		{
			if (one2two)
			{
				return rruInfo.distance1;
			}
			else
			{
				return rruInfo.distance2;
			}
		}
	}

	Set<Long> EciSet = null;
	/**
	 * 相邻小区的RRU切换点
	 */
	public Collection<RruInfo> rruInfos;

	public RailTrain(RailSec railSeg, boolean one2two)
	{
		cityID = railSeg.cityID;
		id = railSeg.id;

		if (one2two)
		{
			stationBegin = railSeg.stationID1;
			stationEnd = railSeg.stationID2;
		}
		else
		{
			stationBegin = railSeg.stationID2;
			stationEnd = railSeg.stationID1;
		}

		length = railSeg.length;

		distanceHelper = new DistanceHelper(railSeg.eci_dist_map, one2two);
		EciSet = railSeg.eci_dist_map.keySet();
		rruInfos = railSeg.rruInfoMap.values();

		m_bRru = rruInfos.size() > 1;
	}

	public int work(HSRData hsrData, Map<String, ImsiInfoSimple> imsiInfoMap,
			Map<Long, Map<String, ImsiEciTime>> xdrMap, int trainid)
	{
		/*
		 * 获取同车用户对
		 */
		System.out.println(new Date() + "--------开始 求同车用户对---------");
		Set<String> validMap = getImsiPair(xdrMap);
		System.out.println(new Date() + "--------结束 求同车用户对---------");

		/*
		 * 根据imsi,进行同车分组
		 */
		System.out.println(new Date() + "--------开始 进行同车分组---------");
		List<Set<String>> imsiGroups = groupImsi(validMap);
		System.out.println(new Date() + "--------结束 进行同车分组---------");

		/*
		 * 整理分组信息
		 */
		System.out.println(new Date() + "--------开始 整理分组信息---------");
		List<ImsiGroupInfo> imsiGroupInfos = getImsiGroups(imsiInfoMap, imsiGroups);
		System.out.println(new Date() + "--------结束 整理分组信息---------");
		
		/*
		 * 获取同车用户的xdr并按时间排序,取出连续20秒的位置点,并输出
		 */
		System.out.println(new Date() + "--------开始 求定位点---------");
		int result = exportPoint(hsrData, imsiGroupInfos, trainid);
		System.out.println(new Date() + "--------结束 求定位点---------");
		return result;

	}

	private List<ImsiGroupInfo> getImsiGroups(Map<String, ImsiInfoSimple> imsiInfoMap, List<Set<String>> imsiGroups)
	{
		List<ImsiGroupInfo> imsiGroupInfos = new ArrayList<ImsiGroupInfo>();
		for (Set<String> set : imsiGroups)
		{
			ImsiGroupInfo imsiGroupInfo = getImsiGroup(imsiInfoMap, set);
			imsiGroupInfos.add(imsiGroupInfo);
		}

		return imsiGroupInfos;
	}

	private ImsiGroupInfo getImsiGroup(Map<String, ImsiInfoSimple> imsiInfoMap, Set<String> set)
	{
		ImsiGroupInfo imsiGroupInfo = new ImsiGroupInfo(this);
		for (String imsi : set)
		{
			if (imsiInfoMap.containsKey(imsi))
			{
				ImsiInfoSimple info = imsiInfoMap.get(imsi);
				imsiGroupInfo.add(info);
			}
		}
		return imsiGroupInfo;
	}

	private HSRSecTrainData createHSRSecTrainData(ImsiGroupInfo group, int trainid)
	{
		HSRSecTrainData data = new HSRSecTrainData(cityID, id, trainid, group.beginTime, group.endTime, distanceHelper.one2two ,this.length);

		data.stopStart = group.stopStart;
		data.stopEnd = group.stopEnd;

		data.startStationid = stationBegin.id;
		data.endStatiionid = stationEnd.id;

		return data;
	}

	/*
	 * 获取同车用户的xdr并按时间排序,取出连续20秒的位置点,并输出
	 */
	private int exportPoint(HSRData hsrData, List<ImsiGroupInfo> imsiGroupInfos, int trainid)
	{
		List<HSRSecTrainData> hsrSecTrainDatas = new ArrayList<HSRSecTrainData>();
		for (ImsiGroupInfo group : imsiGroupInfos)
		{
			trainid++;

			HSRSecTrainData data = createHSRSecTrainData(group, trainid);

			// 用户
			for (ImsiInfoSimple imsiInfo : group.imsiInfoList)
			{
				data.imsiList.add(imsiInfo/*.imsiOrg*/);
			}

			if (m_bRru)
			{
				exportRRULocPoint(data, group);
			}else {
				exportLocPoint(data, group);
			}
			cleanLocPoint(data.locPointList);

			if (fixTrainDataTime(data))
			{
				hsrSecTrainDatas.add(data);
			}
		}
		hsrSecTrainDatas = deleteInvalidTrain(hsrSecTrainDatas);
		hsrData.secTrainDataList.addAll(hsrSecTrainDatas);

		return trainid;
	}
	
	private void exportLocPoint(HSRSecTrainData data, ImsiGroupInfo group){
		XdrGroupHelper xdrGroupHelper = new XdrGroupHelper();
		getGroupXdrRecords(xdrGroupHelper, group, data);
		work(data, xdrGroupHelper);
	}
	
	private void exportRRULocPoint(HSRSecTrainData data, ImsiGroupInfo group){
		//导出无覆盖时间段
//		exportLocPoint(data, group);
		XdrGroupHelperRru xdrGroupHelperRru = new XdrGroupHelperRru();
		getGroupXdrRecords(xdrGroupHelperRru, group);
		workRru(data, xdrGroupHelperRru);
	}
	
	private void cleanLocPoint(List<LocPoint> locPointList)
	{
		if (locPointList.size() < 2) return;

		int index = 0;
		LocPoint lp = locPointList.get(index);
		index++;
		
		while(index < locPointList.size())
		{
			LocPoint _lp = locPointList.get(index);
			
			//速度校验
			if (checkLocPointSpeed(lp, _lp))
			{
				lp = _lp;
				index++;
			}
			else
			{
				locPointList.remove(index);
			}
			
		}
	}
	
	private boolean checkLocPointSpeed(LocPoint lp, LocPoint _lp)
	{
		double distDiff = Math.abs(lp.startStationDistance - _lp.startStationDistance);
		long timeDiff = Math.abs(lp.ltime - _lp.ltime);
		
		double speed = ImsiInfo.speedRatio * distDiff / (timeDiff);
		
		return speed < 450;
	}

	private void workRru(HSRSecTrainData data, XdrGroupHelperRru xdrGroupHelper)
	{
		data.locPointList.clear();
		Map<RruInfo, Long> result = xdrGroupHelper.work(rruInfos);
		for (Map.Entry<RruInfo, Long> kv : result.entrySet())
		{
			long time = kv.getValue();
			RruInfo rruInfo = kv.getKey();
			double dist = distanceHelper.getDistanceByRru(rruInfo);
			List<Long> ecis = new ArrayList<Long>();
			ecis.addAll(rruInfo.ecis1);
			ecis.addAll(rruInfo.ecis2);
			data.add(time, dist, ecis);
		}

		// 排序
		Collections.sort(data.locPointList, new Comparator<LocPoint>()
		{
			@Override
			public int compare(LocPoint o1, LocPoint o2)
			{
				return Long.compare(o1.ltime, o2.ltime);
			}
		});
		
		if (data.locPointList.size() < 2) return;

		int index = 0;
		LocPoint lp = data.locPointList.get(index);
		index++;
		
		while(index < data.locPointList.size())
		{
			LocPoint _lp = data.locPointList.get(index);
			
			double distDiff = Math.abs(lp.startStationDistance - _lp.startStationDistance);
			long timeDiff = Math.abs(lp.ltime - _lp.ltime);
			if (distDiff < 2000 || timeDiff < 15000)
			{
				data.locPointList.remove(index);
			}
			else
			{
				lp = _lp;
				index++;
			}
		}

	}

	/**
	 * 剔除开车时间间隔在1分钟内的记录
	 * 
	 * @param hsrSecTrainDatas
	 */
	private List<HSRSecTrainData> deleteInvalidTrain(List<HSRSecTrainData> hsrSecTrainDatas)
	{
		List<HSRSecTrainData> result = new ArrayList<HSRSecTrainData>();
		Collections.sort(hsrSecTrainDatas, new Comparator<HSRSecTrainData>()
		{
			@Override
			public int compare(HSRSecTrainData o1, HSRSecTrainData o2)
			{
				return Long.compare(o1.letime, o2.letime);
			}
		});

		int count = hsrSecTrainDatas.size();
		for (int i = 0; i < count; i++)
		{
			HSRSecTrainData previous = getHSRSecTrainDataByIndex(hsrSecTrainDatas, i - 1);
			HSRSecTrainData hsrSecTrainData = getHSRSecTrainDataByIndex(hsrSecTrainDatas, i);
			HSRSecTrainData next = getHSRSecTrainDataByIndex(hsrSecTrainDatas, i + 1);

			int userCount = hsrSecTrainData.imsiList.size();
			
			if ((previous == null || hsrSecTrainData.lstime - previous.lstime > BeginTimeSpan)
					&& (next == null || next.lstime - hsrSecTrainData.lstime > BeginTimeSpan)
					&& /*0227add 人数少于10的，认为不准*/(userCount >= 10))
			{
				result.add(hsrSecTrainData);
			}else if (userCount > 50){//180207 add 人数多于50人的，不剔除
				result.add(hsrSecTrainData);
			}

		}

		return result;
	}

	HSRSecTrainData getHSRSecTrainDataByIndex(List<HSRSecTrainData> hsrSecTrainDatas, int index)
	{
		if (index < 0 || index >= hsrSecTrainDatas.size())
		{
			return null;
		}
		return hsrSecTrainDatas.get(index);
	}

	/**
	 * 计算开车/停车时间, 如果车速过大,返回false
	 * 
	 * @param data
	 */
	private boolean fixTrainDataTime(HSRSecTrainData data)
	{
		List<LocPoint> points = new ArrayList<LocPoint>();

		for (LocPoint point : data.locPointList)
		{
			if (point.startStationDistance > 800)
			{
				points.add(point);
			}
		}

		if (/*m_bRru && */points.size() > 1)
		{
			if (!stationBegin.outProvince)
			{
				LocPoint spoint = points.get(0);
				long span1 = calcSpan(spoint.startStationDistance, spoint, points.get(1), data.stopStart, true);
				data.lstime = spoint.ltime - span1;
			}

			if (!stationEnd.outProvince)
			{
				int index = points.size() - 1;
				LocPoint epoint = points.get(index);
				long span2 = calcSpan(length - epoint.startStationDistance, epoint, points.get(index - 1),
						data.stopEnd, false);
				data.letime = epoint.ltime + span2;
			}
		}
		else
		{
			if (!stationBegin.outProvince)
			{
				if (data.stopStart)
				{
					data.lstime -= 150 * 1000;
				}
				else
				{
					data.lstime -= 60 * 1000;
				}
			}

			if (!stationEnd.outProvince)
			{
				if (data.stopEnd)
				{
					data.letime += 150 * 1000;
				}
				else
				{
					data.letime += 60 * 1000;
				}
			}
		}

		double speed = ImsiInfo.speedRatio * length / (data.letime - data.lstime);
		return speed < ImsiInfo.MaxSpeed;

	}

	/**
	 * 获取开车时间和停车时间
	 * 
	 * @param dist2station
	 *            开车/停车 到站点的距离
	 * @param point1
	 *            区间上的定位点
	 * @param point2
	 *            区间上的定位点
	 * @param stop
	 *            是否停站
	 * @return 开车/停车 到站点的时间
	 */

	private long calcSpan(double dist2station, LocPoint point1, LocPoint point2, boolean stop, boolean isStartStation)
	{
		
		double result = maxBeginOrEndSpan;
		if (stop && isStartStation){
			result = calcStartSpanOfStop(dist2station);
		}else if(stop && !isStartStation){
			result = calcEndSpanOfStop(dist2station);
		}else{
			long time = Math.abs(point1.ltime - point2.ltime);
			double dist = Math.abs(point1.startStationDistance - point2.startStationDistance);
			result = time * dist2station / dist;
		}

		if (result > maxBeginOrEndSpan)
		{
			result = maxBeginOrEndSpan;
		}
		return (long) result;
	}
	
	/**
	 * 对加速曲线进行拟合
	 * 时间区间	路程		累计路程	
	 * 0-60s  	1km		1
	 * 60-90s   1km		2
	 * 90-120s  1.5km	3.5
	 * 120-150s 1.75km	5.25
	 * 150-180s 2km		7.25
	 * @param dist2station
	 * @return
	 */
	private long calcStartSpanOfStop(double dist2station){
		if(dist2station > 0 && dist2station <= 1000){
			return (long)(60 * (dist2station / 1000) * 1000);
		}else if(dist2station > 1000 && dist2station <= 2000){
			return (long)((60 + 30 * ((dist2station - 1000) / 1000)) * 1000);
		}else if(dist2station > 2000 && dist2station <= 3500){
			return (long)((90 + 30 * ((dist2station - 2000) / 1500)) * 1000);
		}else if(dist2station > 3500 && dist2station <= 5250){
			return (long)((120 + 30 * ((dist2station - 3500) / 1750)) * 1000);
		}else if(dist2station > 5250 && dist2station <= 7250){
			return (long)((150 + 30 * ((dist2station - 5250) / 2000)) * 1000);
		}else if(dist2station > 7250){
			return (long)180 * 1000;
		}
		return 0;
	}
	
	/**
	 * 减速度按 -1.3m/s^2计算
	 * 见https://zh.wikipedia.org/wiki/铁路制动#cite_note-15
	 * @param dist2station
	 * @return
	 */
	private static long calcEndSpanOfStop(double dist2station){
		dist2station = Math.abs(dist2station);
		//计算初速度 0 - v0 ^ 2 = 2 * a * s
		double v0 = Math.sqrt( 2 * dist2station * 1.3D );
		if(v0 * 3.6 > 300){
			v0 = 300 / 3.6D;
		}
		//返回时间 0 = v0 + at;
		return (long)(v0 / 1.3D * 1000);
	}

	/*
	 * 盲区
	 */
	private void exportBreaks(HSRSecTrainData data, XdrGroupHelper xdrGroupHelper)
	{
		for (XdrGroupHelper.TimePair pair : xdrGroupHelper.BreakTimes)
		{
			data.add(pair.beginTime, pair.endTime);
		}
	}

	/*
	 * 处理一个同车组的xdr
	 */
	private void work(HSRSecTrainData data, XdrGroupHelper xdrGroupHelper)
	{

		List<List<Xdr_ImsiEciTime>> xdrRecords = xdrGroupHelper.work(EciSet);
		System.out.println("eciset.size = " + EciSet.size() + "  连续点的组数=" + xdrRecords.size() +" "+ data.imsiList.size()+"个用户的xdr的个数=" + xdrGroupHelper.getXdrSize() );

		double lastDist = -1;
		for (List<Xdr_ImsiEciTime> list : xdrRecords)
		{
			lastDist = work(data, list, lastDist);
		}
		System.out.println("定位点的个数=" + data.locPointList.size());
		exportBreaks(data, xdrGroupHelper);
	}

	/**
	 * 取出所有小区,并算出平均距离
	 */
	private double work(HSRSecTrainData data, List<Xdr_ImsiEciTime> pos, double lastDist)
	{
//		long min = Long.MAX_VALUE;
//		long max = Long.MIN_VALUE;

		long minValid = Long.MAX_VALUE;
		long maxValid = Long.MIN_VALUE;

		double dist = 0;

		Set<Long> missEciSet = new HashSet<Long>();

		int count = 0;

		List<Long> eciList = new ArrayList<Long>();
		
		for (Xdr_ImsiEciTime xdrRecord : pos)
		{
			long time = xdrRecord.time;
//			if (min > time) min = time;
//			if (max < time) max = time;

//			if (eciSet.add(xdrRecord.eci))
			{
				double _dist = distanceHelper.getDistanceByEci(xdrRecord.eci);
				if (_dist > 0)
				{
					dist += _dist;
					count++;

					if (!eciList.contains(xdrRecord.eci))
					{
						eciList.add(xdrRecord.eci);
					}

					if (minValid > time) minValid = time;
					if (maxValid < time) maxValid = time;
				}else{
					missEciSet.add(xdrRecord.eci);
				}
			}

		}

		if(missEciSet.size() > 0)
			LOGHelper.GetLogger().writeLog(LogType.info, String.format("车次[%s]在距离始发站%s米附近缺失工参", data.trainkey, lastDist) + missEciSet);
//		long time = ((min + max) / 2);
		long time = 0L;

		double meanDist = -1;
		if (count > 0)
		{
			meanDist = dist / count;
			time = ((minValid + maxValid) / 2);
		}
		if (meanDist > 1000 && length - meanDist > 1000  && meanDist - lastDist > locPointCleanDist)//去掉eciList >= 2, RRU拉远情况;加上相邻位置点必须大于200米
//		if (meanDist > 1000 && length - meanDist > 1000  && meanDist > lastDist && eciList.size() >= 2)
//		if (meanDist > 0  && meanDist > lastDist)
		{
			//++
			//对于开始的位置点，应取同一位置下，时间更靠后的点，
			//对于结束的位置点，应取同一位置下，时间更靠前的点
/*			if(meanDist < pointToCleanInDistFromStartEnd && meanDist - lastDist < locPointCleanDist && data.locPointList.size() > 0){
				
				data.locPointList.remove(data.locPointList.size() -1 );
				
			}else if (length - meanDist < pointToCleanInDistFromStartEnd && meanDist - lastDist < locPointCleanDist && data.locPointList.size() > 0){
				
				return lastDist;
				
			}*/
			//++
			data.add(time, meanDist, eciList);
		}

		return Math.max(meanDist, lastDist);
	}

	/**
	 * 获取同车用户的xdr
	 */
	private void getGroupXdrRecords(IXdrGroupHelper xdrGroupHelper, ImsiGroupInfo group)
	{
		for (ImsiInfoSimple imsiInfo : group.imsiInfoList)
		{
			for (Xdr_ImsiEciTime xdrRecord : imsiInfo.xdrRecordList)
			{
				long time = xdrRecord.time;
				if (time >= group.beginTime && time <= group.endTime)
				{
					xdrGroupHelper.add(xdrRecord);
				}
			}
		}
	}

	/**
	 * 获取同车用户的xdr
	 * 20180201 修改为使用 车次时间来过滤 xdr，目的是生成更多的定位点
	 */
	private void getGroupXdrRecords(IXdrGroupHelper xdrGroupHelper, ImsiGroupInfo group, HSRSecTrainData trainInfo)
	{
		long stime = trainInfo.lstime;
		long etime = trainInfo.letime;
		if (!stationBegin.outProvince)
		{
			if (trainInfo.stopStart)
			{
//				stime -= 150 * 1000;
				stime -= 200 * 1000;
			}
			else
			{
//				stime -= 60 * 1000;
				stime -= 100 * 1000;
			}
		}

		if (!stationEnd.outProvince)
		{
			if (trainInfo.stopEnd)
			{
//				etime += 150 * 1000;
				etime += 210 * 1000;
			}
			else
			{
//				etime += 60 * 1000;
				etime += 120 * 1000;
			}
		}
		for (ImsiInfoSimple imsiInfo : group.imsiInfoList)
		{
			for (Xdr_ImsiEciTime xdrRecord : imsiInfo.xdrRecordList)
			{
				long time = xdrRecord.time;
				if (time >= stime && time <= etime)
				{
					xdrGroupHelper.add(xdrRecord);
				}
			}
		}
	}
	
	/*
	 * 根据imsi,进行同车分组
	 */
	private List<Set<String>> groupImsi(Set<String> validMap)
	{
		ImsiGroupHelper gh = new ImsiGroupHelper();

		for (String string : validMap)
		{
			String[] imsis = decodeKey(string);
			gh.add(imsis[0], imsis[1]);
		}

		return gh.Group();

	}

	/*
	 * 获取同车用户对
	 */
	private Set<String> getImsiPair(Map<Long, Map<String, ImsiEciTime>> result)
	{
		Set<String> validMap = new HashSet<String>();// 可能有效的
		Set<String> invalidMap = new HashSet<String>();// 必定无效的

		for (Map<String, ImsiEciTime> xdrRecords : result.values())
		{
			getImsiPairSpan(xdrRecords, validMap, invalidMap);
		}

		/*
		 * 去掉必定无效的
		 */
		for (String key : invalidMap)
		{
			if (validMap.contains(key))
			{
				validMap.remove(key);
			}
		}

		// for (String imsiPair : validMap)
		// {
		// String[] imsis = DecodeKey(imsiPair);
		// store.AddData(imsis[0], imsis[1], 0);
		// }
		//
		// store.Flush();
		return validMap;
	}

	/*
	 * 计算单个小区中,任意两个用户 出现在 该小区的 时间间隔
	 */
	private void getImsiPairSpan(Map<String, ImsiEciTime> imsiEciTimeMap, Set<String> validMap, Set<String> invalidMap)
	{
		ImsiEciTime[] imsiEciTimeArr = imsiEciTimeMap.values().toArray(new ImsiEciTime[0]);

		int count = imsiEciTimeArr.length;
		for (int i = 0; i < count; i++)
		{
			for (int j = i + 1; j < count; j++)
			{
				getImsiPairSpan(validMap, invalidMap, imsiEciTimeArr[i], imsiEciTimeArr[j]);
			}
		}

	}

	private void getImsiPairSpan(Set<String> validMap, Set<String> invalidMap, ImsiEciTime xdr1, ImsiEciTime xdr2)
	{
		String key = encodeKey(xdr1.imsi, xdr2.imsi);
		if (key == null || invalidMap.contains(key)) return;

		long spanMax1 = Math.abs(xdr1.timeMax - xdr2.timeMin);
		long spanMax2 = Math.abs(xdr1.timeMin - xdr2.timeMax);

		long spanMax = Math.max(spanMax1, spanMax2);

		if (spanMax >= InvalidSpan)
		{
			invalidMap.add(key);
		}
		else
		{
			for (long time1 : xdr1.timeSet)
			{
				for (long time2 : xdr2.timeSet)
				{
					long spanMin = Math.abs(time1 - time2);

					if (spanMin <= ValidSpan)
					{
						validMap.add(key);

						return;
					}
				}

			}
		}
	}
	
}
