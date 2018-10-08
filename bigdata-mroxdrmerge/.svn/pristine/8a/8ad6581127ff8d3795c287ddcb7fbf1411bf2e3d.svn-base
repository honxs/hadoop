package cn.mastercom.bigdata.stat.userAna.hsr;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.TreeMap;

import cn.mastercom.bigdata.stat.userAna.hsr.HSRSecTrainData.TimeSpan;
import cn.mastercom.bigdata.stat.userAna.hsr.HSRSecTrainData.TrainSeg;
import cn.mastercom.bigdata.stat.userAna.model.StationImsi;

public class Worker
{
	private HSRConfigHelper m_configHelper = null;

	public List<RailSecImsi> DoWork(List<StationImsi> stationImsiList)
	{
		m_configHelper = HSRConfigHelper.getInstance();
		m_configHelper.clear();

		add2Station(stationImsiList);

		List<RailSecImsi> railSecImsiList = new ArrayList<RailSecImsi>();
		for (RailSec railSec : m_configHelper.id_railSec_map.values())
		{
			RailSecImsi railSecImsi = railSec.work();
			if (railSecImsi != null)
			{
				railSecImsiList.add(railSecImsi);
			}
		}

		return railSecImsiList;
	}

	private void add2Station(List<StationImsi> stationImsiList)
	{
		for (StationImsi stationImsi : stationImsiList)
		{
			if (m_configHelper.id_station_map.containsKey(stationImsi.stationID))
			{
				RailStation station = m_configHelper.id_station_map.get(stationImsi.stationID);
				station.add(stationImsi);
			}
		}
	}

	public void DoWork(RailSecImsi railSegImsi, HSRData hsrData)
	{
		m_configHelper = HSRConfigHelper.getInstance();
		doWork(railSegImsi, hsrData);
	}

	private void doWork(RailSecImsi railSecImsi, HSRData hsrData)
	{
		if (m_configHelper.id_railSec_map.containsKey(railSecImsi.railSecID))
		{
			int secDataSize = hsrData.secTrainDataList.size();
			int sectionId = railSecImsi.railSecID;
			
			System.out.println(new Date() + "--------开始RailSec计算---------");
			RailSec railSec = m_configHelper.id_railSec_map.get(sectionId);
			railSec.work(railSecImsi, hsrData, m_configHelper.eci_station_map);
			System.out.println(new Date() + "--------结束RailSec计算---------");

			if (hsrData.secTrainDataList.isEmpty()) return;
			List<HSRSecTrainData> secTrainData = hsrData.secTrainDataList.subList(secDataSize > 0 ? secDataSize - 1 : 0,
					hsrData.secTrainDataList.size());

			System.out.println(new Date() + "--------开始add路段---------");
			// add 路段信息
			List<RailSeg> railSegList = m_configHelper.secId_seg_map.get(sectionId);
			if (railSegList != null)
			{
				if (!railSegList.isEmpty() && !secTrainData.isEmpty())
				{
					railSec.fillSegInfo(secTrainData, railSegList);
				}
			}
			System.out.println(new Date() + "--------结束add路段---------");
			System.out.println(new Date() + "--------开始add无覆盖---------");
			// add 无覆盖
			for (HSRSecTrainData hsrSecTrainData : secTrainData)
			{
				if (hsrSecTrainData.segList.isEmpty() || hsrSecTrainData.noCoverTimeSpanList.isEmpty())
				{
					continue;
				}
				addNoCoverSeg(hsrSecTrainData);
			}
			System.out.println(new Date() + "--------结束add无覆盖---------");
		}
	}

	// 思路：路段时间段如果跟无覆盖时间段有交集，则认为是无覆盖路段
	private void addNoCoverSeg(HSRSecTrainData hsrSecTrainData)
	{
		// 路段开始时间-路段id
		TreeMap<Double, Integer> stimeToSegId = new TreeMap<>();
		// 路段结束时间-路段id
		TreeMap<Double, Integer> etimeToSegId = new TreeMap<>();

		class NoCoverSegIdPair
		{
			final int startSegId;
			final int endSegId;

			public NoCoverSegIdPair(int startSegId, int endSegId)
			{
				this.startSegId = startSegId;
				this.endSegId = endSegId;
			}
		}

		// 按时间排序
		Collections.sort(hsrSecTrainData.segList, new Comparator<TrainSeg>()
		{

			@Override
			public int compare(TrainSeg o1, TrainSeg o2)
			{
				return (int) (o1.stime - o2.stime);
			}

		});
		for (TrainSeg trainSeg : hsrSecTrainData.segList)
		{
			stimeToSegId.put(trainSeg.stime, trainSeg.segId);
			etimeToSegId.put(trainSeg.stime + trainSeg._t, trainSeg.segId);
		}

		@SuppressWarnings("serial")
		Queue<NoCoverSegIdPair> noCoverSegList = new LinkedList<NoCoverSegIdPair>()
		{
			@Override
			public boolean add(NoCoverSegIdPair newOne)
			{
				NoCoverSegIdPair segIdPair = null;
				if (!isEmpty() && (segIdPair = this.getLast()).endSegId == newOne.startSegId)
				{
					// 合并为一个
					remove(segIdPair);
					add(new NoCoverSegIdPair(segIdPair.startSegId, newOne.endSegId));
					return true;
				}
				else return super.add(newOne);
			}
		};
		for (TimeSpan timespan : hsrSecTrainData.noCoverTimeSpanList)
		{
			// 一定能找到对应的路段
			try
			{
				int startSegId = etimeToSegId.ceilingEntry(timespan.lstime / 1000D).getValue();
				int endSegId = stimeToSegId.floorEntry(timespan.letime / 1000D).getValue();
				noCoverSegList.add(new NoCoverSegIdPair(startSegId, endSegId));
			}
			catch (Exception e)
			{
				/* 出现了奇怪的数据：无覆盖时间不在车次时间内 */
			}
		}
		Queue<TrainSeg> segQueue = new LinkedList<>(hsrSecTrainData.segList);
		TrainSeg seg = null;
		for (NoCoverSegIdPair segIdPair : noCoverSegList)
		{

			// 找到第一个无覆盖路段
			while (!segQueue.isEmpty() && (seg = segQueue.poll()).segId != segIdPair.startSegId);

			if (segQueue.isEmpty()) return;

			hsrSecTrainData.noCoverSegIdList.add(seg.segId);
			while (!segQueue.isEmpty() && (seg = segQueue.poll()).segId != segIdPair.endSegId)
			{
				hsrSecTrainData.noCoverSegIdList.add(seg.segId);
			}

			if (segQueue.isEmpty()) return;
			hsrSecTrainData.noCoverSegIdList.add(seg.segId);
		}
	}

}
