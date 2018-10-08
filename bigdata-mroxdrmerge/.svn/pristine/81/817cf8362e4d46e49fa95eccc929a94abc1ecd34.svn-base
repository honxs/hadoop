package cn.mastercom.bigdata.stat.userAna.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class XdrGroupHelperRru implements IXdrGroupHelper
{
	Map<Long, List<Xdr_ImsiEciTime>> m_xdrRecordMap;

	public XdrGroupHelperRru()
	{
		m_xdrRecordMap = new HashMap<Long, List<Xdr_ImsiEciTime>>();
	}

	@Override
	public void add(Xdr_ImsiEciTime xdrRecord)
	{
		List<Xdr_ImsiEciTime> xdrs = null;
		if (m_xdrRecordMap.containsKey(xdrRecord.eci))
		{
			xdrs = m_xdrRecordMap.get(xdrRecord.eci);
		}
		else
		{
			xdrs = new ArrayList<Xdr_ImsiEciTime>();
			m_xdrRecordMap.put(xdrRecord.eci, xdrs);
		}

		xdrs.add(xdrRecord);
	}

	public Map<RruInfo, Long> work(Collection<RruInfo> rruInfoList)
	{
		Map<RruInfo, Long> result = new HashMap<RruInfo, Long>();
		for (RruInfo rruInfo : rruInfoList)
		{
			work(result, rruInfo);
		}
		return result;
	}

	private void work(Map<RruInfo, Long> result, RruInfo rruInfo)
	{
		List<Xdr_ImsiEciTime> xdrRecords1 = getXdrList(rruInfo.ecis1);
		List<Xdr_ImsiEciTime> xdrRecords2 = getXdrList(rruInfo.ecis2);
		
		if (xdrRecords1.size() > 0 && xdrRecords2.size() > 0)
		{
			long time = work(xdrRecords1, xdrRecords2);
			if (time > 0)
			{
				result.put(rruInfo, time);
			}
		}
	}

	private List<Xdr_ImsiEciTime> getXdrList(List<Long> ecis)
	{
		List<Xdr_ImsiEciTime> result = new ArrayList<Xdr_ImsiEciTime>();
		for (Long eci : ecis)
		{
			if (m_xdrRecordMap.containsKey(eci))
			{
				result.addAll(m_xdrRecordMap.get(eci));
			}
		}
		return result;
	}

	private long work(List<Xdr_ImsiEciTime> xdrRecords1, List<Xdr_ImsiEciTime> xdrRecords2)
	{
		TimePair tp1 = getBound(xdrRecords1);
		TimePair tp2 = getBound(xdrRecords2);

		return Math.max(tp1.min, tp2.min);
	}

	private TimePair getBound(List<Xdr_ImsiEciTime> xdrRecords)
	{
		TimePair tp = new TimePair();

		for (Xdr_ImsiEciTime xdr : xdrRecords)
		{
			if (tp.min > xdr.time) tp.min = xdr.time;
			if (tp.max < xdr.time) tp.max = xdr.time;
		}
		return tp;
	}

	public class TimePair
	{
		public long min = Long.MAX_VALUE;
		public long max = Long.MIN_VALUE;
	}

}
