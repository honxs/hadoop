package cn.mastercom.bigdata.stat.userAna.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/*
 * 连续取点
 */
public class XdrGroupHelper implements IXdrGroupHelper
{
	public class TimePair
	{
		public long beginTime;
		public long endTime;

		public TimePair(long beginTime, long endTime)
		{
			this.beginTime = beginTime;
			this.endTime = endTime;
		}
	}

	/*
	 * 上个时间
	 */
	private long m_lastTime;
	/*
	 * 起始时间
	 */
	private long m_firstTime;

	/*
	 * 当前组
	 */
	private List<Xdr_ImsiEciTime> m_curGroup;

	/*
	 * 总数据
	 */
	private List<Xdr_ImsiEciTime> m_xdrRecords;

	/*
	 * 是否已经排序
	 */
	private boolean m_hasSorted;

	public List<TimePair> BreakTimes;

	// 间隔大于此值则当成不连续
	private final int BreakSpan = 5000;

	// 此时间段内都有连续的点
	private final int ValidSpan = 20000;

	// 找到连续的点后休息此间隔
	@SuppressWarnings("unused")
	private final int RestSpan = 60000;

	public XdrGroupHelper()
	{
		BreakTimes = new ArrayList<TimePair>();
		m_xdrRecords = new ArrayList<Xdr_ImsiEciTime>();
		m_hasSorted = false;
		m_curGroup = null;
		m_lastTime = 0;
		m_firstTime = 0;
	}

	/*
	 * 注入数据
	 */
	public void add(Xdr_ImsiEciTime xdrRecord)
	{
		m_hasSorted = false;
		m_xdrRecords.add(xdrRecord);
	}
public int getXdrSize(){
	return m_xdrRecords.size();
}
	/*
	 * 按时间排序
	 */
	private void sort()
	{
		if (m_hasSorted) return;

		/*
		 * m_xdrRecords.sort(new Comparator<XdrLable>()
		 * {
		 * 
		 * @Override
		 * public int compare(XdrLable o1, XdrLable o2)
		 * {
		 * return Long.compare(o1.getLongTime(), o2.getLongTime());
		 * }
		 * });
		 */
		Collections.sort(m_xdrRecords, new Comparator<Xdr_ImsiEciTime>()
		{
			@Override
			public int compare(Xdr_ImsiEciTime o1, Xdr_ImsiEciTime o2)
			{
				return Long.compare(o1.time, o2.time);
			}
		});

		m_hasSorted = true;
	}

	/**
	 * 取连续20秒的点求中间时间和平均距离
	 */
	public List<List<Xdr_ImsiEciTime>> work(Set<Long> eciSet)
	{
		// 先排序
		sort();

		// 初始化
		m_lastTime = 0;
		m_firstTime = 0;
		m_curGroup = new ArrayList<Xdr_ImsiEciTime>();

		// 运算
		List<List<Xdr_ImsiEciTime>> result = new ArrayList<List<Xdr_ImsiEciTime>>();
		for (Xdr_ImsiEciTime xdrRecord : m_xdrRecords)
		{
			if (workOne(eciSet, xdrRecord))
			{
				result.add(m_curGroup);
				m_curGroup = new ArrayList<Xdr_ImsiEciTime>();
			}
		}
		return result;
	}

	private boolean workOne(Set<Long> eciSet, Xdr_ImsiEciTime xdrRecord)
	{
		if (m_lastTime == 0) // 如果是第一个
		{
			m_lastTime = xdrRecord.time;
			reset(xdrRecord);
		}
		else
		{
			long span = xdrRecord.time - m_lastTime;

			boolean isOk = (span <= BreakSpan);

			if (!isOk)
			{
				BreakTimes.add(new TimePair(m_lastTime, xdrRecord.time));
			}
			m_lastTime = xdrRecord.time;

			if (m_curGroup.size() == 0) // 如果刚处理过数据
			{
				// if (xdrRecord.Time - firstTime >= RestSpan) //
				// 则间隔大于60秒的数据才会开始处理
				{
					reset(xdrRecord);
				}
			}
			else
			{
				if (isOk) // 如果满足条件
				{
					return workValid(eciSet, xdrRecord);
				}
				else
				{
					reset(xdrRecord);
				}
			}

		}
		return false;
	}

	private boolean workValid(Set<Long> eciSet, Xdr_ImsiEciTime xdrRecord)
	{
		m_curGroup.add(xdrRecord);

		if (xdrRecord.time - m_firstTime >= ValidSpan)
		{
			//有两个或以上的eci配置
			Map<Long, Integer> map = new HashMap<Long, Integer>();
			for (Xdr_ImsiEciTime xdr : m_curGroup)
			{
				if (eciSet.contains(xdr.eci))
				{
					map.put(xdr.eci, 1);
				}
			}

			if (map.size() > 1)
			{
				m_firstTime = xdrRecord.time;
				return true;
			}
			else
			{
				return false;
			}
		}
		else
		{
			return false;
		}
	}

	/*
	 * 初始重置
	 */
	private void reset(Xdr_ImsiEciTime xdrRecord)
	{
		// 断层了,重置
		m_curGroup.clear();
		m_curGroup.add(xdrRecord);

		m_firstTime = xdrRecord.time;
	}

	//    public static void main(String[] args) throws Exception
	//    {
	//        XdrGroupHelper xrg = new XdrGroupHelper();
	//
	//        for (int i = 1; i <= 15; i += 5)
	//        {
	//            xrg.add(new XdrLocation(i * 1000));
	//        }
	//
	//        for (int i = 100; i < 400; i += 5)
	//        {
	//            xrg.add(new XdrLocation(i * 1000));
	//        }
	//
	//        for (int i = 45; i <= 80; i += 5)
	//        {
	//            xrg.add(new XdrLocation(i * 1000));
	//        }
	//
	//        for (int i = 600; i < 800; i += 5)
	//        {
	//            xrg.add(new XdrLocation(i * 1000));
	//        }
	//
	//        List<List<XdrLocation>> result = xrg.work();
	//
	//        for (List<XdrLocation> list : result)
	//        {
	//            StringBuilder sb = new StringBuilder();
	//            for (XdrLocation xdrRecord : list)
	//            {
	//                sb.append(xdrRecord.itime);
	//                sb.append(",");
	//            }
	//            System.out.println(sb.toString());
	//        }
	//
	//    }

}
