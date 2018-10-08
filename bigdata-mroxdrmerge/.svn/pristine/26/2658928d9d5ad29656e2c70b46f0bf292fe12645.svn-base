package cn.mastercom.bigdata.stat.userAna.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/*
 * 用于计算用到达修正时间
 */
public class ImsiFixEndTimeHelper
{
	private boolean m_hasSorted;
	private List<ImsiInfo> m_imsiInfos;
	private final int beginTimeSpan = 40000;

	public ImsiFixEndTimeHelper()
	{
		m_hasSorted = false;
		m_imsiInfos = new ArrayList<ImsiInfo>();
	}

	public void add(ImsiInfo imsiInfo)
	{
		m_hasSorted = false;
		m_imsiInfos.add(imsiInfo);
	}

	private void sort()
	{
		if (m_hasSorted)
			return;

		Collections.sort(m_imsiInfos, new Comparator<ImsiInfo>()
		{
			@Override
			public int compare(ImsiInfo o1, ImsiInfo o2)
			{
				return Long.compare(o1.beginTime, o2.beginTime);
			}
		});

		m_hasSorted = true;
	}

	public void fix(boolean noNeed)
	{
		sort();

		for (int i = 0; i < m_imsiInfos.size(); i++)
		{
			fix(i, noNeed);
		}

	}

	private void fix(int index, boolean noNeed)
	{
		ImsiInfo imsiInfo = m_imsiInfos.get(index);
		imsiInfo.endTimeFixed = imsiInfo.endTime;
		if (noNeed)
			return;

		int _index = index;
		while (true)
		{
			_index--;
			if (_index < 0)
				break;
			if (!fix(imsiInfo, _index))
			{
				break;
			}
		}

		_index = index;
		while (true)
		{
			_index++;
			if (_index >= m_imsiInfos.size())
				break;
			if (!fix(imsiInfo, _index))
			{
				break;
			}
		}

	}

	private boolean fix(ImsiInfo imsiInfo, int _index)
	{
		ImsiInfo _imsiInfo = m_imsiInfos.get(_index);
		if (Math.abs(imsiInfo.beginTime - _imsiInfo.beginTime) < beginTimeSpan)
		{
			if (imsiInfo.endTimeFixed > _imsiInfo.endTime)
			{
				imsiInfo.endTimeFixed = _imsiInfo.endTime;
			}
			return true;
		}
		else
		{
			return false;
		}
	}
}
