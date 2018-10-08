package cn.mastercom.bigdata.xdr.loc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LocationItemMng
{
	private Map<Integer, List<LocationItem>> locationItemMap;
	private Map<Integer, List<LocationItem>> locTimeSpanMap;
	private List<LocationItem> locationItemList;
	private final int TimeSpan = 10;
	private long imsi;

	public LocationItemMng()
	{
		locationItemMap = new HashMap<Integer, List<LocationItem>>();
		locTimeSpanMap = new HashMap<Integer, List<LocationItem>>();
		locationItemList = new ArrayList<LocationItem>();
	}

	public long getImsi()
	{
		return imsi;
	}

	public Map<Integer, List<LocationItem>> getLocationItemMap()
	{
		return locationItemMap;
	}

	public void AddItem(LocationItem item)
	{
		if (item.imsi != 0)
		{
			imsi = item.imsi;
		}

		locationItemList.add(item);
	}

	public boolean finInit()
	{
		// 对位置时间进行纠正
		int minSpan = Integer.MAX_VALUE;
		int curSpan = 0;
		int span;
		for (LocationItem item : locationItemList)
		{
			span = item.locTime - item.itime;
			if (minSpan > Math.abs(span))
			{
				curSpan = span;
				minSpan = Math.abs(span);
			}
		}

		for (LocationItem item : locationItemList)
		{
			if (item.locTime > 0)
				item.locTime -= curSpan;
			else
				item.locTime = item.itime;// 如果locTime=0，则取itime
		}

		// 将数据按时间分包
		locationItemMap = new HashMap<Integer, List<LocationItem>>();
		locTimeSpanMap = new HashMap<Integer, List<LocationItem>>();
		int tmtime;
		for (LocationItem item : locationItemList)
		{
			tmtime = item.locTime / TimeSpan * TimeSpan;

			List<LocationItem> itemList = locationItemMap.get(tmtime);
			if (itemList == null)
			{
				itemList = new ArrayList<LocationItem>();
				locationItemMap.put(tmtime, itemList);
			}
			itemList.add(item);

			itemList = locationItemMap.get(tmtime + TimeSpan);
			if (itemList == null)
			{
				itemList = new ArrayList<LocationItem>();
				locationItemMap.put(tmtime + TimeSpan, itemList);
			}
			itemList.add(item);

			itemList = locationItemMap.get(tmtime - TimeSpan);
			if (itemList == null)
			{
				itemList = new ArrayList<LocationItem>();
				locationItemMap.put(tmtime - TimeSpan, itemList);
			}
			itemList.add(item);

			// 按时间分包
			tmtime = item.locTime / 600 * 600;
			itemList = locTimeSpanMap.get(tmtime);
			if (itemList == null)
			{
				itemList = new ArrayList<LocationItem>();
				locTimeSpanMap.put(tmtime, itemList);
			}
			itemList.add(item);
		}

		// 按时间进行排序
		for (List<LocationItem> itemList : locationItemMap.values())
		{
			Collections.sort(itemList, new Comparator<LocationItem>()
			{
				public int compare(LocationItem a, LocationItem b)
				{
					return a.locTime - b.locTime;
				}
			});
		}

		return true;
	}

	private int tmtime;

	public LocationItem getLableItem(int itime)
	{
		if (locationItemMap.size() == 0)
		{
			return null;
		}

		tmtime = itime / TimeSpan * TimeSpan;
		List<LocationItem> itemList = locationItemMap.get(tmtime);
		if (itemList == null)
		{
			return null;
		}

		int minTime = TimeSpan;
		int curTime = 0;
		LocationItem resItem = null;
		for (LocationItem item : itemList)
		{
			curTime = itime - item.locTime;
			if (curTime >= 0)
			{
				if (curTime <= minTime)
				{
					minTime = curTime;
					resItem = item;
				}
			}
			else
			{
				if (-curTime <= minTime)
				{
					minTime = -curTime;
					resItem = item;
				}
				else
				{
					break;
				}
			}
		}
		return resItem;
	}

	public List<LocationItem> getLocSpan(int timeSpan)
	{
		return locTimeSpanMap.get(timeSpan);
	}

	public List<LocationItem> getLocList()
	{
		return locationItemList;
	}
}
