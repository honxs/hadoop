package cn.mastercom.bigdata.stat.imsifill.deal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ImsiIPMng
{
	public static final int TimeSpan = 300;
	
	private Map<Integer, List<ImsiIPItem>> imsiIPItemListMap;
	private int tmTime = 0;

	public ImsiIPMng()
	{
		imsiIPItemListMap = new HashMap<Integer, List<ImsiIPItem>>();
		
	}
	
	public boolean addImsiIPItem(ImsiIPItem item)
	{
		tmTime = item.time/TimeSpan*TimeSpan;	
		List<ImsiIPItem> itemList = imsiIPItemListMap.get(tmTime);
		if(itemList == null)
		{
			itemList = new ArrayList<ImsiIPItem>();
			imsiIPItemListMap.put(tmTime, itemList);
		}
		itemList.add(item);
		
		
		itemList = imsiIPItemListMap.get(tmTime-TimeSpan);
		if(itemList == null)
		{
			itemList = new ArrayList<ImsiIPItem>();
			imsiIPItemListMap.put(tmTime-TimeSpan, itemList);
		}
		itemList.add(item);
		
		
		itemList = imsiIPItemListMap.get(tmTime+TimeSpan);
		if(itemList == null)
		{
			itemList = new ArrayList<ImsiIPItem>();
			imsiIPItemListMap.put(tmTime+TimeSpan, itemList);
		}
		itemList.add(item);
		
		return true;
	}
	
	public boolean finInit()
	{
		for ( List<ImsiIPItem> itemList : imsiIPItemListMap.values())
		{
	        Collections.sort(itemList, new Comparator<ImsiIPItem>() {
	            public int compare(ImsiIPItem a, ImsiIPItem b) {
	                return a.time - b.time;
	            }
	        });
		}
		
		return true;
	}
	
	public ImsiIPItem getNearestImsiIP(int itime)
	{
		tmTime = itime/TimeSpan*TimeSpan;
		List<ImsiIPItem> itemList = imsiIPItemListMap.get(tmTime);
		if(itemList == null)
		{
			return null;
		}
		
		int minTime = TimeSpan;
		int curTime = 0;
		ImsiIPItem resItem = null;
		for (ImsiIPItem item : itemList)
		{
			curTime = itime - item.time;
			if(curTime >= 0)
			{
				if(curTime <= minTime )
				{
					minTime = curTime;
					resItem = item;
				}
			}
			else 
			{
				if(-curTime <= minTime)
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



}
