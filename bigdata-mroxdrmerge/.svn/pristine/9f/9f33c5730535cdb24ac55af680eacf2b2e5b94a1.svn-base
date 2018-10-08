package cn.mastercom.bigdata.xdr.loc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class LocationWFItemMng
{

	private List<LocationWFItem> locationWFList;
	private long imsi;

	public LocationWFItemMng()
	{
		locationWFList = new ArrayList<LocationWFItem>();
	}

	public long getImsi()
	{
		return imsi;
	}

	public List<LocationWFItem> getLocationWFList()
	{
		return locationWFList;
	}

	public void AddItem(LocationWFItem item)
	{
		if (item.imsi != 0)
		{
			imsi = item.imsi;
		}

		locationWFList.add(item);
	}

	public boolean finInit()
	{
		Collections.sort(locationWFList, new Comparator<LocationWFItem>()
		{
			public int compare(LocationWFItem a, LocationWFItem b)
			{
				return a.stime - b.stime;
			}
		});

		return true;
	}

	public LocationWFItem getLableItem(int itime)
	{
		LocationWFItem res = null;
		for (LocationWFItem item : locationWFList)
		{
			if (itime > item.stime && itime < item.etime)
			{
				res = item;
				break;
			}
			else if (itime < item.stime)
			{
				break;
			}
		}
		return res;
	}
}
