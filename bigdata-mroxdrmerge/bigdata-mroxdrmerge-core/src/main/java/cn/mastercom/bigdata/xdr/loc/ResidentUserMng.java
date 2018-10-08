package cn.mastercom.bigdata.xdr.loc;

import java.util.HashMap;

public class ResidentUserMng
{
	private HashMap<ResidentKey, ResidentUser> ImsiCellLocMap = null;
	private long imsi = 0;

	public void cleanList()
	{
		if (ImsiCellLocMap != null)
		{
			ImsiCellLocMap.clear();
		}
	}

	public void setImsi(long imsi)
	{
		if (imsi > 0)
		{
			this.imsi = imsi;
		}
	}

	public ResidentUserMng()
	{
		ImsiCellLocMap = new HashMap<ResidentKey, ResidentUser>();
	}

	public HashMap<ResidentKey, ResidentUser> getImsiCellLocMap()
	{
		return ImsiCellLocMap;
	}

	public long getImsi()
	{
		return imsi;
	}

	public ResidentUser getItem(ResidentKey key)
	{
		return ImsiCellLocMap.get(key);
	}

	public void putItem(ResidentUser item)
	{
		ResidentKey key = new ResidentKey(item.imsi, item.hour, item.eci);
		if (!ImsiCellLocMap.containsKey(key))
		{
			ImsiCellLocMap.put(key, item);
		}
	}
}
