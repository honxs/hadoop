package cn.mastercom.bigdata.stat.hsr.old;

import java.util.ArrayList;
import java.util.List;


public class HostMng
{
	private List<String> hostList;
	private static HostMng instance = null;

	private HostMng()
	{
		
	}
	
	public static HostMng GetInstance()
	{
		if(instance == null)
		{
			instance = new HostMng();
			instance.init();
		}
		return instance;
	}
	
	public boolean init()
	{
		hostList = new ArrayList<String>();
		hostList.add("am.xiaojukeji.com");
		hostList.add("amap.com");
		hostList.add("anjuke.com.");
		hostList.add("baymax.adp.meituan");
		hostList.add("c.waimai.meituan");
		hostList.add("chelaile.net.cn");
		hostList.add("codoon.com");
		hostList.add("db.house.qq");
		hostList.add("didialift.com");
		hostList.add("diditaxi.com.cn");
		hostList.add("diditaxi.qq.com");
		hostList.add("inews.qq.com");
		hostList.add("k.sohu.com");
		hostList.add("l.qq.com");
		hostList.add("luna.58.com");
		hostList.add("map.bai.com");
		hostList.add("map.baidu.com");
		hostList.add("map.qq.com");
		hostList.add("maps.googleapis.com");
		hostList.add("meishi.meituan.com");
		hostList.add("meituan.com");
		hostList.add("mobile.meituan.com");
		hostList.add("myzaker.com");
		hostList.add("nuomi.com");
		hostList.add("o2o.lianwifi.com");
		hostList.add("oupengcloud.net");
		hostList.add("qun.qq.com");
		hostList.add("share.baidu.com");
		hostList.add("snssdk.com");
		hostList.add("udache.com");
		hostList.add("waimai.meituan.com");
		hostList.add("xiaojukeji.com");
		hostList.add("zkshop.myzaker.com");
		
		return true;
	}
	
	
	public boolean isXdrLocation(String host)
	{
		for (String hostStr : hostList)
		{
			if(host.indexOf(hostStr) >= 0)
			{
				return true;
			}
		}
		return false;
	}
	
	
}
