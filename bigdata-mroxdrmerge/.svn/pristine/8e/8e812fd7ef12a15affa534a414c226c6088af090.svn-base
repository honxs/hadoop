package cn.mastercom.bigdata.conf.config;

import cn.mastercom.bigdata.util.DataGeter;

public class HomeBroadbandItem
{
	public String msisdn;
	public int longitude;
	public int latitude;
	public int reliability;	//家宽定位精度
	public int level;	//家宽定位级别

	public HomeBroadbandItem()
	{
		clean();
	}

	public void clean()
	{
		msisdn = "";
		longitude = 0;
		latitude = 0;
		reliability = 0;
		level = 0;
	}

	public HomeBroadbandItem(String msisdn, int longitude, int latitude, int reliability, int level)
	{
		this.msisdn = msisdn;
		this.longitude = longitude;
		this.latitude = latitude;
		this.reliability = reliability;
		this.level = level;
	}

	public static HomeBroadbandItem fillData(String args[])
	{
		HomeBroadbandItem item = new HomeBroadbandItem();
		int i = 0;
		item.msisdn = DataGeter.GetString(args[i++]);
		i++;	//地市
		i++;	//家宽地址
		item.longitude = (int) (DataGeter.GetDouble(args[i++]) * 10000000);
		item.latitude = (int) (DataGeter.GetDouble(args[i++]) * 10000000);
		item.reliability = DataGeter.GetInt(args[i++]);
		item.level = DataGeter.GetInt(args[i++]);
		i++;	//地图解析方式
		return item;
	}

	public String toString()
	{
		return "";
	}
}
