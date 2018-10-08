package cn.mastercom.bigdata.spark.mroxdrmerge.wfloc;

public class YdWlanInfo
{
	
	public String appName;
	public String mac;
	public String buildingid;
	public String level;
	public String longitude;
	public String latitude;
	
	public static final String spliter = "\t";
	public YdWlanInfo()
	{
		
	}
	
	public YdWlanInfo(String args[])
	{
		int i = 0;
		this.appName = args[i++];
		this.mac = args[i++];
		this.buildingid = args[i++];
		this.level = args[i++];
		this.longitude = args[i++];
		this.latitude = args[i++];
	}

	public String getAppName()
	{
		return appName;
	}

	public void setAppName(String appName)
	{
		this.appName = appName;
	}

	public String getMac()
	{
		return mac;
	}

	public void setMac(String mac)
	{
		this.mac = mac;
	}

	public String getBuildingid()
	{
		return buildingid;
	}

	public void setBuildingid(String buildingid)
	{
		this.buildingid = buildingid;
	}

	public String getLevel()
	{
		return level;
	}

	public void setLevel(String level)
	{
		this.level = level;
	}

	public String getLongitude()
	{
		return longitude;
	}

	public void setLongitude(String longitude)
	{
		this.longitude = longitude;
	}

	public String getLatitude()
	{
		return latitude;
	}

	public void setLatitude(String latitude)
	{
		this.latitude = latitude;
	}
	
	public String toString()
	{
		StringBuffer sb = new StringBuffer();
		sb.append(appName);
		sb.append(spliter);
		sb.append(mac);
		sb.append(spliter);
		sb.append(buildingid);
		sb.append(spliter);
		sb.append(level);
		sb.append(spliter);
		sb.append(longitude);
		sb.append(spliter);
		sb.append(latitude);
		
		return sb.toString();		
	}
	
}
