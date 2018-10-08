package cn.mastercom.bigdata.StructData;

public class XdrLocation
{
	public int cityID;
	public long eci;
	public long s1apid;
	public int itime;
	public long imsi;
	public int ilongtude;
	public int ilatitude;

	// xdr 新增信息
	public int testType;
	public int location;
	public long dist;
	public int radius;
	public String loctp;
	public int indoor;
	public String networktype;
	public String lable;

	// 关联信息
	public int longitudeGL;
	public int latitudeGL;
	public int testTypeGL;

	public int locationGL;
	public long distGL;
	public int radiusGL;
	public String loctpGL;
	public int indoorGL;
	public String lableGL;

	public int serviceType;
	public int subServiceType;

	public int moveDirect;

	public int loctimeGL;

	public String host;

	public String wifiName;

	// 新添加imeiTac,区分手机品牌 170517
	public int imei;
	
	//新增定位类型
	public int locSource;

	public XdrLocation()
	{
		loctp = "";
		networktype = "";
		loctpGL = "";
		lableGL = "";
		host = "";
		wifiName = "";
		imei = 0;
		locSource = -1;
	}

	public static XdrLocation FillData(String[] values, int startPos)
	{
		XdrLocation item = new XdrLocation();
		int i = startPos;
		item.cityID = Integer.parseInt(values[i++]);
		item.eci = Long.parseLong(values[i++]);
		item.s1apid = Long.parseLong(values[i++]);
		item.itime = Integer.parseInt(values[i++]);
		item.imsi = Long.parseLong(values[i++]);
		item.ilongtude = Integer.parseInt(values[i++]);
		item.ilatitude = Integer.parseInt(values[i++]);

		item.testType = Integer.parseInt(values[i++]);
		item.location = Integer.parseInt(values[i++]);
		item.dist = Long.parseLong(values[i++]);
		item.radius = Integer.parseInt(values[i++]);
		item.loctp = values[i++];
		item.indoor = Integer.parseInt(values[i++]);
		item.networktype = values[i++];
		item.lable = values[i++];

		item.longitudeGL = Integer.parseInt(values[i++]);
		item.latitudeGL = Integer.parseInt(values[i++]);
		item.testTypeGL = Integer.parseInt(values[i++]);

		item.locationGL = Integer.parseInt(values[i++]);
		item.distGL = Long.parseLong(values[i++]);
		item.radiusGL = Integer.parseInt(values[i++]);
		item.loctpGL = values[i++];
		item.indoorGL = Integer.parseInt(values[i++]);
		item.lableGL = values[i++];

		item.serviceType = Integer.parseInt(values[i++]);
		item.subServiceType = Integer.parseInt(values[i++]);

		item.moveDirect = Integer.parseInt(values[i++]);

		item.loctimeGL = Integer.parseInt(values[i++]);

		item.host = values[i++];

		item.wifiName = values[i++];

		if(i <= values.length-1)
		{
			item.imei = Integer.parseInt(values[i++]);
		}
		
		if(item.loctpGL.equals("ll")
		   || item.loctpGL.equals("lll"))
		{
			item.locSource = StaticConfig.LOCTYPE_GPS;
		}
		else if (item.loctpGL.equals("fg"))
		{
			item.locSource = StaticConfig.LOCTYPE_FG;
		}
		else 
		{
			item.locSource = StaticConfig.LOCTYPE_OTT;
		}

		return item;
	}

}
