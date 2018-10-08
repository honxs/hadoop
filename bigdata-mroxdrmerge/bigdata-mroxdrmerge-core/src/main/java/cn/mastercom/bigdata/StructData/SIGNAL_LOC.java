package cn.mastercom.bigdata.StructData;

public abstract class SIGNAL_LOC implements Comparable<SIGNAL_LOC>
{
	public long Online_ID;
	public long Session_ID;

	public int cityID;
	public int stime;
	public int stime_ms;

	public long IMSI;
	public String MSISDN;
	public String IMEI;

	public int longitude;
	public int latitude;

	public int testType;
	public int location;// 0翰信回填
						// 1，2杂乱的经纬度
						// 3 滴滴司机
						// 4 SDK获取
						// 5 高德地图
						// 6 腾讯SDK
						// 10 用户常驻小区回填
	public int location2;
	public long dist;
	public int radius;
	public String loctp;// 取值范围：ll,ll2,wf,cl
						// ll,ll2:数据来源是gps
						// wf:数据来源是wifi
						// cl:数据来源是cell
						// 空字符串为unknow
						// 表示 指纹库、常驻小区回填
	public int indoor;

	public String networktype;
	public String lable;

	public int moveDirect;

	// 用于给mro关联用
	public int testTypeGL;
	public int longitudeGL;
	public int latitudeGL;

	public int locationGL;
	public long distGL;
	public int radiusGL;
	public String loctpGL;
	public int indoorGL;
	public String lableGL;

	// 自运算标签属性
	public double mt_speed;
	public String mt_label;// high, low, static, unknow

	// 记录原始经纬度时间
	public int loctimeGL;

	// 记录location的返回时间
	public int latlng_time;

	public String wifiName;

	// 记录原始字节数据
	public String valStr = "";

	public abstract String GetCellKey();

	public abstract int GetSampleDistance(int ilongitude, int ilatitude);

	public abstract int GetMaxCellRadius();
	// public abstract boolean IsCityLocation();

	public SIGNAL_LOC()
	{
		Clear();
	}

	public void Clear()
	{
		cityID = -1;

		radius = 10000;
		testType = -1;
		location = -1;
		dist = -1;
		indoor = -1;
		loctp = "unknow";
		networktype = "";
		lable = "unknow";

		testTypeGL = -1;
		longitudeGL = 0;
		latitudeGL = 0;
		locationGL = 0;
		distGL = 0;
		radiusGL = 0;
		loctpGL = "";
		indoorGL = 0;
		lableGL = "unknow";

		mt_speed = StaticConfig.Int_Abnormal;
		mt_label = "unknow";

		moveDirect = -1;
		loctimeGL = 0;

		latlng_time = 0;

	}

	@Override
	public int compareTo(SIGNAL_LOC o)
	{
		double time1 = stime + stime_ms / 1000.0;
		double time2 = o.stime + o.stime_ms / 1000.0;
		if (time1 > time2)
		{
			return 1;
		}
		else if (time1 < time2)
		{
			return -1;
		}
		else
		{
			return (int) (Session_ID - o.Session_ID);
		}
	}

	@Override
	public String toString()
	{
		return ",Online_ID=" + Online_ID + ", Session_ID=" + Session_ID + ", cityID=" + cityID + ", stime="
				+ stime + ", stime_ms=" + stime_ms + ", longitude=" + longitude + ", latitude=" + latitude
				+ ", testType=" + testType + ", location=" + location + ", dist=" + dist + ", radius=" + radius
				+ ", loctp=" + loctp + ", indoor=" + indoor + ", networktype=" + networktype + ", lable=" + lable
				+ ", moveDirect=" + moveDirect + ", testTypeGL=" + testTypeGL + ", longitudeGL=" + longitudeGL
				+ ", latitudeGL=" + latitudeGL + ", locationGL=" + locationGL + ", distGL=" + distGL + ", radiusGL="
				+ radiusGL + ", loctpGL=" + loctpGL + ", indoorGL=" + indoorGL + ", lableGL=" + lableGL + ", mt_speed="
				+ mt_speed + ", mt_label=" + mt_label + ", loctimeGL=" + loctimeGL + ", latlng_time=" + latlng_time
				+ ", wifiName=" + wifiName + ", valStr=" + valStr + "]";
	}

}
