package cn.mastercom.bigdata.mro.loc;

public class XdrLabel
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
	public int imeiTac;
	// 新添加前后5分钟小区切换信息
	public String eciSwitchList;
	public int areaId;
	public int areaType;
	public String msisdn = "";
	public int buildingid;
	public int level;
	public String imei;

	public XdrLabel()
	{
		loctp = "";
		networktype = "";
		loctpGL = "";
		lableGL = "";
		host = "";
		wifiName = "";
		
		imei = "";
	}

	/*
	 * for test
	 */
	public XdrLabel(String line)
	{
		String[] arrs = line.split("\t");
		imsi = Long.parseLong(arrs[0]);
		itime = (int) (Long.parseLong(arrs[1]) / 1000);
		eci = Long.parseLong(arrs[2]);
	}

	public long getLongTime()
	{
		return 1000L * itime;
	}

	public static XdrLabel FillData(String[] values, int startPos)
	{
		XdrLabel item = new XdrLabel();
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
		// 等各地市全部换成新版程序，判断可去掉
		if (i <= values.length - 1)
		{
			try
			{
				item.imeiTac = Integer.parseInt(values[i++]);
			}
			catch (Exception e)
			{
				item.imeiTac = 0;
			}
		}

		if (i <= values.length - 1)
		{
			item.eciSwitchList = values[i++];
		}

		if (i <= values.length - 1)
		{
			item.areaType = Integer.parseInt(values[i++]);
		}

		if (i <= values.length - 1)
		{
			item.areaId = Integer.parseInt(values[i++]);
		}

		if (i <= values.length - 1)
		{
			item.msisdn = values[i++];
		}
		if (i <= values.length - 1)
		{
			item.buildingid = Integer.parseInt(values[i++]);
		}
		if (i <= values.length - 1)
		{
			item.level = Integer.parseInt(values[i++]);
		}
		if (i <= values.length - 1)
		{
			item.imei = values[i++];
		}
		return item;
	}

	public String toString()
	{
		StringBuffer tmSb = new StringBuffer();
		String spliter = "\t";
		tmSb.append(cityID);
		tmSb.append(spliter);

		tmSb.append(eci);
		tmSb.append(spliter);

		tmSb.append(s1apid);
		tmSb.append(spliter);

		tmSb.append(itime);
		tmSb.append(spliter);

		tmSb.append(imsi);
		tmSb.append(spliter);

		tmSb.append(ilongtude);
		tmSb.append(spliter);

		tmSb.append(ilatitude);
		tmSb.append(spliter);

		tmSb.append(testType);
		tmSb.append(spliter);

		tmSb.append(location);
		tmSb.append(spliter);

		tmSb.append(dist);
		tmSb.append(spliter);

		tmSb.append(radius);
		tmSb.append(spliter);

		tmSb.append(loctp);
		tmSb.append(spliter);

		tmSb.append(indoor);
		tmSb.append(spliter);

		tmSb.append(networktype);
		tmSb.append(spliter);

		tmSb.append(lable);
		tmSb.append(spliter);

		tmSb.append(longitudeGL);
		tmSb.append(spliter);

		tmSb.append(latitudeGL);
		tmSb.append(spliter);

		tmSb.append(testTypeGL);
		tmSb.append(spliter);

		tmSb.append(locationGL);
		tmSb.append(spliter);

		tmSb.append(distGL);
		tmSb.append(spliter);

		tmSb.append(radiusGL);
		tmSb.append(spliter);

		tmSb.append(loctpGL);
		tmSb.append(spliter);

		tmSb.append(indoorGL);
		tmSb.append(spliter);

		tmSb.append(lableGL);
		tmSb.append(spliter);

		tmSb.append(serviceType);
		tmSb.append(spliter);

		tmSb.append(subServiceType);
		tmSb.append(spliter);

		tmSb.append(moveDirect);
		tmSb.append(spliter);

		tmSb.append(loctimeGL);
		tmSb.append(spliter);

		tmSb.append(host);
		tmSb.append(spliter);

		tmSb.append(wifiName);
		tmSb.append(spliter);

		tmSb.append(imeiTac);
		tmSb.append(spliter);

		tmSb.append(eciSwitchList);
		tmSb.append(spliter);

		tmSb.append(areaType);
		tmSb.append(spliter);

		tmSb.append(areaId);
		tmSb.append(spliter);

		tmSb.append(msisdn);
		tmSb.append(spliter);

		tmSb.append(buildingid);
		tmSb.append(spliter);

		tmSb.append(level);
		tmSb.append(spliter);

		tmSb.append(imei);
		return tmSb.toString();
	}

}
