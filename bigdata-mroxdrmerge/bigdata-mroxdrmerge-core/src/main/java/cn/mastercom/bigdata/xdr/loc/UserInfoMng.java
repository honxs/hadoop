package cn.mastercom.bigdata.xdr.loc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.util.StringHelper;
import cn.mastercom.bigdata.util.StringUtil;
import cn.mastercom.bigdata.util.data.MyInt;

public class UserInfoMng
{
	private Map<Long, UserInfo> userInfoMap = new HashMap<Long, UserInfo>();

	public Map<Long, UserInfo> getUserInfoMap()
	{
		return userInfoMap;
	}

	private Map<Long, UserActStat> userActStatMap = new HashMap<Long, UserActStat>();

	public Map<Long, UserActStat> getUserActStatMap()
	{
		return userActStatMap;
	}

	public UserInfoMng()
	{
	}

	public void stat(DT_Sample_4G sample)
	{
		if (sample.IMSI <= 0)
		{
			return;
		}

		// 用户统计
		UserInfo userInfo = userInfoMap.get(sample.IMSI);
		if (userInfo == null)
		{
			userInfo = new UserInfo(sample.IMSI);
			userInfoMap.put(sample.IMSI, userInfo);
		}
		userInfo.stat(sample);

		// 用户行为统计
		UserActStat userActStat = userActStatMap.get(sample.IMSI);
		if (userActStat == null)
		{
			userActStat = new UserActStat(sample.IMSI, sample.MSISDN);
			userActStatMap.put(sample.IMSI, userActStat);
		}
		userActStat.dealSample(sample);
	}

	public void finalStat()
	{
		for (Map.Entry<Long, UserInfo> userInfoEntry : userInfoMap.entrySet())
		{
			userInfoEntry.getValue().finalStat();
		}

		for (Map.Entry<Long, UserActStat> userActEntry : userActStatMap.entrySet())
		{
			userActEntry.getValue().finalStat();
		}
	}

	public class UserInfo
	{
		public long imsi;
		public int xdrCount;
		public String brand;
		public String type;
		public String msisdn;
		public String imei;
		public long IPDataUL;
		public int IPDataDurationUL;// Result_DelayFirst;
		public long IPDataDL;
		public int IPDataDurationDL;
		public String ServiceTypeList;
		public String ServiceTypeCountList;
		public String NetType;
		public String EciList;
		public String EciCount;

		public Map<String, MyInt> serviceTypeMap;
		public Map<Long, MyInt> eciMap;

		public UserInfo(long imsi)
		{
			this.imsi = imsi;

			xdrCount = 0;
			brand = "";
			type = "";
			msisdn = "";
			imei = "";
			IPDataUL = 0;
			IPDataDurationUL = 0;
			IPDataDL = 0;
			IPDataDurationDL = 0;
			ServiceTypeList = "";
			ServiceTypeCountList = "";
			NetType = "TDLTE";
			serviceTypeMap = new HashMap<String, MyInt>();
			eciMap = new HashMap<Long, MyInt>();
		}

		public void setIsCPE(boolean isCPE)
		{
			if (isCPE)
			{
				NetType = "TDLTE-CPE";
			}
		}

		private String tmStr;

		public void stat(DT_Sample_4G sample)
		{
			xdrCount++;

			if (sample.UEBrand.length() > 0)
			{
				brand = sample.UEBrand;
				type = sample.UEType;
			}

			if (sample.MSISDN.length() > 0)
			{
				msisdn = sample.MSISDN;
			}

			if (sample.IPDataUL > 0)
			{
				IPDataUL += sample.IPDataUL;
			}

			if (sample.IPDataDL > 0)
			{
				IPDataDL += sample.IPDataDL;
			}

			if (sample.serviceType > 0)
			{
				tmStr = sample.serviceType + "|" + sample.serviceSubType;
				MyInt myInt = serviceTypeMap.get(tmStr);
				if (myInt == null)
				{
					myInt = new MyInt(0);
					serviceTypeMap.put(tmStr, myInt);
				}
				myInt.data++;
			}

			if (sample.Eci > 0)
			{
				MyInt myInt = eciMap.get(sample.Eci);
				if (myInt == null)
				{
					myInt = new MyInt(0);
					eciMap.put(sample.Eci, myInt);
				}
				myInt.data++;
			}

		}

		public void finalStat()
		{
			ServiceTypeList = "";
			ServiceTypeCountList = "";

			List<Map.Entry<String, MyInt>> serviceList = new ArrayList<Map.Entry<String, MyInt>>(
					serviceTypeMap.entrySet());
			Collections.sort(serviceList, new Comparator<Map.Entry<String, MyInt>>()
			{
				public int compare(Map.Entry<String, MyInt> o1, Map.Entry<String, MyInt> o2)
				{
					return o2.getValue().data - o1.getValue().data;
				}
			});

			for (int i = 0; i < serviceList.size(); i++)
			{
				if (i >= 100)
				{
					break;
				}
				Map.Entry<String, MyInt> serviceEntry = serviceList.get(i);

				ServiceTypeList += serviceEntry.getKey() + ",";
				ServiceTypeCountList += serviceEntry.getValue().data + ",";
			}

			ServiceTypeList = StringHelper.SideTrim(ServiceTypeList, ",");
			ServiceTypeCountList = StringHelper.SideTrim(ServiceTypeCountList, ",");

			///////////////////////////////////////////////
			// eci
			EciList = "";
			EciCount = "";

			List<Map.Entry<Long, MyInt>> eciList = new ArrayList<Map.Entry<Long, MyInt>>(eciMap.entrySet());
			Collections.sort(eciList, new Comparator<Map.Entry<Long, MyInt>>()
			{
				public int compare(Map.Entry<Long, MyInt> o1, Map.Entry<Long, MyInt> o2)
				{
					return o2.getValue().data - o1.getValue().data;
				}
			});

			for (int i = 0; i < eciList.size(); i++)
			{
				if (i >= 100)
				{
					break;
				}
				Map.Entry<Long, MyInt> eciEntry = eciList.get(i);

				EciList += eciEntry.getKey() + ",";
				EciCount += eciEntry.getValue().data + ",";
			}

			EciList = StringHelper.SideTrim(EciList, ",");
			EciCount = StringHelper.SideTrim(EciCount, ",");
		}
	}
}
