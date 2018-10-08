package cn.mastercom.bigdata.StructData;

import cn.mastercom.bigdata.util.StringUtil;

public class DT_Event
{
	public Long imsi;

	public int cityID;
	public int fileID;
	public int projectID;
	public int SampleID;
	public int itime;
	public short wtimems;
	public byte bms;
	public int eventID;
	public int ilongitude;
	public int ilatitude;
	public int cqtposid;
	public int iLAC;
	public short wRAC;
	public long iCI;
	public int iTargetLAC;
	public short wTargetRAC;
	public int iTargetCI;
	public long ivalue1;
	public long ivalue2;
	public long ivalue3;
	public long ivalue4;
	public long ivalue5;
	public long ivalue6;
	public long ivalue7;
	public long ivalue8;
	public long ivalue9;
	public long ivalue10;
	public int LocFillType;

	// 新增
	public int testType;
	public int location;
	public long dist;
	public int radius;
	public String loctp;
	public int indoor;

	public String networktype;
	public String label;

	public int moveDirect;

	public void Clear()
	{
		imsi = StaticConfig.Long_Abnormal;
		cityID = StaticConfig.Int_Abnormal;
		fileID = StaticConfig.Int_Abnormal;
		projectID = StaticConfig.Int_Abnormal;
		SampleID = StaticConfig.Int_Abnormal;
		itime = StaticConfig.Int_Abnormal;
		wtimems = StaticConfig.Short_Abnormal;
		bms = 0;
		eventID = StaticConfig.Int_Abnormal;
		ilongitude = StaticConfig.Int_Abnormal;
		ilatitude = StaticConfig.Int_Abnormal;
		cqtposid = StaticConfig.Int_Abnormal;
		iLAC = StaticConfig.Int_Abnormal;
		wRAC = StaticConfig.Short_Abnormal;
		iCI = StaticConfig.Int_Abnormal;
		iTargetLAC = StaticConfig.Int_Abnormal;
		wTargetRAC = StaticConfig.Short_Abnormal;
		iTargetCI = StaticConfig.Int_Abnormal;
		ivalue1 = StaticConfig.Int_Abnormal;
		ivalue2 = StaticConfig.Int_Abnormal;
		ivalue3 = StaticConfig.Int_Abnormal;
		ivalue4 = StaticConfig.Int_Abnormal;
		ivalue5 = StaticConfig.Int_Abnormal;
		ivalue6 = StaticConfig.Int_Abnormal;
		ivalue7 = StaticConfig.Int_Abnormal;
		ivalue8 = StaticConfig.Int_Abnormal;
		ivalue9 = StaticConfig.Int_Abnormal;
		ivalue10 = StaticConfig.Int_Abnormal;
		LocFillType = 0;
		testType = -1;
		location = -1;
		dist = -1;
		radius = -1;
		loctp = "unknown";
		indoor = -1;
		networktype = "unknown";
		label = "unknown";
		moveDirect = -1;
	}
	
	public static DT_Event filleUpEvent(String eventString)
	{
		DT_Event event = new DT_Event();
		String[] temp = eventString.split("\t", -1);
		if (temp.length < 37)
		{
			return null;
		}
		try
		{
			int i = 0;
			event.imsi = Long.parseLong(temp[i++]);
			event.cityID = Integer.parseInt(temp[i++]);
			event.fileID = Integer.parseInt(temp[i++]);
			event.projectID = Integer.parseInt(temp[i++]);
			event.SampleID = Integer.parseInt(temp[i++]);
			event.itime = Integer.parseInt(temp[i++]);
			event.wtimems = Short.parseShort(temp[i++]);
			event.bms = Byte.parseByte(temp[i++]);
			event.eventID = Integer.parseInt(temp[i++]);
			event.ilongitude = Integer.parseInt(temp[i++]);
			event.ilatitude = Integer.parseInt(temp[i++]);
			event.cqtposid = Integer.parseInt(temp[i++]);
			event.iLAC = Integer.parseInt(temp[i++]);
			event.wRAC = Short.parseShort(temp[i++]);
			event.iCI = Integer.parseInt(temp[i++]);
			event.iTargetLAC = Integer.parseInt(temp[i++]);
			event.wTargetRAC = Short.parseShort(temp[i++]);
			event.iTargetCI = Integer.parseInt(temp[i++]);
			event.ivalue1 = Long.parseLong(temp[i++]);
			event.ivalue2 = Long.parseLong(temp[i++]);
			event.ivalue3 = Long.parseLong(temp[i++]);
			event.ivalue4 = Long.parseLong(temp[i++]);
			event.ivalue5 = Long.parseLong(temp[i++]);
			event.ivalue6 = Long.parseLong(temp[i++]);
			event.ivalue7 = Long.parseLong(temp[i++]);
			event.ivalue8 = Long.parseLong(temp[i++]);
			event.ivalue9 = Long.parseLong(temp[i++]);
			event.ivalue10 = Long.parseLong(temp[i++]);
			event.LocFillType = Integer.parseInt(temp[i++]);
			event.testType = Integer.parseInt(temp[i++]);
			event.location = Integer.parseInt(temp[i++]);
			event.dist = Long.parseLong(temp[i++]);
			event.radius = Integer.parseInt(temp[i++]);
			event.loctp = temp[i++];
			event.indoor = Integer.parseInt(temp[i++]);
			event.networktype = temp[i++];
			event.label = temp[i++];
			event.moveDirect = Integer.parseInt(temp[i++]);
		}
		catch (NumberFormatException e)
		{
			e.printStackTrace();
			return null;
		}
		return event;
	}
}
