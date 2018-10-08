package cn.mastercom.bigdata.evt.locall.stat;

import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.util.DataGeter;
import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.util.LOGHelper;

public class LocItem
{
	public int cityID;
	public int itime;
	public short wtimems;
	public long imsi;
	public int ilongitude;
	public int ilatitude;
	public int ibuildid;
	public int iheight;
	public int testType;
	public int doorType;
	public int radius;
	public String loctp;
	public String label;
	public int iAreaType;
	public int iAreaID;
	public int locSource;
	public int lteScRSRP;
	public int lteScSinrUL;
	public long eci;
	public int confidentType;
	// yzx add 2017.10.24
	public long s1apid;
	public String msisdn;
	public int position;
	
	public static final String spliter = "\t";
	
	
	public int dataType = -10000; 
	public LocItem adjacentOut;
	public LocItem adjacentIn;
	public LocItem adjacentRSRP;
	
	public int adjacentOutIndex = -1;
	public int adjacentInIndex = -1;
	public int adjacentOutIndex_right = -1;
	public int adjacentInIndex_right = -1;
	public int adjacentRSRPIndex = -1; 
	
	public int adjacentLeft = -1; 
	public int adjacentRight = -1; 

	public boolean isRSRP() { 
		return lteScRSRP > -150;
	}

	public boolean isOut() {
		return (doorType == StaticConfig.ACTTYPE_OUT || doorType == StaticConfig.ACTTYPE_OUT_2
				|| confidentType == StaticConfig.OH || confidentType == StaticConfig.OL
				|| confidentType == StaticConfig.OM) && ilongitude > 0;
	}

	public boolean isIn() {
		return (doorType == StaticConfig.ACTTYPE_IN || doorType == StaticConfig.ACTTYPE_IN_2
				|| confidentType == StaticConfig.IH || confidentType == StaticConfig.IL
				|| confidentType == StaticConfig.IM)&& ilongitude > 0;
	}
	
	
	

	public LocItem()
	{
		cityID = -1;
		itime = -1;
		wtimems = -1;
		imsi = -1;
		ilongitude = -1;
		ilatitude = -1;
		ibuildid = -1;
		iheight = -1;
		testType = -1;
		doorType = -1;
		radius = -1;
		loctp = "";
		label = "";
		iAreaType = -1;
		iAreaID = -1;
		locSource = -1;
		lteScRSRP = -1000000;
		lteScSinrUL = -1000000;
		eci = 0;
		confidentType = 0;
		msisdn = "";
		s1apid = 0;
		position = 0;
	}

	public void fillData(String[] vals) throws Exception
	{
		int i = 0;
		cityID = Integer.parseInt(vals[i++]);
		itime = Integer.parseInt(vals[i++]);
		wtimems = Short.parseShort(vals[i++]);
		imsi = Long.parseLong(vals[i++]);
		ilongitude = Integer.parseInt(vals[i++]);
		ilatitude = Integer.parseInt(vals[i++]);
		ibuildid = Integer.parseInt(vals[i++]);
		iheight = Integer.parseInt(vals[i++]);
		testType = Integer.parseInt(vals[i++]);

		doorType = DataGeter.GetInt(vals[i++], -1);

		radius = Integer.parseInt(vals[i++]);
		loctp = vals[i++];
		label = vals[i++];
		if (i < vals.length)
		{
			iAreaType = Integer.parseInt(vals[i++]);
		}
		if (i < vals.length)
		{
			iAreaID = Integer.parseInt(vals[i++]);
		}

		if (i < vals.length)
		{
			locSource = Integer.parseInt(vals[i++]);
		}
		if (i < vals.length)
		{
			lteScRSRP = DataGeter.GetInt(vals[i++], -1000000);
		}
		if (i < vals.length)
		{
			lteScSinrUL = DataGeter.GetInt(vals[i++], -1000000);
		}
		if (i < vals.length)
		{
			eci = DataGeter.GetLong(vals[i++], 0);
		}
		if (i < vals.length)
		{
			confidentType = DataGeter.GetInt(vals[i++], 0);
		}
		if (i < vals.length)
		{
			msisdn = vals[i++];
		}
		if (i < vals.length)
		{
			s1apid = DataGeter.GetLong(vals[i++], 0);
		}
		if (i < vals.length)
		{
			try{
				position = Integer.parseInt(vals[i++]);
			}catch(Exception e){
				LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"LocItem Parsing error",
						"LocItem Parsing error: " + e.getMessage(),e);
			}
			
		}
	}

	public static LocItem fillLocLib(String[] vals)
	{
		LocItem loc = new LocItem();
		try
		{
			loc.fillData(vals);
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error,"fillLocLib  error", "fillLocLib  error", e);
		}
		return loc;
	}
	
	//yzx add 2018/06/22
	public void fillData2(LocItem mroLocLibItem)
	{
		cityID = mroLocLibItem.cityID;
		itime = mroLocLibItem.itime;
		wtimems = mroLocLibItem.wtimems;
		imsi = mroLocLibItem.imsi;
		ilongitude = mroLocLibItem.ilongitude;
		ilatitude = mroLocLibItem.ilatitude;
		ibuildid = mroLocLibItem.ibuildid;
		iheight = mroLocLibItem.iheight;
		testType = mroLocLibItem.testType;
		doorType = mroLocLibItem.doorType;
		radius = mroLocLibItem.radius;
		loctp = mroLocLibItem.loctp;
		label = mroLocLibItem.label;
		iAreaType = mroLocLibItem.iAreaType;
		iAreaID = mroLocLibItem.iAreaID;
		locSource = mroLocLibItem.locSource;
		lteScRSRP = mroLocLibItem.lteScRSRP;
		lteScSinrUL = mroLocLibItem.lteScSinrUL;
		eci = mroLocLibItem.eci;
		confidentType = mroLocLibItem.confidentType;
		msisdn = mroLocLibItem.msisdn;
		s1apid = mroLocLibItem.s1apid;
		position = mroLocLibItem.position;
	}

	@Override
	public String toString()
	{
		StringBuffer sb = new StringBuffer();
		sb.delete(0, sb.length());
		sb.append(cityID);
		sb.append(spliter);
		sb.append(itime);
		sb.append(spliter);
		sb.append(wtimems);
		sb.append(spliter);
		sb.append(imsi);
		sb.append(spliter);
		sb.append(ilongitude);
		sb.append(spliter);
		sb.append(ilatitude);
		sb.append(spliter);
		sb.append(ibuildid);
		sb.append(spliter);
		sb.append(iheight);
		sb.append(spliter);
		sb.append(testType);
		sb.append(spliter);
		sb.append(doorType);
		sb.append(spliter);
		sb.append(radius);
		sb.append(spliter);
		sb.append(loctp);
		sb.append(spliter);
		sb.append(label);
		sb.append(spliter);
		sb.append(iAreaType);
		sb.append(spliter);
		sb.append(iAreaID);
		sb.append(spliter);
		sb.append(locSource);
		sb.append(spliter);
		sb.append(lteScRSRP);
		sb.append(spliter);
		sb.append(lteScSinrUL);
		sb.append(spliter);
		sb.append(eci);
		sb.append(spliter);
		sb.append(confidentType);
		sb.append(spliter);
		sb.append(msisdn);
		sb.append(spliter);
		sb.append(s1apid);
		sb.append(spliter);
		sb.append(position);

		return sb.toString();
	}

}
