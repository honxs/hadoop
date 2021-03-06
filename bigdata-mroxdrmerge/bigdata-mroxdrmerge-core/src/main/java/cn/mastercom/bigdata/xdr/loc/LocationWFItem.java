package cn.mastercom.bigdata.xdr.loc;

import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.*;

public class LocationWFItem
{
	public long imsi;
	public String apName;
	public int stime;
	public int stime_ms;
	public int etime;
	public int etime_ms;
	public String mac;
	public int buildid;
	public int level;
	public int longitude;
	public int latitude;

	private long tmTime;

	public LocationWFItem()
	{
		apName = "";
	}

	public boolean FillData(String[] vals, int startPos)
	{
		int i = startPos;

		try
		{
			String imsiStr = DataGeter.GetString(vals[i++], "");
			if (StringUtil.isNum(imsiStr)) {
				imsi = Long.parseLong(imsiStr);
			}
			apName = DataGeter.GetString(vals[i++], "");
			tmTime = DataGeter.GetLong(vals[i++], 0);
			stime = (int) (tmTime / 1000L);
			stime_ms = (int) (tmTime % 1000L);

			tmTime = DataGeter.GetLong(vals[i++], 0);
			etime = (int) (tmTime / 1000L);
			etime_ms = (int) (tmTime % 1000L);
			mac = DataGeter.GetString(vals[i++], "");
			buildid = DataGeter.GetInt(vals[i++], -1);
			level = DataGeter.GetInt(vals[i++], -1);
			longitude = (int) (DataGeter.GetDouble(vals[i++], 0) * 10000000);
			latitude = (int) (DataGeter.GetDouble(vals[i++], 0) * 10000000);
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"xdrloc LocationWFItem.FillData error",
					"xdrloc LocationWFItem.FillData error",	e);
			return false;
		}


		return true;
	}

}
