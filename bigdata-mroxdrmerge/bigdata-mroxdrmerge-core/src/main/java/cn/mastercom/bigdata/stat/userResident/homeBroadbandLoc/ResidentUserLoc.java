package cn.mastercom.bigdata.stat.userResident.homeBroadbandLoc;

import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;

public class ResidentUserLoc {
	public long imsi;
	public long eci;
	public long duration;
	public long longitude;
	public long latitude;
	public int buildId;
	public int height;
	public String locSource;
	public int position;
	
	public ResidentUserLoc()
	{
		clean();
	}
	
	public void clean()
	{
		imsi = 0;
		eci = StaticConfig.Long_Abnormal;
		duration = 0;
		longitude = 0;
		latitude = 0;
		buildId = -1;
		height = -1;
		locSource = "";
		position = 0;
	}
	
	//使用天数据（带day字段）
	public void fillDayData(String[] vals)
	{
		try {
				int i = 0;
				i++;
				i++;
				imsi = Long.parseLong(vals[i++]);
				i++;
				i++;
				i++;
				i++;
				eci = Long.parseLong(vals[i++]);
				duration = Long.parseLong(vals[i++]);
				longitude = Long.parseLong(vals[i++]);
				latitude = Long.parseLong(vals[i++]);
				buildId = Integer.parseInt(vals[i++]);
				height = Integer.parseInt(vals[i++]);
				i++;
				i++;
				locSource = vals[i++];
				i++;
				i++;
				i++;
				position = Integer.parseInt(vals[i++]);
			} 
			catch (NumberFormatException e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error,"fillDayData error", "fillDayData error", e);
			}
	}
}
