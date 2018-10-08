package cn.mastercom.bigdata.evt.locall.struct;

import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;

import java.io.Serializable;

public class Stat_Event_Area implements Serializable
{
    public int iCityID;
    public int iInterface;
    public int kpiSet;
    public int iTime;
    public int iAreatype;
    public int iAreaID;
	
	public double fvalue[] = new double[20];
	public static final String spliter = "\t";
	
	public static Stat_Event_Area fillData(String[] vals, int pos){
		Stat_Event_Area area = new Stat_Event_Area();
		try{
			area.iCityID = Integer.parseInt(vals[pos++]);
			area.iAreatype = Integer.parseInt(vals[pos++]);
			area.iAreaID = Integer.parseInt(vals[pos++]);
			area.iInterface = Integer.parseInt(vals[pos++]);
			area.kpiSet = Integer.parseInt(vals[pos++]);
			area.iTime = Integer.parseInt(vals[pos++]);
			for (int i = 0; i < area.fvalue.length; i++)
			{
				area.fvalue[i] = Double.parseDouble(vals[pos++]);
			}
		}catch(Exception e){
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"Stat_Event_Area.fillData error",
					"Stat_Event_Area.fillData error: " + e.getMessage(),e);
		}
		return area;
	}
	
	@Override
	public String toString()
	{
		StringBuffer sb = new StringBuffer();
		sb.append(iCityID);
		sb.append(spliter);
		sb.append(iAreatype);
		sb.append(spliter);
		sb.append(iAreaID);
		sb.append(spliter);
		sb.append(iInterface);
		sb.append(spliter);
		sb.append(kpiSet);
		sb.append(spliter);
		sb.append(iTime);
		sb.append(spliter);
		for (int i = 0; i < fvalue.length; i++)
		{
			sb.append(fvalue[i]);
			if(i!=fvalue.length-1){
				sb.append(spliter);
			}
		}	
	
		return sb.toString();
	}

}
