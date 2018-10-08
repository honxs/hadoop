package cn.mastercom.bigdata.evt.locall.struct;

import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;

import java.io.Serializable;

public class Stat_Event_Area_Cell implements Serializable
{
    public int iCityID;
    public long iECI;
    public int iInterface;
    public int kpiSet;
    public int iTime;
    
    public int iAreatype;
    public int iAreaID;
	
	public double fvalue[] = new double[20];
	public static final String spliter = "\t";
	
	public static Stat_Event_Area_Cell fillData(String[] vals, int pos){
		Stat_Event_Area_Cell areaCell = new Stat_Event_Area_Cell();
		try{
			areaCell.iCityID = Integer.parseInt(vals[pos++]);
			areaCell.iAreatype = Integer.parseInt(vals[pos++]);
			areaCell.iAreaID = Integer.parseInt(vals[pos++]);
			areaCell.iECI = Long.parseLong(vals[pos++]);
			areaCell.iInterface = Integer.parseInt(vals[pos++]);
			areaCell.kpiSet = Integer.parseInt(vals[pos++]);
			areaCell.iTime = Integer.parseInt(vals[pos++]);
			for (int i = 0; i < areaCell.fvalue.length; i++)
			{
				areaCell.fvalue[i] = Double.parseDouble(vals[pos++]);
			}
		}catch(Exception e){
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"Stat_Event_Area_Cell.fillData error",
					"Stat_Event_Area_Cell.fillData error: " + e.getMessage(),e);
		}
		return areaCell;
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
		sb.append(iECI);
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
