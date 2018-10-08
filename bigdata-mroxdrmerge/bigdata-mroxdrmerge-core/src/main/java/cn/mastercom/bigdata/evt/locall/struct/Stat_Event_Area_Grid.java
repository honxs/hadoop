package cn.mastercom.bigdata.evt.locall.struct;

import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;

import java.io.Serializable;

public class Stat_Event_Area_Grid implements Serializable
{
    public int iCityID;
    public int iTLlongitude;
    public int iTLlatitude;
    public int iBRlongitude;
    public int iBRlatitude;
    public int iInterface;
    public int kpiSet;
    public int iTime;
    public int iAreatype;
    public int iAreaID;
	public double fvalue[] = new double[20];
	public static final String spliter = "\t";
	
	public static Stat_Event_Area_Grid fillData(String[] vals, int pos){
		Stat_Event_Area_Grid areaGrid = new Stat_Event_Area_Grid();
		try{
			areaGrid.iCityID = Integer.parseInt(vals[pos++]);
			areaGrid.iAreatype = Integer.parseInt(vals[pos++]);
			areaGrid.iAreaID = Integer.parseInt(vals[pos++]);
			areaGrid.iTLlongitude = Integer.parseInt(vals[pos++]);
			areaGrid.iTLlatitude = Integer.parseInt(vals[pos++]);
			areaGrid.iBRlongitude = Integer.parseInt(vals[pos++]);
			areaGrid.iBRlatitude = Integer.parseInt(vals[pos++]);
			areaGrid.iInterface = Integer.parseInt(vals[pos++]);
			areaGrid.kpiSet = Integer.parseInt(vals[pos++]);
			areaGrid.iTime = Integer.parseInt(vals[pos++]);
			for (int i = 0; i < areaGrid.fvalue.length; i++)
			{
				areaGrid.fvalue[i] = Double.parseDouble(vals[pos++]);
			}
		}catch(Exception e){
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"Stat_Event_Area_Grid.fillData error",
					"Stat_Event_Area_Grid.fillData error: " + e.getMessage(),e);
		}
		return areaGrid;
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
		sb.append(iTLlongitude);
		sb.append(spliter);
		sb.append(iTLlatitude);
		sb.append(spliter);
		sb.append(iBRlongitude);
		sb.append(spliter);
		sb.append(iBRlatitude);
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
