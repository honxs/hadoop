package cn.mastercom.bigdata.evt.locall.struct;

import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;

import java.io.Serializable;

public class Stat_Event_Area_CellGrid implements Serializable
{
    public int iCityID;
    public int iTLlongitude;
    public int iTLlatitude;
    public int iBRlongitude;
    public int iBRlatitude;
    public long iECI;
    public int iInterface;
    public int kpiSet;
    public int iTime;
    public int iAreatype;
    public int iAreaID;
	public double fvalue[] = new double[20];
	public static final String spliter = "\t";
    
	public static Stat_Event_Area_CellGrid fillData(String[] vals, int pos){
		Stat_Event_Area_CellGrid areaCellGrid = new Stat_Event_Area_CellGrid();
		try{
			areaCellGrid.iCityID = Integer.parseInt(vals[pos++]);
			areaCellGrid.iAreatype = Integer.parseInt(vals[pos++]);
			areaCellGrid.iAreaID = Integer.parseInt(vals[pos++]);
			areaCellGrid.iTLlongitude = Integer.parseInt(vals[pos++]);
			areaCellGrid.iTLlatitude = Integer.parseInt(vals[pos++]);
			areaCellGrid.iBRlongitude = Integer.parseInt(vals[pos++]);
			areaCellGrid.iBRlatitude = Integer.parseInt(vals[pos++]);
			areaCellGrid.iECI = Long.parseLong(vals[pos++]);
			areaCellGrid.iInterface = Integer.parseInt(vals[pos++]);
			areaCellGrid.kpiSet = Integer.parseInt(vals[pos++]);
			areaCellGrid.iTime = Integer.parseInt(vals[pos++]);
			for (int i = 0; i < areaCellGrid.fvalue.length; i++)
			{
				areaCellGrid.fvalue[i] = Double.parseDouble(vals[pos++]);
			}
		}catch(Exception e){
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"Stat_Event_Area_CellGrid.fillData error",
					"Stat_Event_Area_CellGrid.fillData error: " + e.getMessage(),e);
		}
		return areaCellGrid;
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
