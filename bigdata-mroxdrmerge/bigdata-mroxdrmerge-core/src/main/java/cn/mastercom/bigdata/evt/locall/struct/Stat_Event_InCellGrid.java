package cn.mastercom.bigdata.evt.locall.struct;

import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;

import java.io.Serializable;

public class Stat_Event_InCellGrid implements Serializable
{
    public int iCityID;
	protected int iBuildingID;
	protected int iHeight;
    public int iTLlongitude;
    public int iTLlatitude;
    public int iTime;
    public int iBRlongitude;
    public int iBRlatitude;
    public int iInterface;
    public int kpiSet;
    public long iECI;
	public double fvalue[] = new double[20];
	public static final String spliter = "\t";
	
	public static Stat_Event_InCellGrid fillData(String[] vals, int pos){
		Stat_Event_InCellGrid inCellGrid = new Stat_Event_InCellGrid();
		try{
			inCellGrid.iCityID = Integer.parseInt(vals[pos++]);
			inCellGrid.iBuildingID = Integer.parseInt(vals[pos++]);
			inCellGrid.iHeight = Integer.parseInt(vals[pos++]);
			inCellGrid.iTLlongitude = Integer.parseInt(vals[pos++]);
			inCellGrid.iTLlatitude = Integer.parseInt(vals[pos++]);
			inCellGrid.iBRlongitude = Integer.parseInt(vals[pos++]);
			inCellGrid.iBRlatitude = Integer.parseInt(vals[pos++]);
			inCellGrid.iECI = Long.parseLong(vals[pos++]);
			inCellGrid.iInterface = Integer.parseInt(vals[pos++]);
			inCellGrid.kpiSet = Integer.parseInt(vals[pos++]);
			inCellGrid.iTime = Integer.parseInt(vals[pos++]);
			for (int i = 0; i < inCellGrid.fvalue.length; i++)
			{
				inCellGrid.fvalue[i] = Double.parseDouble(vals[pos++]);
			}
		}catch(Exception e){
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"Stat_Event_InCellGrid.fillData error",
					"Stat_Event_InCellGrid.fillData error: " + e.getMessage(),e);
		}
		return inCellGrid;
	}
	
	@Override
	public String toString()
	{
		StringBuffer sb = new StringBuffer();
		sb.append(iCityID);
		sb.append(spliter);
		sb.append(iBuildingID);
		sb.append(spliter);
		sb.append(iHeight);
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
