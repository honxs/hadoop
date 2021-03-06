package cn.mastercom.bigdata.evt.locall.struct;

import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;

import java.io.Serializable;

public class Stat_Event_InGrid implements Serializable
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
	public double fvalue[] = new double[20];
	public static final String spliter = "\t";
	
	public static Stat_Event_InGrid fillData(String[] vals, int pos){
		Stat_Event_InGrid inGrid = new Stat_Event_InGrid();
		try{
			inGrid.iCityID = Integer.parseInt(vals[pos++]);
			inGrid.iBuildingID = Integer.parseInt(vals[pos++]);
			inGrid.iHeight = Integer.parseInt(vals[pos++]);
			inGrid.iTLlongitude = Integer.parseInt(vals[pos++]);
			inGrid.iTLlatitude = Integer.parseInt(vals[pos++]);
			inGrid.iBRlongitude = Integer.parseInt(vals[pos++]);
			inGrid.iBRlatitude = Integer.parseInt(vals[pos++]);
			inGrid.iInterface = Integer.parseInt(vals[pos++]);
			inGrid.kpiSet = Integer.parseInt(vals[pos++]);
			inGrid.iTime = Integer.parseInt(vals[pos++]);
			for (int i = 0; i < inGrid.fvalue.length; i++)
			{
				inGrid.fvalue[i] = Double.parseDouble(vals[pos++]);
			}
		}catch(Exception e){
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"Stat_Event_InGrid.fillData error",
					"Stat_Event_InGrid.fillData error: " + e.getMessage(),e);
		}
		return inGrid;
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
