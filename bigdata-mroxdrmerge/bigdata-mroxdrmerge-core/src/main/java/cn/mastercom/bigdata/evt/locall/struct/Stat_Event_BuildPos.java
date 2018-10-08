package cn.mastercom.bigdata.evt.locall.struct;

import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;

import java.io.Serializable;

public class Stat_Event_BuildPos implements Serializable
{
    public int iCityID;
	protected int iBuildingID;
	public int iHeight;
	public int position;
    public int iInterface;
    public int kpiSet;
    public int iTime;
	public double fvalue[] = new double[20];
	public static final String spliter = "\t";
	
	public static Stat_Event_BuildPos fillData(String[] vals, int pos){
		Stat_Event_BuildPos buildPos = new Stat_Event_BuildPos();
		try{
			buildPos.iCityID = Integer.parseInt(vals[pos++]);
			buildPos.iBuildingID = Integer.parseInt(vals[pos++]);
			buildPos.iHeight = Integer.parseInt(vals[pos++]);
			buildPos.position = Integer.parseInt(vals[pos++]);
			buildPos.iInterface = Integer.parseInt(vals[pos++]);
			buildPos.kpiSet = Integer.parseInt(vals[pos++]);
			buildPos.iTime = Integer.parseInt(vals[pos++]);
			for (int i = 0; i < buildPos.fvalue.length; i++)
			{
				buildPos.fvalue[i] = Double.parseDouble(vals[pos++]);
			}
		}catch(Exception e){
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"Stat_Event_BuildPos.fillData error",
					"Stat_Event_BuildPos.fillData error: " + e.getMessage(),e);
		}
		return buildPos;
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
		sb.append(position);
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
