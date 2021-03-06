package cn.mastercom.bigdata.evt.locall.struct;

import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;

import java.io.Serializable;

public class StatXdrCell implements Serializable
{
    public int iCityID;
    public int iInterface;
	public long lEci;
    public int iTime;

	public double fvalue[] = new double[20];
	public static final String spliter = "\t";
	
	public static StatXdrCell fillData(String[] vals, int pos){
		StatXdrCell area = new StatXdrCell();
		try{
			area.iCityID = Integer.parseInt(vals[pos++]);
			area.iInterface = Integer.parseInt(vals[pos++]);
			area.lEci = Long.parseLong(vals[pos++]);
			area.iTime = Integer.parseInt(vals[pos++]);
			for (int i = 0; i < area.fvalue.length; i++)
			{
				area.fvalue[i] = Double.parseDouble(vals[pos++]);
			}
		}catch(Exception e){
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"StatXdrCell.fillData error",
					"StatXdrCell.fillData error: " + e.getMessage(),e);
		}
		return area;
	}
	
	@Override
	public String toString()
	{
		StringBuffer sb = new StringBuffer();
		sb.append(iCityID);
		sb.append(spliter);
		sb.append(iInterface);
		sb.append(spliter);
		sb.append(lEci);
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
