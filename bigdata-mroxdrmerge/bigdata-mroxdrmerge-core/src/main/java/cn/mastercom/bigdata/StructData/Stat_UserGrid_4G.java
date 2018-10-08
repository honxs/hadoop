package cn.mastercom.bigdata.StructData;

import java.io.Serializable;

public class Stat_UserGrid_4G implements Serializable
{
	public int icityid;
	public int startTime;
	public int endTime;
	public int isamplenum;

	public int itllongitude;
	public int itllatitude;
	public int ibrlongitude;
	public int ibrlatitude;
	public int userCount_4G;

	public Stat_UserGrid_4G()
	{
		icityid = -1;
		startTime = 0;
		endTime = 0;
		userCount_4G = 0;
	}
	
	public static Stat_UserGrid_4G FillData(String[] values, int startPos)
	{
		int i = startPos;
		
		Stat_UserGrid_4G item = new Stat_UserGrid_4G();
		item.icityid = Integer.parseInt(values[i++]);          
		item.startTime = Integer.parseInt(values[i++]); 
		item.endTime = Integer.parseInt(values[i++]);   
		item.itllongitude = Integer.parseInt(values[i++]);
		item.itllatitude = Integer.parseInt(values[i++]);
		item.ibrlongitude = Integer.parseInt(values[i++]);
		item.ibrlatitude = Integer.parseInt(values[i++]); 
		item.isamplenum = Integer.parseInt(values[i++]); 
		item.userCount_4G = Integer.parseInt(values[i++]);  
	
		return item;
	}
	
}
