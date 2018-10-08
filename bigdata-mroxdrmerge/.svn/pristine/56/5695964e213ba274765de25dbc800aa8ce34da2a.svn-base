package cn.mastercom.bigdata.xdr.loc;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.Stat_UserGrid_4G;

public class UserGridStat_4G
{
	private int startTime;
	private int endTime;
	private Stat_UserGrid_4G userGrid; 
	
	public UserGridStat_4G(int startTime, int endTime)
	{
	   this.startTime = startTime;
	   this.endTime = endTime;
	   userGrid  = new Stat_UserGrid_4G();
	}
	
	public Stat_UserGrid_4G getUserGrid()
	{
		return userGrid;
	}
	
	public int getStartTime()
	{
		return startTime;
	}
	
	public int getEndTime()
	{
		return endTime;
	}
	
	public void dealSample(DT_Sample_4G sample)
	{
		userGrid.userCount_4G++;
	}
	
	public void finalDeal()
	{

	}
	

}
