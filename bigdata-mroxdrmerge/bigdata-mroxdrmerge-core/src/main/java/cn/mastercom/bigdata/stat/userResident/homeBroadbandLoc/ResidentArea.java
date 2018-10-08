package cn.mastercom.bigdata.stat.userResident.homeBroadbandLoc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import cn.mastercom.bigdata.xdr.loc.ResidentUser;

public class ResidentArea {
	private long totalTimes;	//区域总驻留时长
	private float dayRatio;	//驻留区域天出现占比
	private int days;	//驻留区域天出现占比
	private ArrayList<BaseStation> baseStationList;	//该区域所驻留的基站
	
	public ResidentArea() 
	{
		clean();
	}
	
	public void clean()
	{
		totalTimes = 0;
		dayRatio = 0.0f;
		days =0;
		baseStationList = new ArrayList<>();
	}

	public long getTotalTimes() {
		return totalTimes;
	}

	public void setTotalTimes(long totalTimes) {
		this.totalTimes = totalTimes;
	}

	public float getDayRatio() {
		return dayRatio;
	}

	public void setDayRatio(float dayRatio) {
		this.dayRatio = dayRatio;
	}
	
	public int getDays() {
		return days;
	}

	public void setDays(int days) {
		this.days = days;
	}

	public ArrayList<BaseStation> getBaseStationList() {
		return baseStationList;
	}

	public void setBaseStationList(ArrayList<BaseStation> baseStationList) {
		this.baseStationList = baseStationList;
	}
	
	public void fillData(BaseStation temp)
	{
		totalTimes = temp.getTotalTimes();
		baseStationList.add(temp);
	}
	
	public void updateTotalTimes(BaseStation temp)
	{
		totalTimes += temp.getTotalTimes();
	}
	
	//计算该用户该驻留区域天出现（3个小时以上）占比
	public void dealResidentAreaDayRatio(int workDayNum)
	{
		HashMap<String, Set<Integer>> dayRatioMap = new HashMap<>();
		int dayNum = 0;
		for(BaseStation temp : baseStationList)
		{
			for (ResidentUser item : temp.getResidentUserList())
			{
				Set<Integer> hourSet = dayRatioMap.get(item.day);
				if(hourSet == null)
				{
					hourSet = new HashSet<>();
					dayRatioMap.put(item.day, hourSet);
				}
				hourSet.add(item.hour);
			}
		}
		
		for(Set<Integer> hourSet : dayRatioMap.values())
		{
			if(hourSet.size() >= 3)
			{
				dayNum++;
			}
		}
		days = dayNum;
		dayRatio = (float) (dayNum * 1.0 / workDayNum);
	}
}
