package cn.mastercom.bigdata.stat.userResident.homeBroadbandLoc;

import java.util.ArrayList;

import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.conf.cellconfig.LteCellInfo;
import cn.mastercom.bigdata.xdr.loc.ResidentUser;

public class BaseStation
{
	private int enbid;
	private int longitude;
	private int latitude;
	private long totalTimes;// //基站总驻留时长
	private ArrayList<ResidentUser> residentUserList;
	
	public BaseStation()
	{
		clean();
	}
	
	public void clean()
	{
		enbid = StaticConfig.Int_Abnormal;
		longitude = 0;
		latitude = 0;
		totalTimes = 0L;
		residentUserList = new ArrayList<>();
	}
	
	public int getEnbid() {
		return enbid;
	}
	
	public void setEnbid(int enbid) {
		this.enbid = enbid;
	}
	
	public int getLongitude() {
		return longitude;
	}
	
	public void setLongitude(int longitude) {
		this.longitude = longitude;
	}
	
	public int getLatitude() {
		return latitude;
	}
	
	public void setLatitude(int latitude) {
		this.latitude = latitude;
	}
	
	public long getTotalTimes() {
		return totalTimes;
	}
	
	public void setTotalTimes(long totalTimes) {
		this.totalTimes = totalTimes;
	}

	public ArrayList<ResidentUser> getResidentUserList() {
		return residentUserList;
	}

	public void setResidentUserList(ArrayList<ResidentUser> residentUserList) {
		this.residentUserList = residentUserList;
	}
	
	public void fillData(LteCellInfo cellInfo)
	{
		enbid = (int) (cellInfo.eci / 256);
		longitude = (int) cellInfo.ilongitude;
		latitude = (int) cellInfo.ilatitude;
	}
	
	public void updateTotalTimes(ResidentUser temp)
	{
		totalTimes += temp.duration;
	}
	
}
