package cn.mastercom.bigdata.stat.userAna.model;

import java.util.ArrayList;
import java.util.List;

public class ImsiInfoSimple
{
	public String imsi;
	public long beginTime = 0;
	public long endTime = 0;
	public long endTimeFixed = 0;
	public double speed = 0;
	/**
	 * 原始imsi
	 */
	public String imsiOrg;
	
	public long minBeginTime = 0;
	public long maxEndTime = 0;
	
	public List<Xdr_ImsiEciTime> xdrRecordList;
	
//	public ImsiInfoSimple(String imsi, long beginTime, long endTime, long endTimeFixed, double speed)
//	{
//		this(imsi, beginTime, endTime, endTimeFixed, speed, imsi);
//	}
//	
//	public ImsiInfoSimple(String imsi, long beginTime, long endTime, long endTimeFixed, double speed, String imsiOrg){
//		this(imsi, beginTime, endTime, endTimeFixed, speed, imsiOrg, beginTime, endTime);
//	}
	
	public ImsiInfoSimple(String imsi, long beginTime, long endTime, long endTimeFixed, double speed, String imsiOrg, long minBeginTime, long maxEndTime)
	{
		this.imsi = imsi;
		this.beginTime = beginTime;
		this.endTime = endTime;
		this.endTimeFixed = endTimeFixed;
		this.speed = speed;
		this.imsiOrg = imsiOrg;
		this.minBeginTime = minBeginTime;
		this.maxEndTime = maxEndTime;
		
		xdrRecordList = new ArrayList<Xdr_ImsiEciTime>();
	}

	public ImsiInfoSimple(ImsiInfo imsiInfo)
	{
		xdrRecordList = new ArrayList<Xdr_ImsiEciTime>();
		
		imsi = imsiInfo.imsi;
		beginTime = imsiInfo.beginTime;
		endTime = imsiInfo.endTime;
		endTimeFixed = imsiInfo.endTimeFixed;
		speed = imsiInfo.speed;
		imsiOrg = imsiInfo.imsiOrg;
		
		minBeginTime = imsiInfo.minBeginTime;
		maxEndTime = imsiInfo.maxEndTime;
	}
	
	/**
	 * 用户在线路上的xdr
	 */
	public void add(Xdr_ImsiEciTime xdrRecord)
	{
		xdrRecordList.add(xdrRecord);
	}

	@Override
	public String toString()
	{
		// TODO Auto-generated method stub
		return new StringBuilder()
			.append(imsi).append("\t")
			.append(beginTime).append("\t")
			.append(endTime).append("\t")
			.append(endTimeFixed).append("\t")
			.append(speed).append("\t")
			.append(imsiOrg).append("\t")
			.append(minBeginTime).append("\t")
			.append(maxEndTime)
			.toString();
	}
	
	public static ImsiInfoSimple fromString(String str){
		String[] strArr = str.split("\t");
		return fromString(strArr);
	}
	
	public static ImsiInfoSimple fromString(String[] strArr){
		return new ImsiInfoSimple(strArr[0],Long.parseLong(strArr[1]),Long.parseLong(strArr[2]),Long.parseLong(strArr[3]),Double.parseDouble(strArr[4]), strArr[5], Long.parseLong(strArr[6]), Long.parseLong(strArr[7]));
		
	}
}
