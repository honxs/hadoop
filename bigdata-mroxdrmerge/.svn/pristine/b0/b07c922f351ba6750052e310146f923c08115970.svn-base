package cn.mastercom.bigdata.stat.userAna.model;

import cn.mastercom.bigdata.stat.userAna.hsr.RailSec;

public class ImsiInfo{

	private RailSec railSeg;
	
	public String imsi;
	public long sTime1 = 0;
	public long eTime1 = 0;
	public long sTime2 = 0;
	public long eTime2 = 0;
	public boolean one2Two = true;
	public long span = 0;// 毫秒
	public boolean enabled = false;

	public long beginTime = 0;
	public long endTime = 0;
	public long endTimeFixed = 0;

	public double speed = 0;

	public static final double MaxSpeed = 500; //公里每小时
	public static final double MinSpeed = 150; //公里每小时
	public static final double speedRatio = 3600; // 米/毫秒 转 公里/小时

	protected long m_sEci1 = 0;
	protected long m_eEci1 = 0;
	protected long m_sEci2 = 0;
	protected long m_eEci2 = 0;
	
	public long minBeginTime = 0;
	public long maxEndTime = 0;
	

	
	/**
	 * 原始imsi
	 */
	public String imsiOrg = null;

	public ImsiInfo(String imsi,/* RailSeg railSeg,*/ StationImsi recs1, StationImsi recs2)
	{
		enabled = false;
		this.imsi = imsi;

		getTime1(recs1);
		getTime2(recs2);
		
	}

	
	public void init(){
		getSpan();
	}
	
	
	private void getTime1(StationImsi recs)
	{

		m_sEci1 = recs.minTimeEci;
		sTime1 = recs.minTime;

		m_eEci1 = recs.maxTimeEci;
		eTime1 = recs.maxTime;

	}

	private void getTime2(StationImsi recs)
	{

		m_sEci2 = recs.minTimeEci;
		sTime2 = recs.minTime;

		m_eEci2 = recs.maxTimeEci;
		eTime2 = recs.maxTime;
	}

	private void getSpan()
	{
		// ETime1<STime2，并记录方向：1->2,记录时间跨度：span= STime2- ETime1；
		if (eTime1 > 0 && eTime1 < sTime2)
		{
			one2Two = true;
			
			if (railSeg.stationID1.outProvince)
			{
				span = sTime2 - sTime1;
			}
			else if (railSeg.stationID2.outProvince)
			{
				span = eTime2 - eTime1;
			}
			else
			{
				span = sTime2 - eTime1;
			}
			
			
		}

		// ETime2 < STime1,并记录方向：2->1,记录时间跨度：span= STime1- ETime2；
		else if (eTime2 > 0 && eTime2 < sTime1)
		{
			one2Two = false;
			span = sTime1 - eTime2;
			if (railSeg.stationID1.outProvince)
			{
				span = eTime1 - eTime2;
			}
			else if (railSeg.stationID2.outProvince)
			{
				span = sTime1 - sTime2;
			}
			else
			{
				span = sTime1 - eTime2;
			}
		}
		else
		{
			return;
		}

		double dist = getDistance();
		if (dist == 0)
			return;

		speed = speedRatio * dist / span;

		enabled = (speed >= MinSpeed && speed <= MaxSpeed);

		if (enabled)
		{
			//  运行开始时间（如果是方向1->2 ,则是ETime1，否则是ETime2）
			//  运行结束时间（如果是方向1->2 ,则是STime2，否则是STime1）

			if (one2Two)
			{
				if(railSeg.stationID1.outProvince){
					beginTime = sTime1;
					endTime = sTime2;
					
					minBeginTime =  sTime1;
					maxEndTime = eTime2; 
				}else if (railSeg.stationID2.outProvince)
				{
					beginTime = eTime1;
					endTime = eTime2;
					
					minBeginTime =  sTime1;
					maxEndTime = eTime2; 
				}else{
					
					beginTime = eTime1;
					endTime = sTime2;
					
					minBeginTime =  sTime1;
					maxEndTime = eTime2; 
				}
			}
			else
			{
				if(railSeg.stationID1.outProvince){
					beginTime = eTime2;
					endTime = eTime1;
					
					minBeginTime =  sTime2;
					maxEndTime = eTime1; 
				}else if (railSeg.stationID2.outProvince)
				{
					beginTime = sTime2;
					endTime = sTime1;
					
					minBeginTime =  sTime2;
					maxEndTime = eTime1; 
				}else{
					
					beginTime = eTime2;
					endTime = sTime1;
					
					minBeginTime =  sTime2;
					maxEndTime = eTime1; 
				}
			}

		}
	}


	public void setMark(int index)
	{
		imsiOrg = imsi;
		if (index > 0)
		{
			imsi = String.valueOf(index) + "." + imsiOrg ;
		}
	}
	
	public ImsiInfo(String imsi, RailSec railSeg, StationImsi recs1, StationImsi recs2) {
		this(imsi, recs1, recs2);
		this.railSeg = railSeg;
	}

	protected double getDistance() {

		try
		{
			if (railSeg.stationID1.outProvince)
			{
				if (one2Two)
				{
					return railSeg.eci_dist_map.get(m_sEci1).distance2;
				}
				else
				{
					return railSeg.eci_dist_map.get(m_eEci1).distance2;
				}
			}
			else if (railSeg.stationID2.outProvince)
			{
				if (one2Two)
				{
					return railSeg.eci_dist_map.get(m_eEci2).distance1;
				}
				else
				{
					return railSeg.eci_dist_map.get(m_sEci2).distance1;
				}
			}
			else
			{
				return railSeg.length;
			}
		}
		catch (Exception e)
		{
			return 0;
		}
	}

}
