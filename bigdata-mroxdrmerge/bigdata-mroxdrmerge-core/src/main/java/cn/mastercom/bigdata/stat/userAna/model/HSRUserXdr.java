package cn.mastercom.bigdata.stat.userAna.model;

public class HSRUserXdr {
	
	public int startStationid;
	public int endStatiionid;
	public long minBeginTime;
	public long lstime;
	public long beginTime;//离开（始发）站点小区的时间（车动的时间）
	public long endTime;
	public long letime;
	public long maxEndTime;

    public String imsi;
    public long eci;
    public long time;
    public double length;
    
    public long minsTime;//最早访问室分小区时间
	public long maxeTime;//最晚访问室分小区时间
    
	
}
