package cn.mastercom.bigdata.stat.userAna.model;



public class StationImsi
{
	public StationImsi(){}
	
	public StationImsi(int stationID, String imsi, long minTime, long minTimeEci, long maxTime, long maxTimeEci)
	{
		super();
		this.stationID = stationID;
		this.imsi = imsi;
		this.maxTime = maxTime;
		this.maxTimeEci = maxTimeEci;
		this.minTime = minTime;
		this.minTimeEci = minTimeEci;
	}
	
	public int stationID = -1;
	public String imsi = null;
	public long maxTime = 0;
	public long maxTimeEci = 0;
	
	public long minTime = 0;
	public long minTimeEci = 0;
	@Override
	public String toString()
	{
		// TODO Auto-generated method stub
		return new StringBuilder()
				.append(stationID).append("\t")
				.append(imsi).append("\t")
				.append(minTime).append("\t")
				.append(minTimeEci).append("\t")
				.append(maxTime).append("\t")
				.append(maxTimeEci)
				.toString();
	}
	
	public static StationImsi fromString(String str){
		String[] strArr = str.split("\t");
		
		StationImsi stationImsi = new StationImsi(Integer.parseInt(strArr[0]), strArr[1], Long.parseLong(strArr[2]), Long.parseLong(strArr[3]), Long.parseLong(strArr[4]), Long.parseLong(strArr[5]));
		return stationImsi;
	}
	
	
	public void merge(StationImsi stationImsi){
		if(this.stationID != stationImsi.stationID || !this.imsi.equals(stationImsi.imsi)){
			return;
		}
		if(stationImsi.maxTime > this.maxTime){
			this.maxTime = stationImsi.maxTime;
			this.maxTimeEci = stationImsi.maxTimeEci;
		}
		if(stationImsi.minTime < this.minTime){
			this.minTime = stationImsi.minTime;
			this.minTimeEci = stationImsi.minTimeEci;
		}
	}
	
	public StationImsi(int stationID, Xdr_ImsiEciTime xdr)
	{
		this.stationID = stationID;
		imsi = xdr.imsi;
		maxTime = xdr.time;
		minTime = maxTime;
		maxTimeEci = xdr.eci;
		minTimeEci = xdr.eci;
	}
	
	public void merge(Xdr_ImsiEciTime xdr)
	{
		long time = xdr.time;
		if (maxTime < time)
		{
			maxTime = time;
			maxTimeEci = xdr.eci;
		}
		else if (minTime > time)
		{
			minTime = time;
			minTimeEci = xdr.eci;
		}
		
	}
	
}
