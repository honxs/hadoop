package cn.mastercom.bigdata.stat.userAna.model;

public class Xdr_ImsiEciTime {

	public long eci;
	
	public long time;
	
	public String imsi;

	@Override
	public String toString() {
		
		return imsi + "\t" + time + "\t" + eci;
	}
	
	public static Xdr_ImsiEciTime fromString(String toString){
		String[] args = toString.split("\t");
		
		Xdr_ImsiEciTime result = new Xdr_ImsiEciTime();
		result.imsi = args[0].trim();
		result.time = Long.parseLong(args[1]);
		result.eci = Long.parseLong(args[2]);
		return result;
	}
	public long getEci() {
		return eci;
	}

	public void setEci(long eci) {
		this.eci = eci;
	}
}
