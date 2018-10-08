package cn.mastercom.bigdata.xdr.loc;

public class TimeSpan
{
	public int stime;
	public int etime;
	
	public int getDuration()
	{
		return etime - stime;
	}
}
