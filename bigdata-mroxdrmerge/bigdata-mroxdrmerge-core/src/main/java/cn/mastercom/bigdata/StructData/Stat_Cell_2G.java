package cn.mastercom.bigdata.StructData;

public class Stat_Cell_2G
{
	public int icityid;
	public int startTime;
	public int endTime;
	public int iduration;
	public int idistance;
	public int isamplenum;

	public int iLAC;
	public int wRAC;
	public long iCI;
	
	public int xdrCount;
	
	public Stat_Cell_2G()
	{
		Clear();
	}
	
	public void Clear()
	{
		xdrCount = 0;
	};
	
	public static Stat_Cell_2G FillData(String[] values, int startPos)
	{		
		return null;
	}
	
	
}
