package cn.mastercom.bigdata.stat.userAna.model;

import java.util.ArrayList;
import java.util.List;

/**
 * 相邻小区的RRU切换点
 * @author heable
 *
 */
public class RruInfo
{
	public List<Long> ecis1;
	public List<Long> ecis2;

	public double distance1;
	public double distance2;
	
	
	public RruInfo( double dist1, double dist2)
	{
		ecis1 = new ArrayList<Long>();
		ecis2 = new ArrayList<Long>();
		
		distance1 = dist1;
		distance2 = dist2;
		
	}
	
	public void Add(long eci1, long eci2)
	{
		ecis1.add(eci1);
		ecis2.add(eci2);
	}
	
}
