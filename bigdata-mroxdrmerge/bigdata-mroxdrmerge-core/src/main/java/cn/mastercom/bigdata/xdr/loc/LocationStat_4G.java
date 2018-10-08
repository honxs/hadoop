package cn.mastercom.bigdata.xdr.loc;

import java.util.HashMap;
import java.util.Map;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.ELocationType;
import cn.mastercom.bigdata.StructData.SIGNAL_XDR_4G;
import cn.mastercom.bigdata.StructData.Stat_Location_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.util.data.MyInt;

public class LocationStat_4G
{

	private int lac;
	private long eci;
	private int startTime;
	private int endTime;
	private Stat_Location_4G locStatItem;

	public LocationStat_4G(int cityID, int lac, long eci, int startTime, int endTime)
	{
		this.lac = lac;
		this.eci = eci;
		this.startTime = startTime;
		this.endTime = endTime;
		
		locStatItem = new Stat_Location_4G();
		locStatItem.Clear();
		
		locStatItem.icityid = cityID;
		locStatItem.startTime = startTime;
		locStatItem.endTime = endTime;
		locStatItem.iLAC = lac;
		locStatItem.wRAC = 0;
		locStatItem.iCI = eci;
	}

	public int getLac()
	{
		return lac;
	}

	public long getEci()
	{
		return eci;
	}
	
	public Stat_Location_4G getLocStatItem()
	{
		return locStatItem;
	}
	
	public void dealSample(SIGNAL_XDR_4G xdrItem)
	{
		int location = xdrItem.location;
		String loctp = xdrItem.loctp; 
		
		locStatItem.xdrCount++;
		
		if(xdrItem.longitude > 0)
		{
			locStatItem.origLocXdrCount++;
			
			if(xdrItem.location > 0)
			{
				if(location == 6)
				{
					if(xdrItem.radius <= 20)
					{
						loctp = ELocationType.Gps1.toString();
					}
					else 
					{
						loctp = "";
					}
				}
				
				if(loctp.length() > 0)
				{
					Map<String, MyInt> loctpMap = null;
					loctpMap = locStatItem.origLocTypeMap.get(location);
					if(loctpMap == null)
					{
						loctpMap = new HashMap<String, MyInt>();
						locStatItem.origLocTypeMap.put(location, loctpMap);
					}
					
					MyInt count = null;
					count = loctpMap.get(loctp);
					if(count == null)
					{
						count = new MyInt(0);
						loctpMap.put(loctp, count);
					}
					
					count.data++;	
				}
				
			}
			
		}
	
		
	}

	public void dealSample(DT_Sample_4G sample)
	{
		boolean isSampleMro = sample.flag.toUpperCase().equals("MRO");
		boolean isSampleMre = sample.flag.toUpperCase().equals("MRE");	
		
		if (isSampleMro || isSampleMre)
		{
			if(isSampleMro)
			{
				locStatItem.mroCount++;
				if(sample.IMSI > 0)
				{
					locStatItem.mroxdrCount++;
				}
			}
			else if(isSampleMre)
			{
				locStatItem.mreCount++;
				if(sample.IMSI > 0)
				{
					locStatItem.mrexdrCount++;
				}
			}		
		}
		else
		{

			if(sample.ilongitude > 0 && sample.isOriginalLoction())
			{
				locStatItem.totalLocXdrCount++;
			
			    if(sample.locType.equals("ll") || sample.locType.equals("ll2") || sample.locType.equals("wf")
			       && sample.radius <= 100 && sample.radius >= 0)
			    {
			    	locStatItem.validLocXdrCount++;
			    }
			    
			    if(sample.testType == StaticConfig.TestType_DT)
			    {
			    	locStatItem.dtXdrCount++;
			    }
			    else if(sample.testType == StaticConfig.TestType_CQT)
			    {
			    	locStatItem.cqtXdrCount++;
			    }
			    else if(sample.testType == StaticConfig.TestType_DT_EX)
			    {
			    	locStatItem.dtexXdrCount++;
			    }
			    
				Map<String, MyInt> loctpMap = null;
				loctpMap = locStatItem.locTypeMap.get(sample.location);
				if(loctpMap == null)
				{
					loctpMap = new HashMap<String, MyInt>();
					locStatItem.locTypeMap.put(sample.location, loctpMap);
				}
				
				MyInt count = null;
				count = loctpMap.get(sample.locType);
				if(count == null)
				{
					count = new MyInt(0);
					loctpMap.put(sample.locType, count);
				}
				
				count.data++;	
			}

		}
			
	}

}
