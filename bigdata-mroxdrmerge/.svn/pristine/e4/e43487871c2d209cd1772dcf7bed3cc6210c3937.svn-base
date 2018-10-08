package cn.mastercom.bigdata.StructData;

import java.util.HashMap;
import java.util.Map;

import cn.mastercom.bigdata.util.data.MyInt;

public class Stat_Grid_2G
{
	public int icityid;
	public int startTime;
	public int endTime;
	public int iduration;
	public int idistance;
	public int isamplenum;

	public int itllongitude;
	public int itllatitude;
	public int ibrlongitude;
	public int ibrlatitude;
	
    public int UserCount;
    public int XdrCount;
    
    
    public Map<Long, MyInt> imsiMap;
	
	public Stat_Grid_2G()
	{

		imsiMap = new HashMap<Long, MyInt>();
		Clear();
	}
	
	public void Clear()
	{		
		UserCount = 0;
		XdrCount = 0;
		
		imsiMap.clear();
	};
	
	public static Stat_Grid_2G FillData(String[] values, int startPos)
	{
		int i = startPos;
		
		Stat_Grid_2G item = new Stat_Grid_2G();
		item.icityid = Integer.parseInt(values[i++]);          
		item.startTime = Integer.parseInt(values[i++]); 
		item.endTime = Integer.parseInt(values[i++]);   
		item.iduration = Integer.parseInt(values[i++]); 
		item.idistance = Integer.parseInt(values[i++]); 
		item.isamplenum = Integer.parseInt(values[i++]);
		item.itllongitude = Integer.parseInt(values[i++]);
		item.itllatitude = Integer.parseInt(values[i++]);
		item.ibrlongitude = Integer.parseInt(values[i++]);
		item.ibrlatitude = Integer.parseInt(values[i++]);      
		
        item.UserCount = Integer.parseInt(values[i++]); 
        item.XdrCount = Integer.parseInt(values[i++]); 
				
		return item;
	}
	
	
}
