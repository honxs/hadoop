package cn.mastercom.bigdata.StructData;

import java.util.HashMap;
import java.util.Map;

import cn.mastercom.bigdata.util.data.MyInt;

public class Stat_CellGrid_23G
{
	public int icityid;
	public int iLac;
	public long iCi;
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
    public int UserCount_2G;
    public int UserCount_3G;
    public int XdrCount;
    
    public Map<Long, MyInt> imsiMap;
    public Map<Long, MyInt> imsiMap_2G;
    public Map<Long, MyInt> imsiMap_3G;
	
	public Stat_CellGrid_23G()
	{
		imsiMap = new HashMap<Long, MyInt>();
		imsiMap_2G = new HashMap<Long, MyInt>();
		imsiMap_3G = new HashMap<Long, MyInt>();
		Clear();
	}
	
	public void Clear()
	{
		XdrCount = 0;
		
		imsiMap.clear();
		imsiMap_2G.clear();
		imsiMap_3G.clear();
	};
	
	public static Stat_CellGrid_23G FillData(String[] values, int startPos)
	{
		int i = startPos;
		
		Stat_CellGrid_23G item = new Stat_CellGrid_23G();
		item.icityid = Integer.parseInt(values[i++]);          
		item.iLac = Integer.parseInt(values[i++]); 
		item.iCi = Long.parseLong(values[i++]); 
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
        item.UserCount_2G = Integer.parseInt(values[i++]); 
        item.UserCount_3G = Integer.parseInt(values[i++]); 
        item.XdrCount = Integer.parseInt(values[i++]); 
				
		return item;
	}
	
	
	
}
