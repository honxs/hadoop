package cn.mastercom.bigdata.stat.userAna.hsr;

import java.util.Map;

import cn.mastercom.bigdata.stat.userAna.model.ImsiInfoSimple;

public class RailSecImsi
{
	public static final int ONE_TO_TWO = 1;
	
	public static final int TWO_TO_ONE = 2;
	
	public int railSecID;
	// one - two
    public Map<String, ImsiInfoSimple> imsiInfoSimpleMap1;
    
    // two - one
    public Map<String, ImsiInfoSimple> imsiInfoSimpleMap2;
    
    public RailSecImsi(int railSegID, Map<String, ImsiInfoSimple> imsiInfoMap1, Map<String, ImsiInfoSimple> imsiInfoMap2)
    {
    	this.railSecID = railSegID;
    	this.imsiInfoSimpleMap1 = imsiInfoMap1;
    	this.imsiInfoSimpleMap2 = imsiInfoMap2;
    }
}
