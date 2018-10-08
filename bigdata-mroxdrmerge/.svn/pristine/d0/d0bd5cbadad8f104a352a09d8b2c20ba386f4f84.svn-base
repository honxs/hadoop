package cn.mastercom.bigdata.stat.userAna.model;

import java.util.Set;
import java.util.TreeSet;

/*
 * 用户所在的区域
 */
public class HSRUserArea {
	
	public enum HSRUserAreaType{
		
		WAIT_ROOM(1, 1, "waitRoom"), //候车厅
//		PLATFORM(1, 2, "platform"), //站台
		INTO_PLAT(1, 3, "intoPlat"), //驶入
		STOP_PLAT(1, 4, "stopPlat"),//经停
		LEAVE_PLAT(1, 5, "leavePlat"), //驶离
		OUT_STATION(1, 6, "outStation"); //出站	
		public int areaType;
		public int areaID;
		public String name;	
		
		HSRUserAreaType(int areaType, int areaID, String name){
			this.areaType = areaType;
			this.areaID = areaID;
			this.name = name;
		}
	}
	
    public String imsi;
    public HSRUserAreaType areaType;
    public long beginTime;//每个场景的时间
	public long endTime;
	public int stationId;
	//public Set<Long> eciSet;
	
	public HSRUserArea(){       
       
    }   	    
    public HSRUserArea(String imsi, long time, int stationId){       
        this.imsi = imsi;
        this.beginTime = time;
        this.endTime = time; 
        this.stationId = stationId;
    }
    public HSRUserArea(long time){       
        this.beginTime = time;
        this.endTime = time; 
    }
//    public HSRUserArea(String imsi, long time, HSRUserAreaType areaType, int flag){      
//        this.imsi = imsi;
//        if(flag == 1){
//        	this.beginTime = time;
//            this.endTime = time;
//        } else if(flag == 2){
//        	this.endTime = time;
//        }
//        this.areaType = areaType;
//    }
    public HSRUserArea(String imsi, long sTime, long eTime, HSRUserAreaType areaType, int stationId){
    	this.imsi = imsi;
        this.beginTime = sTime;
        this.endTime = eTime;
        this.areaType = areaType;
        this.stationId = stationId;
        //this.eciSet.add(eci);
    }	
    public void addEciTime(long time){
        if (beginTime > time) this.beginTime = time;
        if (endTime < time) this.endTime = time;
       
    }
    public void setTime(ImsiEciTime imsi){
        long timeMin = imsi.timeMin;
        long timeMax = imsi.timeMax;
        if (beginTime > timeMin) beginTime = timeMin;
        if (endTime < timeMax) endTime = timeMax;
    }
	

}
