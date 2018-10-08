package cn.mastercom.bigdata.stat.userAna.hsr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import cn.mastercom.bigdata.stat.userAna.model.HSRUserArea;
import cn.mastercom.bigdata.stat.userAna.model.ImsiInfoSimple;

import com.chinamobile.util.TimeUtil;
import com.sun.xml.internal.xsom.impl.scd.Iterators.Map;


public class HSRSecTrainData
{
	public long trainkey;
	
	public int cityID;
    public int sectionid;
    public int trainid;
    public int hourTime;

    public long lstime;
    public long letime;
    public int startStationid;
    public int endStatiionid;
    
    public double distance;
     
    public boolean oneToTwo;
    /**
     * 始站是否停车
     */
    public boolean stopStart = false;
    
    /**
     * 末站是否停车
     */
    public boolean stopEnd = false;

    public List<LocPoint> locPointList;//TB_HSR_LOCPOINT_dd_yymmdd

    public List<ImsiInfoSimple> imsiList;//TB_HSR_IMSI_dd_yymmdd
    public List<TimeSpan> noCoverTimeSpanList;//TB_HSR_IMSI_dd_yymmdd
    
    public List<TrainSeg> segList;
    
    public List<Integer> noCoverSegIdList;

    public HSRSecTrainData(int cityID, int sectionid, int trainid, long stime, long etime, boolean one2two, double distance)
    {
        locPointList = new ArrayList<HSRSecTrainData.LocPoint>();
        imsiList = new ArrayList<ImsiInfoSimple>();
        noCoverTimeSpanList = new ArrayList<TimeSpan>();
        segList = new ArrayList<>();
        noCoverSegIdList = new ArrayList<>();

        this.cityID = cityID;
        this.sectionid = sectionid;
        this.trainid = trainid;
        this.lstime = stime;
        this.letime = etime;
        this.oneToTwo = one2two;
        this.distance = distance;

        //车次唯一ID（时间域+区间ID+车次子ID）,如1711161010000100
//        this.trainkey = Long.parseLong(String.valueOf(this.hourTime).concat(String.valueOf(this.sectionid)).concat(String.valueOf(this.trainid)));
//        this.trainkey = (this.hourTime * 10000L + this.sectionid) * 100L + this.trainid;
        
        this.hourTime =  Integer.parseInt(TimeUtil.getTimeStringFromMillis(lstime, "yyMMddHH"));
        this.trainkey = ((this.hourTime * 10000L + this.sectionid) * 10L + (one2two ? 1 : 0)) * 1000L + this.trainid;
    }
    
    public HSRSecTrainData() {
		super();
	}

	public HSRSecTrainData(long trainkey, int cityID, long lstime, long letime,
			int startStationid, int endStatiionid, double distance,
			boolean stopStart, boolean stopEnd) {
		
		this.trainkey = trainkey;
		this.cityID = cityID;
		this.lstime = lstime;
		this.letime = letime;
		this.startStationid = startStationid;
		this.endStatiionid = endStatiionid;
		this.distance = distance;
		this.stopStart = stopStart;
		this.stopEnd = stopEnd;
	}

	public class TimeSpan
    {
        public long lstime;
        public long letime;
        
        public TimeSpan(long stime, long etime)
        {
            lstime = stime;
            letime = etime;
        }
    }

    public class LocPoint
    {
        public long ltime;
        public double startStationDistance;
        public List<Long> EciList;

        public LocPoint(long time, double dist)
        {
            ltime = time;
            startStationDistance = dist;
        }
        public LocPoint(long time, double dist, List<Long> eciList)
        {
            ltime = time;
            startStationDistance = dist;
        	EciList = eciList;
        }
		public long getLtime() {
			return ltime;
		}
		public void setLtime(long ltime) {
			this.ltime = ltime;
		}
        
    }

    public void add(long time, double dist, List<Long> eciList)
    {
        locPointList.add(new LocPoint(time, dist, eciList));
    }
    
    public void add(long stime, long etime)
    {
        noCoverTimeSpanList.add(new TimeSpan(stime, etime));
    }
    
 
    public class TrainSeg{
    	
    	public int segId;
    	public double stime;
    	public double _t;
    	public double v0;
    	public double acc;
    	
    	public double avgSpeed;
    	
    	public double disToStart;
    	public double disToEnd;
    	
    	
    	
		public TrainSeg(int segId, double disToStart,
				double disToEnd) {
			super();
			this.segId = segId;
			this.disToStart = disToStart;
			this.disToEnd = disToEnd;
		}



		public TrainSeg(int segId, long stime, long _t, double speed) {
			super();
			this.segId = segId;
			this.stime = stime;
			this._t = _t;
			this.avgSpeed = speed;
		}
    	
    	
    }
}
