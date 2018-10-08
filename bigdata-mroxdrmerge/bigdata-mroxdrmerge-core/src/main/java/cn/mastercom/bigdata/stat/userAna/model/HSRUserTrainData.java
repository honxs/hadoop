package cn.mastercom.bigdata.stat.userAna.model;
import java.util.ArrayList;
import java.util.List;

public class HSRUserTrainData {

	//public String imsi;
	public long trainKey;
	public long lstime;//车次开车时间
	public long letime;
    public double distance;
    public int startStationid;
    public int endStatiionid;
    public List pointList;//定位点的时间、距离
	public long getLstime() {
		return lstime;
	}
	public void setLstime(long lstime) {
		this.lstime = lstime;
	}
	public HSRUserTrainData() {
		super();
	}
	public HSRUserTrainData(long trainKey, List pointList) {
		this.pointList = pointList;
		this.trainKey = trainKey;
	}
    
}
