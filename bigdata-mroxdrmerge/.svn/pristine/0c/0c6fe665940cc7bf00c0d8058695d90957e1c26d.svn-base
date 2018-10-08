package cn.mastercom.bigdata.loc.area;

import java.util.List;

import cn.mastercom.bigdata.loc.hsr.HSRConfig;
import cn.mastercom.bigdata.loc.hsr.LocFunc;
import cn.mastercom.bigdata.loc.hsr.TB_HSR_STATION_INFO_dd_yymmdd;
import cn.mastercom.bigdata.util.StringHelper;


public class AreaPointFunc {
	
	private HSRConfig hsrConfig;
//	private LocFunc locFun;
	
	public AreaPointFunc(HSRConfig hsrConfig){
		
//		locFun = new LocFunc(hsrConfig);
//		this.hsrConfig = locFun.hsrConfig;
		this.hsrConfig = hsrConfig;
	}

	public AreaModel findAreaPosition(long imsi, int itime, String imei) {
    	if (!checkInitData()) {
            return null;
        }

    	List<HSRAreaInfo> imsiAreaList = hsrConfig.imsi_AreaMap.get(imsi);   	
    	if(imsiAreaList == null || imsiAreaList.isEmpty()){
    		return null;
    	}
    	HSRAreaInfo areaInfo = findAreaByTime(imsiAreaList, itime, imei);
    	AreaModel timeArea =null;
    	if(areaInfo != null){
    		
    		TB_HSR_STATION_INFO_dd_yymmdd stationInfo = hsrConfig.id_stationMap.get(areaInfo.stationID);
        	timeArea = findPositionByArea(areaInfo, stationInfo);
    	}   	   	   	
        return timeArea;
     }
	
	private AreaModel findPositionByArea(HSRAreaInfo areaInfo, TB_HSR_STATION_INFO_dd_yymmdd stationInfo){
		AreaModel area = new AreaModel();
		area.areaType = areaInfo.areaType;
		area.areaID = areaInfo.areaID;
		if ((areaInfo !=null) && (areaInfo.areaID == 1 ||areaInfo.areaID == 6) ){
			
				area.longitude = stationInfo.lng;
				area.latitude = stationInfo.lat;
						
		}				
		return area;
	}
	/**
	 * @param imsiAreaList imsi所有的场景
	 * @return 传入的时间所在的场景
	 */
	private HSRAreaInfo findAreaByTime(List<HSRAreaInfo> imsiAreaList, int itime, String imei){
		for(HSRAreaInfo imsiArea : imsiAreaList){
    		if(imsiArea == null || (StringHelper.isNotBlank(imsiArea.imei) 
    				&& StringHelper.isNotBlank(imei) && !imei.equals(imsiArea.imei))){
            	continue;
            }
    		//时间不在场景的时间段内
    	    if(itime > imsiArea.etime && itime < imsiArea.stime) {
    	    	continue;
            }
    	    return imsiArea;
    	}
    	return null;
	}
	/**
	 * 检查初始化
	 * @return
	 */
	 private boolean checkInitData() {
		if (hsrConfig.imsi_AreaMap == null || hsrConfig.id_stationMap == null) {
		    return false;
		}
		return true;
	 }
	
}


