package cn.mastercom.bigdata.loc.hsr;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import cn.mastercom.bigdata.util.StringHelper;
import cn.mastercom.bigdata.util.data.Tuple2;

public class LocFunc
{

	private HSRConfig hsrConfig;
	
	public LocFunc(HSRConfig hsrConfig){
		this.hsrConfig = hsrConfig;
		if(this.hsrConfig == null){
			throw new IllegalArgumentException();
		}
	}
	
	/**
	 * 
	 * @param imsi
	 * @param ltime
	 * @return
	 */
    public PositionModel FindPosition(long imsi, int itime)
    {
    	return FindPosition(imsi, itime, -1L);
    }
    
    public PositionModel FindPosition(long imsi, int itime, long eci)
    {
    	return FindPosition(imsi, itime, eci, null);
    }
    
    public PositionModel FindPosition(long imsi, int itime, long eci, String imei)
    {
    	if (!checkInitData())
        {
            return null;
        }
    	List<TB_HSR_IMSI_dd_yymmdd> imsiDataList = hsrConfig.dicImsiData.get(imsi);
    	if(imsiDataList == null || imsiDataList.isEmpty()){
    		return null;
    	}
        TB_HSR_TRAIN_INFO_dd_yymmdd trainInfo = findTrainByTime(imsiDataList, itime, imei);//车次区间信息
        if(trainInfo == null)
	    {
        	return null;
	    }
        if(!hsrConfig.sectionEci.containsKey(trainInfo.sectionid) || !hsrConfig.stationEci.containsKey(trainInfo.endStatiionid) || !hsrConfig.stationEci.containsKey(trainInfo.startStationid)){
          	 return null;
        }
      /* 20180518 修改 发现很多地方的RRU 并不加入工参表中
        boolean validEci = !hsrConfig.sectionEci.get(trainInfo.sectionid).contains(eci)
        && !hsrConfig.stationEci.get(trainInfo.endStatiionid).contains(eci)
        && !hsrConfig.stationEci.get(trainInfo.startStationid).contains(eci);
        
        if(eci > 0 && validEci){
        	return null;
        }*/
        
        double distance = -1;
        try{        	
        	distance = getDistanceFromStartStation(trainInfo, itime);
        }catch(Exception e){
        	return null;
        }
        PositionModel result = doFindPositionBySectionIDAndDist(trainInfo, distance);
        return result;
    }
    
    private TB_HSR_TRAIN_INFO_dd_yymmdd findTrainByTime(List<TB_HSR_IMSI_dd_yymmdd> imsiDataList, int itime, String imei){
    	for(TB_HSR_IMSI_dd_yymmdd imsiData : imsiDataList){
    		if(imsiData == null || (StringHelper.isNotBlank(imsiData.imei) && StringHelper.isNotBlank(imei) && !imei.equals(imsiData.imei))){
            	continue;
            }
    		TB_HSR_TRAIN_INFO_dd_yymmdd trainInfo = hsrConfig.dicTrainInfoData.get(imsiData.trainKey);//车次区间信息
    	    if(trainInfo == null)
    	    {
    	    	continue;
    	    }
    	    if(!(itime >= trainInfo.istime && itime <= trainInfo.ietime))//时间不在车次区间的时间段内
            {
    	    	continue;
            }
    	    return trainInfo;
    	}
    	return null;
    	
    }
    /**
     * 根据时间获取距离该车次始发站的距离
     * @param trainInfo
     * @param ltime
     * @return
     */
    private double getDistanceFromStartStation(TB_HSR_TRAIN_INFO_dd_yymmdd trainInfo, int itime){
    	TreeMap<Integer, Double> sortedLocMap = hsrConfig.dicLocpointData.get(trainInfo.trainKey);
    	
    	if(sortedLocMap == null){//该车次区间没有定位点 
    		throw new RuntimeException();
    	}
    	
    	//如果这个xdr等于定位点
    	if(sortedLocMap.containsKey(itime)){
    		return sortedLocMap.get(itime);
    	}
    	
    	//下一定位点的 <时间，与始发站距离>
    	Map.Entry<Integer, Double> nextLoc = sortedLocMap.ceilingEntry(itime);
    	//上一定位点的 <时间，与始发站距离>
    	Map.Entry<Integer, Double> lastLoc = sortedLocMap.floorEntry(itime);
    	
    	//没有上一个，是第一个定位点与起始站之间
    	if(lastLoc == null){
    		double v = (0.0D + nextLoc.getValue()) / (nextLoc.getKey() - trainInfo.istime);
    		return v * (itime - trainInfo.istime);
    	}
    	
    	//没有下一个，是最后一个定位点与终点站之间
    	if(nextLoc == null){
    		TB_SectionModel sectionData = hsrConfig.dicSectionData.get(trainInfo.sectionid);
    		double v = (sectionData.sectionLength - lastLoc.getValue()) / (trainInfo.ietime - lastLoc.getKey());
    		return sectionData.sectionLength - (v * (trainInfo.ietime - itime));
    	}
    	
    	
    	double v = (nextLoc.getValue() - lastLoc.getValue())/(nextLoc.getKey() - lastLoc.getKey());
    	
    	//当前位置与上一定位点的距离 = 平均速度 * 当前时间与上一定位点时间的 差
    	double distFromLastLoc = v * (itime - lastLoc.getKey());
    	
    	return lastLoc.getValue() + distFromLastLoc;
    }
    
    /**
     * 根据区间ID及距离，查找位置信息等数据
     * @param trainInfo 车次区间信息
     * @param distance 当前IMSI所在位置到区间 起始点的距离
     * @return
     */
    private PositionModel doFindPositionBySectionIDAndDist(TB_HSR_TRAIN_INFO_dd_yymmdd trainInfo, double distance)
    {
        TB_SectionModel sectionData = hsrConfig.dicSectionData.get(trainInfo.sectionid);
        if (sectionData == null)
        {
            return null;
        }
        List<TB_SegModel> listChild = hsrConfig.dicSegListData.get(trainInfo.sectionid);
        if (listChild == null)
        {
            return null;
        }

        //判断车次与区间的站点关系,若与车间站点相反,则距离需要反向查找
        if (trainInfo.startStationid == sectionData.endStatiionid && trainInfo.endStatiionid == sectionData.startStationid)
        {
            distance = sectionData.sectionLength - distance;
        }

        int cnt = listChild.size();
        double minDist = listChild.get(0).distToStart;
        double maxDist = listChild.get(cnt - 1).distToEnd;
        if (distance < minDist || distance > maxDist)
        {
            return null;
        }
        else
        {
            TB_SegModel segData = doFindSegData(listChild, distance, 0, cnt - 1);

//            Double lng = -1D;
//            Double lat = -1D;
//            doFindPositionInSegData(segData, distance, lng, lat);
           Tuple2<Double, Double> lngLatPair = doFindPositionInSegData(segData, distance);

            PositionModel position = new PositionModel();
            position.trainKey = trainInfo.trainKey;
            position.lineid = sectionData.lineid;
            position.sectionid = trainInfo.sectionid;
            position.lng = lngLatPair.first;
            position.lat = lngLatPair.second;
            position.segid = segData.segid;
            return position;
        }
    }

    /**
     * 找出当前距离所在路段(备注:需要生成配置文件的时候已经按距离进行升序)
     * @param listChild
     * @param distance
     * @param sIdx
     * @param eIdx
     * @return
     */
    private TB_SegModel doFindSegData(List<TB_SegModel> listChild, double distance, int sIdx, int eIdx)
    {
        if (eIdx == sIdx)
        {
            return listChild.get(sIdx);
        }
        if (eIdx - sIdx == 1)
        {
        	if(eIdx==listChild.size()-1){
        		return listChild.get(eIdx);
        	}else{
        		return listChild.get(sIdx);
        	}    	
        }
        int mIdx = (sIdx + eIdx) / 2;
        TB_SegModel middleData = listChild.get(mIdx);
        if (middleData.distToStart > distance)
        {
            return doFindSegData(listChild, distance, sIdx, mIdx);
        }
        else if (middleData.distToEnd < distance)
        {
            return doFindSegData(listChild, distance, mIdx, eIdx);
        }
        else
        {
            return middleData;
        }
    }

    /**
     * 检查是否进行初始化
     * @return
     */
    private boolean checkInitData()
    {
        if (hsrConfig.dicImsiData == null || hsrConfig.dicLocpointData == null || hsrConfig.dicSectionData == null || hsrConfig.dicSegListData == null || hsrConfig.dicTrainInfoData == null)
        {
            return false;
        }
        return true;
    }

    /**
     * 查找位置
     * NOTE: 使用包装类型，传递引用以计算值
     * @param segData
     * @param distance
     * @param lng
     * @param lat
     * @return 元组 (经度，纬度)
     */
    private Tuple2<Double, Double> doFindPositionInSegData(TB_SegModel segData, double distance)
    {
        List<PositionModel> listVertexs = segData.GetVertex();
        int cnt = listVertexs.size();
        double length = distance - segData.distToStart; //从起始位置前进的距离
        double lng = -1,  lat = -1;
        double tmpLen = 0;
        for (int i = 0; i < cnt - 1; i++)
        {
            PositionModel p0 = listVertexs.get(i);
            PositionModel p1 = listVertexs.get(i + 1);
            tmpLen += GetDistance(p0.lng, p0.lat, p1.lng, p1.lat);
            if (tmpLen >= length)
            {
                double remainStep = tmpLen - length;
                double atan = Math.atan2(p1.lat - p0.lat, p1.lng - p0.lng);
                lng = p0.lng + Math.cos(atan) * remainStep * 0.00001;
                lat = p0.lat + Math.sin(atan) * remainStep * 0.000009;
                break;
            }
        }
        return new Tuple2<>(lng, lat);
    }

    /**
     * 计算两个经纬度点之间的距离
     * @param x
     * @param y
     * @param x2
     * @param y2
     * @return
     */
    private double GetDistance(double x, double y, double x2, double y2)
    {
        double longitudeDistance = (Math.sin((90 - y2) * 2 * Math.PI / 360) + Math.sin((90 - y) * 2 * Math.PI / 360)) / 2 * (x2 - x) / 360 * 40075360;
        double latitudeDistance = (y2 - y) / 360 * 39940670;
        return (double)Math.sqrt(longitudeDistance * longitudeDistance + latitudeDistance * latitudeDistance);
    }
}




