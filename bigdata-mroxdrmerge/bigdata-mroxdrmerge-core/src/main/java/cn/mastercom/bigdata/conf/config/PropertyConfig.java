package cn.mastercom.bigdata.conf.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.DPoint;
import cn.mastercom.bigdata.util.GisFunction;
import cn.mastercom.bigdata.util.PolygonUtil;
import cn.mastercom.bigdata.util.hadoop.hdfs.FileReader;

public class PropertyConfig {
    /**
     *加载物业点配置
     */

    private final static int gridSize = 40000; //栅格大小 400米
    private static PropertyConfig instance;
    private Map<DPoint, Set<PropertyInfo>> gridMap;

    public static PropertyConfig GetInstance()
    {
        if (instance == null)
        {
            instance = new PropertyConfig();
        }
        return instance;
    }

    public PropertyConfig()
    {
        gridMap = new HashMap<>();
    }

    public boolean loadProperty(Configuration conf) {
        String filePath = MainModel.GetInstance().getAppConfig().getPropertyConfigPath();
        return loadProperty(conf, filePath);
    }

    private boolean loadProperty(Configuration conf, String filePath)
    {
        try{
            FileReader.readFile(conf, filePath, new FileReader.LineHandler() {
                @Override
                public void handle(String line) {
                    String[] values;
                    try {
                        if (line.trim().length()==0)
                        {
                            return;
                        }
                        values = line.split("\t", -1);
                        if (values.length < 17){
                        	return;
                        }
                        PropertyInfo propertyInfo = new PropertyInfo(values);
                        setGridMap(propertyInfo, gridMap);
                    }
                    catch (Exception e) {
                    }
                }
            });
        }catch (Exception e){
        }
        return true;
    }

    //假如重心点落在物业点内，则直接回填物业点。假如重心点不在物业点内，则计算附近1公里的物业点，并且物业点的小区列表内包含用户驻留小区3强，求出满足条件的最近的一个物业点，进行回填。
    public PropertyInfo getAreaID(int longitude, int latitude, Set<Long> eciSet)
    {
        PropertyInfo tempPropertyInfo = null;
        Set<PropertyInfo> propertyInfoSet = findPropertySet(longitude, latitude, gridMap);
        double lot = longitude * 1.0 / 10000000;
        double lat = latitude * 1.0 / 10000000;
        if (propertyInfoSet!=null)
        {
            for (PropertyInfo propertyInfo : propertyInfoSet)
            {
                List<DPoint> vertexes = propertyInfo.polygonPoints;
                //点本身在多边形内
                if (PolygonUtil.isDPointInOrOnPolygon(lot, lat, vertexes))
                {
                	tempPropertyInfo = propertyInfo;
                    break;
                }
                propertyInfo.distance = (long) GisFunction.GetDistance(longitude, latitude,
                		propertyInfo.centerLongitude, propertyInfo.centerLatitude);
            }
            
            if (tempPropertyInfo == null)
            {
            	//对物业点根据距离升序排列
            	List<PropertyInfo> list = new ArrayList(propertyInfoSet);
            	Collections.sort(list, new Comparator<PropertyInfo>() {
        			@Override
        			public int compare(PropertyInfo b1, PropertyInfo b2) {
        				// TODO Auto-generated method stub
        				return (int) (b1.distance - b2.distance);
        			}
        		});
            	
            	for (PropertyInfo propertyInfo : list)
                {
                	if(propertyInfo.getEciSet().containsAll(eciSet))
                	{
                		tempPropertyInfo = propertyInfo;
                        break;
                	}
                }
            }
        }
        return tempPropertyInfo;
    }

    // 筛选配置
    private static Set<PropertyInfo> findPropertySet(int longitude, int latitude,
                                         Map<DPoint, Set<PropertyInfo>> gridMap) {
    	Set<PropertyInfo> propertySet = new HashSet<>();

        int lngLeft = longitude / gridSize * gridSize;
        int latLower = latitude / gridSize * gridSize;
        
        //外扩两圈
        int bigGridLngLeft = lngLeft - gridSize * 2;
        int bigGridLatLower = latLower - gridSize * 2;
        for (int i=bigGridLngLeft;i<=lngLeft + gridSize * 2;i+=gridSize)
        {
        	for(int j=bigGridLatLower;j<=latLower + gridSize * 2;j+=gridSize)
        	{
        		DPoint DPoint = new DPoint(i, j);
        		Set<PropertyInfo> set = gridMap.get(DPoint);
        		if (set != null) {
                	propertySet.addAll(set);
                }
        	}
        }
        return propertySet;
    }

    // 设置栅格Map
    private static void setGridMap(PropertyInfo propertyInfo, Map<DPoint, Set<PropertyInfo>> gridMap) {

        // 栅格左上角和右下角边界的经纬度
        int tlLot = propertyInfo.tlLongitude / gridSize * gridSize;
        int tlLat = propertyInfo.tlLatitude / gridSize * gridSize;
        int brLot = propertyInfo.brLongitude / gridSize * gridSize;
        int brLat = propertyInfo.brLatitude / gridSize * gridSize;

        for (int i = tlLot; i <= brLot; i += gridSize) {
            for (int j = brLat; j <= tlLat; j += gridSize) {
                DPoint subLeftLowerGrid = new DPoint(i, j);
                Set<PropertyInfo> roadSet = gridMap.get(subLeftLowerGrid);
                if (roadSet == null) {
                    Set<PropertyInfo> set = new HashSet<>();
                    set.add(propertyInfo);
                    gridMap.put(subLeftLowerGrid, set);
                } else {
                    roadSet.add(propertyInfo);
                    gridMap.put(subLeftLowerGrid, roadSet);
                }
            }
        }
    }
}
