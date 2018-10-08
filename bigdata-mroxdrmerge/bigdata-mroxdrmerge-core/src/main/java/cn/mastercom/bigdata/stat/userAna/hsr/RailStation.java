package cn.mastercom.bigdata.stat.userAna.hsr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.mastercom.bigdata.stat.userAna.model.StationImsi;

/*
 * 站点信息
 */
public class RailStation
{
    public int id;
    public String name = null;
    public boolean outProvince;

    // public double Longitude = 0;
    // public double Latitude = 0;

    //cityid	站点id	站点名称	经度	维度	站点类型	Comment
    public RailStation(String line)
    {
        imsi_StationImsi_map = new HashMap<String, List<StationImsi>>();

        String[] arrs = line.split("\t");
        
        id = Integer.parseInt(arrs[1]);
        name = arrs[2];
        outProvince = arrs[5].contains("2");
    }

    public Map<String,List<StationImsi>> imsi_StationImsi_map;

    public void add(StationImsi stationImsi)
    {
    	if (imsi_StationImsi_map.containsKey(stationImsi.imsi))
    	{
    		List<StationImsi> ls = imsi_StationImsi_map.get(stationImsi.imsi);
    		ls.add(stationImsi);
    	}
    	else
    	{
    		List<StationImsi> ls = new ArrayList<StationImsi>();
    		ls.add(stationImsi);
    		imsi_StationImsi_map.put(stationImsi.imsi, ls);
    	}
    	
    }

}
