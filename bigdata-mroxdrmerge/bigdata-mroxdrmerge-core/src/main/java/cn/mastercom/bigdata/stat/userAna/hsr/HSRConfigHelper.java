package cn.mastercom.bigdata.stat.userAna.hsr;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

import cn.mastercom.bigdata.util.hadoop.hdfs.FileReader;
import cn.mastercom.bigdata.util.hadoop.hdfs.FileReader.LineHandler;

public class HSRConfigHelper
{

	private static HSRConfigHelper m_configHelper = null;

	public static HSRConfigHelper getInstance()
	{
		if (m_configHelper == null)
		{
			m_configHelper = new HSRConfigHelper();
		}

		return m_configHelper;
	}

	/*
	 * 站点ID - 站点
	 */
	public Map<Integer, RailStation> id_station_map;

	/*
	 * 区间ID - 区间
	 */
	public Map<Integer, RailSec> id_railSec_map;

	/*
	 * ECI - 站点(一个小区只能覆盖一个站点)
	 */
	public Map<Long, Set<RailStation>> eci_station_map;

	/*
	 * 一个小区可能覆盖两段路
	 */
	public Map<Long, Set<RailSec>> eci_railSec_map;
	
	/**
	 * 区间-路段配置
	 */
	public Map<Integer, List<RailSeg>> secId_seg_map;
	
	//eci-室分小区
	//public Map<Long, Set<RailStation>> eci_indoor_map;
	private HSRConfigHelper()
	{
		id_station_map = new HashMap<Integer, RailStation>();
		id_railSec_map = new HashMap<Integer, RailSec>();
		eci_station_map = new HashMap<Long, Set<RailStation>>();
		eci_railSec_map = new HashMap<Long, Set<RailSec>>();
		
		//add 区间-路段配置
		secId_seg_map = new HashMap<Integer, List<RailSeg>>();
		//eci_indoor_map = new HashMap<Long, Set<RailStation>>();
	}

	public void clear()
	{
		for (RailStation railStation : id_station_map.values())
		{
			railStation.imsi_StationImsi_map.clear();
		}
	}

	public void readConfig(Configuration conf, String hsrStationPath, String hsrSectionPath, String hsrStationCellPath,
			String hsrSectionCellPath, String hsrSectionRruPath, String hsrSegFilePath, String hsrIndoorFilePath) throws Exception
	{
		//// String path = "E:\\文件\\高铁测试配置\\test\\";
		FileReader.readFiles(conf,
				hsrStationPath/* path + "tb_高铁分析_配置_高铁站.txt" */, new LineHandler()
				{
					@Override
					public void handle(String line)
					{
						RailStation station = new RailStation(line);
						id_station_map.put(station.id, station);
					}
				});

		FileReader.readFiles(conf,
				hsrSectionPath/* path + "tb_高铁分析_配置_高铁线路.txt" */, new LineHandler()
				{
					@Override
					public void handle(String line)
					{
						RailSec railSec = new RailSec(line, id_station_map);
						id_railSec_map.put(railSec.id, railSec);
					}
				});

		FileReader.readFiles(conf,
				hsrStationCellPath/* path + "tb_高铁分析_配置_高铁站小区.txt" */, new LineHandler()
				{
					@Override
					public void handle(String line)
					{// CityID 站点ID ECI Comment
						String[] arrs = line.split("\t");
						// int id = Integer.parseInt(arrs[0]);
						// long eci = Long.parseLong(arrs[1]);
						int id = Integer.parseInt(arrs[1]);
						long eci = Long.parseLong(arrs[2]);

						if (id_station_map.containsKey(id))
						{
							Set<RailStation> railStationSet = null;
							if((railStationSet = eci_station_map.get(eci)) == null){
								railStationSet = new HashSet<>();
								eci_station_map.put(eci, railStationSet);
							}
							railStationSet.add(id_station_map.get(id));
						}
					}
				});

		FileReader.readFiles(conf, hsrSectionCellPath, new LineHandler()
		{
			@Override
			public void handle(String line)
			{// CityID 区间ID ECI 小区到起始站距离 小区到终点站距离 Comment
				String[] arrs = line.split("\t");
				
				int id = Integer.parseInt(arrs[1]);
				long eci = Long.parseLong(arrs[2]);
				double dist1 = Double.parseDouble(arrs[3]);
				double dist2 = Double.parseDouble(arrs[4]);
				double dist = Double.parseDouble(arrs[5]);//eci离铁路的距离
				
				if(dist > 1000) return;

				/*if (eci_station_map.containsKey(eci))
				{
					RailStation station = eci_station_map.get(eci);
					if (!station.outProvince) // 省外站的eci可以加在线路上
					{
						return;
					}
				}*/

				if (id_railSec_map.containsKey(id))
				{
					RailSec railSec = id_railSec_map.get(id);
					Set<RailSec> railSecs = null;
					if (eci_railSec_map.containsKey(eci))
					{
						railSecs = eci_railSec_map.get(eci);
					}
					else
					{
						railSecs = new HashSet<RailSec>();
						eci_railSec_map.put(eci, railSecs);
					}
					railSec.addEci(eci, dist1, dist2);
					railSecs.add(railSec);

					/*
					 * 工参中已处理 省外站,线路上的eci,加在省外站中
					 * 
					 * if (railSec.stationID1.outProvince) { if (dist2 >= 10000)
					 * // 小区与站点距离>=10km { eci_station_map.put(eci,
					 * railSec.stationID1); } } else if
					 * (railSec.stationID2.outProvince) { if (dist1 >= 10000) //
					 * 小区与站点距离>=10km { eci_station_map.put(eci,
					 * railSec.stationID2); } }
					 */
				}
			}
		});
		try{
			FileReader.readFiles(conf, hsrSectionRruPath, new LineHandler()
				{
					@Override
					public void handle(String line)
					{
						String[] arrs = line.split("\t");
						
						int id = Integer.parseInt(arrs[1]);
						long eci1 = Long.parseLong(arrs[2]);
						long eci2 = Long.parseLong(arrs[3]);
						double dist1 = Double.parseDouble(arrs[4]);
						double dist2 = Double.parseDouble(arrs[5]);

						if (id_railSec_map.containsKey(id))
						{
							RailSec railSec = id_railSec_map.get(id);
							railSec.addRru(eci1, eci2, dist1, dist2);
						}
					}
				});
		}catch( FileNotFoundException e){}
		
		
		FileReader.readFiles(conf, hsrSegFilePath, new LineHandler(){

			@Override
			public void handle(String line)
			{
				String[] values = line.split("\t", -1);

				RailSeg railSeg = new RailSeg();
				railSeg.id = Integer.parseInt(values[1]);
				railSeg.sectionId = Integer.parseInt(values[2]);
				railSeg.distToStart = Double.parseDouble(values[3]);
				railSeg.distToEnd = Double.parseDouble(values[4]);
//				railSeg.PointShape = BitConverter.toBytes(values[5]);

				List<RailSeg> listChild = secId_seg_map.get(railSeg.sectionId);
				if (listChild != null)
				{
					listChild.add(railSeg);
				}
				else
				{
					listChild = new ArrayList<RailSeg>();
					listChild.add(railSeg);
					secId_seg_map.put(railSeg.sectionId, listChild);
				}
			}
		});
		
		//按距离排序
		for(List<RailSeg> listChild : secId_seg_map.values()){
			Collections.sort(listChild, new Comparator<RailSeg>()
			{
				@Override
				public int compare(RailSeg o1, RailSeg o2)
				{
			
					return (int) (o1.distToStart - o2.distToStart);
				}
			});
		}
		
		
	}

	public boolean checkInited(){
		if(id_station_map == null || id_station_map.isEmpty())
			return false;
		if(id_railSec_map == null || id_railSec_map.isEmpty())
			return false;
		if(eci_station_map == null || eci_station_map.isEmpty())
			return false;
		if(eci_railSec_map == null || eci_railSec_map.isEmpty())
			return false;
		return true;
	}
}
