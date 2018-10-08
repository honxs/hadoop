package cn.mastercom.bigdata.standalone.local;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import cn.mastercom.bigdata.standalone.local.FileReader.LineHandler;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;

public class GridBuildingMap
{
	public static Logger LOG = Logger.getLogger(MainModel.class);

	Map<Integer, Map<Integer, Map<Integer, List<Integer>>>> city_lot_lat_building_map;

	int rx;
	int ry;

	/**
	 * 
	 * @param indoorGridSize
	 *            : 单位 米
	 * @param files
	 * @throws Exception
	 */
	public GridBuildingMap(int indoorGridSize, List<File> files) throws Exception
	{
		city_lot_lat_building_map = new HashMap<Integer, Map<Integer, Map<Integer, List<Integer>>>>();
		this.rx = indoorGridSize * 100;
		this.ry = indoorGridSize * 90;

		for (File file : files)
		{
			readFile(file.getAbsolutePath());
		}
	}

	private void readFile(String file) throws Exception
	{
		LOG.info("开始加载楼宇栅格:" + file);

		FileReader.readFileGzUTF8(file, new LineHandler()
		{
			Map<Integer, Map<Integer, List<Integer>>> lot_lat_building_map = null;
			int _cityID = -1;

			@Override
			public void handle(String line)
			{
				String[] arrs = line.split("\t", 0);
				int cityID = Integer.parseInt(arrs[0]);
				if (_cityID != cityID)
				{
					_cityID = cityID;

					if (city_lot_lat_building_map.containsKey(_cityID))
					{
						lot_lat_building_map = city_lot_lat_building_map.get(_cityID);
					}
					else
					{
						lot_lat_building_map = new HashMap<Integer, Map<Integer, List<Integer>>>();
						city_lot_lat_building_map.put(_cityID, lot_lat_building_map);
					}
				}

				readGrid(Integer.parseInt(arrs[1]), Integer.parseInt(arrs[2]), Integer.parseInt(arrs[3]),
						Integer.parseInt(arrs[4]), Integer.parseInt(arrs[5]));
			}

			private void readGrid(int buildingID, int x1, int y1, int x2, int y2)
			{
				int mx1 = x1 / rx;
				int my1 = y1 / ry;
				int mx2 = x2 / rx;
				int my2 = y2 / ry;
				
				for (int lot = mx1; lot <= mx2; lot++)
				{
					for (int lat = my1; lat <= my2; lat++)
					{
						Map<Integer, List<Integer>> lat_building_map = null;
						if (lot_lat_building_map.containsKey(lot))
						{
							lat_building_map = lot_lat_building_map.get(lot);
						}
						else
						{
							lat_building_map = new HashMap<Integer, List<Integer>>();
							lot_lat_building_map.put(lot, lat_building_map);
						}

						List<Integer> ls = null;
						if (lat_building_map.containsKey(lat))
						{
							ls = lat_building_map.get(lat);
						}
						else
						{
							ls = new ArrayList<Integer>();
							lat_building_map.put(lat, ls);
						}

						ls.add(buildingID);
					}
				}
				
				
			}

		});


		LOG.info("加载楼宇栅格完成:" + file);
	}

	public int get(int cityID, int lot, int lat)
	{
		if (city_lot_lat_building_map.containsKey(cityID))
		{
			Map<Integer, Map<Integer, List<Integer>>> lot_lat_building_map = city_lot_lat_building_map.get(cityID);
			int x = lot / rx;
			if (lot_lat_building_map.containsKey(x))
			{
				Map<Integer, List<Integer>> lat_building_map = lot_lat_building_map.get(x);

				int y = lat / ry;
				if (lat_building_map.containsKey(y))
				{
					return lat_building_map.get(y).get(0);
				}
			}
		}
		return -1;
	}
}
