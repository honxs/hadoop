package cn.mastercom.bigdata.conf.cellconfig;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import cn.mastercom.bigdata.mroxdrmerge.MainModel;

public class LteCellConfig
{
	private static LteCellConfig instance;

	public static LteCellConfig GetInstance() throws IOException
	{
		if (instance == null)
		{
			instance = new LteCellConfig();
		}
		return instance;
	}

	private DistributedFileSystem hdfs;
	private Configuration conf = new Configuration();
	private FileSystem fs;

	private String lteCellPath = "";

	private LteCellConfig() throws IOException
	{
		// String hdfsUrl = MainModel.GetInstance().getAppConfig().getFsUri();
		// FileSystem.setDefaultUri(conf, hdfsUrl);
		fs = FileSystem.get(conf);
		hdfs = (DistributedFileSystem) fs;

		lteCellPath = MainModel.GetInstance().getAppConfig().getLteCellConfigPath();

		lteCellInfoMap = new HashMap<Long, LteCellInfo>();
		fcnPciLteCellMap = new HashMap<Long, List<LteCellInfo>>();

	}

	////////////////////////////////////////////////// tdlte
	////////////////////////////////////////////////// ///////////////////////////////////////////////
	private Map<Long, LteCellInfo> lteCellInfoMap;
	public Map<Long, List<LteCellInfo>> fcnPciLteCellMap;

	public Map<Long, LteCellInfo> getLteCellInfoMap()
	{
		return lteCellInfoMap;
	}

	// 数据来源：select distinct enbid,小区标识,地市id,Round(isnull(b.radius, 5000),2) as
	// 理想覆盖半径,经度,纬度,isnull(convert(int,载频频点), 0) as 载频频点,isnull(pci, 0) as pci
	// from dbo.tb_v2_lte小区_全部 a left join [tb_cfg_designed_radius] b on enbid *
	// 256 + 小区标识 = b.[eci]
	// 导出后，需要转化为unicode，去掉包头
	public boolean loadLteCell()
	{
		try
		{
			if (!lteCellPath.contains(":") && !checkFileExist(lteCellPath))
			{
				return false;
			}

			BufferedReader reader = null;
			lteCellInfoMap = new HashMap<Long, LteCellInfo>();
			fcnPciLteCellMap = new HashMap<Long, List<LteCellInfo>>();
			try
			{
				if (lteCellPath.contains(":"))
					reader = new BufferedReader(new InputStreamReader(new FileInputStream(lteCellPath), "UTF-8"));
				else
					reader = new BufferedReader(new InputStreamReader(hdfs.open(new Path(lteCellPath)), "UTF-8"));

				String strData;
				String[] values;
				long eci;
				long fcnPciKey;
				List<LteCellInfo> fcnPciList = null;
				while ((strData = reader.readLine()) != null)
				{
					if (strData.length() == 0)
					{
						continue;
					}

					try
					{
						values = strData.split("\t", -1);
						LteCellInfo item = LteCellInfo.FillData(values);
						if (item.enbid > 0 && item.cellid > 0)
						{
							eci = item.enbid * 256 + item.cellid;
							lteCellInfoMap.put(eci, item);
						}

						if (item.pci > 0 && item.fcn > 0 && item.cityid > 0)
						{
							fcnPciKey = Long.parseLong(String.format("%02d%05d%03d", item.cityid, item.fcn, item.pci));
							fcnPciList = fcnPciLteCellMap.get(fcnPciKey);
							if (fcnPciList == null)
							{
								fcnPciList = new ArrayList<LteCellInfo>();
								fcnPciLteCellMap.put(fcnPciKey, fcnPciList);
							}
							fcnPciList.add(item);
						}
					}
					catch (Exception e)
					{
						return false;
					}
				}
			}
			catch (Exception e)
			{
				return false;
			}
			finally
			{
				if (reader != null)
				{
					reader.close();
				}
			}

		}
		catch (Exception e)
		{
			return false;
		}

		return true;
	}

	public int getCellCityID(long eci)
	{
		LteCellInfo cellInfo = lteCellInfoMap.get(eci);
		if (cellInfo == null)
		{
			return -1;
		}
		return cellInfo.cityid;
	}

	public LteCellInfo getLteCell(long eci)
	{
		return lteCellInfoMap.get(eci);
	}

	public LteCellInfo getNearestCell(int longtitude, int latitude, int cityID, int fcn, int pci)
	{
		if (longtitude <= 0 || latitude <= 0 || cityID <= 0 || fcn <= 0 || pci <= 0)
		{
			return null;
		}

		long fcnPciKey = Long.parseLong(String.format("%02d%05d%03d", cityID, fcn, pci));
		List<LteCellInfo> fcnPciList = null;
		fcnPciList = fcnPciLteCellMap.get(fcnPciKey);
		if (fcnPciList == null)
		{
			return null;
		}
		int distance = Integer.MAX_VALUE;
		int curDistance = 0;
		LteCellInfo nearestCell = null;
		for (LteCellInfo item : fcnPciList)
		{
			curDistance = (int) GetDistance(longtitude, latitude, item.ilongitude, item.ilatitude);
			if (curDistance < distance)
			{
				nearestCell = item;
				distance = curDistance;
			}
		}
		if (distance >= 6000)
		{
			return null;
		}
		return nearestCell;
	}

	public LteCellInfo getNearestCell(long eci, int fcn, int pci)
	{
		try
		{
			LteCellInfo cellInfo = lteCellInfoMap.get(eci);
			if (cellInfo == null)
			{
				return null;
			}
			return getNearestCell(cellInfo.ilongitude, cellInfo.ilatitude, cellInfo.cityid, fcn, pci);
		}
		catch (Exception e)
		{
			return null;
		}
	}

	////////////////////////////////////////////////// tdlte
	////////////////////////////////////////////////// ///////////////////////////////////////////////

	/**
	 * 查看文件是否存在
	 */
	public boolean checkFileExist(String filename)
	{
		try
		{
			Path f = new Path(filename);
			return hdfs.exists(f);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return false;
	}

	public double GetDistance(double x1, double y1, double x2, double y2)
	{
		double longitudeDistance = (Math.sin((90 - y1) * 2 * Math.PI / 360) + Math.sin((90 - y2) * 2 * Math.PI / 360)) / 2 * (x1 - x2) / 360 * 40075360;
		double latitudeDistance = (y1 - y2) / 360 * 39940670;
		return (double) Math.sqrt(longitudeDistance * longitudeDistance + latitudeDistance * latitudeDistance);
	}

	public double GetDistance(int x1, int y1, int x2, int y2)
	{
		return GetDistance(x1 / 10000000.0, y1 / 10000000.0, x2 / 10000000.0, y2 / 10000000.0);
	}

}
