package cn.mastercom.bigdata.locuser_v2;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

//import cellconfig.LteCellConfig;
//import cellconfig.LteCellInfo;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;

public class CfgCell
{
	private Map<Integer, CellC> cfgcell = new HashMap<Integer, CellC>();
	private Map<String, ArrayList<CellC>> cfgpci = new HashMap<String, ArrayList<CellC>>();

	public void Clear()
	{
		cfgpci.clear();
		cfgcell.clear();
	}

	public boolean Init(ReportProgress rptProgress)
	{
		// 一次加载
		if (cfgcell.size() > 0)
		{
			rptProgress.writeLog(0, "cell count = " + String.valueOf(cfgcell.size()));
			return true;
		}
		Clear();

		if (!GetCell(rptProgress))
		{
			rptProgress.writeLog(0, "get cell error!");
			return false;
		}

		return true;
	}

	// public boolean GetCellCfg(ReportProgress rptProgress, CfgInfo ci)
	// {
	// try
	// {
	// LteCellConfig lcconf = LteCellConfig.GetInstance();
	// if (lcconf.getLteCellInfoMap().size() == 0)
	// {
	// if (!lcconf.loadLteCell())
	// {
	// rptProgress.writeLog(0, "3##");
	// return false;
	// }
	// }
	//
	// Map<Long, LteCellInfo> ltemap = lcconf.getLteCellInfoMap();
	//
	// for (LteCellInfo li : ltemap.values())
	// {
	// CellC sam = new CellC();
	//
	// sam.btsid = li.enbid; // enodebid
	// int cellid = li.cellid;
	// sam.cityid = li.cityid;
	// sam.longitude = li.ilongitude;
	// sam.latitude = li.ilatitude;
	// sam.earfcn = li.fcn;
	// sam.pci = li.pci;
	// sam.isindoor = li.indoor;
	// sam.eci = sam.btsid * 256 + cellid;
	//
	// if (!cfgcell.containsKey(sam.eci))
	// {
	// cfgcell.put(sam.eci, sam);
	// }
	// ///////////////////////////////////////////////
	// int nKey = sam.pci * 65536 + sam.earfcn;
	// String sKey = String.valueOf(sam.cityid) + "_" + String.valueOf(nKey);
	// ArrayList<CellC> lcc = null;
	// if (!cfgpci.containsKey(sKey))
	// {
	// lcc = new ArrayList<CellC>();
	// cfgpci.put(sKey, lcc);
	// }
	// else
	// {
	// lcc = cfgpci.get(sKey);
	// }
	// lcc.add(sam);
	// }
	// }
	// catch (IOException e)
	// {
	// rptProgress.writeLog(0, "4##" + e.getMessage());
	// return false;
	// }
	//
	// rptProgress.writeLog(0, "cell count = " +
	// String.valueOf(cfgcell.size()));
	// return (cfgcell.size() > 0);
	// }

	public boolean GetCell(ReportProgress rptProgress)
	{
		// cityid?? select [ENODEBID],[CellID],0-cityid,0
		// radius,isnull([经度],0),isnull([纬度],0),convert(int,isnull(频点,0)),convert(int,isnull(PCI,0)),'','',case
		// when 覆盖类型 in ('室分','室内') then 1 else 0 end,isnull([方向角],0) from
		// [TB_工参信息表]
		String fname = MainModel.GetInstance().getAppConfig().getLteCellConfigPath();

		BufferedReader sr = CfgInfo.getReader(fname, rptProgress);
		if (sr == null)
		{
			rptProgress.writeLog(0, "1##" + "sr==null!");
			return false;
		}

		try
		{
			int i = 0;
			String sline = sr.readLine();
			while (sline != null)
			{
				String[] recs = sline.split("\t", -1);
				if (recs.length != 12)
				{
					sline = sr.readLine();
					continue;
				}

				i = 0;

				try
				{
					CellC sam = new CellC();

					sam.btsid = Integer.parseInt(recs[i++]); // enodebid
					int cellid = Integer.parseInt(recs[i++]);
					sam.cityid = Integer.parseInt(recs[i++]);
					i++; // item.radius = (int)DataGeter.GetDouble(values[i++]);
					sam.longitude = (int) (Double.parseDouble(recs[i++]) * 10000000);
					sam.latitude = (int) (Double.parseDouble(recs[i++]) * 10000000);
					sam.earfcn = Integer.parseInt(recs[i++]);
					sam.pci = Integer.parseInt(recs[i++]);
					i++; // item.cellName = DataGeter.GetString(values[i++]);
					i++; // item.cityName = DataGeter.GetString(values[i++]);
					sam.isindoor = Integer.parseInt(recs[i++]);
					i++; // item.angle = Int32.Parse(recs[i++]);
					sam.eci = sam.btsid * 256 + cellid;

					if (!cfgcell.containsKey(sam.eci))
					{
						cfgcell.put(sam.eci, sam);
					}
					///////////////////////////////////////////////
					int nKey = sam.pci * 65536 + sam.earfcn;
					String sKey = String.valueOf(sam.cityid) + "_" + String.valueOf(nKey);
					ArrayList<CellC> lcc = null;
					if (!cfgpci.containsKey(sKey))
					{
						lcc = new ArrayList<CellC>();
						cfgpci.put(sKey, lcc);
					}
					else
					{
						lcc = cfgpci.get(sKey);
					}
					lcc.add(sam);
				}
				catch (Exception ee)
				{
					rptProgress.writeLog(0, "2##" + ee.getMessage());
				}

				sline = sr.readLine();
			}
		}
		catch (Exception ee)
		{
			rptProgress.writeLog(0, "3##" + ee.getMessage());

			cfgcell.clear();
		}

		if (sr != null)
		{
			try
			{
				sr.close();
			}
			catch (IOException e)
			{
				rptProgress.writeLog(0, "4##" + e.getMessage());
			}
		}

		rptProgress.writeLog(0, "cell count = " + String.valueOf(cfgcell.size()));
		return (cfgcell.size() > 0);
	}

	public CellC GetCell(int eci)
	{
		if (cfgcell.containsKey(eci))
		{
			return cfgcell.get(eci);
		}

		return null;
	}

	private static double GetDistance(double lng1, double lat1, double lng2, double lat2)
	{
		// 计算y1, y2所在位置的纬度圈长度
		double dx1 = Math.sin((90.0 - lat1) * 2 * Math.PI / 360.0);
		double dx2 = Math.sin((90.0 - lat2) * 2 * Math.PI / 360.0);

		double dx = (dx1 + dx2) / 2.0 * (lng1 - lng2) / 360.0 * 40075360;
		double dy = (lat2 - lat1) / 360.0 * 39940670;

		return (double) (Math.sqrt(dx * dx + dy * dy) + 0.5);
	}

	public CellC GetNCell(int cityid, int seci, int pciearfcn, double longitude, double latitude)
	{
		String sKey = String.valueOf(cityid) + "_" + String.valueOf(pciearfcn);
		if (!cfgpci.containsKey(sKey))
		{
			return null;
		}

		double dist = 0;
		double distmin = -1;
		CellC cc = null;
		ArrayList<CellC> lcc = cfgpci.get(sKey);
		for (int ii = 0; ii < lcc.size(); ii++)
		{
			if (lcc.get(ii).eci == seci)
			{
				continue;
			}
			dist = GetDistance(longitude, latitude, lcc.get(ii).longitude / 10000000.0, lcc.get(ii).latitude / 10000000.0);
			if (dist > 3000)
			{
				continue;
			}
			if (distmin < 0)
			{
				distmin = dist;
				cc = lcc.get(ii);
			}
			else if (dist < distmin)
			{
				distmin = dist;
				cc = lcc.get(ii);
			}
		}

		return cc;
	}
}
