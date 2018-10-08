package cn.mastercom.bigdata.xdr.loc;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;
import org.apache.hadoop.io.Text;

import cn.mastercom.bigdata.StructData.CellFreqItem;
import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.GridItem;
import cn.mastercom.bigdata.StructData.NC_LTE;
import cn.mastercom.bigdata.StructData.StatFreqCell;
import cn.mastercom.bigdata.StructData.StatFreqGrid;
import cn.mastercom.bigdata.StructData.Stat_Grid_4G;
import cn.mastercom.bigdata.StructData.Stat_Grid_Freq_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.conf.config.ImeiConfig;
import cn.mastercom.bigdata.mro.stat.tableEnum.MroCsOTTTableEnum;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.util.TimeHelper;

public class DayDataDeal_4G
{
	private Map<Integer, DayDataItem> dayDataDealMap;// time，天统计
	private int curDayTime;
	private int statType;

	public DayDataDeal_4G(int statType)
	{
		dayDataDealMap = new HashMap<Integer, DayDataItem>();
		this.statType = statType;
	}

	public void dealSample(DT_Sample_4G sample)
	{
		if (sample.itime == 0)
		{
			return;
		}

		curDayTime = TimeHelper.getRoundDayTime(sample.itime);
		DayDataItem dayDataDeal = dayDataDealMap.get(curDayTime);
		
		
		if (dayDataDeal == null)
		{
			dayDataDeal = new DayDataItem(curDayTime);
			dayDataDealMap.put(curDayTime, dayDataDeal);
		}

		if (sample.flag.toUpperCase().equals("MRO") || sample.flag.toUpperCase().equals("MRE"))
		{
			dayDataDeal.dealMr(sample);
		}
		else if (sample.flag.toUpperCase().equals("EVT"))
		{
			dayDataDeal.dealEvent(sample);
		}
	}

	public Map<Integer, DayDataItem> getDayDataDealMap()
	{
		return dayDataDealMap;
	}

	public int getGridCount()
	{
		int gridCount = 0;
		for (DayDataItem item : dayDataDealMap.values())
		{
			gridCount += item.getGridDataMap().size();
		}
		return gridCount;
	}

	public int getGridFreqCount()
	{
		int gridCount = 0;
		for (DayDataItem item : dayDataDealMap.values())
		{
			for (Map<GridItem, GridData_Freq> freqMap : item.getGridDataFreqMap().values())
			{
				gridCount += freqMap.size();
			}
		}
		return gridCount;
	}

	public int getCellGridCount()
	{
		int gridCount = 0;
		for (DayDataItem item : dayDataDealMap.values())
		{
			for (CellGridData_4G cellGridData : item.getCellGridDataMap().values())
			{
				gridCount += cellGridData.getGridDataMap().size();
			}
		}
		return gridCount;
	}

	public class DayDataItem
	{
		private int statTime;
		private Map<Long, CellData_4G> cellDataMap;
		private Map<CellFreqItem, CellData_Freq> cellDataFreqMap;
		private Map<Long, CellGridData_4G> cellGridDataMap;
		private Map<GridItem, GridData_4G> gridDataMap;
		private Map<Integer, Map<GridItem, GridData_Freq>> gridDataFreqMap;
		// private Map<Long, LocationStat_4G> locStatMap;
		// 10*10栅格统计
		private Map<Long, CellGridData_4G> ten_cellGridDataMap;
		private Map<GridItem, GridData_4G> ten_gridDataMap;
		private Map<Integer, Map<GridItem, GridData_Freq>> ten_gridDataFreqMap;

		// new stat
		private Map<String, IResultTable> mrOutGridMap;
		private Map<String, IResultTable> mrBuildMap;
		private Map<String, IResultTable> mrInGridMap;
		private Map<String, IResultTable> mrBuildCellMap;
		private Map<String, IResultTable> mrInGridCellMap;
		private Map<String, IResultTable> mrOutGridCellMap;
		private Map<String, IResultTable> mrStatCellMap;
		private Map<String, IResultTable> mrBuildCellNcMap;
		private Map<String, IResultTable> mrInGridCellNcMap;
		private Map<String, IResultTable> mrOutGridCellNcMap;
		private Map<String, IResultTable> topicCellIsolatedMap;
		// new feq cell/grid 20170518
		private Map<String, StatFreqCell> freqLTCellMap;
		private Map<String, StatFreqGrid> ten_LTfreqGridMap;

		private Map<String, StatFreqCell> freqDXCellMap;
		private Map<String, StatFreqGrid> ten_DXfreqGridMap;
		////////////////////////////////////////////////////////////////////////////////////////////////

		public DayDataItem(int statTime)
		{
			this.statTime = statTime;
			cellDataMap = new HashMap<Long, CellData_4G>();
			cellDataFreqMap = new HashMap<CellFreqItem, CellData_Freq>();
			cellGridDataMap = new HashMap<Long, CellGridData_4G>();
			gridDataMap = new HashMap<GridItem, GridData_4G>();
			gridDataFreqMap = new HashMap<Integer, Map<GridItem, GridData_Freq>>();

			ten_cellGridDataMap = new HashMap<Long, CellGridData_4G>();
			ten_gridDataMap = new HashMap<GridItem, GridData_4G>();
			ten_gridDataFreqMap = new HashMap<Integer, Map<GridItem, GridData_Freq>>();

			mrOutGridMap = new HashMap<String, IResultTable>();
			mrBuildMap = new HashMap<String, IResultTable>();
			mrInGridMap = new HashMap<String, IResultTable>();
			mrBuildCellMap = new HashMap<String, IResultTable>();
			mrInGridCellMap = new HashMap<String, IResultTable>();
			mrOutGridCellMap = new HashMap<String, IResultTable>();
			mrStatCellMap = new HashMap<String, IResultTable>();
			mrBuildCellNcMap = new HashMap<String, IResultTable>();
			mrInGridCellNcMap = new HashMap<String, IResultTable>();
			mrOutGridCellNcMap = new HashMap<String, IResultTable>();
			topicCellIsolatedMap = new HashMap<String, IResultTable>();
			///////////////////////////////////////////////////////////////////////////////////
			freqLTCellMap = new HashMap<String, StatFreqCell>();
			ten_LTfreqGridMap = new HashMap<String, StatFreqGrid>();
			freqDXCellMap = new HashMap<String, StatFreqCell>();
			ten_DXfreqGridMap = new HashMap<String, StatFreqGrid>();

		}

		public void outData(ResultOutputer resultOutputer, Text curText, Map<String, Integer> map)
		{
			if (map == null || map.get(MrOutGrid.TypeName) > 100000)
			{
				outOneMap(resultOutputer, curText, MroCsOTTTableEnum.MrOutGrid.getIndex(), mrOutGridMap.values());
				mrOutGridMap.clear();
			}

			if (map == null || map.get(MrBuild.TypeName) > 100000)
			{
				outOneMap(resultOutputer, curText, MroCsOTTTableEnum.MrBuild.getIndex(), mrBuildMap.values());
				mrBuildMap.clear();
			}

			if (map == null || map.get(MrInGrid.TypeName) > 100000)
			{
				outOneMap(resultOutputer, curText, MroCsOTTTableEnum.MrInGrid.getIndex(), mrInGridMap.values());
				mrInGridMap.clear();
			}

			if (map == null || map.get(MrBuildCell.TypeName) > 100000)
			{
				outOneMap(resultOutputer, curText, MroCsOTTTableEnum.MrBuildCell.getIndex(), mrBuildCellMap.values());
				mrBuildCellMap.clear();
			}

			if (map == null || map.get(MrInGridCell.TypeName) > 100000)
			{
				outOneMap(resultOutputer, curText, MroCsOTTTableEnum.MrInGridCell.getIndex(), mrInGridCellMap.values());
				mrInGridCellMap.clear();
			}

			if (map == null || map.get(MrOutGridCell.TypeName) > 100000)
			{
				outOneMap(resultOutputer, curText, MroCsOTTTableEnum.MrOutGridCell.getIndex(), mrOutGridCellMap.values());
				mrOutGridCellMap.clear();
			}

			if (map == null || map.get(MrStatCell.TypeName) > 100000)
			{
				outOneMap(resultOutputer, curText, MroCsOTTTableEnum.MrStatCell.getIndex(), mrStatCellMap.values());
				mrStatCellMap.clear();
			}

			if (map == null || map.get(MrBuildCellNc.TypeName) > 100000)
			{
				outOneMap(resultOutputer, curText, MroCsOTTTableEnum.MrBuildCellNc.getIndex(), mrBuildCellNcMap.values());
				mrBuildCellNcMap.clear();
			}

			if (map == null || map.get(MrInGridCellNc.TypeName) > 100000)
			{
				outOneMap(resultOutputer, curText, MroCsOTTTableEnum.MrInGridCellNc.getIndex(), mrInGridCellNcMap.values());
				mrInGridCellNcMap.clear();
			}

			if (map == null || map.get(MrOutGridCellNc.TypeName) > 100000)
			{
				outOneMap(resultOutputer, curText, MroCsOTTTableEnum.MrOutGridCellNc.getIndex(), mrOutGridCellNcMap.values());
				mrOutGridCellNcMap.clear();
			}

			if (map == null || map.get(TopicCellIsolated.TypeName) > 100000)
			{
				outOneMap(resultOutputer, curText, MroCsOTTTableEnum.TopicCellIsolated.getIndex(), topicCellIsolatedMap.values());
				topicCellIsolatedMap.clear();
			}

			//////////////////////////////////////////////////////////////////////////////////////////////
		}

		private void outOneMap(ResultOutputer resultOutputer, Text curText, int type, Collection<IResultTable> items)
		{
			for (IResultTable item : items)
			{
				curText.set(item.toLine());
				try
				{
					resultOutputer.pushData(type, item.toLine());
				}
				catch (Exception e)
				{
					LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,
							"xdrloc DayDataDeal_4G.outOneMap","xdrloc DayDataDeal_4G.outOneMap " +
							"error",	e);
				}
			}
		}

		public Map<Long, CellGridData_4G> getCellGridDataMap()
		{
			return cellGridDataMap;
		}

		public Map<GridItem, GridData_4G> getGridDataMap()
		{
			return gridDataMap;
		}

		public Map<Integer, Map<GridItem, GridData_Freq>> getGridDataFreqMap()
		{
			return gridDataFreqMap;
		}

		public Map<Long, CellData_4G> getCellDataMap()
		{
			return cellDataMap;
		}

		public Map<Long, CellGridData_4G> getTen_cellGridDataMap()
		{
			return ten_cellGridDataMap;
		}

		public Map<GridItem, GridData_4G> getTen_gridDataMap()
		{
			return ten_gridDataMap;
		}

		public Map<Integer, Map<GridItem, GridData_Freq>> getTen_gridDataFreqMap()
		{
			return ten_gridDataFreqMap;
		}

		public Map<CellFreqItem, CellData_Freq> getCellDataFreqMap()
		{
			return cellDataFreqMap;
		}

		public Map<String, StatFreqCell> getFreqLTCellMap()
		{
			return freqLTCellMap;
		}

		public Map<String, StatFreqCell> getFreqDXCellMap()
		{
			return freqDXCellMap;
		}

		public Map<String, StatFreqGrid> getTen_LTfreqGridMap()
		{
			return ten_LTfreqGridMap;
		}

		public Map<String, StatFreqGrid> getTen_DXfreqGridMap()
		{
			return ten_DXfreqGridMap;
		}

		//////////////////////////////////////////////////////////////////////////////////////////////

		public int getStatTime()
		{
			return statTime;
		}

		public void dealMr(DT_Sample_4G sample)
		{
			if (statType == StatDeal.STATDEAL_ALL)
			{
				statCell(sample);
				statGridForAll(sample);
			}

			if (statType == StatDeal.STATDEAL_DT)
			{
				statGrid(sample);
			}

			if (statType == StatDeal.STATDEAL_CQT)
			{
				statGrid(sample);
			}
		}

		public void dealEvent(DT_Sample_4G sample)
		{
			if (statType == StatDeal.STATDEAL_ALL)
			{
				statCell(sample);
				statGrid(sample);
			}

			if (statType == StatDeal.STATDEAL_DT)
			{
				statGrid(sample);
			}

			if (statType == StatDeal.STATDEAL_CQT)
			{
				statGrid(sample);
			}
		}

		/**
		 * 联通频点
		 * 
		 * @param fcn
		 * @return
		 */
		public boolean ifLtFcn(int fcn)
		{
			int[] lt_fcn_list = { 1600, 1650, 40340 };
			for (int temfcn : lt_fcn_list)
			{
				if (temfcn == fcn)
				{
					return true;
				}
			}
			return false;
		}

		/**
		 * 电信频点
		 * 
		 * @param fcn
		 * @return
		 */
		public boolean ifDxFcn(int fcn)
		{
			int[] dx_fcn_list = { 1775, 1800, 1825, 1850, 1870, 75, 100 };
			for (int temfcn : dx_fcn_list)
			{
				if (temfcn == fcn)
				{
					return true;
				}
			}

			if (fcn >= 2410 && fcn <= 2510)
			{// 800M
				return true;
			}
			return false;
		}

		// 小区统计是全量数据进行运算
		private void statCell(DT_Sample_4G sample)
		{
			if (sample.Eci > 0)
			{
				// 只统计mro,mdt的数据，mre不考虑
				if (!sample.flag.toUpperCase().equals("MRE")
				// || sample.flag.toUpperCase().equals("EVT")
				)
				{
					CellData_4G cellData = cellDataMap.get(sample.Eci);
					if (cellData == null)
					{
						cellData = new CellData_4G(sample.cityID, sample.iLAC, sample.Eci, statTime, statTime + 86400);
						cellDataMap.put(sample.Eci, cellData);
					}
					cellData.dealSample(sample);
				}
				// 统计异频数据
				if (sample.flag.toUpperCase().equals("MRO"))
				{
					List<NC_LTE> itemList = sample.getNclte_Freq();
					int val = ImeiConfig.GetInstance().getValue(sample);
					if (itemList.size() > 0)
					{
						for (NC_LTE item : itemList)
						{
							CellFreqItem key = new CellFreqItem(sample.Eci, item.LteNcEarfcn, item.LteNcPci);

							CellData_Freq cellData = cellDataFreqMap.get(key);
							if (cellData == null)
							{
								cellData = new CellData_Freq(sample.cityID, sample.iLAC, sample.Eci, item.LteNcEarfcn, item.LteNcPci, statTime, statTime + 86400);
								cellDataFreqMap.put(key, cellData);
							}
							cellData.dealSample(sample, item.LteNcRSRP, item.LteNcRSRQ);

							// new cellFreq
							String skey = sample.Eci + "_" + item.LteNcEarfcn;
							StatFreqCell freqCell = freqLTCellMap.get(skey);
							if (freqCell == null)
							{
								freqCell = new StatFreqCell(sample.cityID, statTime, statTime + 86400, (int) sample.Eci, item.LteNcEarfcn);
								if (ifLtFcn(item.LteNcEarfcn))
								{
									freqLTCellMap.put(skey, freqCell);
								}
								else if (ifDxFcn(item.LteNcEarfcn))
								{
									freqDXCellMap.put(skey, freqCell);
								}
							}
							freqCell.dealSample(sample, item.LteNcRSRP, item.LteNcRSRQ);
						}
					}
					else if (val > 0)// 手机支持联通或电信 却没有收到异频数据
					{
						String skey = sample.Eci + "_" + 0;
						StatFreqCell freqCell = freqLTCellMap.get(skey);
						if (freqCell == null)
						{
							freqCell = new StatFreqCell(sample.cityID, statTime, statTime + 86400, (int) sample.Eci, 0);
							if (val == 1)// 只支持电信
							{
								freqDXCellMap.put(skey, freqCell);
							}
							else if (val == 2)// 只支持联通
							{
								freqLTCellMap.put(skey, freqCell);
							}
							else if (val == 3)// 全网通
							{
								freqDXCellMap.put(skey, freqCell);
								freqLTCellMap.put(skey, freqCell);
							}
						}
						freqCell.dealSample(sample, StaticConfig.Int_Abnormal, StaticConfig.Int_Abnormal);
					}
				}
			}
		}

		// 小区栅格，栅格只算筛选的数据 全量统计专用
		private void statGridForAll(DT_Sample_4G sample)
		{
			if (sample.Eci > 0 && sample.ilongitude > 0)
			{
				{// 小区栅格统计
					CellGridData_4G cellGridData = cellGridDataMap.get(sample.Eci);
					if (cellGridData == null)
					{
						cellGridData = new CellGridData_4G(sample.cityID, sample.iLAC, sample.Eci, statTime, statTime + 86400);
						cellGridDataMap.put(sample.Eci, cellGridData);
					}
					cellGridData.dealSample(sample);
				}
				{// 小区10*10栅格统计
					CellGridData_4G cellGridData = ten_cellGridDataMap.get(sample.Eci);
					if (cellGridData == null)
					{
						cellGridData = new CellGridData_4G(sample.cityID, sample.iLAC, sample.Eci, statTime, statTime + 86400);
						ten_cellGridDataMap.put(sample.Eci, cellGridData);
					}
					cellGridData.deal_ten_Sample(sample);
				}

			}

			if (sample.ilongitude > 0 && sample.ilatitude > 0)
			{
				{// 栅格统计
					GridItem gridItem = GridItem.GetGridItem(sample.cityID, sample.ilongitude, sample.ilatitude);
					GridData_4G gridData = gridDataMap.get(gridItem);
					if (gridData == null)
					{
						gridData = new GridData_4G(statTime, statTime + 86400);

						Stat_Grid_4G lteGrid = gridData.getLteGrid();
						lteGrid.icityid = sample.cityID;
						lteGrid.itllongitude = gridItem.getTLLongitude();
						lteGrid.itllatitude = gridItem.getTLLatitude();
						lteGrid.ibrlongitude = gridItem.getBRLongitude();
						lteGrid.ibrlatitude = gridItem.getBRLatitude();
						lteGrid.startTime = statTime;
						lteGrid.endTime = statTime + 86400;

						gridDataMap.put(gridItem, gridData);
					}
					gridData.dealSample(sample);
				}
				{// 10*10栅格统计
					GridItem gridItem = GridItem.Get_ten_GridItem(sample.cityID, sample.ilongitude, sample.ilatitude);
					GridData_4G gridData = ten_gridDataMap.get(gridItem);
					if (gridData == null)
					{
						gridData = new GridData_4G(statTime, statTime + 86400);

						Stat_Grid_4G lteGrid = gridData.getLteGrid();
						lteGrid.icityid = sample.cityID;
						lteGrid.itllongitude = gridItem.getTLLongitude();
						lteGrid.itllatitude = gridItem.getTLLatitude();
						lteGrid.ibrlongitude = gridItem.getBRLongitude();
						lteGrid.ibrlatitude = gridItem.getBRLatitude();
						lteGrid.startTime = statTime;
						lteGrid.endTime = statTime + 86400;

						ten_gridDataMap.put(gridItem, gridData);
					}
					gridData.dealSample(sample);
				}
			}
		}

		// 小区栅格，栅格只算筛选的数据 dt、cqt专用
		private void statGrid(DT_Sample_4G sample)
		{
			statGridForAll(sample);
			// 异频统计
			// 只统计mro，mre数据
			if (sample.flag.toUpperCase().equals("MRO") || sample.flag.toUpperCase().equals("MRE"))
			{
				// 栅格统计
				if (sample.ilongitude > 0 && sample.ilatitude > 0)
				{
					List<NC_LTE> itemList = sample.getNclte_Freq();
					int val = ImeiConfig.GetInstance().getValue(sample);
					if (itemList.size() > 0)
					{
						for (NC_LTE nc_LTE : itemList)
						{
							{
								Map<GridItem, GridData_Freq> freqDataMap = gridDataFreqMap.get(nc_LTE.LteNcEarfcn);
								if (freqDataMap == null)
								{
									freqDataMap = new HashMap<GridItem, GridData_Freq>();
									gridDataFreqMap.put(nc_LTE.LteNcEarfcn, freqDataMap);
								}

								GridItem gridItem = GridItem.GetGridItem(sample.cityID, sample.ilongitude, sample.ilatitude);
								GridData_Freq gridData = freqDataMap.get(gridItem);
								if (gridData == null)
								{
									gridData = new GridData_Freq(statTime, statTime + 86400, nc_LTE.LteNcEarfcn);

									Stat_Grid_Freq_4G lteGrid = gridData.getLteGrid();
									lteGrid.icityid = sample.cityID;
									lteGrid.itllongitude = gridItem.getTLLongitude();
									lteGrid.itllatitude = gridItem.getTLLatitude();
									lteGrid.ibrlongitude = gridItem.getBRLongitude();
									lteGrid.ibrlatitude = gridItem.getBRLatitude();
									lteGrid.startTime = statTime;
									lteGrid.endTime = statTime + 86400;

									freqDataMap.put(gridItem, gridData);
								}
								gridData.dealSample(sample, nc_LTE.LteNcRSRP, nc_LTE.LteNcRSRQ);
							}
							{// 10*10异频栅格统计
								Map<GridItem, GridData_Freq> freqDataMap = ten_gridDataFreqMap.get(nc_LTE.LteNcEarfcn);
								if (freqDataMap == null)
								{
									freqDataMap = new HashMap<GridItem, GridData_Freq>();
									ten_gridDataFreqMap.put(nc_LTE.LteNcEarfcn, freqDataMap);
								}

								GridItem gridItem = GridItem.Get_ten_GridItem(sample.cityID, sample.ilongitude, sample.ilatitude);
								GridData_Freq gridData = freqDataMap.get(gridItem);
								if (gridData == null)
								{
									gridData = new GridData_Freq(statTime, statTime + 86400, nc_LTE.LteNcEarfcn);

									Stat_Grid_Freq_4G lteGrid = gridData.getLteGrid();
									lteGrid.icityid = sample.cityID;
									lteGrid.itllongitude = gridItem.getTLLongitude();
									lteGrid.itllatitude = gridItem.getTLLatitude();
									lteGrid.ibrlongitude = gridItem.getBRLongitude();
									lteGrid.ibrlatitude = gridItem.getBRLatitude();
									lteGrid.startTime = statTime;
									lteGrid.endTime = statTime + 86400;

									freqDataMap.put(gridItem, gridData);
								}
								gridData.dealSample(sample, nc_LTE.LteNcRSRP, nc_LTE.LteNcRSRQ);
							}

							{// new freqGrid 170518
								String sKey = sample.cityID + "_" + (sample.ilongitude / 1000) * 1000 + "_" + (sample.ilatitude / 900) * 900 + 900 + "_" + nc_LTE.LteNcEarfcn;
								StatFreqGrid freqGrid = ten_LTfreqGridMap.get(sKey);
								if (freqGrid == null)
								{
									freqGrid = new StatFreqGrid(sample.cityID, statTime, statTime + 86400, nc_LTE.LteNcEarfcn, (sample.ilongitude / 1000) * 1000, (sample.ilatitude / 900) * 900 + 900,
											(sample.ilongitude / 1000) * 1000 + 1000, (sample.ilatitude / 900) * 900);
									if (ifLtFcn(nc_LTE.LteNcEarfcn))
									{
										ten_LTfreqGridMap.put(sKey, freqGrid);
									}
									else if (ifDxFcn(nc_LTE.LteNcEarfcn))
									{
										ten_DXfreqGridMap.put(sKey, freqGrid);
									}
								}
								freqGrid.dealSample(sample, nc_LTE.LteNcRSRP, nc_LTE.LteNcRSRQ);
							}
						}
					}
					else if (val != 0)
					{
						{// new freqGrid 170518
							String sKey = sample.cityID + "_" + (sample.ilongitude / 1000) * 1000 + "_" + (sample.ilatitude / 900) * 900 + 900 + "_" + 0;
							StatFreqGrid freqGrid = ten_LTfreqGridMap.get(sKey);
							if (freqGrid == null)
							{
								freqGrid = new StatFreqGrid(sample.cityID, statTime, statTime + 86400, 0, (sample.ilongitude / 1000) * 1000, (sample.ilatitude / 900) * 900 + 900,
										(sample.ilongitude / 1000) * 1000 + 1000, (sample.ilatitude / 900) * 900);
								if (val == 1)
								{
									ten_LTfreqGridMap.put(sKey, freqGrid);
								}
								else if (val == 2)
								{
									ten_DXfreqGridMap.put(sKey, freqGrid);
								}
								else if (val == 3)
								{
									ten_LTfreqGridMap.put(sKey, freqGrid);
									ten_DXfreqGridMap.put(sKey, freqGrid);
								}
							}
							freqGrid.dealSample(sample, StaticConfig.Int_Abnormal, StaticConfig.Int_Abnormal);
						}
					}
				}
			}
		}
	}

}
