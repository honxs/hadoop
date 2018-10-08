package cn.mastercom.bigdata.xdr.loc;

import java.util.HashMap;
import java.util.Map;

import cn.mastercom.bigdata.StructData.DT_Sample_23G;
import cn.mastercom.bigdata.StructData.GridItem;
import cn.mastercom.bigdata.StructData.Stat_Grid_23G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.conf.cellconfig.CellConfig;
import cn.mastercom.bigdata.util.TimeHelper;

public class DayDataDeal_23G
{

	private Map<Integer, DayDataItem> dayDataDealMap;// time，天统计
	private int curDayTime;
	private int statType;
	
	public DayDataDeal_23G(int statType)
	{
		dayDataDealMap = new HashMap<Integer, DayDataItem>();
		this.statType = statType;
	}
	
	public void dealSample(DT_Sample_23G sample)
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
		dayDataDeal.dealSample(sample);
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

	public int getCellGridCount()
	{
		int gridCount = 0;
		for (DayDataItem item : dayDataDealMap.values())
		{
			for (CellGridData_23G cellGridData : item.getCellGridDataMap().values())
			{
				gridCount += cellGridData.getGridCellGridMap().size();
			}
		}
		return gridCount;
	}
	
	
	public class DayDataItem
	{
		private int statTime;
		private Map<Long, CellGridData_23G> cellGridDataMap;
		private Map<GridItem, GridData_23G> gridDataMap;

		public DayDataItem(int statTime)
		{
			this.statTime = statTime;
			cellGridDataMap = new HashMap<Long, CellGridData_23G>();
			gridDataMap = new HashMap<GridItem, GridData_23G>();
		}

		public Map<Long, CellGridData_23G> getCellGridDataMap()
		{
			return cellGridDataMap;
		}

		public Map<GridItem, GridData_23G> getGridDataMap()
		{
			return gridDataMap;
		}

		public int getStatTime()
		{
			return statTime;
		}

		public void dealSample(DT_Sample_23G sample)
		{
			//小区栅格，栅格只算筛选的数据
			if (sample.testType == StaticConfig.TestType_DT 
					|| sample.testType == StaticConfig.TestType_CQT
					|| sample.testType == StaticConfig.TestType_DT_EX)
			{
			    long cellKey = CellConfig.makeGsmCellKey(sample.iLAC, sample.iCI);
				
				if (cellKey > 0)
				{
					{// 小区栅格统计
						CellGridData_23G cellGridData = cellGridDataMap.get(cellKey);
						if (cellGridData == null)
						{
							cellGridData = new CellGridData_23G(sample.cityID, sample.iLAC, sample.iCI, statTime,
									statTime + 86400);
							cellGridDataMap.put(cellKey, cellGridData);
						}
						cellGridData.dealSample(sample);
					}

				}

				if (sample.ilongitude > 0 && sample.ilatitude > 0)
				{
					GridItem gridItem = GridItem.GetGridItem(sample.cityID, sample.ilongitude, sample.ilatitude);
					GridData_23G gridData = gridDataMap.get(gridItem);
					if (gridData == null)
					{
						gridData = new GridData_23G(statTime, statTime + 86400);

						Stat_Grid_23G lteGrid = gridData.getGridItem();
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
			}

			
		}
	}	
	

	
}
