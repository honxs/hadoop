package cn.mastercom.bigdata.xdr.loc;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.GridItem;
import cn.mastercom.bigdata.StructData.Stat_Grid_4G;
import cn.mastercom.bigdata.StructData.Stat_UserGrid_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;

public class HourDataDeal_4G
{	
	private Map<Integer, HourDataItem> hourDataDealMap;// time，天统计
	private int curHourTime;
	private int statType;
	
	public HourDataDeal_4G(int statType)
	{
		hourDataDealMap = new HashMap<Integer, HourDataItem>();
		this.statType = statType;
	}
	
	public void dealSample(DT_Sample_4G sample)
	{
		if (sample.itime == 0)
		{
			return;
		}

		curHourTime = sample.itime/3600*3600;
		HourDataItem hourDataDeal = hourDataDealMap.get(curHourTime);
		if (hourDataDeal == null)
		{
			hourDataDeal = new HourDataItem(curHourTime);
			hourDataDealMap.put(curHourTime, hourDataDeal);
		}
		hourDataDeal.dealSample(sample);
	}
	
	public void dealXdr(DT_Sample_4G sample)
	{
		if (sample.itime == 0)
		{
			return;
		}

		curHourTime = sample.itime/3600*3600;
		HourDataItem hourDataDeal = hourDataDealMap.get(curHourTime);
		if (hourDataDeal == null)
		{
			hourDataDeal = new HourDataItem(curHourTime);
			hourDataDealMap.put(curHourTime, hourDataDeal);
		}
		hourDataDeal.dealXdr(sample);
	}
	
	
	public Map<Integer, HourDataItem> getHourDataDealMap()
	{
		return hourDataDealMap;
	}

	public int getGridCount()
	{
		int gridCount = 0;
		for (HourDataItem item : hourDataDealMap.values())
		{
			gridCount += item.getGridDataMap().size();
		}
		return gridCount;
	}	
	
	public class HourDataItem
	{
		private int statTime;
		private Map<GridItem, GridData_4G> gridDataMap;
		private Map<GridItem, UserGridStat_4G> userGirdDataMap;
		private Set<Long> userGetLocSet;

		public HourDataItem(int statTime)
		{
			this.statTime = statTime;
			gridDataMap = new HashMap<GridItem, GridData_4G>();
			userGirdDataMap = new HashMap<GridItem, UserGridStat_4G>();
			userGetLocSet = new HashSet<Long>();
		}

		public Map<GridItem, GridData_4G> getGridDataMap()
		{
			return gridDataMap;
		}
		
		public Map<GridItem, UserGridStat_4G> getUserGirdDataMap()
		{
			return userGirdDataMap;
		}

		public int getStatTime()
		{
			return statTime;
		}

		public void dealSample(DT_Sample_4G sample)
		{
			
			//小区栅格，栅格只算筛选的数据
			if (sample.testType == StaticConfig.TestType_DT 
					|| sample.testType == StaticConfig.TestType_CQT
					|| sample.testType == StaticConfig.TestType_DT_EX)
			{
				if (sample.ilongitude > 0 && sample.ilatitude > 0)
				{
					{//统计小区栅格数据
						GridItem gridItem = GridItem.GetGridItem(sample.cityID, sample.ilongitude, sample.ilatitude);
						GridData_4G gridData = gridDataMap.get(gridItem);
						if (gridData == null)
						{
							gridData = new GridData_4G(statTime, statTime + 3600);

							Stat_Grid_4G lteGrid = gridData.getLteGrid();
							lteGrid.icityid = sample.cityID;
							lteGrid.itllongitude = gridItem.getTLLongitude();
							lteGrid.itllatitude = gridItem.getTLLatitude();
							lteGrid.ibrlongitude = gridItem.getBRLongitude();
							lteGrid.ibrlatitude = gridItem.getBRLatitude();
							lteGrid.startTime = statTime;
							lteGrid.endTime = statTime + 3600;

							gridDataMap.put(gridItem, gridData);
						}
						gridData.dealSample(sample);
					}

				}
			}		
			
		}
	
		public void dealXdr(DT_Sample_4G sample)
		{	
			if (sample.ilongitude > 0 && sample.ilatitude > 0)
			{
				if(!userGetLocSet.contains(sample.IMSI))
				{
					GridItem gridItem = GridItem.GetGridItem(sample.cityID, sample.ilongitude, sample.ilatitude);
					UserGridStat_4G userGirdData = userGirdDataMap.get(gridItem);
					if (userGirdData == null)
					{
						userGirdData = new UserGridStat_4G(statTime, statTime + 3600);

						Stat_UserGrid_4G userGrid = userGirdData.getUserGrid();
						userGrid.icityid = sample.cityID;
						userGrid.itllongitude = gridItem.getTLLongitude();
						userGrid.itllatitude = gridItem.getTLLatitude();
						userGrid.ibrlongitude = gridItem.getBRLongitude();
						userGrid.ibrlatitude = gridItem.getBRLatitude();
						userGrid.startTime = statTime;
						userGrid.endTime = statTime + 3600;

						userGirdDataMap.put(gridItem, userGirdData);
					}
					userGirdData.dealSample(sample);
					
					userGetLocSet.add(sample.IMSI);
				}
			}
				
		}
	
	
	}	
	
	
	
}
