package cn.mastercom.bigdata.xdr.loc;

import java.util.HashMap;
import java.util.Map;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.GridItem;
import cn.mastercom.bigdata.StructData.Stat_CellGrid_4G;
import cn.mastercom.bigdata.util.LteStatHelper;
import cn.mastercom.bigdata.util.data.MyInt;

public class CellGridData_4G
{
	private int cityID;
	private int lac;
	private long eci;
	private int startTime;
	private int endTime;

	private Map<GridItem, Stat_CellGrid_4G> gridDataMap;

	public CellGridData_4G(int cityID, int lac, long eci, int startTime, int endTime)
	{
		this.cityID = cityID;
		this.lac = lac;
		this.eci = eci;
		this.startTime = startTime;
		this.endTime = endTime;

		gridDataMap = new HashMap<GridItem, Stat_CellGrid_4G>();

	}

	public Map<GridItem, Stat_CellGrid_4G> getGridDataMap()
	{
		return gridDataMap;
	}

	public void dealSample(DT_Sample_4G sample)
	{
		// 小区栅格统计
		if (sample.ilongitude > 0 && sample.ilatitude > 0)
		{
			GridItem gridItem = GridItem.GetGridItem(sample.cityID, sample.ilongitude, sample.ilatitude);
			Stat_CellGrid_4G lteGrid = gridDataMap.get(gridItem);
			if (lteGrid == null)
			{
				lteGrid = new Stat_CellGrid_4G();

				lteGrid.icityid = sample.cityID;
				lteGrid.iLac = sample.iLAC;
				lteGrid.iCi = (long) sample.Eci;
				lteGrid.itllongitude = gridItem.getTLLongitude();
				lteGrid.itllatitude = gridItem.getTLLatitude();
				lteGrid.ibrlongitude = gridItem.getBRLongitude();
				lteGrid.ibrlatitude = gridItem.getBRLatitude();
				lteGrid.startTime = startTime;
				lteGrid.endTime = endTime;

				gridDataMap.put(gridItem, lteGrid);
			}

			boolean isMroSample = sample.flag.toUpperCase().equals("MRO");
			boolean isMreSample = sample.flag.toUpperCase().equals("MRE");

			lteGrid.isamplenum++;
			if (isMroSample || isMreSample)
			{
				lteGrid.MrCount++;
				LteStatHelper.statMro(sample, lteGrid.tStat);
			}
			else
			{
				lteGrid.XdrCount++;
				lteGrid.iduration += sample.duration;
				LteStatHelper.statEvt(sample, lteGrid.tStat);

				// 只有xdr，才算用户的个数，mr不用算
				if (sample.IMSI > 0)
				{
					MyInt item = lteGrid.imsiMap.get(sample.IMSI);
					if (item == null)
					{
						item = new MyInt(0);
						lteGrid.imsiMap.put(sample.IMSI, item);
					}
					item.data++;
				}
			}
			dealOverla(sample, lteGrid);
		}
	}

	/**
	 * 重叠覆盖
	 * 
	 * @param sample
	 * @param lteGrid
	 */
	public void dealOverla(DT_Sample_4G sample, Stat_CellGrid_4G lteGrid)
	{
		lteGrid.overlapden++;
		if (sample.sfcnJamCellCount > 3)
		{
			lteGrid.overlapnum++;
		}
	}

	public void deal_ten_Sample(DT_Sample_4G sample)
	{
		// 小区栅格统计
		if (sample.ilongitude > 0 && sample.ilatitude > 0)
		{
			GridItem gridItem = GridItem.Get_ten_GridItem(sample.cityID, sample.ilongitude, sample.ilatitude);
			Stat_CellGrid_4G lteGrid = gridDataMap.get(gridItem);
			if (lteGrid == null)
			{
				lteGrid = new Stat_CellGrid_4G();

				lteGrid.icityid = sample.cityID;
				lteGrid.iLac = sample.iLAC;
				lteGrid.iCi = (int) sample.Eci;
				lteGrid.itllongitude = gridItem.getTLLongitude();
				lteGrid.itllatitude = gridItem.getTLLatitude();
				lteGrid.ibrlongitude = gridItem.getBRLongitude();
				lteGrid.ibrlatitude = gridItem.getBRLatitude();
				lteGrid.startTime = startTime;
				lteGrid.endTime = endTime;

				gridDataMap.put(gridItem, lteGrid);
			}

			boolean isMroSample = sample.flag.toUpperCase().equals("MRO");
			boolean isMreSample = sample.flag.toUpperCase().equals("MRE");

			lteGrid.isamplenum++;
			if (isMroSample || isMreSample)
			{
				lteGrid.MrCount++;
				LteStatHelper.statMro(sample, lteGrid.tStat);
			}
			else
			{
				lteGrid.XdrCount++;
				lteGrid.iduration += sample.duration;
				LteStatHelper.statEvt(sample, lteGrid.tStat);

				// 只有xdr，才算用户的个数，mr不用算
				if (sample.IMSI > 0)
				{
					MyInt item = lteGrid.imsiMap.get(sample.IMSI);
					if (item == null)
					{
						item = new MyInt(0);
						lteGrid.imsiMap.put(sample.IMSI, item);
					}
					item.data++;
				}
			}
			dealOverla(sample, lteGrid);
		}
	}

	public void finalDeal()
	{
		for (Stat_CellGrid_4G item : gridDataMap.values())
		{
			item.UserCount_4G = item.imsiMap.size();
		}
	}

}
