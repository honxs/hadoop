package cn.mastercom.bigdata.xdr.loc;

import java.util.HashMap;
import java.util.Map;

import cn.mastercom.bigdata.StructData.DT_Sample_23G;
import cn.mastercom.bigdata.StructData.GridItem;
import cn.mastercom.bigdata.StructData.Stat_CellGrid_23G;
import cn.mastercom.bigdata.util.data.MyInt;

public class CellGridData_23G
{

	private int cityID;
	private int lac;
	private long ci;
	private int startTime;
	private int endTime;
	
	private Map<GridItem, Stat_CellGrid_23G> gridCellGridMap;

	public CellGridData_23G(int cityID, int lac, long ci, int startTime, int endTime)
	{
		this.cityID = cityID;
		this.lac = lac;
		this.ci = ci;
		this.startTime = startTime;
		this.endTime = endTime;
		
		gridCellGridMap = new HashMap<GridItem, Stat_CellGrid_23G>();
	}

	public int getLac()
	{
		return lac;
	}

	public long getCi()
	{
		return ci;
	}
	
	public Map<GridItem, Stat_CellGrid_23G> getGridCellGridMap()
	{
		return gridCellGridMap;
	}

	public void dealSample(DT_Sample_23G sample)
	{		
		//小区栅格统计
		if (sample.ilongitude > 0 && sample.ilatitude > 0)
		{
			GridItem grid = GridItem.GetGridItem(sample.cityID, sample.ilongitude, sample.ilatitude);
			Stat_CellGrid_23G gridItem = gridCellGridMap.get(grid);
			if (gridItem == null)
			{
				gridItem = new Stat_CellGrid_23G();
				
				gridItem.icityid = sample.cityID;
				gridItem.iLac = sample.iLAC;
				gridItem.iCi = (int)sample.iCI;
				gridItem.itllongitude = grid.getTLLongitude();
				gridItem.itllatitude = grid.getTLLatitude();
				gridItem.ibrlongitude = grid.getBRLongitude();
				gridItem.ibrlatitude = grid.getBRLatitude();
				gridItem.startTime = startTime;
				gridItem.endTime = endTime;
				
				gridCellGridMap.put(grid, gridItem);
			}

			gridItem.XdrCount++;
			
			MyInt myInt = null;
			
			myInt = gridItem.imsiMap.get(sample.IMSI);
			if(myInt == null)
			{
				myInt = new MyInt(0);
				gridItem.imsiMap.put(sample.IMSI, myInt);
			}
			myInt.data++;
			
			if(sample.nettypeFB == 1)//gsm
			{
				myInt = gridItem.imsiMap_2G.get(sample.IMSI);
				if(myInt == null)
				{
					myInt = new MyInt(0);
					gridItem.imsiMap_2G.put(sample.IMSI, myInt);
				}
				myInt.data++;
			}
			
			if(sample.nettypeFB == 2)//tdscdma
			{
				myInt = gridItem.imsiMap_3G.get(sample.IMSI);
				if(myInt == null)
				{
					myInt = new MyInt(0);
					gridItem.imsiMap_3G.put(sample.IMSI, myInt);
				}
				myInt.data++;
			}

		}	

	}

	public void finalDeal()
	{
		for (Stat_CellGrid_23G item : gridCellGridMap.values())
		{
			item.UserCount = item.imsiMap.size();
			item.UserCount_2G = item.imsiMap_2G.size();
			item.UserCount_3G = item.imsiMap_3G.size();
		}
	}

	
}
