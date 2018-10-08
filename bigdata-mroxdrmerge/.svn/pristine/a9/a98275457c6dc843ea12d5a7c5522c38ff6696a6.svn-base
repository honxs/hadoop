package cn.mastercom.bigdata.xdr.loc;

import cn.mastercom.bigdata.StructData.DT_Sample_23G;
import cn.mastercom.bigdata.StructData.Stat_Grid_23G;
import cn.mastercom.bigdata.util.data.MyInt;

public class GridData_23G
{

	private int startTime;
	private int endTime;
	private Stat_Grid_23G gridItem;

	public GridData_23G(int startTime, int endTime)
	{
		this.startTime = startTime;
		this.endTime = endTime;
		gridItem = new Stat_Grid_23G();
	}

	public Stat_Grid_23G getGridItem()
	{
		return gridItem;
	}

	public int getStartTime()
	{
		return startTime;
	}

	public int getEndTime()
	{
		return endTime;
	}

	public void dealSample(DT_Sample_23G sample)
	{		
		if (sample.IMSI == 0)
		{
			return;
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

	public void finalDeal()
	{
		gridItem.UserCount = gridItem.imsiMap.size();
		gridItem.UserCount_2G = gridItem.imsiMap_2G.size();
		gridItem.UserCount_3G = gridItem.imsiMap_3G.size();
	}

}
