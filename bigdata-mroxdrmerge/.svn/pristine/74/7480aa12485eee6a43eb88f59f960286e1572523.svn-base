package cn.mastercom.bigdata.evt.locall.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;

public class EventInCellGrid_mergeDo implements IMergeDataDo,Serializable
{
	private int dataType = 0;
	public Stat_Event_InCellGrid inCellGrid = new Stat_Event_InCellGrid();
	private StringBuffer sbTemp = new StringBuffer();
	
	@Override
	public String getMapKey()
	{
		// TODO Auto-generated method stub
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(inCellGrid.iECI);
		sbTemp.append("_");
		sbTemp.append(inCellGrid.iTLlongitude);
		sbTemp.append("_");
		sbTemp.append(inCellGrid.iTLlatitude);
		sbTemp.append("_");
		sbTemp.append(inCellGrid.iBuildingID);
		sbTemp.append("_");
		sbTemp.append(inCellGrid.iHeight);
		sbTemp.append("_");
		sbTemp.append(inCellGrid.iInterface);
		sbTemp.append("_");
		sbTemp.append(inCellGrid.kpiSet);
		return sbTemp.toString();
	}

	@Override
	public int getDataType()
	{
		// TODO Auto-generated method stub
		return dataType;
	}

	@Override
	public int setDataType(int dataType)
	{
		// TODO Auto-generated method stub
		this.dataType = dataType;
		return 0;
	}

	@Override
	public boolean mergeData(Object o)
	{
		// TODO Auto-generated method stub
		EventInCellGrid_mergeDo data = (EventInCellGrid_mergeDo) o;
		for (int i = 0; i < data.inCellGrid.fvalue.length; i++)
		{
			if(data.inCellGrid.fvalue[i] > 0)
			{
				if(inCellGrid.fvalue[i] < 0){
					inCellGrid.fvalue[i] = data.inCellGrid.fvalue[i];
				}else{
					inCellGrid.fvalue[i] += data.inCellGrid.fvalue[i];
				}
			}
		}
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		// TODO Auto-generated method stub
		inCellGrid = Stat_Event_InCellGrid.fillData(vals, 0);
		return true;
	}

	@Override
	public String getData()
	{
		// TODO Auto-generated method stub
		return inCellGrid.toString();
	}

}
