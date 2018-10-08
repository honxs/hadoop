package cn.mastercom.bigdata.evt.locall.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;

public class EventAreaCellGrid_mergeDo implements IMergeDataDo,Serializable
{
	private int dataType = 0;
	public Stat_Event_Area_CellGrid areaCellGrid = new Stat_Event_Area_CellGrid();
	private StringBuffer sbTemp = new StringBuffer();
	
	@Override
	public String getMapKey()
	{
		// TODO Auto-generated method stub
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(areaCellGrid.iCityID);
		sbTemp.append("_");
		sbTemp.append(areaCellGrid.iAreatype);
		sbTemp.append("_");
		sbTemp.append(areaCellGrid.iAreaID);
		sbTemp.append("_");
		sbTemp.append(areaCellGrid.iTLlongitude);
		sbTemp.append("_");
		sbTemp.append(areaCellGrid.iTLlatitude);
		sbTemp.append("_");
		sbTemp.append(areaCellGrid.iECI);
		sbTemp.append("_");
		sbTemp.append(areaCellGrid.iTime);
		sbTemp.append("_");
		sbTemp.append(areaCellGrid.iInterface);
		sbTemp.append("_");
		sbTemp.append(areaCellGrid.kpiSet);
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
		EventAreaCellGrid_mergeDo data = (EventAreaCellGrid_mergeDo) o;
		for (int i = 0; i < data.areaCellGrid.fvalue.length; i++)
		{
			if(data.areaCellGrid.fvalue[i] > 0)
			{
				if(areaCellGrid.fvalue[i] < 0){
					areaCellGrid.fvalue[i] = data.areaCellGrid.fvalue[i];
				}else{
					areaCellGrid.fvalue[i] += data.areaCellGrid.fvalue[i];
				}
			}
		}
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		// TODO Auto-generated method stub
		areaCellGrid = Stat_Event_Area_CellGrid.fillData(vals, 0);
		return true;
	}

	@Override
	public String getData()
	{
		// TODO Auto-generated method stub
		return areaCellGrid.toString();
	}

}
