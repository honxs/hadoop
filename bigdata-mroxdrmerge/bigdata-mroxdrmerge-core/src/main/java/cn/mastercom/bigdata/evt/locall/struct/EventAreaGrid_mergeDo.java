package cn.mastercom.bigdata.evt.locall.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;

public class EventAreaGrid_mergeDo implements IMergeDataDo,Serializable
{
	private int dataType = 0;
	public Stat_Event_Area_Grid areaGrid = new Stat_Event_Area_Grid();
	private StringBuffer sbTemp = new StringBuffer();
	
	@Override
	public String getMapKey()
	{
		// TODO Auto-generated method stub
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(areaGrid.iCityID);
		sbTemp.append("_");
		sbTemp.append(areaGrid.iAreatype);
		sbTemp.append("_");
		sbTemp.append(areaGrid.iAreaID);
		sbTemp.append("_");
		sbTemp.append(areaGrid.iTLlongitude);
		sbTemp.append("_");
		sbTemp.append(areaGrid.iTLlatitude);
		sbTemp.append("_");
		sbTemp.append(areaGrid.iTime);
		sbTemp.append("_");
		sbTemp.append(areaGrid.iInterface);
		sbTemp.append("_");
		sbTemp.append(areaGrid.kpiSet);
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
		EventAreaGrid_mergeDo data = (EventAreaGrid_mergeDo) o;
		for (int i = 0; i < data.areaGrid.fvalue.length; i++)
		{
			if(data.areaGrid.fvalue[i] > 0)
			{				
				if(areaGrid.fvalue[i] < 0){
					areaGrid.fvalue[i] = data.areaGrid.fvalue[i];
				}else{
					areaGrid.fvalue[i] += data.areaGrid.fvalue[i];
				}
			}
		}
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		// TODO Auto-generated method stub
		areaGrid = Stat_Event_Area_Grid.fillData(vals, 0);
		return true;
	}

	@Override
	public String getData()
	{
		// TODO Auto-generated method stub
		return areaGrid.toString();
	}

}
