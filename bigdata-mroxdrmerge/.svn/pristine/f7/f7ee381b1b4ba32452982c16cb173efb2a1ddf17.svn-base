package cn.mastercom.bigdata.evt.locall.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;

public class EventAreaCell_mergeDo implements IMergeDataDo,Serializable
{
	private int dataType = 0;
	public Stat_Event_Area_Cell areaCell = new Stat_Event_Area_Cell();
	private StringBuffer sbTemp = new StringBuffer();
	
	@Override
	public String getMapKey()
	{
		// TODO Auto-generated method stub
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(areaCell.iCityID);
		sbTemp.append("_");
		sbTemp.append(areaCell.iAreatype);
		sbTemp.append("_");
		sbTemp.append(areaCell.iAreaID);
		sbTemp.append("_");
		sbTemp.append(areaCell.iECI);
		sbTemp.append("_");
		sbTemp.append(areaCell.iTime);
		sbTemp.append("_");
		sbTemp.append(areaCell.iInterface);
		sbTemp.append("_");
		sbTemp.append(areaCell.kpiSet);
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
		EventAreaCell_mergeDo data = (EventAreaCell_mergeDo) o;
		for (int i = 0; i < data.areaCell.fvalue.length; i++)
		{
			if(data.areaCell.fvalue[i] > 0)
			{
				if(areaCell.fvalue[i] < 0){
					areaCell.fvalue[i] = data.areaCell.fvalue[i];
				}else{
					areaCell.fvalue[i] += data.areaCell.fvalue[i];
				}
			}
		}
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		// TODO Auto-generated method stub
		areaCell = Stat_Event_Area_Cell.fillData(vals, 0);
		return true;
	}

	@Override
	public String getData()
	{
		// TODO Auto-generated method stub
		return areaCell.toString();
	}

}
