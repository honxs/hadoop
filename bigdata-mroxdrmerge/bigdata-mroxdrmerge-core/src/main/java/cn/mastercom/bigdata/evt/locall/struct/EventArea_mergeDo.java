package cn.mastercom.bigdata.evt.locall.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;

public class EventArea_mergeDo implements IMergeDataDo,Serializable
{
	private int dataType = 0;
	public Stat_Event_Area area = new Stat_Event_Area();
	private StringBuffer sbTemp = new StringBuffer();
	
	@Override
	public String getMapKey()
	{
		// TODO Auto-generated method stub
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(area.iCityID);
		sbTemp.append("_");
		sbTemp.append(area.iAreatype);
		sbTemp.append("_");
		sbTemp.append(area.iAreaID);
		sbTemp.append("_");
		sbTemp.append(area.iTime);
		sbTemp.append("_");
		sbTemp.append(area.iInterface);
		sbTemp.append("_");
		sbTemp.append(area.kpiSet);
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
		EventArea_mergeDo data = (EventArea_mergeDo) o;
		for (int i = 0; i < data.area.fvalue.length; i++)
		{
			if(data.area.fvalue[i] > 0)
			{
				if(area.fvalue[i] < 0){
					area.fvalue[i] = data.area.fvalue[i];
				}else{
					area.fvalue[i] += data.area.fvalue[i];
				}
			}
		}
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		// TODO Auto-generated method stub
		area = Stat_Event_Area.fillData(vals, 0);
		return true;
	}

	@Override
	public String getData()
	{
		// TODO Auto-generated method stub
		return area.toString();
	}

}
