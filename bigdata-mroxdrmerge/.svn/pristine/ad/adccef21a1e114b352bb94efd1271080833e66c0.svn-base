package cn.mastercom.bigdata.evt.locall.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;

public class EventBuildGrid_mergeDo implements IMergeDataDo,Serializable
{
	private int dataType = 0;
	public Stat_Event_BuildGrid buildGrid = new Stat_Event_BuildGrid();
	private StringBuffer sbTemp = new StringBuffer();
	@Override
	public String getMapKey()
	{
		// TODO Auto-generated method stub
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(buildGrid.iBuildingID);
		sbTemp.append("_");
		sbTemp.append(buildGrid.iHeight);
		sbTemp.append("_");
		sbTemp.append(buildGrid.iInterface);
		sbTemp.append("_");
		sbTemp.append(buildGrid.kpiSet);
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
		EventBuildGrid_mergeDo data = (EventBuildGrid_mergeDo) o;
		for (int i = 0; i < data.buildGrid.fvalue.length; i++)
		{
			if(data.buildGrid.fvalue[i] > 0)
			{
				if(buildGrid.fvalue[i] < 0){
					buildGrid.fvalue[i] = data.buildGrid.fvalue[i];
				}else{
					buildGrid.fvalue[i] += data.buildGrid.fvalue[i];
				}
			}
		}
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		// TODO Auto-generated method stub
		buildGrid = Stat_Event_BuildGrid.fillData(vals, 0);
		return true;
	}

	@Override
	public String getData()
	{
		// TODO Auto-generated method stub
		return buildGrid.toString();
	}

}
