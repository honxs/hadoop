package cn.mastercom.bigdata.evt.locall.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;

public class EventInGrid_mergeDo implements IMergeDataDo,Serializable
{
	private int dataType = 0;
	public Stat_Event_InGrid inGrid = new Stat_Event_InGrid();
	private StringBuffer sbTemp = new StringBuffer();
	@Override
	public String getMapKey()
	{
		// TODO Auto-generated method stub
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(inGrid.iBuildingID);
		sbTemp.append("_");
		sbTemp.append(inGrid.iHeight);
		sbTemp.append("_");
		sbTemp.append(inGrid.iTLlongitude);
		sbTemp.append("_");
		sbTemp.append(inGrid.iTLlatitude);
		sbTemp.append("_");
		sbTemp.append(inGrid.iInterface);
		sbTemp.append("_");
		sbTemp.append(inGrid.kpiSet);
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
		EventInGrid_mergeDo data = (EventInGrid_mergeDo) o;
		for (int i = 0; i < data.inGrid.fvalue.length; i++)
		{
			if(data.inGrid.fvalue[i] > 0)
			{
				if(inGrid.fvalue[i] < 0){
					inGrid.fvalue[i] = data.inGrid.fvalue[i];
				}else{
					inGrid.fvalue[i] += data.inGrid.fvalue[i];
				}
			}
		}
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		// TODO Auto-generated method stub
		inGrid = Stat_Event_InGrid.fillData(vals, 0);
		return true;
	}

	@Override
	public String getData()
	{
		// TODO Auto-generated method stub
		return inGrid.toString();
	}

}
