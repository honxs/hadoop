package cn.mastercom.bigdata.evt.locall.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;

public class EventOutGrid_mergeDo implements IMergeDataDo,Serializable
{
	private int dataType = 0;
	public Stat_Event_OutGrid outGrid = new Stat_Event_OutGrid();
	private StringBuffer sbTemp = new StringBuffer();
	
	@Override
	public String getMapKey()
	{
		// TODO Auto-generated method stub
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(outGrid.iTLlongitude);
		sbTemp.append("_");
		sbTemp.append(outGrid.iTLlatitude);
		sbTemp.append("_");
		sbTemp.append(outGrid.iInterface);
		sbTemp.append("_");
		sbTemp.append(outGrid.kpiSet);
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
		EventOutGrid_mergeDo data = (EventOutGrid_mergeDo) o;
		for (int i = 0; i < data.outGrid.fvalue.length; i++)
		{
			if(data.outGrid.fvalue[i] > 0)
			{
				if(outGrid.fvalue[i] < 0){
					outGrid.fvalue[i] = data.outGrid.fvalue[i];
				}else{
					outGrid.fvalue[i] += data.outGrid.fvalue[i];
				}
			}
		}
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		// TODO Auto-generated method stub
		outGrid = Stat_Event_OutGrid.fillData(vals, 0);
		return true;
	}

	@Override
	public String getData()
	{
		// TODO Auto-generated method stub
		return outGrid.toString();
	}

}
