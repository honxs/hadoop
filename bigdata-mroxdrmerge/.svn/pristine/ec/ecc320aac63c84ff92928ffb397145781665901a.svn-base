package cn.mastercom.bigdata.evt.locall.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;

public class EventOutCellGrid_mergeDo implements IMergeDataDo,Serializable
{
	private int dataType = 0;
	public Stat_Event_OutCellGrid outCellGrid = new Stat_Event_OutCellGrid();
	private StringBuffer sbTemp = new StringBuffer();
	
	@Override
	public String getMapKey()
	{
		// TODO Auto-generated method stub
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(outCellGrid.iECI);
		sbTemp.append("_");
		sbTemp.append(outCellGrid.iTLlongitude);
		sbTemp.append("_");
		sbTemp.append(outCellGrid.iTLlatitude);
		sbTemp.append("_");
		sbTemp.append(outCellGrid.iInterface);
		sbTemp.append("_");
		sbTemp.append(outCellGrid.kpiSet);
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
		EventOutCellGrid_mergeDo data = (EventOutCellGrid_mergeDo) o;
		for (int i = 0; i < data.outCellGrid.fvalue.length; i++)
		{
			if(data.outCellGrid.fvalue[i] > 0)
			{
				if(outCellGrid.fvalue[i] < 0){
					outCellGrid.fvalue[i] = data.outCellGrid.fvalue[i];
				}else{
					outCellGrid.fvalue[i] += data.outCellGrid.fvalue[i];
				}
			}
		}
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		// TODO Auto-generated method stub
		outCellGrid = Stat_Event_OutCellGrid.fillData(vals, 0);
		return true;
	}

	@Override
	public String getData()
	{
		// TODO Auto-generated method stub
		return outCellGrid.toString();
	}

}
