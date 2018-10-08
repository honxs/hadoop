package cn.mastercom.bigdata.evt.locall.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;

public class EventCell_mergeDo implements IMergeDataDo,Serializable
{
	private int dataType = 0;
	public Stat_Event_Cell cell = new Stat_Event_Cell();
	private StringBuffer sbTemp = new StringBuffer();
	
	@Override
	public String getMapKey()
	{
		// TODO Auto-generated method stub
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(cell.iECI);
		sbTemp.append("_");
		sbTemp.append(cell.iInterface);
		sbTemp.append("_");
		sbTemp.append(cell.kpiSet);
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
		EventCell_mergeDo data = (EventCell_mergeDo) o;
		for (int i = 0; i < data.cell.fvalue.length; i++)
		{
			if(data.cell.fvalue[i] > 0)
			{
				if(cell.fvalue[i] < 0){
					cell.fvalue[i] = data.cell.fvalue[i];
				}else{
					cell.fvalue[i] += data.cell.fvalue[i];
				}
			}
		}
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		// TODO Auto-generated method stub
		cell = Stat_Event_Cell.fillData(vals, 0);
		return true;
	}

	@Override
	public String getData()
	{
		// TODO Auto-generated method stub
		return cell.toString();
	}

}
