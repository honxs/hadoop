package cn.mastercom.bigdata.evt.locall.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;

public class EventBuildCellPos_mergeDo implements IMergeDataDo,Serializable
{
	private int dataType = 0;
	public Stat_Event_BuildCellPos buildCellPos = new Stat_Event_BuildCellPos();
	private StringBuffer sbTemp = new StringBuffer();
	
	@Override
	public String getMapKey()
	{
		// TODO Auto-generated method stub
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(buildCellPos.iECI);
		sbTemp.append("_");
		sbTemp.append(buildCellPos.iBuildingID);
		sbTemp.append("_");
		sbTemp.append(buildCellPos.iHeight);
		sbTemp.append("_");
		sbTemp.append(buildCellPos.position);
		sbTemp.append("_");
		sbTemp.append(buildCellPos.iInterface);
		sbTemp.append("_");
		sbTemp.append(buildCellPos.kpiSet);
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
		EventBuildCellPos_mergeDo data = (EventBuildCellPos_mergeDo) o;
		for (int i = 0; i < data.buildCellPos.fvalue.length; i++)
		{
			if(data.buildCellPos.fvalue[i] > 0)
			{
				if(buildCellPos.fvalue[i] < 0){
					buildCellPos.fvalue[i] = data.buildCellPos.fvalue[i];
				}else{
					buildCellPos.fvalue[i] += data.buildCellPos.fvalue[i];
				}
			}
		}
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		// TODO Auto-generated method stub
		buildCellPos = Stat_Event_BuildCellPos.fillData(vals, 0);
		return true;
	}

	@Override
	public String getData()
	{
		// TODO Auto-generated method stub
		return buildCellPos.toString();
	}

}
