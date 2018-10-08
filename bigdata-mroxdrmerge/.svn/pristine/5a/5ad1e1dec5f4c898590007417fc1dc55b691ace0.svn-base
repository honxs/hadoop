package cn.mastercom.bigdata.evt.locall.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;

public class EventBuildPos_mergeDo implements IMergeDataDo,Serializable
{
	private int dataType = 0;
	public Stat_Event_BuildPos buildPos = new Stat_Event_BuildPos();
	private StringBuffer sbTemp = new StringBuffer();
	@Override
	public String getMapKey()
	{
		// TODO Auto-generated method stub
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(buildPos.iBuildingID);
		sbTemp.append("_");
		sbTemp.append(buildPos.iHeight);
		sbTemp.append("_");
		sbTemp.append(buildPos.position);
		sbTemp.append("_");
		sbTemp.append(buildPos.iInterface);
		sbTemp.append("_");
		sbTemp.append(buildPos.kpiSet);
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
		EventBuildPos_mergeDo data = (EventBuildPos_mergeDo) o;
		for (int i = 0; i < data.buildPos.fvalue.length; i++)
		{
			if(data.buildPos.fvalue[i] > 0)
			{
				if(buildPos.fvalue[i] < 0){
					buildPos.fvalue[i] = data.buildPos.fvalue[i];
				}else{
					buildPos.fvalue[i] += data.buildPos.fvalue[i];
				}
			}
		}
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		// TODO Auto-generated method stub
		buildPos = Stat_Event_BuildPos.fillData(vals, 0);
		return true;
	}

	@Override
	public String getData()
	{
		// TODO Auto-generated method stub
		return buildPos.toString();
	}

}
