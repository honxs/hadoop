package cn.mastercom.bigdata.xdr.loc;

import java.io.Serializable;

import cn.mastercom.bigdata.StructData.Stat_UserGrid_4G;
import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;

public class UserGridMergeDataDo_4G implements IMergeDataDo,Serializable
{
	private int dataType = 0;
	public Stat_UserGrid_4G statItem = new Stat_UserGrid_4G();

	private StringBuffer sbTemp = new StringBuffer();

	@Override
	public String getMapKey()
	{
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(statItem.icityid);sbTemp.append("_");
		sbTemp.append(statItem.itllongitude);sbTemp.append("_");
		sbTemp.append(statItem.itllatitude);sbTemp.append("_");
		sbTemp.append(statItem.startTime);
		return sbTemp.toString();
	}

	@Override
	public int getDataType()
	{
		return dataType;
	}
	
	@Override
	public int setDataType(int dataType)
	{
		this.dataType = dataType;
		return 0;
	}

	@Override
	public boolean mergeData(Object o)
	{
		UserGridMergeDataDo_4G tmpItem = (UserGridMergeDataDo_4G)o;
		if(tmpItem == null)
		{
			return false;
		}
		
		statItem.userCount_4G += tmpItem.statItem.userCount_4G;
		
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		sPos = 0;
		statItem = Stat_UserGrid_4G.FillData(vals, 0);
		
		return true;
	}

	@Override
	public String getData()
	{	
		return ResultHelper.getPutUserGridInfo(statItem);
	}

	
	
	
}
