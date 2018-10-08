package cn.mastercom.bigdata.mro.loc;

import java.io.Serializable;

import cn.mastercom.bigdata.StructData.Stat_UserAct_Cell;
import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;
import cn.mastercom.bigdata.xdr.loc.ResultHelper;

public class UserActCellMergeDataDo implements IMergeDataDo,Serializable
{
	private int dataType = 0;
	public Stat_UserAct_Cell statItem = new Stat_UserAct_Cell();

	private StringBuffer sbTemp = new StringBuffer();

	@Override
	public String getMapKey()
	{
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(statItem.icityid);sbTemp.append("_");
		sbTemp.append(statItem.imsi);sbTemp.append("_");
		sbTemp.append(statItem.eci);sbTemp.append("_");
		sbTemp.append(statItem.nbeci);sbTemp.append("_");
		sbTemp.append(statItem.stime);
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
		UserActCellMergeDataDo tmpItem = (UserActCellMergeDataDo)o;
		if(tmpItem == null)
		{
			return false;
		}
		
		if(tmpItem.statItem.rsrpTotal > 0)
		{
			statItem.rsrpSum += tmpItem.statItem.rsrpSum;
			statItem.rsrpTotal += tmpItem.statItem.rsrpTotal;
		}

		if(statItem.rsrpMaxMark >-150 && statItem.rsrpMaxMark < 0)
		{
			if(tmpItem.statItem.rsrpMaxMark >-150 && tmpItem.statItem.rsrpMaxMark < 0)
			{
				statItem.rsrpMaxMark = Math.max(statItem.rsrpMaxMark, tmpItem.statItem.rsrpMaxMark);
			}
		}
		else 
		{
			statItem.rsrpMaxMark = tmpItem.statItem.rsrpMaxMark;
		}
		
		
		if(statItem.rsrpMinMark >-150 && statItem.rsrpMinMark < 0)
		{
			if(tmpItem.statItem.rsrpMinMark >-150 && tmpItem.statItem.rsrpMinMark < 0)
			{
				statItem.rsrpMinMark = Math.min(statItem.rsrpMinMark, tmpItem.statItem.rsrpMinMark);
			}
		}
		else 
		{
			statItem.rsrpMinMark = tmpItem.statItem.rsrpMinMark;
		}
		
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		sPos = 0;
		statItem = Stat_UserAct_Cell.FillData(vals, 0);
		
		return true;
	}

	@Override
	public String getData()
	{	
		return ResultHelper.getPutUserActCell(statItem);
	}

	
	
	
}
