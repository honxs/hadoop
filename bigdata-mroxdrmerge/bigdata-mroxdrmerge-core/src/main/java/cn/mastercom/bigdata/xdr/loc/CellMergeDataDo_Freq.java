package cn.mastercom.bigdata.xdr.loc;


import java.io.Serializable;

import cn.mastercom.bigdata.StructData.Stat_Cell_Freq;
import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;

public class CellMergeDataDo_Freq implements IMergeDataDo,Serializable
{
	private int dataType = 0;
	public Stat_Cell_Freq statItem = new Stat_Cell_Freq();

	private StringBuffer sbTemp = new StringBuffer();

	@Override
	public String getMapKey()
	{
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(statItem.icityid);sbTemp.append("_");
		sbTemp.append(statItem.iCI);sbTemp.append("_");
		sbTemp.append(statItem.freq);sbTemp.append("_");
		sbTemp.append(statItem.pci);sbTemp.append("_");
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
		CellMergeDataDo_Freq tmpItem = (CellMergeDataDo_Freq)o;
		if(tmpItem == null)
		{
			return false;
		}
		
		statItem.iduration += tmpItem.statItem.iduration;
		statItem.idistance += tmpItem.statItem.idistance;
		statItem.isamplenum += tmpItem.statItem.isamplenum;
		
		// rsrp
		if (tmpItem.statItem.RSRP_nTotal > 0)
		{
			statItem.RSRP_nTotal += tmpItem.statItem.RSRP_nTotal;
			statItem.RSRP_nSum += tmpItem.statItem.RSRP_nSum;
		}

		for (int i = 0; i < statItem.RSRP_nCount.length; i++)
		{
			statItem.RSRP_nCount[i] += tmpItem.statItem.RSRP_nCount[i];
		}

		// rsrq		
		if (tmpItem.statItem.RSRQ_nTotal > 0)
		{
			statItem.RSRQ_nTotal += tmpItem.statItem.RSRQ_nTotal;
			statItem.RSRQ_nSum += tmpItem.statItem.RSRQ_nSum;
		}
		
		for (int i = 0; i < statItem.RSRQ_nCount.length; i++)
		{
			statItem.RSRQ_nCount[i] += tmpItem.statItem.RSRQ_nCount[i];
		}
		statItem.RSRP_nCount7 += tmpItem.statItem.RSRP_nCount7;
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		sPos = 0;
		statItem = Stat_Cell_Freq.FillData(vals, 0);
		
		return true;
	}

	@Override
	public String getData()
	{	
		return ResultHelper.getPutCell_Freq(statItem);
	}

	
	
	
}
