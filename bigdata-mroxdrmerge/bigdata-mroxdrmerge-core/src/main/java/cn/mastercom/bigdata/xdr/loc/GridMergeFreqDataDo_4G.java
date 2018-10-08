package cn.mastercom.bigdata.xdr.loc;

import java.io.Serializable;

import cn.mastercom.bigdata.StructData.Stat_Grid_Freq_4G;
import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;

public class GridMergeFreqDataDo_4G implements IMergeDataDo,Serializable
{
	private int dataType = 0;
	public Stat_Grid_Freq_4G statItem = new Stat_Grid_Freq_4G();

	private StringBuffer sbTemp = new StringBuffer();

	@Override
	public String getMapKey()
	{
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(statItem.icityid);sbTemp.append("_");
		sbTemp.append(statItem.freq);sbTemp.append("_");
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
		GridMergeFreqDataDo_4G tmpItem = (GridMergeFreqDataDo_4G)o;
		if(tmpItem == null)
		{
			return false;
		}
		
		statItem.iduration += tmpItem.statItem.iduration;
		statItem.idistance += tmpItem.statItem.idistance;
		statItem.isamplenum += tmpItem.statItem.isamplenum;

		// rsrp
		if (tmpItem.statItem.tStat.RSRP_nTotal > 0)
		{
			statItem.tStat.RSRP_nTotal += tmpItem.statItem.tStat.RSRP_nTotal;
			statItem.tStat.RSRP_nSum += tmpItem.statItem.tStat.RSRP_nSum;
		}

		for (int i = 0; i < statItem.tStat.RSRP_nCount.length; i++)
		{
			statItem.tStat.RSRP_nCount[i] += tmpItem.statItem.tStat.RSRP_nCount[i];
		}
		statItem.tStat.RSRP_nCount7 += tmpItem.statItem.tStat.RSRP_nCount7;

		if (tmpItem.statItem.tStat.RSRQ_nTotal > 0)
		{
			statItem.tStat.RSRQ_nTotal += tmpItem.statItem.tStat.RSRQ_nTotal;
			statItem.tStat.RSRQ_nSum += tmpItem.statItem.tStat.RSRQ_nSum;
		}

		for (int i = 0; i < tmpItem.statItem.tStat.RSRQ_nCount.length; i++)
		{
			statItem.tStat.RSRQ_nCount[i] += tmpItem.statItem.tStat.RSRQ_nCount[i];
		}
		
		// sinr
		if (tmpItem.statItem.tStat.SINR_nTotal > 0)
		{
			statItem.tStat.SINR_nTotal += tmpItem.statItem.tStat.SINR_nTotal;
			statItem.tStat.SINR_nSum += tmpItem.statItem.tStat.SINR_nSum;
		}

		for (int i = 0; i < statItem.tStat.SINR_nCount.length; i++)
		{
			statItem.tStat.SINR_nCount[i] += tmpItem.statItem.tStat.SINR_nCount[i];
		}

		statItem.tStat.RSRP100_SINR0 += tmpItem.statItem.tStat.RSRP100_SINR0;
		statItem.tStat.RSRP105_SINR0 += tmpItem.statItem.tStat.RSRP105_SINR0;
		statItem.tStat.RSRP110_SINR3 += tmpItem.statItem.tStat.RSRP110_SINR3;
		statItem.tStat.RSRP110_SINR0 += tmpItem.statItem.tStat.RSRP110_SINR0;
		
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		sPos = 0;
		statItem = Stat_Grid_Freq_4G.FillData(vals, 0);
		
		return true;
	}

	@Override
	public String getData()
	{	
		return ResultHelper.getPutGrid_4G_FREQ(statItem);
	}

	
	
	
}
