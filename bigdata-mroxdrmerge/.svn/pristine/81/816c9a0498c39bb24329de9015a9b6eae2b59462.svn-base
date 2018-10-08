package cn.mastercom.bigdata.xdr.loc;

import java.io.Serializable;

import cn.mastercom.bigdata.StructData.StatFreqGrid;
import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;

public class GridMergeFreqByImeiDataDo_4G implements IMergeDataDo,Serializable
{

	private int dataType = 0;
	public StatFreqGrid freqGrid = new StatFreqGrid();
	private StringBuffer sbTemp = new StringBuffer();

	@Override
	public String getMapKey()
	{
		// TODO Auto-generated method stub
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(freqGrid.icityid);
		sbTemp.append("_");
		sbTemp.append(freqGrid.itllongitude);
		sbTemp.append("_");
		sbTemp.append(freqGrid.itllatitude);
		sbTemp.append("_");
		sbTemp.append(freqGrid.freq);
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
		GridMergeFreqByImeiDataDo_4G gridmergeFreq = (GridMergeFreqByImeiDataDo_4G) o;
		if (gridmergeFreq == null)
		{
			return false;
		}
		freqGrid.iMRCnt += gridmergeFreq.freqGrid.iMRCnt;
		freqGrid.fRSRPValue += gridmergeFreq.freqGrid.fRSRPValue;
		freqGrid.iMRCnt_95 += gridmergeFreq.freqGrid.iMRCnt_95;
		freqGrid.iMRCnt_100 += gridmergeFreq.freqGrid.iMRCnt_100;
		freqGrid.iMRCnt_103 += gridmergeFreq.freqGrid.iMRCnt_103;
		freqGrid.iMRCnt_105 += gridmergeFreq.freqGrid.iMRCnt_105;
		freqGrid.iMRCnt_110 += gridmergeFreq.freqGrid.iMRCnt_110;
		freqGrid.iMRCnt_113 += gridmergeFreq.freqGrid.iMRCnt_113;
		freqGrid.iMRCnt_128 += gridmergeFreq.freqGrid.iMRCnt_128;
		freqGrid.iMRRSRQCnt += gridmergeFreq.freqGrid.iMRRSRQCnt;
		freqGrid.fRSRQValue += gridmergeFreq.freqGrid.fRSRQValue;
		freqGrid.iNCMRCnt += gridmergeFreq.freqGrid.iNCMRCnt;
		freqGrid.fNCRSRPValue += gridmergeFreq.freqGrid.fNCRSRPValue;
		freqGrid.iNCMRCnt_95 += gridmergeFreq.freqGrid.iNCMRCnt_95;
		freqGrid.iNCMRCnt_100 += gridmergeFreq.freqGrid.iNCMRCnt_100;
		freqGrid.iNCMRCnt_103 += gridmergeFreq.freqGrid.iNCMRCnt_103;
		freqGrid.iNCMRCnt_105 += gridmergeFreq.freqGrid.iNCMRCnt_105;
		freqGrid.iNCMRCnt_110 += gridmergeFreq.freqGrid.iNCMRCnt_110;
		freqGrid.iNCMRCnt_113 += gridmergeFreq.freqGrid.iNCMRCnt_113;
		freqGrid.iNCMRCnt_128 += gridmergeFreq.freqGrid.iNCMRCnt_128;
		freqGrid.iNCMRRSRQCnt += gridmergeFreq.freqGrid.iNCMRRSRQCnt;
		freqGrid.fNCRSRQValue += gridmergeFreq.freqGrid.fNCRSRQValue;
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		freqGrid = StatFreqGrid.FillData(vals, 0);
		return true;
	}

	@Override
	public String getData()
	{
		return freqGrid.toLine();
	}

}
