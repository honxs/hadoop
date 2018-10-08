package cn.mastercom.bigdata.xdr.loc;

import java.io.Serializable;

import cn.mastercom.bigdata.StructData.StatFreqCell;
import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;

public class FreqCellByImeiDataMergeDo implements IMergeDataDo,Serializable
{
	private int dataType = 0;
	public StatFreqCell freqCell = new StatFreqCell();
	private StringBuffer sbTemp = new StringBuffer();

	@Override
	public String getMapKey()
	{
		// TODO Auto-generated method stub
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(freqCell.eci);
		sbTemp.append("_");
		sbTemp.append(freqCell.freq);
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
		FreqCellByImeiDataMergeDo cellfreq = (FreqCellByImeiDataMergeDo) o;
		freqCell.iMRCnt += cellfreq.freqCell.iMRCnt;
		freqCell.fRSRPValue += cellfreq.freqCell.fRSRPValue;
		freqCell.iMRCnt_95 += cellfreq.freqCell.iMRCnt_95;
		freqCell.iMRCnt_100 += cellfreq.freqCell.iMRCnt_100;
		freqCell.iMRCnt_103 += cellfreq.freqCell.iMRCnt_103;
		freqCell.iMRCnt_105 += cellfreq.freqCell.iMRCnt_105;
		freqCell.iMRCnt_110 += cellfreq.freqCell.iMRCnt_110;
		freqCell.iMRCnt_113 += cellfreq.freqCell.iMRCnt_113;
		freqCell.iMRCnt_128 += cellfreq.freqCell.iMRCnt_128;
		freqCell.iMRRSRQCnt += cellfreq.freqCell.iMRRSRQCnt;
		freqCell.fRSRQValue += cellfreq.freqCell.fRSRQValue;
		freqCell.iNCMRCnt += cellfreq.freqCell.iNCMRCnt;
		freqCell.fNCRSRPValue += cellfreq.freqCell.fNCRSRPValue;
		freqCell.iNCMRCnt_95 += cellfreq.freqCell.iNCMRCnt_95;
		freqCell.iNCMRCnt_100 += cellfreq.freqCell.iNCMRCnt_100;
		freqCell.iNCMRCnt_103 += cellfreq.freqCell.iNCMRCnt_103;
		freqCell.iNCMRCnt_105 += cellfreq.freqCell.iNCMRCnt_105;
		freqCell.iNCMRCnt_110 += cellfreq.freqCell.iNCMRCnt_110;
		freqCell.iNCMRCnt_113 += cellfreq.freqCell.iNCMRCnt_113;
		freqCell.iNCMRCnt_128 += cellfreq.freqCell.iNCMRCnt_128;
		freqCell.iNCMRRSRQCnt += cellfreq.freqCell.iNCMRRSRQCnt;
		freqCell.fNCRSRQValue += cellfreq.freqCell.fNCRSRQValue;
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		// TODO Auto-generated method stub
		freqCell = StatFreqCell.FillData(vals, 0);
		return true;
	}

	@Override
	public String getData()
	{
		// TODO Auto-generated method stub
		return freqCell.toLine();
	}

}
