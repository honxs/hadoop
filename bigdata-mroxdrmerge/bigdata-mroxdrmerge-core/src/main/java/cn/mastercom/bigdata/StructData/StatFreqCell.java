package cn.mastercom.bigdata.StructData;

import java.io.Serializable;

public class StatFreqCell implements Serializable
{
	public int icityid;
	public int startTime;
	public int endTime;
	public int eci;
	public int freq;
	public int iMRCnt;
	public double fRSRPValue;
	public int iMRCnt_95;
	public int iMRCnt_100;
	public int iMRCnt_103;
	public int iMRCnt_105;
	public int iMRCnt_110;
	public int iMRCnt_113;
	public int iMRCnt_128;
	public int iMRRSRQCnt;
	public double fRSRQValue;
	public int iNCMRCnt;
	public double fNCRSRPValue;
	public int iNCMRCnt_95;
	public int iNCMRCnt_100;
	public int iNCMRCnt_103;
	public int iNCMRCnt_105;
	public int iNCMRCnt_110;
	public int iNCMRCnt_113;
	public int iNCMRCnt_128;
	public int iNCMRRSRQCnt;
	public double fNCRSRQValue;
	public static final String spliter = "\t";

	public String toLine()
	{
		StringBuffer bf = new StringBuffer();
		bf.append(icityid);
		bf.append(spliter);
		bf.append(startTime);
		bf.append(spliter);
		bf.append(endTime);
		bf.append(spliter);
		bf.append(eci);
		bf.append(spliter);
		bf.append(freq);
		bf.append(spliter);
		bf.append(iMRCnt);
		bf.append(spliter);
		bf.append(fRSRPValue);
		bf.append(spliter);
		bf.append(iMRCnt_95);
		bf.append(spliter);
		bf.append(iMRCnt_100);
		bf.append(spliter);
		bf.append(iMRCnt_103);
		bf.append(spliter);
		bf.append(iMRCnt_105);
		bf.append(spliter);
		bf.append(iMRCnt_110);
		bf.append(spliter);
		bf.append(iMRCnt_113);
		bf.append(spliter);
		bf.append(iMRCnt_128);
		bf.append(spliter);
		bf.append(iMRRSRQCnt);
		bf.append(spliter);
		bf.append(fRSRQValue);
		bf.append(spliter);
		bf.append(iNCMRCnt);
		bf.append(spliter);
		bf.append(fNCRSRPValue);
		bf.append(spliter);
		bf.append(iNCMRCnt_95);
		bf.append(spliter);
		bf.append(iNCMRCnt_100);
		bf.append(spliter);
		bf.append(iNCMRCnt_103);
		bf.append(spliter);
		bf.append(iNCMRCnt_105);
		bf.append(spliter);
		bf.append(iNCMRCnt_110);
		bf.append(spliter);
		bf.append(iNCMRCnt_113);
		bf.append(spliter);
		bf.append(iNCMRCnt_128);
		bf.append(spliter);
		bf.append(iNCMRRSRQCnt);
		bf.append(spliter);
		bf.append(fNCRSRQValue);
		return bf.toString();
	}

	public static StatFreqCell FillData(String[] val, int startPos)
	{
		int i = startPos;
		StatFreqCell freqcell = new StatFreqCell();
		freqcell.icityid = Integer.parseInt(val[i++]);
		freqcell.startTime = Integer.parseInt(val[i++]);
		freqcell.endTime = Integer.parseInt(val[i++]);
		freqcell.eci = Integer.parseInt(val[i++]);
		freqcell.freq = Integer.parseInt(val[i++]);
		freqcell.iMRCnt = Integer.parseInt(val[i++]);
		freqcell.fRSRPValue = Double.parseDouble(val[i++]);
		freqcell.iMRCnt_95 = Integer.parseInt(val[i++]);
		freqcell.iMRCnt_100 = Integer.parseInt(val[i++]);
		freqcell.iMRCnt_103 = Integer.parseInt(val[i++]);
		freqcell.iMRCnt_105 = Integer.parseInt(val[i++]);
		freqcell.iMRCnt_110 = Integer.parseInt(val[i++]);
		freqcell.iMRCnt_113 = Integer.parseInt(val[i++]);
		freqcell.iMRCnt_128 = Integer.parseInt(val[i++]);
		freqcell.iMRRSRQCnt = Integer.parseInt(val[i++]);
		freqcell.fRSRQValue = Double.parseDouble(val[i++]);
		freqcell.iNCMRCnt = Integer.parseInt(val[i++]);
		freqcell.fNCRSRPValue = Double.parseDouble(val[i++]);
		freqcell.iNCMRCnt_95 = Integer.parseInt(val[i++]);
		freqcell.iNCMRCnt_100 = Integer.parseInt(val[i++]);
		freqcell.iNCMRCnt_103 = Integer.parseInt(val[i++]);
		freqcell.iNCMRCnt_105 = Integer.parseInt(val[i++]);
		freqcell.iNCMRCnt_110 = Integer.parseInt(val[i++]);
		freqcell.iNCMRCnt_113 = Integer.parseInt(val[i++]);
		freqcell.iNCMRCnt_128 = Integer.parseInt(val[i++]);
		freqcell.iNCMRRSRQCnt = Integer.parseInt(val[i++]);
		freqcell.fNCRSRQValue = Double.parseDouble(val[i++]);
		return freqcell;
	}

	public StatFreqCell()
	{
	}

	public StatFreqCell(int icityid, int startTime, int endTime, int eci, int freq)
	{
		this.icityid = icityid;
		this.startTime = startTime;
		this.endTime = endTime;
		this.eci = eci;
		this.freq = freq;
	}

	public void dealSample(DT_Sample_4G sample, int ncRsrp, int ncRsrq)
	{
		if (!(sample.LteScRSRP >= -150 && sample.LteScRSRP <= -30))
		{
			return;
		}
		iMRCnt++;
		fRSRPValue += sample.LteScRSRP;
		if (sample.LteScRSRP >= -95)
		{
			iMRCnt_95++;
		}
		if (sample.LteScRSRP >= -100)
		{
			iMRCnt_100++;
		}
		if (sample.LteScRSRP >= -103)
		{
			iMRCnt_103++;
		}
		if (sample.LteScRSRP >= -105)
		{
			iMRCnt_105++;
		}
		if (sample.LteScRSRP >= -110)
		{
			iMRCnt_110++;
		}
		if (sample.LteScRSRP >= -113)
		{
			iMRCnt_113++;
		}
		if (sample.LteScRSRP >= -128)
		{
			iMRCnt_128++;
		}
		if (sample.LteScRSRQ != -1000000)
		{
			iMRRSRQCnt++;
			fRSRQValue += sample.LteScRSRQ;
		}
		if (ncRsrp >= -150 && ncRsrp <= -30)
		{
			iNCMRCnt++;
			fNCRSRPValue += ncRsrp;
		}
		if (ncRsrp >= -95)
		{
			iNCMRCnt_95++;
		}
		if (ncRsrp >= -100)
		{
			iNCMRCnt_100++;
		}
		if (ncRsrp >= -103)
		{
			iNCMRCnt_103++;
		}
		if (ncRsrp >= -105)
		{
			iNCMRCnt_105++;
		}
		if (ncRsrp >= -110)
		{
			iNCMRCnt_110++;
		}
		if (ncRsrp >= -113)
		{
			iNCMRCnt_113++;
		}
		if (ncRsrp >= -128)
		{
			iNCMRCnt_128++;
		}
		if (ncRsrq != -1000000)
		{
			iNCMRRSRQCnt++;
			fNCRSRQValue += ncRsrq;
		}
	}

}
