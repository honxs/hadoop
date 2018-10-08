package cn.mastercom.bigdata.xdr.loc;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.Stat_Cell_Freq;

public class CellData_Freq
{
	private int lac;
	private long eci;
	private int startTime;
	private int endTime;
	private int freq;
	private Stat_Cell_Freq lteCell;

	public CellData_Freq(int cityID, int lac, long eci, int freq, int pci, int startTime, int endTime)
	{
		this.lac = lac;
		this.eci = eci;
		this.freq = freq;
		this.startTime = startTime;
		this.endTime = endTime;

		lteCell = new Stat_Cell_Freq();
		lteCell.Clear();

		lteCell.icityid = cityID;
		lteCell.startTime = startTime;
		lteCell.endTime = endTime;
		lteCell.iLAC = lac;
		lteCell.wRAC = 0;
		lteCell.iCI = eci;
		lteCell.freq = freq;
		lteCell.pci  = pci;
	}

	public int getLac()
	{
		return lac;
	}

	public long getEci()
	{
		return eci;
	}

	public Stat_Cell_Freq getLteCell()
	{
		return lteCell;
	}

	public void dealSample(DT_Sample_4G sample, int rsrp, int rsrq)
	{
		boolean isSampleMro = sample.flag.toUpperCase().equals("MRO");
		boolean isSampleMre = sample.flag.toUpperCase().equals("MRE");

		// 小区统计
		lteCell.iduration += sample.duration;
		if (isSampleMro || isSampleMre)
		{
			lteCell.isamplenum++;

			if (rsrp >= -141 && rsrp <= 200)
			{
				lteCell.RSRP_nTotal++;

				lteCell.RSRP_nSum += rsrp;
				if (rsrp < -113)
				{
					lteCell.RSRP_nCount7++;
				}
				
				// RSRP_nCount[6]; //
				// [-141,-110),[-110,-105),[-105,-100),[-100,-95),[-95,-85),[-85,)
				if (rsrp < -110)
				{
					lteCell.RSRP_nCount[0]++;
				}
				else if (rsrp < -105)
				{
					lteCell.RSRP_nCount[1]++;
				}
				else if (rsrp < -100)
				{
					lteCell.RSRP_nCount[2]++;
				}
				else if (rsrp < -95)
				{
					lteCell.RSRP_nCount[3]++;
				}
				else if (rsrp < -85)
				{
					lteCell.RSRP_nCount[4]++;
				}
				else
				{
					lteCell.RSRP_nCount[5]++;
				}
			}

			if (rsrq >= -40 && rsrq <= 40)
			{
				lteCell.RSRQ_nTotal++; // 总数

				lteCell.RSRQ_nSum += rsrq;

				// int SINR_nCount[8]; //
				// [-40,-20),[-20,-16),[-16,-12),[-12,-8),[-8,0),[0,40]
				if (sample.LteScSinrUL < -20)
				{
					lteCell.RSRQ_nCount[0]++;
				}
				else if (sample.LteScSinrUL < -16)
				{
					lteCell.RSRQ_nCount[1]++;
				}
				else if (sample.LteScSinrUL < -12)
				{
					lteCell.RSRQ_nCount[2]++;
				}
				else if (sample.LteScSinrUL < -8)
				{
					lteCell.RSRQ_nCount[3]++;
				}
				else if (sample.LteScSinrUL < 0)
				{
					lteCell.RSRQ_nCount[4]++;
				}
				else if (sample.LteScSinrUL <= 40)
				{
					lteCell.RSRQ_nCount[5]++;
				}

			}
		}

	}

}
