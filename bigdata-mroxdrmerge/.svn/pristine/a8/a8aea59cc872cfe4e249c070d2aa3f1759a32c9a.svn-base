package cn.mastercom.bigdata.StructData;

public class Stat_CellGrid_Freq_4G extends Stat_Grid_Freq_4G
{
	public long eci;

	public Stat_CellGrid_Freq_4G()
	{
		super();
		this.eci = 0L;
	}

	public static Stat_CellGrid_Freq_4G FillData(String[] values, int startPos)
	{
		int i = startPos;
		Stat_CellGrid_Freq_4G item = new Stat_CellGrid_Freq_4G();
		item.eci = Long.parseLong(values[i++]);
		item.icityid = Integer.parseInt(values[i++]);
		item.freq = Integer.parseInt(values[i++]);
		item.startTime = Integer.parseInt(values[i++]);
		item.endTime = Integer.parseInt(values[i++]);
		item.iduration = Integer.parseInt(values[i++]);
		item.idistance = Integer.parseInt(values[i++]);
		item.isamplenum = Integer.parseInt(values[i++]);
		item.itllongitude = Integer.parseInt(values[i++]);
		item.itllatitude = Integer.parseInt(values[i++]);
		item.ibrlongitude = Integer.parseInt(values[i++]);
		item.ibrlatitude = Integer.parseInt(values[i++]);
		item.tStat.RSRP_nTotal = Integer.parseInt(values[i++]);
		item.tStat.RSRP_nSum = Long.parseLong(values[i++]);

		for (int j = 0; j < item.tStat.RSRP_nCount.length; j++)
		{
			item.tStat.RSRP_nCount[j] = Integer.parseInt(values[i++]);
		}

		item.tStat.SINR_nTotal = Integer.parseInt(values[i++]);
		item.tStat.SINR_nSum = Long.parseLong(values[i++]);

		for (int j = 0; j < item.tStat.SINR_nCount.length; j++)
		{
			item.tStat.SINR_nCount[j] = Integer.parseInt(values[i++]);
		}

		item.tStat.RSRP100_SINR0 = Integer.parseInt(values[i++]);
		item.tStat.RSRP105_SINR0 = Integer.parseInt(values[i++]);
		item.tStat.RSRP110_SINR3 = Integer.parseInt(values[i++]);
		item.tStat.RSRP110_SINR0 = Integer.parseInt(values[i++]);
		item.tStat.RSRP_nCount7 = Integer.parseInt(values[i++]);
		return item;

	}

}
