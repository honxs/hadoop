package cn.mastercom.bigdata.locuser;

public class MrPoint
{
	public int itime = 0; // 聚合时间点
	public int eci = 0;
	public long MmeUeS1apId = 0;
	public double rsrp_max = -1000000;
	public double rsrp_min = -1000000;
	public double rsrq_max = -1000000;
	public double rsrq_min = -1000000;
	public Mrcell cell = new Mrcell();

	// add by yht 设置距离限制
	public void FSetDisLimit()
	{
		if (rsrp_max >= -70)
		{
			if (cell.iheight > 0)
			{
				if (cell.iheight <= 12)
				{
					cell.isevdislimit = 120;
				}
				else if (cell.iheight <= 22)
				{
					cell.isevdislimit = 300;
				}
				else
				{
					cell.isevdislimit = 500;
				}
			}
		}
		else if (rsrp_max >= -80)
		{
			if (cell.iheight > 0)
			{
				if (cell.iheight <= 12)
				{
					cell.isevdislimit = 300;
				}
				else
				{
					cell.isevdislimit = 500;
				}
			}
		}
		else if (rsrp_max >= -90)
		{
			if (cell.iheight > 0)
			{
				if (cell.iheight <= 12)
				{
					cell.isevdislimit = 300;
				}
			}
		}
		if ((cell.issamebts > 0))
		{
			if ((cell.isevdislimit > 300) && (rsrp_max >= -100))
			{
				cell.isevdislimit = 400;
			}
			else if (cell.isevdislimit > 1000)
			{
				cell.isevdislimit = 1000;
			}
		}
		if (cell.isindoor == 1)
		{
			if (rsrp_max > -100)
			{
				if (cell.isevdislimit > 300)
				{
					cell.isevdislimit = 300;
				}
			}
		}
	}
}
