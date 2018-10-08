package cn.mastercom.bigdata.stat.freqloc;

import java.text.DecimalFormat;

public class FreqCellItem
{
	// public int cityId;
	// public int freq;
	public int pci;
	public int longitude;
	public int latitude;
	public int Time;
	public int MRCnt;
	public int MRCnt_Indoor;
	public int MRCnt_Outdoor;
	public double RSRPValue;
	public double RSRPValue_Indoor;
	public double RSRPValue_Outdoor;
	public int MRCnt_Indoor_0_70;
	public int MRCnt_Indoor_70_80;
	public int MRCnt_Indoor_80_90;
	public int MRCnt_Indoor_90_95;
	public int MRCnt_Indoor_100;
	public int MRCnt_Indoor_103;
	public int MRCnt_Indoor_105;
	public int MRCnt_Indoor_110;
	public int MRCnt_Indoor_113;
	public int MRCnt_Outdoor_0_70;
	public int MRCnt_Outdoor_70_80;
	public int MRCnt_Outdoor_80_90;
	public int MRCnt_Outdoor_90_95;
	public int MRCnt_Outdoor_100;
	public int MRCnt_Outdoor_103;
	public int MRCnt_Outdoor_105;
	public int MRCnt_Outdoor_110;
	public int MRCnt_Outdoor_113;
	
	public static final String spliter = "\t";

	public FreqCellItem()
	{
		clean();
	}

	public void clean()
	{
		longitude = 0;
		latitude = 0;
		Time = 0;
		MRCnt = 0;
		MRCnt_Indoor = 0;
		MRCnt_Outdoor = 0;
		RSRPValue = 0;
		RSRPValue_Indoor = 0;
		RSRPValue_Outdoor = 0;
		MRCnt_Indoor_0_70 = 0;
		MRCnt_Indoor_70_80 = 0;
		MRCnt_Indoor_80_90 = 0;
		MRCnt_Indoor_90_95 = 0;
		MRCnt_Indoor_100 = 0;
		MRCnt_Indoor_103 = 0;
		MRCnt_Indoor_105 = 0;
		MRCnt_Indoor_110 = 0;
		MRCnt_Indoor_113 = 0;
		MRCnt_Outdoor_0_70 = 0;
		MRCnt_Outdoor_70_80 = 0;
		MRCnt_Outdoor_80_90 = 0;
		MRCnt_Outdoor_90_95 = 0;
		MRCnt_Outdoor_100 = 0;
		MRCnt_Outdoor_103 = 0;
		MRCnt_Outdoor_105 = 0;
		MRCnt_Outdoor_110 = 0;
		MRCnt_Outdoor_113 = 0;
	}

	public void fillData(String[] vals)
	{
		Time = Integer.parseInt(vals[3]);
		pci = Integer.parseInt(vals[5]);
	}

	/**
	 * 做统计
	 * 
	 * @param rsrp
	 * @param ibuildingID
	 * @param longitude
	 * @param latitude
	 */
	public void doSample(int rsrp, int ibuildingID)
	{
		if (rsrp >= -150 && rsrp <= -30)
		{
			MRCnt++;

			RSRPValue += rsrp;

			if (ibuildingID > 0)
			{
				MRCnt_Indoor++;

				RSRPValue_Indoor += rsrp;
				if (rsrp >= -70 && rsrp < 0)
				{
					MRCnt_Indoor_0_70++;
				}
				else if (rsrp >= -80 && rsrp < -70)
				{
					MRCnt_Indoor_70_80++;
				}
				else if (rsrp >= -90 && rsrp < -80)
				{
					MRCnt_Indoor_80_90++;
				}
				else if (rsrp >= -95 && rsrp < -90)
				{
					MRCnt_Indoor_90_95++;
				}

				if (rsrp >= -100 && rsrp < 0)
				{
					MRCnt_Indoor_100++;
				}
				if (rsrp >= -103 && rsrp < 0)
				{
					MRCnt_Indoor_103++;
				}
				if (rsrp >= -105 && rsrp < 0)
				{
					MRCnt_Indoor_105++;
				}
				if (rsrp >= -110 && rsrp < 0)
				{
					MRCnt_Indoor_110++;
				}
				if (rsrp >= -113 && rsrp < 0)
				{
					MRCnt_Indoor_113++;
				}
			}
			else if (ibuildingID <= 0)
			{
				MRCnt_Outdoor++;

				RSRPValue_Outdoor += rsrp;

				if (rsrp >= -70 && rsrp < 0)
				{
					MRCnt_Outdoor_0_70++;
				}
				else if (rsrp >= -80 && rsrp < -70)
				{
					MRCnt_Outdoor_70_80++;
				}
				else if (rsrp >= -90 && rsrp < -80)
				{
					MRCnt_Outdoor_80_90++;
				}
				else if (rsrp >= -95 && rsrp < -90)
				{
					MRCnt_Outdoor_90_95++;
				}

				if (rsrp >= -100 && rsrp < 0)
				{
					MRCnt_Outdoor_100++;
				}
				if (rsrp >= -103 && rsrp < 0)
				{
					MRCnt_Outdoor_103++;
				}
				if (rsrp >= -105 && rsrp < 0)
				{
					MRCnt_Outdoor_105++;
				}
				if (rsrp >= -110 && rsrp < 0)
				{
					MRCnt_Outdoor_110++;
				}
				if (rsrp >= -113 && rsrp < 0)
				{
					MRCnt_Outdoor_113++;
				}
			}
			
		}
	}
	
	public String toLine()
	{
		DecimalFormat df = new DecimalFormat("0.0");
		StringBuffer bf = new StringBuffer();
		bf.append(pci);
		bf.append(spliter);
		bf.append(longitude);
		bf.append(spliter);
		bf.append(latitude);
		bf.append(spliter);
		bf.append(Time);
		bf.append(spliter);
		bf.append(MRCnt);
		bf.append(spliter);
		bf.append(MRCnt_Indoor);
		bf.append(spliter);
		bf.append(MRCnt_Outdoor);
		bf.append(spliter);
		bf.append(df.format(RSRPValue));
		bf.append(spliter);
		bf.append(df.format(RSRPValue_Indoor));
		bf.append(spliter);
		bf.append(df.format(RSRPValue_Outdoor));
		bf.append(spliter);
		bf.append(MRCnt_Indoor_0_70);
		bf.append(spliter);
		bf.append(MRCnt_Indoor_70_80);
		bf.append(spliter);
		bf.append(MRCnt_Indoor_80_90);
		bf.append(spliter);
		bf.append(MRCnt_Indoor_90_95);
		bf.append(spliter);
		bf.append(MRCnt_Indoor_100);
		bf.append(spliter);
		bf.append(MRCnt_Indoor_103);
		bf.append(spliter);
		bf.append(MRCnt_Indoor_105);
		bf.append(spliter);
		bf.append(MRCnt_Indoor_110);
		bf.append(spliter);
		bf.append(MRCnt_Indoor_113);
		bf.append(spliter);
		bf.append(MRCnt_Outdoor_0_70);
		bf.append(spliter);
		bf.append(MRCnt_Outdoor_70_80);
		bf.append(spliter);
		bf.append(MRCnt_Outdoor_80_90);
		bf.append(spliter);
		bf.append(MRCnt_Outdoor_90_95);
		bf.append(spliter);
		bf.append(MRCnt_Outdoor_100);
		bf.append(spliter);
		bf.append(MRCnt_Outdoor_103);
		bf.append(spliter);
		bf.append(MRCnt_Outdoor_105);
		bf.append(spliter);
		bf.append(MRCnt_Outdoor_110);
		bf.append(spliter);
		bf.append(MRCnt_Outdoor_113);
		return bf.toString();
	}

}
