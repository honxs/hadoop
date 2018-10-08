package cn.mastercom.bigdata.mro.loc;

import java.io.IOException;

import cn.mastercom.bigdata.conf.config.CellBuildWifi;

public class WifiFixed
{
	public static void main(String args[])
	{
		try
		{
			CellBuildWifi cellBuildWifi = new CellBuildWifi();
			cellBuildWifi.loadWifi(null, "D:\\cell_build_wifi_25365772.data.gz", true);
			String[] wifiList = new String[] { "001f7a995bb0;39;,001f7a995bb1;40;,001f7a995bb8;54;,001f7a995bb9;54;,1c5f2b410b68;56;,",
					"001f7a995bb0;41;,001f7a995bb1;41;,286c076de343;58;,001f7a995bb8;59;,001f7a995bb9;59;,", "f0b429e1c4f3;71;,",
					"fc75169c06be;55;,94b40fdede41;67;,0019702049af;73;,0619702049af;74;,b000b4348bf0;75;,", "fc75169c06be;56;,94b40fdede41;57;,ccd539bb0b70;62;,ccd539bb0b7f;64;,b000b4348bf1;66;,",
					"fc75169c06be;56;,ccd539bb0b7f;59;,b000b4348bf1;66;,3897d6717bd0;67;,3897d6717bd1;67;," };
			for (int i = 0; i < wifiList.length; i++)
			{
				System.out.println(returnLevel(cellBuildWifi, wifiList[i], 142474));
			}
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static int returnLevel(CellBuildWifi buildWifi, String wifiList, int buildid)
	{
		int level = -1;
		String macs[] = wifiList.split(",", -1);
		int wifisize = macs.length;
		Wifi wf = null;
		float weight = 0f;
		float denominator = 0f;// 分母
		float numerator = 0f;// 分子

		for (int i = 0; i < wifisize; i++)
		{
			wf = new Wifi();
			String temp[] = macs[i].split(";", -1);
			if (temp.length >= 2)
			{
				wf.mac = temp[0];
				wf.rsrp = Float.parseFloat(temp[1]) * -1;
				level = buildWifi.getLevel(buildid, wf.mac);
				if (level > 0)
				{
					wf.level = level;
					if (wf.rsrp >= -60)
					{
						weight = 1;
					}
					else if (wf.rsrp <= -100)
					{
						weight = 0;
					}
					else
					{
						weight = (wf.rsrp + 100) / 40;
					}
					numerator += wf.level * weight;
					denominator += weight;
				}
				else
				{
					continue;
				}
			}
		}

		if (denominator > 0)
		{
			return Math.round(numerator / denominator) / 3 * 3;
		}
		return -1;
	}

}
