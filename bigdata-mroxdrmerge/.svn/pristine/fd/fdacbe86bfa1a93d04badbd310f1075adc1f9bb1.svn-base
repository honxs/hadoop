package cn.mastercom.bigdata.conf.config;

import cn.mastercom.bigdata.util.DataGeter;

public class BuildIdCellInfo
{
	public int buildid;
	public long eci;

	public static BuildIdCellInfo FillData(String[] values)
	{
		BuildIdCellInfo item = new BuildIdCellInfo();
		int i = 0;
		item.buildid = DataGeter.GetInt(values[i++]);
		item.eci = DataGeter.GetLong(values[i++]);
		return item;
	}

}
