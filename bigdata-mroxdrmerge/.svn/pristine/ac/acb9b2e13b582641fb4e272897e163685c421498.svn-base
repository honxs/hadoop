package cn.mastercom.bigdata.conf.cellconfig;

import cn.mastercom.bigdata.util.DataGeter;

public class GsmCellInfo
{
    public int lac;
    public int ci;
    public int btsid;
    public int cityid;
	public int radius;
	public int ilongitude;
	public int ilatitude;
	public int bcch;
	public int bsic;

	
    public static GsmCellInfo FillData(String[] values)
    {
    	GsmCellInfo item = new GsmCellInfo();
    	int i = 0;
    	item.lac = DataGeter.GetInt(values[i++]);
    	item.ci = DataGeter.GetInt(values[i++]);
    	item.btsid = DataGeter.GetInt(values[i++]);
    	item.cityid = DataGeter.GetInt(values[i++]);
    	item.radius = (int)DataGeter.GetDouble(values[i++]);
    	item.ilongitude = (int)(DataGeter.GetDouble(values[i++])*10000000);
    	item.ilatitude = (int)(DataGeter.GetDouble(values[i++])*10000000);
    	item.bcch = DataGeter.GetInt(values[i++]);
    	item.bsic = DataGeter.GetInt(values[i++]);
    	return item;
    }
}
