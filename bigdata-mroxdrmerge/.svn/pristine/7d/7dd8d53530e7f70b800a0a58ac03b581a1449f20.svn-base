package cn.mastercom.bigdata.StructData;

import java.io.Serializable;

import cn.mastercom.bigdata.util.DataGeter;
import cn.mastercom.bigdata.util.StringUtil;

public class Stat_UserAct_Cell implements Serializable
{
	
	private static final long serialVersionUID = 1L;
	public int icityid;
	public long imsi;
	public String msisdn;
	public int stime;
	public int etime;

	public long eci;
	public int sn;
	public long nbeci;
	public int rsrpSum;
	public int rsrpTotal;
	public int rsrpMaxMark;
	public int rsrpMinMark;

	public Stat_UserAct_Cell()
	{
		icityid = -1;
		stime = 0;
		etime = 0;

		eci = 0;
		sn = 0;
		nbeci = 0;
		rsrpSum = 0;
		rsrpTotal = 0;
		rsrpMaxMark = StaticConfig.Int_Abnormal;
		rsrpMinMark = StaticConfig.Int_Abnormal;
	}

	public static Stat_UserAct_Cell FillData(String[] values, int startPos)
	{
		int i = startPos;

		Stat_UserAct_Cell item = new Stat_UserAct_Cell();
		item.icityid = DataGeter.GetInt(values[i++]);
		item.imsi = DataGeter.GetLong(values[i++]);
		item.msisdn = values[i++];
		item.stime = DataGeter.GetInt(values[i++]);
		item.etime = DataGeter.GetInt(values[i++]);

		item.eci = DataGeter.GetLong(values[i++]);
		item.sn = DataGeter.GetInt(values[i++]);
		item.nbeci = DataGeter.GetLong(values[i++]);
		item.rsrpSum = DataGeter.GetInt(values[i++]);
		item.rsrpTotal = DataGeter.GetInt(values[i++]);
		item.rsrpMaxMark = DataGeter.GetInt(values[i++]);
		item.rsrpMinMark = DataGeter.GetInt(values[i++]);

		return item;
	}
}
