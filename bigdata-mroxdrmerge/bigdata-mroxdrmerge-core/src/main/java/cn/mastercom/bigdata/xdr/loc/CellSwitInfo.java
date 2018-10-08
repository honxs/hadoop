package cn.mastercom.bigdata.xdr.loc;

import cn.mastercom.bigdata.StructData.SIGNAL_LOC;
import cn.mastercom.bigdata.StructData.SIGNAL_XDR_23G;
import cn.mastercom.bigdata.StructData.SIGNAL_XDR_4G;

public class CellSwitInfo
{
	public int cityID;
	public long imsi;
	public long eci;
	public int stime;
	public int etime;
	public long longtitude;
	public long latitude;
	public int num;	// 在这个小区上报了几个位置
	public String msisdn;	// 用户号码
	public String imei;	//设备号
	public String locSource; //位置来源
	public int testType; //运动状态

	public CellSwitInfo()
	{
		
	}
	
	public CellSwitInfo(SIGNAL_LOC xdrItem)
	{
		if(xdrItem  instanceof  SIGNAL_XDR_4G)
		{
			SIGNAL_XDR_4G xdr4G = (SIGNAL_XDR_4G) xdrItem;
			this.cityID = xdr4G.cityID;
			this.imsi = xdr4G.IMSI;
			this.eci = xdr4G.Eci;
			this.stime = xdr4G.stime;
			this.etime = xdr4G.stime;
			this.msisdn = xdr4G.MSISDN;
			this.imei = xdr4G.IMEI;
			this.locSource = xdr4G.loctp;
			this.longtitude = xdr4G.longitude;
			this.latitude = xdr4G.latitude;
			this.testType = xdr4G.testType;
		}
		else if (xdrItem instanceof SIGNAL_XDR_23G)
		{
			SIGNAL_XDR_23G xdr_23G = (SIGNAL_XDR_23G) xdrItem;
			this.cityID = xdr_23G.cityID;
			this.imsi = xdr_23G.IMSI;
			this.eci = xdr_23G.lac * 65536 + xdr_23G.ci;
			this.stime = xdr_23G.stime;
			this.etime = xdr_23G.stime;
			this.msisdn = xdr_23G.MSISDN;
			this.imei = xdr_23G.IMEI;
			this.locSource = xdr_23G.loctp;
			this.longtitude = xdr_23G.longitude;
			this.latitude = xdr_23G.latitude;
			this.testType = xdr_23G.testType;
		}
	}

	@Override
	public String toString()
	{
		return eci + "";
	}
}
