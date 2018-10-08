package cn.mastercom.bigdata.spark.mroxdrmerge.wfloc;

import java.io.Serializable;

public class WflocTmp extends YdWlanInfo implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	public String imsi;
	public String msisdn;
	public String sTime;
	public String eTime;
	
	public static final String spliter = "\t";
	public WflocTmp()
	{
		
	}
	
	public WflocTmp(String imsi, String msisdn, String sTime, String eTime)
	{
		super();
		this.imsi = imsi;
		this.msisdn = msisdn;
		this.sTime = sTime;
		this.eTime = eTime;
	}
	
	public String getImsi() {
		return imsi;
	}

	public void setImsi(String imsi) {
		this.imsi = imsi;
	}

	public String getMsisdn() {
		return msisdn;
	}

	public void setMsisdn(String msisdn) {
		this.msisdn = msisdn;
	}

	public String getsTime() {
		return sTime;
	}

	public void setsTime(String sTime) {
		this.sTime = sTime;
	}

	public String geteTime() {
		return eTime;
	}

	public void seteTime(String eTime) {
		this.eTime = eTime;
	}

	public void filldata(String[] args)
	{
		int i = 0;
		appName = args[i++];
		mac = args[i++];
		buildingid = args[i++];
		level = args[i++];
		longitude = args[i++];
		latitude = args[i++];
		imsi = args[i++];
		msisdn = args[i++];
		sTime = args[i++];
		eTime = args[i++];
	}
	
	public void filldata1(String[] args)
	{
		int i = 0;
		appName = args[i++];
		mac = args[i++];
		buildingid = args[i++];
		level = args[i++];
		longitude = args[i++];
		latitude = args[i++];
		imsi = args[i++];
		sTime = args[i++];
		eTime = args[i++];
	}
	
	public String toString()
	{
		StringBuffer sb = new StringBuffer();
		sb.append(appName);
		sb.append(spliter);
		sb.append(mac);
		sb.append(spliter);
		sb.append(buildingid);
		sb.append(spliter);
		sb.append(level);
		sb.append(spliter);
		sb.append(longitude);
		sb.append(spliter);
		sb.append(latitude);
		sb.append(spliter);
		sb.append(imsi);
		sb.append(spliter);
		sb.append(sTime);
		sb.append(spliter);
		sb.append(eTime);
		
		return sb.toString();		
	}
	
	/**
	 * 吐出最终结果
	 */
	public String toLine()
	{
		StringBuffer sb = new StringBuffer();
		sb.append(imsi);
		sb.append(spliter);
		sb.append(appName);
		sb.append(spliter);
		sb.append(sTime);
		sb.append(spliter);
		sb.append(eTime);
		sb.append(spliter);
		sb.append(mac);
		sb.append(spliter);
		sb.append(buildingid);
		sb.append(spliter);
		sb.append(level);
		sb.append(spliter);
		sb.append(longitude);
		sb.append(spliter);
		sb.append(latitude);
		
		return sb.toString();		
	}
}
