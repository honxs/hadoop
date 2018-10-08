package cn.mastercom.bigdata.spark.mroxdrmerge.wfloc;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class LogInfo {
	private String imsi;
	private String msisdn;
	private String sTime;
	private String eTime;
	private String mac;

	private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public static final String spliter = "\t";

	public LogInfo() {

	}

	public LogInfo(String args[]) {
		int i = 0;
		this.imsi = args[i++];
		this.msisdn = args[i++];
		this.sTime = args[i++];
		this.eTime = args[i++];
		this.mac = args[i++];
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

	public String getStime() {
		return sTime;
	}

	public void setStime(String sTime) {
		this.sTime = sTime;
	}

	public String getEtime() {
		return eTime;
	}

	public void setEtime(String eTime) {
		this.eTime = eTime;
	}

	public String getMac() {
		return mac;
	}

	public void setMac(String mac) {
		this.mac = mac;
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(imsi.trim());
		sb.append(spliter);
		// 格式化手机号，取后11位
		if (msisdn.startsWith("86") || msisdn.startsWith("+86")) {
			msisdn = msisdn.trim().substring(msisdn.length() - 11, msisdn.length());
		}
		sb.append(msisdn);
		sb.append(spliter);
		try {
			sb.append(format.parse(sTime).getTime());
			sb.append(spliter);
			sb.append(format.parse(eTime).getTime());
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return sb.toString();
	}
}
