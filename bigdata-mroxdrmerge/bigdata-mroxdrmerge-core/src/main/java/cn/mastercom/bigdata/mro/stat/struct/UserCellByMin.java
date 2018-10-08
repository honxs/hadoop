package cn.mastercom.bigdata.mro.stat.struct;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.Func;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.TimeHelper;

import java.io.Serializable;

public class UserCellByMin implements IStat_4G,Serializable
{
	public int CityID;
	public long ECI;
	public int Time;
	public long Imsi;
	public String Msisdn;
	public int MRCnt;
	public int MRRSRQCnt;
	public int MRSINRCnt;
	public int MRPHRCnt;
	public int MRTADVCnt;
	public float RSRPValue;
	public float RSRQValue;
	public float SINRValue;
	public float PHRValue;
	public int TaDvValue;

	public static final String spliter = "\t";

	public UserCellByMin()
	{
	}

	public UserCellByMin(DT_Sample_4G sample)
	{
		CityID = sample.cityID;
		Imsi = sample.IMSI;
		ECI = sample.Eci;
		Time = FormatTime.RoundTimeForFiveMins(sample.itime);
	}

	public void doSample(DT_Sample_4G sample)
	{
		if (sample.IMSI <= 0 || sample.Eci <= 0)
		{
			return;
		}
		int rsrp = sample.LteScRSRP;
		int rsrq = sample.LteScRSRQ;
		int sinrul = sample.LteScSinrUL;
		if (sample.MSISDN.length() > 0)
		{
			Msisdn = sample.MSISDN;
		}
		if (rsrp >= -150 && rsrp <= -30)
		{
			MRCnt++;
			RSRPValue += rsrp;
		}
		if (rsrq > -10000)
		{
			MRRSRQCnt++;
			RSRQValue += rsrq;
		}
		if (sinrul >= -1000 && sinrul <= 1000)
		{
			MRSINRCnt++;
			SINRValue += sinrul;
		}

		if(sample.LteScPHR > -10000)
		{
			MRPHRCnt++;
			PHRValue += sample.LteScPHR;
		}
		if (sample.LteScTadv > 0)
		{
			MRTADVCnt++;
			TaDvValue += sample.LteScTadv;
		}
	}

	public String toLine()
	{
		StringBuffer bf = new StringBuffer();
		bf.append(CityID);
		bf.append(spliter);
		bf.append(Func.getEncrypt(Imsi));
		bf.append(spliter);
		bf.append(Func.getEncrypt(Msisdn));
		bf.append(spliter);
		bf.append(ECI);
		bf.append(spliter);
		bf.append(Time);
		bf.append(spliter);
		bf.append(MRCnt);
		bf.append(spliter);
		bf.append(MRRSRQCnt);
		bf.append(spliter);
		bf.append(MRSINRCnt);
		bf.append(spliter);
		bf.append(MRPHRCnt);
		bf.append(spliter);
		bf.append(MRTADVCnt);
		bf.append(spliter);
		bf.append(RSRPValue);
		bf.append(spliter);
		bf.append(RSRQValue);
		bf.append(spliter);
		bf.append(SINRValue);
		bf.append(spliter);
		bf.append(PHRValue);
		bf.append(spliter);
		bf.append(TaDvValue);

		return bf.toString();
	}
	
	public String mergeStatToLine()
	{
		StringBuffer bf = new StringBuffer();
		bf.append(CityID);
		bf.append(spliter);
		bf.append(Imsi);
		bf.append(spliter);
		bf.append(Msisdn);
		bf.append(spliter);
		bf.append(ECI);
		bf.append(spliter);
		bf.append(Time);
		bf.append(spliter);
		bf.append(MRCnt);
		bf.append(spliter);
		bf.append(MRRSRQCnt);
		bf.append(spliter);
		bf.append(MRSINRCnt);
		bf.append(spliter);
		bf.append(MRPHRCnt);
		bf.append(spliter);
		bf.append(MRTADVCnt);
		bf.append(spliter);
		bf.append(RSRPValue);
		bf.append(spliter);
		bf.append(RSRQValue);
		bf.append(spliter);
		bf.append(SINRValue);
		bf.append(spliter);
		bf.append(PHRValue);
		bf.append(spliter);
		bf.append(TaDvValue);
		return bf.toString();
	}

	public static UserCellByMin FillData(String[] vals, int pos)
	{
		int i = pos;
		UserCellByMin usercell = new UserCellByMin();
		try
		{
			usercell.CityID = Integer.parseInt(vals[i++]);
			usercell.Imsi = Long.parseLong(vals[i++]);
			usercell.Msisdn = vals[i++];
			usercell.ECI = Long.parseLong(vals[i++]);
			usercell.Time = Integer.parseInt(vals[i++]);
			usercell.MRCnt = Integer.parseInt(vals[i++]);
			usercell.MRRSRQCnt = Integer.parseInt(vals[i++]);
			usercell.MRSINRCnt = Integer.parseInt(vals[i++]);
			usercell.MRPHRCnt = Integer.parseInt(vals[i++]);
			usercell.MRTADVCnt = Integer.parseInt(vals[i++]);
			usercell.RSRPValue = Float.parseFloat(vals[i++]);
			usercell.RSRQValue = Float.parseFloat(vals[i++]);
			usercell.SINRValue = Float.parseFloat(vals[i++]);
			usercell.PHRValue = Float.parseFloat(vals[i++]);
			usercell.TaDvValue = Integer.parseInt(vals[i++]);
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error,"UserCellByMin filldata error", "", e);
		}
		return usercell;
	}

	@Override
	public void doSampleLT(DT_Sample_4G sample)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void doSampleDX(DT_Sample_4G sample)
	{
		// TODO Auto-generated method stub

	}

}
