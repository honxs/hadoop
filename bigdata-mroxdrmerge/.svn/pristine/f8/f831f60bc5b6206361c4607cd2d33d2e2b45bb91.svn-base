package cn.mastercom.bigdata.mro.stat.struct;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.Func;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.TimeHelper;

import java.io.Serializable;

public class UserStat implements IStat_4G,Serializable
{
	public int CityID;
	public int Time;
	public long Imsi;
	public String Msisdn = "";
	public int mmeCount;
	public int httpCount;
	public int locCount;
	public int mroCount;
	public static final String spliter = "\t";

	public UserStat()
	{
	}

	public UserStat(DT_Sample_4G sample)
	{
		CityID = sample.cityID;
		Time = FormatTime.RoundTimeForHour(sample.itime);
		Imsi = sample.IMSI;
	}

	public void doSample(DT_Sample_4G sample)
	{
		if (sample.IMSI <= 0)
		{
			return;
		}

		if (sample.MSISDN.length()>0)
		{
			Msisdn = sample.MSISDN;
		}

		mroCount ++;
	}

	public String toLine()
	{
		StringBuffer bf = new StringBuffer();
		bf.append(CityID);
		bf.append(spliter);
		bf.append(Time);
		bf.append(spliter);
		bf.append(Func.getEncrypt(Imsi));
		bf.append(spliter);
		bf.append(Func.getEncrypt(Msisdn));
		bf.append(spliter);
		bf.append(mmeCount);
		bf.append(spliter);
		bf.append(httpCount);
		bf.append(spliter);
		bf.append(locCount);
		bf.append(spliter);
		bf.append(mroCount);
		return bf.toString();
	}
	
	public String mergeStatToLine()
	{
		StringBuffer bf = new StringBuffer();
		bf.append(CityID);
		bf.append(spliter);
		bf.append(Time);
		bf.append(spliter);
		bf.append(Imsi);
		bf.append(spliter);
		bf.append(Msisdn);
		bf.append(spliter);
		bf.append(mmeCount);
		bf.append(spliter);
		bf.append(httpCount);
		bf.append(spliter);
		bf.append(locCount);
		bf.append(spliter);
		bf.append(mroCount);
		return bf.toString();
	}

	public static UserStat FillData(String[] vals, int pos)
	{
		int i = pos;
		UserStat userStat = new UserStat();
		try
		{
			userStat.CityID = Integer.parseInt(vals[i++]);
			userStat.Time = TimeHelper.getRoundDayTime(Integer.parseInt(vals[i++]));
			userStat.Imsi = Long.parseLong(vals[i++]);
			userStat.Msisdn = vals[i++];
			userStat.mmeCount = Integer.parseInt(vals[i++]);
			userStat.httpCount = Integer.parseInt(vals[i++]);
			userStat.locCount = Integer.parseInt(vals[i++]);
			userStat.mroCount = Integer.parseInt(vals[i++]);
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error,"UserStat filldata error", "", e);
		}
		return userStat;
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
