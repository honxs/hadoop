package cn.mastercom.bigdata.mro.stat.struct;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.NC_LTE;

import cn.mastercom.bigdata.util.FormatTime;
import cn.mastercom.bigdata.util.Func;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;

public class Stat_Resident implements IStat_4G,Serializable
{

	public int cityID;
	public long IMSI;
	public int hour;
	public long eci;
	public float RSRPValue;
	public int MRCnt; 
	public int LteNcEci1;
	public float LteNcRSRP1Value;
	public int LteNcRSRP1Cnt;
	public int LteNcEci2;
	public float LteNcRSRP2Value;
	public int LteNcRSRP2Cnt;
	public int LteNcEci3;
	public float LteNcRSRP3Value;
	public int LteNcRSRP3Cnt;
	public int LteNcEci4;
	public float LteNcRSRP4Value;
	public int LteNcRSRP4Cnt;
	public int LteNcEci5;
	public float LteNcRSRP5Value;
	public int LteNcRSRP5Cnt;
	public int LteNcEci6;
	public float LteNcRSRP6Value;
	public int LteNcRSRP6Cnt;
	// key:邻区eci
	private Map<Long, CountAndRSRP> countMap = new HashMap<Long, CountAndRSRP>();

	public void doFirstSample(DT_Sample_4G sample)
	{

		long time = sample.itime * 1000L;

		Calendar instance = Calendar.getInstance();
		instance.setTime(new Date(time));
		// 绝对秒转为小时
		hour = instance.get(Calendar.HOUR_OF_DAY);

		cityID = sample.cityID;
		IMSI = sample.IMSI;

		eci = sample.Eci;
	}

	@Override
	public void doSample(DT_Sample_4G sample)
	{
		long time = sample.itime * 1000L;
		Calendar instance = Calendar.getInstance();
		instance.setTime(new Date(time));
		hour = instance.get(Calendar.HOUR_OF_DAY);

		RSRPValue += sample.LteScRSRP;
		MRCnt++;
		NC_LTE[] tlte = sample.tlte;
		for (NC_LTE nc_LTE : tlte)
		{
			if (nc_LTE.LteNcEci == -1000000)
			{
				continue;
			}
			CountAndRSRP countAndRSRP = countMap.get(nc_LTE.LteNcEci);
			if (countAndRSRP == null)
			{
				countAndRSRP = new CountAndRSRP(0, 0);
				countMap.put(nc_LTE.LteNcEci, countAndRSRP);
			}

			countAndRSRP.count++;
			countAndRSRP.rsrpSum += nc_LTE.LteNcRSRP;
		}

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

	@Override
	public String toLine()
	{
		ArrayList<EciWithCount> arrayList = new ArrayList<EciWithCount>();
		Iterator<Entry<Long, CountAndRSRP>> iterator = countMap.entrySet().iterator();
		// 找到出现次数最多的六个邻区
		while (iterator.hasNext())
		{
			Entry<Long, CountAndRSRP> next = iterator.next();
			Long eci = next.getKey();
			CountAndRSRP countAndRSRP = next.getValue();
			arrayList.add(new EciWithCount(eci, countAndRSRP));

		}
		StringBuilder sb = null;
		String ecisToLine = null;
		// 按出现次数排序
		if (arrayList.size() > 0)
		{
			Collections.sort(arrayList);

			ecisToLine = ecisToLine(arrayList);

		}

		String res = cityID + "\t" + Func.getEncrypt(IMSI) + "\t" + hour + "\t" + eci + "\t" + RSRPValue + "\t" + MRCnt;
		StringBuilder stringBuilder = new StringBuilder();
		//若没有邻区信息，则补齐
		if (ecisToLine == null)
		{
			stringBuilder.append(0).append("\t").append(0f).append("\t").append(0);

			stringBuilder.append("\t").append(0).append("\t").append(0f).append("\t").append(0);

			stringBuilder.append("\t").append(0).append("\t").append(0f).append("\t").append(0);
			stringBuilder.append("\t").append(0).append("\t").append(0f).append("\t").append(0);
			stringBuilder.append("\t").append(0).append("\t").append(0f).append("\t").append(0);
			stringBuilder.append("\t").append(0).append("\t").append(0f).append("\t").append(0);
		
			ecisToLine=stringBuilder.toString();
		}

		return res + "\t" + ecisToLine;

	}

	// 将多个邻区信息合并为一行输出
	private String ecisToLine(ArrayList<EciWithCount> arrayList)
	{
		if (arrayList == null || arrayList.size() == 0)
		{
			return null;
		}
		StringBuilder sb = null;
		for (int i = 0; i < 6; i++)
		{

			if (i < arrayList.size())
			{

				EciWithCount eciWithCount = arrayList.get(i);
				if (sb == null)
				{
					sb = new StringBuilder();
				} else
				{
					sb.append("\t");
				}

				sb.append(eciWithCount.eci).append("\t").append(eciWithCount.countAndRSRP.rsrpSum).append("\t")
						.append(eciWithCount.countAndRSRP.count);
			}
			//// 若邻区不满6个则补齐
			else
			{
				sb.append("\t").append(0).append("\t").append(0f).append("\t").append(0);
			}
		}

		return sb.toString();

	}

	public static Stat_Resident FillData(String[] vals, int pos)
	{
	   
		int i = pos;  
		Stat_Resident stat_Resident = new Stat_Resident();
		try
		{
			stat_Resident.cityID = Integer.parseInt(vals[i++]);
			stat_Resident.IMSI = Long.parseLong(vals[i++]);
			stat_Resident.hour = Integer.parseInt(vals[i++]);
			stat_Resident.eci = Integer.parseInt(vals[i++]);
			stat_Resident.RSRPValue = Float.parseFloat(vals[i++]);
			stat_Resident.MRCnt = Integer.parseInt(vals[i++]);

			/*if (vals.length <= i)  
			{
				return stat_Resident;
			}*/
			stat_Resident.LteNcEci1 = Integer.parseInt(vals[i++]);
			stat_Resident.LteNcRSRP1Value = Float.parseFloat(vals[i++]);
			stat_Resident.LteNcRSRP1Cnt = Integer.parseInt(vals[i++]);

			/*if (vals.length <= i)
			{
				return stat_Resident;
			}*/
			stat_Resident.LteNcEci2 = Integer.parseInt(vals[i++]);
			stat_Resident.LteNcRSRP2Value = Float.parseFloat(vals[i++]);
			stat_Resident.LteNcRSRP2Cnt = Integer.parseInt(vals[i++]);

			/*if (vals.length <= i)
			{
				return stat_Resident;
			}*/
			stat_Resident.LteNcEci3 = Integer.parseInt(vals[i++]);
			stat_Resident.LteNcRSRP3Value = Float.parseFloat(vals[i++]);
			stat_Resident.LteNcRSRP3Cnt = Integer.parseInt(vals[i++]);

			/*if (vals.length <= i)
			{
				return stat_Resident;
			}*/
			stat_Resident.LteNcEci4 = Integer.parseInt(vals[i++]);
			stat_Resident.LteNcRSRP4Value = Float.parseFloat(vals[i++]);
			stat_Resident.LteNcRSRP4Cnt = Integer.parseInt(vals[i++]);

			/*if (vals.length <= i)
			{
				return stat_Resident;
			}*/
			stat_Resident.LteNcEci5 = Integer.parseInt(vals[i++]);
			stat_Resident.LteNcRSRP5Value = Float.parseFloat(vals[i++]);
			stat_Resident.LteNcRSRP5Cnt = Integer.parseInt(vals[i++]);

			/*if (vals.length <= i)
			{
				return stat_Resident;
			}*/
			stat_Resident.LteNcEci6 = Integer.parseInt(vals[i++]);
			stat_Resident.LteNcRSRP6Value = Float.parseFloat(vals[i++]);
			stat_Resident.LteNcRSRP6Cnt = Integer.parseInt(vals[i++]);
		} catch (Exception e)
		{

			LOGHelper.GetLogger().writeLog(LogType.error,"Stat_Resident filldata error", "", e);
		}
		return stat_Resident;

	}

	public static class CountAndRSRP
	{
		public int count;
		public float rsrpSum;

		public CountAndRSRP(int count, float rsrpSum)
		{
			super();
			this.count = count;
			this.rsrpSum = rsrpSum;
		}
	}

	public static class EciWithCount implements Comparable<EciWithCount>
	{
		long eci;
		CountAndRSRP countAndRSRP;

		public EciWithCount(long eci, CountAndRSRP countAndRSRP)
		{
			super();
			this.eci = eci;
			this.countAndRSRP = countAndRSRP;
		}

		@Override
		public int compareTo(EciWithCount o)
		{
			if (o.countAndRSRP.count < countAndRSRP.count)
			{
				return -1;
			} else if (o.countAndRSRP.count > countAndRSRP.count)
			{
				return 1;
			}
			return 0;
		}

	}

	static class EciAndRsrp
	{
		long eci;

		public EciAndRsrp(long eci, int rsrp)
		{
			super();
			this.eci = eci;
			this.rsrp = rsrp;
		}

		int rsrp;
	}

}
