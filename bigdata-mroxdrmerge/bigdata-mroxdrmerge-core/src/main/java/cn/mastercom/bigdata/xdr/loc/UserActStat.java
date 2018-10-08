package cn.mastercom.bigdata.xdr.loc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.util.StringUtil;

public class UserActStat
{
	public long imsi;
	public String msisdn;

	public Map<Integer, UserActTime> userActTimeMap = new HashMap<Integer, UserActTime>();

	public UserActStat(long imsi, String msisdn)
	{
		this.imsi = imsi;
		this.msisdn = msisdn;
	}

	public void dealSample(DT_Sample_4G sample)
	{
		int tmTime = sample.itime / 3600 * 3600;
		UserActTime userActTime = userActTimeMap.get(tmTime);
		if (userActTime == null)
		{
			userActTime = new UserActTime(tmTime, tmTime + 3600);
			userActTimeMap.put(tmTime, userActTime);
		}
		userActTime.dealSample(sample);
	}

	public void dealMro(DT_Sample_4G sample)
	{
		int tmTime = sample.itime / 3600 * 3600;
		UserActTime userActTime = userActTimeMap.get(tmTime);
		if (userActTime == null)
		{
			userActTime = new UserActTime(tmTime, tmTime + 3600);
			userActTimeMap.put(tmTime, userActTime);
		}
	}

	public void finalStat()
	{
		for (UserActTime userActTime : userActTimeMap.values())
		{
			userActTime.finalStat();
		}
	}

	public class UserActTime
	{
		public int stime;
		public int etime;

		public int oldtime;
		public int duration;
		public int cellcount = 0;
		public int xdrcountTotal;

		public Map<Long, UserAct> userActMap;
		private Set<Long> cellset;

		public UserActTime(int stime, int etime)
		{
			this.stime = stime;
			this.etime = etime;

			oldtime = -1;
			xdrcountTotal = 0;
			userActMap = new HashMap<Long, UserAct>();
			cellset = new HashSet<Long>();
		}

		public void dealSample(DT_Sample_4G sample)
		{
			if (oldtime > sample.itime || sample.itime <= 0)
			{
				return;
			}

			if (oldtime <= 0)
			{
				oldtime = sample.itime;
			}

			UserAct userAct = userActMap.get(sample.Eci);
			if (userAct == null)
			{
				userAct = new UserAct(sample.Eci);
				userActMap.put(sample.Eci, userAct);
			}
			userAct.dealSample(sample, sample.itime - oldtime);

			if (!cellset.contains(sample.Eci))
			{
				cellset.add(sample.Eci);
			}
			duration += sample.itime - oldtime;
			xdrcountTotal++;

			oldtime = sample.itime;
		}

		public void finalStat()
		{
			cellcount = cellset.size();

			List<UserAct> userActs = new ArrayList<>(userActMap.values());
			Collections.sort(userActs, new Comparator<UserAct>()
			{
				@Override
				public int compare(UserAct a, UserAct b)
				{
					return b.cellduration - a.cellduration;
				}
			});

			int sn = 1;
			for (UserAct userAct : userActs)
			{
				userAct.sn = sn;
				if (duration > 0)
				{
					userAct.durationrate = userAct.cellduration * 1.0 / duration;
				}
				sn++;
			}
		}

	}

	public class UserAct
	{
		public long eci;
		public int cellduration;
		public int xdrcount;
		public double durationrate;
		public int sn;

		public UserAct(long eci)
		{
			this.eci = eci;

			durationrate = 0;
		}

		public void dealSample(DT_Sample_4G sample, int duration)
		{
			cellduration += duration;
			xdrcount++;
		}
	}
}
