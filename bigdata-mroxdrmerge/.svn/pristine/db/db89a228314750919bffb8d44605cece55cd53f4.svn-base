package cn.mastercom.bigdata.evt.locall.stat;

import java.util.HashMap;
import java.util.Map;

import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.*;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;


public class EventDataImsiStat extends BaseStatDo
{
	private Map<Long, EventDataImsi> dataMap;
	private int stime;
	private int etime;
	private int resultTBName;

	private ResultOutputer resultOutputer;
	private StringBuffer sb;

	public EventDataImsiStat(int stime, int etime, int resultTBName, ResultOutputer resultOutputer)
	{
		this.stime = stime;
		this.etime = etime;
		this.resultOutputer = resultOutputer;
		this.resultTBName = resultTBName;
		dataMap = new HashMap<Long, EventDataImsi>();
		sb = new StringBuffer();
	}

	@Override
	public int statSub(Object o)
	{
		EventData event = (EventData) o;

		if (event.IMSI <= 0)
		{
			return 0;
		}
		EventDataImsi item = dataMap.get(event.IMSI);
		if (item == null)
		{
			item = new EventDataImsi(event.iCityID, event.IMSI, stime);
			dataMap.put(item.imsi, item);
		}
		item.stat(event);
		return 0;
	}

	@Override
	public int outDealingResultSub()
	{
		return 0;
	}

	@Override
	public int outFinalReusltSub()
	{
		for (EventDataImsi item : dataMap.values())
		{

			try
			{
				sb.delete(0, sb.length());
				item.toString(sb);
				resultOutputer.pushData(resultTBName, sb.toString());
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"EventDataImsiStat.outFinalReusltSub Exception",
						"EventDataImsiStat.outFinalReusltSub Exception: " + e.getMessage(),e);
			}
		}
		return 0;
	}

	public class EventDataImsi extends BaseEventDataStatDo
	{
		protected int iCityID;
		protected long imsi;
		protected int iTime;

		public EventDataImsi(int iCityID, long imsi, int iTime)
		{
			super();

			this.iCityID = iCityID;
			this.imsi = imsi;
			this.iTime = iTime;
		}

		@Override
		public int toString(StringBuffer sb)
		{
			int pos = 0;
			for (Map.Entry<EventDataStatKey, EventDataStruct> statModelEntry : statModelMap.entrySet())
			{
				pos++;
				sb.append(iCityID);
				sb.append("\t");
				sb.append(imsi);
				sb.append("\t");
				sb.append(statModelEntry.getKey().getInterface());
				sb.append("\t");
				sb.append(statModelEntry.getKey().getKpiset());
				sb.append("\t");
				sb.append(iTime);
				sb.append("\t");
				// sb.append(statModelEntry.getKey().getProcedureType());sb.append("\t");
				statModelEntry.getValue().toString(sb);
				if (pos < statModelMap.size())
				{
					sb.append("\n");
				}
			}
			return 0;
		}

	}

}
