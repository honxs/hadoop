package cn.mastercom.bigdata.evt.locall.stat;

import java.util.HashMap;
import java.util.Map;

import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.ResultOutputer;



public class EventDataAreaStat extends BaseStatDo
{
	private Map<String, EventDataArea> dataMap;
	private int stime;
	private int etime;
	private int resultTBName;

	private ResultOutputer resultOutputer;
	private StringBuffer sb;
	private String tmStr;

	public EventDataAreaStat(int stime, int etime, int resultTBName,
			ResultOutputer resultOutputer)
	{
		this.stime = stime;
		this.etime = etime;
		this.resultOutputer = resultOutputer;
		this.resultTBName = resultTBName;
		dataMap = new HashMap<String, EventDataArea>();
		sb = new StringBuffer();
	}

	@Override
	public int statSub(Object o)
	{
		EventData event = (EventData) o;

		if (event.iEci <= 0)
		{
			return 0;
		}
		tmStr =  event.iCityID + "," + event.iAreaType + "," + event.iAreaID + ","+ stime;
		EventDataArea item = dataMap.get(tmStr);
		if (item == null)
		{
			item = new EventDataArea(event.iCityID, stime,event.iAreaType,event.iAreaID);
			dataMap.put(tmStr, item);
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
		for (EventDataArea item : dataMap.values())
		{
			try
			{
				sb.delete(0, sb.length());
				item.toString(sb);
				resultOutputer.pushData(resultTBName, sb.toString());
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error,"EventDataAreaStat.outFinalReusltSub Exception",
						"EventDataAreaStat.outFinalReusltSub Exception: " + e.getMessage(),e);
			}
		}
		return 0;
	}

	public class EventDataArea extends BaseEventDataStatDo
	{
		protected int iCityID;
		protected int iAreaType;
		protected int iAreaID;
		protected int iTime;

		public EventDataArea(int iCityID, int iTime,int iAreaType,int iAreaID)
		{
			super();

			this.iCityID = iCityID;
			this.iTime = iTime;
			
			this.iAreaType = iAreaType;
			this.iAreaID = iAreaID;
		}

		@Override
		public int toString(StringBuffer sb)
		{
			int pos = 0;
			for (Map.Entry<EventDataStatKey, EventDataStruct> statModelEntry : statModelMap.entrySet())
			{
				pos++;
				sb.append(iCityID);sb.append("\t");
				sb.append(iAreaType);sb.append("\t");
				sb.append(iAreaID);sb.append("\t");
				sb.append(statModelEntry.getKey().getInterface());sb.append("\t");
				sb.append(statModelEntry.getKey().getKpiset());sb.append("\t");
				sb.append(iTime);sb.append("\t");
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
