package cn.mastercom.bigdata.evt.locall.stat;

import java.util.HashMap;
import java.util.Map;

import cn.mastercom.bigdata.util.IWriteLogCallBack;
import org.apache.hadoop.io.Text;

import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.ResultOutputer;


public class EventDataHsrTrainsegCellStat extends BaseStatDo
{
	private Map<String, EventDataHsrTrainsegCell> dataMap;
	private int stime;
	private int etime;
	private int resultTBName;

	private ResultOutputer resultOutputer;
	private Text curText;
	private StringBuffer sb;
	private String tmStr;

	public EventDataHsrTrainsegCellStat(int stime, int etime, int resultTBName,
			ResultOutputer resultOutputer)
	{
		this.stime = stime;
		this.etime = etime;
		this.resultOutputer = resultOutputer;
		this.resultTBName = resultTBName;
		curText = new Text();
		dataMap = new HashMap<String, EventDataHsrTrainsegCell>();
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
		tmStr =  event.iCityID + "," + event.lTrainKey + "," + event.iSegmentId + ","+event.iEci + "," 
		+ stime;
		EventDataHsrTrainsegCell item = dataMap.get(tmStr);
		if (item == null)
		{
			item = new EventDataHsrTrainsegCell(event.iCityID, event.iEci, stime,event.lTrainKey, event.iSegmentId);
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
		for (EventDataHsrTrainsegCell item : dataMap.values())
		{

			try
			{
				sb.delete(0, sb.length());
				item.toString(sb);
				curText.set(sb.toString());
				resultOutputer.pushData(resultTBName, sb.toString());
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"EventDataHsrTrainsegCellStat.outFinalReusltSub Exception",
						"EventDataHsrTrainsegCellStat.outFinalReusltSub Exception: " + e.getMessage(),e);
			}
		}
		return 0;
	}

	public class EventDataHsrTrainsegCell extends BaseEventDataStatDo
	{
		protected int iCityID;
		protected long iECI;
		protected int iTime;
		
		protected long lTrainKey;
		protected int iSegmentId;

		public EventDataHsrTrainsegCell(int iCityID, long iECI, int iTime,long lTrainKey,int iSegmentId)
		{
			super();

			this.iCityID = iCityID;
			this.iECI = iECI;
			this.iTime = iTime;
			
			this.lTrainKey = lTrainKey;
			this.iSegmentId = iSegmentId;
		}

		@Override
		public int toString(StringBuffer sb)
		{
			int pos = 0;
			for (Map.Entry<EventDataStatKey, EventDataStruct> statModelEntry : statModelMap.entrySet())
			{
				pos++;
				sb.append(iCityID);sb.append("\t");
				sb.append(lTrainKey);sb.append("\t");
				sb.append(iSegmentId);sb.append("\t");
				sb.append(iECI);sb.append("\t");
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
