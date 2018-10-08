package cn.mastercom.bigdata.evt.locall.stat;

import java.util.HashMap;
import java.util.Map;

import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;
import org.apache.hadoop.io.Text;

import cn.mastercom.bigdata.util.ResultOutputer;

public class EventDataInGridStat extends BaseStatDo
{
	private Map<String, EventDataInGrid> dataMap;
	private int stime;
	private int etime;
	private int resultTBName;

	private ResultOutputer resultOutputer;
	private Text curText;
	private StringBuffer sb;
	private String tmStr;

	public EventDataInGridStat(int stime, int etime, int resultTBName, ResultOutputer resultOutputer)
	{
		this.stime = stime;
		this.etime = etime;
		this.resultOutputer = resultOutputer;
		this.resultTBName = resultTBName;

		dataMap = new HashMap<String, EventDataInGrid>();
		sb = new StringBuffer();
		curText = new Text();
	}

	@Override
	public int statSub(Object o)
	{
		EventData event = (EventData) o;

		if (event.iBuildID <= 0)
		{
			return 0;
		}
		

		tmStr = event.iBuildID + "," + event.iHeight + "," + event.gridItem.tllongitude + "," + event.gridItem.tllatitude;
		EventDataInGrid item = dataMap.get(tmStr);
		if (item == null)
		{
			// item = new EventDataInGrid(event.iCityID, event.iBuildID,
			// event.iHeight, event.iLongitude / 1000 * 1000,
			// event.iLatitude / 900 * 900+900, stime);
			item = new EventDataInGrid(event.iCityID, event.iBuildID, event.iHeight, event.gridItem.tllongitude,
					event.gridItem.tllatitude, event.gridItem.brlongitude, event.gridItem.brlatitude,
					stime);
			dataMap.put(tmStr, item);
		}
		item.stat(event);
		return 0;
	}

	@Override
	public int outDealingResultSub()
	{
		for (EventDataInGrid item : dataMap.values())
		{
			try
			{
				sb.delete(0, sb.length());
				item.toString(sb);
				resultOutputer.pushData(resultTBName, sb.toString());
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"EventDataInGridStat.outDealingResultSub Exception",
						"EventDataInGridStat.outDealingResultSub Exception: " + e.getMessage(),e);
			}
		}

		dataMap = new HashMap<String, EventDataInGrid>();

		return 0;
	}

	@Override
	public int outFinalReusltSub()
	{
		for (EventDataInGrid item : dataMap.values())
		{
			try
			{
				sb.delete(0, sb.length());
				item.toString(sb);
				resultOutputer.pushData(resultTBName, sb.toString());
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"EventDataInGridStat.outFinalReusltSub Exception",
						"EventDataInGridStat.outFinalReusltSub Exception: " + e.getMessage(),e);
			}
		}

		dataMap = new HashMap<String, EventDataInGrid>();
		return 0;
	}

	public class EventDataInGrid extends BaseEventDataStatDo
	{
		protected int iCityID;
		protected int iBuildingID;
		protected int iHeight;
		protected int iTLlongitude;
		protected int iTLlatitude;
		protected int iBRlongitude;
		protected int iBRlatitude;
		protected int iTime;

		public EventDataInGrid(int iCityID, int iBuildingID, int iHeight, int iTLlongitude, int iTLlatitude,
				int iBRlongitude, int iBRlatitude, int iTime)
		{
			super();

			this.iCityID = iCityID;
			this.iBuildingID = iBuildingID;
			this.iHeight = iHeight;
			this.iTLlongitude = iTLlongitude;
			this.iTLlatitude = iTLlatitude;
			this.iBRlongitude = iBRlongitude;
			this.iBRlatitude = iBRlatitude;
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
				sb.append(iBuildingID);
				sb.append("\t");
				sb.append(iHeight);
				sb.append("\t");
				sb.append(iTLlongitude);
				sb.append("\t");
				sb.append(iTLlatitude);
				sb.append("\t");
				sb.append(iBRlongitude);sb.append("\t");
				sb.append(iBRlatitude);sb.append("\t");
				sb.append(statModelEntry.getKey().getInterface());
				sb.append("\t");
				sb.append(statModelEntry.getKey().getKpiset());
				sb.append("\t");
				sb.append(iTime);
				sb.append("\t");

				statModelEntry.getValue().toString(sb);
				if(pos<statModelMap.size()){
					sb.append("\n");			
				}
			}
			return 0;
		}

	}

}
