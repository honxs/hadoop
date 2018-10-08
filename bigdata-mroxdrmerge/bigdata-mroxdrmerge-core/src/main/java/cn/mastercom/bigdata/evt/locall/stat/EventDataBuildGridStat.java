package cn.mastercom.bigdata.evt.locall.stat;

import java.util.HashMap;
import java.util.Map;

import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.ResultOutputer;


public class EventDataBuildGridStat extends BaseStatDo
{
	private Map<String, EventDataBuildGrid> dataMap;
	private int stime;
	private int etime;
	private int resultTBName;
	
	private ResultOutputer resultOutputer;
	private StringBuffer sb;
	private String tmStr;

	public EventDataBuildGridStat(int stime, int etime, int resultTBName, ResultOutputer resultOutputer)
	{
		this.stime = stime;
		this.etime = etime;
		this.resultOutputer = resultOutputer;
		this.resultTBName = resultTBName;
		
		dataMap = new HashMap<String, EventDataBuildGrid>();
		sb = new StringBuffer();
	}

	@Override
	public int statSub(Object o)
	{
		EventData event = (EventData) o;

		if(event.iBuildID <= 0)
		{
			return 0;
		}
		tmStr = event.iBuildID + ","+event.iHeight;
		EventDataBuildGrid item = dataMap.get(tmStr);
		if(item == null)
		{
			item = new EventDataBuildGrid(event.iCityID, event.iBuildID,event.iHeight, stime);
			dataMap.put(tmStr, item);
		}
		item.stat(event);		
		return 0;
	}

	@Override
	public int outDealingResultSub()
	{
		for (EventDataBuildGrid item : dataMap.values())
		{
			try
			{
				sb.delete(0, sb.length());
				item.toString(sb);
				resultOutputer.pushData(resultTBName, sb.toString());
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"EventDataBuildGridStat.outDealingReusltSub Exception",
						"EventDataBuildGridStat.outDealingReusltSub Exception: " + e.getMessage(),e);
			}
		}
		
		dataMap = new HashMap<String, EventDataBuildGrid>();
		
		return 0;
	}

	@Override
	public int outFinalReusltSub()
	{
		for (EventDataBuildGrid item : dataMap.values())
		{
			try
			{
				sb.delete(0, sb.length());
				item.toString(sb);
				resultOutputer.pushData(resultTBName, sb.toString());
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"EventDataBuildGridStat.outFinalReusltSub Exception",
						"EventDataBuildGridStat.outFinalReusltSub Exception: " + e.getMessage(),e);
			}
		}
		
		
		dataMap = new HashMap<String, EventDataBuildGrid>();	
		return 0;
	}
		
	
	
	public class EventDataBuildGrid extends BaseEventDataStatDo
	{
	    protected int iCityID;
	    protected int iBuildingID;
	    protected int iTime;
	    protected int iHeight;
	    
	    public EventDataBuildGrid(int iCityID, int iBuildingID,int iHeight, int iTime)
	    {
	    	super();
	    	
	    	this.iCityID = iCityID;
	    	this.iBuildingID = iBuildingID;	
	    	this.iTime = iTime;	
	    	this.iHeight = iHeight;
	    }


		@Override
		public int toString(StringBuffer sb)
		{
			int pos =0 ;
			for (Map.Entry<EventDataStatKey, EventDataStruct> statModelEntry : statModelMap.entrySet())
			{
				pos++;
				sb.append(iCityID);sb.append("\t");
				sb.append(iBuildingID);sb.append("\t");
				sb.append(iHeight);sb.append("\t");

				sb.append(statModelEntry.getKey().getInterface());sb.append("\t");
				sb.append(statModelEntry.getKey().getKpiset());sb.append("\t");
				sb.append(iTime);sb.append("\t");
				statModelEntry.getValue().toString(sb);	
				if(pos<statModelMap.size()){
					sb.append("\n");			
				}
			}
			return 0;
		}
	      
	      
	}
      
      
}
