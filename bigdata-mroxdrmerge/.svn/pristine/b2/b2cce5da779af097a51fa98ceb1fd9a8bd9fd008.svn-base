package cn.mastercom.bigdata.evt.locall.stat;

import java.util.HashMap;
import java.util.Map;

import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;
import org.apache.hadoop.io.Text;

import cn.mastercom.bigdata.util.ResultOutputer;


public class EventDataBuildCellPosStat extends BaseStatDo
{
	private Map<String, EventDataBuildCellPos> dataMap;
	private int stime;
	private int etime;
	private int resultTBName;
	
	private ResultOutputer resultOutputer;
	private Text curText;
	private StringBuffer sb;
	private String tmStr;

	public EventDataBuildCellPosStat(int stime, int etime, int resultTBName, ResultOutputer resultOutputer)
	{
		this.stime = stime;
		this.etime = etime;
		this.resultOutputer = resultOutputer;
		this.resultTBName = resultTBName;
		
		dataMap = new HashMap<String, EventDataBuildCellPos>();
		sb = new StringBuffer();
		curText= new Text();
	}

	@Override
	public int statSub(Object o)
	{
		EventData event = (EventData) o;

		if(event.iBuildID <= 0 || event.iEci <= 0)
		{
			return 0;
		}
		tmStr = event.iEci + "," + event.iBuildID+","+event.iHeight+","+event.position;
		EventDataBuildCellPos item = dataMap.get(tmStr);
		if(item == null)
		{
			item = new EventDataBuildCellPos(event.iCityID, event.iBuildID, event.iHeight, event.iEci, stime, event.position);
			dataMap.put(tmStr, item);
		}
		item.stat(event);		
		return 0;
	}

	@Override
	public int outDealingResultSub()
	{
		for (EventDataBuildCellPos item : dataMap.values())
		{
			try
			{
				sb.delete(0, sb.length());
				item.toString(sb);
				resultOutputer.pushData(resultTBName, sb.toString());
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"EventDataBuildCellPosStat.outDealingResultSub Exception",
						"EventDataBuildCellPosStat.outDealingResultSub Exception: " + e.getMessage(),e);
			}
		}
		
		dataMap = new HashMap<String, EventDataBuildCellPos>();
		
		return 0;
	}

	@Override
	public int outFinalReusltSub()
	{
		for (EventDataBuildCellPos item : dataMap.values())
		{
			try
			{
				sb.delete(0, sb.length());
				item.toString(sb);
				resultOutputer.pushData(resultTBName, sb.toString());
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"EventDataBuildCellPosStat.outFinalReusltSub Exception",
						"EventDataBuildCellPosStat.outFinalReusltSub Exception: " + e.getMessage(),e);
			}
		}
		
		
		dataMap = new HashMap<String, EventDataBuildCellPos>();	
		return 0;
	}
	
	
	
	
	public class EventDataBuildCellPos extends BaseEventDataStatDo
	{
	    protected int iCityID;
	    protected int iBuildingID;
	    protected int iHeight;
	    protected long iECI;
	    protected int iTime;
	    protected int position;
	    
	    public EventDataBuildCellPos(int iCityID, int iBuildingID, int iHeight, long iECI, int iTime, int position)
	    {
	    	super();
	    	
	    	this.iCityID = iCityID;
	    	this.iBuildingID = iBuildingID;
	    	this.iHeight = iHeight;
	    	this.position = position;
	    	this.iECI = iECI;
	    	this.iTime = iTime;
	    }


		@Override
		public int toString(StringBuffer sb)
		{
			int pos = 0;
			for (Map.Entry<EventDataStatKey, EventDataStruct> statModelEntry : statModelMap.entrySet())
			{
				pos++;
		
				sb.append(iCityID);sb.append("\t");
				sb.append(iBuildingID);sb.append("\t");
				sb.append(iHeight);sb.append("\t");
				sb.append(position);sb.append("\t");
				sb.append(iECI);sb.append("\t");
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
