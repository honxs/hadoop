package cn.mastercom.bigdata.evt.locall.stat;

import java.util.HashMap;
import java.util.Map;

import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;
import org.apache.hadoop.io.Text;

import cn.mastercom.bigdata.util.ResultOutputer;

public class EventDataAreaGridStat extends BaseStatDo
{
	private Map<String, EventDataAreaGrid> dataMap;
	private int stime;
	private int etime;
	private int resultTBName;
		
	private ResultOutputer resultOutputer;
	private Text curText;
	private StringBuffer sb;
	private String tmStr;
	
	public EventDataAreaGridStat(int stime, int etime, int resultTBName, ResultOutputer resultOutputer)
	{
		this.stime = stime;
		this.etime = etime;
		this.resultOutputer = resultOutputer;
		this.resultTBName = resultTBName;
		curText= new Text();
		dataMap = new HashMap<String, EventDataAreaGrid>();
		sb = new StringBuffer();
		
	}

	@Override
	public int statSub(Object o)
	{
		EventData event = (EventData) o;

		/**
		 * TODO zhaikaishun 2017-09-26 [2017-09-26]
		 * 这个buildID>0是否需要，应该是不需要，我先暂时注释掉
		 */
//		if(event.iBuildID > 0)
//		{
//			return 0;
//		}
		
		tmStr =event.iCityID + "," + event.iAreaType + "," + event.iAreaID+","+event.gridItem.tllongitude + ","
		+ event.gridItem.tllatitude+","+stime;
		EventDataAreaGrid item = dataMap.get(tmStr);
		if(item == null)
		{
			item = new EventDataAreaGrid(event.iCityID, event.gridItem.tllongitude, event.gridItem.tllatitude,
					event.gridItem.brlongitude,event.gridItem.brlatitude, stime,event.iAreaType,event.iAreaID);
			dataMap.put(tmStr, item);
		}
		item.stat(event);		
		return 0;
	}

	@Override
	public int outDealingResultSub()
	{
		for (EventDataAreaGrid item : dataMap.values())
		{
			try
			{
				sb.delete(0, sb.length());
				item.toString(sb);
				resultOutputer.pushData(resultTBName, sb.toString());
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"EventDataAreaGridStat" +
								".outDealingResultSub Exception",
						"EventDataAreaGridStat.outDealingResultSub Exception: " + e.getMessage(),e);
			}
		}
		
		dataMap = new HashMap<String, EventDataAreaGrid>();
		
		return 0;
	}

	@Override
	public int outFinalReusltSub()
	{
		for (EventDataAreaGrid item : dataMap.values())
		{
			try
			{
				sb.delete(0, sb.length());
				item.toString(sb);
				resultOutputer.pushData(resultTBName, sb.toString());
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"EventDataAreaGridStat.outFinalReusltSub Exception",
						"EventDataAreaGridStat.outFinalReusltSub Exception: " + e.getMessage(),e);
			}
		}
		dataMap = new HashMap<String, EventDataAreaGrid>();	
		return 0;
	}
	
	
	
	
	
	public class EventDataAreaGrid extends BaseEventDataStatDo
	{
	    protected int iCityID;
	    protected int iLongitude;
	    protected int iLatitude;
	    protected int iTime;
	    protected int iBRlongitude;
	    protected int iBRlatitude;
	    
	    protected int iAreaType;
	    protected int iAreaID;
	    
	    public EventDataAreaGrid(int iCityID, int iLongitude, int iLatitude,int iBRlongitude,
	    		int iBRlatitude, int iTime,int iAreaType,int iAreaID)
	    {
	    	super();
	    	
	    	this.iCityID = iCityID;
	    	this.iLongitude = iLongitude;
	    	this.iLatitude = iLatitude;
	    	this.iBRlongitude = iBRlongitude;
	    	this.iBRlatitude = iBRlatitude;
	    	this.iTime = iTime; 	
	    	
	    	this.iAreaType = iAreaType;
	    	this.iAreaID = iAreaID;
	    }


		@Override
		public int toString(StringBuffer sb)
		{
			int pos =0;
			for (Map.Entry<EventDataStatKey, EventDataStruct> statModelEntry : statModelMap.entrySet())
			{
				pos++;
				sb.append(iCityID);sb.append("\t");
				sb.append(iAreaType);sb.append("\t");
				sb.append(iAreaID);sb.append("\t");
				sb.append(iLongitude);sb.append("\t");
				sb.append(iLatitude);sb.append("\t");
				sb.append(iBRlongitude);sb.append("\t");
				sb.append(iBRlatitude);sb.append("\t");
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
