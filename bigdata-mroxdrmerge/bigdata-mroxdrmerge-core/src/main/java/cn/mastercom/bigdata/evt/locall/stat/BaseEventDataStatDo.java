package cn.mastercom.bigdata.evt.locall.stat;

import java.util.HashMap;
import java.util.Map;

public abstract class BaseEventDataStatDo
{
	public Map<EventDataStatKey, EventDataStruct> statModelMap;

	public BaseEventDataStatDo()
	{
		statModelMap = new HashMap<EventDataStatKey, EventDataStruct>();
	}

	public int stat(EventData eventData)
	{
		EventDataStatKey key = new EventDataStatKey(eventData.Interface, eventData.iKpiSet, eventData.iProcedureType);
		EventDataStruct statModel = statModelMap.get(key);
		if (statModel == null)
		{
			statModel = new EventDataStruct();
			statModelMap.put(key, statModel);
		}

		statModel.stat(eventData.eventStat);

		return 0;
	}

	public abstract int toString(StringBuffer sb);

}
