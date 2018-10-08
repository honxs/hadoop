package cn.mastercom.bigdata.evt.locall.model;

import java.util.ArrayList;
import java.util.HashMap;

import cn.mastercom.bigdata.evt.locall.stat.EventData;



public class StatVideoLag
{

	/**
	 * 
	 * @param xdrDataBaseList
	 * @return 满足视频统计后的toEvent后的List
	 */
	public ArrayList<EventData> statVideoLag(ArrayList<XdrDataBase> xdrDataBaseList)
	{

		HashMap<String, ArrayList<XdrDataBase>> sessionMapData = new HashMap<>(); // 优酷
		HashMap<String, ArrayList<XdrDataBase>> uuidMapData = new HashMap<>(); // 乐视
		ArrayList<XdrDataBase> aiqiyiList = new ArrayList<XdrDataBase>(); // 爱奇艺

		// put到sessionData中去
		for (XdrDataBase xdrDataBase : xdrDataBaseList)
		{
			//
			XdrData_Http xdrData_Http = (XdrData_Http) xdrDataBase;
			if (xdrData_Http.HOST.contains("statis.api.3g.youku.com"))
			{
				if (xdrData_Http.URI.contains("sessionid="))
				{

					String sessionID = xdrData_Http.URI.split("sessionid=", 2)[1].split("&", 2)[0];
					if(sessionID.equals("null")){
						continue;  //因为竟然有等于null的东西
					}
					if (sessionMapData.containsKey(sessionID))
					{
						sessionMapData.get(sessionID).add(xdrData_Http);
					}
					else
					{
						ArrayList<XdrDataBase> sessionDataList = new ArrayList<XdrDataBase>();
						sessionDataList.add(xdrData_Http);
						sessionMapData.put(sessionID, sessionDataList);
					}

				}
			}
			else if (xdrData_Http.HOST.contains("apple.www.letv.com"))
			{
				if (xdrData_Http.URI.contains("uuid="))
				{
					String uuID = xdrData_Http.URI.split("uuid=", 2)[1].split("&", 2)[0];
					/**
					 * TODO 不确定是否是这样
					 */
					if (uuID.endsWith("_0") || uuID.endsWith("_1") || uuID.endsWith("_2"))
					{
						uuID = uuID.substring(0, uuID.length() - 2);
					}
					if (uuidMapData.containsKey(uuID))
					{
						uuidMapData.get(uuID).add(xdrData_Http);
					}
					else
					{
						ArrayList<XdrDataBase> arrayList = new ArrayList<XdrDataBase>();
						arrayList.add(xdrData_Http);
						uuidMapData.put(uuID, arrayList);
					}

				}

			}
			else if (xdrData_Http.HOST.contains("msg.71.am"))
			{
				aiqiyiList.add(xdrData_Http);
			}

		}

		ArrayList<EventData> allEventDataList = new ArrayList<EventData>();

		if (sessionMapData.size() != 0)
		{
			for (String sessionKey : sessionMapData.keySet())
			{
				ArrayList<XdrDataBase> dataList = sessionMapData.get(sessionKey);
				VideoLagYouku videoYouku = new VideoLagYouku();
				for (int i = 0; i < dataList.size(); i++)
				{
					if (i == 0)
					{
						videoYouku.fillOriginData(dataList.get(i));
					}
					videoYouku.fillData(dataList.get(i));

				}
				// 判断是否是完整的
				if (videoYouku.checkAvailable())
				{
					EventData eventData = videoYouku.toEvent();
					allEventDataList.add(eventData);
				}

			}
		}
		if (uuidMapData.size() != 0)
		{
			for (String uuidKey : uuidMapData.keySet())
			{
				ArrayList<XdrDataBase> dataList = uuidMapData.get(uuidKey);
				VideoLagLetv videoletv = new VideoLagLetv();
				for (int i = 0; i < dataList.size(); i++)
				{
					if (i == 0)
					{
						videoletv.fillOriginData(dataList.get(i));
					}
					videoletv.fillData(dataList.get(i));

				}
				// TODO 判断是否是有值 应该先check一下
				EventData eventData = videoletv.toEvent();
				allEventDataList.add(eventData);
			}
		}
		if (aiqiyiList.size() > 0)
		{
			VideoLagAiqiyi videoAiqiyi = new VideoLagAiqiyi();
			for (int i = 0; i < aiqiyiList.size(); i++)
			{
				if (i == 0)
				{
					videoAiqiyi.fillOriginData(aiqiyiList.get(i));
				}
				videoAiqiyi.fillData(aiqiyiList.get(i));
			}
			EventData eventData = videoAiqiyi.toEvent();
			// TODO 判断是否是有值 应该先check一下
			allEventDataList.add(eventData);
		}

		return allEventDataList;
	}
}
