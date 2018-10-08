package cn.mastercom.bigdata.evt.locall.stat;

import java.util.ArrayList;
import java.util.HashMap;

import cn.mastercom.bigdata.evt.locall.model.HttpPage;
import cn.mastercom.bigdata.evt.locall.model.XdrDataBase;
import cn.mastercom.bigdata.evt.locall.model.XdrData_Http;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;


public class HttpPageDeal
{
	public static DataStater dataStater = null;

	/**
	 * 对http的数据进行判断，如果满足一定的条件，那么就进行一个toEvent话单，如果满足话单吐出的条件，则进行吐出
	 * @param xdrDataBaseList
	 * @return
	 */
	public static ArrayList<EventData> deal(ArrayList<XdrDataBase> xdrDataBaseList)
	{

		ArrayList<EventData> eventDataListAll = new ArrayList<EventData>();

		HashMap<String, HttpPage> httpHuaDangMap = new HashMap<>();

		for (XdrDataBase xdrData : xdrDataBaseList)
		{
			XdrData_Http xdrDataHttp = (XdrData_Http) xdrData;

			if (xdrDataHttp.App_Type == 15 && "text/html".equals(xdrDataHttp.HTTP_content_type)
					&& !"".equals(xdrDataHttp.URI) && xdrDataHttp.TRANSACTION_TYPE == 6)
			{
				if (httpHuaDangMap.containsKey(xdrDataHttp.URI))
				{
					HttpPage httpPage = httpHuaDangMap.get(xdrDataHttp.URI);


					if (httpPage.合并话单数 == 1)
					{
						httpHuaDangMap.remove(xdrDataHttp.URI);
						HttpPage huaPage = new HttpPage();
						huaPage.loadData(xdrDataHttp);
						huaPage.statData(xdrDataHttp);
						httpHuaDangMap.put(xdrDataHttp.URI, huaPage);
						continue;
					}
					else if ((xdrDataHttp.istime * 1000L + xdrDataHttp.istimems
							- httpPage.Procedure_Start_Time) < 10000)
					{
						continue;
					}

					if (httpPage.合并话单数 > 5 && (httpPage.Procedure_End_Time - httpPage.Procedure_Start_Time) < 60000)
					{
						// 吐出
						ArrayList<EventData> eventDataList = httpPage.toEventData();
						eventDataListAll.addAll(eventDataList);
						httpHuaDangMap.remove(xdrDataHttp.URI);

						HttpPage huaPage = new HttpPage();
						huaPage.loadData(xdrDataHttp);
						huaPage.statData(xdrDataHttp);
						httpHuaDangMap.put(xdrDataHttp.URI, huaPage);
					}

				}else{
					HttpPage huaPage = new HttpPage();
					huaPage.loadData(xdrDataHttp);
					huaPage.statData(xdrDataHttp);
					httpHuaDangMap.put(xdrDataHttp.URI, huaPage);
				}

			}

			else if (xdrDataHttp.TRANSACTION_TYPE == 6)
			{
				if (!"".equals(xdrDataHttp.Refer_URI) && httpHuaDangMap.containsKey(xdrDataHttp.Refer_URI))
				{

					HttpPage thisHuaDang = httpHuaDangMap.get(xdrDataHttp.Refer_URI);

					// 1. 页面元素的开始时间-上一次的结束时间小于2000毫秒
					long dtime = 2000L;
					if(MainModel.GetInstance().getCompile().Assert(CompileMark.HaiNan)){
						dtime = 10000L;
					}
					
					if (xdrDataHttp.istime * 1000L + xdrDataHttp.istimems - thisHuaDang.lastEndTime <= dtime)
					{
						thisHuaDang.statData(xdrDataHttp);

					}
				}
			}
		}

		for (String url : httpHuaDangMap.keySet())
		{
			HttpPage httpHuaDang = httpHuaDangMap.get(url);
			if (httpHuaDang.合并话单数 > 5 && (httpHuaDang.Procedure_End_Time - httpHuaDang.Procedure_Start_Time) < 60000)
			{
				// 吐出
				ArrayList<EventData> eventDataList = httpHuaDang.toEventData();
				eventDataListAll.addAll(eventDataList);
			}
		}
		httpHuaDangMap.clear();
		return eventDataListAll;

	}

}
