package cn.mastercom.bigdata.evt.locall.model;

import java.util.ArrayList;
import java.util.HashMap;

import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.evt.locall.stat.EventData;
import cn.mastercom.bigdata.evt.locall.stat.EventDataStruct;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;

/**
 * 
 * @author ZhaiKaiShun 2017-10-21
 *
 */
public class VideoLagYouku
{
	// 基础数据
	public int App_Typ;
	public String Refer_URI;
	public int longitude;
	public int latitude;
	public int iCityID;
	public int City;
	public int istime;
	public String strloctp;
	public String label;
	public int ibuildid;
	public int ibuildheight;
	public long Eci;
	public int Interface;
	public int testType;
	public int iDoorType;
	public int locSource;
	public int confidentType;
	public int iAreaType;
	public int iAreaID;

	public String HOST;
	public String URI;
	public int lteScRSRP;
	public int lteScSinrUL;

	public long Procedure_Start_Time;
	public long imsi;
	public String SessionID;
	public int 首播时延;
	public int 卡顿次数;
	public int 卡顿时长;
	public int 播放时长;
	public int 播放次数;
	public int 播放成功次数;
	public HashMap<String, ArrayList<String>> sessionData;

	public long type_begin; // 视频开始播放
	public long type_end; // 视频结束播放
	public double before_duration; // 视频的加载时长，代表从广告播放结束到视频启动播放的时间间隔，单位为ms
	public double adv_before_duration; // 广告的加载时长 ms
	public double allLagTime; // 卡顿时长 ms
	public int lagNums; // 卡顿次数
	public boolean 播放成功标识 = true;
	public int position;
	public void fillOriginData(XdrDataBase xdrDataBase)
	{
		// 1. 基本经纬度的添加 添加过一次就算了
		XdrData_Http xdrData_Http = (XdrData_Http) xdrDataBase;

		Procedure_Start_Time = xdrData_Http.istime * 1000L + xdrData_Http.istimems;

		App_Typ = xdrData_Http.App_Type;

		HOST = xdrData_Http.HOST;
		URI = xdrData_Http.URI;
		Refer_URI = xdrData_Http.Refer_URI;
		longitude = xdrData_Http.iLongitude;
		latitude = xdrData_Http.iLatitude;

		// 其他的几个
		iCityID = xdrData_Http.iCityID;
		City = xdrData_Http.iCityID;
		istime = xdrData_Http.istime;
		// wtimems = xdrData_Http.wTimems;
		strloctp = "";
		label = xdrData_Http.label;
		ibuildid = xdrData_Http.ibuildid;

		ibuildheight = xdrData_Http.iheight;

		imsi = xdrData_Http.imsi;
		Eci = xdrData_Http.Eci;

		Interface = StaticConfig.INTERFACE_S1_U;

		testType = xdrData_Http.testType;
		iDoorType = xdrData_Http.iDoorType;
		locSource = xdrData_Http.locSource;

		confidentType = xdrData_Http.confidentType;
		iAreaType = xdrData_Http.iAreaType;
		iAreaID = xdrData_Http.iAreaID;
		position = xdrData_Http.position;
        lteScRSRP = xdrData_Http.LteScRSRP;
        lteScSinrUL = xdrData_Http.LteScSinrUL;
	}

	public void fillData(XdrDataBase xdrDataBase)
	{
		if(confidentType<=0){
			fillOriginData(xdrDataBase);
		}
        if(lteScRSRP==-1000000){
            lteScRSRP = xdrDataBase.LteScRSRP;
            lteScSinrUL = xdrDataBase.LteScSinrUL;
        }
		XdrData_Http xdrData_Http = (XdrData_Http)xdrDataBase;
		URI = xdrData_Http.URI;
		
		// 2. 这些数据的添加
		if (URI.contains("&type=begin"))
		{
			// 视频开始时间
			type_begin = xdrDataBase.istime * 1000 + xdrDataBase.istimems;
		}
		else if (URI.contains("&type=end"))
		{
			// 视频结束时间
			type_end = xdrDataBase.istime * 1000 + xdrDataBase.istimems;

		}

		if (URI.contains("&before_duration="))
		{
			// 视频的加载时长：ms
			before_duration = Double.parseDouble(URI.split("&before_duration=", 2)[1].split("&", 2)[0]);

		}
		if (URI.contains("adv_before_duration="))
		{
			//广告的加载时长
			adv_before_duration = Double.parseDouble(URI.split("adv_before_duration=", 2)[1].split("&", 2)[0]);

		}

		if (URI.contains("&play_load_events="))
		{
			String[] play_load_times;
			
			String play_load_events = URI.split("play_load_events=", 2)[1].split("&", 2)[0];
			
			/**
			 * TODO zhaikaishun 2017-10-21 不同的地方，分割方法还不一样，是否有一个标准
			 */
			if(MainModel.GetInstance().getCompile().Assert(CompileMark.BeiJing)){
				play_load_times = play_load_events.split("\\s+");
			}else{
				play_load_times = play_load_events.split("\\|");
			}
			// 卡顿时长
//			allLagTime = 0;
			for (int i = 0; i < play_load_times.length; i++)
			{
				try{
					String thisLagTime = play_load_times[i].split(",", 3)[1]; // 这里使用3的原因是有的数据有三个的值, 很诡异
					allLagTime = allLagTime + Double.parseDouble(thisLagTime);
				}catch(Exception e){
					LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"VideoLagYouku.format allLagTime error",
							"VideoLagYouku.format allLagTime error: " + e.getMessage(),e);
				}

			}
			// 卡顿次数
			lagNums = play_load_times.length;

		}

		if (URI.contains("video_1level_err=2004"))
		{	// 只要有一个地方有错，那么就算这个程序有错
			// 说明视频播放失败
			播放成功标识 = false;
		}

	}

	public boolean checkAvailable()
	{
		if (type_begin > 0 && type_end > 0)
		{
			return true;
		}
		else
		{
			return false;
		}

	}

	/**
	 * 
	 * @return long类型 首播时延 ms
	 */
	public double getDelay()
	{

		return before_duration + adv_before_duration;
	}

	/**
	 * 
	 * @return 播放时长
	 */
	public long getPlayTime()
	{
		return type_end - type_begin;
	}

	/**
	 * TODO 如何判断什么时候需要Event
	 * @return
	 */
	public EventData toEvent()
	{
		
		EventData eventData = new EventData();
		eventData.eventStat = new EventDataStruct();
		if(City>0){
			eventData.iCityID = City;
		}else{
			eventData.iCityID = iCityID;
		}
		
		eventData.iTime = istime;
		eventData.wTimems = 0; // istimems;//没用，给0
		eventData.strLoctp = strloctp;
		eventData.strLabel = label;
		eventData.iLongitude = longitude;
		eventData.iLatitude = latitude;
		eventData.iBuildID = ibuildid;
		eventData.iHeight = ibuildheight;
		eventData.IMSI = imsi;
		eventData.iEci = Eci;
		eventData.Interface = StaticConfig.INTERFACE_S1_U;
		eventData.position = position;
		eventData.iKpiSet = 2;
		eventData.iProcedureType = 1;

		eventData.iTestType = testType;
		eventData.iDoorType = iDoorType;
		eventData.iLocSource = locSource;
		eventData.confidentType = confidentType;
		eventData.iAreaType = iAreaType;
		eventData.iAreaID = iAreaID;
		eventData.lteScRSRP = lteScRSRP;
		eventData.lteScSinrUL = lteScSinrUL;

		/**
		 * SessionID到底放在哪里，貌似这个不太容易做统计，除非在eventData中加一个字段视频ID
		 */
		eventData.eventStat.strvalue[1] = SessionID;
		eventData.eventStat.fvalue[0] = getDelay(); //首播时延: 单位 毫秒
		eventData.eventStat.fvalue[1] = lagNums;  //卡顿次数
		eventData.eventStat.fvalue[2] = allLagTime; //卡顿总时长，单位 ms
		eventData.eventStat.fvalue[3] = getPlayTime(); // 播放时长，单位 ms
		eventData.eventStat.fvalue[4] = 1;  //播放次数
		eventData.eventStat.fvalue[5] = 播放成功标识 == true ? 0 : 1;  //播放失败次数
		
		return eventData;
	}

}
