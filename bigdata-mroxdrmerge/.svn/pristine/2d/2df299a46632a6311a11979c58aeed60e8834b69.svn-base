package cn.mastercom.bigdata.evt.locall.model;

import java.util.ArrayList;

import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.evt.locall.stat.EventData;
import cn.mastercom.bigdata.evt.locall.stat.EventDataStruct;


/**
 * 
 * @author ZhaiKaiShun 2017-10-21
 *
 */
public class VideoLagLetv
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
	public long Procedure_Start_Time;
	public long imsi;
	public int lteScRSRP;
	public int lteScSinrUL;

	public long first_video_time;
	public long last_video_time;

	public long ac_launch; // 点击视频触发播放
	public long ac_init; // 初始化
	public long act_ac; // 开始加载广告
	public long act_ab; // 开始播放广告
	public long act_ae; // 广告播放结束
	public long ac_play; // 开始播放视频
	public long ac_block; // 卡顿开始发生
	public long ac_eblock; // 卡顿结束
	public long ac_end_finish; // 退出视频或播放结束
	public boolean 是否播放成功 = true;
	public int position;
	public String uuid;
	public ArrayList<Long> blockList = new ArrayList<>();
	public ArrayList<Long> eblockList = new ArrayList<>();

	public void fillOriginData(XdrDataBase xdrDataBase)
	{

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
		lteScRSRP = xdrData_Http.LteScRSRP;
		lteScSinrUL = xdrData_Http.LteScSinrUL;
		// 记录第一条的xdr时间
		first_video_time = xdrDataBase.istime * 1000 + xdrDataBase.istimems;
		position = xdrData_Http.position;
	}

	public void fillData(XdrDataBase xdrDataBase)
	{
		if (confidentType <= 0)
		{
			fillOriginData(xdrDataBase);
		}
		if(lteScRSRP==-1000000){
			lteScRSRP = xdrDataBase.LteScRSRP;
			lteScSinrUL = xdrDataBase.LteScSinrUL;
		}

		XdrData_Http xdrData_Http = (XdrData_Http) xdrDataBase;
		URI = xdrData_Http.URI;

		// 记录最后一条xdr的时间
		last_video_time = xdrDataBase.istime * 1000 + xdrDataBase.istimems;

		uuid = URI.split("uuid=", 2)[1].split("&", 2)[0];

		// 逻辑是否是这样的，需要搞清楚
		boolean changeClarity = false; // 是否切换清晰度
		if (uuid.endsWith("_1") || uuid.endsWith("_2"))
		{
			changeClarity = true;
		}

		if (URI.contains("&ac=launch") && !changeClarity)// && !changeClarity
		{
			ac_launch = xdrDataBase.istime * 1000 + xdrDataBase.istimems;
		}

		if (URI.contains("&ac=init"))
		{
			ac_init = xdrDataBase.istime * 1000 + xdrDataBase.istimems;
		}

		if (URI.contains("act=ac&"))
		{
			act_ac = xdrDataBase.istime * 1000 + xdrDataBase.istimems;
		}

		if (URI.contains("act=ab&"))
		{
			act_ab = xdrDataBase.istime * 1000 + xdrDataBase.istimems;
		}

		if (URI.contains("act=ae&")) // 这个后面加&
		{
			act_ae = xdrDataBase.istime * 1000 + xdrDataBase.istimems;
		}

		if (URI.contains("&ac=play") && !changeClarity) // && !changeClarity
		{
			ac_play = xdrDataBase.istime * 1000 + xdrDataBase.istimems;
		}
		if (URI.contains("&ac=block"))
		{
			ac_block = xdrDataBase.istime * 1000 + xdrDataBase.istimems;

			blockList.add(ac_block);
		}
		if (URI.contains("&ac=eblock"))
		{
			ac_eblock = xdrDataBase.istime * 1000 + xdrDataBase.istimems;
			eblockList.add(ac_eblock);
		}
		if (URI.contains("&ac=end") || URI.contains("&ac=finish"))
		{
			ac_end_finish = xdrDataBase.istime * 1000 + xdrDataBase.istimems;
		}

		if (URI.contains("&err=1"))
		{
			是否播放成功 = false;
		}

	}

	/**
	 * 
	 * @return 首播时延
	 */
	public int getplay_delay()
	{
		if (ac_play == 0 && ac_launch == 0)
		{
			return 0;
		}
		if(act_ae == 0 && act_ab != 0){
			return 0;
		}
	
		if(act_ae != 0 && act_ab == 0){
			return 0;
		}
		
		int playDelay = (int) (ac_play - ac_launch - (act_ae - act_ab)) / 1000;
		if (playDelay > 0 && playDelay <= 15)
		{
			return playDelay;
		}
		else
		{
			return 0;
		}
	}

	/**
	 * TODO zhaikaishun 2017-10-16 算法挺复杂的，以后再补充
	 * 
	 * @return 卡顿次数,卡顿时长
	 */
	public int[] getLagNum(ArrayList<Long> blockTime, ArrayList<Long> endBlockTime)
	{
		int i = 0;
		int j = 0;
		int num = 0;
		long allTime = 0;
		while (i < blockTime.size() && j < endBlockTime.size())
		{
			long beginTime = 0;
			long endTime = 0;
			int index = 0;
			while (i < blockTime.size() && j < endBlockTime.size() && endBlockTime.get(j) >= blockTime.get(i))
			{
				// 只记录第一个
				if (index == 0)
				{
					beginTime = blockTime.get(i);
				}
				index++;
				i++;
			}
			while (i < blockTime.size() && j < endBlockTime.size() && endBlockTime.get(j) < blockTime.get(i))
			{
				endTime = endBlockTime.get(j);
				j++;

			}
			while (i == blockTime.size() && j < endBlockTime.size() && endBlockTime.get(j) > blockTime.get(i - 1))
			{
				endTime = endBlockTime.get(j);
				j++;
			}

			if (endTime != 0 && beginTime != 0)
			{
				num++;
				System.out.println("时间: " + (endTime - beginTime));
				if ((endTime - beginTime) < 50)
				{
					allTime += endTime - beginTime;
				}
				else
				{
					allTime += 50;
				}

			}

		}
		if (blockTime.size() > 0 && endBlockTime.size() > 0)
		{
			if (blockTime.get(blockTime.size() - 1) > endBlockTime.get(endBlockTime.size() - 1))
			{
				num++;
				allTime += 50000;
			}
		}
		// 如果有blockTime，没有endBlockTime，先不考虑

		if (num > 0)
		{
			System.out.println("次数: " + num);
			System.out.println("总时间: " + allTime);
		}

		int[] result = new int[2];
		result[0] = num;
		result[1] = (int) (allTime / 1000);

		return result;
	}

	/**
	 * 
	 * @return 播放总时长
	 */
	public long getplay_endTime()
	{
		long video_begin_time;
		long video_end_time;
		if (ac_launch == 0)
		{
			video_begin_time = first_video_time;
		}
		else
		{
			video_begin_time = ac_launch;
		}
		if (ac_end_finish == 0)
		{
			video_end_time = last_video_time;
		}
		else
		{
			video_end_time = ac_end_finish;
		}

		return video_end_time - video_begin_time;
	}

	public EventData toEvent()
	{

		EventData eventData = new EventData();
		eventData.eventStat = new EventDataStruct();
		if(City>0){
			eventData.iCityID = City;
		}else {
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
		eventData.Interface =  StaticConfig.INTERFACE_S1_U;
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

		// 原始的数据加载
		long 首播时延 = getplay_delay();
		int[] lagNumAndTime = getLagNum(blockList, eblockList);
		int 卡顿次数 = lagNumAndTime[0];
		int 卡顿总时长 = lagNumAndTime[1];
		long 播放时长 = getplay_endTime();

		eventData.eventStat.fvalue[0] = 首播时延;
		eventData.eventStat.fvalue[1] = 卡顿次数;
		eventData.eventStat.fvalue[2] = 卡顿总时长;
		eventData.eventStat.fvalue[3] = 播放时长; // 单位毫秒
		eventData.eventStat.fvalue[4] = 1; //播放次数
		eventData.eventStat.fvalue[5] = 是否播放成功 == true ? 0 : 1;  //播放失败次数

		return eventData;
	}

}
