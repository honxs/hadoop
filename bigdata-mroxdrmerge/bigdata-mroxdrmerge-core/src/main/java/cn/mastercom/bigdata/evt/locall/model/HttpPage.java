package cn.mastercom.bigdata.evt.locall.model;



import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;

import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.evt.locall.stat.EventData;
import cn.mastercom.bigdata.evt.locall.stat.EventDataStruct;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.DataAdapterConf.ParseItem;
import cn.mastercom.bigdata.util.DataAdapterReader;

public class HttpPage extends XdrDataBase
{
	public long Procedure_Start_Time;
	public long a_Procedure_End_time;
	public long Procedure_End_Time;
	public int App_Typ;
	public int App_Status = -1;
	public double DL_Data;
	public long 最后一条话单的HTTP最后一个内容包的时间点;
	public long 最后一条话单的HTTP最后一个Ack包的时间点;
	public String HOST;
	public String URI;
	public String Refer_URI;
	public int longitude;
	public int latitude;
	public int 合并话单数;
	public int HTTP响应成功会话数;
	public int HTTP传输完成会话数;
	// 用来判断的
	public long lastEndTime;
	public String URL;
	public int ibuildheight;
	public long Eci;
	public int Interface;

	// 用来统计的
	public int 显示时长大于5秒次数;
	/**
	 * 20180830 增加业务总时延
	 */
	private double 业务总时延;


	public void loadData(XdrData_Http xdrData_Http)
	{
		istimems = xdrData_Http.istimems;

		Procedure_Start_Time = xdrData_Http.istime * 1000L + istimems;
		a_Procedure_End_time = xdrData_Http.ietime * 1000L + ietimems;
		App_Typ = xdrData_Http.App_Type;

		HOST = xdrData_Http.HOST;
		URI = xdrData_Http.URI;
		Refer_URI = xdrData_Http.Refer_URI;
		longitude = xdrData_Http.iLongitude;
		latitude = xdrData_Http.iLatitude;
		// 其他的几个
		iCityID = xdrData_Http.iCityID;
		istime = xdrData_Http.istime;
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
		
		trainKey = xdrData_Http.trainKey;
		sectionId = xdrData_Http.sectionId;
		segmentId = xdrData_Http.segmentId;

		imei = xdrData_Http.imei;
		LteScRSRP = xdrData_Http.LteScRSRP;
		LteScSinrUL = xdrData_Http.LteScSinrUL;
	}

	public void statData(XdrData_Http xdrData_Http)
	{
		Procedure_End_Time = xdrData_Http.ietime * 1000L + xdrData_Http.ietimems;
		DL_Data = DL_Data + xdrData_Http.DL_Data;

		if (longitude <= 0 && xdrData_Http.iLongitude > 0)
		{
			longitude = xdrData_Http.iLongitude;
			latitude = xdrData_Http.iLatitude;
			ibuildid = xdrData_Http.ibuildid;
			ibuildheight = xdrData_Http.iheight;
			testType = xdrData_Http.testType;
			iDoorType = xdrData_Http.iDoorType;
			locSource = xdrData_Http.locSource;
			confidentType = xdrData_Http.confidentType;
			iAreaType = xdrData_Http.iAreaType;
			iAreaID = xdrData_Http.iAreaID;
			imei = xdrData_Http.imei;
			LteScRSRP = xdrData_Http.LteScRSRP;
			LteScSinrUL = xdrData_Http.LteScSinrUL;
		}
        if(MainModel.GetInstance().getCompile().Assert(CompileMark.SiChuan)){
            最后一条话单的HTTP最后一个内容包的时间点 = (xdrData_Http.istime * 1000000L + xdrData_Http.istimems*1000L)
                    + xdrData_Http.last_http_content_delay_ms;
            最后一条话单的HTTP最后一个Ack包的时间点 = (xdrData_Http.istime * 1000000L + xdrData_Http.istimems*1000L)
                    + xdrData_Http.last_ack_delay_ms;
        }else{
            最后一条话单的HTTP最后一个内容包的时间点 = (xdrData_Http.istime * 1000L + xdrData_Http.istimems)
                    + xdrData_Http.last_http_content_delay_ms;
            最后一条话单的HTTP最后一个Ack包的时间点 = (xdrData_Http.istime * 1000L + xdrData_Http.istimems)
                    + xdrData_Http.last_ack_delay_ms;
        }



		合并话单数++;
		if (xdrData_Http.HTTP_WAP_STATUS > 0 && xdrData_Http.HTTP_WAP_STATUS < 400 && xdrData_Http.HTTP_WAP_STATUS > 0)
		{
			HTTP响应成功会话数++;
		}
		if (xdrData_Http.HTTP_WAP_STATUS > 0 && xdrData_Http.HTTP_WAP_STATUS < 400 && xdrData_Http.HTTP_WAP_STATUS > 0
				&& xdrData_Http.businessFinishMark == 3)// &&
														// 2017-10-26我又加上后面的一位了
		// xdrData_Http.SESSION_MARK_END==3)
		{
			HTTP传输完成会话数++;
		}

		// 记录上一次的时间点
		lastEndTime = xdrData_Http.ietime * 1000L + xdrData_Http.ietimems;
		//业务总时延
		业务总时延+=xdrData_Http.TCP_RESPONSE_DELAY+xdrData_Http.TCP_CONFIRM_DELAY
				+xdrData_Http.last_http_content_delay_ms;
	}

	@Override
	public int getInterfaceCode() {
		return StaticConfig.INTERFACE_S1U_HTTP;
	}

	@Override
	public ParseItem getDataParseItem() throws IOException
	{

		return null;
	}

	@Override
	public boolean FillData_short(DataAdapterReader dataAdapterReader) throws ParseException, IOException
	{

		return false;
	}

	@Override
	public boolean FillData(DataAdapterReader dataAdapterReader) throws ParseException, IOException
	{

		return false;
	}

	@Override
	public ArrayList<EventData> toEventData()
	{
		if(MainModel.GetInstance().getCompile().Assert(CompileMark.SiChuan)){
			业务总时延 = 业务总时延/1000.0;
		}

		if (HTTP传输完成会话数 / (合并话单数 * 1.0) >= 0.9)
		{
			App_Status = 0;
		}
		else
		{
			App_Status = -1;
		}
		//因为四川的内容包时间段是微秒级别的
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.SiChuan)) {
			if (最后一条话单的HTTP最后一个内容包的时间点/1000.0 - Procedure_Start_Time > 5000)
			{
				显示时长大于5秒次数 = 1;
			}
			else
			{
				显示时长大于5秒次数 = 0;
			}
		}else {
			if (最后一条话单的HTTP最后一个内容包的时间点 - Procedure_Start_Time > 5000)
			{
				显示时长大于5秒次数 = 1;
			}
			else
			{
				显示时长大于5秒次数 = 0;
			}
		}

		ArrayList<EventData> eventDataList = new ArrayList<EventData>();
		EventData eventData = new EventData();
		eventData.eventStat = new EventDataStruct();
		eventData.iCityID = iCityID;
		eventData.iTime = istime;
		eventData.wTimems = istimems;
		eventData.strLoctp = strloctp;
		eventData.strLabel = label;
		eventData.iLongitude = longitude;
		eventData.iLatitude = latitude;
		eventData.iBuildID = ibuildid;
		eventData.iHeight = ibuildheight;
		eventData.position = position;
		eventData.IMSI = imsi;
		eventData.iEci = Eci;
		eventData.Interface = Interface;
		eventData.iKpiSet = 1;
		eventData.iProcedureType = 1;

		eventData.iTestType = testType;
		eventData.iDoorType = iDoorType;
		eventData.iLocSource = locSource;
		eventData.confidentType = confidentType;
		eventData.iAreaType = iAreaType;
		eventData.iAreaID = iAreaID;
		
		eventData.lTrainKey = trainKey;
		eventData.iSectionId = sectionId;
		eventData.iSegmentId = segmentId;
		eventData.lteScRSRP = LteScRSRP;
		eventData.lteScSinrUL = LteScSinrUL;
		eventData.imei = imei;
		// 网页显示时长
		eventData.eventStat.fvalue[8] = (最后一条话单的HTTP最后一个内容包的时间点/1000.0 - Procedure_Start_Time) / 1000.0;
		// 网页请求次数
		eventData.eventStat.fvalue[9] = 1;
		// 显示时长大于5秒次数
		eventData.eventStat.fvalue[10] = 显示时长大于5秒次数;
		// 显示成功次数
		if (App_Status == 0)
		{
			eventData.eventStat.fvalue[11] = 1;
		}
		else
		{
			eventData.eventStat.fvalue[11] = 0;
		}
		// DLData下载字节数
		eventData.eventStat.fvalue[12] = DL_Data;
        if(MainModel.GetInstance().getCompile().Assert(CompileMark.BeiJing)
                ||MainModel.GetInstance().getCompile().Assert(CompileMark.SiChuan)){
            eventData.eventDetial = new EventDataStruct();
            eventData.eventDetial.sampleType = StaticConfig.FAILURE_SAMPLE;
            eventData.eventDetial.strvalue[0] = "HTTP_PAGE";
            eventData.eventDetial.fvalue[0]=eventData.eventStat.fvalue[8];
            eventData.eventDetial.fvalue[1]=eventData.eventStat.fvalue[9];
            eventData.eventDetial.fvalue[2] = eventData.eventStat.fvalue[10];
            eventData.eventDetial.fvalue[3] = eventData.eventStat.fvalue[11];
            eventData.eventDetial.fvalue[4] = eventData.eventStat.fvalue[12];
            eventData.eventDetial.fvalue[5]=业务总时延;

        }else{
            eventData.eventDetial = null;
        }

		eventDataList.add(eventData);

		// 小包高时延，大包低速率
//		if (eventData.eventStat.fvalue[8]>=6) {
		if ((业务总时延/1000.0)>=8 && eventData.eventStat.fvalue[12]<512000) {
				EventData eventData1 = new EventData();
				eventData1.iCityID = iCityID;
				eventData1.iTime = istime;
				eventData1.wTimems = istimems;
				eventData1.strLoctp = strloctp;
				eventData1.strLabel = label;
				eventData1.iLongitude = longitude;
				eventData1.iLatitude = latitude;
				eventData1.iBuildID = ibuildid;
				eventData1.iHeight = ibuildheight;
				eventData1.position = position;
				eventData1.IMSI = imsi;
				eventData1.iEci = Eci;
				eventData1.Interface = Interface;
				eventData1.iKpiSet = 1;
				eventData1.iProcedureType = 1;
				eventData1.iTestType = testType;
				eventData1.iDoorType = iDoorType;
				eventData1.iLocSource = locSource;
				eventData1.confidentType = confidentType;
				eventData1.iAreaType = iAreaType;
				eventData1.iAreaID = iAreaID;
				eventData1.lTrainKey = trainKey;
				eventData1.iSectionId = sectionId;
				eventData1.iSegmentId = segmentId;
				eventData1.lteScRSRP = LteScRSRP;
				eventData1.lteScSinrUL = LteScSinrUL;
				eventData1.imei = imei;

				eventData1.eventStat = null;
				eventData1.eventDetial.sampleType = StaticConfig.FAILURE_SAMPLE;
				eventData1.eventDetial.strvalue[0] = "小包高时延";
				eventDataList.add(eventData1);
			}
//		}
		//DlData大于1M
//		if(eventData.eventStat.fvalue[12]>1048576){
		if(eventData.eventStat.fvalue[12]>=512000){
            if (业务总时延>0&&(DL_Data*8*1000.0/(业务总时延*1024.0))<500) {
                EventData eventData12 = new EventData();
                eventData12.iCityID = iCityID;
                eventData12.iTime = istime;
                eventData12.wTimems = istimems;
                eventData12.strLoctp = strloctp;
                eventData12.strLabel = label;
                eventData12.iLongitude = longitude;
                eventData12.iLatitude = latitude;
                eventData12.iBuildID = ibuildid;
                eventData12.iHeight = ibuildheight;
                eventData12.position = position;
                eventData12.IMSI = imsi;
                eventData12.iEci = Eci;
                eventData12.Interface = Interface;
                eventData12.iKpiSet = 1;
                eventData12.iProcedureType = 1;
                eventData12.iTestType = testType;
                eventData12.iDoorType = iDoorType;
                eventData12.iLocSource = locSource;
                eventData12.confidentType = confidentType;
                eventData12.iAreaType = iAreaType;
                eventData12.iAreaID = iAreaID;
                eventData12.lTrainKey = trainKey;
                eventData12.iSectionId = sectionId;
                eventData12.iSegmentId = segmentId;
                eventData12.lteScRSRP = LteScRSRP;
                eventData12.lteScSinrUL = LteScSinrUL;
                eventData12.imei = imei;
                eventData12.eventStat = null;
                eventData12.eventDetial.sampleType = StaticConfig.FAILURE_SAMPLE;
                eventData12.eventDetial.strvalue[0] = "大包低速率";
                eventDataList.add(eventData12);
            }
        }

		return eventDataList;
	}

	@Override
	public void toString(StringBuffer sb)
	{

	}
}
