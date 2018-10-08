package cn.mastercom.bigdata.evt.locall.model;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import cn.mastercom.bigdata.StructData.GridItem;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.evt.locall.stat.EventData;
import cn.mastercom.bigdata.evt.locall.stat.EventDataStruct;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.DataAdapterConf.ParseItem;
import cn.mastercom.bigdata.util.DataAdapterReader;
import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;

public class XdrData_Http extends XdrDataBase
{

	private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
	private Date tmDate = new Date();
	private static ParseItem parseItem;
	public int City;
	public String XDR_ID;
	public String MSISDN;
	public long Cell_ID;
	public long Eci;
	public String APN;
	public String App_Type_Code;
	public int App_Type;
	public int App_Sub_type;
	public int App_Status;
	public double UL_Data;
	public double DL_Data;
	public long UL_IP_Packet;
	public long DL_IP_Packet;
	public long tcp_suc_first_req_delay_ms;
	public long first_req_first_resp_delay_ms;
	public long last_http_resp_delay_ms;
	public long last_ack_delay_ms;
	public String HOST;
	public int TCP_RESPONSE_DELAY;
	public int TCP_CONFIRM_DELAY;
	public int TCP_ATT_CNT;
	public int TCP_CONN_STATUS;
	public int SESSION_MARK_END;
	public int TRANSACTION_TYPE;
	public int HTTP_WAP_STATUS;
	public int FIRST_HTTP_RES_DELAY;
	public int last_http_content_delay_ms;
	public int WTP_INTERRUPT_TYPE; //这个字段现在不用了

	public int businessFinishMark;

	public double IPThroughputUL;
	public double IPThroughputDL;

	// 统计的指标 2017-07-24 zhaikaishun加
	public long HTTP_latency;
	public long HTTP_Accept;
	public double traffic_ip_all;
	public long trans_delay1;
	public int HTTP_ATTEMPT;
	public int TCP_ATTEMPT;
	public int TCP_Accept;
	public long TCP_latency;
	// 话单统计
	public String HTTP_content_type;
	public String URI;
	public String Refer_URI;

	public StringBuffer value;

	private double LTE下行视频业务量;
	private double LTE上行视频业务量;
	private double LTE下行微信业务量 = 0;
	private double LTE上行微信业务量;
	private int LTE微信用户数 = 0;
	private double LTE下行QQ通信业务量 = 0;
	private double LTE上行QQ通信业务量 = 0;
	private double LTE下行网页浏览业务量 = 0;
	private double LTE上行网页浏览业务量 = 0;
	private int LTE网页浏览业务请求数 = 0;

	private long LTE下行视频业务量_时长;
	private long LTE下行QQ微信业务量_时长;
	private long LTE上行QQ微信业务量_时长;
	private long LTE下行网页浏览业务量_时长;

	public XdrData_Http()
	{
		super();

		clear();

		if (parseItem == null)
		{
			parseItem = MainModel.GetInstance().getEventAdapterConfig().getParseItem("LOCALL-S1-HTTP");
		}
	}

	@Override
	public int getInterfaceCode() {
		return StaticConfig.INTERFACE_S1_U;
	}

	public void clear()
	{
		XDR_ID = "";
		MSISDN = "";
		imei = "";
		Cell_ID = StaticConfig.Int_Abnormal;
		Eci = StaticConfig.Int_Abnormal;
		APN = "";
		App_Type_Code = "";
		App_Type = StaticConfig.Int_Abnormal;
		App_Sub_type = StaticConfig.Int_Abnormal;
		App_Status = StaticConfig.Int_Abnormal;
		UL_Data = 0;
		DL_Data = StaticConfig.Int_Abnormal;
		UL_IP_Packet = StaticConfig.Int_Abnormal;
		DL_IP_Packet = StaticConfig.Int_Abnormal;
		tcp_suc_first_req_delay_ms = 0;
		first_req_first_resp_delay_ms = 0;
		last_http_resp_delay_ms = 0;
		last_ack_delay_ms = 0;
		HOST = "";
		TCP_RESPONSE_DELAY = StaticConfig.Int_Abnormal;
		TCP_CONFIRM_DELAY = StaticConfig.Int_Abnormal;
		TCP_ATT_CNT = StaticConfig.Int_Abnormal;
		TCP_CONN_STATUS = StaticConfig.Int_Abnormal;
		SESSION_MARK_END = StaticConfig.Int_Abnormal;
		TRANSACTION_TYPE = StaticConfig.Int_Abnormal;
		HTTP_WAP_STATUS = StaticConfig.Int_Abnormal;
		FIRST_HTTP_RES_DELAY = StaticConfig.Int_Abnormal;
		last_http_content_delay_ms = 0;
		WTP_INTERRUPT_TYPE = StaticConfig.Int_Abnormal;
		businessFinishMark = 3; //默认是3
		HTTP_content_type = "";
		URI = "";
		Refer_URI = "";
		value = new StringBuffer();
	}

	@Override
	public ParseItem getDataParseItem() throws IOException
	{
		return parseItem;
	}

	@Override
	public boolean FillData_short(DataAdapterReader dataAdapterReader) throws ParseException, IOException
	{
		try
		{
			imsi = dataAdapterReader.GetLongValue("IMSI", 0);

			// stime
			tmDate = dataAdapterReader.GetDateValue("Procedure_Start_Time", new Date(1970, 1, 1));
			istime = (int) (tmDate.getTime() / 1000L);
			istimems = (int) (tmDate.getTime() % 1000L);
			// etime
			tmDate = dataAdapterReader.GetDateValue("Procedure_End_Time", new Date(1970, 1, 1));
			ietime = (int) (tmDate.getTime() / 1000L);
			ietimems = (int) (tmDate.getTime() % 1000L);

			if(MainModel.GetInstance().getCompile().Assert(CompileMark.GuangXi2))
			{
				//TODO 直接改配置文件就好了
				String cellIdStr = dataAdapterReader.GetStrValue("Cell_ID", "0");
				Cell_ID = Long.parseLong(cellIdStr,16);
			}else{
				Cell_ID = dataAdapterReader.GetLongValue("Cell_ID", 0);
			}

			Eci = Cell_ID;
			ecgi = Eci;
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"XdrData_Http.fillData_short error",
					"XdrData_Http.fillData_short error: " + e.getMessage(),e);
			return false;
		}

		return true;

	}

	@Override
	public boolean FillData(DataAdapterReader dataAdapterReader) throws ParseException, IOException
	{

		try
		{
			// stime
			tmDate = dataAdapterReader.GetDateValue("Procedure_Start_Time", new Date(1970, 1, 1));
			istime = (int) (tmDate.getTime() / 1000L);
			istimems = (int) (tmDate.getTime() % 1000L);
			// etime
			tmDate = dataAdapterReader.GetDateValue("Procedure_End_Time", new Date(1970, 1, 1));
			ietime = (int) (tmDate.getTime() / 1000L);
			ietimems = (int) (tmDate.getTime() % 1000L);
			if(MainModel.GetInstance().getCompile().Assert(CompileMark.GuangXi2))
			{	
				//TODO 直接改配置文件就好了
				String cellIdStr = dataAdapterReader.GetStrValue("Cell_ID", "0");
				Cell_ID = Long.parseLong(cellIdStr,16);
			}else{
				Cell_ID = dataAdapterReader.GetLongValue("Cell_ID", 0);
			}
			
			Eci = Cell_ID;
			ecgi = Eci;
			imsi = dataAdapterReader.GetLongValue("IMSI", 0);
			value = dataAdapterReader.getTmStrs();

		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"XdrData_Http.fillData error",
					"XdrData_Http.fillData error: " + e.getMessage(),e);
			throw e;
		}

		try
		{
			XDR_ID = dataAdapterReader.GetStrValue("XDR_ID", "");
			// zhaikaishun
			imsi = dataAdapterReader.GetLongValue("IMSI", 0L);
			imei = dataAdapterReader.GetStrValue("IMEI", "");
			MSISDN = dataAdapterReader.GetStrValue("MSISDN", "");
			Cell_ID = dataAdapterReader.GetLongValue("Cell_ID", 0);
			Eci = Cell_ID;
			APN = dataAdapterReader.GetStrValue("APN", "");
			App_Type_Code = dataAdapterReader.GetStrValue("App_Type_Code", "");
			App_Type = dataAdapterReader.GetIntValue("App_Type", StaticConfig.Int_Abnormal);
			App_Sub_type = dataAdapterReader.GetIntValue("App_Sub_type", StaticConfig.Int_Abnormal);
			App_Status = dataAdapterReader.GetIntValue("App_Status", StaticConfig.Int_Abnormal);
			UL_Data = dataAdapterReader.GetDoubleValue("UL_Data", 0);
			DL_Data = dataAdapterReader.GetDoubleValue("DL_Data", 0);
			UL_IP_Packet = dataAdapterReader.GetLongValue("UL_IP_Packet", 0);
			DL_IP_Packet = dataAdapterReader.GetLongValue("DL_IP_Packet", 0);
			tcp_suc_first_req_delay_ms = dataAdapterReader.GetLongValue("tcp_suc_first_req_delay_ms", 0);
			first_req_first_resp_delay_ms = dataAdapterReader.GetLongValue("first_req_first_resp_delay_ms", 0);
			last_http_resp_delay_ms = dataAdapterReader.GetLongValue("last_http_resp_delay_ms", 0);
			last_ack_delay_ms = dataAdapterReader.GetDoubleValue("last_ack_delay_ms", 0).longValue();
			HOST = dataAdapterReader.GetStrValue("HOST", "");
			TCP_RESPONSE_DELAY = dataAdapterReader.GetIntValue("TCP_RESPONSE_DELAY", 0);
			TCP_CONFIRM_DELAY = dataAdapterReader.GetDoubleValue("TCP_CONFIRM_DELAY", 0).intValue();
			TCP_ATT_CNT = dataAdapterReader.GetDoubleValue("TCP_ATT_CNT", StaticConfig.Int_Abnormal).intValue();
			TCP_CONN_STATUS = dataAdapterReader.GetIntValue("TCP_CONN_STATUS", StaticConfig.Int_Abnormal);
			SESSION_MARK_END = dataAdapterReader.GetIntValue("SESSION_MARK_END", StaticConfig.Int_Abnormal);
			TRANSACTION_TYPE = dataAdapterReader.GetIntValue("TRANSACTION_TYPE", StaticConfig.Int_Abnormal);
			HTTP_WAP_STATUS = dataAdapterReader.GetIntValue("HTTP_WAP_STATUS", StaticConfig.Int_Abnormal);
			FIRST_HTTP_RES_DELAY = dataAdapterReader.GetIntValue("FIRST_HTTP_RES_DELAY", 0);
			last_http_content_delay_ms = dataAdapterReader.GetDoubleValue("last_http_content_delay_ms",
					0).intValue();
			WTP_INTERRUPT_TYPE = dataAdapterReader.GetIntValue("WTP_INTERRUPT_TYPE", StaticConfig.Int_Abnormal);
			HTTP_content_type = dataAdapterReader.GetStrValue("HTTP_content_type", "");
			URI = dataAdapterReader.GetStrValue("URI", "");
			Refer_URI = dataAdapterReader.GetStrValue("Refer_URI", "");
			if (App_Status == 1)
			{
				if (FIRST_HTTP_RES_DELAY > 0)
				{
					IPThroughputUL = (double) (UL_Data * 8.0 / (FIRST_HTTP_RES_DELAY / 1000.0)) / 1024;
					IPThroughputDL = (double) (DL_Data * 8.0 / (FIRST_HTTP_RES_DELAY / 1000.0)) / 1024;
				}
			}
			try
			{
				businessFinishMark = dataAdapterReader.GetIntValue("businessFinishMark", 3);// 业务完成标识
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"XdrData_Http.fillData1 error:",
						"XdrData_Http.fillData1 error: " + e.getMessage(),e);
				businessFinishMark = 3;
			}

		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"XdrData_Http.fillData error",
					"XdrData_Http.fillData error: " + e.getMessage(),e);
		}

		return true;
	}

	@Override
	public String toString()
	{
		return "";
	}

	@Override
	public ArrayList<EventData> toEventData()
	{
		ArrayList<EventData> eventDataList = new ArrayList<EventData>();

		stat();

		EventData eventDataKpiset1 = new EventData();
		eventDataKpiset1.iCityID = iCityID;
		eventDataKpiset1.IMSI = imsi;
		eventDataKpiset1.iEci = ecgi;
		eventDataKpiset1.iTime = istime;
		eventDataKpiset1.wTimems = 0;
		eventDataKpiset1.strLoctp = strloctp;
		eventDataKpiset1.strLabel = label;
		eventDataKpiset1.iLongitude = iLongitude;
		eventDataKpiset1.iLatitude = iLatitude;
		eventDataKpiset1.iBuildID = ibuildid;
		eventDataKpiset1.iHeight = iheight;
		eventDataKpiset1.position = position;
		eventDataKpiset1.Interface = StaticConfig.INTERFACE_S1_U;
		eventDataKpiset1.iKpiSet = 1;
		eventDataKpiset1.iProcedureType = 1;

		eventDataKpiset1.iTestType = testType;
		eventDataKpiset1.iDoorType = iDoorType;
		eventDataKpiset1.iLocSource = locSource;

		eventDataKpiset1.confidentType = confidentType;
		eventDataKpiset1.iAreaType = iAreaType;
		eventDataKpiset1.iAreaID = iAreaID;

		eventDataKpiset1.confidentType = confidentType;
		eventDataKpiset1.iAreaType = iAreaType;
		eventDataKpiset1.iAreaID = iAreaID;
		eventDataKpiset1.lTrainKey = trainKey;
		eventDataKpiset1.iSectionId = sectionId;
		eventDataKpiset1.iSegmentId = segmentId;
		eventDataKpiset1.lteScRSRP = LteScRSRP;
		eventDataKpiset1.lteScSinrUL = LteScSinrUL;
		eventDataKpiset1.imei = imei;
		// event stat
		eventDataKpiset1.eventStat = new EventDataStruct();

		eventDataKpiset1.eventStat.fvalue[0] = HTTP_latency;
		eventDataKpiset1.eventStat.fvalue[1] = HTTP_Accept;
		eventDataKpiset1.eventStat.fvalue[2] = traffic_ip_all;
		eventDataKpiset1.eventStat.fvalue[3] = trans_delay1;
		eventDataKpiset1.eventStat.fvalue[4] = HTTP_ATTEMPT;
		eventDataKpiset1.eventStat.fvalue[5] = TCP_ATTEMPT;
		eventDataKpiset1.eventStat.fvalue[6] = TCP_Accept;
		eventDataKpiset1.eventStat.fvalue[7] = TCP_latency;

		if (!eventDataKpiset1.haveEventStat())
		{
			eventDataKpiset1.eventStat=null;
			eventDataKpiset1.eventDetial=null;
		}
		eventDataKpiset1.eventDetial = null;
		eventDataList.add(eventDataKpiset1);


		EventData eventDataKpiset2 = new EventData();

		eventDataKpiset2.iCityID = iCityID;

		eventDataKpiset2.IMSI = imsi;
		eventDataKpiset2.iEci = ecgi;
		eventDataKpiset2.iTime = istime;
		eventDataKpiset2.wTimems = 0;
		eventDataKpiset2.strLoctp = strloctp;
		eventDataKpiset2.strLabel = label;
		eventDataKpiset2.iLongitude = iLongitude;
		eventDataKpiset2.iLatitude = iLatitude;
		eventDataKpiset2.iBuildID = ibuildid;
		eventDataKpiset2.iHeight = iheight;
		eventDataKpiset2.Interface = StaticConfig.INTERFACE_S1_U;

		eventDataKpiset2.iKpiSet = 4;
		eventDataKpiset2.iProcedureType = 1;
		eventDataKpiset2.position = position;
		eventDataKpiset2.iTestType = testType;
		eventDataKpiset2.iDoorType = iDoorType;
		eventDataKpiset2.iLocSource = locSource;

		eventDataKpiset2.confidentType = confidentType;
		eventDataKpiset2.iAreaType = iAreaType;
		eventDataKpiset2.iAreaID = iAreaID;
		eventDataKpiset2.lTrainKey = trainKey;
		eventDataKpiset2.iSectionId = sectionId;
		eventDataKpiset2.iSegmentId = segmentId;
		eventDataKpiset2.lteScRSRP = LteScRSRP;
		eventDataKpiset2.lteScSinrUL = LteScSinrUL;
		eventDataKpiset2.imei = imei;


		if (App_Type == 5)
		{
			LTE下行视频业务量 += DL_Data;
			LTE上行视频业务量 += UL_Data;
			if(LTE下行视频业务量 >0){
				LTE下行视频业务量_时长 = trans_delay1;
			}
		}

		if (App_Type == 1 && App_Sub_type == 9)
		{
			LTE下行微信业务量 += DL_Data;
			LTE上行微信业务量 += UL_Data;
			LTE微信用户数 = 1;

			if(LTE下行微信业务量 >0){
				LTE下行QQ微信业务量_时长 = trans_delay1;
			}
			if(LTE上行微信业务量 >0){
				LTE上行QQ微信业务量_时长 =trans_delay1;
			}

		}

		if (App_Type == 1 && App_Sub_type == 5)
		{
			LTE下行QQ通信业务量 += DL_Data;
			LTE上行QQ通信业务量 += UL_Data;
			if(LTE下行QQ通信业务量 >0){
                LTE下行QQ微信业务量_时长 = trans_delay1;
			}
			if(LTE上行QQ通信业务量 >0){
                LTE上行QQ微信业务量_时长 = trans_delay1;
			}

		}
		if (App_Type == 15)
		{
			LTE下行网页浏览业务量 += DL_Data;
			LTE上行网页浏览业务量 +=UL_Data;
			LTE网页浏览业务请求数++;
			if(LTE下行网页浏览业务量 >0){
				LTE下行网页浏览业务量_时长 = trans_delay1;
			}
		}
		
		// event stat
		eventDataKpiset2.eventStat = new EventDataStruct();
		eventDataKpiset2.eventStat.fvalue[0] = LTE下行视频业务量;
		eventDataKpiset2.eventStat.fvalue[1] = LTE上行视频业务量;
		eventDataKpiset2.eventStat.fvalue[2] = LTE下行微信业务量;
		eventDataKpiset2.eventStat.fvalue[3] = LTE微信用户数;
		eventDataKpiset2.eventStat.fvalue[4] = LTE下行QQ通信业务量;
		eventDataKpiset2.eventStat.fvalue[5] = LTE下行网页浏览业务量;
		eventDataKpiset2.eventStat.fvalue[6] = LTE网页浏览业务请求数;
		eventDataKpiset2.eventStat.fvalue[7] = LTE上行微信业务量;
		eventDataKpiset2.eventStat.fvalue[8] = LTE上行QQ通信业务量;
		eventDataKpiset2.eventStat.fvalue[9] = LTE上行网页浏览业务量;

		eventDataKpiset2.eventStat.fvalue[10] = LTE下行视频业务量_时长;
		eventDataKpiset2.eventStat.fvalue[11] = LTE下行QQ微信业务量_时长;
		eventDataKpiset2.eventStat.fvalue[12] = LTE上行QQ微信业务量_时长;
		eventDataKpiset2.eventStat.fvalue[13] = LTE下行网页浏览业务量_时长;


		if(eventDataKpiset2.haveEventStat() &&
				!MainModel.GetInstance().getCompile().Assert(CompileMark.BeiJing)){
			//不需要EventDetial的数据,北京不需要这个数据的吐出
			eventDataKpiset2.eventDetial = null;
			eventDataList.add(eventDataKpiset2);
		}

		return eventDataList;
	}

	public void stat()
	{
		
		if (HTTP_WAP_STATUS < 400 && HTTP_WAP_STATUS > 0 && TRANSACTION_TYPE == 6)
		{
			HTTP_latency = HTTP_latency + last_http_content_delay_ms;
			
		}

		if (HTTP_WAP_STATUS < 400 && HTTP_WAP_STATUS > 0 && TRANSACTION_TYPE == 6)
		{
			HTTP_Accept++;
			
		}

		if (TRANSACTION_TYPE == 6 && businessFinishMark == 3 && last_http_content_delay_ms < 300000)
		{
			traffic_ip_all = traffic_ip_all + DL_Data;
			
		}

		if (TRANSACTION_TYPE == 6 && businessFinishMark == 3 && last_http_content_delay_ms < 300000)
		{
			trans_delay1 = trans_delay1 + last_http_content_delay_ms;
			
		}

		if (TRANSACTION_TYPE == 6)
		{
			HTTP_ATTEMPT++;
		}

		if (TCP_ATT_CNT > 0)
		{
			TCP_ATTEMPT++;
		}

		if (TCP_ATT_CNT > 0 && TCP_CONN_STATUS == 0)
		{
			TCP_Accept++;
			
		}

		if (TCP_CONN_STATUS == 0)
		{
			TCP_latency = TCP_latency + TCP_CONFIRM_DELAY;// +
															// TCP_RESPONSE_DELAY;
		}
	}

	@Override
	public void toString(StringBuffer sb)
	{

		StaticConfig.putCityNameByCityId();
		String fenge = parseItem.getSplitMark();
		if (fenge.contains("\\"))
		{
			fenge = fenge.replace("\\", "");
		}

		sb.append(value);
		sb.append(fenge);
		sb.append("");
		sb.append(fenge);
		sb.append(iLongitude);
		sb.append(fenge);
		sb.append(iLatitude);
		sb.append(fenge);
		sb.append(iheight);
		sb.append(fenge);
		sb.append(iDoorType);
		sb.append(fenge);

		sb.append(iRadius);
		sb.append(fenge);
		GridItem gridItem = GridItem.GetGridItem(0, iLongitude, iLatitude);

		int icentLng = gridItem.getBRLongitude() / 2 + gridItem.getTLLongitude() / 2;
		int icentLat = gridItem.getBRLatitude() / 2 + gridItem.getTLLatitude() / 2;
		if (StaticConfig.cityId_Name.containsKey(iCityID))
		{
			sb.append(StaticConfig.cityId_Name.get(iCityID) + "_" + icentLng + "_" + icentLat);
			sb.append(fenge);
		}
		else
		{
			sb.append("nocity" + "_" + icentLng + "_" + icentLat);
			sb.append(fenge);
		}

		sb.append(-1);
		sb.append(fenge);
		sb.append(-1);

	}

}
