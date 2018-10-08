package cn.mastercom.bigdata.evt.locall.model;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;

import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.evt.locall.stat.EventData;
import cn.mastercom.bigdata.evt.locall.stat.EventDataStruct;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.DataAdapterConf.ParseItem;
import cn.mastercom.bigdata.util.DataAdapterReader;
import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;

public class XdrData_Cdr_Quality extends XdrDataBase
{
	private Date tmDate = new Date();
	private static ParseItem parseItem;

	private String LAST_EGCI;
	private long LAST_MME_UE_S1AP_ID;

	private int INTERFACE;
	private double UL_MOS_AVG;
	private long RTCP_UL_PACKET_NUM;
	private long RTCP_UL_LOSSPACKER_NUM;
	private double DL_MOS_AVG;
	private double RTCP_DL_PACKET_NUM;
	private double RTCP_DL_LOSSPACKET_NUM;
	private double UL_IPMOS_AVG;
	private double RTP_UL_PACKET_NUM;
	private double RTP_UL_LOSSPACKET_NUM;
	private double DL_IPMOS_AVG;
	private double RTP_DL_PACKET_NUM;
	private double RTP_DL_LOSSPACKET_NUM;
	private double conn_latency;
	private String Service_Type;
	private double ONE_WAY_IDENTIFY;
	private double CALL_DURATION;
	private int PEER_CALL_TYPE;
	
	private double UL_ONE_WAY_NUM;
	private double DW_ONE_WAY_NUM;
	private double upstoptime;
	private double downstoptime;
	private double volte上行单通次数;
	private double volte下行单通次数;
	private double volte上行断续次数;
	private double volte下行断续次数;
	
	
	// 统计指标
	private double volte上行rtcp总包数;
	private double volte上行rtcp丢包数;
	private double volte下行rtcp总包数;
	private double volte下行rtcp丢包数;
	private double VOLTE下行RTP丢包数_GM;
	private double VOLTE下行RTP总包数_GM;
	private double VoLTE下行RTP丢包数_S1U;
	private double VOLTE下行RTP总包数_S1U;
	private double VoLTE下行RTP丢包数_MW;
	private double VOLTE下行RTP总包数_MW;
	private double VoLTE上行RTP丢包数_S1U;
	private double VoLTE上行RTP总包数_S1U;
	private double VoLTE上行RTP丢包数_GM;
	private double VoLTE上行RTP总包数_GM;
	private double VoLTE上行RTP丢包数_MW;
	private double VoLTE上行RTP总包数_MW;
	
	private double MOS采样点数=0;
	private double MOS3采样点数=0;
	/* 2018-02-23 增加上下行等采样点数 */
	private double MOS上行采样点数_VALL;
	private double MOS下行采样点数_VALL;
	private double MOS30上行采样点数_VALL;
	private double MOS30下行采样点数_VALL;
	private double MOS上行采样点数_VV;
	private double MOS下行采样点数_VV;
	private double MOS30上行采样点数_VV;
	private double MOS30下行采样点数_VV;
	private double MOS上行采样点数_VC;
	private double MOS下行采样点数_VC;
	private double MOS30上行采样点数_VC;
	private double MOS30下行采样点数_VC;

	/* 20180821 增加质差数 */
	private int MOS质差;

	public XdrData_Cdr_Quality()
	{
		super();
		clear();

		if (parseItem == null)
		{
			parseItem = MainModel.GetInstance().getEventAdapterConfig().getParseItem("LOCALL-CDR-QUALITY");
		}
	}

	@Override
	public int getInterfaceCode() {
		return StaticConfig.INTERFACE_NEW_S1U;
	}

	public void clear()
	{
		volte上行单通次数=0;
		volte下行单通次数=0;
		volte上行断续次数=0;
		volte下行断续次数=0;
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

			s1apid = dataAdapterReader.GetLongValue("LAST_MME_UE_S1AP_ID", StaticConfig.Int_Abnormal);

			try
			{
				String strEci = dataAdapterReader.GetStrValue("LAST_EGCI", "");
				
				
				if (strEci == null || strEci.length() == 0 || strEci.contains("."))
				{
					return false;
				}

				// TODO 如果是13，14，15 等多位， 应该设计最后两位是cell_id，去掉前面的5位
				if (strEci.length() >= 12)
				{
					ecgi = Long.parseLong(strEci.substring(5, 10), 16) * 256L + Long.parseLong(strEci.substring(10, 12), 16);
				}
				
				
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"XdrData_Cdr_Quality.fillData_short error:",
						"XdrData_Cdr_Quality.fillData_short error: " + e.getMessage(),e);
				return false;
			}

			// stime
			tmDate = dataAdapterReader.GetDateValue("STARTTIME", new Date(1970, 1, 1));
			istime = (int) (tmDate.getTime() / 1000L);
			istimems = dataAdapterReader.GetIntValue("STARTTIME_MS", 0);

			// etime
			tmDate = dataAdapterReader.GetDateValue("ENDTIME", new Date(1970, 1, 1));
			ietime = (int) (tmDate.getTime() / 1000L);
			ietimems = dataAdapterReader.GetIntValue("ENDTIME_MS", 0);

		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"XdrData_Cdr_Quality.fillData_short error",
					"XdrData_Cdr_Quality.fillData_short error: " + e.getMessage(),e);
			return false;
		}

		return true;
	}

	@Override
	public boolean FillData(DataAdapterReader dataAdapterReader) throws ParseException, IOException
	{
		try
		{
			s1apid = dataAdapterReader.GetLongValue("LAST_MME_UE_S1AP_ID", StaticConfig.Int_Abnormal);

			try
			{
				String strEci = dataAdapterReader.GetStrValue("LAST_EGCI", "");
				if (strEci == null || strEci.length() == 0 || strEci.contains("."))
				{
					return false;
				}

				if (strEci.length() >= 12)
				{
					ecgi = Long.parseLong(strEci.substring(5, 10), 16) * 256L + Long.parseLong(strEci.substring(10, 12), 16);
				}

			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"XdrData_Cdr_Quality.fillData error",
						"XdrData_Cdr_Quality.fillData error: " + e.getMessage(),e);
				return false;
			}

			// stime
			tmDate = dataAdapterReader.GetDateValue("STARTTIME", new Date(1970, 1, 1));
			istime = (int) (tmDate.getTime() / 1000L);
			istimems = dataAdapterReader.GetIntValue("STARTTIME_MS", 0);
            imei = dataAdapterReader.GetStrValue("IMEI", "");
			// etime
			tmDate = dataAdapterReader.GetDateValue("ENDTIME", new Date(1970, 1, 1));
			ietime = (int) (tmDate.getTime() / 1000L);
			ietimems = dataAdapterReader.GetIntValue("ENDTIME_MS", 0);

			INTERFACE = dataAdapterReader.GetIntValue("INTERFACE", StaticConfig.Int_Abnormal);
			UL_MOS_AVG = dataAdapterReader.GetDoubleValue("UL_MOS_AVG", StaticConfig.Int_Abnormal);
			RTCP_UL_PACKET_NUM = dataAdapterReader.GetLongValue("RTCP_UL_PACKET_NUM", StaticConfig.Int_Abnormal);
			RTCP_UL_LOSSPACKER_NUM = dataAdapterReader.GetLongValue("RTCP_UL_LOSSPACKER_NUM",
					StaticConfig.Int_Abnormal);
			DL_MOS_AVG = dataAdapterReader.GetDoubleValue("DL_MOS_AVG", StaticConfig.Int_Abnormal);
			RTCP_DL_PACKET_NUM = dataAdapterReader.GetDoubleValue("RTCP_DL_PACKET_NUM", StaticConfig.Int_Abnormal);
			RTCP_DL_LOSSPACKET_NUM = dataAdapterReader.GetDoubleValue("RTCP_DL_LOSSPACKET_NUM", StaticConfig.Int_Abnormal);
			
			UL_IPMOS_AVG = dataAdapterReader.GetDoubleValue("UL_IPMOS_AVG", StaticConfig.Int_Abnormal);
			RTP_UL_PACKET_NUM = dataAdapterReader.GetDoubleValue("RTP_UL_PACKET_NUM", StaticConfig.Int_Abnormal);
			RTP_UL_LOSSPACKET_NUM = dataAdapterReader.GetDoubleValue("RTP_UL_LOSSPACKET_NUM",
					StaticConfig.Int_Abnormal);
			DL_IPMOS_AVG = dataAdapterReader.GetDoubleValue("DL_IPMOS_AVG", StaticConfig.Int_Abnormal);
			RTP_DL_PACKET_NUM = dataAdapterReader.GetDoubleValue("RTP_DL_PACKET_NUM", StaticConfig.Int_Abnormal);
			RTP_DL_LOSSPACKET_NUM = dataAdapterReader.GetDoubleValue("RTP_DL_LOSSPACKET_NUM",
					StaticConfig.Int_Abnormal);
			conn_latency = dataAdapterReader.GetDoubleValue("conn_latency", StaticConfig.Int_Abnormal);

			Service_Type = dataAdapterReader.GetStrValue("Service_Type", "1");

			UL_ONE_WAY_NUM = dataAdapterReader.GetDoubleValue("UL_ONE_WAY_NUM", 0);
			DW_ONE_WAY_NUM = dataAdapterReader.GetDoubleValue("DW_ONE_WAY_NUM", 0);

			upstoptime = dataAdapterReader.GetDoubleValue("upstoptime", 0);
			downstoptime = dataAdapterReader.GetDoubleValue("downstoptime", 0);
			
			ONE_WAY_IDENTIFY = dataAdapterReader.GetDoubleValue("ONE_WAY_IDENTIFY", 0);
			String CALL_DURATIONStr = dataAdapterReader.GetStrValue("CALL_DURATION", "");
			try{
				if(CALL_DURATIONStr.length()>0){
					CALL_DURATION = Double.parseDouble(CALL_DURATIONStr);
				}
			}catch(Exception e){
				CALL_DURATION=0;
				LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"XdrData_Cdr_Quality.fillData CALL_DURATION error",
						"XdrData_Cdr_Quality.fillData CALL_DURATION error: " + e.getMessage(),e);
			}
			PEER_CALL_TYPE = dataAdapterReader.GetIntValue("PEER_CALL_TYPE", StaticConfig.Int_Abnormal);
			
			return true;
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"XdrData_Cdr_Quality.fillData error:",
					"XdrData_Cdr_Quality.fillData error: " + e.getMessage(),e);
			return false;
		}

	}

	@Override
	public ArrayList<EventData> toEventData()
	{
		ArrayList<EventData> eventDataList = new ArrayList<EventData>();
		
		if("1".equals(Service_Type) && INTERFACE==34){
			if(upstoptime>0 && CALL_DURATION>0){
				volte上行断续次数 = 1;
			}
			if(downstoptime>0 && CALL_DURATION>0){
				volte下行断续次数 = 1;
			}
			
		}
		
		if (conn_latency > 0 && "1".equals(Service_Type))
		{ 
			if (INTERFACE == 34 && UL_MOS_AVG > 0)
			{
				volte上行rtcp总包数 = RTCP_UL_PACKET_NUM;
			}

			if (INTERFACE == 34 && UL_MOS_AVG > 0)
			{
				volte上行rtcp丢包数 = RTCP_UL_LOSSPACKER_NUM;
				if(RTCP_UL_LOSSPACKER_NUM>RTCP_UL_PACKET_NUM){
					volte上行rtcp总包数 = 0;
					volte上行rtcp丢包数 = 0;
				}
			}
			if (INTERFACE == 34 && DL_MOS_AVG > 0)
			{
				volte下行rtcp总包数 = RTCP_DL_PACKET_NUM;
			}

			if (INTERFACE == 34 && DL_MOS_AVG > 0)
			{
				volte下行rtcp丢包数 = RTCP_DL_LOSSPACKET_NUM; 
				if(RTCP_DL_LOSSPACKET_NUM>RTCP_DL_PACKET_NUM){
					volte下行rtcp总包数 = 0;
					volte下行rtcp丢包数 = 0;
				}
			}
			
			
			if (INTERFACE == 54 && DL_IPMOS_AVG > 0)
			{
				VOLTE下行RTP丢包数_GM = RTP_DL_LOSSPACKET_NUM;
			}

			if (INTERFACE == 54 && DL_IPMOS_AVG > 0)
			{
				VOLTE下行RTP总包数_GM = RTP_DL_PACKET_NUM;
			}

			if (INTERFACE == 34 && DL_IPMOS_AVG > 0)
			{
				VoLTE下行RTP丢包数_S1U = RTP_DL_LOSSPACKET_NUM;
			}

			if (INTERFACE == 34 && DL_IPMOS_AVG > 0)
			{
				VOLTE下行RTP总包数_S1U = RTP_DL_PACKET_NUM;
			}

			if (INTERFACE == 55 && DL_IPMOS_AVG > 0)
			{
				VoLTE下行RTP丢包数_MW = RTP_DL_LOSSPACKET_NUM;
			}

			if (INTERFACE == 55 && DL_IPMOS_AVG > 0)
			{
				VOLTE下行RTP总包数_MW = RTP_DL_PACKET_NUM;
			}

			if (INTERFACE == 34 && UL_IPMOS_AVG > 0)
			{
				VoLTE上行RTP丢包数_S1U = RTP_UL_LOSSPACKET_NUM;
			}

			if (INTERFACE == 34 && UL_IPMOS_AVG > 0)
			{
				VoLTE上行RTP总包数_S1U = RTP_UL_PACKET_NUM;
			}

			if (INTERFACE == 54 && UL_IPMOS_AVG > 0)
			{
				VoLTE上行RTP丢包数_GM = RTP_UL_LOSSPACKET_NUM;
			}
			if (INTERFACE == 54 && UL_IPMOS_AVG > 0)
			{
				VoLTE上行RTP总包数_GM = RTP_UL_PACKET_NUM;
			}

			if (INTERFACE == 55 && UL_IPMOS_AVG > 0)
			{
				VoLTE上行RTP丢包数_MW = RTP_UL_LOSSPACKET_NUM;
			}
			if (INTERFACE == 55 && UL_IPMOS_AVG > 0)
			{
				VoLTE上行RTP总包数_MW = RTP_UL_PACKET_NUM;
			}

			if(ONE_WAY_IDENTIFY==1 || ONE_WAY_IDENTIFY==3){
				volte上行单通次数 = 1;
			}
			if(ONE_WAY_IDENTIFY==2 || ONE_WAY_IDENTIFY==3){
				volte下行单通次数 = 1;
			}
			if(UL_MOS_AVG>0 && DL_MOS_AVG>0){
				if(((UL_MOS_AVG+DL_MOS_AVG)/2)<3.0){
					MOS质差 = 1;
				}
			}
			
			
			EventData eventData = new EventData();
			eventData.iCityID = iCityID;
			eventData.IMSI = imsi;
			eventData.iEci = ecgi;
			eventData.iTime = istime;
			eventData.wTimems = 0;
			eventData.strLoctp = strloctp;
			eventData.strLabel = label;
			eventData.iLongitude = iLongitude;
			eventData.iLatitude = iLatitude;
			eventData.iBuildID = ibuildid;
			eventData.iHeight = iheight;
			eventData.position = position;
			eventData.Interface = StaticConfig.INTERFACE_NEW_S1U;
			eventData.iKpiSet = 2;
			eventData.iProcedureType = 1;

			eventData.iTestType = testType;
			eventData.iDoorType = iDoorType;
			eventData.iLocSource = locSource;

			eventData.confidentType = confidentType;
			eventData.iAreaType = iAreaType;
			eventData.iAreaID = iAreaID;
			eventData.lteScRSRP = LteScRSRP;
			eventData.lteScSinrUL = LteScSinrUL;

			// event stat
			eventData.eventStat = new EventDataStruct();
			
			eventData.eventStat.fvalue[0] = volte上行rtcp总包数;
			eventData.eventStat.fvalue[1] = volte上行rtcp丢包数;
			eventData.eventStat.fvalue[2] = volte下行rtcp总包数;
			eventData.eventStat.fvalue[3] = volte下行rtcp丢包数;
			eventData.eventStat.fvalue[4] = VOLTE下行RTP总包数_GM;
			eventData.eventStat.fvalue[5] = VOLTE下行RTP丢包数_GM;
			eventData.eventStat.fvalue[6] = VOLTE下行RTP总包数_S1U;
			eventData.eventStat.fvalue[7] = VoLTE下行RTP丢包数_S1U;
			eventData.eventStat.fvalue[8] = VOLTE下行RTP总包数_MW;
			eventData.eventStat.fvalue[9] = VoLTE下行RTP丢包数_MW;
			eventData.eventStat.fvalue[10] = VoLTE上行RTP总包数_S1U;
			eventData.eventStat.fvalue[11] = VoLTE上行RTP丢包数_S1U;
			eventData.eventStat.fvalue[12] = VoLTE上行RTP总包数_GM;
			eventData.eventStat.fvalue[13] = VoLTE上行RTP丢包数_GM;
			eventData.eventStat.fvalue[14] = VoLTE上行RTP总包数_MW;
			eventData.eventStat.fvalue[15] = VoLTE上行RTP丢包数_MW;
			eventData.eventStat.fvalue[16] = volte上行单通次数+volte下行单通次数;
			eventData.eventStat.fvalue[17] = volte上行断续次数+volte下行断续次数;
			eventData.eventStat.fvalue[18] = MOS质差;

			
			eventData.lTrainKey = trainKey;
			eventData.iSectionId = sectionId;
			eventData.iSegmentId = segmentId;
			
			if(eventData.haveEventStat()){
				if(MOS质差==1){
				    eventData.eventDetial.sampleType = StaticConfig.FAILURE_SAMPLE;
					eventData.eventDetial.strvalue[0] = "MOS质差";
				}else{
					eventData.eventDetial = null;
				}
				eventDataList.add(eventData);
			}
			
			
			EventData eventDataKpiset1 = new EventData();
			eventDataKpiset1.eventDetial = null;
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
			eventDataKpiset1.Interface = StaticConfig.INTERFACE_NEW_S1U;
			eventDataKpiset1.iKpiSet = 1;
			eventDataKpiset1.iProcedureType = 1;

			eventDataKpiset1.iTestType = testType;
			eventDataKpiset1.iDoorType = iDoorType;
			eventDataKpiset1.iLocSource = locSource;

			eventDataKpiset1.confidentType = confidentType;
			eventDataKpiset1.iAreaType = iAreaType;
			eventDataKpiset1.iAreaID = iAreaID;
            eventDataKpiset1.lteScRSRP = LteScRSRP;
            eventDataKpiset1.lteScSinrUL = LteScSinrUL;
			stateMos();


			
			// event stat
			eventDataKpiset1.eventStat = new EventDataStruct();
			eventDataKpiset1.eventStat.fvalue[6] = MOS上行采样点数_VALL;
			eventDataKpiset1.eventStat.fvalue[7] = MOS下行采样点数_VALL;
			
			eventDataKpiset1.eventStat.fvalue[9] = MOS30上行采样点数_VALL;
			eventDataKpiset1.eventStat.fvalue[10] =  MOS30下行采样点数_VALL;
			eventDataKpiset1.eventStat.fvalue[11] =MOS上行采样点数_VV;
			eventDataKpiset1.eventStat.fvalue[12] =MOS下行采样点数_VV;
			eventDataKpiset1.eventStat.fvalue[13] =MOS30上行采样点数_VV;
			eventDataKpiset1.eventStat.fvalue[14] =MOS30下行采样点数_VV;
			eventDataKpiset1.eventStat.fvalue[15] =MOS上行采样点数_VC;
			eventDataKpiset1.eventStat.fvalue[16] =MOS下行采样点数_VC;
			eventDataKpiset1.eventStat.fvalue[17] =MOS30上行采样点数_VC;
			eventDataKpiset1.eventStat.fvalue[18] =MOS30下行采样点数_VC;
			
			
			eventDataKpiset1.lTrainKey = trainKey;
			eventDataKpiset1.iSectionId = sectionId;
			eventDataKpiset1.iSegmentId = segmentId;
			if(eventDataKpiset1.haveEventStat()){
				eventDataList.add(eventDataKpiset1);
			}
			
		}

		return eventDataList;
	}

	private void stateMos() {
		if(INTERFACE == 34){
			
			if(UL_MOS_AVG>0){
				MOS上行采样点数_VALL++;
				if(PEER_CALL_TYPE==1){
					MOS上行采样点数_VV++;
				}
				if(PEER_CALL_TYPE==0||PEER_CALL_TYPE==2){
					MOS上行采样点数_VC++;
				}
			}
			
			if(DL_MOS_AVG>0){
				MOS下行采样点数_VALL++;
				if(PEER_CALL_TYPE==1){
					MOS下行采样点数_VV++;
				}
				if(PEER_CALL_TYPE==0||PEER_CALL_TYPE==2){
					MOS下行采样点数_VC++;
				}
			}
			
			if(UL_MOS_AVG>=3){
				MOS30上行采样点数_VALL++;
				if(PEER_CALL_TYPE==1){
					MOS30上行采样点数_VV++;
				}
				if(PEER_CALL_TYPE==0||PEER_CALL_TYPE==2){
					MOS30上行采样点数_VC++;
				}
			}
			
			if(DL_MOS_AVG>=3){
				MOS30下行采样点数_VALL++;
				if(PEER_CALL_TYPE==1){
					MOS30下行采样点数_VV++;
				}
				if(PEER_CALL_TYPE==0||PEER_CALL_TYPE==2){
					MOS30下行采样点数_VC++;
				}
			}

		}
		
	}

	@Override
	public void toString(StringBuffer sb)
	{
		// TODO Auto-generated method stub

	}

}
