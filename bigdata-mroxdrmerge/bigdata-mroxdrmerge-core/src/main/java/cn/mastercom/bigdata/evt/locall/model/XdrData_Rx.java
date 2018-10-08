package cn.mastercom.bigdata.evt.locall.model;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;

import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.evt.locall.stat.EventData;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.DataAdapterConf.ParseItem;
import cn.mastercom.bigdata.util.DataAdapterReader;
import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;


public class XdrData_Rx extends XdrDataBase
{
	private Date tmDate = new Date();
	
	private static ParseItem parseItem;
	public int Interface;
	public int ProceDure_Type;

	public int ABORT_CAUSE;
	public int MEDIA_TYPE;
	public int RESULT_CODE;

	public long ECI;

	// 统计字段
	private long VoLTE语音掉话次数 = StaticConfig.Int_Abnormal;
	private long VoLTE视频掉话次数 = StaticConfig.Int_Abnormal;

	public XdrData_Rx()
	{
		super();
		clear();

		if (parseItem == null)
		{
			parseItem = MainModel.GetInstance().getEventAdapterConfig().getParseItem("LOCALL-Rx");
		}
	}

	@Override
	public int getInterfaceCode() {
		return StaticConfig.INTERFACE_RX;
	}

	public void clear()
	{
		Interface = StaticConfig.Int_Abnormal;
		ProceDure_Type = StaticConfig.Int_Abnormal;

		ABORT_CAUSE = StaticConfig.Int_Abnormal;
		MEDIA_TYPE = 0; // StaticConfig.Int_Abnormal; 因为这个原始数据中为空
		RESULT_CODE = StaticConfig.Int_Abnormal;
		ECI = StaticConfig.Int_Abnormal;
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

			Interface = dataAdapterReader.GetIntValue("Interface", StaticConfig.Int_Abnormal);
			if(Interface>0 && Interface!=StaticConfig.INTERFACE_RX){
				return false;
			}

			
			imsi = dataAdapterReader.GetLongValue("IMSI", 0);

			// stime
			tmDate = dataAdapterReader.GetDateValue("Procedure_Start_Time", new Date(1970, 1, 1));
			istime = (int) (tmDate.getTime() / 1000L);
			istimems = (int) (tmDate.getTime() % 1000L);
			// etime
			tmDate = dataAdapterReader.GetDateValue("Procedure_End_Time", new Date(1970, 1, 1));
			ietime = (int) (tmDate.getTime() / 1000L);
			ietimems = (int) (tmDate.getTime() % 1000L);
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"XdrData_Rx.fillData_short error",
					"XdrData_Rx.fillData_short error: " + e.getMessage(),e);
			return false;
		}

		return true;
	}

	@Override
	public boolean FillData(DataAdapterReader dataAdapterReader) throws ParseException, IOException
	{
		try
		{
			imsi = dataAdapterReader.GetLongValue("IMSI", 0);
			tmDate = dataAdapterReader.GetDateValue("Procedure_Start_Time", new Date(1970, 1, 1));
			istime = (int) (tmDate.getTime() / 1000L);
			istimems = (int) (tmDate.getTime() % 1000L);
			// etime
			tmDate = dataAdapterReader.GetDateValue("Procedure_End_Time", new Date(1970, 1, 1));
			ietime = (int) (tmDate.getTime() / 1000L);
			ietimems = (int) (tmDate.getTime() % 1000L);
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"XdrData_Rx.fillData1 error",
					"XdrData_Rx.fillData1 error: " + e.getMessage(),e);
			return false;
		}

		Interface = dataAdapterReader.GetIntValue("Interface", StaticConfig.Int_Abnormal);
		// public int IMSI;
		imei = dataAdapterReader.GetStrValue("IMEI", "");
		ProceDure_Type = dataAdapterReader.GetIntValue("ProceDure_Type", StaticConfig.Int_Abnormal);

		ABORT_CAUSE = dataAdapterReader.GetIntValue("ABORT_CAUSE", StaticConfig.Int_Abnormal);
		MEDIA_TYPE = dataAdapterReader.GetIntValue("MEDIA_TYPE", 0);
		RESULT_CODE = dataAdapterReader.GetIntValue("RESULT_CODE", StaticConfig.Int_Abnormal);
		try {
            ecgi = dataAdapterReader.GetLongValue("ECI",0);
        }catch (Exception e){
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"XdrData_Rx.fillData error",
					"XdrData_Rx.fillData error: " + e.getMessage(),e);
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
		
		int voice = 0;
		int video = 1;
		if(MainModel.GetInstance().getCompile().Assert(CompileMark.SiChuan)){
			voice = 1;
			video = 2;
		}
		ArrayList<EventData> eventDataList = new ArrayList<EventData>();
		EventData eventData = new EventData();
		if(MainModel.GetInstance().getCompile().Assert(CompileMark.SiChuan)){
            if (ProceDure_Type == 6 && (ABORT_CAUSE == 2) && MEDIA_TYPE == voice)
            {
                VoLTE语音掉话次数 = 1;
            }
            if (ProceDure_Type == 6 && (ABORT_CAUSE == 2) && MEDIA_TYPE == video)
            {
                VoLTE视频掉话次数 = 1;
            }
		}else{
			if (ProceDure_Type == 4 && ABORT_CAUSE != 3 && MEDIA_TYPE == voice)
			{
				VoLTE语音掉话次数 = 1;
			}
			if (ProceDure_Type == 4 && ABORT_CAUSE != 3 && MEDIA_TYPE == video)
			{
				VoLTE视频掉话次数 = 1;
			}
		}

		eventData.iCityID = iCityID;
		eventData.IMSI = imsi;
		//RX使用回填的ecgi
		eventData.iEci = ecgi;
		eventData.iTime = istime;
		eventData.wTimems = istimems; // 开始时间里面的毫秒
		eventData.strLoctp = strloctp;
	    eventData.strLabel = label;
		eventData.iLongitude = iLongitude;
		eventData.iLatitude = iLatitude;
		eventData.iBuildID = ibuildid;
		eventData.iHeight = iheight;
		eventData.position = position;
		eventData.Interface = StaticConfig.INTERFACE_RX; 
		eventData.iKpiSet = 1;
		eventData.iProcedureType = ProceDure_Type;
		eventData.iTestType = testType;
		eventData.iDoorType = iDoorType;
		eventData.iLocSource = locSource;
		eventData.confidentType =confidentType;
		eventData.iAreaType = iAreaType;
		eventData.iAreaID = iAreaID;
		eventData.lteScRSRP = LteScRSRP;
		eventData.lteScSinrUL = LteScSinrUL;
		eventData.eventStat.fvalue[0] = VoLTE语音掉话次数;
		eventData.eventStat.fvalue[1] = VoLTE视频掉话次数;
		eventData.eventDetial.sampleType = StaticConfig.FAILURE_SAMPLE;	
		eventData.eventDetial.strvalue[0]="掉话";
		eventData.eventDetial.fvalue[0] = LteScRSRP;
		eventData.eventDetial.fvalue[1] = LteScSinrUL;
		eventData.eventDetial.fvalue[2] = ABORT_CAUSE;
		eventData.eventDetial.fvalue[3] = MEDIA_TYPE;
		eventData.eventDetial.fvalue[4] = RESULT_CODE;
		eventData.lTrainKey = trainKey;
		eventData.iSectionId = sectionId;
		eventData.iSegmentId = segmentId;
		if(eventData.haveEventStat()){
			eventDataList.add(eventData);
		}
		return eventDataList;
	}

	@Override
	public void toString(StringBuffer sb)
	{
		

	}

}
