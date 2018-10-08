package cn.mastercom.bigdata.evt.locall.model;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import cn.mastercom.bigdata.StructData.GridItem;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.evt.locall.stat.EventData;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.DataAdapterConf.ParseItem;
import cn.mastercom.bigdata.util.DataAdapterReader;
import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;

public class XdrData_Mw extends XdrDataBase
{
	private Date tmDate = new Date();

	private static ParseItem parseItem;
	public int Interface;
	public long XDR_ID;
	public String MSISDN;
	public int ProceDure_Type;
	public int Service_Type;
	public int Procedure_Status;
	public long CALLING_NUMBER;
	public long CALLED_NUMBER;
	public int CALL_SIDE;
	public int SOURCE_ACCESS_TYPE;
	public int DEST_ACCESS_TYPE;
	public int REDIRECT_REASON;
	public int RESPONSE_CODE;

	public int FIRFAILTIME;
	public int ALERTING_TIME;
	public int ANSWER_TIME;
	public int RELEASE_TIME;
	public int CALL_DURATION;
	public long ECI;
	public StringBuffer value; 
	// 统计字段
	private long VoLTE语音始呼接通次数 = StaticConfig.Int_Abnormal;
	private long VoLTE语音终呼接通次数 = StaticConfig.Int_Abnormal;
	private long VoLTE语音始呼应答次数 = StaticConfig.Int_Abnormal;
	private long VoLTE语音终呼应答次数 = StaticConfig.Int_Abnormal;
	private long VoLTE语音始呼总次数 = StaticConfig.Int_Abnormal;
	private long VoLTE语音终呼总次数 = StaticConfig.Int_Abnormal;
	private long VoLTE视频始呼接通次数 = StaticConfig.Int_Abnormal;
	private long VoLTE视频终呼接通次数 = StaticConfig.Int_Abnormal;
	private long VoLTE视频始呼应答次数 = StaticConfig.Int_Abnormal;
	private long VoLTE视频终呼应答次数 = StaticConfig.Int_Abnormal;
	private long VoLTE视频始呼总次数 = StaticConfig.Int_Abnormal;
	private long VoLTE视频终呼总次数 = StaticConfig.Int_Abnormal;

	private long VoLTE语音始呼用户早释次数 = StaticConfig.Int_Abnormal;
	private long VoLTE语音终呼用户早释次数 = StaticConfig.Int_Abnormal;
	private long VoLTE视频始呼用户早释次数 = StaticConfig.Int_Abnormal;
	private long VoLTE视频终呼用户早释次数 = StaticConfig.Int_Abnormal;

	private long UE成功注册次数 = StaticConfig.Int_Abnormal;
	private long UE注册请求次数 = StaticConfig.Int_Abnormal;
	private double VoLTE语音始呼接续时长 = StaticConfig.Int_Abnormal;
	private double VoLTE视频始呼接续时长 = StaticConfig.Int_Abnormal;
	private List<Integer> RESPONSE_CODEArys;

	// 异常事件输出
	private long VoLTE语音始呼未接通 = StaticConfig.Int_Abnormal;
	private long VoLTE语音终呼未接通 = StaticConfig.Int_Abnormal;
	private long VoLTE视频始呼未接通 = StaticConfig.Int_Abnormal;
	private long VoLTE视频终呼未接通 = StaticConfig.Int_Abnormal;
	private long UE注册失败 = StaticConfig.Int_Abnormal;

	public XdrData_Mw()
	{
		super();
		clear();

		if (parseItem == null)
		{
			parseItem = MainModel.GetInstance().getEventAdapterConfig().getParseItem("LOCALL-Mw");
		}
	}

	@Override
	public int getInterfaceCode() {
		return StaticConfig.INTERFACE_MW;
	}

	public void clear()
	{
		value = new StringBuffer();
		Interface = StaticConfig.Int_Abnormal;
		XDR_ID = StaticConfig.Int_Abnormal;
		// public int IMSI;
		MSISDN = "";
		ProceDure_Type = StaticConfig.Int_Abnormal;

		Service_Type = StaticConfig.Int_Abnormal;
		Procedure_Status = StaticConfig.Int_Abnormal;
		CALLING_NUMBER = StaticConfig.Int_Abnormal;
		;
		CALLED_NUMBER = StaticConfig.Int_Abnormal;
		;
		CALL_SIDE = StaticConfig.Int_Abnormal;
		SOURCE_ACCESS_TYPE = StaticConfig.Int_Abnormal;
		DEST_ACCESS_TYPE = StaticConfig.Int_Abnormal;
		REDIRECT_REASON = StaticConfig.Int_Abnormal;
		RESPONSE_CODE = StaticConfig.Int_Abnormal;

		FIRFAILTIME = StaticConfig.Int_Abnormal;
		ALERTING_TIME = StaticConfig.Int_Abnormal;
		ANSWER_TIME = StaticConfig.Int_Abnormal;
		RELEASE_TIME = StaticConfig.Int_Abnormal;
		CALL_DURATION = StaticConfig.Int_Abnormal;
		ECI = StaticConfig.Int_Abnormal;
		RESPONSE_CODEArys = Arrays.asList(403, 404, 405, 413, 414, 415, 416, 422, 423, 480, 486, 487,488, 600, 603, 604,
				606);
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
			if (Interface>0 && Interface != StaticConfig.INTERFACE_MW)
			{
				return false;
			}


			imsi = dataAdapterReader.GetLongValue("IMSI", -1);
			// stime
			tmDate = dataAdapterReader.GetDateValue("Procedure_Start_Time", new Date(1970, 1, 1));
			istime = (int) (tmDate.getTime() / 1000L);
			istimems = (int) (tmDate.getTime() % 1000L);
			// etime
			tmDate = dataAdapterReader.GetDateValue("Procedure_End_Time", new Date(1970, 1, 1));
			ietime = (int) (tmDate.getTime() / 1000L);
			ietimems = (int) (tmDate.getTime() % 1000L);

			if (MainModel.GetInstance().getCompile().Assert(CompileMark.SiChuan))
			{
				String strEci = dataAdapterReader.GetStrValue("ECI", "");
				if (strEci.length() < 12)
				{
					return false;
				}

					ECI = Long.parseLong(strEci.substring(5, 10), 16) * 256L +
							Long.parseLong(strEci.substring(10, 12), 16);

			}
			else
			{
				ECI = dataAdapterReader.GetLongValue("ECI", StaticConfig.Int_Abnormal);
			}
			ecgi = ECI;

		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"XdrData_Mw.fillData_short error",
					"XdrData_Mw.fillData_short error: " + e.getMessage(),e);
			return false;
		}

		return true;

	}

	@Override
	public boolean FillData(DataAdapterReader dataAdapterReader) throws ParseException, IOException
	{
		try
		{

			imsi = dataAdapterReader.GetLongValue("IMSI", -1);

			// stime
			// stime
			tmDate = dataAdapterReader.GetDateValue("Procedure_Start_Time", new Date(1970, 1, 1));
			istime = (int) (tmDate.getTime() / 1000L);
			istimems = (int) (tmDate.getTime() % 1000L);
			// etime
			tmDate = dataAdapterReader.GetDateValue("Procedure_End_Time", new Date(1970, 1, 1));
			ietime = (int) (tmDate.getTime() / 1000L);
			ietimems = (int) (tmDate.getTime() % 1000L);
			value = dataAdapterReader.getTmStrs();
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"XdrData_Http.fillData_short error",
					"XdrData_Http.fillData_short error: " + e.getMessage(),e);
			return false;
		}
		Interface = dataAdapterReader.GetIntValue("Interface", StaticConfig.Int_Abnormal);
		XDR_ID = dataAdapterReader.GetLongValue("XDR_ID", StaticConfig.Int_Abnormal);
		imei = dataAdapterReader.GetStrValue("IMEI", "");
		MSISDN = dataAdapterReader.GetStrValue("MSISDN", "");
		ProceDure_Type = dataAdapterReader.GetIntValue("ProceDure_Type", StaticConfig.Int_Abnormal);
		Service_Type = dataAdapterReader.GetIntValue("Service_Type", StaticConfig.Int_Abnormal);
		Procedure_Status = dataAdapterReader.GetIntValue("Procedure_Status", StaticConfig.Int_Abnormal);
		String curCALLING_NUMBER = dataAdapterReader.GetStrValue("CALLING_NUMBER", "-10000");
		String curCALLED_NUMBER = dataAdapterReader.GetStrValue("CALLED_NUMBER", "-10000");
		if (curCALLING_NUMBER.startsWith("+86")||curCALLING_NUMBER.startsWith("#86")) {
			curCALLING_NUMBER = curCALLING_NUMBER.substring(3);
		}
		if(curCALLED_NUMBER.startsWith("+86")||curCALLED_NUMBER.startsWith("#86")){
			curCALLED_NUMBER = curCALLED_NUMBER.substring(3);
		}
		CALLING_NUMBER = Long.parseLong(curCALLING_NUMBER);
		CALLED_NUMBER = Long.parseLong(curCALLED_NUMBER);
		CALL_SIDE = dataAdapterReader.GetIntValue("CALL_SIDE", StaticConfig.Int_Abnormal);
		SOURCE_ACCESS_TYPE = dataAdapterReader.GetIntValue("SOURCE_ACCESS_TYPE", StaticConfig.Int_Abnormal);
		DEST_ACCESS_TYPE = dataAdapterReader.GetIntValue("DEST_ACCESS_TYPE", StaticConfig.Int_Abnormal);
		REDIRECT_REASON = dataAdapterReader.GetIntValue("REDIRECT_REASON", StaticConfig.Int_Abnormal);
		RESPONSE_CODE = dataAdapterReader.GetIntValue("RESPONSE_CODE", StaticConfig.Int_Abnormal);

		FIRFAILTIME = dataAdapterReader.GetIntValue("FIRFAILTIME", StaticConfig.Int_Abnormal);
		ALERTING_TIME = dataAdapterReader.GetIntValue("ALERTING_TIME", StaticConfig.Int_Abnormal);
		ANSWER_TIME = dataAdapterReader.GetIntValue("ANSWER_TIME", StaticConfig.Int_Abnormal);
		RELEASE_TIME = dataAdapterReader.GetIntValue("RELEASE_TIME", StaticConfig.Int_Abnormal);
		CALL_DURATION = dataAdapterReader.GetIntValue("CALL_DURATION", StaticConfig.Int_Abnormal);

		if (MainModel.GetInstance().getCompile().Assert(CompileMark.SiChuan))
		{
			String strEci = dataAdapterReader.GetStrValue("ECI", "");
			if (strEci.length() < 12)
			{
				return false;
			}
			ECI = Long.parseLong(strEci.substring(5, 10), 16) * 256L + Long.parseLong(strEci.substring(10, 12), 16);
		}
		else
		{
			ECI = dataAdapterReader.GetLongValue("ECI", StaticConfig.Int_Abnormal);
		}
		ecgi = ECI;

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
		int 始呼 = 0;
		int 终呼 = 1;
		int 语言 = 0;
		int 视频 = 1;

		if (MainModel.GetInstance().getCompile().Assert(CompileMark.QingHai)
				|| MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng)
				|| MainModel.GetInstance().getCompile().Assert(CompileMark.TianJin)
				|| MainModel.GetInstance().getCompile().Assert(CompileMark.GuangXi2))
		{
			始呼 = 1;
			终呼 = 2;
			StaticConfig.successProdoceStatusValue = 1;
			语言 = 1;
			视频 = 2;
		}

		ArrayList<EventData> eventDataList = new ArrayList<EventData>();
		EventData eventData = new EventData();
		
		if (Service_Type == 语言 && ProceDure_Type == 5 && CALL_SIDE == 始呼 && (ALERTING_TIME > 0 || ANSWER_TIME > 0))
		{
			VoLTE语音始呼接通次数 = 1;
		}
		if (Service_Type == 语言 && ProceDure_Type == 5 && CALL_SIDE == 终呼 && ALERTING_TIME > 0)
		{
			VoLTE语音终呼接通次数 = 1;
		}
		if (Service_Type == 语言 && ProceDure_Type == 5 && CALL_SIDE == 始呼 && ANSWER_TIME > 0)
		{
			VoLTE语音始呼应答次数 = 1;
		}
		if (Service_Type == 语言 && ProceDure_Type == 5 && CALL_SIDE == 终呼 && ANSWER_TIME > 0)
		{
			VoLTE语音终呼应答次数 = 1;
		}

		if (Service_Type == 语言 && ProceDure_Type == 5 && CALL_SIDE == 始呼)
		{
			VoLTE语音始呼总次数 = 1;
		}

		if (Service_Type == 语言 && ProceDure_Type == 5 && CALL_SIDE == 终呼)
		{
			VoLTE语音终呼总次数 = 1;
		}

		if (Service_Type == 视频 && ProceDure_Type == 5 && CALL_SIDE == 始呼 && (ALERTING_TIME > 0 || ANSWER_TIME > 0))
		{
			VoLTE视频始呼接通次数 = 1;
		}

		if (Service_Type == 视频 && ProceDure_Type == 5 && CALL_SIDE == 终呼 && ALERTING_TIME > 0)
		{
			VoLTE视频终呼接通次数 = 1;
		}

		if (Service_Type == 视频 && ProceDure_Type == 5 && CALL_SIDE == 始呼 && ANSWER_TIME > 0)
		{
			VoLTE视频始呼应答次数 = 1;
		}

		if (Service_Type == 视频 && ProceDure_Type == 5 && CALL_SIDE == 终呼 && ANSWER_TIME > 0)
		{
			VoLTE视频终呼应答次数 = 1;
		}
		if (Service_Type == 视频 && ProceDure_Type == 5 && CALL_SIDE == 始呼)
		{
			VoLTE视频始呼总次数 = 1;
		}
		if (Service_Type == 视频 && ProceDure_Type == 5 && CALL_SIDE == 终呼)
		{
			VoLTE视频终呼总次数 = 1;
		}

		/*
		 * =================================修改4个指标=============================
		 */

		if (Service_Type == 语言 && ProceDure_Type == 5 && CALL_SIDE == 始呼)
		{

			if (RESPONSE_CODEArys.contains(RESPONSE_CODE))
			{
				VoLTE语音始呼用户早释次数 = 1;

			}

		}

		if (Service_Type == 语言 && ProceDure_Type == 5 && CALL_SIDE == 终呼)
		{

			if (RESPONSE_CODEArys.contains(RESPONSE_CODE))
			{
				VoLTE语音终呼用户早释次数 = 1;

			}

		}

		if (Service_Type == 视频 && ProceDure_Type == 5 && CALL_SIDE == 始呼)
		{

			if (RESPONSE_CODEArys.contains(RESPONSE_CODE))
			{
				VoLTE视频始呼用户早释次数 = 1;

			}
		}

		if (Service_Type == 视频 && ProceDure_Type == 5 && CALL_SIDE == 终呼)
		{

			if (RESPONSE_CODEArys.contains(RESPONSE_CODE))
			{
				VoLTE视频终呼用户早释次数 = 1;

			}
		}


		if (ProceDure_Type == 1 && Procedure_Status == StaticConfig.successProdoceStatusValue)
		{
			UE成功注册次数 = 1;
		}
		if (ProceDure_Type == 1)
		{
			UE注册请求次数 = 1;
		}

		if (Service_Type == 语言 && ProceDure_Type == 5 && CALL_SIDE == 始呼)
		{
			VoLTE语音始呼接续时长 = ALERTING_TIME;
		}
		if (Service_Type == 视频 && ProceDure_Type == 5 && CALL_SIDE == 始呼)
		{
			VoLTE视频始呼接续时长 = ALERTING_TIME;
		}
		// =========================================异常事件输出
		if (Service_Type == 语言 && ProceDure_Type == 5 && CALL_SIDE == 始呼 && (ALERTING_TIME <= 0 && ANSWER_TIME <= 0))
		{
			//去掉早释放的
			if(VoLTE语音始呼用户早释次数!=1){
				VoLTE语音始呼未接通 = 1;
			}
		}
		if (Service_Type == 语言 && ProceDure_Type == 5 && CALL_SIDE == 终呼 && ALERTING_TIME <= 0)
		{
			if(VoLTE语音终呼用户早释次数!=1){
				VoLTE语音终呼未接通 = 1;
			}
		}
		if (Service_Type == 视频 && ProceDure_Type == 5 && CALL_SIDE == 始呼 && (ALERTING_TIME <= 0 && ANSWER_TIME <= 0))
		{
			if(VoLTE视频始呼用户早释次数!=1){
				VoLTE视频始呼未接通 = 1;
			}
		}
		if (Service_Type == 视频 && ProceDure_Type == 5 && CALL_SIDE == 终呼 && ALERTING_TIME <= 0)
		{
			if(VoLTE视频终呼用户早释次数!=1){
				VoLTE视频终呼未接通 = 1;
			}
		}
		if (ProceDure_Type == 1 && Procedure_Status != StaticConfig.successProdoceStatusValue)
		{
			UE注册失败 = 1;
		}

		eventData.iCityID = iCityID;
		eventData.IMSI = imsi;
		eventData.iEci = ECI;
		eventData.iTime = istime;
		eventData.wTimems = istimems; // 开始时间里面的毫秒
		eventData.strLoctp = strloctp;
		eventData.strLabel = label;
		eventData.iLongitude = iLongitude;
		eventData.iLatitude = iLatitude;
		eventData.iBuildID = ibuildid;
		eventData.iHeight = iheight;
		eventData.position = position;
		eventData.Interface = StaticConfig.INTERFACE_MW;
		eventData.iKpiSet = 1;
		eventData.iProcedureType = ProceDure_Type;
		eventData.iTestType = testType;
		eventData.iDoorType = iDoorType;
		eventData.iLocSource = locSource;
		eventData.confidentType = confidentType;
		eventData.iAreaType = iAreaType;
		eventData.iAreaID = iAreaID;
		eventData.lteScRSRP = LteScRSRP;
		eventData.lteScSinrUL = LteScSinrUL;
		// eventData.iDoorType = new Random().nextInt(5);
		// eventData.iLocSource = new Random().nextInt(5);
		// event detail

		// eventDetial
		eventData.eventStat.fvalue[0] = VoLTE语音始呼接通次数;
		eventData.eventStat.fvalue[1] = VoLTE语音终呼接通次数;
		eventData.eventStat.fvalue[2] = VoLTE语音始呼应答次数;
		eventData.eventStat.fvalue[3] = VoLTE语音终呼应答次数;
		eventData.eventStat.fvalue[4] = VoLTE语音始呼总次数;
		eventData.eventStat.fvalue[5] = VoLTE语音终呼总次数;
		eventData.eventStat.fvalue[6] = VoLTE视频始呼接通次数;
		eventData.eventStat.fvalue[7] = VoLTE视频终呼接通次数;
		eventData.eventStat.fvalue[8] = VoLTE视频始呼应答次数;
		eventData.eventStat.fvalue[9] = VoLTE视频终呼应答次数;
		eventData.eventStat.fvalue[10] = VoLTE视频始呼总次数;
		eventData.eventStat.fvalue[11] = VoLTE视频终呼总次数;
		eventData.eventStat.fvalue[12] = VoLTE语音始呼接续时长;
		eventData.eventStat.fvalue[13] = VoLTE视频始呼接续时长;
		eventData.eventStat.fvalue[14] = VoLTE语音始呼用户早释次数;
		eventData.eventStat.fvalue[15] = VoLTE语音终呼用户早释次数;
		eventData.eventStat.fvalue[16] = VoLTE视频始呼用户早释次数;
		eventData.eventStat.fvalue[17] = VoLTE视频终呼用户早释次数;
		eventData.eventStat.fvalue[18] = UE成功注册次数;
		eventData.eventStat.fvalue[19] = UE注册请求次数;

		if (!eventData.haveEventStat())
		{ // 如果没有统计数据，那么eventStat设置为空
			eventData.eventStat = null;
		}

		boolean haveDetail = false;
		// 异常事件输出
		if (VoLTE语音始呼未接通 == 1)
		{
			eventData.eventDetial.strvalue[0] = "VoLTE语音始呼未接通";
			haveDetail = true;
		}
		else if (VoLTE语音终呼未接通 == 1)
		{
			eventData.eventDetial.strvalue[0] = "VoLTE语音终呼未接通";
			haveDetail = true;
		}
		else if (VoLTE视频始呼未接通 == 1)
		{
			eventData.eventDetial.strvalue[0] = "VoLTE视频始呼未接通";
			haveDetail = true;
		}
		else if (VoLTE视频终呼未接通 == 1)
		{
			eventData.eventDetial.strvalue[0] = "VoLTE视频终呼未接通";
			haveDetail = true;
		}

		if (UE注册失败 == 1)
		{
			eventData.eventDetial.strvalue[0] = "UE注册失败";
			haveDetail = true;
		}

		if (!haveDetail)
		{
			eventData.eventDetial = null;
		}

		if (eventData.eventDetial != null)
		{
			eventData.eventDetial.sampleType = StaticConfig.FAILURE_SAMPLE;
			eventData.eventDetial.fvalue[0] = LteScRSRP;
			eventData.eventDetial.fvalue[1] = LteScSinrUL;

			eventData.eventDetial.fvalue[2] = Procedure_Status;
			eventData.eventDetial.fvalue[3] = CALLING_NUMBER;
			eventData.eventDetial.fvalue[4] = CALLED_NUMBER;
			eventData.eventDetial.fvalue[5] = CALL_SIDE;
			eventData.eventDetial.fvalue[6] = SOURCE_ACCESS_TYPE;
			eventData.eventDetial.fvalue[7] = DEST_ACCESS_TYPE;
			eventData.eventDetial.fvalue[8] = REDIRECT_REASON;
			eventData.eventDetial.fvalue[9] = RESPONSE_CODE;
			eventData.eventDetial.fvalue[10] = FIRFAILTIME;
			eventData.eventDetial.fvalue[11] = ALERTING_TIME;
			eventData.eventDetial.fvalue[12] = ANSWER_TIME;
			eventData.eventDetial.fvalue[13] = RELEASE_TIME;
			eventData.eventDetial.fvalue[14] = CALL_DURATION;
		}
		eventData.lTrainKey = trainKey;
		eventData.iSectionId = sectionId;
		eventData.iSegmentId = segmentId;
		
		eventDataList.add(eventData);
		return eventDataList;

	}

	@Override
	public void toString(StringBuffer sb)
	{
		
		StaticConfig.putCityNameByCityId();
		String fenge = parseItem.getSplitMark();
		if(fenge.contains("\\")){
			fenge = fenge.replace("\\", "");
		}
		
		sb.append(value);
		sb.append(fenge);
		sb.append(iLongitude);sb.append(fenge);
		sb.append(iLatitude);sb.append(fenge);
		sb.append(iheight);sb.append(fenge);
		sb.append(iDoorType);sb.append(fenge);
		
		sb.append(iRadius);sb.append(fenge);
		GridItem gridItem  = GridItem.GetGridItem(0,iLongitude,iLatitude);
		
		int icentLng =gridItem.getBRLongitude()/2+gridItem.getTLLongitude()/2;
		int icentLat = gridItem.getBRLatitude()/2+gridItem.getTLLatitude()/2;
		
		if(StaticConfig.cityId_Name.containsKey(iCityID)){
			sb.append(StaticConfig.cityId_Name.get(iCityID)+"_"+icentLng+"_"+icentLat);sb.append(fenge); 
		}else {
			sb.append("nocity"+"_"+icentLng+"_"+icentLat);sb.append(fenge);
		}
		
		sb.append(-1);sb.append(fenge);
		sb.append(-1);

	}

}
