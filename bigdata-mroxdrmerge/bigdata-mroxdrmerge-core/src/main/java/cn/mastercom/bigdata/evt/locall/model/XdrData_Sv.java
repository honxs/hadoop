package cn.mastercom.bigdata.evt.locall.model;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;

import cn.mastercom.bigdata.StructData.GridItem;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.evt.locall.stat.EventData;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.DataAdapterConf.ParseItem;
import cn.mastercom.bigdata.util.DataAdapterReader;
import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;


public class XdrData_Sv extends XdrDataBase
{
	private Date tmDate = new Date();
	private static ParseItem parseItem;
	public int Interface;
	public long XDR_ID;
	public String IMSI;
	public int ProceDure_Type;

	public int REQUEST_RESULT;
	public int RESULT;
	public int SV_CAUSE;
	public int POST_FAILURE_CAUSE;
	public long RESP_DELAY;
	public int SV_DELAY;
	public long ECI;
	public StringBuffer value;

	// 统计字段
	private long SRVCC切换请求次数 = StaticConfig.Int_Abnormal;
	private long SRVCC切换成功次数 = StaticConfig.Int_Abnormal;

	// detail字段
	private String  SRVCC切换请求= "";
	private String  SRVCC切换请求失败= "";

	public XdrData_Sv()
	{
		super();
		clear();

		if (parseItem == null)
		{
			parseItem = MainModel.GetInstance().getEventAdapterConfig().getParseItem("LOCALL-Sv");
		}
	}

	@Override
	public int getInterfaceCode() {
		return StaticConfig.INTERFACE_SV;
	}

	public void clear()
	{

		Interface = StaticConfig.Int_Abnormal;
		;
		XDR_ID = StaticConfig.Int_Abnormal;
		IMSI = "";
		ProceDure_Type = StaticConfig.Int_Abnormal;

		REQUEST_RESULT = StaticConfig.Int_Abnormal;
		RESULT = StaticConfig.Int_Abnormal;
		SV_CAUSE = StaticConfig.Int_Abnormal;
		POST_FAILURE_CAUSE = StaticConfig.Int_Abnormal;
		RESP_DELAY = StaticConfig.Int_Abnormal;
		SV_DELAY = StaticConfig.Int_Abnormal;
		ECI = StaticConfig.Int_Abnormal;
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
			Interface = dataAdapterReader.GetIntValue("Interface", StaticConfig.Int_Abnormal);
			if (Interface>0 && Interface != StaticConfig.INTERFACE_SV)
			{
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
			ecgi=ECI;
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"XdrData_Sv.fillData_short error",
					"XdrData_Sv.fillData_short error: " + e.getMessage(),e);
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
			// stime
			tmDate = dataAdapterReader.GetDateValue("Procedure_Start_Time", new Date(1970, 1, 1));
			istime = (int) (tmDate.getTime() / 1000L);
			istimems = (int) (tmDate.getTime() % 1000L);
			// etime
			tmDate = dataAdapterReader.GetDateValue("Procedure_End_Time", new Date(1970, 1, 1));
			ietime = (int) (tmDate.getTime() / 1000L);
			ietimems = (int) (tmDate.getTime() % 1000L);


			Interface = dataAdapterReader.GetIntValue("Interface", StaticConfig.Int_Abnormal);
			XDR_ID = dataAdapterReader.GetLongValue("XDR_ID", StaticConfig.Int_Abnormal);
			imei = dataAdapterReader.GetStrValue("IMEI", "");
			IMSI = dataAdapterReader.GetStrValue("IMSI", "");
			ProceDure_Type = dataAdapterReader.GetIntValue("ProceDure_Type", StaticConfig.Int_Abnormal);

			REQUEST_RESULT = dataAdapterReader.GetIntValue("REQUEST_RESULT", StaticConfig.Int_Abnormal);
			String theResult = dataAdapterReader.GetStrValue("RESULT","-1000000");
			if("null".equals(theResult)||"NULL".equals(theResult)||"".equals(theResult)){
				RESULT = StaticConfig.Int_Abnormal;
			}else {
				RESULT = Integer.parseInt(theResult);
			}

			SV_CAUSE = dataAdapterReader.GetIntValue("SV_CAUSE", StaticConfig.Int_Abnormal);
			POST_FAILURE_CAUSE = dataAdapterReader.GetIntValue("POST_FAILURE_CAUSE", StaticConfig.Int_Abnormal);
			RESP_DELAY = dataAdapterReader.GetLongValue("RESP_DELAY", StaticConfig.Int_Abnormal);
			SV_DELAY = dataAdapterReader.GetIntValue("SV_DELAY", StaticConfig.Int_Abnormal);

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
			ecgi=ECI;
			value = dataAdapterReader.getTmStrs();
			return true;
		} catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"XdrData_Sv.fillData_short error",
					"XdrData_Sv.fillData_short error: " + e.getMessage(),e);
			return false;
		}
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
		EventData eventData = new EventData();

		if (ProceDure_Type == 1)
		{
			SRVCC切换请求次数 = 1;
			if (RESULT == 0)
			{
				SRVCC切换成功次数 = 1;
			}
		}

		if(SRVCC切换请求次数 == 1){
			SRVCC切换请求 = "SRVCC切换请求";
		}
		if (SRVCC切换请求次数==1 && SRVCC切换成功次数!=1)
		{
			SRVCC切换请求失败 = "SRVCC切换请求失败";
		}


		eventData.iCityID = iCityID;
		eventData.IMSI = imsi;
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
		eventData.Interface = StaticConfig.INTERFACE_SV;
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
        eventData.lTrainKey = trainKey;
        eventData.iSectionId = sectionId;
        eventData.iSegmentId = segmentId;

        eventData.eventStat.fvalue[0] = SRVCC切换请求次数;
        eventData.eventStat.fvalue[1] = SRVCC切换成功次数;

        if(SRVCC切换请求.length()>0){
            // 虽然有个请求详单，但还是归结于异常详单
            eventData.eventDetial.sampleType = StaticConfig.FAILURE_SAMPLE;
            eventData.eventDetial.strvalue[0] = SRVCC切换请求;
            eventData.eventDetial.fvalue[0] = LteScRSRP;
            eventData.eventDetial.fvalue[1] = LteScSinrUL;
            eventData.eventDetial.fvalue[2] = REQUEST_RESULT;
            eventData.eventDetial.fvalue[3] = RESULT;
            eventData.eventDetial.fvalue[4] = SV_CAUSE;
            eventData.eventDetial.fvalue[5] = POST_FAILURE_CAUSE;
            eventData.eventDetial.fvalue[6] = RESP_DELAY;
            eventData.eventDetial.fvalue[7] = SV_DELAY;
        }else{
            eventData.eventDetial = null;
        }
        if(eventData.haveEventStat()){
            //有事件才吐出，详单也是一样的
            eventDataList.add(eventData);
        }
        if(SRVCC切换请求失败.length()>0){
			EventData eventData1 = new EventData();
			eventData1.eventStat = null;
			eventData1.eventDetial.sampleType = StaticConfig.FAILURE_SAMPLE;
			eventData1.eventDetial.strvalue[0] = SRVCC切换请求失败;
			eventData1.eventDetial.fvalue[0] = LteScRSRP;
			eventData1.eventDetial.fvalue[1] = LteScSinrUL;
			eventData1.eventDetial.fvalue[2] = REQUEST_RESULT;
			eventData1.eventDetial.fvalue[3] = RESULT;
			eventData1.eventDetial.fvalue[4] = SV_CAUSE;
			eventData1.eventDetial.fvalue[5] = POST_FAILURE_CAUSE;
			eventData1.eventDetial.fvalue[6] = RESP_DELAY;
			eventData1.eventDetial.fvalue[7] = SV_DELAY;
			eventDataList.add(eventData);
		}
		return eventDataList;
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

		}
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
