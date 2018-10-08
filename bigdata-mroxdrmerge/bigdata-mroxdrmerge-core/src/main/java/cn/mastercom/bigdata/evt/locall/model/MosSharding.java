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

public class MosSharding extends XdrDataBase {
	
	private Date tmDate = new Date();
	private static ParseItem parseItem;
	public double mos = 0;
	public String md5IMSI = "";
	
	public MosSharding() {
		super();
		clear();
		if (parseItem == null)
		{
			parseItem = MainModel.GetInstance().getEventAdapterConfig().getParseItem("LOCALL-Mos_Sharding");
		}
	}

	@Override
	public int getInterfaceCode() {
		return StaticConfig.INTERFACE_NEW_S1U;
	}


	private void clear() {

		
	}


	@Override
	public ParseItem getDataParseItem() throws IOException {

		return parseItem;
	}

	@Override
	public boolean FillData_short(DataAdapterReader dataAdapterReader) throws ParseException, IOException {
		try{
			imsi = dataAdapterReader.GetLongValue("IMSI", 0);
			tmDate = dataAdapterReader.GetDateValue("Procedure_Start_Time", new Date(1970, 1, 1));
			istime = (int) (tmDate.getTime() / 1000L);
			istimems = (int) (tmDate.getTime() % 1000L);
			// etime
			tmDate = dataAdapterReader.GetDateValue("Procedure_End_Time", new Date(1970, 1, 1));
			ietime = (int) (tmDate.getTime() / 1000L);
			ietimems = (int) (tmDate.getTime() % 1000L);
		}catch(Exception e){
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"MosSharding.fillData_short error",
					"MosSharding.fillData_short error: " + e.getMessage(),e);
			return false;
		}
		return true;

	}

	@Override
	public boolean FillData(DataAdapterReader dataAdapterReader) throws ParseException, IOException {
		// TODO Auto-generated method stub
		try{
			imsi = dataAdapterReader.GetLongValue("IMSI", 0);
			tmDate = dataAdapterReader.GetDateValue("Procedure_Start_Time", new Date(1970, 1, 1));
			istime = (int) (tmDate.getTime() / 1000L);
			istimems = (int) (tmDate.getTime() % 1000L);
			// etime
			tmDate = dataAdapterReader.GetDateValue("Procedure_End_Time", new Date(1970, 1, 1));
			ietime = (int) (tmDate.getTime() / 1000L);
			ietimems = (int) (tmDate.getTime() % 1000L);
//			String strEci = dataAdapterReader.GetStrValue("ecgi", "");
//			if (strEci == null || strEci.length() == 0 || strEci.contains("."))
//			{
//				return false;
//			}
//
//			if (strEci.length() >= 12)
//			{
//				ecgi = Long.parseLong(strEci.substring(5, 10), 16) * 256L + Long.parseLong(strEci.substring(10, 12), 16);
//			}
			mos = dataAdapterReader.GetDoubleValue("mos", 1000000);
			md5IMSI = dataAdapterReader.GetStrValue("md5IMSI", "");
			imei = dataAdapterReader.GetStrValue("IMEI", "");
		}catch(Exception e){
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"MosSharding.fillData error",
					"MosSharding.fillData error: " + e.getMessage(),e);
			return false;
		}
		return true;

	}

	@Override
	public ArrayList<EventData> toEventData() {
		ArrayList<EventData> eventDataList = new ArrayList<EventData>();
		EventData eventData = new EventData();
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
		eventData.Interface = StaticConfig.INTERFACE_NEW_S1U;
		eventData.iKpiSet = 1;
		eventData.iProcedureType = 0;
		eventData.iTestType = testType;
		eventData.iDoorType = iDoorType;
		eventData.iLocSource = locSource;
		eventData.confidentType = confidentType;
		eventData.iAreaType = iAreaType;
		eventData.iAreaID = iAreaID;
		eventData.lteScRSRP = LteScRSRP;
		eventData.lteScSinrUL = LteScSinrUL;
		if(mos<2){
			eventData.eventStat.fvalue[8] = 1;
			eventData.eventDetial.strvalue[0]="MOS5S质差";
			eventData.eventDetial.sampleType = StaticConfig.FAILURE_SAMPLE;
			eventData.eventDetial.fvalue[0] = LteScRSRP;
			eventData.eventDetial.fvalue[1] = LteScSinrUL;
			
			eventDataList.add(eventData);
		}
		
		return eventDataList;
	}

	@Override
	public void toString(StringBuffer sb) {
		// TODO Auto-generated method stub
		
	}

}
