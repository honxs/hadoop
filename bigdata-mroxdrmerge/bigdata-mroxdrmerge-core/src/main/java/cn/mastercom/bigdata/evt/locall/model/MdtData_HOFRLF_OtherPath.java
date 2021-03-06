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

public class MdtData_HOFRLF_OtherPath extends XdrDataBase {

	private static ParseItem parseItem;
	private Date tmDate = new Date();
	private String collect_Type="";
	private double UE经度 = 0;
	private double UE纬度=0;
	private double 发生RLF小区标识;
	private int 发生RLF小区PCI=0;
	private int 发生RLF小区频点=0;
	private int 发生RLF小区RSRP;
	private int 发生RLF小区RSRQ;

	public MdtData_HOFRLF_OtherPath() {
		super();
		clear();
		if (parseItem == null) {
			parseItem = MainModel.GetInstance().getEventAdapterConfig().getParseItem("LOCALL-MDT-RLFHOF-OTHERPATH");
		}
	}

	@Override
	public int getInterfaceCode() {
		return StaticConfig.INTERFACE_MDT_RLFHOF;
	}

	@Override
	public ParseItem getDataParseItem() throws IOException {

		return parseItem;

	}

	public void clear() {

	}

	@Override
	public boolean FillData_short(DataAdapterReader dataAdapterReader) throws ParseException, IOException {

		try {
			s1apid = dataAdapterReader.GetLongValue("MME_UE_S1AP_ID", StaticConfig.Int_Abnormal);
			ecgi = dataAdapterReader.GetLongValue("Report_CID", 0);
			
			// stime
			tmDate = dataAdapterReader.GetDateValue("TimeStamp", new Date(1970, 1, 1));
			istime = (int) (tmDate.getTime() / 1000L);
			istimems = (int) (tmDate.getTime() % 1000L);
			

		} catch (Exception e) {
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"MdrData_HOFRLF_OtherPath.fillData_short error:",
					"MdrData_HOFRLF_OtherPath.fillData_short error: " + e.getMessage(),e);
			return false;
		}

		return true;
	}


	
	@Override
	public boolean FillData(DataAdapterReader dataAdapterReader) throws ParseException, IOException {
		//TODO 一些容错的问题
		try {

			s1apid = dataAdapterReader.GetLongValue("MME_UE_S1AP_ID", StaticConfig.Int_Abnormal);
			ecgi = dataAdapterReader.GetLongValue("Report_CID", 0);

			// stime
			tmDate = dataAdapterReader.GetDateValue("TimeStamp", new Date(1970, 1, 1));
			istime = (int) (tmDate.getTime() / 1000L);
			istimems = (int) (tmDate.getTime() % 1000L);

			collect_Type = dataAdapterReader.GetStrValue("CF_Type", "");
			UE经度 = dataAdapterReader.GetDoubleValue("Longitude", 0);
			UE纬度 = dataAdapterReader.GetDoubleValue("Latitude", 0);
			发生RLF小区标识 = dataAdapterReader.GetIntValue("RLF_CID",0);
			发生RLF小区PCI = dataAdapterReader.GetIntValue("RLF_PCI", 0);
			发生RLF小区频点 = dataAdapterReader.GetIntValue("RLF_RSRP", 0);

			发生RLF小区RSRP = dataAdapterReader.GetIntValue("RLF_RSRP",0);
			发生RLF小区RSRQ = dataAdapterReader.GetIntValue("RLF_RSRQ",0);
			imei = dataAdapterReader.GetStrValue("IMEI", "");
		} catch (Exception e) {
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"MdrData_HOFRLF_OtherPath.fillData error:",
					"MdrData_HOFRLF_OtherPath.fillData error: " + e.getMessage(),e);
			return false;
		}

		return true;
	}

	@Override
	public ArrayList<EventData> toEventData() {

		if("RLF".equals(collect_Type) && UE经度>0){
			iLatitude =(int) (UE纬度*10000000);
			iLongitude =(int) (UE经度*10000000);
		}

		ArrayList<EventData> eventDataList = new ArrayList<EventData>();
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
		eventData.Interface = StaticConfig.INTERFACE_MDT_RLFHOF;
		eventData.iKpiSet = 1;
		eventData.iProcedureType = 1;

		eventData.iTestType = testType;
		eventData.iDoorType = iDoorType;
		eventData.iLocSource = locSource;

		eventData.confidentType = confidentType;
		eventData.iAreaType = iAreaType;
		eventData.iAreaID = iAreaID;
		eventData.lteScRSRP = LteScRSRP;
		eventData.lteScSinrUL = LteScSinrUL;

		eventData.eventDetial.strvalue[0] = collect_Type;
		eventData.eventDetial.fvalue[0] = LteScRSRP;
		eventData.eventDetial.fvalue[1] = LteScSinrUL;
		eventData.eventDetial.fvalue[4] = 发生RLF小区PCI;
		eventData.eventDetial.fvalue[5] = 发生RLF小区频点;
		eventData.eventDetial.fvalue[6] = 发生RLF小区RSRP;
		eventData.eventDetial.fvalue[7] = 发生RLF小区RSRQ;
		if("RLF".equals(collect_Type)){
			eventData.eventStat.fvalue[1]=1;
		}else if("HOF".equals(collect_Type)){
			eventData.eventStat.fvalue[0]=1;
		}
		eventDataList.add(eventData);
		return eventDataList;
	}

	@Override
	public void toString(StringBuffer sb) {
		// TODO Auto-generated method stub

	}

}
