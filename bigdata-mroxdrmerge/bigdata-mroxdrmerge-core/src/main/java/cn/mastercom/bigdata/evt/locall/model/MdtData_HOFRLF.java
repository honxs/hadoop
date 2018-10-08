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

/**
 * 这是中兴的
 */
public class MdtData_HOFRLF extends XdrDataBase {

	private static ParseItem parseItem;
	private Date tmDate = new Date();
	

	public MdtData_HOFRLF() {
		super();
		clear();
		if (parseItem == null) {
			parseItem = MainModel.GetInstance().getEventAdapterConfig().getParseItem("LOCALL-MDT-RLFHOF");
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
			s1apid = dataAdapterReader.GetLongValue("Report_MME_UES1APID", StaticConfig.Int_Abnormal);
			ecgi = dataAdapterReader.GetLongValue("Report_ECI", 0);
			
			// stime
			tmDate = dataAdapterReader.GetDateValue("STARTTIME", new Date(1970, 1, 1));
			istime = (int) (tmDate.getTime() / 1000L);
			istimems = dataAdapterReader.GetIntValue("STARTTIME_MS", 0);
			

		} catch (Exception e) {
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"MdrData_HOFRLF.fillData_short error:",
					"MdrData_HOFRLF.fillData_short error: " + e.getMessage(),e);
			return false;
		}

		return true;
	}

	private String collect_Type="";
	private int collect_enbid=0;
	private int report_enbid=0;
	private double curLng = 0;
	private double curLat=0;
	private int collect_pci=0;
	private int collect_arc=0;
	
	@Override
	public boolean FillData(DataAdapterReader dataAdapterReader) throws ParseException, IOException {
		//TODO 一些容错的问题
		try {
			s1apid = dataAdapterReader.GetLongValue("Report_MME_UES1APID", StaticConfig.Int_Abnormal);
			ecgi = dataAdapterReader.GetLongValue("Report_ECI", 0);
			
			// stime
			tmDate = dataAdapterReader.GetDateValue("STARTTIME", new Date(1970, 1, 1));
			istime = (int) (tmDate.getTime() / 1000L);
			istimems = dataAdapterReader.GetIntValue("STARTTIME_MS", 0);
			collect_Type = dataAdapterReader.GetStrValue("collect_Type", "");
			
			collect_enbid = dataAdapterReader.GetIntValue("collect_enbid", 0);
			report_enbid = dataAdapterReader.GetIntValue("report_enbid", 0);

			collect_pci = dataAdapterReader.GetIntValue("collect_pci", 0);
			collect_arc = dataAdapterReader.GetIntValue("collect_arc", 0);
			
			curLng = dataAdapterReader.GetDoubleValue("curLng", 0);
			curLat = dataAdapterReader.GetDoubleValue("curLat", 0);
			if("RLF".equals(collect_Type) && collect_enbid!=report_enbid && curLat==0 
					&& collect_enbid!=0 && report_enbid!=0){
				return false;
			}
			imei = dataAdapterReader.GetStrValue("IMEI", "");
		} catch (Exception e) {
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"MdrData_HOFRLF.fillData error:",
					"MdrData_HOFRLF.fillData error: " + e.getMessage(),e);
			return false;
		}

		return true;
	}

	@Override
	public ArrayList<EventData> toEventData() {

		// 哈尔滨的需要有经纬度
		if(MainModel.GetInstance().getCompile().Assert(CompileMark.HaErBin)){
			if(curLat>0){
				iLatitude =(int) (curLat*10000000);
				iLongitude =(int) (curLng*10000000);
				confidentType = StaticConfig.OM;
			}
			
		}else{
			if("RLF".equals(collect_Type) && curLat>0){
				iLatitude =(int) (curLat*10000000);
				iLongitude =(int) (curLng*10000000);
				confidentType = StaticConfig.OM;
			}	
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
		eventData.eventDetial.fvalue[2] = collect_pci;
		eventData.eventDetial.fvalue[3] = collect_arc;
		if("RLF".equals(collect_Type)){
			eventData.eventStat.fvalue[1]=1;
		}else if("HOF".equals(collect_Type)){
			eventData.eventStat.fvalue[0]=1;
		}


		if(!"".equals(collect_Type)){
			if(MainModel.GetInstance().getCompile().Assert(CompileMark.GanSu)){
				eventData.eventStat=null;
			}
			eventDataList.add(eventData);
		} 
		
		return eventDataList;
	}

	@Override
	public void toString(StringBuffer sb) {

	}

}
