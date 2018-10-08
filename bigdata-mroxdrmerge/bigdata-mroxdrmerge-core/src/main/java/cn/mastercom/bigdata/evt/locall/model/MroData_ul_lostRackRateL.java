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

public class MroData_ul_lostRackRateL extends XdrDataBase {

	private Date tmDate = new Date();
	private static ParseItem parseItem;
	public StringBuffer value;

	public MroData_ul_lostRackRateL() {
		super();
		clear();
		if(parseItem == null)
		{
			parseItem = MainModel.GetInstance().getEventAdapterConfig().getParseItem("LOCALL-MRO_ULLOS");
		}	
	}

	@Override
	public int getInterfaceCode() {
		return 0;
	}

	public void clear(){
		value = new StringBuffer();
	}

	@Override
	public ParseItem getDataParseItem() throws IOException {
		return parseItem;
	}

	@Override
	public boolean FillData_short(DataAdapterReader dataAdapterReader) 
			throws ParseException, IOException {

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
			
			String aString = dataAdapterReader.GetStrValue("ECI", "0-0");
			ecgi = Long.parseLong(aString.split("-")[0])*256L+Long.parseLong(aString.split("-")[1]);
			s1apid = dataAdapterReader.GetLongValue("S1APID", 0);
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"MroData_ul_lostRackRatel.fillData_short error",
					"MroData_ul_lostRackRatel.fillData_short error: " + e.getMessage(),e);
			return false;
		}
		
	    return true;
		
	}

	@Override
	public boolean FillData(DataAdapterReader dataAdapterReader) throws ParseException, IOException {

		try{

			imsi = dataAdapterReader.GetLongValue("IMSI", 0);
			tmDate = dataAdapterReader.GetDateValue("Procedure_Start_Time", new Date(1970, 1, 1));
			istime = (int) (tmDate.getTime() / 1000L);
			istimems = (int) (tmDate.getTime() % 1000L);
			// etime
			tmDate = dataAdapterReader.GetDateValue("Procedure_End_Time", new Date(1970, 1, 1));
			ietime = (int) (tmDate.getTime() / 1000L);
			ietimems = (int) (tmDate.getTime() % 1000L);
			
			String aString = dataAdapterReader.GetStrValue("ECI", "0-0");
			ecgi = Long.parseLong(aString.split("-")[0])*256L+Long.parseLong(aString.split("-")[1]);
			s1apid = dataAdapterReader.GetLongValue("S1APID", 0);
			
			value = dataAdapterReader.getTmStrs();
			imei = dataAdapterReader.GetStrValue("IMEI", "");
		}catch(Exception e){
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"MroData_ul_lostRackRatel.fillData error",
					"MroData_ul_lostRackRatel.fillData error: " + e.getMessage(),e);
			return false;
		}
		return true;
	}

	@Override
	public ArrayList<EventData> toEventData() {
		
		return null;
	}

	@Override
	public void toString(StringBuffer sb) {
		StaticConfig.putCityNameByCityId();
		String fenge = parseItem.getSplitMark();
		if(fenge.contains("\\")){
			fenge = fenge.replace("\\", "");
		}
		
		sb.append(value);
		sb.append(fenge);
		sb.append(noEntryImsi).append(fenge); // 内蒙回填的imsi是没有加密的imsi
		sb.append(msisdn_neimeng).append(fenge);
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
