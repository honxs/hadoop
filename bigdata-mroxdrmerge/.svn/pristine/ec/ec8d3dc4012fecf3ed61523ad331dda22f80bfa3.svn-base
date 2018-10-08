package cn.mastercom.bigdata.evt.locall.model;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;

import cn.mastercom.bigdata.StructData.GridItem;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.evt.locall.stat.EventData;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.DataAdapterConf.ParseItem;
import cn.mastercom.bigdata.util.DataAdapterReader;
import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;


public class XdrData_Mg extends XdrDataBase
{
	private Date tmDate = new Date();
	
	private static ParseItem parseItem;
	private long Cell_ID;
	
	private Date d_beginTime;
	private Date d_endTime;
	private String strTime;
	private StringBuffer value; 
    //2017/6/30  0:23:34
//	private SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd  HH:mm:ss");
	
	public XdrData_Mg()
	{
		super();
		clear();
		
		if(parseItem == null)
		{
			parseItem = MainModel.GetInstance().getEventAdapterConfig().getParseItem("LOCALL-S1-Mg");
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
	public ParseItem getDataParseItem() throws IOException
	{
		return parseItem;
	}

	@Override
	public boolean FillData_short(DataAdapterReader dataAdapterReader) throws ParseException, IOException
	{
		try
		{
		
			tmDate = new Date(dataAdapterReader.GetLongValue("Procedure_Start_Time", -1));
			// stime
			istime = (int) (tmDate.getTime() / 1000L);
			istimems = (int) (tmDate.getTime() % 1000L);
			// etime
			tmDate = new Date(dataAdapterReader.GetLongValue("Procedure_End_Time", -1));
			ietime = (int) (tmDate.getTime() / 1000L);
			ietimems = (int) (tmDate.getTime() % 1000L);

			imsi = dataAdapterReader.GetLongValue("IMSI", 0);
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"XdrData_Mg.fillData_short error",
					"XdrData_Mg.fillData_short error: " + e.getMessage(),e);
			return false;
		}
		
	    return true;
	}

	@Override
	public boolean FillData(DataAdapterReader dataAdapterReader) throws ParseException, IOException
	{
		try
		{
			tmDate = new Date(dataAdapterReader.GetLongValue("Procedure_Start_Time", -1));
			// stime
			istime = (int) (tmDate.getTime() / 1000L);
			istimems = (int) (tmDate.getTime() % 1000L);
			// etime
			tmDate = new Date(dataAdapterReader.GetLongValue("Procedure_End_Time", -1));
			ietime = (int) (tmDate.getTime() / 1000L);
			ietimems = (int) (tmDate.getTime() % 1000L);

			imsi = dataAdapterReader.GetLongValue("IMSI", 0);

			value = dataAdapterReader.getTmStrs();
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"XdrData_Mg.fillData error",
					"XdrData_Mg.fillData error: " + e.getMessage(),e);
			return false;
		}
		
		return true;
	}

	@Override
	public ArrayList<EventData> toEventData()
	{
		// 不用写了
		return new ArrayList<>();
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
