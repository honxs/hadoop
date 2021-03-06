package cn.mastercom.bigdata.evt.locall.model;



import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import cn.mastercom.bigdata.StructData.GridItem;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.evt.locall.stat.EventData;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.DataAdapterConf.ParseItem;
import cn.mastercom.bigdata.util.DataAdapterReader;
import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;

public class XdrData_Uu extends XdrDataBase
{
	private Date tmDate = new Date();
	private static ParseItem parseItem;
	private StringBuffer value;

	public int Procedure_Type;
	public int Procedure_Status;
	public int KeyWord1;
	public int tac;

	private long eNB间切换出成功次数;
	private long eNB间切换出请求次数;
	private long eNB内切换出成功次数;
	private long eNB内切换出请求次数;
	private long RRC连接建立成功次数;
	private long RRC连接建立请求次数;
	private long RRC连接建立时长;
	private long RRC连接重建成功次数;

	// 异常事件detail
	private long 专用承载激活失败;
	private long eNB间切换出失败;
	private long eNB内切换出失败;
	private long RRC连接建立失败;
	private long RRC连接重建失败;

	private long 专用承载激活成功;
	private long eNB间切换出成功;
	private long eNB内切换出成功;
	private long RRC连接建立成功;
	private long RRC连接重建成功;

	public static HashMap<Integer, Integer> produceTypeMaps = new HashMap<>();

	public XdrData_Uu()
	{
		super();
		clear();

		if (parseItem == null)
		{
			parseItem = MainModel.GetInstance().getEventAdapterConfig().getParseItem("LOCALL-uu");
		}
	}

	@Override
	public int getInterfaceCode() {
		return StaticConfig.INTERFACE_UU;
	}

	public void clear()
	{
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
			tmDate = dataAdapterReader.GetDateValue("Procedure_Start_Time", new Date(1970, 1, 1));
			istime = (int) (tmDate.getTime() / 1000L);
			istimems = (int) (tmDate.getTime() % 1000L);
			// etime
			tmDate = dataAdapterReader.GetDateValue("Procedure_End_Time", new Date(1970, 1, 1));
			ietime = (int) (tmDate.getTime() / 1000L);
			ietimems = (int) (tmDate.getTime() % 1000L);
			imsi = dataAdapterReader.GetLongValue("IMSI", 0);

			// TODO 看地市了
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.YunNan))
			{
				initUUProduceType();

				ecgi = dataAdapterReader.GetLongValue("CELLID", 0);
				s1apid = dataAdapterReader.GetLongValue("LAST_MME_UE_S1AP_ID", 0);
				if (produceTypeMaps.containsKey(Procedure_Type))
				{
					Procedure_Type = produceTypeMaps.get(Procedure_Type);
				}
			}
			else
			{
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.QingHai))
				{
					s1apid = dataAdapterReader.GetLongValue("LAST_MME_UE_S1AP_ID", 0);
				}
				long cellID = dataAdapterReader.GetLongValue("CELLID", 0);
				long enbID = dataAdapterReader.GetLongValue("ENBID", 0);
				if (cellID > 256)
				{
					ecgi = cellID;
				}
				else
				{
					ecgi = enbID * 256L + cellID;
				}

			}

		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"XdrData_Uu.fillData_short error",
					"XdrData_Uu.fillData_short error: " + e.getMessage(),e);
			return false;
		}

		return true;
	}

	@Override
	public boolean FillData(DataAdapterReader dataAdapterReader) throws ParseException, IOException
	{
		try
		{
			tmDate = dataAdapterReader.GetDateValue("Procedure_Start_Time", new Date(1970, 1, 1));
			istime = (int) (tmDate.getTime() / 1000L);
			istimems = (int) (tmDate.getTime() % 1000L);
			// etime
			tmDate = dataAdapterReader.GetDateValue("Procedure_End_Time", new Date(1970, 1, 1));
			ietime = (int) (tmDate.getTime() / 1000L);
			ietimems = (int) (tmDate.getTime() % 1000L);

			imsi = dataAdapterReader.GetLongValue("IMSI", 0);
			imei = dataAdapterReader.GetStrValue("IMEI", "");
			Procedure_Type = dataAdapterReader.GetIntValue("Procedure_Type", StaticConfig.Int_Abnormal);
			Procedure_Status = dataAdapterReader.GetIntValue("Procedure_Status", StaticConfig.Int_Abnormal);
			KeyWord1 = dataAdapterReader.GetIntValue("KeyWord1", StaticConfig.Int_Abnormal);
			tac = dataAdapterReader.GetIntValue("Tac", StaticConfig.Int_Abnormal);
			// TODO 看地市了
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.YunNan))
			{
				initUUProduceType();

				ecgi = dataAdapterReader.GetLongValue("CELLID", 0);
				s1apid = dataAdapterReader.GetLongValue("LAST_MME_UE_S1AP_ID", 0);
				if (produceTypeMaps.containsKey(Procedure_Type))
				{
					Procedure_Type = produceTypeMaps.get(Procedure_Type);
				}
			}
			else
			{
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.QingHai))
				{
					s1apid = dataAdapterReader.GetLongValue("LAST_MME_UE_S1AP_ID", 0);
				}

				long cellID = dataAdapterReader.GetLongValue("CELLID", 0);
				long enbID = dataAdapterReader.GetLongValue("ENBID", 0);
				if (cellID > 256)
				{
					ecgi = cellID;
				}
				else
				{
					ecgi = enbID * 256L + cellID;
				}
			}

			// value = dataAdapterReader.getTmStrs();
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"XdrData_Uu.fillData error",
					"XdrData_Uu.fillData error: " + e.getMessage(),e);
			return false;
		}

		return true;
	}

	@Override
	public ArrayList<EventData> toEventData()
	{
		ArrayList<EventData> eventDataLists = new ArrayList<EventData>();

		boolean haveEventStat = false;

		haveEventStat = statToEvent(haveEventStat);

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
		eventData.Interface = StaticConfig.INTERFACE_UU;
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

		eventData.eventStat.fvalue[0] = eNB间切换出成功次数;
		eventData.eventStat.fvalue[1] = eNB间切换出请求次数;
		eventData.eventStat.fvalue[2] = eNB内切换出成功次数;
		eventData.eventStat.fvalue[3] = eNB内切换出请求次数;
		eventData.eventStat.fvalue[4] = RRC连接建立成功次数;
		eventData.eventStat.fvalue[5] = RRC连接建立请求次数;
		eventData.eventStat.fvalue[6] = RRC连接建立时长;
		eventData.eventStat.fvalue[7] = RRC连接重建成功次数;

		eventData.iAreaType = iAreaType;
		eventData.iAreaID = iAreaID;
		
		eventData.lTrainKey = trainKey;
		eventData.iSectionId = sectionId;
		eventData.iSegmentId = segmentId;

		// detail 异常事件
		String exceptionEetail = "";
		String successDetail = "";
		if (Procedure_Type == 13 && Procedure_Status != StaticConfig.successProdoceStatusValue)
		{
			专用承载激活失败 = 1;
			exceptionEetail = "专用承载激活失败";
		}
		else if (Procedure_Type == 13 && Procedure_Status == StaticConfig.successProdoceStatusValue)
		{
			专用承载激活成功 = 1;
			successDetail = "专用承载激活成功";
		}

		if (Procedure_Type == 8 && Procedure_Status != StaticConfig.successProdoceStatusValue)
		{
			eNB间切换出失败 = 1;
			exceptionEetail = "eNB间切换出失败";
		}
		else if (Procedure_Type == 8 && Procedure_Status == StaticConfig.successProdoceStatusValue)
		{
			eNB间切换出成功 = 1;
			successDetail = "eNB间切换出成功";
		}

		if (Procedure_Type == 7 && Procedure_Status != StaticConfig.successProdoceStatusValue)
		{
			eNB内切换出失败 = 1;
			exceptionEetail = "eNB内切换出失败";
		}
		else if (Procedure_Type == 7 && Procedure_Status == StaticConfig.successProdoceStatusValue)
		{
			eNB内切换出成功 = 1;
			successDetail = "eNB内切换出成功";
		}

		if (Procedure_Type == 1 && Procedure_Status != StaticConfig.successProdoceStatusValue)
		{
			RRC连接建立失败 = 1;
			exceptionEetail = "RRC连接建立失败";
		}
		else if (Procedure_Type == 1 && Procedure_Status == StaticConfig.successProdoceStatusValue)
		{
			RRC连接建立成功 = 1;
			successDetail = "RRC连接建立成功";
		}

		if (Procedure_Type == 4 && Procedure_Status != StaticConfig.successProdoceStatusValue)
		{
			RRC连接重建失败 = 1;
			exceptionEetail = "RRC连接重建失败";
		}
		else if (Procedure_Type == 4 && Procedure_Status == StaticConfig.successProdoceStatusValue)
		{
			RRC连接重建成功 = 1;
			successDetail = "RRC连接重建成功";
		}
		// 如果要输出失败的事件
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.EventDataOutFailure))
		{
			if (exceptionEetail.length() > 0 && successDetail.length() == 0)
			{
				eventData.eventDetial.sampleType = StaticConfig.FAILURE_SAMPLE;
				eventData.eventDetial.strvalue[0] = exceptionEetail;
				eventData.eventDetial.fvalue[0] = LteScRSRP;
				eventData.eventDetial.fvalue[1] = LteScSinrUL;
				eventData.eventDetial.fvalue[2] = 专用承载激活失败;
				eventData.eventDetial.fvalue[3] = eNB间切换出失败;
				eventData.eventDetial.fvalue[4] = eNB内切换出失败;
				eventData.eventDetial.fvalue[5] = RRC连接建立失败;
				eventData.eventDetial.fvalue[6] = RRC连接重建失败;
			}
		}
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.EventDataOutSuccess))
		{

			if (successDetail.length() > 0 && exceptionEetail.length() == 0)
			{
				eventData.eventDetial.sampleType = StaticConfig.SUCCESS_SAMPLE;
				eventData.eventDetial.strvalue[0] = successDetail;
				eventData.eventDetial.fvalue[0] = LteScRSRP;
				eventData.eventDetial.fvalue[1] = LteScSinrUL;
				eventData.eventDetial.fvalue[2] = 专用承载激活成功;
				eventData.eventDetial.fvalue[3] = eNB间切换出成功;
				eventData.eventDetial.fvalue[4] = eNB内切换出成功;
				eventData.eventDetial.fvalue[5] = RRC连接建立成功;
				eventData.eventDetial.fvalue[6] = RRC连接重建成功;
			}

		}

		if (!haveEventStat)
		{
			eventData.eventStat = null;
		}
		if (exceptionEetail.length() == 0 && successDetail.length() == 0)
		{
			eventData.eventDetial = null;
		}
		eventDataLists.add(eventData);

		// 不用写了
		return eventDataLists;
	}

	private boolean statToEvent(boolean haveEventStat)
	{

		if (Procedure_Type == 8 && Procedure_Status == StaticConfig.successProdoceStatusValue)
		{
			eNB间切换出成功次数 = 1;
			haveEventStat = true;
		}
		if (Procedure_Type == 8)
		{
			eNB间切换出请求次数 = 1;
			haveEventStat = true;
		}
		if (Procedure_Type == 7 && Procedure_Status == StaticConfig.successProdoceStatusValue)
		{
			eNB内切换出成功次数 = 1;
			haveEventStat = true;
		}
		if (Procedure_Type == 7)
		{
			eNB内切换出请求次数 = 1;
			haveEventStat = true;
		}
		if (Procedure_Type == 1 && Procedure_Status == StaticConfig.successProdoceStatusValue)
		{
			RRC连接建立成功次数 = 1;
			RRC连接建立时长 = ietime * 1000L + ietimems - istime * 1000L - istimems;
			haveEventStat = true;
		}
		if (Procedure_Type == 1)
		{
			RRC连接建立请求次数 = 1;
			haveEventStat = true;
		}
		if (Procedure_Type == 4 && Procedure_Status == StaticConfig.successProdoceStatusValue)
		{
			RRC连接重建成功次数 = 1;
			haveEventStat = true;
		}
		return haveEventStat;
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

	private void initUUProduceType()
	{
		if (produceTypeMaps.size() == 0)
		{
			produceTypeMaps.put(200, 1);
			produceTypeMaps.put(201, 4);
			produceTypeMaps.put(202, 3);
			produceTypeMaps.put(203, 2);
			produceTypeMaps.put(204, 5);
			produceTypeMaps.put(205, 6);
			produceTypeMaps.put(206, 7);
			produceTypeMaps.put(207, 8);
			produceTypeMaps.put(208, 9);
			produceTypeMaps.put(209, 10);
			produceTypeMaps.put(210, 11);
			produceTypeMaps.put(211, 12);
			produceTypeMaps.put(212, 13);
		}
	}

}
