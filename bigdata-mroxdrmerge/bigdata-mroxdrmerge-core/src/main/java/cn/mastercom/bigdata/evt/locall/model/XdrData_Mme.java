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
import cn.mastercom.bigdata.evt.locall.stat.EventDataStruct;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.DataAdapterConf.ParseItem;
import cn.mastercom.bigdata.util.DataAdapterReader;
import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;

public class XdrData_Mme extends XdrDataBase {
	private Date tmDate = new Date();
	private static ParseItem parseItem;
	public StringBuffer value;

	public int Procedure_Type;
	public int Procedure_Status;
	public int KeyWord1;
	public int Request_Cause;
	public int tac;
	public int Failure_Cause;
	public long Cell_ID;
	public int EPS_Bearer_Number;
	public int bearer_status_index; // bearer_status_index 等于多少需要写死
	public int bear_status1; // bear_status为1的个数
	public int bear_status12; // bear_status 为1 或者2的个数
	public int bear_statusover1; // bear_status大于1的个数
	public String MSISDN = "";

	public int qci0=0;
	public int qci1=0;
	public int qci2=0;
	public int qci3=0;
	public int qci4=0;
	public int qci5=0;
	
	// 统计事件
	private long CombinedEPS附着成功次数;
	private long CombinedEPS附着请求次数;
	private long eNB请求释放上下文数;
	private long EPS附着成功次数;
	private long EPS附着请求次数;
	private long ERAB建立成功数;
	private long ERAB建立请求数;
	private long MME内S1接口每相邻关系切出尝试次数;
	private long MME内S1接口切出成功次数;
	private long MME内S1接口切入尝试次数;
	private long MME内S1接口切入成功次数;
	private long MME内X2接口切换尝试数;
	private long MME内X2接口切换成功次数;
	private long 初始上下文建立成功次数;
	private long 跟踪区更新成功次数;
	private long 跟踪区更新请求次数;
	private long 联合跟踪区更新成功次数;
	private long 联合跟踪区更新请求次数;
	private long 寻呼记录接收个数;
	private long 遗留上下文个数;
	private long 正常的eNB请求释放上下文数;
	private long 专用承载激活成功次数;
	private long 专用承载激活请求次数;

	// 异常 事件
	private long CombinedEPS附着失败;
	private long EPS附着失败;
	private long ERAB建立失败;
	private long MME内S1接口切出失败;
	private long MME内S1接口切入失败;
	private long MME内X2接口切换失败;
	private long 初始上下文建立失败;
	private long eNB请求释放上下文失败;
	private long 跟踪区更新失败;
	private long 联合跟踪区更新失败;
	private long 非正常的eNB请求释放上下文;
	private long 专用承载激活失败;

	// 成功事件
	private long CombinedEPS附着成功;
	private long EPS附着成功;
	private long ERAB建立成功;
	private long MME内S1接口切出成功;
	private long MME内S1接口切入成功;
	private long MME内X2接口切换成功;
	private long 初始上下文建立成功;
	private long eNB请求释放成功上下文数;
	private long 跟踪区更新成功;
	private long 联合跟踪区更新成功;
	private long 正常的eNB请求释放上下文;
	private long 专用承载激活成功;

	public XdrData_Mme() {
		super();

		clear();

		if (parseItem == null) {
			parseItem = MainModel.GetInstance().getEventAdapterConfig().getParseItem("LOCALL-mme");
		}
	}

	@Override
	public int getInterfaceCode() {
		return StaticConfig.INTERFACE_MME;
	}

	public void clear() {
		value = new StringBuffer();
	}

	@Override
	public ParseItem getDataParseItem() throws IOException {
		return parseItem;
	}

	@Override
	public boolean FillData_short(DataAdapterReader dataAdapterReader) throws ParseException, IOException {
		try {
			tmDate = dataAdapterReader.GetDateValue("Procedure_Start_Time", new Date(1970, 1, 1));
			istime = (int) (tmDate.getTime() / 1000L);
			istimems = (int) (tmDate.getTime() % 1000L);
			// etime
			tmDate = dataAdapterReader.GetDateValue("Procedure_End_Time", new Date(1970, 1, 1));
			ietime = (int) (tmDate.getTime() / 1000L);
			ietimems = (int) (tmDate.getTime() % 1000L);

			imsi = dataAdapterReader.GetLongValue("IMSI", 0);

		} catch (Exception e) {
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"XdrData_Mme.fillData_short error:",
					"XdrData_Mme.fillData_short error: " + e.getMessage(),e);
			return false;
		}

		return true;
	}

	@Override
	public boolean FillData(DataAdapterReader dataAdapterReader) throws ParseException, IOException {
		try {
			
			if(MainModel.GetInstance().getCompile().Assert(CompileMark.GanSu)
				|| MainModel.GetInstance().getCompile().Assert(CompileMark.ShanXiJin)){
				qci0 = dataAdapterReader.GetIntValue("QCI0", 0);
				qci1 = dataAdapterReader.GetIntValue("QCI1", 0);
				qci2 = dataAdapterReader.GetIntValue("QCI2", 0);
				qci3 = dataAdapterReader.GetIntValue("QCI3", 0);
				qci4 = dataAdapterReader.GetIntValue("QCI4", 0);
				qci5 = dataAdapterReader.GetIntValue("QCI5", 0);	
			}
			
			
			tmDate = dataAdapterReader.GetDateValue("Procedure_Start_Time", new Date(1970, 1, 1));
			istime = (int) (tmDate.getTime() / 1000L);
			istimems = (int) (tmDate.getTime() % 1000L);
			// etime
			tmDate = dataAdapterReader.GetDateValue("Procedure_End_Time", new Date(1970, 1, 1));
			ietime = (int) (tmDate.getTime() / 1000L);
			ietimems = (int) (tmDate.getTime() % 1000L);

			imsi = dataAdapterReader.GetLongValue("IMSI", 0);

			value = dataAdapterReader.getTmStrs();

			Procedure_Type = dataAdapterReader.GetIntValue("Procedure_Type", StaticConfig.Int_Abnormal);
			Procedure_Status = dataAdapterReader.GetIntValue("Procedure_Status", StaticConfig.Int_Abnormal);

			KeyWord1 = dataAdapterReader.GetIntValue("KEYWORD1", StaticConfig.Int_Abnormal);// no
			Request_Cause = dataAdapterReader.GetIntValue("REQUEST_CAUSE", 0);// no
			if(MainModel.GetInstance().getCompile().Assert(CompileMark.BeiJing)){
				Request_Cause = Request_Cause%256;
			}
			
			// 用不上 tac= dataAdapterReader.GetIntValue("TAC",
			// StaticConfig.Int_Abnormal);
			Failure_Cause = dataAdapterReader.GetIntValue("FAILURE_CAUSE", StaticConfig.Int_Abnormal);// no
			Cell_ID = dataAdapterReader.GetLongValue("Cell_ID", StaticConfig.Int_Abnormal);
			ecgi = Cell_ID;
			MSISDN = dataAdapterReader.GetStrValue("MSISDN", "");
			imei = dataAdapterReader.GetStrValue("IMEI", "");
			/**
			 * TODO 2018-6-07 需要做成可配置的
			 */
			List<Integer> BEARERIDList = new ArrayList<>();
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.GuangXi2)) {
//				BEARERIDList = Arrays.asList(38, 46, 54, 62, 70, 78);
				BEARERIDList = Arrays.asList(41, 49, 57, 65, 73, 81);
			}
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.NingXia)) {
//				BEARERIDList = Arrays.asList(49, 59, 69, 79, 89, 99);
				BEARERIDList = Arrays.asList(52, 62, 72, 82, 92, 102);

			}
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.QingHai)) {
				BEARERIDList = Arrays.asList(18, 19, 20, 21, 22, 23);

			}
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.YunNan)) {
				BEARERIDList = Arrays.asList(27, 28, 29, 30, 31, 32);
			}
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.BeiJing)) {
				BEARERIDList = Arrays.asList(18, 19, 20, 21, 22, 23);
			}
			if(MainModel.GetInstance().getCompile().Assert(CompileMark.ShanXi)){
				BEARERIDList = Arrays.asList(19, 21, 23, 25, 27, 29);
			}
			if(MainModel.GetInstance().getCompile().Assert(CompileMark.GanSu)){
				BEARERIDList = Arrays.asList(42,48,54,60,66,72);
			}
			if(MainModel.GetInstance().getCompile().Assert(CompileMark.TianJin)){
				BEARERIDList = Arrays.asList(53,63,73,83,93,103);
			}

			for (int i = 0; i < BEARERIDList.size(); i++) {
				try {
					if (BEARERIDList.get(i) < dataAdapterReader.tmStrs.length) {

						String bearerStr = dataAdapterReader.tmStrs[BEARERIDList.get(i)];
						if (bearerStr.length() == 0) {
							continue;
						}
						int Bearer_num = Integer.parseInt(bearerStr);
						if (Bearer_num == 1) {
							bear_status1++;
						}
						if (Bearer_num == 1 || Bearer_num == 2) {
							bear_status12++;
						}
						if (Bearer_num > 1) {
							bear_statusover1++;
						}
					}
				} catch (Exception e) {
					LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"XdrData_Mme.fillData1 error:",
							"XdrData_Mme.fillData1 error: " + e.getMessage(),e);
				}
			}

		} catch (Exception e) {
			LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"XdrData_Mme.fillData error:",
					"XdrData_Mme.fillData error: " + e.getMessage(),e);
			return false;
		}
		return true;
	}

	@Override
	public ArrayList<EventData> toEventData() {

		if(MainModel.GetInstance().getCompile().Assert(CompileMark.HaiNan)){
			return new ArrayList<>();
		}

		if (MainModel.GetInstance().getCompile().Assert(CompileMark.TianJin)){
			// 天津很奇怪，mme默认的successProdoceStatusValue是1，但是mw中却是0
			StaticConfig.successProdoceStatusValue = 1;
		}
		// || MainModel.GetInstance().getCompile().Assert(CompileMark.ShanXiJin)
		ArrayList<EventData> eventDataListAll = new ArrayList<EventData>();
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.GanSu)
				|| MainModel.GetInstance().getCompile().Assert(CompileMark.ShanXiJin)
                || MainModel.GetInstance().getCompile().Assert(CompileMark.TianJin)) {
			EventData eventDataKpiset2 = new EventData();
			int qci = 0;
			if((qci0==1||qci0==2)||(qci1==1||qci1==2)||(qci2==1||qci2==2)||(qci3==1||qci3==2)||(qci4==1||qci4==2)
					||(qci5==1||qci5==2)){
				qci = 1;
			}
			int VOLTE接通失败次数S1 = 0;
			int VOLTE呼叫请求次数S1 = 0;
			int VOLTE掉话次数S1 = 0;
			int VOLTE呼叫释放请求次数S1 = 0;
			String detailStr=null; //详单事件
			
			if (Procedure_Type == 18 && (Procedure_Status==1 || Procedure_Status == 255) && (qci == 1 || qci == 2)) {
				VOLTE接通失败次数S1 = 1;
				detailStr = "VOLTE接通失败S1";
			}
			if (Procedure_Type == 18 && (qci == 1 || qci == 2)) {
				VOLTE呼叫请求次数S1 = 1;
			}
			if ((qci == 1 || qci == 2) && (Procedure_Type == 21 || Procedure_Type == 20) && (Procedure_Status==1 || Procedure_Status == 255)) {
				VOLTE掉话次数S1 = 1;
				detailStr = "VOLTE掉话S1";
			}
			if ((qci == 1 || qci == 2) && (Procedure_Type == 21 || Procedure_Type == 20)) {
				VOLTE呼叫释放请求次数S1 = 1;
			}

			eventDataKpiset2.iCityID = iCityID;
			eventDataKpiset2.IMSI = imsi;
			eventDataKpiset2.iEci = Cell_ID;
			eventDataKpiset2.iTime = istime;
			eventDataKpiset2.wTimems = istimems; // 开始时间里面的毫秒
			eventDataKpiset2.strLoctp = strloctp;
			eventDataKpiset2.strLabel = label;
			eventDataKpiset2.iLongitude = iLongitude;
			eventDataKpiset2.iLatitude = iLatitude;
			eventDataKpiset2.iBuildID = ibuildid;
			eventDataKpiset2.iHeight = iheight;
			eventDataKpiset2.Interface = StaticConfig.INTERFACE_MME;
			eventDataKpiset2.position = position;
			eventDataKpiset2.iKpiSet = 2; // !!!!!TODO  ,20180419这里改成了2
			eventDataKpiset2.iProcedureType = Procedure_Type;
			eventDataKpiset2.iTestType = testType;
			eventDataKpiset2.iDoorType = iDoorType;
			eventDataKpiset2.iLocSource = locSource;
			eventDataKpiset2.confidentType = confidentType;
			eventDataKpiset2.iAreaType = iAreaType;
			eventDataKpiset2.iAreaID = iAreaID;
			eventDataKpiset2.lteScRSRP = LteScRSRP;
			eventDataKpiset2.lteScSinrUL = LteScSinrUL;
            eventDataKpiset2.imei = imei;

			eventDataKpiset2.lTrainKey = trainKey;
			eventDataKpiset2.iSectionId = sectionId;
			eventDataKpiset2.iSegmentId = segmentId;

//			eventDataKpiset2.eventStat.fvalue[3] = VOLTE接通失败次数S1;
//			eventDataKpiset2.eventStat.fvalue[4] = VOLTE呼叫请求次数S1;
//			eventDataKpiset2.eventStat.fvalue[5] = VOLTE掉话次数S1;
//			eventDataKpiset2.eventStat.fvalue[6] = VOLTE呼叫释放请求次数S1;
			
			
			eventDataKpiset2.eventStat.fvalue[4] = VOLTE接通失败次数S1;
			eventDataKpiset2.eventStat.fvalue[5] = VOLTE呼叫释放请求次数S1;
			eventDataKpiset2.eventStat.fvalue[6] = VOLTE掉话次数S1;
			eventDataKpiset2.eventStat.fvalue[7] = VOLTE呼叫请求次数S1;
			
			if(detailStr!=null){
				eventDataKpiset2.eventDetial.sampleType = StaticConfig.FAILURE_SAMPLE;
				eventDataKpiset2.eventDetial.strvalue[0]=detailStr;
				eventDataKpiset2.eventDetial.fvalue[0] = LteScRSRP;
				eventDataKpiset2.eventDetial.fvalue[1] = LteScSinrUL;

			}else{
				eventDataKpiset2.eventDetial = null;
			}
			ArrayList<EventData> eventDataLists = new ArrayList<EventData>();
			if(eventDataKpiset2.haveEventStat()){
				eventDataLists.add(eventDataKpiset2);
			}
			eventDataListAll.addAll(eventDataLists);
		}

		ArrayList<EventData> eventDataLists = new ArrayList<EventData>();
		EventData eventDataKpiset1 = new EventData();
		// 统计事件：
		if (Procedure_Type == 1 && KeyWord1 == 2 && Procedure_Status == StaticConfig.successProdoceStatusValue) {
			CombinedEPS附着成功次数 = 1;
		}
		if (Procedure_Type == 1 && KeyWord1 == 2) {
			CombinedEPS附着请求次数 = 1;
		}
		// 2017-10-27 把 Procedure_Type == 20 改成 Procedure_Type == 19
		if (Procedure_Type == 20 && Procedure_Status == StaticConfig.successProdoceStatusValue) {
			eNB请求释放上下文数 = 1;
		}
		if (Procedure_Type == 1 && Procedure_Status == StaticConfig.successProdoceStatusValue) {
			EPS附着成功次数 = 1;
		}
		if (Procedure_Type == 1) {
			EPS附着请求次数 = 1;
		}

		if ((Procedure_Type == 7 || Procedure_Type == 9 || Procedure_Type == 10 || Procedure_Type == 13)
				&& bear_status1 > 0) {
			ERAB建立成功数 = bear_status1;
		}
		if ((Procedure_Type == 7 || Procedure_Type == 9 || Procedure_Type == 10 || Procedure_Type == 13)
				&& bear_status12 > 0) {
			ERAB建立请求数 = bear_status12;
		}
		if (Procedure_Type == 15) {
			MME内S1接口每相邻关系切出尝试次数 = 1;
		}
		if (Procedure_Type == 15 && Procedure_Status == StaticConfig.successProdoceStatusValue) {
			MME内S1接口切出成功次数 = 1;
		}
		if (Procedure_Type == 16) {
			MME内S1接口切入尝试次数 = 1;
		}
		if (Procedure_Type == 16 && Procedure_Status == StaticConfig.successProdoceStatusValue) {
			MME内S1接口切入成功次数 = 1;
		}
		if (Procedure_Type == 14) {
			MME内X2接口切换尝试数 = 1;
		}
		if (Procedure_Type == 14 && Procedure_Status == StaticConfig.successProdoceStatusValue) {
			MME内X2接口切换成功次数 = 1;
		}
		if ((Procedure_Type == 7 || Procedure_Type == 18)
				&& Procedure_Status == StaticConfig.successProdoceStatusValue) {
			初始上下文建立成功次数 = 1;
		}
		if (Procedure_Type == 5 && Procedure_Status == StaticConfig.successProdoceStatusValue) {
			跟踪区更新成功次数 = 1;
		}
		if (Procedure_Type == 5) {
			跟踪区更新请求次数 = 1;
		}
		if (Procedure_Type == 5 && (KeyWord1 == 1 || KeyWord1 == 2)
				&& Procedure_Status == StaticConfig.successProdoceStatusValue) {
			联合跟踪区更新成功次数 = 1;
		}
		if (Procedure_Type == 5 && (KeyWord1 == 1 || KeyWord1 == 2)) {
			联合跟踪区更新请求次数 = 1;
		}
		if (Procedure_Type == 4) {
			寻呼记录接收个数 = 1;
		}

		/**
		 * TODO zhaikaishun 2017-09-26 [2017-09-26] 遗留上下文个数
		 */

		// kpiset为2
		if (Procedure_Type == 20 && (Request_Cause == 20 || Request_Cause == 23 || Request_Cause == 28
				|| Request_Cause == 24 || Request_Cause == 36)) {

			正常的eNB请求释放上下文数 = 1;
		}
		if (Procedure_Type == 13 && Procedure_Status == StaticConfig.successProdoceStatusValue) {
			专用承载激活成功次数 = 1;
		}
		if (Procedure_Type == 13) {
			专用承载激活请求次数 = 1;

		}
		eventDataKpiset1.iCityID = iCityID;
		eventDataKpiset1.IMSI = imsi;
		eventDataKpiset1.iEci = Cell_ID;
		eventDataKpiset1.iTime = istime;
		eventDataKpiset1.wTimems = istimems; // 开始时间里面的毫秒
		eventDataKpiset1.strLoctp = strloctp;
		eventDataKpiset1.strLabel = label;
		eventDataKpiset1.iLongitude = iLongitude;
		eventDataKpiset1.iLatitude = iLatitude;
		eventDataKpiset1.iBuildID = ibuildid;
		eventDataKpiset1.iHeight = iheight;
		eventDataKpiset1.Interface = StaticConfig.INTERFACE_MME;
		eventDataKpiset1.position = position;
		eventDataKpiset1.iKpiSet = 1;
		eventDataKpiset1.iProcedureType = Procedure_Type;
		eventDataKpiset1.iTestType = testType;
		eventDataKpiset1.iDoorType = iDoorType;
		eventDataKpiset1.iLocSource = locSource;
		eventDataKpiset1.confidentType = confidentType;
		eventDataKpiset1.iAreaType = iAreaType;
		eventDataKpiset1.iAreaID = iAreaID;
		eventDataKpiset1.lteScRSRP = LteScRSRP;
		eventDataKpiset1.lteScSinrUL = LteScSinrUL;
		eventDataKpiset1.imei = imei;

		eventDataKpiset1.eventStat.fvalue[0] = CombinedEPS附着成功次数;
		eventDataKpiset1.eventStat.fvalue[1] = CombinedEPS附着请求次数;
		eventDataKpiset1.eventStat.fvalue[2] = EPS附着成功次数;
		eventDataKpiset1.eventStat.fvalue[3] = EPS附着请求次数;
		eventDataKpiset1.eventStat.fvalue[4] = ERAB建立成功数;
		eventDataKpiset1.eventStat.fvalue[5] = ERAB建立请求数;
		eventDataKpiset1.eventStat.fvalue[6] = MME内S1接口每相邻关系切出尝试次数;
		eventDataKpiset1.eventStat.fvalue[7] = MME内S1接口切出成功次数;
		eventDataKpiset1.eventStat.fvalue[8] = MME内S1接口切入尝试次数;
		eventDataKpiset1.eventStat.fvalue[9] = MME内S1接口切入成功次数;
		eventDataKpiset1.eventStat.fvalue[10] = MME内X2接口切换尝试数;
		eventDataKpiset1.eventStat.fvalue[11] = MME内X2接口切换成功次数;
		eventDataKpiset1.eventStat.fvalue[12] = 初始上下文建立成功次数;
		eventDataKpiset1.eventStat.fvalue[13] = eNB请求释放上下文数;
		eventDataKpiset1.eventStat.fvalue[14] = 跟踪区更新成功次数;
		eventDataKpiset1.eventStat.fvalue[15] = 跟踪区更新请求次数;
		eventDataKpiset1.eventStat.fvalue[16] = 联合跟踪区更新成功次数;
		eventDataKpiset1.eventStat.fvalue[17] = 联合跟踪区更新请求次数;
		eventDataKpiset1.eventStat.fvalue[18] = 遗留上下文个数;

		eventDataKpiset1.eventStat.fvalue[19] = 正常的eNB请求释放上下文数;

		eventDataKpiset1.lTrainKey = trainKey;
		eventDataKpiset1.iSectionId = sectionId;
		eventDataKpiset1.iSegmentId = segmentId;

		EventData eventDataKpiset2 = new EventData();

		eventDataKpiset2.iCityID = iCityID;
		eventDataKpiset2.IMSI = imsi;
		eventDataKpiset2.iEci = Cell_ID;
		eventDataKpiset2.iTime = istime;
		eventDataKpiset2.wTimems = istimems; // 开始时间里面的毫秒
		eventDataKpiset2.strLoctp = strloctp;
		eventDataKpiset2.strLabel = label;
		eventDataKpiset2.iLongitude = iLongitude;
		eventDataKpiset2.iLatitude = iLatitude;
		eventDataKpiset2.iBuildID = ibuildid;
		eventDataKpiset2.iHeight = iheight;
		eventDataKpiset2.position = position;
		eventDataKpiset2.Interface = StaticConfig.INTERFACE_MME;
		eventDataKpiset2.iKpiSet = 2;
		eventDataKpiset2.iProcedureType = Procedure_Type;
		eventDataKpiset2.iTestType = testType;
		eventDataKpiset2.iDoorType = iDoorType;
		eventDataKpiset2.iLocSource = locSource;
		eventDataKpiset2.confidentType = confidentType;
		eventDataKpiset2.iAreaType = iAreaType;
		eventDataKpiset2.iAreaID = iAreaID;
		eventDataKpiset2.imei = imei;
		// TODO 北京的不跑这些数据
		if(!MainModel.GetInstance().getCompile().Assert(CompileMark.BeiJing)){
			eventDataKpiset2.eventStat.fvalue[0] = 寻呼记录接收个数;
			eventDataKpiset2.eventStat.fvalue[1] = 专用承载激活成功次数;
			eventDataKpiset2.eventStat.fvalue[2] = 专用承载激活请求次数;
		}
		eventDataKpiset2.lTrainKey = trainKey;
		eventDataKpiset2.iSectionId = sectionId;
		eventDataKpiset2.iSegmentId = segmentId;
		// 统计事件，统计mme的个数
		if(trainKey!=0){
			eventDataKpiset2.eventStat.fvalue[3] = 1;
		}


		// 异常事件
		String exceptionDetail = "";

		// 北京不吐出,20180305， 北京也要吐出了
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.EventDataOutFailure)) {
			if (Procedure_Type == 1 && KeyWord1 == 2
					&& Procedure_Status != StaticConfig.successProdoceStatusValue) {
				CombinedEPS附着失败 = 1;
				exceptionDetail = "CombinedEPS附着失败";
			}

			if (Procedure_Type == 1 && KeyWord1 != 2
					&& Procedure_Status != StaticConfig.successProdoceStatusValue) {
				EPS附着失败 = 1;
				exceptionDetail = "EPS附着失败";
			}
			if ((Procedure_Type == 7 || Procedure_Type == 9 || Procedure_Type == 10 || Procedure_Type == 13)
					&& bear_statusover1 > 0) {
				ERAB建立失败 = bear_statusover1;
				exceptionDetail = "ERAB建立失败";
			}
			if (Procedure_Type == 15 && Procedure_Status != StaticConfig.successProdoceStatusValue) {
				MME内S1接口切出失败 = 1;
				exceptionDetail = "MME内S1接口切出失败";
			}
			if (Procedure_Type == 16 && Procedure_Status != StaticConfig.successProdoceStatusValue) {
				MME内S1接口切入失败 = 1;
				exceptionDetail = "MME内S1接口切入失败";
			}
			if (Procedure_Type == 14 && Procedure_Status != StaticConfig.successProdoceStatusValue) {
				MME内X2接口切换失败 = 1;
				exceptionDetail = "MME内X2接口切换失败";
			}
			if ((Procedure_Type == 7 || Procedure_Type == 18)
					&& Procedure_Status != StaticConfig.successProdoceStatusValue) {
				初始上下文建立失败 = 1;
				exceptionDetail = "初始上下文建立失败";
			}
			if (Procedure_Type == 20 && Procedure_Status != StaticConfig.successProdoceStatusValue) {
				eNB请求释放上下文失败 = 1;
				exceptionDetail = "eNB请求释放上下文失败";
			}
			if (Procedure_Type == 5 && (KeyWord1 != 1 && KeyWord1 != 2)
					&& Procedure_Status != StaticConfig.successProdoceStatusValue) {
				跟踪区更新失败 = 1;
				exceptionDetail = "跟踪区更新失败";
			}
			if (Procedure_Type == 5 && (KeyWord1 == 1 || KeyWord1 == 2)
					&& Procedure_Status != StaticConfig.successProdoceStatusValue) {
				联合跟踪区更新失败 = 1;
				exceptionDetail = "联合跟踪区更新失败";
			}
			if(!MainModel.GetInstance().getCompile().Assert(CompileMark.BeiJing)
					&& !MainModel.GetInstance().getCompile().Assert(CompileMark.ShanXi)){
				if (Procedure_Type == 20 && (Request_Cause != 20 && Request_Cause != 23 && Request_Cause != 27
						&& Request_Cause != 24 && Request_Cause != 36)) {
					非正常的eNB请求释放上下文 = 1;
					exceptionDetail = "非正常的eNB请求释放上下文";
				}
			}

			if (Procedure_Type == 13 && Procedure_Status != StaticConfig.successProdoceStatusValue) {
				专用承载激活失败 = 1;
				exceptionDetail = "专用承载激活失败";
			}
		}

		// 成功事件
		String exceptionDetaiSuccess = "";

		// 判断是否需要吐出,广西不吐出
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.EventDataOutSuccess)
				&& !MainModel.GetInstance().getCompile().Assert(CompileMark.BeiJing)) {
			if (Procedure_Type == 1 && KeyWord1 == 2
					&& Procedure_Status == StaticConfig.successProdoceStatusValue) {
				CombinedEPS附着成功 = 1;
				exceptionDetaiSuccess = "CombinedEPS附着成功";
			}

			if (Procedure_Type == 1 && KeyWord1 != 2
					&& Procedure_Status == StaticConfig.successProdoceStatusValue) {
				EPS附着成功 = 1;
				exceptionDetaiSuccess = "EPS附着成功";
			}
			if ((Procedure_Type == 7 || Procedure_Type == 9 || Procedure_Type == 10 || Procedure_Type == 13)
					&& bear_statusover1 <= 0) // 是不是这样 TODO 2017-10-09
			{
				ERAB建立成功数 = bear_statusover1;
				exceptionDetaiSuccess = "ERAB建立成功数";
			}
			if (Procedure_Type == 15 && Procedure_Status == StaticConfig.successProdoceStatusValue) {
				MME内S1接口切出成功 = 1;
				exceptionDetaiSuccess = "MME内S1接口切出成功";
			}
			if (Procedure_Type == 16 && Procedure_Status == StaticConfig.successProdoceStatusValue) {
				MME内S1接口切入成功 = 1;
				exceptionDetaiSuccess = "MME内S1接口切入成功";
			}
			if(!MainModel.GetInstance().getCompile().Assert(CompileMark.GuangXi2)){
				if (Procedure_Type == 14 && Procedure_Status == StaticConfig.successProdoceStatusValue) {
					MME内X2接口切换成功 = 1;
					exceptionDetaiSuccess = "MME内X2接口切换成功";
				}
			}

			if(!MainModel.GetInstance().getCompile().Assert(CompileMark.GuangXi2)){

                if ((Procedure_Type == 7 || Procedure_Type == 17)
                        && Procedure_Status == StaticConfig.successProdoceStatusValue) {
                    初始上下文建立成功 = 1;
                    exceptionDetaiSuccess = "初始上下文建立成功";
                }

                if (Procedure_Type == 20 && Procedure_Status == StaticConfig.successProdoceStatusValue) {
                    eNB请求释放成功上下文数 = 1;
                    exceptionDetaiSuccess = "eNB请求释放成功上下文数";
                }
				if (Procedure_Type == 5 && (KeyWord1 != 1 && KeyWord1 != 2)
						&& Procedure_Status == StaticConfig.successProdoceStatusValue) {
					跟踪区更新成功 = 1;
					exceptionDetaiSuccess = "跟踪区更新成功";
				}
				if (Procedure_Type == 5 && (KeyWord1 == 1 || KeyWord1 == 2)
						&& Procedure_Status == StaticConfig.successProdoceStatusValue) {
					联合跟踪区更新成功 = 1;
					exceptionDetaiSuccess = "联合跟踪区更新成功";
				}
				if (Procedure_Type == 20 && (Request_Cause == 20 || Request_Cause == 23 || Request_Cause == 27
						|| Request_Cause == 24 || Request_Cause == 36)) {
					正常的eNB请求释放上下文 = 1;
					exceptionDetaiSuccess = "正常的eNB请求释放上下文";
				}

                if (Procedure_Type == 13 && Procedure_Status == StaticConfig.successProdoceStatusValue) {
                    专用承载激活成功 = 1;
                    exceptionDetaiSuccess = "专用承载激活成功";
                }
			}

		}

		if (!eventDataKpiset1.haveEventStat()) {
			eventDataKpiset1.eventStat = null;
		}
		//北京的暂时不需要这些指标
		if(!MainModel.GetInstance().getCompile().Assert(CompileMark.BeiJing)){
			eventDataLists.add(eventDataKpiset1);
		}


		if (exceptionDetail.length() > 0) {
			// 因为这些指标不需要放到sqlserver中去，所以改成了SUCCESS_SAMPLE
//				eventDataKpiset1.eventDetial.sampleType = StaticConfig.FAILURE_SAMPLE;
			eventDataKpiset1.eventDetial.sampleType = StaticConfig.SUCCESS_SAMPLE;
			eventDataKpiset1.eventDetial.strvalue[0] = exceptionDetail;
			eventDataKpiset1.eventDetial.fvalue[0] = LteScRSRP;
			eventDataKpiset1.eventDetial.fvalue[1] = LteScSinrUL;
			eventDataKpiset1.eventDetial.fvalue[2] = CombinedEPS附着失败;
			eventDataKpiset1.eventDetial.fvalue[3] = EPS附着失败;
			eventDataKpiset1.eventDetial.fvalue[4] = ERAB建立失败;
			eventDataKpiset1.eventDetial.fvalue[5] = MME内S1接口切出失败;
			eventDataKpiset1.eventDetial.fvalue[6] = MME内S1接口切入失败;
			eventDataKpiset1.eventDetial.fvalue[7] = MME内X2接口切换失败;
			eventDataKpiset1.eventDetial.fvalue[8] = 初始上下文建立失败;
			eventDataKpiset1.eventDetial.fvalue[9] = eNB请求释放上下文失败;
			eventDataKpiset1.eventDetial.fvalue[10] = 跟踪区更新失败;
			eventDataKpiset1.eventDetial.fvalue[11] = 联合跟踪区更新失败;
			eventDataKpiset1.eventDetial.fvalue[12] = 非正常的eNB请求释放上下文;
			eventDataKpiset1.eventDetial.fvalue[13] = 专用承载激活失败;

		} else if (exceptionDetaiSuccess.length() > 0) {
			if(MainModel.GetInstance().getCompile().Assert(CompileMark.ShanXi)){
				eventDataKpiset1.eventDetial = null;
			}else{
				//北京不要这个正常的事件
				eventDataKpiset1.eventDetial.sampleType = StaticConfig.SUCCESS_SAMPLE;
				eventDataKpiset1.eventDetial.strvalue[0] = exceptionDetaiSuccess;
				eventDataKpiset1.eventDetial.fvalue[0] = LteScRSRP;
				eventDataKpiset1.eventDetial.fvalue[1] = LteScSinrUL;
				eventDataKpiset1.eventDetial.fvalue[2] = CombinedEPS附着成功;
				eventDataKpiset1.eventDetial.fvalue[3] = EPS附着成功;
				eventDataKpiset1.eventDetial.fvalue[4] = ERAB建立成功数;
				eventDataKpiset1.eventDetial.fvalue[5] = MME内S1接口切出成功;
				eventDataKpiset1.eventDetial.fvalue[6] = MME内S1接口切入成功;
				eventDataKpiset1.eventDetial.fvalue[7] = MME内X2接口切换成功;
				eventDataKpiset1.eventDetial.fvalue[8] = 初始上下文建立成功;
				eventDataKpiset1.eventDetial.fvalue[9] = eNB请求释放成功上下文数;
				eventDataKpiset1.eventDetial.fvalue[10] = 跟踪区更新成功;
				eventDataKpiset1.eventDetial.fvalue[11] = 联合跟踪区更新成功;
				eventDataKpiset1.eventDetial.fvalue[12] = 正常的eNB请求释放上下文;
				eventDataKpiset1.eventDetial.fvalue[13] = 专用承载激活成功;

			}


		} else {
			eventDataKpiset1.eventDetial = null;
		}

		eventDataKpiset2.eventDetial = null; // 这个detial不用输出的，所有的detial都输出到eventData里面去了
		if(eventDataKpiset2.haveEventStat()){
			eventDataLists.add(eventDataKpiset2);
		}
		eventDataKpiset1.iAreaType = iAreaType;
		eventDataKpiset1.iAreaID = iAreaID;
		eventDataListAll.addAll(eventDataLists);

		/*TODO 天津暂时不给他跑MME，但是要跑重定向，这块不知道如何屏蔽，于是暂时这样来修改
		* 20180815 zhaikaishun
		* */
		if(MainModel.GetInstance().getCompile().Assert(CompileMark.TianJin)){
		    return new ArrayList<EventData>();
        }else{
            return eventDataListAll;
        }
	}

	public EventData toRedirectEventData() {
		EventData eventData = new EventData();
		eventData.iCityID = iCityID;
		eventData.IMSI = imsi;
		eventData.iEci = Cell_ID;
		eventData.iTime = istime;
		eventData.wTimems = istimems; // 开始时间里面的毫秒
		eventData.strLoctp = strloctp;
		eventData.strLabel = label;
		eventData.iLongitude = iLongitude;
		eventData.iLatitude = iLatitude;
		eventData.iBuildID = ibuildid;
		eventData.iHeight = iheight;
		eventData.Interface = StaticConfig.INTERFACE_MME;
		eventData.position = position;
		eventData.iKpiSet = 1;
		eventData.iProcedureType = Procedure_Type;
		eventData.iTestType = testType;
		eventData.iDoorType = iDoorType;
		eventData.iLocSource = locSource;
		eventData.confidentType = confidentType;
		eventData.iAreaType = iAreaType;
		eventData.iAreaID = iAreaID;
		eventData.lTrainKey = trainKey;
		eventData.iSectionId = sectionId;
		eventData.iSegmentId = segmentId;
        eventData.lteScRSRP = LteScRSRP;
        eventData.lteScSinrUL = LteScSinrUL;
        eventData.imei = imei;
		eventData.eventDetial.sampleType = StaticConfig.FAILURE_SAMPLE;
		eventData.eventDetial.strvalue[0] = "重定向质差";
		//eventData.eventDetial.strvalue[1] = MSISDN; // 手机号不加了
		eventData.eventDetial.fvalue[0] = LteScRSRP;
		eventData.eventDetial.fvalue[1] = LteScSinrUL;
		eventData.eventDetial.fvalue[2] = Cell_ID;
		eventData.eventDetial.fvalue[3] = istime * 1000L + istimems; // 使用的是ms
		eventData.eventStat = new EventDataStruct();
		eventData.eventStat.fvalue[9] = 1; // 重定向次数
		return eventData;
	}

	@Override
	public void toString(StringBuffer sb) {

		StaticConfig.putCityNameByCityId();
		String fenge = parseItem.getSplitMark();
		if (fenge.contains("\\")) {
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

		if (StaticConfig.cityId_Name.containsKey(iCityID)) {
			sb.append(StaticConfig.cityId_Name.get(iCityID) + "_" + icentLng + "_" + icentLat);
			sb.append(fenge);
		} else {
			sb.append("nocity" + "_" + icentLng + "_" + icentLat);
			sb.append(fenge);
		}

		sb.append(-1);
		sb.append(fenge);
		sb.append(-1);

	}

}
