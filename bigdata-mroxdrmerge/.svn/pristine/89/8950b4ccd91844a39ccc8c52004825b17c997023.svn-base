package cn.mastercom.bigdata.evt.locall.model;

import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.evt.locall.stat.EventData;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.DataAdapterConf.ParseItem;
import cn.mastercom.bigdata.util.DataAdapterReader;
import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;

public class MdtDataHOLRLFRCEF extends XdrDataBase  {
    private static ParseItem parseItem;
    private Date tmDate = new Date();
    private double curLng = 0;
    private double curLat=0;
    private long collect_pci=0;
    private long collect_arc=0;
    private int curRsrp;
    private int curRsrq;
    String report_type="";
    String cf_type="";
    Random random = new Random();
    public MdtDataHOLRLFRCEF(){
        super();
        clear();
        if (parseItem == null) {
            parseItem = MainModel.GetInstance().getEventAdapterConfig().getParseItem("LOCALL-MDT-RLFHOFRCEF");
        }
    }
    public void clear() {

    }

    @Override
    public int getInterfaceCode() {
        return StaticConfig.INTERFACE_MDT_RLFHOF;
    }

    @Override
    public ParseItem getDataParseItem() throws IOException {
        return parseItem;
    }

    @Override
    public boolean FillData_short(DataAdapterReader dataAdapterReader) throws ParseException, IOException {
        try {
            s1apid = dataAdapterReader.GetLongValue("Report_MME_UES1APID", StaticConfig.Int_Abnormal);

            report_type = dataAdapterReader.GetStrValue("report_type","");
            cf_type = dataAdapterReader.GetStrValue("cf_type","");
            if("RLF".equals(cf_type) || "HOF".equals(cf_type)){
                setRLFHOFEcgi(dataAdapterReader);
            }else if("RCEF".equals(report_type)){
                ecgi = dataAdapterReader.GetLongValue("RCEF_ECI", 0);
                if(ecgi==4294967295L){
                    ecgi = random.nextInt(500);
                }
            }else if("RlF".equals(report_type)){
                setRLFHOFEcgi(dataAdapterReader);
            }
            if (MainModel.GetInstance().getCompile().Assert(CompileMark.SiChuan)) {
                String theTime = dataAdapterReader.GetStrValue("STARTTIME", "1970-01-01T08:00:00");
                tmDate = getRealTime(theTime);
                istime = (int) (tmDate.getTime() / 1000L);
                istimems = (int) (tmDate.getTime() % 1000L);
            }else {
                tmDate = dataAdapterReader.GetDateValue("STARTTIME", new Date(1970, 1, 1));
                istime = (int) (tmDate.getTime() / 1000L);
                istimems = (int) (tmDate.getTime() % 1000L);
            }



        } catch (Exception e) {
            LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"MdrData_HOFRLFRCEF.fillData_short error",
                    "MdrData_HOFRLFRCEF.fillData_short error: " + e.getMessage(),e);
            return false;
        }
        return true;
    }

    @Override
    public boolean FillData(DataAdapterReader dataAdapterReader) throws ParseException, IOException {
        try {
            s1apid = dataAdapterReader.GetLongValue("Report_MME_UES1APID", StaticConfig.Int_Abnormal);
            report_type = dataAdapterReader.GetStrValue("report_type","");
            cf_type = dataAdapterReader.GetStrValue("cf_type","");
            if("RLF".equals(cf_type) || "HOF".equals(cf_type)){
                setRLFHOFEcgiByFillData(dataAdapterReader);
            }else if("RCEF".equals(report_type)){
                ecgi = dataAdapterReader.GetLongValue("RCEF_ECI", 0);
            }else if("RlF".equals(report_type)){
                setRLFHOFEcgiByFillData(dataAdapterReader);
            }

            // stime
            if (MainModel.GetInstance().getCompile().Assert(CompileMark.SiChuan)){
                String theTime = dataAdapterReader.GetStrValue("STARTTIME", "1970-01-01T08:00:00");
                tmDate = getRealTime(theTime);
                istime = (int) (tmDate.getTime() / 1000L);
                istimems = (int) (tmDate.getTime() % 1000L);
            }else {
                tmDate = dataAdapterReader.GetDateValue("STARTTIME", new Date(1970, 1, 1));
                istime = (int) (tmDate.getTime() / 1000L);
                istimems = (int) (tmDate.getTime() % 1000L);
            }


            collect_pci = dataAdapterReader.GetLongValue("collect_pci", 0);
            collect_arc = dataAdapterReader.GetLongValue("collect_arc", 0);
            curLng = dataAdapterReader.GetDoubleValue("curLng", 0);
            curLat = dataAdapterReader.GetDoubleValue("curLat", 0);
            curRsrp = dataAdapterReader.GetIntValue("curRsrp", 0);
            curRsrq = dataAdapterReader.GetIntValue("curRsrq", 0);

        } catch (Exception e) {
            LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"MdrData_HOFRLFRCEF.fillData error:",
                    "MdrData_HOFRLFRCEF.fillData error: " + e.getMessage(),e);
            return false;
        }
        return true;
    }

    private void setRLFHOFEcgi(DataAdapterReader dataAdapterReader) throws ParseException {
        ecgi = dataAdapterReader.GetLongValue("RLFHOF_REBUILD_ECI", 0);
        if(ecgi==4294967295L){
            ecgi = dataAdapterReader.GetLongValue("RLFHOF_PRE_ECI", 0);
        }
        if(ecgi==4294967295L){
            //这种是异常的eci，但是也要分发出去，这样会发生数据倾斜，所以应该是将eci分成多份分发出去
            // 由于这种数据不可能超过1T所以分发500个，每个最多2G多一点，是可以运行的
            ecgi = random.nextInt(500);
        }
    }
    private void setRLFHOFEcgiByFillData(DataAdapterReader dataAdapterReader) throws ParseException {
        ecgi = dataAdapterReader.GetLongValue("RLFHOF_REBUILD_ECI", 0);
        if(ecgi==4294967295L){
            ecgi = dataAdapterReader.GetLongValue("RLFHOF_PRE_ECI", 0);
        }
        //吐出的ECI就只用一个
    }
    private Date getRealTime(String theTime){
        Date date = new Date(1970, 1, 1);
        String[] dayHoursMin = theTime.split("T");
        if(dayHoursMin.length!=2){
            return date;
        }
        String[] daySplit = dayHoursMin[0].split("-");
        if(daySplit.length!=3){
            return date;
        }
        if (daySplit[0].length()!=4) {
            return date;
        }
        if(daySplit[1].length()==1){
            daySplit[1]="0"+daySplit[1];
        }
        if (daySplit[2].length()==1) {
            daySplit[2]="0"+daySplit[2];
        }
        String[] hoursSplit = dayHoursMin[1].split(":");
        if(hoursSplit.length!=3){
            return date;
        }
        if (hoursSplit[0].length()==1) {
            hoursSplit[0]="0"+hoursSplit[0];
        }
        if (hoursSplit[1].length()==1) {
            hoursSplit[1]="0"+hoursSplit[1];
        }
        if (hoursSplit[2].startsWith("0.")||hoursSplit[2].startsWith("1.")) {
            hoursSplit[2]="0"+hoursSplit[2];
        }
        String result = daySplit[0]+"-"+daySplit[1]+"-"+daySplit[2]+" "+hoursSplit[0]+":"+hoursSplit[1
                ]+":"+hoursSplit[2];
        SimpleDateFormat dateFormat2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
        try {
             date = dateFormat2.parse(result);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date;
    }

    @Override
    public ArrayList<EventData> toEventData() {
        if(curLat>0){
            iLatitude =(int) (curLat*10000000);
            iLongitude =(int) (curLng*10000000);
            confidentType = StaticConfig.OM;
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
        if(curRsrp>0){
            eventData.lteScRSRP = curRsrp;
        }else {
            eventData.lteScRSRP = LteScRSRP;
        }
        if(curRsrq>0){
            eventData.lteScSinrUL = curRsrq;
        }else {
            eventData.lteScSinrUL = LteScSinrUL;
        }
        if("RLF".equals(cf_type)||"HOF".equals(cf_type)){
            eventData.eventDetial.strvalue[0] = cf_type;
            //事件统计
            if("RLF".equals(cf_type)){
                eventData.eventStat.fvalue[1]=1;
            }else if("HOF".equals(cf_type)){
                eventData.eventStat.fvalue[0]=1;
            }

        }else if("RCEF".equals(report_type)){
            eventData.eventDetial.strvalue[0] =report_type;
            eventData.eventStat.fvalue[2]=1;
        }
        eventData.eventDetial.sampleType = StaticConfig.FAILURE_SAMPLE;
        eventData.eventDetial.fvalue[0] = LteScRSRP;
        eventData.eventDetial.fvalue[1] = LteScSinrUL;
        eventData.eventDetial.fvalue[2] = collect_pci;
        eventData.eventDetial.fvalue[3] = collect_arc;
        eventDataList.add(eventData);
        return eventDataList;
    }

    @Override
    public void toString(StringBuffer sb) {

    }
}
