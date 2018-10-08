package cn.mastercom.bigdata.evt.locall.model;

import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.evt.locall.stat.EventData;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.DataAdapterConf;
import cn.mastercom.bigdata.util.DataAdapterReader;
import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;

public class MdtImm5 extends XdrDataBase {
    private static DataAdapterConf.ParseItem parseItem;
    private Date tmDate = new Date();

    private double ULThpTime;
    private double ULThpVolume;
    private double ULLastTTIVolume;
    private double DLThpTimeUE;
    private double DLThpVolumeUE;
    private double DLLastTTIVolume;
    private double ERAB1DLThpTimes;
    private double ERAB1DLThpVolumes;
    private double ERAB2DLThpTimes;
    private double ERAB2DLThpVolumes;
    private double ERAB3DLThpTimes;
    private double ERAB3DLThpVolumes;
    private double ERAB4DLThpTimes;
    private double ERAB4DLThpVolumes;
    private double ERAB5DLThpTimes;
    private double ERAB5DLThpVolumes;
    private double ERAB6DLThpTimes;
    private double ERAB6DLThpVolumes;
    private double ERAB7DLThpTimes;
    private double ERAB7DLThpVolumes;
    private double ERAB8DLThpTimes;
    private double ERAB8DLThpVolumes;

    public MdtImm5() {
        super();
        clear();
        if (parseItem == null) {
            parseItem = MainModel.GetInstance().getEventAdapterConfig().
                    getParseItem("LOCALL-MDT-IMM5");
        }
    }

    @Override
    public int getInterfaceCode() {
        return StaticConfig.INTERFACE_MDT_RLFHOF;
    }

    private void clear() {

    }

    @Override
    public DataAdapterConf.ParseItem getDataParseItem() throws IOException {
        return parseItem;
    }

    @Override
    public boolean FillData_short(DataAdapterReader dataAdapterReader) throws ParseException, IOException {
        try {

            tmDate = dataAdapterReader.GetDateValue("TimeStamp", new Date(1970, 1, 1));
            // stime
            istime = (int) (tmDate.getTime() / 1000L);
            istimems = (int) (tmDate.getTime() % 1000L);
            // etime
            tmDate = new Date(dataAdapterReader.GetLongValue("Procedure_End_Time", -1));
            ietime = (int) (tmDate.getTime() / 1000L);
            ietimems = (int) (tmDate.getTime() % 1000L);

            imsi = dataAdapterReader.GetLongValue("IMSI", 0);
            s1apid = dataAdapterReader.GetLongValue("MME_UE_S1AP_ID",0);
            ecgi = dataAdapterReader.GetLongValue("CellID",0);//服务小区ID
        } catch (Exception e) {
            LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"MdtImm5.fillData_short error",
                    "MdtImm5.fillData_short error: " + e.getMessage(),e);
            return false;
        }

        return true;
    }

    @Override
    public boolean FillData(DataAdapterReader dataAdapterReader) throws ParseException, IOException {
        try {

            tmDate = dataAdapterReader.GetDateValue("TimeStamp", new Date(1970, 1, 1));
            // stime
            istime = (int) (tmDate.getTime() / 1000L);
            istimems = (int) (tmDate.getTime() % 1000L);
            imsi = dataAdapterReader.GetLongValue("IMSI", 0);

            s1apid = dataAdapterReader.GetLongValue("MME_UE_S1AP_ID",0);
            ecgi = dataAdapterReader.GetLongValue("CellID",0);//服务小区ID

            ULThpTime = dataAdapterReader.GetDoubleValue("ULThpTime", 0);
            ULThpVolume = dataAdapterReader.GetDoubleValue("ULThpVolume", 0);
            ULLastTTIVolume = dataAdapterReader.GetDoubleValue("ULLastTTIVolume", 0);
            DLThpTimeUE = dataAdapterReader.GetDoubleValue("DLThpTimeUE", 0);
            DLThpVolumeUE = dataAdapterReader.GetDoubleValue("DLThpVolumeUE", 0);
            DLLastTTIVolume = dataAdapterReader.GetDoubleValue("DLLastTTIVolume", 0);
            ERAB1DLThpTimes = dataAdapterReader.GetDoubleValue("ERAB1DLThpTimes", 0);
            ERAB1DLThpVolumes = dataAdapterReader.GetDoubleValue("ERAB1DLThpVolumes", 0);
            ERAB2DLThpTimes = dataAdapterReader.GetDoubleValue("ERAB2DLThpTimes", 0);
            ERAB2DLThpVolumes = dataAdapterReader.GetDoubleValue("ERAB2DLThpVolumes", 0);
            ERAB3DLThpTimes = dataAdapterReader.GetDoubleValue("ERAB3DLThpTimes", 0);
            ERAB3DLThpVolumes = dataAdapterReader.GetDoubleValue("ERAB3DLThpVolumes", 0);
            ERAB4DLThpTimes = dataAdapterReader.GetDoubleValue("ERAB4DLThpTimes", 0);
            ERAB4DLThpVolumes = dataAdapterReader.GetDoubleValue("ERAB4DLThpVolumes", 0);
            ERAB5DLThpTimes = dataAdapterReader.GetDoubleValue("ERAB5DLThpTimes", 0);
            ERAB5DLThpVolumes = dataAdapterReader.GetDoubleValue("ERAB5DLThpVolumes", 0);
            ERAB6DLThpTimes = dataAdapterReader.GetDoubleValue("ERAB6DLThpTimes", 0);
            ERAB6DLThpVolumes = dataAdapterReader.GetDoubleValue("ERAB6DLThpVolumes", 0);
            ERAB7DLThpTimes = dataAdapterReader.GetDoubleValue("ERAB7DLThpTimes", 0);
            ERAB7DLThpVolumes = dataAdapterReader.GetDoubleValue("ERAB7DLThpVolumes", 0);
            ERAB8DLThpTimes = dataAdapterReader.GetDoubleValue("ERAB8DLThpTimes", 0);
            ERAB8DLThpVolumes = dataAdapterReader.GetDoubleValue("ERAB8DLThpVolumes", 0);
            imei = dataAdapterReader.GetStrValue("IMEI", "");
        } catch (Exception e) {
            LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"MdtImm5.fillData error",
                    "MdtImm5.fillData error: " + e.getMessage(),e);
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
        eventData.wTimems = 0;
        eventData.strLoctp = strloctp;
        eventData.strLabel = label;
        eventData.iLongitude = iLongitude;
        eventData.iLatitude = iLatitude;
        eventData.iBuildID = ibuildid;
        eventData.iHeight = iheight;
        eventData.position = position;
        eventData.Interface = StaticConfig.INTERFACE_MDT_RLFHOF;
        eventData.iKpiSet = 3;
        eventData.iProcedureType = 1;

        eventData.iTestType = testType;
        eventData.iDoorType = iDoorType;
        eventData.iLocSource = locSource;

        eventData.confidentType = confidentType;
        eventData.iAreaType = iAreaType;
        eventData.iAreaID = iAreaID;
        eventData.lteScRSRP = LteScRSRP;
        eventData.lteScSinrUL = LteScSinrUL;
        eventData.eventStat.fvalue[0] = ULThpTime;
        eventData.eventStat.fvalue[1] = ULThpVolume;
        eventData.eventStat.fvalue[2] = DLThpTimeUE;
        eventData.eventStat.fvalue[3] = DLThpVolumeUE;
        eventData.eventStat.fvalue[4] = ERAB1DLThpTimes;
        eventData.eventStat.fvalue[5] = ERAB1DLThpVolumes;
        eventData.eventStat.fvalue[6] = ERAB2DLThpTimes;
        eventData.eventStat.fvalue[7] = ERAB2DLThpVolumes;
        eventData.eventStat.fvalue[8] = ERAB3DLThpTimes;
        eventData.eventStat.fvalue[9] = ERAB3DLThpVolumes;
        eventData.eventStat.fvalue[10] = ERAB4DLThpTimes;
        eventData.eventStat.fvalue[11] = ERAB4DLThpVolumes;
        eventData.eventStat.fvalue[12] = ERAB5DLThpTimes;
        eventData.eventStat.fvalue[13] = ERAB5DLThpVolumes;
        eventData.eventStat.fvalue[14] = ERAB6DLThpTimes;
        eventData.eventStat.fvalue[15] = ERAB6DLThpVolumes;
        eventData.eventStat.fvalue[16] = ERAB7DLThpTimes;
        eventData.eventStat.fvalue[17] = ERAB7DLThpVolumes;
        eventData.eventStat.fvalue[18] = ERAB8DLThpTimes;
        eventData.eventStat.fvalue[19] = ERAB8DLThpVolumes;
        eventData.eventDetial = null;
        if (eventData.haveEventStat()) {
            eventDataList.add(eventData);
        }
        return eventDataList;
    }

    @Override
    public void toString(StringBuffer sb) {

    }
}
