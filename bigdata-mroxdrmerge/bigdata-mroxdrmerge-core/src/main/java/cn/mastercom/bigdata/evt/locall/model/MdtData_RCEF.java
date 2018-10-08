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
import java.util.ArrayList;
import java.util.Date;

public class MdtData_RCEF extends XdrDataBase {
    public static void main(String[] args) {

    }
    private int 发生接入失败小区标识;
    private double UE经度 = 0;
    private double UE纬度=0;
    private int 发生接入失败小区pci=0;
    private int 发生接入失败小区频点=0;
    private int 失败小区rsrp;
    private int 失败小区rsrq;

    private static ParseItem parseItem;
    private Date tmDate = new Date();


    public MdtData_RCEF() {
        super();
        clear();
        if (parseItem == null) {
            parseItem = MainModel.GetInstance().getEventAdapterConfig().getParseItem("LOCALL-MDT-RCEF");
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
            LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"MdrData_RCEF.fillData_short error",
                    "MdrData_RCEF.fillData_short error: " + e.getMessage(),e);
            return false;
        }

        return true;
    }

    @Override
    public boolean FillData(DataAdapterReader dataAdapterReader) throws ParseException, IOException {
        try {
            s1apid = dataAdapterReader.GetLongValue("MME_UE_S1AP_ID", StaticConfig.Int_Abnormal);
            ecgi = dataAdapterReader.GetLongValue("Report_CID", 0);

            // stime
            tmDate = dataAdapterReader.GetDateValue("TimeStamp", new Date(1970, 1, 1));
            istime = (int) (tmDate.getTime() / 1000L);
            istimems = (int) (tmDate.getTime() % 1000L);
            发生接入失败小区标识 = dataAdapterReader.GetIntValue("RCEF_CID", 0);
            发生接入失败小区pci = dataAdapterReader.GetIntValue("RCEF_PCI", 0);
            发生接入失败小区频点 = dataAdapterReader.GetIntValue("RCEF_Freq", 0);
            UE经度 = dataAdapterReader.GetDoubleValue("Longitude", 0);
            UE纬度 = dataAdapterReader.GetDoubleValue("Latitude", 0);
            失败小区rsrp = dataAdapterReader.GetIntValue("RCEF_RSRP", 0);
            失败小区rsrq =dataAdapterReader.GetIntValue("RCEF_RSRQ", 0);
            imei = dataAdapterReader.GetStrValue("IMEI", "");

        } catch (Exception e) {
            LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"MdrData_RCEF.fillData error",
                    "MdrData_RCEF.fillData error: " + e.getMessage(),e);
            return false;
        }

        return true;
    }

    @Override
    public ArrayList<EventData> toEventData() {

        // 哈尔滨的需要有经纬度

        if(UE纬度>0){
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

        eventData.eventDetial.strvalue[0] = "RCEF";
        eventData.eventDetial.fvalue[0] = LteScRSRP;
        eventData.eventDetial.fvalue[1] = LteScSinrUL;
        //8,9,10开始
        eventData.eventDetial.fvalue[8] = 发生接入失败小区标识;
        eventData.eventDetial.fvalue[9] = 发生接入失败小区pci;
        eventData.eventDetial.fvalue[10] = 发生接入失败小区频点;
        eventData.eventDetial.fvalue[11] = 失败小区rsrp;
        eventData.eventDetial.fvalue[12] = 失败小区rsrq;
        eventData.eventStat.fvalue[2]=1;
        eventDataList.add(eventData);
        return eventDataList;
    }

    @Override
    public void toString(StringBuffer sb) {

    }

}
