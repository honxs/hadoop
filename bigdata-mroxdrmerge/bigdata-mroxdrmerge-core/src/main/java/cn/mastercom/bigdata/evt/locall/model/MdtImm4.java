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

public class MdtImm4 extends XdrDataBase  {
    private static DataAdapterConf.ParseItem parseItem;
    private Date tmDate = new Date();
    private double ERAB1PdcpULTput;
    private double ERAB2PdcpULTput;
    private double ERAB3PdcpULTput;
    private double ERAB4PdcpULTput;
    private double ERAB5PdcpULTput;
    private double ERAB6PdcpULTput;
    private double ERAB7PdcpULTput;
    private double ERAB8PdcpULTput;

    private double ERAB1PdcpDLTput;
    private double ERAB2PdcpDLTput;
    private double ERAB3PdcpDLTput;
    private double ERAB4PdcpDLTput;
    private double ERAB5PdcpDLTput;
    private double ERAB6PdcpDLTput;
    private double ERAB7PdcpDLTput;
    private double ERAB8PdcpDLTput;
    private double Period;

    public MdtImm4() {
        super();
        clear();
        if (parseItem == null) {
            parseItem = MainModel.GetInstance().getEventAdapterConfig().
                    getParseItem("LOCALL-MDT-IMM4");
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
        try
        {

            tmDate = dataAdapterReader.GetDateValue("TimeStamp", new Date(1970, 1, 1));
            // stime
            istime = (int) (tmDate.getTime() / 1000L);
            istimems = (int) (tmDate.getTime() % 1000L);

            imsi = dataAdapterReader.GetLongValue("IMSI", 0);
            s1apid = dataAdapterReader.GetLongValue("MME_UE_S1AP_ID",0);
            ecgi = dataAdapterReader.GetLongValue("CellID",0);//服务小区ID
        }
        catch (Exception e)
        {
            LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"MdtImm4.fillData_short error",
                    "MdtImm4.fillData_short error: " + e.getMessage(),e);
            return false;
        }

        return true;
    }

    @Override
    public boolean FillData(DataAdapterReader dataAdapterReader) throws ParseException, IOException {
        try
        {

            tmDate = dataAdapterReader.GetDateValue("TimeStamp", new Date(1970, 1, 1));
            // stime
            istime = (int) (tmDate.getTime() / 1000L);
            istimems = (int) (tmDate.getTime() % 1000L);
            imsi = dataAdapterReader.GetLongValue("IMSI", 0);
            s1apid = dataAdapterReader.GetLongValue("MME_UE_S1AP_ID",0);
            ecgi = dataAdapterReader.GetLongValue("CellID",0);//服务小区ID
            ERAB1PdcpULTput = dataAdapterReader.GetDoubleValue("ERAB1PdcpULTput",0);
            ERAB2PdcpULTput = dataAdapterReader.GetDoubleValue("ERAB2PdcpULTput",0);
            ERAB3PdcpULTput = dataAdapterReader.GetDoubleValue("ERAB3PdcpULTput",0);
            ERAB4PdcpULTput = dataAdapterReader.GetDoubleValue("ERAB4PdcpULTput",0);
            ERAB5PdcpULTput = dataAdapterReader.GetDoubleValue("ERAB5PdcpULTput",0);
            ERAB6PdcpULTput = dataAdapterReader.GetDoubleValue("ERAB6PdcpULTput",0);
            ERAB7PdcpULTput = dataAdapterReader.GetDoubleValue("ERAB7PdcpULTput",0);
            ERAB8PdcpULTput = dataAdapterReader.GetDoubleValue("ERAB8PdcpULTput",0);

            ERAB1PdcpDLTput = dataAdapterReader.GetDoubleValue("ERAB1PdcpDLTput",0);
            ERAB2PdcpDLTput = dataAdapterReader.GetDoubleValue("ERAB2PdcpDLTput",0);
            ERAB3PdcpDLTput = dataAdapterReader.GetDoubleValue("ERAB3PdcpDLTput",0);
            ERAB4PdcpDLTput = dataAdapterReader.GetDoubleValue("ERAB4PdcpDLTput",0);
            ERAB5PdcpDLTput = dataAdapterReader.GetDoubleValue("ERAB5PdcpDLTput",0);
            ERAB6PdcpDLTput = dataAdapterReader.GetDoubleValue("ERAB6PdcpDLTput",0);
            ERAB7PdcpDLTput = dataAdapterReader.GetDoubleValue("ERAB7PdcpDLTput",0);
            ERAB8PdcpDLTput = dataAdapterReader.GetDoubleValue("ERAB8PdcpDLTput",0);
            Period = dataAdapterReader.GetDoubleValue("Period",0);
            imei = dataAdapterReader.GetStrValue("IMEI", "");
        }
        catch (Exception e)
        {
            LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"MdtImm4.fillData error",
                    "MdtImm4.fillData error: " + e.getMessage(),e);
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
        eventData.iKpiSet = 2;
        eventData.iProcedureType = 1;

        eventData.iTestType = testType;
        eventData.iDoorType = iDoorType;
        eventData.iLocSource = locSource;

        eventData.confidentType = confidentType;
        eventData.iAreaType = iAreaType;
        eventData.iAreaID = iAreaID;
        eventData.lteScRSRP = LteScRSRP;
        eventData.lteScSinrUL = LteScSinrUL;

        eventData.eventStat.fvalue[0]=ERAB1PdcpULTput;
        eventData.eventStat.fvalue[1]=ERAB2PdcpULTput;
        eventData.eventStat.fvalue[2]=ERAB3PdcpULTput;
        eventData.eventStat.fvalue[3]=ERAB4PdcpULTput;
        eventData.eventStat.fvalue[4]=ERAB5PdcpULTput;
        eventData.eventStat.fvalue[5]=ERAB6PdcpULTput;
        eventData.eventStat.fvalue[6]=ERAB7PdcpULTput;
        eventData.eventStat.fvalue[7]=ERAB8PdcpULTput;

        eventData.eventStat.fvalue[8]=ERAB1PdcpDLTput;
        eventData.eventStat.fvalue[9]=ERAB2PdcpDLTput;
        eventData.eventStat.fvalue[10]=ERAB3PdcpDLTput;
        eventData.eventStat.fvalue[11]=ERAB4PdcpDLTput;
        eventData.eventStat.fvalue[12]=ERAB5PdcpDLTput;
        eventData.eventStat.fvalue[13]=ERAB6PdcpDLTput;
        eventData.eventStat.fvalue[14]=ERAB7PdcpDLTput;
        eventData.eventStat.fvalue[15]=ERAB8PdcpDLTput;
        eventData.eventStat.fvalue[16] = Period;
        eventData.eventDetial = null;
        if(eventData.haveEventStat()){
            eventDataList.add(eventData);
        }
        return eventDataList;

    }

    @Override
    public void toString(StringBuffer sb) {

    }
}
