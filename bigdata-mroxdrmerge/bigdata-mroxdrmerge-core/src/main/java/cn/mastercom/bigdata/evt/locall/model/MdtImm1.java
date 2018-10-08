package cn.mastercom.bigdata.evt.locall.model;

import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.evt.locall.stat.EventData;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.DataAdapterConf;
import cn.mastercom.bigdata.util.DataAdapterReader;
import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;

public class MdtImm1 extends XdrDataBase {

    private static DataAdapterConf.ParseItem parseItem;
    private Date tmDate = new Date();

    public MdtImm1() {
        super();
        clear();
        if (parseItem == null) {
            parseItem = MainModel.GetInstance().getEventAdapterConfig().
                    getParseItem("LOCALL-MDT-IMM1");
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
            LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"MdrImm1.fillData_short error",
                    "MdrImm1.fillData_short error: " + e.getMessage(),e);
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

            //经纬度
            iLongitude = (int)(dataAdapterReader.GetDoubleValue("Longitude",0)*10000000);
            iLatitude = (int)(dataAdapterReader.GetDoubleValue("Latitude",0)*10000000);
            confidentType = dataAdapterReader.GetIntValue("Confidence",2);//默认为室外中精度
            imei = dataAdapterReader.GetStrValue("IMEI", "");

        }
        catch (Exception e)
        {
            LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"MdrImm1.fillData error",
                    "MdrImm1.fillData error: " + e.getMessage(),e);
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

    }
}
