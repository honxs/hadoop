package cn.mastercom.bigdata.spark.evtstat;

import cn.mastercom.bigdata.evt.locall.model.XdrDataFactory;
import cn.mastercom.bigdata.evt.locall.stat.ImsiKey;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.Func;
import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class MapByLocFillData implements PairFunction<String, String, String> {
    private String locString = "";
    private String[] valstrs;
    private long imsi;
    private int ilongtitude;
    private int ilatitude;
    private ImsiKey imsiKey;

    @Override
    public Tuple2<String, String> call(String value) throws Exception {
        String[] split = value.split("\t", -1);
        try
        {
            // imsi已经是加密了的
            imsi = Long.parseLong(valstrs[3]);
            ilongtitude = Integer.parseInt(valstrs[4]);
            ilatitude = Integer.parseInt(valstrs[5]);

            if (imsi <= 0 || ilongtitude <= 0 || ilatitude <= 0 || imsi == 6061155539545534980L)
            {
                return null;
            }
            if (MainModel.GetInstance().getCompile().Assert(CompileMark.ResidentEncrypt)) {
                imsi = Func.getEncrypt(imsi);
            }
            
            return new Tuple2<String, String>(String.valueOf(imsi),value);

        }
        catch (Exception e)
        {
            if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
            {
                LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"format error", "format error：" + locString, e);
            }
            return null;
        }
    }
}
