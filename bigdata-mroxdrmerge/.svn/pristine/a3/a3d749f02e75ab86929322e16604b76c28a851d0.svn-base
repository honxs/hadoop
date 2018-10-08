package cn.mastercom.bigdata.spark.evtstat;

import cn.mastercom.bigdata.evt.locall.model.XdrDataBase;
import cn.mastercom.bigdata.evt.locall.model.XdrDataFactory;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.DataAdapterReader;
import cn.mastercom.bigdata.util.Func;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class MapByXdrFillData implements PairFunction<String, String, String> {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	int type = 0;

    public MapByXdrFillData(int type) {
        this.type = type;
    }

    @Override
    public Tuple2<String, String> call(String value) throws Exception {
        XdrDataBase xdrDataBase = XdrDataFactory.GetInstance().getXdrDataObject(type);
        DataAdapterReader dataAdapterReader = new DataAdapterReader(xdrDataBase.getDataParseItem());
        dataAdapterReader.readData(value.split(xdrDataBase.getDataParseItem().getSplitMark(),-1));
        try {
            xdrDataBase.FillData_short(dataAdapterReader);
        }catch (Exception e){
        	e.printStackTrace();
            return null;
        }
        long imsi = xdrDataBase.getImsi();
        if (imsi <= 0 || imsi==6061155539545534980L)
        { //没考虑内蒙输出的情况
            return null;
        }
        Tuple2<String, String> resultTuple = new Tuple2<>(String.valueOf(imsi), type + "####" + value);
        return resultTuple;
    }
}
