package cn.mastercom.bigdata.spark.mergestat;

import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;
import cn.mastercom.bigdata.mergestat.deal.MergeDataFactory;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class MapByKeyFunc_StringKey implements PairFunction<String, String, String> {
    private static final long serialVersionUID = 1L;

    private int type;
    private IMergeDataDo mergeItem;

    public MapByKeyFunc_StringKey(int type) throws IllegalAccessException, InstantiationException {
        this.type = type;
        mergeItem = MergeDataFactory.GetInstance().getMergeDataObject(type);
    }

    @Override
    public Tuple2<String, String> call(String line) throws Exception {
        mergeItem.fillData(line.split("\t"), 0);

        String key = mergeItem.getMapKey();

        return new Tuple2<>(key, line);
    }
}
