package cn.mastercom.bigdata.base.function;

import cn.mastercom.bigdata.base.model.DO;
import cn.mastercom.bigdata.util.data.Tuple2;

/**
 * Created by Kwong on 2018/7/13.
 */
public abstract class MapToPairFunction<O extends DO, K, V> extends AbstractFunction<O, Tuple2<K, V>>{

    public Tuple2<K, V> apply(O item) {

        return new Tuple2<>(
                getKey(item),
                getValue(item)
        );
    }

    /**
     * 组装key
     * @param item
     * @return
     */
    public abstract K getKey(O item);

    /**
     * 组装值
     * @param item
     * @return
     */
    public abstract V getValue(O item);
}
