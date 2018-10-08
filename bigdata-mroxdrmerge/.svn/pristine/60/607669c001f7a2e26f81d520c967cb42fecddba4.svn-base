package cn.mastercom.bigdata.base.function.impl;

import cn.mastercom.bigdata.base.function.MapToPairFunction;
import cn.mastercom.bigdata.base.model.DO;
import cn.mastercom.bigdata.base.model.Stat;
import cn.mastercom.bigdata.stat.dimension.DelegateDimension;
import cn.mastercom.bigdata.util.data.Tuple2;

/**
 * 代理：对于同一种统计值，代理了apply()方法，以实现合并多个key
 * PS.继承此类后 getKey()/getValue()方法不再起效
 * Created by Kwong on 2018/7/17.
 */
public class DelegateMapToPairFunction<O extends DO, D extends Stat.Dimension, V extends Stat.Aggregator.Values> extends MapToPairFunction<O, Stat.Dimension, V>  {

    /**
     * 被代理的函数
     */
    MapToPairFunction<O, D, V> mapToPairFunction;

    public DelegateMapToPairFunction(MapToPairFunction<O, D, V> mapToPairFunction){
        this.mapToPairFunction = mapToPairFunction;
    }

    @Override
    public Stat.Dimension getKey(O item) {
        return mapToPairFunction.getKey(item);
    }

    @Override
    public V getValue(O item) {
        return mapToPairFunction.getValue(item);
    }
}
