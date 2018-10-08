package cn.mastercom.bigdata.base.function.impl;

import cn.mastercom.bigdata.base.function.*;
import cn.mastercom.bigdata.base.model.DO;
import cn.mastercom.bigdata.base.model.Stat;
import cn.mastercom.bigdata.util.data.Tuple2;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 *
 * 原理：apply(DO) = predicate.apply(DO) -> mapToPairFunction.apply(DO) -> stat.apply(DO)
 *
 * 设计意图：
 * 统计对象由范型O指定，统计内容由构函入参clazz。(由于在java中范型在定义时是可选的，明确约束的要通过构函校验)
 * 统计类别由构函入参predicate注入，统计维度由构函入参mapToPairFunction注入
 * Created by Kwong on 2018/7/13.
 */
public class SimpleStatConsumer<O extends DO, D extends Stat.Dimension, A extends Stat.Aggregator<V>, V extends Stat.Aggregator.Values> extends StatConsumer<O, D, A> {

    /**
     * 该实体是否统计
     */
    AbstractPredicate<O> predicate;

    /**
     * 将实体转化为键值对
     */
    MapToPairFunction<O, D, V> mapToPairFunction;

    public SimpleStatConsumer(AbstractPredicate<O> predicate, MapToPairFunction<O, D, V> mapToPairFunction, Class<A> statClass){
        this(predicate, mapToPairFunction, null, statClass);
    }

    public SimpleStatConsumer(AbstractPredicate<O> predicate, MapToPairFunction<O, D, V> mapToPairFunction, Map<D, A> statMap, Class<A> statClass){

        super(statClass, statMap);

        Objects.requireNonNull(predicate);
        Objects.requireNonNull(mapToPairFunction);

        this.predicate = predicate;
        this.mapToPairFunction = mapToPairFunction;

    }


    @Override
    public void accept(O var1) {

        if (predicate.test(var1)){

            Tuple2<D, V> tuple2 = mapToPairFunction.apply(var1);

            stat(tuple2.first, tuple2.second);
        }

    }

}
