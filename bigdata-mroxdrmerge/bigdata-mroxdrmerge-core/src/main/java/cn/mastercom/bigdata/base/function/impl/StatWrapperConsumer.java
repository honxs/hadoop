package cn.mastercom.bigdata.base.function.impl;

import cn.mastercom.bigdata.base.Flushable;
import cn.mastercom.bigdata.base.constant.DataConstant;
import cn.mastercom.bigdata.base.function.*;
import cn.mastercom.bigdata.base.model.DO;
import cn.mastercom.bigdata.base.model.Stat;
import cn.mastercom.bigdata.project.enums.IOutPutPathEnum;
import cn.mastercom.bigdata.util.ResultOutputer;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

/**
 * {@link SimpleStatConsumer}的包装类，增加统计的吐出功能
 * Created by Kwong on 2018/7/13.
 */
public class StatWrapperConsumer<O extends DO, D extends Stat.Dimension, A extends Stat.Aggregator<V>, V extends Stat.Aggregator.Values> extends StatConsumer<O, D, A> implements Flushable{

    /**
     * 多少条记录进行一次写出
     */
    private int FLUSH_RECORDS_THRESHOLD = 100000;

    /**
     * 被包装的统计
     */
    StatConsumer<O, D, A> statConsumer;

    /**
     * 结果写出工具
     */
    ResultOutputer resultOutputer;

    /**
     * 输出表路径
     */
    IOutPutPathEnum outPutPathEnum;

    /**
     * @deprecated 用条数来统计的值不太靠谱
     * 计数器
     */
    private int count = 0;

    private StatWrapperConsumer(){
        super();
    }


    @Override
    public void accept(O var1) {

        statConsumer.accept(var1);
        //count++;

        if (statMap.size() >= FLUSH_RECORDS_THRESHOLD){

            flush();
//            count = 0;
        }
    }

    /**
     * 将统计内容刷写到磁盘
     */
    public void flush(){

        Iterator<Map.Entry<D, A>> iterator = this.statMap.entrySet().iterator();

        while (iterator.hasNext()){

            Map.Entry<D, A> entry = iterator.next();

            try {
                resultOutputer.pushData(outPutPathEnum.getIndex(), entry.getKey().toFormatedString() + DataConstant.DEFAULT_COLUMN_SEPARATOR + entry.getValue().toFormatedString());
            } catch (Exception e) {
                e.printStackTrace();
            }

            iterator.remove();
        }
    }

    @Override
    public AbstractConsumer<O> andThen(final AbstractConsumer<? super O> var1) {
        Objects.requireNonNull(var1);

        return new StatWrapperConsumer<O, D, A, V>() {

            @Override
            public void accept(O var2) {
                StatWrapperConsumer.this.accept(var2);
                var1.accept(var2);
            }

            @Override
            public void flush() {
                if (var1 instanceof Flushable){
                    ((Flushable) var1).flush();
                }
                StatWrapperConsumer.this.flush();
            }
        };
    }

    public static <O extends DO, D extends Stat.Dimension, A extends Stat.Aggregator<V>, V extends Stat.Aggregator.Values> Builder<O, D, A, V> builder(){
        return new Builder<>();
    }


    /**
     * 构建器
     */
    public static class Builder<O extends DO, D extends Stat.Dimension, A extends Stat.Aggregator<V>, V extends Stat.Aggregator.Values>{

        private StatWrapperConsumer<O, D, A, V> statWrapperConsumer;

        private AbstractPredicate<O> predicate;

        private MapToPairFunction<O, D, V> mapToPairFunction;

        private FlatMapToPairFunction<O, D, V> flatMapToPairFunction;

        private Class<A> statClass;

        Builder(){

            statWrapperConsumer = new StatWrapperConsumer<>();
        }

        public Builder<O, D, A, V> setPredicate(AbstractPredicate<O> predicate){

            Objects.requireNonNull(predicate);

            this.predicate = predicate;

            return this;
        }

        public Builder<O, D, A, V> setMapToPairFunction(MapToPairFunction<O, D, V> mapToPairFunction){

            Objects.requireNonNull(mapToPairFunction);

           this.mapToPairFunction = mapToPairFunction;

            return this;
        }

        public Builder<O, D, A, V> setFlatMapToPairFunction(FlatMapToPairFunction<O, D, V> flatMapToPairFunction){

            Objects.requireNonNull(flatMapToPairFunction);

            this.flatMapToPairFunction = flatMapToPairFunction;

            return this;
        }

        public Builder<O, D, A, V> setStatClass(Class<A> statClass){

            Objects.requireNonNull(statClass);

            this.statClass = statClass;

            return this;
        }

        public Builder<O, D, A, V> setFlushRecordsThreshold(int threshold){

            if (threshold <= 0 || threshold > Integer.MAX_VALUE){
                throw new IllegalArgumentException();
            }

            statWrapperConsumer.FLUSH_RECORDS_THRESHOLD = threshold;

            return this;
        }

        public Builder<O, D, A, V> setResultOutputer(ResultOutputer resultOutputer){

            Objects.requireNonNull(resultOutputer);

            statWrapperConsumer.resultOutputer = resultOutputer;

            return this;
        }

        public Builder<O, D, A, V> setOutputPathEnum(IOutPutPathEnum outputPathEnum){

            Objects.requireNonNull(outputPathEnum);

            statWrapperConsumer.outPutPathEnum = outputPathEnum;

            return this;
        }

        public Builder<O, D, A, V> setStatMap(Map<D, A> statMap){

            Objects.requireNonNull(statMap);

            statWrapperConsumer.statMap = statMap;

            return this;
        }

        public StatWrapperConsumer<O, D, A, V> build(){

            Objects.requireNonNull(statWrapperConsumer);
            Objects.requireNonNull(statWrapperConsumer.outPutPathEnum);
            Objects.requireNonNull(statWrapperConsumer.resultOutputer);
            Objects.requireNonNull(this.predicate);
            Objects.requireNonNull(this.statClass);

            if(this.mapToPairFunction != null){

                statWrapperConsumer.statConsumer = new SimpleStatConsumer<O, D, A, V>(predicate, mapToPairFunction, statWrapperConsumer.statMap, statClass);

            }else if (this.flatMapToPairFunction != null){

                statWrapperConsumer.statConsumer = new FlatStatConsumer<O, D, A, V>(predicate, flatMapToPairFunction, statWrapperConsumer.statMap, statClass);

            }else{

                throw new NullPointerException("没有指定统计的ToPair函数");

            }

            return statWrapperConsumer;
        }
    }
}
