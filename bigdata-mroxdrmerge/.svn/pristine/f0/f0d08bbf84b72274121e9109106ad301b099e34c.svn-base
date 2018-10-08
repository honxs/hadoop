package cn.mastercom.bigdata.base.function;

import cn.mastercom.bigdata.base.model.DO;
import cn.mastercom.bigdata.base.model.Stat;
import cn.mastercom.bigdata.util.data.Tuple2;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Created by Kwong on 2018/7/14.
 */
public abstract class StatConsumer<O extends DO, D extends Stat.Dimension, A extends Stat.Aggregator> extends AbstractConsumer<O> {

    /**
     * 统计类
     */
    protected Class statClass;

    /**
     * 统计用的map, key是维度, value是统计对象
     */
    protected Map<D, A> statMap;

    protected StatConsumer(){
        init();
    }

    protected StatConsumer(Class<?> statClass){

        this(statClass, null);
    }

    protected StatConsumer(Class<?> statClass, Map<D, A> statMap){

        Objects.requireNonNull(statClass);

        this.statClass = statClass;
        this.statMap = statMap;
        init();
    }

    private void init(){

        if (this.statMap == null){

            this.statMap = new HashMap<>();

        }

    }

    /**
     * 根据key统计value
     * @param keyToStat
     * @param valueToStat
     */
    protected void stat(D  keyToStat, Stat.Aggregator.Values valueToStat){

        A stat = statMap.get(keyToStat);

        if(stat == null){

            try{

                stat = (A) statClass.newInstance();
                //初始化统计维度
//                stat.init(valueToStat);

            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }

            statMap.put(keyToStat, stat);
        }

        stat.apply(valueToStat);
    }

    /**
     * only for test
     */
    public void printStatMap(){
        for (Map.Entry<D, A> entry :statMap.entrySet()) {
            System.out.println("============================");
            System.out.println("key:" + entry.getKey().toFormatedString());
            System.out.println("value:" + entry.getValue().toFormatedString());
            System.out.println("============================");
        }
    }
}
