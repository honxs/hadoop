package cn.mastercom.bigdata.base.model;

/**
 * 按维度来进行统计指标
 * Created by Kwong on 2018/7/13.
 */
public interface Stat<T> extends DO{

    /**
     * 初始化维度数据
     * @param item
     */
    void init(T item);

    /**
     * 统计
     * @param item
     */
    void apply(T item);

    /**
     * 维度
     */
    interface Dimension extends DO{

    }

    /**
     * 聚合器
     */
    interface Aggregator<V extends Aggregator.Values> extends DO{

        void apply(V values);

        interface Values{

        }
    }
}
