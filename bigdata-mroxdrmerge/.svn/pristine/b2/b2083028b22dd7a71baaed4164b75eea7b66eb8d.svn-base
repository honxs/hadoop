package cn.mastercom.bigdata.stat.aggregate.aggregator;


import cn.mastercom.bigdata.base.model.Stat;
import cn.mastercom.bigdata.stat.aggregate.value.OneValue;

/**
 * 最简单的计数器
 * Created by Kwong on 2018/7/16.
 */
public class OneAggregator implements Stat.Aggregator<OneValue>{

    long count;

    @Override
    public void apply(OneValue values) {
        count++;
    }

    @Override
    public void fromFormatedString(String value) throws Exception {

    }

    @Override
    public String toFormatedString() {
        return String.valueOf(count);
    }
}
