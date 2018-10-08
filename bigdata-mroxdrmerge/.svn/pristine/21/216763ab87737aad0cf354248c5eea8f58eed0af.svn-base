package cn.mastercom.bigdata.stat.aggregate.value;

import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.base.model.Stat;

/**
 * Created by Kwong on 2018/7/20.
 */
public class ArrayValue implements Stat.Aggregator.Values {

    public double[] values;

    public ArrayValue(int length){

        if (length < 1 && length >100){
            throw new IllegalArgumentException("不支持数据组长度："+ length);
        }
        values = new double[length];

        for (int i = 0; i < values.length; i++)
        {
            values[i] = StaticConfig.Int_Abnormal;
        }
    }
}
