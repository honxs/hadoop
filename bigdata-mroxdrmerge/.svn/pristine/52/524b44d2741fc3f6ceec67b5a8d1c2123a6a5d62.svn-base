package cn.mastercom.bigdata.stat.aggregate.aggregator;

import cn.mastercom.bigdata.base.constant.DataConstant;
import cn.mastercom.bigdata.base.model.Stat;
import cn.mastercom.bigdata.stat.aggregate.value.ArrayValue;
import cn.mastercom.bigdata.util.StringUtil;

/**
 * Created by Kwong on 2018/7/20.
 */
public class ArrayAggregator implements Stat.Aggregator<ArrayValue> {

    private ArrayValue result;

    @Override
    public void fromFormatedString(String value) throws Exception {

        String[] values = value.split(DataConstant.DEFAULT_COLUMN_SEPARATOR, -1);
        result.values = new double[values.length];

        for (int i = 0; i < values.length; i++){
            result.values[i] = Double.parseDouble(values[i]);
        }
    }

    @Override
    public String toFormatedString() {

        if (result == null)
            return "";

        return StringUtil.join(result.values, DataConstant.DEFAULT_COLUMN_SEPARATOR.charAt(0));
    }

    @Override
    public void apply(ArrayValue values) {

        if(result == null){
            result = values;
            return;
        }

        if (values.values.length != result.values.length){
            return;
        }

        for (int i = 0; i < result.values.length; i++){

            if(values.values[i] < 0){
                continue;
            }

            if(result.values[i] < 0){
                result.values[i] = values.values[i];
            }else{
                result.values[i] += values.values[i];
            }
        }
    }

}
