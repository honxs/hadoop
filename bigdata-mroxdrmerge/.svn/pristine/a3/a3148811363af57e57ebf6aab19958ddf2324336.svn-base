package cn.mastercom.bigdata.stat.dimension;

import cn.mastercom.bigdata.base.constant.DataConstant;
import cn.mastercom.bigdata.base.model.impl.AbstractStat;

/**
 * Created by Kwong on 2018/7/16.
 */
public class FreqDimension extends AbstractStat.AbstractDimension {

    public int freq;

    FreqDimension(){}

    public FreqDimension(int freq){
        this.freq = freq;
    }

    @Override
    public void fromFormatedString(String value) throws Exception {
        if (value.contains(DataConstant.DEFAULT_COLUMN_SEPARATOR)){
            value = value.split(DataConstant.DEFAULT_COLUMN_SEPARATOR)[0];
        }
        freq = Integer.parseInt(value);
    }

    @Override
    public String toFormatedString() {

        return String.valueOf(freq);
    }


}
