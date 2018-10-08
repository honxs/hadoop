package cn.mastercom.bigdata.stat.dimension;

import cn.mastercom.bigdata.base.constant.DataConstant;
import cn.mastercom.bigdata.base.model.impl.AbstractStat;

/**
 * Created by Kwong on 2018/7/26.
 */
public class HeightDimension extends AbstractStat.AbstractDimension  {

    public int height;

    HeightDimension(){}

    public HeightDimension(int height){
        this.height = height;
    }


    @Override
    public void fromFormatedString(String value) throws Exception {
        if (value.contains(DataConstant.DEFAULT_COLUMN_SEPARATOR)){
            value = value.split(DataConstant.DEFAULT_COLUMN_SEPARATOR)[0];
        }
        height = Integer.parseInt(value);
    }

    @Override
    public String toFormatedString() {
        return String.valueOf(height);
    }
}
