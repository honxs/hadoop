package cn.mastercom.bigdata.stat.dimension;


import cn.mastercom.bigdata.base.constant.DataConstant;
import cn.mastercom.bigdata.base.model.impl.AbstractStat;

/**
 * Created by Kwong on 2018/7/16.
 */
public class CityDimension  extends AbstractStat.AbstractDimension {

    public int cityId;

    CityDimension(){}

    public CityDimension(int cityId){
        this.cityId = cityId;
    }

    @Override
    public void fromFormatedString(String value) throws Exception {
        if (value.contains(DataConstant.DEFAULT_COLUMN_SEPARATOR)){
            value = value.split(DataConstant.DEFAULT_COLUMN_SEPARATOR)[0];
        }
        cityId = Integer.parseInt(value);
    }

    @Override
    public String toFormatedString() {

        return String.valueOf(cityId);
    }
}
