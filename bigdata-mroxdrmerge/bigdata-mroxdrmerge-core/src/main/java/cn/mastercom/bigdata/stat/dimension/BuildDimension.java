package cn.mastercom.bigdata.stat.dimension;

import cn.mastercom.bigdata.base.constant.DataConstant;
import cn.mastercom.bigdata.base.model.impl.AbstractStat;

/**
 * Created by Kwong on 2018/7/26.
 */
public class BuildDimension extends AbstractStat.AbstractDimension  {

    public int buildingId;

    BuildDimension(){}

    public BuildDimension(int buildingId){
        this.buildingId = buildingId;
    }

    @Override
    public void fromFormatedString(String value) throws Exception {
        if (value.contains(DataConstant.DEFAULT_COLUMN_SEPARATOR)){
            value = value.split(DataConstant.DEFAULT_COLUMN_SEPARATOR)[0];
        }
        buildingId = Integer.parseInt(value);
    }

    @Override
    public String toFormatedString() {
        return String.valueOf(buildingId);
    }
}
