package cn.mastercom.bigdata.stat.dimension;

import cn.mastercom.bigdata.base.constant.DataConstant;
import cn.mastercom.bigdata.base.model.Stat;
import cn.mastercom.bigdata.base.model.impl.AbstractStat;

/**
 * Created by Kwong on 2018/7/16.
 */
public class CellDimension extends AbstractStat.AbstractDimension {

    public long eci;

    CellDimension(){}

    public CellDimension(long eci){
        this.eci = eci;
    }

    @Override
    public void fromFormatedString(String value) throws Exception {
        if (value.contains(DataConstant.DEFAULT_COLUMN_SEPARATOR)){
            value = value.split(DataConstant.DEFAULT_COLUMN_SEPARATOR)[0];
        }
        eci = Long.parseLong(value);
    }

    @Override
    public String toFormatedString() {

        return String.valueOf(eci);
    }
}
