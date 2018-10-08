package cn.mastercom.bigdata.stat.dimension;

import cn.mastercom.bigdata.base.constant.DataConstant;
import cn.mastercom.bigdata.base.model.impl.AbstractStat;

/**
 * Created by Kwong on 2018/7/19.
 */
public class XdrInterfaceDimension extends AbstractStat.AbstractDimension {

    public int interfaceCode;

    XdrInterfaceDimension(){}

    public XdrInterfaceDimension(int interfaceCode){
        this.interfaceCode = interfaceCode;
    }

    @Override
    public void fromFormatedString(String value) throws Exception {
        if (value.contains(DataConstant.DEFAULT_COLUMN_SEPARATOR)){
            value = value.split(DataConstant.DEFAULT_COLUMN_SEPARATOR)[0];
        }
        interfaceCode = Integer.parseInt(value);
    }

    @Override
    public String toFormatedString() {
        return String.valueOf(interfaceCode);
    }
}
