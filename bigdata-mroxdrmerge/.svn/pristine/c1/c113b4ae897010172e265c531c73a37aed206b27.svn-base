package cn.mastercom.bigdata.stat.dimension;

import cn.mastercom.bigdata.base.constant.DataConstant;
import cn.mastercom.bigdata.base.model.Stat;
import cn.mastercom.bigdata.base.model.impl.AbstractStat;

/**
 * Created by Kwong on 2018/7/16.
 */
public class GridDimension extends AbstractStat.AbstractDimension{

    public int tllongitude;
    public int tllatitude;
    public int brlongitude;
    public int brlatitude;

    GridDimension(){}

    public GridDimension(int tllongitude, int tllatitude, int brlongitude, int brlatitude) {
        this.tllongitude = tllongitude;
        this.tllatitude = tllatitude;
        this.brlongitude = brlongitude;
        this.brlatitude = brlatitude;
    }

    @Override
    public void fromFormatedString(String value) throws Exception {

        String[] values = value.split(DataConstant.DEFAULT_COLUMN_SEPARATOR);

        tllongitude = Integer.parseInt(values[0]);
        tllatitude = Integer.parseInt(values[1]);
        brlongitude = Integer.parseInt(values[2]);
        brlatitude = Integer.parseInt(values[3]);
    }

    @Override
    public String toFormatedString() {

        return new StringBuilder().append(tllongitude).append(DataConstant.DEFAULT_COLUMN_SEPARATOR)
                .append(tllatitude).append(DataConstant.DEFAULT_COLUMN_SEPARATOR)
                .append(brlongitude).append(DataConstant.DEFAULT_COLUMN_SEPARATOR)
                .append(brlatitude).toString();
    }
}
