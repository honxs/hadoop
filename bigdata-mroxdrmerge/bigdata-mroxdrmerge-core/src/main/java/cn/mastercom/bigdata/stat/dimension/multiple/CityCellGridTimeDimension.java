package cn.mastercom.bigdata.stat.dimension.multiple;

import cn.mastercom.bigdata.stat.dimension.*;

/**
 * Created by Kwong on 2018/7/24.
 */
public class CityCellGridTimeDimension extends DelegateDimension {

    public CityCellGridTimeDimension(int cityId, long eci, int tlLongitude, int tlLatitude, int brLongitude, int brLatitude, int time){
        this(new CityDimension(cityId), new CellDimension(eci), new GridDimension(tlLongitude, tlLatitude, brLongitude, brLatitude), new TimeDimension(time));
    }

    public CityCellGridTimeDimension(CityDimension cityDimension, CellDimension cellDimension, GridDimension gridDimension, TimeDimension timeDimension) {
        super(cityDimension, cellDimension, gridDimension, timeDimension);
    }
}
