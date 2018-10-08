package cn.mastercom.bigdata.stat.impl.xdr;

import cn.mastercom.bigdata.base.function.MapToPairFunction;
import cn.mastercom.bigdata.base.function.impl.DelegateMapToPairFunction;
import cn.mastercom.bigdata.base.model.Stat;
import cn.mastercom.bigdata.evt.locall.model.XdrDataBase;
import cn.mastercom.bigdata.stat.aggregate.value.ArrayValue;
import cn.mastercom.bigdata.stat.aggregate.value.OneValue;
import cn.mastercom.bigdata.stat.dimension.*;
import cn.mastercom.bigdata.util.FormatTime;

/**
 * Created by Kwong on 2018/7/18.
 */
public class XdrMapToPairFunctions {

    /**
     * 按Xdr数据接口分组
     */
    public static class MapByInterfaceFunction extends MapToPairFunction<XdrDataBase, Stat.Dimension, ArrayValue> {


        @Override
        public Stat.Dimension getKey(XdrDataBase item) {
            return new DelegateDimension(new CityDimension(item.iCityID), new XdrInterfaceDimension(item.getInterfaceCode()));
        }

        @Override
        public ArrayValue getValue(XdrDataBase item) {
            ArrayValue arrayValue = new ArrayValue(20);
            arrayValue.values[0] = 1;
            return arrayValue;
        }
    }

    /**
     * 按小区分组
     */
    public static class MapByCellFunction extends DelegateMapToPairFunction<XdrDataBase, Stat.Dimension, ArrayValue> {


        public MapByCellFunction(MapToPairFunction<XdrDataBase, Stat.Dimension, ArrayValue> mapToPairFunction) {
            super(mapToPairFunction);
        }

        @Override
        public Stat.Dimension getKey(XdrDataBase item) {
            return new DelegateDimension(new CellDimension(item.ecgi), super.getKey(item));
        }

    }

    /**
     * 按时间分组
     */
    public static class MapByDayFunction extends DelegateMapToPairFunction<XdrDataBase, Stat.Dimension, ArrayValue> {


        public MapByDayFunction(MapToPairFunction<XdrDataBase, Stat.Dimension, ArrayValue> mapToPairFunction) {
            super(mapToPairFunction);
        }

        @Override
        public Stat.Dimension getKey(XdrDataBase item) {
            return new DelegateDimension(new TimeDimension(FormatTime.getRoundDayTime(item.getIstime())), super.getKey(item));
        }

    }
}
