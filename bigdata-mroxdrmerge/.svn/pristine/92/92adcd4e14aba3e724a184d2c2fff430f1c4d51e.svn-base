package cn.mastercom.bigdata.stat.impl.xdr;

import cn.mastercom.bigdata.base.Flushable;
import cn.mastercom.bigdata.base.function.AbstractConsumer;
import cn.mastercom.bigdata.base.function.StatConsumer;
import cn.mastercom.bigdata.base.function.impl.StatWrapperConsumer;
import cn.mastercom.bigdata.base.model.Stat;
import cn.mastercom.bigdata.evt.locall.model.XdrDataBase;
import cn.mastercom.bigdata.evt.locall.stat.TypeIoEvtEnum;
import cn.mastercom.bigdata.project.enums.IOutPutPathEnum;
import cn.mastercom.bigdata.stat.aggregate.aggregator.ArrayAggregator;
import cn.mastercom.bigdata.stat.aggregate.aggregator.MRAggregator;
import cn.mastercom.bigdata.stat.aggregate.aggregator.OneAggregator;
import cn.mastercom.bigdata.stat.aggregate.value.ArrayValue;
import cn.mastercom.bigdata.stat.aggregate.value.MRAggregateValue;
import cn.mastercom.bigdata.stat.dimension.CellDimension;
import cn.mastercom.bigdata.stat.dimension.CityDimension;
import cn.mastercom.bigdata.util.ResultOutputer;
import static cn.mastercom.bigdata.stat.impl.xdr.XdrMapToPairFunctions.*;

/**
 * Created by Kwong on 2018/7/18.
 */
public class XdrStatConsumers extends StatConsumer<XdrDataBase, Stat.Dimension, MRAggregator> implements Flushable{

    AbstractConsumer<XdrDataBase> statConsumer;

    public XdrStatConsumers(ResultOutputer resultOutputer){
        statConsumer = cellStat(resultOutputer, TypeIoEvtEnum.XDR_CELL);
    }

    public static StatConsumer<XdrDataBase, Stat.Dimension, ArrayAggregator> cellStat(ResultOutputer resultOutputer, IOutPutPathEnum outPutPathEnum){
        return StatWrapperConsumer.<XdrDataBase, Stat.Dimension, ArrayAggregator, ArrayValue>builder()
                .setPredicate(new XdrPredicates.EciPredicate())
                .setMapToPairFunction(new MapByDayFunction(new MapByCellFunction(new MapByInterfaceFunction())))
                .setStatClass(ArrayAggregator.class)
                .setOutputPathEnum(outPutPathEnum)
                .setResultOutputer(resultOutputer)
                .build();
    }

    @Override
    public void accept(XdrDataBase var1) {
        statConsumer.accept(var1);
    }


    @Override
    public void flush() {
        if (statConsumer instanceof Flushable){

            ((Flushable)statConsumer).flush();
        }
    }
}
