package cn.mastercom.bigdata.stat.impl.mr;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.base.Flushable;
import cn.mastercom.bigdata.base.function.AbstractConsumer;
import cn.mastercom.bigdata.base.function.AbstractPredicate;
import cn.mastercom.bigdata.base.function.StatConsumer;
import cn.mastercom.bigdata.base.function.impl.StatWrapperConsumer;
import cn.mastercom.bigdata.base.model.Stat;
import cn.mastercom.bigdata.project.enums.IOutPutPathEnum;
import cn.mastercom.bigdata.stat.aggregate.aggregator.MRAggregator;
import cn.mastercom.bigdata.stat.dimension.CellDimension;
import cn.mastercom.bigdata.stat.aggregate.value.MRAggregateValue;
import cn.mastercom.bigdata.util.ResultOutputer;

import java.util.Objects;

import static cn.mastercom.bigdata.stat.impl.mr.SampleMapToPairFunctions.*;
import static cn.mastercom.bigdata.stat.impl.mr.SamplePredicates.*;

public class SampleStatConsumers extends StatConsumer<DT_Sample_4G, Stat.Dimension, MRAggregator> implements Flushable {

    /**
     * 统计的运营商
     */
    public enum TelecomOperatorEnum{

        YD(1, "移动"),
        LT(2, "联通"),
        DX(3, "电信"),
        YD_LT(4, "移动联通"),
        YD_DX(5, "移动电信");

        int id;
        String name;

        TelecomOperatorEnum(int id, String name){
            this.id = id;
            this.name = name;
        }
    }

    protected AbstractConsumer<DT_Sample_4G> statConsumer;

    @Override
    public void flush() {
        if (statConsumer instanceof Flushable){

            ((Flushable)statConsumer).flush();
        }
    }

    @Override
    public void accept(DT_Sample_4G var1) {
        statConsumer.accept(var1);
    }


    public SampleStatConsumers(TelecomOperatorEnum telecomOperator, ResultOutputer resultOutputer){

        Objects.requireNonNull(telecomOperator);
        Objects.requireNonNull(resultOutputer);
    }

    static StatConsumer<DT_Sample_4G, Stat.Dimension, MRAggregator> cellStat(TelecomOperatorEnum telecomOperatorEnum, ResultOutputer resultOutputer, IOutPutPathEnum outPutPathEnum){
        return StatWrapperConsumer.<DT_Sample_4G, Stat.Dimension, MRAggregator, MRAggregateValue>builder()
                .setPredicate(new EciPredicate())
                .setMapToPairFunction(new MapByHourFunction(new MapByCellFunction(new TelecomOperatorsMapToPairFunction(telecomOperatorEnum, false))))//小区统计不需要频点作维度
                .setStatClass(MRAggregator.class)
                .setOutputPathEnum(outPutPathEnum)
                .setResultOutputer(resultOutputer)
                .build();
    }

    static StatConsumer<DT_Sample_4G, Stat.Dimension, MRAggregator> gridStat(int confidenceType, TelecomOperatorEnum telecomOperatorEnum, ResultOutputer resultOutputer, IOutPutPathEnum outPutPathEnum){
        return StatWrapperConsumer.<DT_Sample_4G, Stat.Dimension, MRAggregator, MRAggregateValue>builder()
                .setPredicate(new ConfidencePredicate(confidenceType).and(new EciPredicate()).and(new LngLatPredicate()).and(new GridPredicate()))
                .setMapToPairFunction(new MapByHourFunction(new MapByGridFunction(new TelecomOperatorsMapToPairFunction(telecomOperatorEnum, true))))
                .setStatClass(MRAggregator.class)
                .setOutputPathEnum(outPutPathEnum)
                .setResultOutputer(resultOutputer)
                .build();
    }

    static StatConsumer<DT_Sample_4G, Stat.Dimension, MRAggregator> cellGridStat(int confidenceType, TelecomOperatorEnum telecomOperatorEnum, ResultOutputer resultOutputer, IOutPutPathEnum outPutPathEnum){
        return StatWrapperConsumer.<DT_Sample_4G, Stat.Dimension, MRAggregator, MRAggregateValue>builder()
                .setPredicate(new ConfidencePredicate(confidenceType).and(new EciPredicate()).and(new LngLatPredicate()).and(new GridPredicate()))
                .setMapToPairFunction(new MapByHourFunction(new MapByCellFunction(new MapByGridFunction(new TelecomOperatorsMapToPairFunction(telecomOperatorEnum, false)))))
                .setStatClass(MRAggregator.class)
                .setOutputPathEnum(outPutPathEnum)
                .setResultOutputer(resultOutputer)
                .build();
    }

    static  AbstractConsumer<DT_Sample_4G> sampleConsumer(final int confidenceType, final ResultOutputer resultOutputer, final IOutPutPathEnum outPutPathEnum){
        return new AbstractConsumer<DT_Sample_4G>(){

            private AbstractPredicate<DT_Sample_4G> predicate = new ConfidencePredicate(confidenceType).and(new EciPredicate());

            @Override
            public void accept(DT_Sample_4G sample) {
                if (predicate.test(sample)){
                    try {
                        resultOutputer.pushData(outPutPathEnum.getIndex(), sample.createNewSampleToLine());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

            @Override
            @SuppressWarnings("unchecked")
            public AbstractConsumer<DT_Sample_4G> andThen(AbstractConsumer var1) {
                Objects.requireNonNull(var1);

                if (var1 instanceof Flushable){
                    return var1.andThen(this);
                }
                return super.andThen(var1);
            }
        };
    }
}
