package cn.mastercom.bigdata.stat.impl.mr;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.base.function.MapToPairFunction;
import cn.mastercom.bigdata.base.function.impl.DelegateMapToPairFunction;
import cn.mastercom.bigdata.base.model.Stat;
import cn.mastercom.bigdata.stat.dimension.*;
import cn.mastercom.bigdata.stat.aggregate.value.MRAggregateValue;
import cn.mastercom.bigdata.util.FormatTime;

import java.util.Objects;

/**
 * Created by Kwong on 2018/7/16.
 */
public class SampleMapToPairFunctions {

    /**
     * 按运营商分回不同统计值
     */
    public static class TelecomOperatorsMapToPairFunction extends MapToPairFunction<DT_Sample_4G, Stat.Dimension, MRAggregateValue>{

        MRFreqValueGenerator aggregateValueGenerator;

        public TelecomOperatorsMapToPairFunction(boolean isMapByFreq){
            this(SampleStatConsumers.TelecomOperatorEnum.YD, isMapByFreq);
        }

        public TelecomOperatorsMapToPairFunction(SampleStatConsumers.TelecomOperatorEnum telecomOperatorEnum, final boolean isMapByFreq){

            Objects.requireNonNull(telecomOperatorEnum);

            class YDMRAggregateValueGenerator implements MRFreqValueGenerator{
                @Override
                public FreqDimension generateFreqDimension(DT_Sample_4G sample) {
                    return isMapByFreq ? new FreqDimension(sample.LteScEarfcn) : null;
                }

                public MRAggregateValue generateValue(DT_Sample_4G sample){
                    return new MRAggregateValue(sample.LteScRSRP, sample.LteScRSRQ, sample.LteScSinrUL, sample.overlapSameEarfcn, sample.OverlapAll);
                }
            }
            class LTMRAggregateValueGenerator implements MRFreqValueGenerator{
                @Override
                public FreqDimension generateFreqDimension(DT_Sample_4G sample) {
                    return isMapByFreq ? new FreqDimension(sample.lt_freq[0].LteNcEarfcn) : null;
                }

                public MRAggregateValue generateValue(DT_Sample_4G sample){
                    return new MRAggregateValue(sample.lt_freq[0].LteNcRSRP, sample.lt_freq[0].LteNcRSRQ, StaticConfig.Int_Abnormal, sample.overlapSameEarfcn,
                            sample.OverlapAll);
                }
            }
            class DXMRAggregateValueGenerator implements MRFreqValueGenerator{
                @Override
                public FreqDimension generateFreqDimension(DT_Sample_4G sample) {
                    return isMapByFreq ? new FreqDimension(sample.dx_freq[0].LteNcEarfcn) : null;
                }

                public MRAggregateValue generateValue(DT_Sample_4G sample){
                    return new MRAggregateValue(sample.dx_freq[0].LteNcRSRP, sample.dx_freq[0].LteNcRSRQ, StaticConfig.Int_Abnormal, sample.overlapSameEarfcn,
                            sample.OverlapAll);
                }
            }

            switch (telecomOperatorEnum){
                case YD:
                case YD_LT:
                case YD_DX:
                    aggregateValueGenerator = new YDMRAggregateValueGenerator();
                    break;
                case LT:
                    aggregateValueGenerator = new LTMRAggregateValueGenerator();
                    break;
                case DX:
                    aggregateValueGenerator = new DXMRAggregateValueGenerator();
                    break;
                default:
                    aggregateValueGenerator = new YDMRAggregateValueGenerator();
            }

        }

        interface MRFreqValueGenerator{

            FreqDimension generateFreqDimension(DT_Sample_4G sample);

            MRAggregateValue generateValue(DT_Sample_4G sample);
        }

        /**
         * 带上cityid
         * @param sample
         * @return
         */
        @Override
        public Stat.Dimension getKey(DT_Sample_4G sample) {
            return new DelegateDimension(new CityDimension(sample.cityID), aggregateValueGenerator.generateFreqDimension(sample));
        }

        @Override
        public MRAggregateValue getValue(DT_Sample_4G sample) {
            return aggregateValueGenerator.generateValue(sample);
        }

    }

    /**
     * 按小时分组
     */
    public static class MapByHourFunction extends DelegateMapToPairFunction<DT_Sample_4G, Stat.Dimension, MRAggregateValue> {

        public MapByHourFunction(MapToPairFunction mapToPairFunction) {
            super(mapToPairFunction);
        }

        @Override
        public Stat.Dimension getKey(DT_Sample_4G item) {
            return new DelegateDimension(new TimeDimension(FormatTime.RoundTimeForHour(item.itime)), super.getKey(item));
        }
    }

    /**
     * 按小区分组
     */
    public static class MapByCellFunction extends DelegateMapToPairFunction<DT_Sample_4G, Stat.Dimension, MRAggregateValue> {

        public MapByCellFunction(MapToPairFunction mapToPairFunction){
            super(mapToPairFunction);
        }

        @Override
        public Stat.Dimension getKey(DT_Sample_4G item) {
            return new DelegateDimension(new CellDimension(item.Eci), super.getKey(item));
        }
    }

    /**
     * 按栅格分组
     */
    public static class MapByGridFunction extends DelegateMapToPairFunction<DT_Sample_4G, Stat.Dimension, MRAggregateValue> {

        public MapByGridFunction(MapToPairFunction mapToPairFunction){
            super(mapToPairFunction);
        }

        @Override
        public Stat.Dimension getKey(DT_Sample_4G item) {
            return new DelegateDimension(new GridDimension(item.grid.tllongitude, item.grid.tllatitude, item.grid.brlongitude, item.grid.brlatitude), super.getKey(item));
        }

    }

}
