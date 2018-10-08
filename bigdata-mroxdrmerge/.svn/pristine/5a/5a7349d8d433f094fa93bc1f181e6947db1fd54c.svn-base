package cn.mastercom.bigdata.stat.impl.mr;

import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.util.ResultOutputer;

import static cn.mastercom.bigdata.mro.stat.tableEnum.MroBsTablesEnum.*;
import static cn.mastercom.bigdata.stat.impl.mr.SampleStatConsumers.TelecomOperatorEnum.YD;


public class YDSampleStatConsumers extends SampleStatConsumers {

    public YDSampleStatConsumers(ResultOutputer resultOutputer) {
        super(YD, resultOutputer);

        statConsumer = cellStat(YD, resultOutputer, mroYdCell)

                .andThen(cellGridStat(StaticConfig.OH, YD, resultOutputer, mroOutgridCellHigh))
                .andThen(gridStat(StaticConfig.OH, YD, resultOutputer, mroYdOutgridHigh))
                .andThen(sampleConsumer(StaticConfig.OH, resultOutputer, mroOutsampleHigh))

                .andThen(cellGridStat(StaticConfig.OM, YD, resultOutputer, mroOutgridCellMid))
                .andThen(gridStat(StaticConfig.OM, YD, resultOutputer, mroYdOutgridMid))
                .andThen(sampleConsumer(StaticConfig.OM, resultOutputer, mroOutsampleMid))

                .andThen(cellGridStat(StaticConfig.OL, YD, resultOutputer, mroOutgridCellLow))
                .andThen(gridStat(StaticConfig.OL, YD, resultOutputer, mroYdOutgridLow))
                .andThen(sampleConsumer(StaticConfig.OL, resultOutputer, mroOutsampleLow))

                .andThen(cellGridStat(StaticConfig.IH, YD, resultOutputer, mroIngridCellHigh))
                .andThen(gridStat(StaticConfig.IH, YD, resultOutputer, mroYdIngridHigh))
                .andThen(sampleConsumer(StaticConfig.IH, resultOutputer, mroInsampleHigh))

                .andThen(cellGridStat(StaticConfig.IM, YD, resultOutputer, mroIngridCellMid))
                .andThen(gridStat(StaticConfig.IM, YD, resultOutputer, mroYdIngridMid))
                .andThen(sampleConsumer(StaticConfig.IM, resultOutputer, mroInsampleMid))

                .andThen(cellGridStat(StaticConfig.IL, YD, resultOutputer, mroIngridCellLow))
                .andThen(gridStat(StaticConfig.IL, YD, resultOutputer, mroYdIngridLow))
                .andThen(sampleConsumer(StaticConfig.IL, resultOutputer, mroInsampleLow));
    }
}
