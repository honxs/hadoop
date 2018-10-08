package cn.mastercom.bigdata.stat.impl.mr;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.base.function.AbstractConsumer;
import cn.mastercom.bigdata.base.function.StatConsumer;
import cn.mastercom.bigdata.util.FileTypeResult;
import cn.mastercom.bigdata.base.function.impl.StatWrapperConsumer;
import cn.mastercom.bigdata.base.model.Stat;
import cn.mastercom.bigdata.mro.stat.tableEnum.MroBsTablesEnum;
import cn.mastercom.bigdata.project.enums.IOutPutPathEnum;
import cn.mastercom.bigdata.stat.aggregate.aggregator.MRAggregator;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.util.hadoop.hdfs.FileReader;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Created by Kwong on 2018/7/19.
 */
@RunWith(JUnit4.class)
public class TestSampleStatConsumers {

    AbstractConsumer<DT_Sample_4G> statConsumer;

    ResultOutputer resultOutputer ;

    String inputPath = "/mt_wlyh/Data/BeiJing/mroxdrmerge/mro_loc/data_01_180125_0719/tb_mr_outsample_high_dd_180125/outhighsample-r-00000";

    String outputPath = "E:/tmp/test";

    @Before
    public void before(){

        resultOutputer = new ResultOutputer(new FileTypeResult(outputPath, "180719", new IOutPutPathEnum[]{MroBsTablesEnum.mroYdCell}));

        statConsumer = SampleStatConsumers.cellStat(SampleStatConsumers.TelecomOperatorEnum.YD, resultOutputer, MroBsTablesEnum.mroYdCell)
                .andThen(SampleStatConsumers.cellGridStat(StaticConfig.OH, SampleStatConsumers.TelecomOperatorEnum.YD, resultOutputer, MroBsTablesEnum.mroYdCell))
                //.adnThen(...)
        ;
    }

    @Ignore
    @Test
    public void test(){

        long startTime = System.currentTimeMillis();
        //加载数据
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.1.31:9000");
        try{
            FileReader.readFiles(conf, inputPath, new FileReader.LineHandler() {
                @Override
                public void handle(String line) {
                    DT_Sample_4G sample = new DT_Sample_4G();
                    try {
                        sample.fromFormatedString(line);
                    } catch (Exception e) {
                        e.printStackTrace();
                        return;
                    }
                    statConsumer.accept(sample);
                }
            });

        }catch (Exception e){
            e.printStackTrace();
        }

        System.out.println("------------------cost " + (System.currentTimeMillis() - startTime) + "(ms)---------------------------");
    }

    @After
    public  void after(){

        ((StatWrapperConsumer)statConsumer).printStatMap();

        ((StatWrapperConsumer)statConsumer).flush();

        try {
            resultOutputer.clear();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
