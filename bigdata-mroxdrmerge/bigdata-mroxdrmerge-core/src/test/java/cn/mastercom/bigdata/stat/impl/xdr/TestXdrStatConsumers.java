package cn.mastercom.bigdata.stat.impl.xdr;

import cn.mastercom.bigdata.base.function.StatConsumer;
import cn.mastercom.bigdata.stat.aggregate.aggregator.ArrayAggregator;
import cn.mastercom.bigdata.stat.aggregate.value.ArrayValue;
import cn.mastercom.bigdata.util.FileTypeResult;
import cn.mastercom.bigdata.base.function.impl.StatWrapperConsumer;
import cn.mastercom.bigdata.base.model.Stat;
import cn.mastercom.bigdata.evt.locall.model.XdrDataBase;
import cn.mastercom.bigdata.evt.locall.model.XdrDataFactory;
import cn.mastercom.bigdata.evt.locall.stat.TypeIoEvtEnum;
import cn.mastercom.bigdata.mro.stat.tableEnum.MroBsTablesEnum;
import cn.mastercom.bigdata.project.enums.IOutPutPathEnum;
import cn.mastercom.bigdata.stat.aggregate.aggregator.MRAggregator;
import cn.mastercom.bigdata.util.DataAdapterConf;
import cn.mastercom.bigdata.util.DataAdapterReader;
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
public class TestXdrStatConsumers {

    StatConsumer<XdrDataBase, Stat.Dimension, ArrayAggregator> statConsumer;

    ResultOutputer resultOutputer ;

    String inputPath = "/mt_wlyh/Data/BeiJing/MME/180126/000000_0";

    String outputPath = "E:/tmp/test";

    int dataType = TypeIoEvtEnum.ORIGINMME.getIndex();

    @Before
    public void before(){

        resultOutputer = new ResultOutputer(new FileTypeResult(outputPath, "01_180719", new IOutPutPathEnum[]{TypeIoEvtEnum.XDR_CELL}));

        statConsumer = XdrStatConsumers.cellStat(resultOutputer, TypeIoEvtEnum.XDR_CELL);

    }

    @Ignore
    @Test
    public void test(){

        long startTime = System.currentTimeMillis();
        //加载数据
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.1.31:9000");
        try{
            FileReader.readFile(conf, inputPath, new FileReader.LineHandler() {
                @Override
                public void handle(String line) {
                    try {
                        XdrDataBase xdr = XdrDataFactory.GetInstance().getXdrDataObject(dataType);
                        DataAdapterConf.ParseItem parseItem = xdr.getDataParseItem();
                        DataAdapterReader curDataAdapterReader = new DataAdapterReader(parseItem);
                        xdr = XdrDataFactory.GetInstance().getXdrDataObject(dataType);
                        String[] strs = line.split(parseItem.getSplitMark(), -1);
                        curDataAdapterReader.readData(strs);
                        xdr.FillData(curDataAdapterReader);

                        statConsumer.accept(xdr);

                    } catch (Exception e) {
                        e.printStackTrace();
                        return;
                    }
                }
            });

        }catch (Exception e){
            e.printStackTrace();
        }

        System.out.println("------------------cost " + (System.currentTimeMillis() - startTime) + "(ms)---------------------------");
    }

    @After
    public  void after(){

        statConsumer.printStatMap();

        ((StatWrapperConsumer)statConsumer).flush();

        try {
            resultOutputer.clear();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
