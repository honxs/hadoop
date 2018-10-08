package cn.mastercom.bigdata.spark.evtstat;

import cn.mastercom.bigdata.evt.locall.model.XdrDataFactory;
import cn.mastercom.bigdata.evt.locall.stat.BinarySearchJoin;
import cn.mastercom.bigdata.evt.locall.stat.ImsiKey;
import cn.mastercom.bigdata.evt.locall.stat.LocItem;
import cn.mastercom.bigdata.evt.locall.stat.TypeIoEvtEnum;
import cn.mastercom.bigdata.evt.locall.stat.XdrLocallexDeal2;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.util.spark.MapByTypeFuncBaseV2;
import cn.mastercom.bigdata.util.spark.TypeInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;

public class MapByTypeFuncEvt extends MapByTypeFuncBaseV2
        implements PairFlatMapFunction<Iterator<Tuple2<String,Tuple2<Iterable<String>,Iterable<String>>>>, TypeInfo, Iterable<String>>{

    private ResultOutputer resultOutputer;
    private XdrLocallexDeal2 xdrLocallexDeal;
   
    public MapByTypeFuncEvt(Object[] args) throws Exception {
        super(args);
        registerTableInfo();
    }

    @Override
    protected int init_once_sub() throws Exception {
        // 初始化基础配置
        Configuration conf = new Configuration();
        if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
    	{
    		conf.set("fs.defaultFS", "hdfs://192.168.1.31:9000");
    	}
    	conf.set("mapreduce.job.date", "01_" + dateStr);
        MainModel.GetInstance().setConf(conf);
        resultOutputer = new ResultOutputer(dataOutputMng);
        xdrLocallexDeal = new XdrLocallexDeal2(resultOutputer);
        return 0;
    }

    @Override
    public Iterable<Tuple2<TypeInfo, Iterable<String>>>
    call(Iterator<Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>>> tuple2Iterator)
            throws Exception {
        init_once();
        BinarySearchJoin.clear();
        // 开始业务代码了
        while (tuple2Iterator.hasNext()) {
            Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>> tp2 = tuple2Iterator.next();
            //这个imsiKey只是用来适配这个init(imsiKey)的
            ImsiKey imsiKey = new ImsiKey(Long.parseLong(tp2._1), 0);
            xdrLocallexDeal.init(imsiKey);
            // xdrLocallexDeal.locArr = new LocItem[86400];

            Iterable<String> xdrItera = tp2._2._1;
            Iterable<String> locItera = tp2._2._2;
            collectLocData(locItera);
            collectXdrData(xdrItera);
            //统计和吐出了
            xdrLocallexDeal.statData();
        }
        xdrLocallexDeal.dataStater.outResult();
        return new ArrayList<>();
    }

    /**
     * 
     * @param xdrItera
     */
    private void collectXdrData(Iterable<String> xdrItera) 
    {
    	Iterator<String> iterator = xdrItera.iterator();
        while (iterator.hasNext()) 
        {
            String locValue = iterator.next();
            String[] split = locValue.split("####",2);
            int type = Integer.parseInt(split[0]);
            xdrLocallexDeal.pushData(type,split[1]);
        }
    }
    
    /**
     * 
     * @param locItera
     */
    private void collectLocData(Iterable<String> locItera) 
    {
    	Iterator<String> iterator = locItera.iterator();
        while (iterator.hasNext()) 
        {
            String locValue = iterator.next();
            xdrLocallexDeal.pushData(XdrDataFactory.LOCTYPE_XDRLOC,locValue);
        }
    }

    

    /**
     * 注册表结构
     */
    public void registerTableInfo(){
        for (TypeIoEvtEnum typeIoEvtEnum : TypeIoEvtEnum.values()) {
            TypeInfo typeInfo = new TypeInfo(typeIoEvtEnum.getIndex(),
                    typeIoEvtEnum.getFileName(), typeIoEvtEnum.getPath(outPath, dateStr));
            registTypeInfo(typeInfo);
        }

    }

    /**
     * 构造输出器，要么是hive输出器，要么是hdfs输出器
     * @param typeInfo
     */
    private void registTypeInfo(TypeInfo typeInfo)
    {
        if (!MainModel.GetInstance().getCompile().Assert(CompileMark.SaveToHive))
        {
            typeInfoMng_HDFS.registTypeInfo(typeInfo);
        }
        else
        {
            typeInfoMng_RDD.registTypeInfo(typeInfo);
        }
    }
}
