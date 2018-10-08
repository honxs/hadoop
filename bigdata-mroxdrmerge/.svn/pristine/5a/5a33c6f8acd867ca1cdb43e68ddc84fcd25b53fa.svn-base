package cn.mastercom.bigdata.spark.evtstat;


import cn.mastercom.bigdata.evt.locall.stat.TypeIoEvtEnum;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.spark.RDDLog;
import cn.mastercom.bigdata.util.spark.RDDResultOutputer;
import cn.mastercom.bigdata.util.spark.TypeInfo;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;

public class EvtStatMain {
    //开始时间01_180125
    private static String startTime = "";
    //读取的hive库
    private static String hiveDBName = "";

    public static String mroXdrMergePath = "";
    
    public static String[] parm = null;
    
    public EvtStatMain()
    {
    	
    }
    public EvtStatMain(String[] args) {
        startTime = args[0];
        hiveDBName = args[1];
        mroXdrMergePath = MainModel.GetInstance().getAppConfig().getMroXdrMergePath();
    }

    public static void main(String[] args) throws Exception {
		EvtStatMain evtStatMain = new EvtStatMain(args);
		
		// parm  
		parm = new String[4];
		parm[0] = startTime;
		parm[1] = mroXdrMergePath + "" + parm[0]; 
		parm[2] = "";
		parm[3] = "";
		evtStatMain.doSparkEvtJob(parm);
		
	}
    
    
    public void doSparkEvtJob(String[] args) throws Exception {
        SparkConf sparkConf = EvtStatUtil.makeConf();
        final JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        final HiveContext hiveContext = new HiveContext(JavaSparkContext.toSparkContext(ctx));
        ArrayList<HashMap<Integer, String>> inputPathLists = EvtStatUtil.getInputPathMaps("01_"+startTime);
        HashMap<Integer, String> imsiMap = inputPathLists.get(0); //imsi关联的map
        HashMap<Integer, String> s1apidMap = inputPathLists.get(1);//s1apid关联的map

        // xdr位置库
        String xdrLocLibPath = String.format("%s/mro_loc/data_%s/XDR_LOC_LIB_%s", mroXdrMergePath, startTime,
                startTime);
        //tb位置库
        String tbLocLibPath = String.format("%s/mro_loc/data_%s/TB_LOC_LIB_%s", mroXdrMergePath, startTime, startTime);

        // xdrData组成 key为imsi,value为data_type+"####"+Value
        JavaPairRDD<String, String> allXdrRDD = null;
        for (final Integer type : imsiMap.keySet()) {
            JavaRDD<String> xdrRDD = ctx.textFile(imsiMap.get(type));
            // imsi, type+value
            MapByXdrFillData mapByXdrFillData = new MapByXdrFillData(type);
            JavaPairRDD<String, String> xdrPairRDD = xdrRDD.mapToPair(mapByXdrFillData).filter(new Function<Tuple2<String, String>, Boolean>() {
                @Override
                public Boolean call(Tuple2<String, String> value) throws Exception {
                    return value !=null;
                }
            });
            allXdrRDD = allXdrRDD.union(xdrPairRDD);
        }
        //位置库组成  key为imsi,value为data_type+"####"+Value
        MapByLocFillData mapByLocFillData = new MapByLocFillData();
        JavaPairRDD<String, String> allLocRDD = ctx.textFile(xdrLocLibPath + "," + tbLocLibPath)
                .mapToPair(mapByLocFillData).filter(new Function<Tuple2<String, String>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, String> value) throws Exception {
                        return value !=null;
                    }
                });

        JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> cogroupRDD =
                allXdrRDD.cogroup(allLocRDD);

        //关联统计
        MapByTypeFuncEvt mapByTypeFuncEvt = new MapByTypeFuncEvt(args);
        JavaPairRDD<TypeInfo, Iterable<String>> resultRDD = cogroupRDD.mapPartitionsToPair(mapByTypeFuncEvt);

        RDDResultOutputer rddResultOutputer = new RDDResultOutputer(mapByTypeFuncEvt.getTypeInfoMng(), resultRDD);

        //保存
        if(MainModel.GetInstance().getCompile().Assert(CompileMark.SaveToHive)) {
        	hiveDBName = MainModel.GetInstance().getSparkConfig().get_HiveDBName();
            ArrayList<Integer> indexList = new ArrayList<>();
            indexList.add(RDDLog.DataType_HIVE_LOG);
            for (TypeIoEvtEnum typeIoEvtEnum : TypeIoEvtEnum.values()){
                indexList.add(typeIoEvtEnum.getIndex());
            }
//            rddResultOutputer.saveAsHiveTableTime(hiveContext, hiveDBName, Integer.parseInt(parallelism), "wymr_dw_RESULT_TIME_yyyyMMDD", "wymr_dw_TB_MODEL_RESULT_TIME_yyyyMMDD",
//                    false, startTime, indexList);
        }
    }
}
