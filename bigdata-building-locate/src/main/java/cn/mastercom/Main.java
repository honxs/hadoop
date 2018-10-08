package cn.mastercom;

import cn.mastercom.api.MrLocationService;
import cn.mastercom.api.impl.MrLocationServiceOnSparkImpl;
import lombok.extern.log4j.Log4j;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

/**
 * Created by Kwong on 2018/8/6.
 */
@Log4j
public class Main {

    public static void main(String[] args){

        if (args == null || args.length < 2){
            log.info("usage: spark-submit [conf] xxx.jar <inputPath> <outputPath>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("mrBuildCalculate");
//        sparkConf.setMaster("local[*]");

        SparkContext sc = new SparkContext(sparkConf);

        try (JavaSparkContext jsc = new JavaSparkContext(sc)){

            MrLocationService service = new MrLocationServiceOnSparkImpl(jsc, args);
            service.train();
        }

        sc.stop();
    }
}