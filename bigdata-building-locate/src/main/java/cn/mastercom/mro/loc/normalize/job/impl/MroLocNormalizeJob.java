package cn.mastercom.mro.loc.normalize.job.impl;

import cn.mastercom.mro.loc.normalize.job.SparkJob;
import cn.mastercom.mro.loc.normalize.spark.function.PairNormalizeFunction;
import cn.mastercom.mro.loc.normalize.spark.function.SrcMapToPairFunction;
import cn.mastercom.mro.loc.normalize.spark.function.ToFinalOutputFunction;
import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class MroLocNormalizeJob extends SparkJob {
    private String newInputPath;
    private String oldInputPath;
    private String outputPath;
    private String appName;

    private static final String DEFAULT_APP_NAME = "mro-loc_normalize";

    public MroLocNormalizeJob(String newInputPath, String oldInputPath, String outputPath) {
        this(newInputPath, oldInputPath, outputPath, DEFAULT_APP_NAME);
    }

    private MroLocNormalizeJob(String newInputPath, String oldInputPath, String outputPath, String appName) {
        this.newInputPath = newInputPath;
        this.oldInputPath = oldInputPath;
        this.outputPath = outputPath;
        this.appName = appName;

        this.javaSparkContext = new JavaSparkContext(sparkConf());
    }

    @Override
    public void doJob() {
        JavaRDD<String> newSrcRdd = javaSparkContext.textFile(newInputPath);
        JavaPairRDD<String, int[]> srcPairRdd = newSrcRdd.flatMapToPair(SrcMapToPairFunction.getNewSrcFunction());
        JavaPairRDD<String, Iterable<int[]>> groupSrcRdd = srcPairRdd.groupByKey();
        JavaPairRDD<String, Iterable<int[]>> normalizeNewRdd = groupSrcRdd.mapValues(PairNormalizeFunction.getNewNormalizeFunction());

        JavaRDD<String> finalResult;
        if (noOldData()) {
            finalResult = normalizeNewRdd.flatMap(ToFinalOutputFunction.getPairFunction());
        } else {
            JavaRDD<String> oldSrcRdd = javaSparkContext.textFile(oldInputPath);
            JavaPairRDD<String, int[]> oldSrcPairRdd = oldSrcRdd.flatMapToPair(SrcMapToPairFunction.getOldSrcFunction());
            JavaPairRDD<String, Iterable<int[]>> oldGroupSrcRdd = oldSrcPairRdd.groupByKey();
            JavaPairRDD<String, Tuple2<Optional<Iterable<int[]>>, Optional<Iterable<int[]>>>> newOldJoinRdd = normalizeNewRdd.fullOuterJoin(oldGroupSrcRdd);
            finalResult = newOldJoinRdd.flatMap(PairNormalizeFunction.getNewOldNormalizeFunction());
        }

        finalResult.saveAsTextFile(outputPath);
    }
    private boolean noOldData() {
        return null == oldInputPath || "".equals(oldInputPath) || "NULL".equals(oldInputPath.toUpperCase());
    }

    private SparkConf sparkConf() {
        return new SparkConf().setAppName(this.appName);
    }
}
