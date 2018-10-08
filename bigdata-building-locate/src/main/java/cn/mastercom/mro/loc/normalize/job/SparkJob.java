package cn.mastercom.mro.loc.normalize.job;

import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

public abstract class SparkJob implements Serializable {
    protected transient JavaSparkContext javaSparkContext;

    public abstract void doJob();

    public final void doJobAndStop() {
        doJob();
        this.javaSparkContext.stop();
    }
}
