package cn.mastercom.bigdata.spark.common.output.format;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

import java.text.NumberFormat;

public class MultiTextOutputFormat extends MultipleTextOutputFormat<String, String> {
    private transient NumberFormat numberFormat;
    @Override
    protected String generateFileNameForKeyValue(String key, String value, String name) {
        return key + "/part";
    }

    @Override
    protected String getInputFileBasedOutputFileName(JobConf job, String name) {
        int partition = job.getInt(JobContext.TASK_PARTITION, -1);

        String taskType = "r";

        if (null == numberFormat) {
            numberFormat = NumberFormat.getInstance();
            numberFormat.setMinimumIntegerDigits(5);
            numberFormat.setGroupingUsed(false);
        }

        return name + "-" + taskType + "-" + numberFormat.format(partition);
    }

    @Override
    protected String generateActualKey(String key, String value) {
        return null;
    }
}
