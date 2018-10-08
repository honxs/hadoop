package cn.mastercom.mro.loc.normalize.spark.function;

import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.LinkedList;
import java.util.List;

import static cn.mastercom.mro.loc.normalize.util.MroLocExtractor.FlagType.SAVE;
import static cn.mastercom.mro.loc.normalize.util.MroLocExtractor.ItemIndex.FLAG;
import static cn.mastercom.mro.loc.normalize.util.MroLocExtractor.outputData;

public class ToFinalOutputFunction {
    public static FlatMapFunction<Tuple2<String, Iterable<int[]>>, String> getPairFunction() {
        return new FlatMapFunction<Tuple2<String, Iterable<int[]>>, String>() {
            @Override
            public Iterable<String> call(Tuple2<String, Iterable<int[]>> valueGroup) {
                return resultGroup(valueGroup._2);
            }
        };
    }

    private static Iterable<String> resultGroup(Iterable<int[]> values) {
        List<String> results = new LinkedList<>();

        for (int[] value : values) {
            if (value[FLAG] == SAVE) {
                String line = outputData(value);
                results.add(line);
            }
        }

        return results;
    }
}
