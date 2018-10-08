package cn.mastercom.mro.loc.normalize.spark.function;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.Collections;

import static cn.mastercom.mro.loc.normalize.util.MroLocExtractor.key;
import static cn.mastercom.mro.loc.normalize.util.MroLocExtractor.newValue;
import static cn.mastercom.mro.loc.normalize.util.MroLocExtractor.oldValue;

public class SrcMapToPairFunction {
    public static PairFlatMapFunction<String, String, int[]> getNewSrcFunction() {
        return new PairFlatMapFunction<String, String, int[]>() {
            @Override
            public Iterable<Tuple2<String, int[]>> call(String line) {
                int[] value = newValue(line);
                if (null == value) {
                    return Collections.emptyList();
                }
                String key = key(value);
                return Collections.singleton(new Tuple2<>(key, value));
            }
        };
    }

    public static PairFlatMapFunction<String, String, int[]> getOldSrcFunction() {
        return new PairFlatMapFunction<String, String, int[]>() {
            @Override
            public Iterable<Tuple2<String, int[]>> call(String s) {
                int[] value = oldValue(s);
                if (null == value) {
                    return Collections.emptyList();
                }
                String key = key(value);
                return Collections.singleton(new Tuple2<>(key, value));
            }
        };
    }
}
