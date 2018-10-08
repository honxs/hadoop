package cn.mastercom.mro.loc.normalize.spark.function;

import com.google.common.base.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static cn.mastercom.mro.loc.normalize.util.MroLocExtractor.FlagType.*;
import static cn.mastercom.mro.loc.normalize.util.MroLocExtractor.ItemIndex.FLAG;
import static cn.mastercom.mro.loc.normalize.util.MroLocExtractor.outputData;
import static cn.mastercom.mro.loc.normalize.util.MroLocHandler.compareNewWithOld;
import static cn.mastercom.mro.loc.normalize.util.MroLocHandler.merge;

public class PairNormalizeFunction {
    public static Function<Iterable<int[]>, Iterable<int[]>> getNewNormalizeFunction() {
        return new Function<Iterable<int[]>, Iterable<int[]>>() {
            @Override
            public Iterable<int[]> call(Iterable<int[]> newData) {
                return iterateNewWithExist(newData, Collections.<int[]>emptyList());
            }

        };
    }

    public static FlatMapFunction<Tuple2<String, Tuple2<Optional<Iterable<int[]>>,Optional<Iterable<int[]>>>>, String> getNewOldNormalizeFunction() {
        return new FlatMapFunction<Tuple2<String, Tuple2<Optional<Iterable<int[]>>, Optional<Iterable<int[]>>>>, String>() {
            @Override
            public Iterable<String> call(Tuple2<String, Tuple2<Optional<Iterable<int[]>>, Optional<Iterable<int[]>>>> tuple) {
                Iterable<int[]> newData = tuple._2._1.or(Collections.<int[]>emptyList());
                Iterable<int[]> oldData = tuple._2._2.or(Collections.<int[]>emptyList());

                List<int[]> values = iterateNewWithExist(newData, oldData);

                List<String> results = new ArrayList<>(values.size());
                for (int[] value : values) {
                    results.add(outputData(value));
                }
                return results;
            }
        };
    }

    private static List<int[]> iterateNewWithExist(Iterable<int[]> newData, Iterable<int[]> savedData) {
        List<int[]> finalResults = new LinkedList<>();

        for (int[] saved : savedData) {
            finalResults.add(saved);
        }

        outer:
        for (int[] newValue : newData) {
            inner:
            for (int[] savedValue : finalResults) {
                switch (compareNewWithOld(newValue, savedValue)) {
                    case PENDING:
                        continue inner;
                    case SAVE:
                        break inner;
                    case MERGED:
                    case DELETE:
                        default:
                            merge(newValue, savedValue);
                            continue outer;
                }
            }
            saveValue(finalResults, newValue);
        }

        return finalResults;
    }
    private static void saveValue(List<int[]> savedGroup, int[] newValue) {
        newValue[FLAG] = SAVE;
        savedGroup.add(newValue);
    }
}
