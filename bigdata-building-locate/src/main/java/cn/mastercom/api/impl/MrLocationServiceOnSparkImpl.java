package cn.mastercom.api.impl;

import cn.mastercom.api.MrLocationService;
import cn.mastercom.dl4j.EciBuildingModel;
import cn.mastercom.dl4j.MrDataSetIterator;
import cn.mastercom.bean.EciMrData;
import lombok.NonNull;
import lombok.extern.log4j.Log4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.SerializableConfiguration;
import scala.Tuple2;

import java.io.*;
import java.util.*;

import static cn.mastercom.util.ValueUtils.*;
import static cn.mastercom.constants.TrainingConstant.*;
@Log4j
public class MrLocationServiceOnSparkImpl implements MrLocationService {

    private transient JavaSparkContext jsc;

    private Broadcast<SerializableConfiguration> confBroadcast;

    private String inputPath;
    private String outputPath;

    /**
     * 用于统计总的小区数量
     */
    private long totalCellCount;
    private long cellInTraining;

    public MrLocationServiceOnSparkImpl(@NonNull JavaSparkContext jsc, @NonNull String... args) {

        this.inputPath = args[0];
        this.outputPath = args[1];
        this.jsc = jsc;
        this.confBroadcast = jsc.broadcast(new SerializableConfiguration(jsc.hadoopConfiguration()));//广播配置以在partition中获取FileSystem
    }

    @Override
    public void train() {
        long startTime = System.currentTimeMillis();

        JavaPairRDD<Integer, Iterable<EciMrData>> eciToSamplesRDD = loadEciToSamplesRDD();
        eciToSamplesRDD.persist(StorageLevel.MEMORY_AND_DISK_SER());

        Map<Integer, List<String>> featureList = toFeaturelist(eciToSamplesRDD);
        Map<Integer, List<Integer>> labelList = toLabelList(eciToSamplesRDD);
        Map<Integer, List<Integer>> topNC = getTopNC(eciToSamplesRDD, TOP_NC_THRESHOLD);
        Map<Integer, Integer> countByEciList = countByEci(eciToSamplesRDD);

        // 统计总的小区数量
        totalCellCount = featureList.size();

        final Broadcast<Map<Integer, List<String>>> featureListBroadcast = jsc.broadcast(featureList);
        final Broadcast<Map<Integer, List<Integer>>> labelListBroadcast = jsc.broadcast(labelList);
        final Broadcast<Map<Integer, List<Integer>>> topNCBroadcast = jsc.broadcast(topNC);
        final Broadcast<Map<Integer, Integer>> countByEciListBroadcast = jsc.broadcast(countByEciList);

                // EciMrDatas分区进行训练
        eciToSamplesRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<Integer, Iterable<EciMrData>>>>() {

            @Override
            public void call(Iterator<Tuple2<Integer, Iterable<EciMrData>>> tuple2Iterator) throws Exception {
                FileSystem fs = FileSystem.get(confBroadcast.getValue().value());

                while (tuple2Iterator.hasNext()) {//同一个ECI
                    Tuple2<Integer, Iterable<EciMrData>> eciToSamples = tuple2Iterator.next();

                    int currentEci = eciToSamples._1();
                    Iterable<EciMrData> sameples = eciToSamples._2();

                    List<String> features = featureListBroadcast.getValue().get(currentEci);
                    List<Integer> labels = labelListBroadcast.getValue().get(currentEci);
                    List<Integer> topNC = topNCBroadcast.getValue().get(currentEci);
                    Integer count = countByEciListBroadcast.getValue().get(currentEci);

                    if (count == null || count < LEAST_BATCH_SIZE) continue;

                    EciBuildingModel model = new EciBuildingModel(currentEci, outputPath, fs);
                    if (model.exists()){
                        model.load();
                    } else if (features != null && labels != null){
                        model.getConfig().setFeatureList(features);
                        model.getConfig().setLabelList(labels);
                        model.getConfig().setTopNC(topNC);
                        model.getConfig().setLoad(true);
                        model.load();
                    } else {
                        continue;
                    }

                    MrDataSetIterator mrDataSetIterator = new MrDataSetIterator(currentEci, sameples, features, labels, BATCH_SIZE, LEAST_BATCH_SIZE);

                    if (model.isLoad()){
                        model.fit(mrDataSetIterator, true);
                        model.save();
                    }
                }
            }
        });

        eciToSamplesRDD.unpersist();

        log.info(String.format("本次训练结束：%n\t共耗时：%s(s)%n\t参与训练的小区数为：%d%n\t实际训练的小区数为：%d"
                , (System.currentTimeMillis() - startTime)/1000
                , getTotalCellCount()
                , getCellInTraining())
        );
    }

    private long getTotalCellCount(){
        return  totalCellCount;
    }

    private long getCellInTraining() {
        if (cellInTraining==0)
            cellInTraining = countCellInTraining();
        return cellInTraining;
    }

    private long countCellInTraining() {
        try {
            FileSystem fs = FileSystem.get(confBroadcast.getValue().value());
            return fs.listStatus(new Path(outputPath), new PathFilter() {
                @Override
                public boolean accept(Path path) {
                    String name = path.getName();
                    return name.endsWith(".txt") && name.matches("^[1-9].*");
                }
            }).length;
        } catch (IOException e) {
            log.error(e);
            return 0;
        }
    }

    private Map<Integer, Integer> countNCEci(Map<Integer, Integer> map, int ncEci) {
        if (ncEci > 0) {
            Integer ncEciCount = map.get(ncEci);
            if (ncEciCount != null) {
                map.put(ncEci, ++ncEciCount);
            } else
                map.put(ncEci, 1);
        }
        return map;
    }

    private Map<Integer, List<Integer>> getTopNC(JavaPairRDD<Integer, Iterable<EciMrData>> eciSampleRDD, final int topN) {
        return eciSampleRDD.mapToPair(new PairFunction<Tuple2<Integer, Iterable<EciMrData>>, Integer, List<Integer>>() {
            @Override
            public Tuple2<Integer, List<Integer>> call(Tuple2<Integer, Iterable<EciMrData>> tuple2) throws Exception {
                // 统计所有EciMrData中邻区的数量（已过滤掉不存在的邻区）
                Map<Integer, Integer> map = new HashMap<>();
                for (EciMrData eciMrData : tuple2._2) {
                    countNCEci(map, eciMrData.getLtenceci1());
                    countNCEci(map, eciMrData.getLtenceci2());
                    countNCEci(map, eciMrData.getLtenceci3());
                    countNCEci(map, eciMrData.getLtenceci4());
                    countNCEci(map, eciMrData.getLtenceci5());
                    countNCEci(map, eciMrData.getLtenceci6());
                }
                // 排序
                List<Map.Entry<Integer, Integer>> sortedList = new ArrayList<>(map.entrySet());
                Collections.sort(sortedList, new Comparator<Map.Entry<Integer, Integer>>() {
                    @Override
                    public int compare(Map.Entry<Integer, Integer> o1, Map.Entry<Integer, Integer> o2) {
                        return o2.getValue() - o1.getValue(); // 降序
                    }
                });
                List<Integer> resultList = new ArrayList<>(topN);
                if (map.size() > 0){
                    for (int i = 0; i < topN && i < sortedList.size(); i++) {
                        resultList.add(sortedList.get(i).getKey());
                    }
                }
                return new Tuple2<>(tuple2._1, resultList);
            }
        }).collectAsMap();
    }

    /**
     * 根据传入的pairRdd提取特征向量。
     * <p>
     * 特征向量的值包括："LteScRSRP"、"LteScRSRQ"、"LteScBSR"、"LteScTadv"、"LteScPHR"、"LteScSinrUL"、"Nccount0"，
     * 如果所有EciMrData里的AOA数据有一个不一致的话，那么特征向量应该包含"LteScAOA"，
     * 以及一个采样点对应的6个邻区的"LteNcRsrp"和"LteNcRsrq"
     * </p>
     *
     * @param pairRdd key是eci，value是EciMrData
     * @return 散列表楼宇id，特征列表）
     */
    private Map<Integer, List<String>> toFeaturelist(JavaPairRDD<Integer, Iterable<EciMrData>> pairRdd) {

        Objects.requireNonNull(pairRdd);
        return pairRdd.mapToPair(new PairFunction<Tuple2<Integer, Iterable<EciMrData>>, Integer, List<String>>() {
            @Override
            public Tuple2<Integer, List<String>> call(Tuple2<Integer, Iterable<EciMrData>> tuple2) {
                // 先把这七个值加到列表："LteScRSRP"、"LteScRSRQ"、"LteScBSR"、"LteScTadv"、"LteScPHR"、"LteScSinrUL"、"Nccount0"
                List<String> featureList = new ArrayList<>(Arrays.asList(FIXED_FEATURE_MARKS));

                // 判断是否要把AOA加入特征列表
                Set<Integer> set = new HashSet<>();
                boolean includeAoa = false;
                for (EciMrData eciMrData : tuple2._2) {
                    set.add(eciMrData.getLteScAOA());
                    if (set.size() > 1) {
                        includeAoa = true;
                        break;
                    }
                }
                if (includeAoa) {
                    featureList.add(OPTION_FEATURE_MARK_SC_AOA);
                }

                // 用来过滤重复邻区
                Set<String> featureSet = new HashSet<>();
                // 把六个邻区的rsrp和rsrq都加入特征列表
                for (EciMrData eciMrData : tuple2._2) {
                    int ncEarf1 = eciMrData.getLteNcEarfcn1();
                    int ncPci1 = eciMrData.getLteNcPci1();
                    if (ncEarf1 != INVALID_INT && ncPci1 != INVALID_INT) {
                        featureSet.add(getFeatureMarkNcRsrp(ncEarf1, ncPci1));
                        featureSet.add(getFeatureMarkNcRsrq(ncEarf1, ncPci1));
                    }
                    int ncEarf2 = eciMrData.getLteNcEarfcn2();
                    int ncPci2 = eciMrData.getLteNcPci2();
                    if (ncEarf2 != INVALID_INT && ncPci2 != INVALID_INT) {
                        featureSet.add(getFeatureMarkNcRsrp(ncEarf2, ncPci2));
                        featureSet.add(getFeatureMarkNcRsrq(ncEarf2, ncPci2));
                    }
                    int ncEarf3 = eciMrData.getLteNcEarfcn3();
                    int ncPci3 = eciMrData.getLteNcPci3();
                    if (ncEarf3 != INVALID_INT && ncPci3 != INVALID_INT) {
                        featureSet.add(getFeatureMarkNcRsrp(ncEarf3, ncPci3));
                        featureSet.add(getFeatureMarkNcRsrq(ncEarf3, ncPci3));
                    }
                    int ncEarf4 = eciMrData.getLteNcEarfcn4();
                    int ncPci4 = eciMrData.getLteNcPci4();
                    if (ncEarf4 != INVALID_INT && ncPci4 != INVALID_INT) {
                        featureSet.add(getFeatureMarkNcRsrp(ncEarf4, ncPci4));
                        featureSet.add(getFeatureMarkNcRsrq(ncEarf4, ncPci4));
                    }
                    int ncEarf5 = eciMrData.getLteNcEarfcn5();
                    int ncPci5 = eciMrData.getLteNcPci5();
                    if (ncEarf5 != INVALID_INT && ncPci5 != INVALID_INT) {
                        featureSet.add(getFeatureMarkNcRsrp(ncEarf5, ncPci5));
                        featureSet.add(getFeatureMarkNcRsrq(ncEarf5, ncPci5));
                    }
                    int ncEarf6 = eciMrData.getLteNcEarfcn6();
                    int ncPci6 = eciMrData.getLteNcPci6();
                    if (ncEarf6 != INVALID_INT && ncPci6 != INVALID_INT) {
                        featureSet.add(getFeatureMarkNcRsrp(ncEarf6, ncPci6));
                        featureSet.add(getFeatureMarkNcRsrq(ncEarf6, ncPci6));
                    }
                }
                featureList.addAll(featureSet);
                return new Tuple2<>(tuple2._1, featureList);
            }
        }).collectAsMap();
    }

    /**
     * 根据传入的pairRdd提取楼宇id
     *
     * @param pairRDD key是eci，value是EciMrData数组
     * @return 散列表（楼宇id，楼宇列表）
     */
    private Map<Integer, List<Integer>> toLabelList(JavaPairRDD<Integer, Iterable<EciMrData>> pairRDD) {
        Objects.requireNonNull(pairRDD);
        return pairRDD.mapToPair(new PairFunction<Tuple2<Integer, Iterable<EciMrData>>, Integer, List<Integer>>() {
            @Override
            public Tuple2<Integer, List<Integer>> call(Tuple2<Integer, Iterable<EciMrData>> tuple2) throws Exception {
                // 获取楼宇id
                Set<Integer> set = new HashSet<>();
                for (EciMrData eciMrData : tuple2._2) {
                    set.add(eciMrData.getBuilding());
                }
                return new Tuple2<Integer, List<Integer>>(tuple2._1, new ArrayList<>(set));
            }
        }).collectAsMap();
    }

    private Map<Integer, Integer> countByEci(JavaPairRDD<Integer, Iterable<EciMrData>> pairRDD) {
        Objects.requireNonNull(pairRDD);
        return pairRDD.mapToPair(new PairFunction<Tuple2<Integer, Iterable<EciMrData>>, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Iterable<EciMrData>> tuple2) throws Exception {
                // 获取楼宇id
                int count = 0;
                for (EciMrData eciMrData : tuple2._2) {
                    count++;
                }
                return new Tuple2<>(tuple2._1, count);
            }
        }).collectAsMap();
    }

    private JavaPairRDD<Integer, Iterable<EciMrData>> loadEciToSamplesRDD() {
        return jsc.textFile(inputPath).map(new Function<String, EciMrData>() {

            @Override
            public EciMrData call(String line) {

                String[] words = line.split("\t", -1);

                if (words.length < 100) return null;

                return new EciMrData(words[8], getValidInt(words[3]), getValidInt(words[4]), getValidInt(words[14]), getValidInt(words[15]), getValidInt(words[18]), getValidInt(words[20]), getValidInt(words[21]), getValidInt(words[22]), getValidInt(words[24]), getValidInt(words[25]), getValidInt(words[27]), getValidInt(words[28]), getValidInt(words[29]), getValidInt(words[30]), getValidInt(words[32]), getValidInt(words[33]), getValidInt(words[34]), getValidInt(words[35]), getValidInt(words[37]), getValidInt(words[38]), getValidInt(words[39]), getValidInt(words[40]), getValidInt(words[42]), getValidInt(words[43]), getValidInt(words[44]), getValidInt(words[45]), getValidInt(words[47]), getValidInt(words[48]), getValidInt(words[49]), getValidInt(words[50]), getValidInt(words[52]), getValidInt(words[53]), getValidInt(words[54]), getValidInt(words[55]), getValidInt(words[31]), getValidInt(words[36]), getValidInt(words[41]), getValidInt(words[46]), getValidInt(words[51]), getValidInt(words[56]));

            }

        }).filter(new Function<EciMrData, Boolean>() {

            @Override
            public Boolean call(EciMrData eciMrData) {
                return eciMrData != null;
            }
        }).groupBy(new Function<EciMrData, Integer>() {
            @Override
            public Integer call(EciMrData v1) throws Exception {
                return v1.getEci();
            }
        });
    }

}