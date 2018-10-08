package cn.mastercom.api.impl;

import cn.mastercom.dl4j.EciBuildingModel;
import cn.mastercom.exception.ModelNotTrainedException;
import cn.mastercom.bean.EciMrData;
import lombok.NonNull;
import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.util.*;

import static cn.mastercom.util.ValueUtils.INVALID_INT;

/**
 * 预测
 * Created by Kwong on 2018/8/13.
 */
@Log4j
public class MrLocationPredict {

    private String configPath;

    private Map<Integer, EciBuildingModel> cache;

    private int eci = -1;

    private Configuration conf;

    public MrLocationPredict(long eci, @NonNull String path, Configuration conf) {

        this.cache = new HashMap<>();
        this.configPath = path;

        if (conf == null) {
            conf = new Configuration();
        }
        this.conf = conf;

        if (eci > 0 && eci < Integer.MAX_VALUE) {
            this.eci = (int) eci;
            try {
                EciBuildingModel model = new EciBuildingModel(this.eci, configPath, FileSystem.get(this.conf));
                if (!model.exists())
                    throw new IllegalArgumentException(String.format("在目录【%s】中找不到eci【%s】训练后的配置。", configPath, this.eci));
            } catch (IOException e) {
                throw new IllegalArgumentException("无法获取文件系统");
            }
        }
    }

    public MrLocationPredict(long eci, @NonNull String path) {
        this(eci, path, null);
    }

    public MrLocationPredict(@NonNull String path) {
        this(-1, path);
    }

    public int predict(EciMrData eciMrData) {

        if (eci > 0 && eciMrData.getEci() != eci) {
            return -1;
        }

        EciBuildingModel model = getModel(eciMrData.getEci());

        if (model == null)
            return -1;

        return model.predict(eciMrData);
    }

    public int ensemblePredict(EciMrData eciMrData) throws ModelNotTrainedException {

        int result = predict(eciMrData);

        if (result <= 0) {
            return -1;
        }

        List<Integer> ncList = getModel(eciMrData.getEci()).getConfig().getTopNC();
        if (ncList == null || ncList.isEmpty()) {
            return result;
        }

        Map<Integer, Integer> resultEciNum = new HashMap<>();
        resultEciNum.put(result, 1);
        for (Integer ncEci : ncList) {
            eciMrData.setEci(ncEci);
            int tmpResult = INVALID_INT;
            try {
                tmpResult = predict(eciMrData);
            } catch (Exception e) {
                log.warn(e);
            }

            if (tmpResult > 0) {
                Integer count = resultEciNum.get(tmpResult);
                if (count == null) {
                    resultEciNum.put(tmpResult, 1);
                } else {
                    resultEciNum.put(tmpResult, count + 1);
                }
            }
        }
        Iterator<Map.Entry<Integer, Integer>> iterator = resultEciNum.entrySet().iterator();
        int maxTimes = -1;
        while (iterator.hasNext()) {
            Map.Entry<Integer, Integer> entry = iterator.next();
            if (entry.getValue() > maxTimes) {
                maxTimes = entry.getValue();
                result = entry.getKey();
            }
        }
        return result;
    }

    private EciBuildingModel getModel(int eci) {
        EciBuildingModel model = cache.get(eci);

        if (model == null) {
            try {
                model = new EciBuildingModel(eci, configPath, FileSystem.get(conf));
                if (model.exists())
                    model.load();
                else return null;
            } catch (IOException e) {
                log.error(e);
                return null;
            }
            cache.put(eci, model);
        }
        return model;
    }

}
