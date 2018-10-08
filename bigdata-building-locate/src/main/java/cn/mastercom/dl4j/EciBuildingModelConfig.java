package cn.mastercom.dl4j;

import cn.mastercom.api.impl.AbstractSavable;
import cn.mastercom.util.FileUtils;
import lombok.Cleanup;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.log4j.Log4j;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static cn.mastercom.util.FileUtils.getInputStreamFrom;

/**
 * Created by Kwong on 2018/8/31.
 */
@Log4j
public class EciBuildingModelConfig extends AbstractSavable {

    @Getter @Setter private List<Integer> topNC;
    @Getter @Setter private List<Integer> labelList;
    @Getter @Setter private List<String> featureList;
    @Getter @Setter private List<Double> colMean;
    @Getter @Setter private List<Double> colStd;

    public EciBuildingModelConfig(@NonNull String savingPath, @NonNull FileSystem fileSystem) {
        super(savingPath, fileSystem);
    }

    @Override
    public void save() {
        @NonNull OutputStream os = FileUtils.getOutputStreamTo(savingPath, fileSystem);
        @Cleanup PrintStream out = new PrintStream(os);

        if (topNC.isEmpty()) {
            out.print("-1");
        } else {
            save0(topNC, out);
        }
        out.println();
        save0(labelList, out);
        out.println();
        save0(featureList, out);
        out.println();
        save0(colMean, out);
        out.println();
        save0(colStd, out);
    }

    @Override
    public void load() {
        topNC = new ArrayList<>();
        labelList = new ArrayList<>();
        featureList = new ArrayList<>();
        colMean = new ArrayList<>();
        colStd = new ArrayList<>();

        InputStream is = getInputStreamFrom(savingPath, fileSystem);
        if (is == null) throw new NullPointerException(String.format("获取配置【%s】输入流失败", savingPath));

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            load0(reader);
            setLoad(true);
        } catch (IOException e) {
            log.error("", e);
        }
    }

    private void load0(BufferedReader reader) throws IOException{
        // 读取邻区
        String topNCLine = reader.readLine();
        String[] nCs = topNCLine.split(",");
        if (!nCs[0].equals("-1"))
            for (String str : nCs)
                topNC.add(Integer.parseInt(str));
        // 读取标签列表
        String labelsLine = reader.readLine();
        String[] labels = labelsLine.split(",");
        for (String str : labels)
            labelList.add(Integer.parseInt(str));
        // 读取特征列表
        String featuresLine = reader.readLine();
        String[] features = featuresLine.split(",");
        featureList.addAll(Arrays.asList(features));
        // 读取均值
        String meansLine = reader.readLine();
        String[] means = meansLine.split(",");
        for (String str : means)
            colMean.add(Double.parseDouble(str));
        // 读取标差
        String stdLine = reader.readLine();
        String[] stds = stdLine.split(",");
        for (String str : stds)
            colStd.add(Double.parseDouble(str));
    }

    private void save0(List<?> data, PrintStream out) {
        if (data != null){
            String str;
            for (int i = 0; i < data.size(); i++) {
                if (i < data.size() - 1)
                    str = data.get(i) + ",";
                else
                    str = String.valueOf(data.get(i));
                out.print(str);
            }
        }
    }

}
