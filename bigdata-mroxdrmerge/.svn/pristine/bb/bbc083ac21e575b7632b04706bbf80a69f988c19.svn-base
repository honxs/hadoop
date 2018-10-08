package cn.mastercom.bigdata.conf.cellconfig;

import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class TdCellConfig {
    private static TdCellConfig instance;
    private Map<String, TdCellInfo> tdCellInfoMap;

    public static TdCellConfig GetInstance() {
        if (null == instance) {
            synchronized (TdCellConfig.class) {
                if (null == instance) {
                    instance = new TdCellConfig();
                }
            }
        }
        return instance;
    }

    private TdCellConfig() {
        tdCellInfoMap = new HashMap<>();
    }

    public Map<String, TdCellInfo> getTdCellInfoMap() {
        return tdCellInfoMap;
    }

    public boolean loadTdCell(Configuration configuration) {
        try {
            HDFSOper hdfsOper = new HDFSOper(configuration);
            String tdCellConfigPath = MainModel.GetInstance().getAppConfig().getTDCellConfigPath();
            if (!hdfsOper.checkFileExist(tdCellConfigPath)) {
                LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error, "config not exists: "  + tdCellConfigPath);
                return false;
            }

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(hdfsOper.getHdfs().open(new Path(tdCellConfigPath)), "UTF-8"))) {
                String line = null;
                while (null != (line = reader.readLine())) {
                    try {
                        String[] split = line.split("\t", -1);
                        TdCellInfo tdCellInfo = TdCellInfo.FillData(split);
                        if (tdCellInfo.lac > 0 && tdCellInfo.ci > 0) {
                            tdCellInfoMap.put(tdCellKey(tdCellInfo), tdCellInfo);
                        }
                    }catch (IndexOutOfBoundsException e) {
                        LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"loadGsmCell error1",
                                "loadGsmCell error: " +
                                line, e);
                    }
                }
            }
        } catch (Exception e) {
            LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"loadTdCell error", "loadTdCell error: ", e);
            return false;
        }

        return true;
    }
    private String tdCellKey(TdCellInfo tdCellInfo) {
        return tdCellKey(tdCellInfo.lac, tdCellInfo.ci);
    }
    private String tdCellKey(int lac, int ci) {
        return lac + "_" + ci;
    }

    public TdCellInfo getTdCellInfo(int lac, int ci) {
        return tdCellInfoMap.get(tdCellKey(lac, ci));
    }

    public static void main(String[] args) {
        TdCellConfig.GetInstance().loadTdCell(new Configuration());
        TdCellInfo tdCellInfo = TdCellConfig.GetInstance().getTdCellInfo(Integer.parseInt(args[0]), Integer.parseInt(args[1]));
        System.out.println(tdCellInfo.pci);
    }
}
