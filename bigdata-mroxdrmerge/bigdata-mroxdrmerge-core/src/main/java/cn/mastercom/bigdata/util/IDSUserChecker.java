package cn.mastercom.bigdata.util;

import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import static cn.mastercom.bigdata.stat.userResident.enmus.BuildIndoorCellTablesEnum.User_Resident_Indoor;

public class IDSUserChecker implements Serializable {
    // ${mroxdrmergepath}/resident_config/tb_user_resident_indoor
    private static final String TABLE_PATH = User_Resident_Indoor.getPath(MainModel.GetInstance().getAppConfig().getMroXdrMergePath(), null);

    private Set<String> idsEciImsi;

    private transient static IDSUserChecker instance;

    private static IDSUserChecker getInstance() {
        if (null == instance) {
            synchronized (IDSUserChecker.class) {
                if (null == instance) {
                    instance = new IDSUserChecker();
                }
            }
        }
        return instance;
    }

    public static boolean isIdsUser(String imsi) {
        return getInstance().idsEciImsi.contains(imsi);
    }
    public static boolean isIdsUser(long imsi) {
        return isIdsUser(String.valueOf(imsi));
    }

    private IDSUserChecker() {
        this.idsEciImsi = load();
    }

    private Set<String> load() {
        Set<String> set = new HashSet<>();

        try {
            FileSystem fileSystem = FileSystem.get(MainModel.GetInstance().getConf());
            Path path = new Path(TABLE_PATH);
            RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(path, false);
            while (iterator.hasNext()) {
                LocatedFileStatus file = iterator.next();
                if (fileSystem.isFile(file.getPath())) {
                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(file.getPath())))) {
                        String line = reader.readLine();
                        while (null != line && !"".equals(line)) {
                            String[] split = line.split("\t");
                            if (6 <= split.length) {
                                set.add(split[5]);
                            }
                            line = reader.readLine();
                            split = null; // JVM栈内部设计可能导致String[]不会马上被视为无引用链 因此这里置为null
                        }
                    }
                }
            }
        } catch (IOException e) {
            LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"IDSuserChecker加载失败", "加载 " + TABLE_PATH +
                            "" +
                            " 失败",
                    e);
        }
        return set;
    }

    public static void main(String[] args) {
        boolean b1 = IDSUserChecker.isIdsUser("6110334926513308311");
        boolean b2 = !IDSUserChecker.isIdsUser("123456789");
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        System.out.println(b1);
        System.out.println(b2);
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
    }
}
