package cn.mastercom.api.impl;

import cn.mastercom.api.Savable;
import cn.mastercom.util.FileUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.log4j.Log4j;
import org.apache.hadoop.fs.FileSystem;
import static cn.mastercom.util.FileUtils.checkFileExists;
/**
 * Created by Kwong on 2018/8/31.
 */
@Log4j
public abstract class AbstractSavable implements Savable {

    /**
     * 保存路径
     */
    protected String savingPath;

    /**
     * 文件系统
     */
    protected FileSystem fileSystem;

    /**
     * 是否已经加载到内存
     */
    @Getter @Setter private boolean isLoad;

    protected AbstractSavable(@NonNull String savingPath, @NonNull FileSystem fileSystem) {
        this.savingPath = savingPath;
        this.fileSystem = fileSystem;
    }

    @Override
    public boolean exists() {
        return checkFileExists(savingPath, fileSystem);
    }

    @Override
    public boolean delete() {
        return FileUtils.delete(savingPath, fileSystem);
    }

    @Override
    public void load() {
        isLoad = true;
    }
}
