package cn.mastercom.api;

/**
 * 可保存的
 * Created by Kwong on 2018/8/31.
 */
public interface Savable {

    void save();

    void load();

    boolean exists();

    boolean delete();
}
