package cn.mastercom.bigdata.base.model;

import cn.mastercom.bigdata.util.DataAdapterReader;

import java.io.IOException;
import java.text.ParseException;

/**
 * 外部实体
 * 外部数据需要用 adapter 来适配
 * Created by Kwong on 2018/7/13.
 */
public interface ExternalDO extends DO {

    boolean fillData(DataAdapterReader dataAdapterReader) throws ParseException, IOException;
}
