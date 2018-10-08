package com.chinamobile.xdr;

import com.chinamobile.util.NotProguard;

import java.util.List;

/**
 * Created by Zhou Xingwei on 2016-01-31.
 */

@NotProguard
public interface LocationLoggerCallBack
{
    void onLocationReceived(List<LocationInfo> locationInfoList);
    void onStringReceived(String stringToLog);

}
