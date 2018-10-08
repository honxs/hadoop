package com.chinamobile.util;

/**
 * Created by Zhou Xingwei on 2015-07-28.
 */


public class NetworkUtil
{

    public static String eci_found;

    //有460-00
    public static String getECGI(String hexStr, int radix)
    {
        try
        {
            int ltecid = Integer.valueOf(hexStr, radix);
            int enbid = ltecid >> 8;
            int cid = ltecid & 0x00000FF;
            return "460-00-" + enbid + "-" + cid;
        } catch (Exception e)
        {
            return null;
        }
    }
    //无460-00
    public static String getECGI2(String hexStr, int radix)
    {
        try
        {
            int ltecid = Integer.valueOf(hexStr, radix);
            int enbid = ltecid >> 8;
            int cid = ltecid & 0x00000FF;
            return enbid + "-" + cid;
        } catch (Exception e)
        {
            return null;
        }
    }


    public static int getEnbId(String hexStr, int radix)
    {
        try
        {
            int ltecid = Integer.valueOf(hexStr, radix);
            int enbid = ltecid >> 8;
            return enbid;
        } catch (Exception e)
        {
            return -1;
        }
    }




}
