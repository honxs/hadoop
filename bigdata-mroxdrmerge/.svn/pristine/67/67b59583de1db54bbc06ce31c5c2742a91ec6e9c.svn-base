package com.chinamobile.util;

import java.math.BigDecimal;

/**
 * Created by Zhou Xingwei on 2015-12-26.
 */


public class CalculatorUtil
{
    /**
     * param:String
     * 将Null转为Zero
     * @return "0"
     */
    private String null2zero(String str)
    {
        if (str == null || "".equals(str))
        {
            str = "0";
        }
        return str;
    }

    /**
     * 提供精确的加法运算
     * @param v1 被加
     * @param v2 加数
     * @return 两个参数的和
     */
    public String add(String v1, String v2)
    {
//        v1 = null2zero(v1);
//        v2 = null2zero(v2);
        if(v1==null || v2==null || "".equals(v1) || "".equals(v2)  || "null".equals(v1) || "null".equals(v2))
        {
            return null;
        }
        BigDecimal b1 = new BigDecimal(v1);
        BigDecimal b2 = new BigDecimal(v2);
        return b1.add(b2).toString();
    }

    /**
     * 提供精确的加法运算
     * @param params
     * @return 两个参数的和
     */
    public String add2(String[] params)
    {
        String v0 = null2zero(params[0]);
        BigDecimal b0 = new BigDecimal(v0);
        for (int i = 1; i < params.length; i++)
        {
            String v1 = null2zero(params[i]);
            b0 = b0.add(new BigDecimal(v1));
        }
        return b0.toString();
    }

    /**
     * 提供精确的减法运算
     * @param v1 被减
     * @param v2 减数
     * @return 两个参数的差
     */
    public String sub(String v1, String v2)
    {
//        v1 = null2zero(v1);
//        v2 = null2zero(v2);
        if(v1==null || v2==null || "".equals(v1) || "".equals(v2)  || "null".equals(v1) || "null".equals(v2))
        {
            return null;
        }
        else
        {
            BigDecimal b1 = new BigDecimal(v1);
            BigDecimal b2 = new BigDecimal(v2);
            return b1.subtract(b2).toString();
        }
    }

    /**
     * 提供精确的减法运算
     * @param params
     * @return 传入参数 下标为0,与其他参数的差
     */
    public String sub2(String[] params)
    {
        String v0 = null2zero(params[0]);
        BigDecimal b = new BigDecimal(v0);
        for (int i = 1; i < params.length; i++)
        {
            String v1 = null2zero(params[i]);
            b = b.subtract(new BigDecimal(v1));
        }
        return b.toString();
    }

    /**
     * 提供精确的乘法运算
     * @param v1 被乘
     * @param v2 乘数
     * @return 两个参数的积
     */
    public String mul(String v1, String v2)
    {
//        v1 = null2zero(v1);
//        v2 = null2zero(v2);
        if(v1==null || v2==null || "".equals(v1) || "".equals(v2) || "null".equals(v1) || "null".equals(v2) )
        {

            return null;
        }
        else
        {

            BigDecimal b1 = new BigDecimal(v1);
            BigDecimal b2 = new BigDecimal(v2);
            return b1.multiply(b2).toString();
        }
    }

    /**
     * DESC:提供精确的乘法运算
     * @param params
     * @return 传入参数的积
     */
    public String mul2(String[] params)
    {
        String v0 = null2zero(params[0]);
        BigDecimal b1 = new BigDecimal(v0);
        for (int i = 1; i < params.length; i++)
        {
            String v1 = null2zero(params[i]);
            b1 = b1.multiply(new BigDecimal(v1));
        }
        return b1.toString();
    }

    /**
     * 提供（相对）精确的除法运算 当发生除不尽的情况时，由scale参数
     * 定精度，以后的数字四舍五入
     * @param v1 被除
     * @param v2 除数
     * @param scale 表示表示 精确到小数点以后几位
     * @return 两个参数的商
     */
    public String div(String v1, String v2, int scale)
    {
        if (scale < 0)
        {
            throw new IllegalArgumentException("The scale must be a positive integer or zero");
        }
//        v1 = null2zero(v1);
//        v2 = null2zero(v2);
        if(v1==null || v2==null || "".equals(v1) || "".equals(v2)  || "null".equals(v1) || "null".equals(v2))
        {
            return null;
        }
        else
        {
            try
            {
                BigDecimal b1 = new BigDecimal(v1);
                BigDecimal b2 = new BigDecimal(v2);
                String result = b1.divide(b2, scale, BigDecimal.ROUND_HALF_UP).toString();
                return result;
            }
            catch(Exception e)
            {
                return null;
            }
        }
    }

}
