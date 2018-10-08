package com.chinamobile.util;

/**
 * Created by Zhou Xingwei on 2015-12-26.
 */


public class RunStringReg
{
    private static CalculatorUtil ps = new CalculatorUtil();

    /**
     * DESC：判断表达式是否有括号，有括号，则先计算括号内的表达式，无则直接运算结果。
     * @param str
     * @return 运算结果
     */
    public static String cacComplex(String str) {
        if (str.equals(""))
            return null;
        int has = str.indexOf("[");
        int have = str.indexOf("{");
        //System.out.println("RUN BEFORE:"+str);
        if(has!=-1||have!=-1){
            str = str.replaceAll("[\\[\\{]", "(").replaceAll("[\\]\\}]", ")");//将字符串中的"{}"、"[]"替换成"()"
            System.out.println("TURN '{,[':" + str);
        }
        int cl = str.lastIndexOf('(');//判断字符串中是否存在"("
        if (cl == -1)
            return cac(str);//不存在"("直接处理运行表达式
        int cr = str.indexOf(')', cl);
        String left = str.substring(0, cl);
        String right = str.substring(cr + 1);
        String middle = str.substring(cl + 1, cr);//如果存在"("提取括号中的表达式
        return cacComplex(left + cac(middle) + right);
    }

    /**
     * DESC:计算表达式，判断是否存在乘除运算，存在则先执行乘除运算，然后执行加减运算，返回运算结果；
     * 不存在，直接运行加减运算，返回运算结果。
     * @param str
     * @return 运算结果
     */
    private static String cac(String str) {
        if(str==null || "null".equals(str) || str.equals(""))
        {
            return null;
        }
        int ml = str.indexOf('*');
        int dl = str.indexOf('/');
        if (ml == -1 && dl == -1) {
            return cacNoMD(str);
        }
        int index = ml == -1 ? dl : ml;
        String left = str.substring(0, index);
        String m1 = lastNumber(left);
        left = left.substring(0, left.length() - m1.length());
        String right = str.substring(index + 1);
        String m2 = firstNumber(right);
        right = right.substring(m2.length());
        String tmp = "0";
        if (index == ml) {
            tmp = ps.mul(m1, m2);
        } else if (index == dl) {
            tmp = ps.div(m1, m2, 4);
        }
        return cac(left + tmp + right);
    }
    /**
     * Description:获得表达式的最后一位数字
     * @param str
     * @return 表达式最后一位数字
     */
    private static String lastNumber(String str) {
        StringBuilder sb = new StringBuilder();
        for (int i = str.length() - 1; i >= 0; i--) {
            char c = str.charAt(i);
            if (!Character.isDigit(c) && c != '.')
                break;
            sb.append(c);
        }
        return sb.reverse().toString();
    }

    /**
     * DESC:获得表达式的的一位数字
     * @param str
     * @return 表达式的第一位数字
     */
    private static String firstNumber(String str) {
        StringBuilder sb = new StringBuilder();
        if(str.startsWith("-"))
        {
            for (char c : str.toCharArray()) {
                if (!Character.isDigit(c) && c != '.' && c != '-')
                    break;
                sb.append(c);
            }
        }
        else
        {
            for (char c : str.toCharArray()) {
                if (!Character.isDigit(c) && c != '.')
                    break;
                sb.append(c);
            }
        }

        return sb.toString();
    }

    /**
     * DESC:加减运算表达式计算
     * @param str
     * @return 计算结果
     */
    private static String cacNoMD(String str) {
        if(str.contains("null"))
        {
            return null;
        }
        String ret = "0";
        StringBuilder sb = new StringBuilder();
        char sign = '+';
        if(str.charAt(0)=='-')
        {
            sign = '-';
        }
        for (char c : (str + "+").toCharArray()) {
            if (!Character.isDigit(c) && c != '.') {
                if (sb.length() == 0)
                    continue;
                if (sign == '+') {
                    ret = ps.add(ret, sb.toString());
                } else {
                    ret = ps.sub(ret, sb.toString());
                }
                sb = new StringBuilder();
                sign = c;
            } else {
                sb.append(c);
            }
        }
        return ret;
    }
}
