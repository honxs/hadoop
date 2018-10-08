package com.chinamobile.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;

/**
 * Created by Zhou Xingwei on 2015-10-09.
 */


public class StringUtil
{

    public static String reverse(String s)
    {
        char[] array = s.toCharArray();
        String reverse = "";
        for (int i = array.length - 1; i >= 0; i--)
            reverse += array[i];
        return reverse;
    }

    public static boolean isNum(String s){
        try {
            if(s.contains("."))
            {
                Double.valueOf(s);//把字符串强制转换为数字
            }
            else
            {
                Long.valueOf(s);
            }
            return true;//如果是数字，返回True
        } catch (Exception e) {
            return false;//如果抛出异常，返回False
        }
    }

    //按照字典表顺序替换originalString中的字符
    public static String ReplaceString(String originalString,
                                        ArrayList<String[]> replaceStringList)
    {
        for (String[] tmpregex : replaceStringList)
        {
            String oldString = tmpregex[0];
            String newString = tmpregex[1];
            originalString = originalString.replace(oldString, newString);
        }
        return originalString;
    }

    /**
     *  Convert byte[] to hex string
     * @param src
     * @return
     */
    public static String bytesToHexString(byte[] src){
        StringBuilder stringBuilder = new StringBuilder("");
        if(src==null||src.length<=0){
            return null;
        }
        for (int i = 0; i < src.length; i++) {
            int v = src[i] & 0xFF;
            String hv = Integer.toHexString(v);
            if (hv.length() < 2) {
                stringBuilder.append(0);
            }
            stringBuilder.append(hv+"");
        }
        return stringBuilder.toString();
    }

    public static long hexStringToLong(String hexString) {
    	byte[] byt = hexStringToBytes(hexString);
    	long ret = Bytes2Long(byt);
    	return ret;
    }
    
    
    /**
     * Convert hex string to byte[]
     * @param hexString
     * @return
     */
    public static byte[] hexStringToBytes(String hexString) {
        if (hexString == null || hexString.equals("")) {
        	byte[] bts = new byte[1];
        	bts[0] = 0;
            return bts;
        }
        hexString = hexString.replace(" ", "");
        if(hexString.length() %2 == 1)
        	hexString = '0' + hexString;

        hexString = hexString.toUpperCase();
        int length = hexString.length() / 2;
        char[] hexChars = hexString.toCharArray();
        byte[] d = new byte[length];
        for (int i = 0; i < length; i++) {
            int pos = i * 2;
            d[i] = (byte) (charToByte(hexChars[pos]) << 4 | charToByte(hexChars[pos + 1]));
        }
        return d;
    }

    /**
     * Convert char to byte
     * @param c
     * @return
     */
    private static byte charToByte(char c) {
        return (byte) "0123456789ABCDEF".indexOf(c);
    }

    /**
     * digits位字节数组转换为整型
     * @param b
     * @return
     */
    public static int byte2Int(byte[] b, int digits) {
        int intValue = 0;
        for (int i = 0; i < b.length; i++) {
            intValue += (b[i] & 0xFF) << (8 * (digits-1 - i));
        }
        return intValue;
    }


    /**
     *byte数组转为long类型
     * 左边为高位
     */
    public static long AnyBytes2Long(byte arr[],int digits)
    {
        long result = 0;
        for(int i=0; i<digits; i++)
        {
            result += (long)(arr[i]&0xFF) << (digits-i-1)*8;
        }

        return result;
    }

    /**
     *byte数组转为long类型
     * 左边为高位
     */
    public static long Bytes2Long(byte arr[])
    {  	
		if(arr.length<4)
		{
	    	long ret = 0;
	    	if(arr.length>=1)
	    	{
	    		ret += arr[arr.length-1] & 0xFF;
	    	}
	    	if(arr.length>=2)
	    	{
	    		ret += (long)(arr[arr.length-2]&0xFF) << 8;
	    	}    
	    	if(arr.length>=3)
	    	{
	    		ret += (long)(arr[arr.length-3]&0xFF) << 16;
	    	}  
	    	return ret;
    	}
        return ((long)(arr[0]&0xFF) << 24)
                + ((long)(arr[1]&0xFF) << 16)
                + ((long)(arr[2]&0xFF) << 8)
                + ((long)(arr[3]&0xFF)) ;
    }

    /**
     *byte数组转为long类型
     * 右边为高位,逆序
     */
    public static long reverseByte2Long(byte arr[])
    {
        return ((long)(arr[3]&0xFF) << 24)
                + ((long)(arr[2]&0xFF) << 16)
                + ((long)(arr[1]&0xFF) << 8)
                + ((long)(arr[0]&0xFF)) ;
    }


    /**
     * long类型转为byte数组
     * @param num
     * @return
     */
    public static byte[] long2Bytes(long num)
    {
        byte[] byteNum = new byte[8];
        for(int ix=0; ix<8; ix++)
        {
            int offset = 64 - (ix+1)*8;
            byteNum[ix] = (byte) ((num>>offset) & 0xff);
        }
        return byteNum;
    }

    /**
     * 整型转换为4位字节数组
     * @param intValue
     * @return
     */
    public static byte[] int2Byte(int intValue) {
        byte[] b = new byte[4];
        for (int i = 0; i < 4; i++) {
            b[i] = (byte) (intValue >> 8 * (3 - i) & 0xFF);
            //System.out.print(Integer.toBinaryString(b[i])+" ");
            //System.out.print((b[i] & 0xFF) + " ");
        }
        return b;
    }

    /**
     * 整型转换为4位字节数组，逆序
     * @param intValue
     * @return
     */
    public static byte[] int2ReverseByte(int intValue) {
        byte[] b = new byte[4];
        for (int i = 0; i < 4; i++) {
            b[3-i] = (byte) (intValue >> 8 * (3 - i) & 0xFF);
            //System.out.print(Integer.toBinaryString(b[i])+" ");
            //System.out.print((b[i] & 0xFF) + " ");
        }
        return b;
    }


    public static long getUnSignedLong(long l)
    {
        return getLong(longToDword(l),0);
    }

    private static byte[] longToDword(long value)
    {
        byte[] data = new byte[4];
        for(int i=0; i<data.length; i++)
        {
            data[i] = (byte) (value >> (8*i));
        }
        return data;
    }

    private static long getLong(byte buf[], int index)
    {

        int firstByte = (0x000000FF & ((int) buf[index]));
        int secondByte = (0x000000FF & ((int) buf[index + 1]));
        int thirdByte = (0x000000FF & ((int) buf[index + 2]));
        int fourthByte = (0x000000FF & ((int) buf[index + 3]));

        long unsignedLong = ((long) (firstByte | secondByte << 8 | thirdByte << 16 | fourthByte << 24)) & 0xFFFFFFFFL;

        return unsignedLong;
    }

    /**
     * 调反byte顺序
     */
    public static byte[] reverseBytes(byte[] originalBytes, int length)
    {
        byte[] reversedBytes = new byte[length];

        for(int i=0; i<length; i++)
        {
            reversedBytes[i] = originalBytes[length-1-i];
        }

        return reversedBytes;
    }

    
	public static void main(String[] args)
    {    	   	
		String hex = "af01234501";
		long eci = StringUtil.hexStringToLong(hex.substring(0,7));
		System.out.println(eci);
    }

    /**
     * 判断byte[]中是否含有某个字符串，返回索引
     */
    public static int indexOf(byte[] bytes, String str)
    {
        int index = -1;
        byte[] strBytes = str.getBytes();
        if(bytes!=null && bytes.length>strBytes.length)
        {
            for(int i=0; i+strBytes.length<=bytes.length; i++)
            {
                byte[] tmp = Arrays.copyOfRange(bytes, i, i + strBytes.length);
                if(Arrays.equals(tmp,strBytes))
                {
                    index = i;
                    break;
                }
            }
        }
        return index;

    }

    /**
     * 合并两个byte数组
     * @param byte_1
     * @param byte_2
     * @return
     */

    public static byte[] byteMerger(byte[] byte_1, byte[] byte_2){
        byte[] byte_3 = new byte[byte_1.length+byte_2.length];
        System.arraycopy(byte_1, 0, byte_3, 0, byte_1.length);
        System.arraycopy(byte_2, 0, byte_3, byte_1.length, byte_2.length);
        return byte_3;
    }


    public static byte[] gunzip(byte[] zipBytes)
    {
        try
        {
            byte[] buffer = new byte[0x1000];
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            InputStream is = new ByteArrayInputStream(zipBytes);
            GZIPInputStream in = null;
            in = new GZIPInputStream(is);
            byte[] buf = new byte[1024];
            int len;

            if (in != null)
            {
                while ((len = in.read(buf)) > 0)
                {
                    out.write(buf, 0, len);
                }

                buffer = out.toByteArray();

                in.close();
                is.close();
                out.close();
            }
            return buffer;

        } catch (Exception e)
        {
            return null;
        }
    }

    public static String Bytes2IP(byte[] ipBytes)
    {
        if(ipBytes.length!=4)
        {
            return null;
        }
        return (int)(ipBytes[0]&0xFF)+"."
                +(int)(ipBytes[1]&0xFF)+"."
                +(int)(ipBytes[2]&0xFF)+"."
                +(int)(ipBytes[3]&0xFF);
    }

    /**
     * url的utf解码类
     * @param uri
     * @return
     * @throws java.io.UnsupportedEncodingException
     */
    public static String UrlReplacePercentage(String uri)  {
        try {
            return URLDecoder.decode(uri.replaceAll("%", "%25"), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static byte[] MappedByteBuffer2Bytes(MappedByteBuffer mappedByteBuffer, int size)
    {
        byte[] result = new byte[size];
        for(int i=0; i<size; i++)
        {
            result[i] = mappedByteBuffer.get(i);
        }

        return result;
    }

}
