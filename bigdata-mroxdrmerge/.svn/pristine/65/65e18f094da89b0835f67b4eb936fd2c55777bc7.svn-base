package com.chinamobile.util;

/**
 * Copyright © Zhou Xingwei. All Rights Reserved
 * Email: zhouxingwei@139.com
 * Function:
 * Usage:
 */
@NotProguard
public class ByteUtil
{
    public static String dec2hex(byte[] b)
    {
        String hex = null;
        for (int i = 0; i < b.length; i++)
        {
            hex = Integer.toHexString(b[i] & 0xFF);
            if (hex.length() == 1)
            {
                hex = '0' + hex;
            }
            System.out.print(hex.toUpperCase());
        }

        return hex;
    }
}
