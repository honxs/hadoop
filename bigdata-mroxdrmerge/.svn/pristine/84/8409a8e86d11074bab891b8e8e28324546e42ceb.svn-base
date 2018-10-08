package com.chinamobile.util;

import java.util.Arrays;

/**
 * Copyright © Zhou Xingwei. All Rights Reserved
 * Email: zhouxingwei@139.com
 * Function:
 * Usage:
 */
@NotProguard
public class SignallingFilterUtil
{

    public static boolean isValidS1UIPv4(byte[] currentBytes)
    {
        //!(tcp.srcport == 80) && !(tcp.srcport == 8080) && http contains "200 OK" && http contains "com"
        boolean isValid = false;
        if (currentBytes == null || currentBytes.length == 0)
        {
            return false;
        }

        if ((currentBytes[0] & 0xFF) == 0x45)
        {
            if (currentBytes.length <= 20)
            {
                return false;
            }
            //IPv4 Header Length = 20
            int ipLength = StringUtil.byte2Int(Arrays.copyOfRange(currentBytes, 2, 4), 2);
            byte protocolFlag = currentBytes[9];

            if(ipLength<20)
            {
                return false;
            }

            currentBytes = Arrays.copyOfRange(currentBytes, 20, ipLength);

            if ((protocolFlag & 0xFF) == 0x06)
            {
                //TCP Header Length = 20

                if (currentBytes.length <= 20)
                {
                    return false;
                }

                int innerSourcePort = StringUtil.byte2Int(new byte[]{currentBytes[0], currentBytes[1]}, 2);
                int innerTargetPort = StringUtil.byte2Int(new byte[]{currentBytes[2], currentBytes[3]}, 2);

                //TCP长度只能反推
                int TCPHeaderLen = (int)((currentBytes[12] & 0xFF)>>4)*4;

                if(currentBytes.length-TCPHeaderLen<32)
                {
                    return false;
                }

                if (innerSourcePort == 80 || innerSourcePort == 8080)
                {
                    if ((currentBytes[12] & 0xFF) == 0x50 && (currentBytes[13] & 0xFF) == 0x18)
                    {
                        return true;
                    } else
                    {
                        return false;
                    }
                } else if (innerTargetPort == 80 || innerTargetPort == 8080)
                {
                    return true;
                }
            }

        }

        return isValid;

    }

    public static boolean isValidDACK(byte[] currentBytes)
    {
        //Data Chunk header length = 16

        if (currentBytes.length < 16)
        {
            return false;
        }

        int datachunkLength = StringUtil.byte2Int(Arrays.copyOfRange(currentBytes, 2, 4), 2);

        //Data chunk to datagram tail
        currentBytes = Arrays.copyOfRange(currentBytes, 16, datachunkLength);
        return isValidS1APPDU(currentBytes);

    }

    public static boolean isValidS1APPDU(byte[] pdu)
    {
        if (pdu == null || pdu.length < 7)
        {
            return false;
        }

        int procedureCode = StringUtil.byte2Int(new byte[]{pdu[0], pdu[1]}, 2);

        if (procedureCode == 10)
        {
            return false;

        } else
        {
            return true;
        }
    }
}
