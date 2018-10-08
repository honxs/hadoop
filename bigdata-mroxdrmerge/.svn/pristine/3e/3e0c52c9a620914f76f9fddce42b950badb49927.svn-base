package com.chinamobile.xdr;

import com.chinamobile.util.NotProguard;

/**
 * Copyright © Zhou Xingwei. All Rights Reserved
 * Email: zhouxingwei@139.com
 * Function:
 * Usage:
 */
@NotProguard
public class TcpData
{

    public long frameIndex;
    public long frameTimeStamp;
    public long frameTimeStampSyn;
    public String outerSourceIP;
    public int outerSourcePort;
    public String outerTargetIP;
    public int outerTargetPort;

    public String innerSourceIP;
    public int innerSourcePort;
    public String innerTargetIP;
    public int innerTargetPort;

    public long sequenceNumber;
    public long nextSequenceNumber;

    public String sourceTEID;
    public String targetTEID;

    public byte[] payload;

    public boolean isStart;
    public boolean isEnd;
    
    public String host = "";
    public String url = "";

    public TcpData()
    {

        isStart = false;
        isEnd = false;
    }

    public TcpData(TcpData originalTcpData)
    {
        frameIndex = originalTcpData.frameIndex;
        frameTimeStamp = originalTcpData.frameTimeStamp;
        outerSourceIP = originalTcpData.outerSourceIP;
        outerSourcePort = originalTcpData.outerSourcePort;
        outerTargetIP = originalTcpData.outerTargetIP;
        outerTargetPort = originalTcpData.outerTargetPort;
        innerSourceIP = originalTcpData.innerSourceIP;
        innerSourcePort = originalTcpData.innerSourcePort;
        innerTargetIP = originalTcpData.innerTargetIP;
        innerTargetPort = originalTcpData.innerTargetPort;
        sequenceNumber = originalTcpData.sequenceNumber;
        nextSequenceNumber = originalTcpData.nextSequenceNumber;
        sourceTEID = originalTcpData.sourceTEID;
        targetTEID = originalTcpData.targetTEID;
        payload = originalTcpData.payload;
        isStart = originalTcpData.isStart;
        isEnd = originalTcpData.isEnd;
    }

//    @Override
//    public String toString()
//    {
//        //System.out.println(frameIndex);
//        return "TcpData{" +
//                "frameIndex=" + frameIndex +
//                ", frameTimeStamp='" + TimeUtil.getTimeStringFromMillis(frameTimeStamp, "yyyy-MM-dd HH:mm:ss.SSS") + '\'' +
//                ", outerSourceIP='" + outerSourceIP + '\'' +
//                ", outerSourcePort=" + outerSourcePort +
//                ", outerTargetIP='" + outerTargetIP + '\'' +
//                ", outerTargetPort=" + outerTargetPort +
//                ", innerSourceIP='" + innerSourceIP + '\'' +
//                ", innerSourcePort=" + innerSourcePort +
//                ", innerTargetIP='" + innerTargetIP + '\'' +
//                ", innerTargetPort=" + innerTargetPort +
//                ", sequenceNumber=" + sequenceNumber +
//                ", nextSequenceNumber=" + nextSequenceNumber +
//                ", TEID='" + TEID + '\'' +
//                //", payload=" + Arrays.toString(payload) +
//                ", payload=" + payload.length +
//                '}';
//    }

    @Override
    public String toString()
    {
        //System.out.println(frameIndex);
        return "TcpData{" +
                "frameIndex=" + frameIndex +
                ", sequenceNumber=" + sequenceNumber +
                ", payload=" + payload.length +
                '}';
    }
}
