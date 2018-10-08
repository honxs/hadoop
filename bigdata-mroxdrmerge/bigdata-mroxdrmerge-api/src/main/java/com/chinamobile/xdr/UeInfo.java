package com.chinamobile.xdr;

public class UeInfo
{

    public long frameIndex;

    public String cgi;
    public String tmsi;

    @Override
    public String toString()
    {
        return "UeInfo{" +
                "frameIndex=" + frameIndex +
                ", cgi='" + cgi + '\'' +
                ", tmsi='" + tmsi + '\'' +
                '}';
    }
}
