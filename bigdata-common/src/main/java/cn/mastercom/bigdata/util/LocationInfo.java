package cn.mastercom.bigdata.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class LocationInfo extends GtpInfo
{
    public long frameIndex;
    public long frameTimeStamp;
    public long frameTimeStampSyn;
    public String innerSourceIP;
    public int innerSourcePort;
    public String innerTargetIP;
    public int innerTargetPort;

    public String outerSourceIP;
    public int outerSourcePort;
    public String outerTargetIP;
    public int outerTargetPort;

    public String sourceTEID;
    public String targetTEID;

    public String requestType;    //GET POST 200OK
    public String host;       //HOST+post 
    public String url;
    public String locationType;     //gps wf cl
    public String coorType;
    public long timeStamp;
    public double longitude;
    public double latitude;
    public double radius;
    public String indoor;

    public String networkType;
    public String locCgi;
    public List<String> wifiList;
    public String imsi = "";

    public String amapstr = "";
    public String amapCifa = "";

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
    
    public static String getTimeStringFromMillis(long timeMillis, String timeFormat)
    {
    	Date date = new Date(timeMillis);
    	DateFormat dateFormat = new SimpleDateFormat(timeFormat);
        return dateFormat.format(date);
    }
    
   
    
    @Override
    public String toString()
    {
        return  "frameIndex=" + frameIndex +
                ", frameTimeStamp='" + getTimeStringFromMillis(frameTimeStamp, "yyyy-MM-dd HH:mm:ss.SSS") + '\'' +
                ", innerSourceIP='" + innerSourceIP + '\'' +
                ", innerSourcePort=" + innerSourcePort +
                ", innerTargetIP='" + innerTargetIP + '\'' +
                ", innerTargetPort=" + innerTargetPort +
                ", outerSourceIP='" + outerSourceIP + '\'' +
                ", outerSourcePort=" + outerSourcePort +
                ", outerTargetIP='" + outerTargetIP + '\'' +
                ", outerTargetPort=" + outerTargetPort +
                ", sourceTEID='" + sourceTEID + '\'' +
                ", targetTEID='" + targetTEID + '\'' +
                ", _enbS1APId='" + super._enbS1ApId + '\'' +
                ", _mmeS1APId='" + super._mmeS1ApId + '\'' +
                ", _cgi='" + super._cgi + '\'' +
                ", _TMSI='" + super._tmsi + '\'' +
                ", requestType='" + requestType + '\'' +
                ", host='" + host + '\'' +
                ", url='" + url + '\'' +
                ", locationType='" + locationType + '\'' +
                ", coorType='" + coorType + '\'' +
                ", timeStamp='" + getTimeStringFromMillis(timeStamp, "yyyy-MM-dd HH:mm:ss") + '\'' +
                ", longitude=" + longitude +
                ", latitude=" + latitude +
                ", radius=" + radius +
                ", indoor='" + indoor + '\'' +
                ", networkType='" + networkType + '\'' +
                ", locCgi='" + locCgi + '\'' +
                ", wifiList=" + wifiList +
                ", enodeB=" +getEnbId(super._cgi, 10) +
                '}';
    }
}
