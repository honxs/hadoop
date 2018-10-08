package cn.mastercom.bigdata.util;


public class GtpInfo
{

    public long _frameIndex = -1L;

    public long _frameTimestamp = 0L;

    public long _mmeS1ApId = -1L;
    public long _enbS1ApId = -1L;
    public String _cgi;
    public String _tmsi;
    public String _mmeGroupId;
    public String _mmeCode;

    @Override
    public String toString()
    {
        return "GtpInfo{" +
                "_frameIndex=" + _frameIndex +
                ", _frameTimestamp=" + _frameTimestamp +
                ", _mmeS1ApId=" + _mmeS1ApId +
                ", _enbS1ApId=" + _enbS1ApId +
                ", _cgi='" + _cgi + '\'' +
                ", _tmsi='" + _tmsi + '\'' +
                ", _mmeGroupId='" + _mmeGroupId + '\'' +
                ", _mmeCode='" + _mmeCode + '\'' +
                '}';
    }
}
