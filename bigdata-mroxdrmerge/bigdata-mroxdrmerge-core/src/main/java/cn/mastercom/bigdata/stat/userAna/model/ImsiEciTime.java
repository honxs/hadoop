package cn.mastercom.bigdata.stat.userAna.model;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

/*
 * 用户在同一个小区下出现的时间点
 */
public class ImsiEciTime
{
    public String imsi;
    public long timeMin;
    public long timeMax;
    public Set<Long> timeSet;
    
    public ImsiEciTime(Xdr_ImsiEciTime xdr)
    {
        timeSet = new TreeSet<Long>();
        
        imsi = xdr.imsi;
        timeMin = xdr.time;
        timeMax = timeMin;
        timeSet.add(timeMin);
    }
    
    public void stat(Xdr_ImsiEciTime xdr)
    {
        long time = xdr.time;
        if (timeMin > time)timeMin = time;
        if (timeMax < time)timeMax = time;
        timeSet.add(time);
    }
   
}
