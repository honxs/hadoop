package cn.mastercom.bigdata.locuser_v3;

import java.util.HashMap;
import java.util.Map;

public class MrSplice
{
    public int itime = 0;        // 聚合时间点
    public int section_btime = 0;// 段落开始时间
    public int section_etime = 0;// 段落结束时间
    public int splice_btime = 0; // 切片开始时间
    public int splice_etime = 0; // 切片结束时间
    public int cityid = 0;
    public int eci = 0; // 服务小区
    public long s1apid = 0;
    public int loctype = 0; // 1小区 2simu 3 室内random
    public int doortype = 0; // 0未知 1室内 2室外        
    public int buildingid = 0;
    public int ilevel = -1;
    public int longitude = 0;
    public int latitude = 0;
    public double grate = 0;
    public int gcount = 0;

    public int org_radius = 0;
    public String org_loctp = "";
    public int org_indr = 0;
    public String org_label = "";
    
    public int ta = -1;
    public int aoa = 0;
    public int naoa = 0;
    public int sinr = -1000000;
    public int sinr_c = 0;    
    public int isroad = 0;
    public int moving = 0;
    public int nout95 = 0;  // 邻区室外场强超过-95dB的邻区数
    public int iscenter = 0;
    
    public MrPoint nicell = null; // 邻区最强室分
    public MrPoint nocell = null; // 邻区最强室外
    public MrPoint scell = null; // 主服小区
    public PosInfo posinfo = null;
    public Map<Integer, MrPoint> ncells = new HashMap<Integer, MrPoint>(); // 不含主服
    
}
