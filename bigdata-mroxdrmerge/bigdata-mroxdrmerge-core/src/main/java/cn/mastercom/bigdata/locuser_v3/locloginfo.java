package cn.mastercom.bigdata.locuser_v3;

public class locloginfo
{
    public int idoortype_err_2 = 0;//错误的室内外判断个数
    public int idoortype_err_1 = 0;//错误的室内外判断个数
    public int idoortype_err_0 = 0;//错误的室内外判断个数
    public int idoortype_err = 0;//错误的室内外判断个数
    public int iloctype_ott = 0; //聚类OTT起始
    public int iloctype_simu = 0; //聚类指纹起始
    public int iloctype_simuott = 0;//聚类有OTT转指纹起始

    public double iinitsimuspan = 0; //初始化指纹时间,毫秒
    public double ilocatetime = 0; //定位时间
    public double itrylocattime = 0; //尝试定位时间
    public int ilocatecnt = 0;  //汇聚定位次数
    public int itrylocatecnt = 0; //尝试定位次数
    public int iindoorcnt = 0; //室内个数
    public int ioutdoorcnt = 0;//室外个数
    public int iunknowncnt = 0;//未知个数
    public int iunlocated = 0;//未定位个数
    public int irandomloc = 0;//随机定位个数
}
