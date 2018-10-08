package cn.mastercom.bigdata.locuser;

public class Locmidinfo
{
    public void init() {
        iSameBtsHitCount = 0;
        iHitCellCount = 0;
        iSvsMLessF5 = 0;
        emdist = 1000;
    }
    public int iSameBtsHitCount = 0; //同站的信号hit个数
    public int iHitCellCount = 0;  //所有的hit小区数
    public int iSvsMLessF5 = 1000; //仿真比MR小于-5dB的个数
    public double emdist = 1000; //计算出来的匹配距离

    public void CopyLoc(Locmidinfo d)
    {
        this.iSameBtsHitCount = d.iSameBtsHitCount;
        this.iHitCellCount = d.iHitCellCount;
        this.iSvsMLessF5 = d.iSvsMLessF5;
        this.emdist = d.emdist;
    }
}
//add by yht end
