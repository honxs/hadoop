package cn.mastercom.bigdata.locuser;

//add by yht 定位中间结果
public class LocSpliceMidInfo
{
    public void init()
    {
        ilongitude = 0;
        ilatitude = 0;
        ieci = 0;
        isection_btime = 0;
        is1apid = 0;
        ibuildingid = 0;
        ilevel = 0;
    }
    public int isection_btime = 0; //3-1 用户判断是否为一个section
    public int ieci = 0;//3-2 用户判断是否为一个section
    public long is1apid = 0;//3-3 用户判断是否为一个section
    public int ibuildingid = 0;//上一次定位的经纬度
    public int ilongitude = 0;//上一次定位的经纬度
    public int ilatitude = 0; //上一次定位的经纬度
    public int ilevel = -1;
    public void CopyLoc(LocSpliceMidInfo d)
    {
        this.ilongitude = d.ilongitude;
        this.ilatitude = d.ilatitude;
        this.ieci = d.ieci;
        this.is1apid = d.is1apid;
        this.isection_btime = d.isection_btime;
        this.ibuildingid = d.ibuildingid;
        this.ilevel = d.ilevel;
    }
}
