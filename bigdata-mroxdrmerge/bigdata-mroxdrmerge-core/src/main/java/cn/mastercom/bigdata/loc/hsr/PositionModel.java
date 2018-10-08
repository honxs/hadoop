package cn.mastercom.bigdata.loc.hsr;

public class PositionModel
{

    public PositionModel() { }
    public PositionModel(double x, double y)
    {
        this.lng = x;
        this.lat = y;
    }
    /// <summary>
    /// 经度
    /// </summary>
    public double lng;
    /// <summary>
    /// 纬度
    /// </summary>
    public double lat;
    /// <summary>
    /// 区间ID
    /// </summary>
    public int sectionid;
    /// <summary>
    /// 路段ID
    /// </summary>
    public int segid;
    /// <summary>
    /// 线路ID
    /// </summary>
    public int lineid;
	
	
	public long trainKey;
}
