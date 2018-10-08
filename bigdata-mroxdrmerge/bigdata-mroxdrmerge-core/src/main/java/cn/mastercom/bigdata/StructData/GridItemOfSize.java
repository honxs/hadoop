package cn.mastercom.bigdata.StructData;

public class GridItemOfSize
{
	public int cityid;
	public int tllongitude;
	public int tllatitude;
	public int brlongitude;
	public int brlatitude;

	public GridItemOfSize(int cityid, int longitude, int latitude, int size)
	{
		this.cityid = cityid;
		this.tllongitude = longitude / (100 * size) * (100 * size);
		this.tllatitude = latitude / (90 * size) * (90 * size) + (90 * size);
		this.brlongitude = longitude / (100 * size) * (100 * size) + (100 * size);
		this.brlatitude = latitude / (90 * size) * (90 * size);
	}

}
