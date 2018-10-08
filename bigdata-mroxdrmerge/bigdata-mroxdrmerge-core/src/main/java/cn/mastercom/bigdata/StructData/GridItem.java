package cn.mastercom.bigdata.StructData;

public class GridItem
{
	private int cityid;
	private int tllongitude;
	private int tllatitude;
	private int brlongitude;
	private int brlatitude;

	private GridItem(int cityid, int tllongitude, int tllatitude)
	{
		this.cityid = cityid;
		this.tllongitude = tllongitude;
		this.tllatitude = tllatitude;
		this.brlongitude = tllongitude + 4000;
		this.brlatitude = tllatitude - 3600;
	}

	private GridItem(int cityid, int tllongitude, int tllatitude, int type)
	{
		this.cityid = cityid;
		this.tllongitude = tllongitude;
		this.tllatitude = tllatitude;
		this.brlongitude = tllongitude + 1000;
		this.brlatitude = tllatitude - 900;
	}

	public static GridItem GetGridItem(int cityid, int longitude, int latitude)
	{
		int tllongitude = (int) ((long) longitude / 4000 * 4000);
		int tllatitude = (int) ((long) latitude / 3600 * 3600 + 3600);
		GridItem girdItem = new GridItem(cityid, tllongitude, tllatitude);
		return girdItem;
	}

	public static GridItem Get_ten_GridItem(int cityid, int longitude, int latitude)
	{
		int tllongitude = (int) ((long) longitude / 1000 * 1000);
		int tllatitude = (int) ((long) latitude / 900 * 900 + 900);
		GridItem girdItem = new GridItem(cityid, tllongitude, tllatitude, 10);
		return girdItem;
	}

	public int getCityid()
	{
		return cityid;
	}

	public int getTLLongitude()
	{
		return tllongitude;
	}

	public int getTLLatitude()
	{
		return tllatitude;
	}

	public int getBRLongitude()
	{
		return brlongitude;
	}

	public int getBRLatitude()
	{
		return brlatitude;
	}

	@Override
	public boolean equals(Object o)
	{
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		GridItem item = (GridItem) o;

		if (cityid == item.cityid && tllongitude == item.getTLLongitude() && tllatitude == item.getTLLatitude())
			return true;

		return false;
	}

	@Override
	public int hashCode()
	{
		return toString().hashCode();
	}

	@Override
	public String toString()
	{
		return cityid + "," + tllongitude + "," + tllatitude;
	}

}
