package cn.mastercom.bigdata.spark.mergestat;


import java.io.Serializable;

public class MergeTypeItem implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	public String key;
	public int type;

	public MergeTypeItem(String key, int type)
	{
		this.key = key;
		this.type = type;
	}
	
	public int getType()
	{
		return type;
	}
	
	public String getKey()
	{
		return key;
	}

	@Override
	public boolean equals(Object o)
	{
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		MergeTypeItem item = (MergeTypeItem) o;

		if (key.equals(item.key) && type == item.type)
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
		return key + "," + type;
	}
}
