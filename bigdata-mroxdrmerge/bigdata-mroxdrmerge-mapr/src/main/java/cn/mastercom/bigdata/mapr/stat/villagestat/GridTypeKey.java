package cn.mastercom.bigdata.mapr.stat.villagestat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class GridTypeKey implements WritableComparable<GridTypeKey>
{
	private int tllong = 0;
	private int tllat = 0;
	private int dataType = 0;// 1 village mark ;100 mrodata

	// 要写一个默认构造函数，否则MapReduce的反射机制，无法创建该类报错
	public GridTypeKey()
	{
	}

	/**
	 *
	 * @param eci
	 * @param timeSpan
	 */
	public GridTypeKey(int tllong, int tllat, int dataType)
	{
		super();
		
		this.tllong = tllong;
		this.tllat = tllat;
		this.dataType = dataType;
	}

	public int getTllong()
	{
		return tllong;
	}
	
	public int getTllat()
	{
		return tllat;
	}

	public int getDataType()
	{
		return dataType;
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeInt(this.tllong);
		out.writeInt(this.tllat);
		out.writeInt(this.dataType);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		this.tllong = in.readInt();
		this.tllat = in.readInt();
		this.dataType = in.readInt();
	}

	/**
	 * We want sort in descending count and descending avgts，
	 * Java里面排序默认小的放前面，即返回-1的放前面，这里直接把小值返回1，就会被排序到后面了。
	 * 
	 * Mro Data late about 20m to XDR Data
	 * 
	 */
	@Override
	public int compareTo(GridTypeKey o)
	{
		if (tllong > o.getTllong())
		{
			return 1;
		}
		else if (tllong < o.getTllong())
		{
			return -1;
		}
		else
		{
			if (tllat > o.getTllat())
			{
				return 1;
			}
			else if (tllat < o.getTllat())
			{
				return -1;
			}
			else
			{
				if (dataType > o.getDataType())
				{
					return 1;
				}
				else if (dataType < o.getDataType())
				{
					return -1;
				}
				else
				{
					return 0;
				}
			}
		}
	}

	// 这个方法需要Overrride
	@Override
	public int hashCode()
	{
		return toString().hashCode();
	}

	@Override
	public String toString()
	{
		return tllong + "_" + tllat + "_" + dataType;
	}

	// 这个方法，写不写都不会影响的，至少我测的是这样
	@Override
	public boolean equals(Object obj)
	{
		if (obj == null)
		{
			return false;
		}
		if (this == obj)
		{
			return true;
		}

		if (obj instanceof GridTypeKey)
		{
			GridTypeKey s = (GridTypeKey) obj;

			return tllat == s.getTllat() && tllong == s.getTllong() && dataType == s.getDataType();
		}
		else
		{
			return false;
		}
	}

}
