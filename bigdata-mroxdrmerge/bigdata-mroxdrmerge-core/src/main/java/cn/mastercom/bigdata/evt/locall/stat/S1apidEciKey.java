package cn.mastercom.bigdata.evt.locall.stat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class S1apidEciKey implements WritableComparable<S1apidEciKey>
{
	// ZhaiKaiShun
	private long s1apid;
	private long eci;
	private int dataType;// 1 locinfo; 2 s1u-http

	public long getS1apid()
	{
		return s1apid;
	}

	public void setS1apid(long s1apid)
	{
		this.s1apid = s1apid;
	}

	public long getEci()
	{
		return eci;
	}
	
	public int getDataType()
	{
		return dataType;
	}

	// 要写一个默认构造函数，否则MapReduce的反射机制，无法创建该类报错
	public S1apidEciKey()
	{
	}

	public S1apidEciKey(long s1apid, long eci, int dataType)
	{
		this.s1apid = s1apid;
		this.eci = eci;
		this.dataType = dataType;
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeLong(this.s1apid);
		out.writeLong(this.eci);
		out.writeInt(this.dataType);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		this.s1apid = in.readLong();
		this.eci = in.readLong();
		this.dataType = in.readInt();
	}

	@Override
	public int hashCode()
	{
		return toString().hashCode();
	}

	@Override
	public String toString()
	{
		return s1apid + "_" + eci + "_" + dataType;
	}

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

		if (obj instanceof S1apidEciKey)
		{
			S1apidEciKey s = (S1apidEciKey) obj;

			return s1apid == s.getS1apid() && eci==s.getEci() && dataType == s.getDataType();
		}
		else
		{
			return false;
		}
	}

	// 对比到10分钟粒度就可以了，不用太细
	@Override
	public int compareTo(S1apidEciKey o)
	{
		if (s1apid > o.s1apid)
		{
			return 1;
		}
		else if (s1apid < o.s1apid)
		{
			return -1;
		}
		else
		{
			if (eci>o.eci)
			{
				return 1;
			}
			else if (eci<o.eci)
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
				return 0;
			}

		}
	}
}
