package cn.mastercom.bigdata.evt.locall.stat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ImsiKey implements WritableComparable<ImsiKey>
{
	private long imsi;
	private int dataType;// 1 locinfo; 2 s1u-http

	// 要写一个默认构造函数，否则MapReduce的反射机制，无法创建该类报错
	public ImsiKey()
	{
	}

	public ImsiKey(long imsi, int dataType)
	{
		this.imsi = imsi;
		this.dataType = dataType;
	}

	public long getImsi()
	{
		return imsi;
	}

	public int getDataType()
	{
		return dataType;
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeLong(this.imsi);
		out.writeInt(this.dataType);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		this.imsi = in.readLong();
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
		return imsi + "_" + dataType;
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

		if (obj instanceof ImsiKey)
		{
			ImsiKey s = (ImsiKey) obj;

			return imsi == s.getImsi() && dataType == s.getDataType();
		}
		else
		{
			return false;
		}
	}

	// 对比到10分钟粒度就可以了，不用太细
	@Override
	public int compareTo(ImsiKey o)
	{
		if (imsi > o.imsi)
		{
			return 1;
		}
		else if (imsi < o.imsi)
		{
			return -1;
		}
		else 
		{
			if(dataType > o.getDataType())
			{
				return 1;
			}
			else if(dataType < o.getDataType())
			{
				return -1;
			}
			return 0;
		}
	}
}
