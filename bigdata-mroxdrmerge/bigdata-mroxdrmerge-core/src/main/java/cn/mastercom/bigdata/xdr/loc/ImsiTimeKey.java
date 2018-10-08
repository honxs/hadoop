package cn.mastercom.bigdata.xdr.loc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class ImsiTimeKey implements WritableComparable<ImsiTimeKey>
{
	private long imsi = 0;
	private int time;
	private int timeSpan;
	private int dataType;//1 locinfo; 2 labelinfo; 3 xdrdata; 4 cpedata
	
    //要写一个默认构造函数，否则MapReduce的反射机制，无法创建该类报错
	public ImsiTimeKey() {}

	public ImsiTimeKey(long imsi, int time, int timeSpan, int dataType)
	{
		this.imsi = imsi;
		this.time = time;
		this.timeSpan = timeSpan;
		this.dataType = dataType;
	}

	public long getImsi()
	{
		return imsi;
	}

	public int getTime()
	{
		return time;
	}
	
	public int getTimeSpan()
	{
		return timeSpan;
	}
	
	public int getDataType()
	{
		return dataType;
	}

	public void setImsi(long imsi)
	{
		this.imsi = imsi;
	}

	public void setTime(int time)
	{
		this.time = time;
	}

	public void setTimeSpan(int timeSpan)
	{
		this.timeSpan = timeSpan;
	}

	public void setDataType(int dataType)
	{
		this.dataType = dataType;
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeLong(this.imsi);
		out.writeInt(this.time);
		out.writeInt(this.timeSpan);
		out.writeInt(this.dataType);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		this.imsi = in.readLong();
		this.time = in.readInt();
		this.timeSpan = in.readInt();
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
		return imsi + "_" + time + "_" + timeSpan + "_" + dataType;
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

		if (obj instanceof ImsiTimeKey)
		{
			ImsiTimeKey s = (ImsiTimeKey) obj;

			return imsi == s.getImsi() 
					&& time == s.getTime() 
					&& timeSpan == s.getTimeSpan() 
					&& dataType == s.getDataType();
		}
		else
		{
			return false;
		}
	}

	
	//对比到10分钟粒度就可以了，不用太细
	@Override
	public int compareTo(ImsiTimeKey o)
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
			if(timeSpan > o.getTimeSpan())
			{
				return 1;
			}
			else if (timeSpan < o.getTimeSpan())
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

}
