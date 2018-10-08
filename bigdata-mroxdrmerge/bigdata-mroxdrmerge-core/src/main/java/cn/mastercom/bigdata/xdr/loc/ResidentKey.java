package cn.mastercom.bigdata.xdr.loc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ResidentKey implements WritableComparable<ResidentKey>
{
	public long imsi;
	public long dayhour; // 小时时间
	public long eci;

	public ResidentKey()
	{
	}

	/**
	 * 
	 * @param imsi
	 * @param daytime
	 * @param eci
	 */
	public ResidentKey(long imsi, long dayhour, long eci)
	{
		this.imsi = imsi;
		this.dayhour = dayhour;
		this.eci = eci;
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		this.imsi = in.readLong();
		this.dayhour = in.readLong();
		this.eci = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeLong(this.imsi);
		out.writeLong(this.dayhour);
		out.writeLong(this.eci);
	}

	@Override
	public int compareTo(ResidentKey o)
	{
		// TODO Auto-generated method stub
		if (this.imsi > o.imsi)
		{
			return 1;
		}
		else if (this.imsi < o.imsi)
		{
			return -1;
		}
		else
		{
			if (this.dayhour > o.dayhour)
			{
				return 1;
			}
			else if (this.dayhour < o.dayhour)
			{
				return -1;
			}
			else
			{
				if (this.eci > o.eci)
				{
					return 1;
				}
				else if (this.eci < o.eci)
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

	public String toString()
	{
		return imsi + "," + dayhour + "," + eci;
	}

	@Override
	public int hashCode()
	{
		return toString().hashCode();
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj == null)
		{
			return false;
		}
		else if (this == obj)
		{
			return true;
		}
		else if (obj instanceof ResidentKey)
		{
			ResidentKey s = (ResidentKey) obj;
			return imsi == s.imsi && dayhour == s.dayhour && eci == s.eci;
		}
		else
		{
			return false;
		}
	}
}
