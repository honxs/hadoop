package cn.mastercom.bigdata.stat.userResident.homeBroadbandLoc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MergeUserResidentKey implements WritableComparable<MergeUserResidentKey>
{
	public long imsi;
	public long eci;
	public int dataType; // 数据类型  0.UserResident // 1.OldUserResident

	public MergeUserResidentKey()
	{

	}

	public MergeUserResidentKey(long imsi, long eci, int dataType)
	{
		this.imsi = imsi;
		this.eci = eci;
		this.dataType = dataType;
	}

	public long getImsi()
	{
		return imsi;
	}

	public void setImsi(long imsi)
	{
		this.imsi = imsi;
	}
	
	public long getEci()
	{
		return eci;
	}

	public void setEci(long eci)
	{
		this.eci = eci;
	}

	public int getDataType()
	{
		return dataType;
	}

	public void setDataType(int dataType)
	{
		this.dataType = dataType;
	}

	/**
	 * 
	 * @param imsi
	 * @param eci
	 */

	@Override
	public void readFields(DataInput in) throws IOException
	{
		this.imsi = in.readLong();
		this.eci = in.readLong();
		this.dataType = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeLong(this.imsi);
		out.writeLong(this.eci);
		out.writeInt(this.dataType);
	}

	@Override
	public int compareTo(MergeUserResidentKey o)
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
				if (this.dataType > o.dataType)
				{
					return 1;
				}
				else if (this.dataType < o.dataType)
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
		return imsi + "," + eci + "," + dataType;
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
		else if (obj instanceof MergeUserResidentKey)
		{
			MergeUserResidentKey s = (MergeUserResidentKey) obj;
			return imsi == s.imsi && eci == s.eci && dataType == s.dataType;
		}
		else
		{
			return false;
		}
	}
}
