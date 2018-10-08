package cn.mastercom.bigdata.loc.userResident;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class UserResidentKey implements WritableComparable<UserResidentKey>
{
	private long eci;
	private int dataType; // 数据类型 0.UserResident 1.MroLocLib

	public UserResidentKey()
	{

	}

	public UserResidentKey(long eci, int dataType)
	{
		this.eci = eci;
		this.dataType = dataType;
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
		this.eci = in.readLong();
		this.dataType = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeLong(this.eci);
		out.writeInt(this.dataType);
	}

	@Override
	public int compareTo(UserResidentKey o)
	{
		// TODO Auto-generated method stub

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

	public String toString()
	{
		return eci + "," + dataType;
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
		else if (obj instanceof UserResidentKey)
		{
			UserResidentKey s = (UserResidentKey) obj;
			return eci == s.eci && dataType == s.dataType;
		}
		else
		{
			return false;
		}
	}
}
