package cn.mastercom.bigdata.stat.userResident.buildIndoorCell;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class BuildIndoorCellKey implements WritableComparable<BuildIndoorCellKey>
{
	public int cityId;
	public int buildId; // 楼宇id

	public BuildIndoorCellKey()
	{

	}

	public BuildIndoorCellKey(int cityId, int buildId)
	{
		this.cityId = cityId;
		this.buildId = buildId;
	}

	public int getBuildId() {
		return buildId;
	}

	public void setBuildId(int buildId) {
		this.buildId = buildId;
	}
	
	public int getCityId() {
		return cityId;
	}

	public void setCityId(int cityId) {
		this.cityId = cityId;
	}

	/**
	 * 
	 * @param imsi
	 * @param eci
	 */

	@Override
	public void readFields(DataInput in) throws IOException
	{
		this.cityId = in.readInt();
		this.buildId = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeInt(this.cityId);
		out.writeInt(this.buildId);
	}

	@Override
	public int compareTo(BuildIndoorCellKey o)
	{
		// TODO Auto-generated method stub
		if (this.cityId > o.cityId)
		{
			return 1;
		}
		else if (this.cityId < o.cityId)
		{
			return -1;
		}
		else
		{
			if (this.buildId > o.buildId)
			{
				return 1;
			}
			else if (this.buildId < o.buildId)
			{
				return -1;
			}
			else
			{
				return 0;
			}
		}
	}

	@Override
	public String toString()
	{
		return cityId + "" + buildId;
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
		else if (obj instanceof BuildIndoorCellKey)
		{
			BuildIndoorCellKey s = (BuildIndoorCellKey) obj;
			return buildId == s.buildId && cityId == s.cityId;
		}
		else
		{
			return false;
		}
	}
}
