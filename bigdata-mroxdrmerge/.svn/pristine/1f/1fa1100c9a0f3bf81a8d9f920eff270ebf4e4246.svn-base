package cn.mastercom.bigdata.mergestat.deal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MergeKey implements WritableComparable<MergeKey>
{
	private int dataType = 0;
    private String mapKey = "";
	
	public MergeKey()
	{
	}

	/**
	 *
	 * @param dataType
	 * @param mapKey
	 */
	public MergeKey(int dataType, String mapKey)
	{
		super();
		this.dataType = dataType;
		this.mapKey = mapKey;
	}

    public int getDataType()
	{
		return dataType;
	}
    
    public String getMapKey()
	{
		return mapKey;
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeInt(this.dataType);
		out.writeUTF(this.mapKey);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		this.dataType = in.readInt();
		this.mapKey = in.readUTF();
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
		return dataType + "_" + mapKey;
	}
	
	@Override
	public int compareTo(MergeKey o)
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
			return mapKey.compareTo(o.getMapKey());
		}
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

		if (obj instanceof MergeKey)
		{
			MergeKey s = (MergeKey) obj;

			return dataType == s.getDataType() &&  mapKey.equals(s.getMapKey());
		}
		else
		{
			return false;
		}
	}

}
