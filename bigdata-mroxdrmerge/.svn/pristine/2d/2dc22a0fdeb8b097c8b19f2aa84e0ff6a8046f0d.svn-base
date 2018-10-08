package cn.mastercom.bigdata.stat.freqloc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class FreqLocKey implements WritableComparable<FreqLocKey>
{
	public int cityId;
	public int freq;
	public int pci;
	
	public int getCityId()
	{
		return cityId;
	}
	public void setCityId(int cityId)
	{
		this.cityId = cityId;
	}
	public int getFreq()
	{
		return freq;
	}
	public void setFreq(int freq)
	{
		this.freq = freq;
	}
	public int getPci()
	{
		return pci;
	}
	public void setPci(int pci)
	{
		this.pci = pci;
	}
	
	public FreqLocKey()
	{
		
	}
	
	/**
	 * 
	 * @param freq
	 * @param pci
	 */
	public FreqLocKey(int cityId, int freq, int pci)
	{
		super();
		this.cityId = cityId;
		this.freq = freq;
		this.pci = pci;
	}
	
	@Override
	public int compareTo(FreqLocKey o)
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
			if (this.freq > o.freq)
			{
				return 1;
			}
			else if (this.freq < o.freq)
			{
				return -1;
			}
			else
			{
				if (this.pci > o.pci)
				{
					return 1;
				}
				else if (this.pci < o.pci)
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
		return cityId + "," +freq + "," + pci;
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
		else if (obj instanceof FreqLocKey)
		{
			FreqLocKey s = (FreqLocKey) obj;
			return cityId == s.cityId && freq == s.freq && pci == s.pci;
		}
		else
		{
			return false;
		}
	}
	
	@Override
	public void readFields(DataInput in) throws IOException
	{
		this.cityId = in.readInt();
		this.freq = in.readInt();
		this.pci = in.readInt();
	}
	
	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeInt(this.cityId);
		out.writeInt(this.freq);
		out.writeInt(this.pci);
	}
	
}
