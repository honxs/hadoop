package cn.mastercom.bigdata.xdr.loc;

public class CellTimeKey
{
	public long eci;
	public long dayhour;

	public CellTimeKey()
	{
	}

	public CellTimeKey(long eci, long dayhour)
	{
		this.eci = eci;
		this.dayhour = dayhour;
	}

	public String toString()
	{
		return eci + "_" + dayhour;
	}

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
		else if (obj == this)
		{
			return true;
		}
		else
		{
			CellTimeKey temp = (CellTimeKey) obj;
			return this.eci == temp.eci && this.dayhour == temp.dayhour;
		}
	}

}
