package cn.mastercom.bigdata.stat.imsifill.deal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ImsiIPKey implements WritableComparable<ImsiIPKey>
{
	private String userIP = "";
	private int userPort = 0;
	private String serverIP = "";
	private int dataType = -1;

	public ImsiIPKey()
	{
	}

	/**
	 *
	 * @param eci
	 * @param timeSpan
	 */
	public ImsiIPKey(String userIP, int userPort, String serverIP, int dataType)
	{
		super();

        this.userIP = userIP;
        this.userPort = userPort;
        this.serverIP = serverIP;
        this.dataType = dataType;
	}

    public String getUserIP()
	{
		return userIP;
	}
    
    public int getUserPort()
	{
		return userPort;
	}
    
    public String getServerIP()
	{
		return serverIP;
	}
    
    public int getDataType()
	{
		return dataType;
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeUTF(this.userIP);
		out.writeInt(this.userPort);
		out.writeUTF(this.serverIP);
		out.writeInt(this.dataType);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		this.userIP = in.readUTF();
		this.userPort = in.readInt();
		this.serverIP = in.readUTF();
		this.dataType = in.readInt();
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
		return userIP + "_" + userPort + "_" + serverIP + "_" + dataType;
	}
	
	@Override
	public int compareTo(ImsiIPKey o)
	{
		if (userIP.compareTo(o.getUserIP()) > 0)
		{
			return 1;
		}
		else if (userIP.compareTo(o.getUserIP()) < 0)
		{
			return -1;
		}
		else
		{
			if (userPort > o.getUserPort())
			{
				return 1;
			}
			else if (userPort < o.getUserPort())
			{
				return -1;
			}
			else
			{
				if (serverIP.compareTo(o.getServerIP()) > 0)
				{
					return 1;
				}
				else if (serverIP.compareTo(o.getServerIP()) < 0)
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

		if (obj instanceof ImsiIPKey)
		{
			ImsiIPKey s = (ImsiIPKey) obj;

			return userIP.equals(s.getUserIP()) 
					&& userPort == s.getUserPort()
					&& serverIP.equals(s.getServerIP())
					&& dataType == s.getDataType();
		}
		else
		{
			return false;
		}
	}

}
