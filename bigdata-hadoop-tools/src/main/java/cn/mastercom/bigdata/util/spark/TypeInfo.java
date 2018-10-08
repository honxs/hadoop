package cn.mastercom.bigdata.util.spark;

import java.io.Serializable;

public class TypeInfo implements Comparable<TypeInfo>, Serializable
{
	private static final long serialVersionUID = 1L;
	
	protected Integer type;
	protected String typeName;
	protected String typePath;

	public Integer getType()
	{
		return type;
	}

	public String getTypeName()
	{
		return typeName;
	}

	public String getTypePath()
	{
		return typePath;
	}

	public TypeInfo(int type, String typeName, String typePath)
	{
		this.type = type;
		this.typeName = typeName;
		this.typePath = typePath;
	}
	
	// 这个方法需要Overrride
	@Override
	public int hashCode()
	{
		return type.hashCode();
	}

	@Override
	public String toString()
	{
		return typeName;
	}

	@Override
	public int compareTo(TypeInfo o)
	{
		return type - o.type;
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

		if (obj instanceof TypeInfo)
		{
			TypeInfo s = (TypeInfo) obj;

			return type == s.type;
		}
		else
		{
			return false;
		}
	}
}
