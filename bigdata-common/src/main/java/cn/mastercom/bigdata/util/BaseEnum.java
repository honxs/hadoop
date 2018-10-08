package cn.mastercom.bigdata.util;

import java.util.HashMap;
import java.util.Map;

public enum BaseEnum
{
	None("None", 0);

	protected static Map<Integer, BaseEnum> keyMap;

	static
	{
		keyMap = new HashMap<Integer, BaseEnum>();

		for (BaseEnum c : BaseEnum.values())
		{
			keyMap.put(c.getValue(), c);
		}
	}
	
	public static BaseEnum GetEnum(int value)
	{
		if(keyMap.containsKey(value))
		{
			return keyMap.get(value);
		}
		return None;
	}

	protected final String name;
	protected final int value;

    BaseEnum(String name, int value)
	{
		this.name = name;
		this.value = value;
	}
	
	public String getName()
	{
		return name;
	}

	public int getValue()
	{
		return value;
	}

	@Override
	public String toString()
	{
		return this.name;
	}
}
