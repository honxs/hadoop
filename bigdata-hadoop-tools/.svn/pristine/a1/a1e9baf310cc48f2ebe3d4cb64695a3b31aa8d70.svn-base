package cn.mastercom.bigdata.util.hadoop.schedule;

import java.util.HashMap;
import java.util.Map;

public enum EJobLevel
{
	None("None", 0), 
	Low("Low", 1), 
	Normal("Normal", 2), 
	High("High", 3), 
	Highest("Highest", 4);

	private static Map<Integer, EJobLevel> keyMap;
	private static Map<String, EJobLevel> nameMap;

	static
	{
		keyMap = new HashMap<Integer, EJobLevel>();
		for (EJobLevel c : EJobLevel.values())
		{
			keyMap.put(c.getValue(), c);
		}
		
		nameMap = new HashMap<String, EJobLevel>();
		for (EJobLevel c : EJobLevel.values())
		{
			nameMap.put(c.getName().toUpperCase(), c);
		}
	}
	
	public static EJobLevel GetEnum(int value)
	{
		if(keyMap.containsKey(value))
		{
			return keyMap.get(value);
		}
		return None;
	}
	
	public static EJobLevel GetEnum(String value)
	{
		String name = value.toUpperCase();
		if(nameMap.containsKey(name))
		{
			return nameMap.get(value);
		}
		return None;
	}

	private final String name;
	private final int value;

	EJobLevel(String name, int value)
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
