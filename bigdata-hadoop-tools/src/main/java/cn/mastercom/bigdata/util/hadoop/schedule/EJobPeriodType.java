package cn.mastercom.bigdata.util.hadoop.schedule;

import java.util.HashMap;
import java.util.Map;

public enum EJobPeriodType
{
	None("None", 0), 
	Minute("Minute", 1), 
	Hour("Hour", 2), 
	Day("Day", 3), 
	Weak("Weak", 4),
	Month("Month", 5);

	private static Map<Integer, EJobPeriodType> keyMap;
	private static Map<String, EJobPeriodType> nameMap;

	static
	{
		keyMap = new HashMap<Integer, EJobPeriodType>();
		for (EJobPeriodType c : EJobPeriodType.values())
		{
			keyMap.put(c.getValue(), c);
		}
		
		nameMap = new HashMap<String, EJobPeriodType>();
		for (EJobPeriodType c : EJobPeriodType.values())
		{
			nameMap.put(c.getName().toUpperCase(), c);
		}
	}
	
	public static EJobPeriodType GetEnum(int value)
	{
		if(keyMap.containsKey(value))
		{
			return keyMap.get(value);
		}
		return None;
	}
	
	public static EJobPeriodType GetEnum(String value)
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

	EJobPeriodType(String name, int value)
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
