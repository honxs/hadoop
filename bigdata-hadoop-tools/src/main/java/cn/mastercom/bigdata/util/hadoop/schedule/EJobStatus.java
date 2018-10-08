package cn.mastercom.bigdata.util.hadoop.schedule;

import java.util.HashMap;
import java.util.Map;

public enum EJobStatus
{
	None("None", 0), 
	Prepare("Prepare", 1), 
	Working("Working", 2), 
	Success("Success", 3), 
	Fail("Fail", 4);

	private static Map<Integer, EJobStatus> keyMap;
	private static Map<String, EJobStatus> nameMap;

	static
	{
		keyMap = new HashMap<Integer, EJobStatus>();
		for (EJobStatus c : EJobStatus.values())
		{
			keyMap.put(c.getValue(), c);
		}
		
		nameMap = new HashMap<String, EJobStatus>();
		for (EJobStatus c : EJobStatus.values())
		{
			nameMap.put(c.getName().toUpperCase(), c);
		}
	}
	
	public static EJobStatus GetEnum(int value)
	{
		if(keyMap.containsKey(value))
		{
			return keyMap.get(value);
		}
		return None;
	}
	
	public static EJobStatus GetEnum(String value)
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

	EJobStatus(String name, int value)
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
