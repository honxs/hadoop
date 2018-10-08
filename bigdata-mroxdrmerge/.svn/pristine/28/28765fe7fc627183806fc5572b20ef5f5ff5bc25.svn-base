package cn.mastercom.bigdata.StructData;

import java.util.HashMap;
import java.util.Map;

public enum ELocationType
{

	None("None", -1), 
	Gps1("ll", 1), //gps
	Gps2("ll2", 2), //gps2
	Wifi("wf", 3),//wifi 
	Cell("cl", 4),//cell
	GpsMT("lll", 5), //gps2
	Unknown("unknown", 100);

	private static Map<Integer, ELocationType> keyMap;
	private static Map<String, ELocationType> nameMap;

	static
	{
		keyMap = new HashMap<Integer, ELocationType>();
		for (ELocationType c : ELocationType.values())
		{
			keyMap.put(c.getValue(), c);
		}
		
		nameMap = new HashMap<String, ELocationType>();
		for (ELocationType c : ELocationType.values())
		{
			nameMap.put(c.getName().toUpperCase(), c);
		}
	}
	
	public static ELocationType GetEnum(int value)
	{
		if(keyMap.containsKey(value))
		{
			return keyMap.get(value);
		}
		return None;
	}
	
	public static ELocationType GetEnum(String value)
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

	ELocationType(String name, int value)
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
