package cn.mastercom.bigdata.StructData;

import java.util.HashMap;
import java.util.Map;


public enum ELocationMark
{
	None("None", -1), 
	BackFill("BackFill", 1), 
	DiDiDriver2("DiDiDriver", 2), 
	DiDiDriver("DiDiDriver", 3), 
	BaiDuSDK("BaiDuSDK", 4),
	GaoDeSDK("GaoDeSDK", 5),
	TengXunSDK("TengXunSDK", 6),
	CMCCWLAN("CmccWlan",7);

	private static Map<Integer, ELocationMark> keyMap;
	private static Map<String, ELocationMark> nameMap;

	static
	{
		keyMap = new HashMap<Integer, ELocationMark>();
		for (ELocationMark c : ELocationMark.values())
		{
			keyMap.put(c.getValue(), c);
		}
		
		nameMap = new HashMap<String, ELocationMark>();
		for (ELocationMark c : ELocationMark.values())
		{
			nameMap.put(c.getName().toUpperCase(), c);
		}
	}
	
	public static ELocationMark GetEnum(int value)
	{
		if(keyMap.containsKey(value))
		{
			return keyMap.get(value);
		}
		return None;
	}
	
	public static ELocationMark GetEnum(String value)
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

	ELocationMark(String name, int value)
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
