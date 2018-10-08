package cn.mastercom.bigdata.mro.stat;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class TypeInfoMng implements Serializable
{
	private Map<Integer, TypeInfo> typeInfoMap;

	public TypeInfoMng()
	{
		typeInfoMap = new HashMap<Integer, TypeInfo>();
	}

	public int registTypeInfo(TypeInfo item)
	{
		typeInfoMap.put(item.getType(), item);
		return 0;
	}

	public TypeInfo getTypeInfo(int type)
	{
		return typeInfoMap.get(type);
	}

	public Map<Integer, TypeInfo> getTypeInfoMap()
	{
		return typeInfoMap;
	}

}
