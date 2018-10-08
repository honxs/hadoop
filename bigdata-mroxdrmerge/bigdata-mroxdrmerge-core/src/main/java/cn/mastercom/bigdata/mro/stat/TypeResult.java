package cn.mastercom.bigdata.mro.stat;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

@SuppressWarnings("serial")
public class TypeResult implements Serializable
{
	protected Map<TypeInfo, StringBuffer> resultMap;
	protected TypeInfoMng typeInfoMng;

	public TypeResult(TypeInfoMng typeInfoMng)
	{
		this.typeInfoMng = typeInfoMng;

		resultMap = new HashMap<TypeInfo, StringBuffer>();
	}

	public TypeResult()
	{
	}

	public int pushData(int type, String data)
	{
		TypeInfo item = typeInfoMng.getTypeInfo(type);
		if (item == null)
		{
			return -1;
		}

		StringBuffer aList = resultMap.get(item);
		if (aList == null)
		{
			aList = new StringBuffer();
			resultMap.put(item, aList);
			aList.append(data);
		}
		else
		{
			aList.append("\r\n");
			aList.append(data);
		}
		return 0;
	}

	public void cleanMap()
	{
		resultMap.clear();
	}

	public Set<Entry<TypeInfo, StringBuffer>> getMapEntry()
	{
		return resultMap.entrySet();
	}
}
