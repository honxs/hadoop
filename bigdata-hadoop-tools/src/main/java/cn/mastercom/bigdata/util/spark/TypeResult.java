package cn.mastercom.bigdata.util.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.mastercom.bigdata.util.IDataOutputer;
import scala.Tuple2;

public class TypeResult implements Serializable, IDataOutputer
{
	private static final long serialVersionUID = 1L;
	
	private Map<TypeInfo, ArrayList<String>> resultMap; 
	private TypeInfoMng typeInfoMng;
	
	public TypeResult(TypeInfoMng typeInfoMng)
	{
		this.typeInfoMng = typeInfoMng;
		
		resultMap = new HashMap<TypeInfo, ArrayList<String>>();
	}		
	
    public List<Tuple2<TypeInfo, Iterable<String>>> GetRdds()
    {
    	List<Tuple2<TypeInfo, Iterable<String>>> rtList = new ArrayList<Tuple2<TypeInfo, Iterable<String>>>();
    	if(resultMap != null)
    	{ 		    
    		for(Map.Entry<TypeInfo, ArrayList<String>> item : resultMap.entrySet())
        	{
    			rtList.add(new Tuple2<TypeInfo, Iterable<String>>(item.getKey(),item.getValue()));       		         		
        	}
    	}
    	return rtList;
    }
    
	@Override
	public int pushData(int type, List<String> dataList)
	{
		TypeInfo item = typeInfoMng.getTypeInfo(type);
		if(item == null)
		{
			return -1;
		}
		
		ArrayList<String> aList = resultMap.get(item);
		if(aList == null)
		{
			aList = new ArrayList<String>();
			resultMap.put(item, aList);
		}		
		aList.addAll(dataList);
		return 0;
	}
	
	@Override
	public int pushData(int type, String data) 
	{
		TypeInfo item = typeInfoMng.getTypeInfo(type);
		if(item == null)
		{
			return -1;
		}
		
		ArrayList<String> aList = resultMap.get(item);
		if(aList == null)
		{
			aList = new ArrayList<String>();
			resultMap.put(item, aList);
		}		
		aList.add(data);
		return 0;
	}
    
    @Override
	public void clear()
    {
    	resultMap = new HashMap<TypeInfo, ArrayList<String>>();
    }

	
}
