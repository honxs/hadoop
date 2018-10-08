package cn.mastercom.bigdata.util;

import java.util.List;

public interface IDataOutputer
{
    
	public int pushData(int dataType, String value);
	public int pushData(int dataType, List<String> valueList);
	public void clear() ;
}
