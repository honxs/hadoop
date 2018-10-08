package cn.mastercom.bigdata.util;

import java.io.Serializable;
import java.util.List;

public class ResultOutputer implements Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private IDataOutputer dataOutputer = null;

	public ResultOutputer(IDataOutputer dataOutputer) 
	{
		this.dataOutputer = dataOutputer;
	}
	
	public int pushData(int type, List<String> valueList) throws Exception
	{
		return dataOutputer.pushData(type, valueList);
	}
	
	public int pushData(int type, String value) throws Exception
	{
		return dataOutputer.pushData(type, value);
	}
	
    public void clear() throws Exception
    {
    	dataOutputer.clear();
    }

}
