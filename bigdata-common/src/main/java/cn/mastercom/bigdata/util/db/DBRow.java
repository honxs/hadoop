package cn.mastercom.bigdata.util.db;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class DBRow
{
	private Map<String, Object> values;
	private Object[] valuesIndex;
	private int columnCount;
	
	public DBRow(int columnCount)
	{
		this.columnCount = columnCount;
		
		values = new HashMap<String, Object>();
		valuesIndex = new Object[columnCount];
	}
	
	public int getColumnCount()
	{
		return columnCount;
	}
	
	public void add(String name, int index, Object value)
	{
		values.put(name, value);
		valuesIndex[index] = value;
	}
	
	public Object get(String name, Object defualtValue)
	{
		if(!values.containsKey(name))
		{
			return defualtValue;
		}
		return values.get(name);
	}
	
	public Object get(int index)
	{
		return valuesIndex[index];	
	}
	
	public Object get(String name)
	{
		if(!values.containsKey(name))
		{
			return null;
		}
		return values.get(name);	
	}
	
	public String getString(String name)
	{
		return String.valueOf(get(name));
	}
	
	public String getString(int index)
	{
		return String.valueOf(get(index));
	}
	
	public int getInt(String name)
	{
		return Integer.parseInt(get(name).toString());
	}
	
	public int getInt(int index)
	{
		return Integer.parseInt(get(index).toString());
	}
	
	public long getLong(String name)
	{
		return Long.parseLong(get(name).toString());
	}
	
	public long getLong(int index)
	{
		return Long.parseLong(get(index).toString());
	}
	
	public boolean getBoolean(String name)
	{
		return Boolean.parseBoolean(get(name).toString());
	}
	
	public boolean getBoolean(int index)
	{
		return Boolean.parseBoolean(get(index).toString());
	}
	
	public float getFloat(String name)
	{
		return Float.parseFloat(get(name).toString());
	}
	
	public float getFloat(int index)
	{
		return Float.parseFloat(get(index).toString());
	}
	
	public double getDouble(String name)
	{
		return Double.parseDouble(get(name).toString());
	}
	
	public double getDouble(int index)
	{
		return Double.parseDouble(get(index).toString());
	}
	
	public Date getDate(String name)
	{
		return (Date)get(name);
	}
	
	public Date getDate(int index)
	{
		return (Date)get(index);
	}
	
	
}
