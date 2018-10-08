package cn.mastercom.bigdata.util;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.configuration.XMLConfiguration;

public class StreamConfigure implements IConfigure
{
	protected InputStream inputStream = null;
	protected Map<String, Object> dataMap;

	public StreamConfigure(InputStream inputStream)
	{
		this.inputStream = inputStream;
		dataMap = new HashMap<String, Object>();
	}

	@Override
	public boolean loadConfigure()
	{
		try
		{
			XMLConfiguration config = new XMLConfiguration();
			config.setEncoding("UTF-8");
			config.setDelimiterParsingDisabled(true); 
			config.load(inputStream);
			Iterator<String> iter = config.getKeys();
			while (iter.hasNext())
			{
				String key = iter.next().toString();
				dataMap.put(key, config.getProperty(key)); 
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return false;
		}

		return true;
	}

	@Override
	public boolean saveConfigure()
	{
		return true;
	}

	@Override
	public Object getValue(String name)
	{
		return dataMap.get(name);
	}

	@Override
	public boolean setValue(String name, Object value)
	{
		dataMap.put(name, value);
		return true;
	}

	@Override
	public Object getValue(String name, Object defaultValue)
	{
		if(dataMap.containsKey(name))
		{
			return getValue(name);
		}
		else 
		{
			setValue(name, defaultValue);
			return defaultValue;
		}
	}
	
}
