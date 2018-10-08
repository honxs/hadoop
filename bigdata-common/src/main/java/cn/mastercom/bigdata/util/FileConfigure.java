package cn.mastercom.bigdata.util;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.configuration.XMLConfiguration;


public class FileConfigure implements IConfigure
{
	protected String confPath = "";
	protected Map<String, Object> dataMap;
	
	public String getConfPath()
	{
		return confPath;
	}

	public FileConfigure(String confPath)
	{
		this.confPath = confPath;
		dataMap = new HashMap<String, Object>();
	}

	@Override
	public boolean loadConfigure()
	{
		if (!new File(confPath).exists())
		{
			return false;
		}

		try
		{
			XMLConfiguration config = new XMLConfiguration(confPath);
			config.setEncoding("UTF-8");
			config.setDelimiterParsingDisabled(true); 
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
		try
		{
			if (!new File(confPath).exists())
			{
				XMLConfiguration config = new XMLConfiguration();
				config.save(confPath);
			}

			XMLConfiguration config = new XMLConfiguration(confPath);
			config.setEncoding("UTF-8");
			config.setDelimiterParsingDisabled(true); 
			for (Map.Entry<String, Object> mapItem : dataMap.entrySet())
			{
				config.setProperty(mapItem.getKey(), mapItem.getValue());
			}
			config.save();
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return false;
		}
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
