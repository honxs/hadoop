package com.chinamobile.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@NotProguard
public class PropertyHelper
{

	private static Properties property = new Properties();

	private static PropertyHelper newPorperty = null;

	private PropertyHelper()
	{
	}

	public static PropertyHelper getInstance()
	{
		if (null == newPorperty)
		{
			newPorperty = new PropertyHelper();
		}
		return newPorperty;
	}

	public String getProperty(String key, InputStream inStream)
	{
		try
		{
			property.load(inStream);
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		return property.getProperty(key);
	}
}
