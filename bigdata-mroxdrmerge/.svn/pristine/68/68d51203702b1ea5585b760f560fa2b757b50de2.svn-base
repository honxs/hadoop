package com.chinamobile.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NotProguard
public class ConfigUtil
{
	private static Logger log = LoggerFactory.getLogger(ConfigUtil.class);
	
	public static void main(final String args[]) {
		System.out.println("hello");
	}
	
	public static InputStream getConfigFile(String confpath, String filename)
	{
		InputStream inStream = null;
		ClassLoader clzld = Thread.currentThread().getContextClassLoader();
		String config = clzld.getResource(confpath + filename).getFile();

		try
		{
			inStream = new FileInputStream(config);
			return inStream;
			
		} catch (FileNotFoundException e1)
		{
			log.error(confpath + filename + " cannot be found.");
			return null;
		}
	}

}
