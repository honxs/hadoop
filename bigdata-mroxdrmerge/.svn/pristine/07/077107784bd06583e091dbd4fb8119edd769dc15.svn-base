package com.chinamobile.util;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

/**
 * @author zxw
 * 
 */

public class ArgUtil
{
	//private static Logger log = LoggerFactory.getLogger(ArgUtil.class);
	private static final Pattern argParamPattern = Pattern
			.compile("\\$\\s*(\\w+)\\s*\\$");
	private static Properties argsProperties = new Properties();

	private static boolean isInit = false;

	public static String getArg(String arg)
	{
		return argsProperties.getProperty(arg);
	}

	public static boolean isContainArg(String expression)
	{
		Matcher paraMatcher = argParamPattern.matcher(expression);
		if (paraMatcher.find())
		{
			return true;
		} else
		{
			return false;
		}
	}

	public static String replaceArgPara(String expression)
	{
		Matcher paraMatcher = argParamPattern.matcher(expression);
		StringBuffer paraStrBuf = new StringBuffer();
		while (paraMatcher.find())
		{
			String paraName = paraMatcher.group(1);
			String paraValue = argsProperties.getProperty(paraName);
			if (paraValue == null)
			{
				paraValue = "";
			} else
			{
				paraValue = paraValue.trim();
			}
			paraMatcher.appendReplacement(paraStrBuf, paraValue);
		}
		paraMatcher.appendTail(paraStrBuf);
		expression = paraStrBuf.toString();
		return expression;
	}

	/**
	 * 获取命令行参数，参数格式 a=b c= d getArgs
	 * 
	 * @param args
	 */
	public static void initArgs(String[] args)
	{
		if (!isInit)
		{
			Properties inproperties = new Properties();
			Pattern pattern = Pattern.compile("(\\w+)\\s*=\\s*([^=]+)\\s+");
			StringBuffer argBuffer = new StringBuffer();
			for (int i = 0; i < args.length; i++)
			{
				argBuffer.append(args[i]);
				argBuffer.append(" ");
			}
			System.out.println(argBuffer.toString());
			Matcher matcher = pattern.matcher(argBuffer.toString());
			while (matcher.find())
			{
				String property = matcher.group(1);
				String propertyValue = matcher.group(2).trim();
				inproperties.put(property, propertyValue);
				//log.debug(property + "\t" + propertyValue);
			}
			argsProperties = inproperties;
			isInit = true;
		}
	}

	public static boolean checkParam(Properties properties,
			String[] mustExistsKeys, String[] totalKeys, String useAge)
	{
		if (mustExistsKeys == null && totalKeys == null)
		{
			//log.error(useAge);
			return true;
		}
		if ((properties == null || properties.size() == 0)
				&& (mustExistsKeys != null || totalKeys != null))
		{
			//log.error(useAge);
			return false;
		}
		String lostKey = "";
		boolean isFit = true;
		for (String key : mustExistsKeys)
		{
			if (!properties.containsKey(key))
			{
				lostKey += key + "\t";
				isFit = false;
			}
		}
		String tooManyKeys = "";
		for (Object pkey : properties.keySet())
		{
			boolean isfind = false;
			for (String totalKey : totalKeys)
			{
				if (totalKey.equals(pkey))
				{
					isfind = true;
				}
			}
			if (isfind == false)
			{
				tooManyKeys += pkey + "\t";
			}
		}
		if ((lostKey != null && !"".equals(lostKey)) || (tooManyKeys != null)
				&& !"".equals(tooManyKeys))
		{
			//log.error("\n缺少参数：" + lostKey + "\n多的参数：" + tooManyKeys
			//		+ "\nuseAge: " + useAge);
		}
		return isFit;
	}
}
