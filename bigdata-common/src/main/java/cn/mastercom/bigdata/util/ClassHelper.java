package cn.mastercom.bigdata.util;

import java.lang.reflect.Constructor;

public class ClassHelper
{
	public static Object GetClass(String className)
	{
		try
		{
			Class c = Class.forName(className);
			return c.newInstance();
		}
		catch (Exception e)
		{
			return null;
		}
	}

	public static Constructor GetClassConstructor(String className,
			Class<?>... parameterTypes)
	{
		try
		{
			Class c = Class.forName(className);
			java.lang.reflect.Constructor constructor = c
					.getConstructor(parameterTypes);
			return constructor;
		}
		catch (Exception e)
		{
			return null;
		}
	}

	public static Object GetClass(Constructor constructor, Object... initargs)
	{
		try
		{
			return constructor.newInstance(initargs);
		}
		catch (Exception e)
		{
			return null;
		}

	}

}
