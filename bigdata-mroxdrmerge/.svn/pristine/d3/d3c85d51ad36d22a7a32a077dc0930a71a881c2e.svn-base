package cn.mastercom.bigdata.util;

import java.util.concurrent.atomic.AtomicInteger;

public final class IndexGenerator
{
	private IndexGenerator()
	{
	}

	static AtomicInteger index = new AtomicInteger(1);

	public static int generate()
	{
		return index.getAndIncrement();
	}
}
