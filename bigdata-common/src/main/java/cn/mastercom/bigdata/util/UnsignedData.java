package cn.mastercom.bigdata.util;

import java.io.IOException;
import java.math.BigDecimal;

public class UnsignedData
{

	public static final BigDecimal readUnsignedLong(long value)
			throws IOException
	{
		if (value >= 0)
			return new BigDecimal(value);
		long lowValue = value & 0x7fffffffffffffffL;
		return BigDecimal.valueOf(lowValue).add(BigDecimal.valueOf(Long.MAX_VALUE)).add(BigDecimal.valueOf(1));
	}

}
