package cn.mastercom.bigdata.mro.loc;

import org.apache.hadoop.io.Text;

public class MroTableStat
{
	public void dealMro(Iterable<Text> values)
	{

	}

	public void dealXdrLocation(String xdrlocation)
	{
		String[] strs = xdrlocation.split("\t", -1);
		for (int i = 0; i < strs.length; ++i)
		{
			strs[i] = strs[i].trim();
		}
	}
}
