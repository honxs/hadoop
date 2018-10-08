package cn.mastercom.bigdata.util;

import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("Complie info")
public class CompileInfo
{
	@XStreamAlias("name")
	public String name;

	@XStreamAlias("complie mark")
	public String compile_mark;
}
